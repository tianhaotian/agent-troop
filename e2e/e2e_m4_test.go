//go:build e2e

// M4 端到端验收（docs/plan/M4-trigger-pipeline.md §5）：
// 两任务流水线——consume 挂起等 produce 的完成事件（subtask.succeeded + where 谓词），
// produce 完成后 sweeper 增量评估唤醒 consume；Agent 经 /v1/intents 幂等提交 Mission。
package e2e

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agenttroop/internal/api"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store/memory"
)

// findOffer 在 Agent 的 offers 中按子任务 ID 后缀定位（sub_<mission>_<name>，不依赖调度顺序）。
func findOffer(t *testing.T, agentID, nameSuffix string) map[string]any {
	t.Helper()
	for _, o := range do(t, "GET", "/v1/agents/"+agentID+"/offers", nil, 200)["offers"].([]any) {
		om := o.(map[string]any)
		sub := om["subtask"].(map[string]any)
		if strings.HasSuffix(sub["id"].(string), "_"+nameSuffix) {
			return om
		}
	}
	return nil
}

// acceptAndStart 领取 offer 并启动子任务，返回 (subtaskID, fencingToken, startedVersion)。
func acceptAndStart(t *testing.T, agentID string, o map[string]any) (string, int64, int64) {
	t.Helper()
	sub := o["subtask"].(map[string]any)
	token := int64(o["fencing_token"].(float64))
	accepted := do(t, "POST", "/v1/leases/"+o["lease_id"].(string)+"/accept", map[string]any{
		"agent_id": agentID, "fencing_token": token, "subtask_version": int64(sub["version"].(float64)),
	}, 200)
	started := do(t, "POST", "/v1/subtasks/"+sub["id"].(string)+"/start", map[string]any{
		"agent_id": agentID, "fencing_token": token, "version": int64(accepted["version"].(float64)),
	}, 200)
	return sub["id"].(string), token, int64(started["version"].(float64))
}

// TestE2EEventWakePipeline event 唤醒流水线：B 挂起等"A 完成"事件，A 完成后 sweeper 唤醒 B。
func TestE2EEventWakePipeline(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig()).
		WithStrategy(core.NewRoundRobin())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	fakeAgent(t, "agt_a", "web.research")
	fakeAgent(t, "agt_b", "web.research")

	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "event 唤醒流水线",
		"tasks": []map[string]any{
			{"name": "produce", "kind": "agent", "required_skills": []string{"web.research"}},
			{"name": "consume", "kind": "agent", "required_skills": []string{"web.research"}},
		},
	}, 201)
	mid := created["id"].(string)

	// round-robin：两任务分派给不同 Agent（具体谁拿谁不依赖出队次序）
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	holder := func(name string) (string, map[string]any) {
		for _, ag := range []string{"agt_a", "agt_b"} {
			if o := findOffer(t, ag, name); o != nil {
				return ag, o
			}
		}
		return "", nil
	}

	// consume 持有方启动后挂起：等 produce 的完成事件（where 谓词按 result_ref 过滤）
	consAgent, offerC := holder("consume")
	if offerC == nil {
		t.Fatal("no agent holds consume offer")
	}
	consID, consToken, consVer := acceptAndStart(t, consAgent, offerC)
	do(t, "POST", "/v1/subtasks/"+consID+"/suspend", map[string]any{
		"agent_id": consAgent, "fencing_token": consToken, "version": consVer,
		"wake_on": map[string]any{
			"kind":     "event",
			"event":    map[string]any{"types": []string{"subtask.succeeded"}, "where": map[string]any{"result_ref": "artifact://a-done"}},
			"deadline": time.Now().Add(30 * time.Second).UTC(),
		},
		"checkpoint": map[string]any{"stage": "waiting-input"},
	}, 200)

	// produce 持有方完成 produce → 产生 subtask.succeeded 事件（在注册水位线之后）
	prodAgent, offerP := holder("produce")
	if offerP == nil {
		t.Fatal("no agent holds produce offer")
	}
	prodID, prodToken, prodVer := acceptAndStart(t, prodAgent, offerP)
	do(t, "POST", "/v1/subtasks/"+prodID+"/complete", map[string]any{
		"agent_id": prodAgent, "fencing_token": prodToken, "version": prodVer,
		"idempotency_key": "e2e-m4-produce", "result_ref": "artifact://a-done",
	}, 200)

	// sweeper 增量评估：事件命中 → CAS 唤醒 consume
	if err := svc.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	midState := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	consState := ""
	for _, x := range midState["subtasks"].([]any) {
		xs := x.(map[string]any)
		if strings.HasSuffix(xs["id"].(string), "_consume") {
			consState = xs["state"].(string)
		}
	}
	if consState != "READY" {
		t.Fatalf("consume = %s, want READY（事件命中唤醒）", consState)
	}

	// 唤醒后重新调度续跑完成（可换 Agent：谁拿到 offer 谁跑）
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	var o map[string]any
	resumer := "agt_a"
	if o = findOffer(t, resumer, "consume"); o == nil {
		resumer = "agt_b"
		if o = findOffer(t, resumer, "consume"); o == nil {
			t.Fatal("no agent holds woken consume offer")
		}
	}
	sub := o["subtask"].(map[string]any)
	if cp, ok := sub["checkpoint"].(map[string]any); !ok || cp["stage"] != "waiting-input" {
		t.Fatalf("resumer must see checkpoint, got %v", sub["checkpoint"])
	}
	id, token, ver := acceptAndStart(t, resumer, o)
	do(t, "POST", "/v1/subtasks/"+id+"/complete", map[string]any{
		"agent_id": resumer, "fencing_token": token, "version": ver,
		"idempotency_key": "e2e-m4-consume", "result_ref": "artifact://final",
	}, 200)

	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("mission = %v", final["mission"])
	}
	fmt.Println("suspend(event) → produce completes → sweeper wakes → resume: ok")
}

// TestE2EIntentIdempotent /v1/intents：Agent 触发入口幂等——重发同键返回原 Mission。
func TestE2EIntentIdempotent(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL

	body := map[string]any{
		"source":          map[string]any{"kind": "agent", "id": "agt_ext"},
		"action":          "create_mission",
		"idempotency_key": "e2e-intent-1",
		"owner":           "e2e", "goal": "intent 幂等提交",
		"tasks": []map[string]any{{"name": "only", "kind": "agent"}},
	}
	first := do(t, "POST", "/v1/intents", body, 200)
	mid := first["mission_id"].(string)
	if mid == "" || first["deduplicated"] == true {
		t.Fatalf("first submit = %v", first)
	}
	second := do(t, "POST", "/v1/intents", body, 200)
	if second["mission_id"] != mid || second["deduplicated"] != true {
		t.Fatalf("duplicate submit = %v, want same mission + deduplicated:true", second)
	}
	// 触发者留痕：mission.created 事件 actor = intent source
	events, err := svc.ListMissionEvents(context.Background(), mid, 0, 100)
	if err != nil {
		t.Fatalf("ListMissionEvents: %v", err)
	}
	found := false
	for _, e := range events {
		if e.Type == "mission.created" && e.Actor.Kind == "agent" && e.Actor.ID == "agt_ext" {
			found = true
		}
	}
	if !found {
		t.Fatal("mission.created event should carry intent source as actor")
	}
	fmt.Println("POST /v1/intents idempotent create_mission + source tracing: ok")
}
