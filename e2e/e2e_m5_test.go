//go:build e2e

// M5 端到端验收（docs/plan/M5-cel-scope.md §5）：
// 1) CEL 条件流水线——consume 挂起等 board.shared.input_ready == true，
//    produce 完成后写黑板，BoardPut 增量钩子唤醒 consume；
// 2) scope 三级授权——未授权 Agent intent 403 且不消耗幂等键，
//    注册带 scope 后幂等提交成功。
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

// TestE2ECELConditionPipeline CEL 条件唤醒流水线（H1）。
func TestE2ECELConditionPipeline(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig()).
		WithStrategy(core.NewRoundRobin())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	fakeAgent(t, "agt_a", "web.research")
	fakeAgent(t, "agt_b", "web.research")

	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "CEL 条件流水线",
		"tasks": []map[string]any{
			{"name": "produce", "kind": "agent", "required_skills": []string{"web.research"}},
			{"name": "consume", "kind": "agent", "required_skills": []string{"web.research"}},
		},
	}, 201)
	mid := created["id"].(string)

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

	// consume 持有方启动后挂起：CEL 条件等输入就绪标志
	consAgent, offerC := holder("consume")
	if offerC == nil {
		t.Fatal("no agent holds consume offer")
	}
	consID, consToken, consVer := acceptAndStart(t, consAgent, offerC)
	do(t, "POST", "/v1/subtasks/"+consID+"/suspend", map[string]any{
		"agent_id": consAgent, "fencing_token": consToken, "version": consVer,
		"wake_on": map[string]any{
			"kind":      "condition",
			"condition": map[string]any{"expr": `board.shared.input_ready == true`},
			"deadline":  time.Now().Add(30 * time.Second).UTC(),
		},
	}, 200)

	// produce 完成 → 然后写黑板标志位 → BoardPut 增量钩子直接唤醒（无需 sweep）
	prodAgent, offerP := holder("produce")
	if offerP == nil {
		t.Fatal("no agent holds produce offer")
	}
	prodID, prodToken, prodVer := acceptAndStart(t, prodAgent, offerP)
	do(t, "POST", "/v1/subtasks/"+prodID+"/complete", map[string]any{
		"agent_id": prodAgent, "fencing_token": prodToken, "version": prodVer,
		"idempotency_key": "e2e-m5-produce", "result_ref": "artifact://input",
	}, 200)
	do(t, "PUT", "/v1/missions/"+mid+"/board/shared/input_ready", true, 200)

	midState := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	consState := ""
	for _, x := range midState["subtasks"].([]any) {
		xs := x.(map[string]any)
		if strings.HasSuffix(xs["id"].(string), "_consume") {
			consState = xs["state"].(string)
		}
	}
	if consState != "READY" {
		t.Fatalf("consume = %s, want READY（CEL 条件命中唤醒）", consState)
	}

	// 唤醒后重新调度续跑完成
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
	id, token, ver := acceptAndStart(t, resumer, o)
	do(t, "POST", "/v1/subtasks/"+id+"/complete", map[string]any{
		"agent_id": resumer, "fencing_token": token, "version": ver,
		"idempotency_key": "e2e-m5-consume", "result_ref": "artifact://final",
	}, 200)

	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("mission = %v", final["mission"])
	}
	fmt.Println("suspend(condition.expr) → BoardPut hit → incremental wake → resume: ok")
}

// TestE2EIntentScopeAuthz scope 三级授权（H2）：403 → 授权 → 幂等成功。
func TestE2EIntentScopeAuthz(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL

	intent := map[string]any{
		"source":          map[string]any{"kind": "agent", "id": "agt_ext"},
		"action":          "create_mission",
		"idempotency_key": "e2e-m5-scope",
		"owner":           "e2e", "goal": "scope 授权",
		"tasks":           []map[string]any{{"name": "only", "kind": "agent"}},
	}
	// 未注册 Agent → 403
	do(t, "POST", "/v1/intents", intent, 403)
	// 注册但无 scope（默认收紧）→ 403
	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": "agt_ext", "name": "agt_ext", "platform": "custom",
	}, 200)
	do(t, "POST", "/v1/intents", intent, 403)
	// 授予 trigger.create_mission 后放行；且此前 403 未消耗幂等键（非 deduplicated）
	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": "agt_ext", "name": "agt_ext", "platform": "custom",
		"trigger_scopes": []string{"trigger.create_mission"},
	}, 200)
	first := do(t, "POST", "/v1/intents", intent, 200)
	if first["mission_id"] == "" || first["deduplicated"] == true {
		t.Fatalf("403 attempts must not burn idempotency key: %v", first)
	}
	// 重发同键 → 正常幂等
	second := do(t, "POST", "/v1/intents", intent, 200)
	if second["mission_id"] != first["mission_id"] || second["deduplicated"] != true {
		t.Fatalf("resubmit = %v, want same mission + deduplicated:true", second)
	}
	fmt.Println("intent 403 (unregistered/no-scope) → grant scope → idempotent submit: ok")
}
