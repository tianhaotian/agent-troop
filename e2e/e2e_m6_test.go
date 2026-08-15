//go:build e2e

// M6 端到端验收（docs/plan/M6-delegate.md §5）：主子委托循环全程——
// Lead 领任务 → delegate 派生子女 → suspend 精确等待 → Worker 完成 → Lead 唤醒
// → 验收不通过 rework（带 feedback）→ 新子女完成 → accept → Mission SUCCEEDED。
package e2e

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"agenttroop/internal/api"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store/memory"
)

// subState 从 Mission 投影读取指定子任务状态。
func subState(t *testing.T, mid, subID string) string {
	t.Helper()
	st := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	for _, x := range st["subtasks"].([]any) {
		xs := x.(map[string]any)
		if xs["id"] == subID {
			return xs["state"].(string)
		}
	}
	t.Fatalf("subtask %s not found", subID)
	return ""
}

func TestE2EMasterSubDelegateLoop(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig()).
		WithStrategy(core.NewRoundRobin())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	// Lead 持 spawn_subtask scope（§7.4(1)）；Worker 纯执行
	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": "agt_lead", "name": "lead", "platform": "custom",
		"capabilities":   []map[string]any{{"skill": "lead.coordinate", "level": 0.9}},
		"trigger_scopes": []string{"trigger.spawn_subtask"},
	}, 200)
	fakeAgent(t, "agt_worker", "web.research")

	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "主子委托调研",
		"tasks": []map[string]any{
			{"name": "lead", "kind": "agent", "required_skills": []string{"lead.coordinate"}},
		},
	}, 201)
	mid := created["id"].(string)

	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offerL := findOffer(t, "agt_lead", "lead")
	if offerL == nil {
		t.Fatal("no offer for lead")
	}
	leadID, leadToken, leadVer := acceptAndStart(t, "agt_lead", offerL)

	// 1) Lead delegate 派生调研子女
	dlg := do(t, "POST", "/v1/intents", map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_lead"},
		"action": "delegate", "idempotency_key": "e2e-m6-dlg-1",
		"parent_subtask_id": leadID, "fencing_token": leadToken, "parent_version": leadVer,
		"task": map[string]any{
			"name": "research", "required_skills": []string{"web.research"},
			"input": map[string]any{"topic": "储能行业"},
		},
	}, 200)
	childID := dlg["subtask_id"].(string)
	if childID == "" || dlg["deduplicated"] == true {
		t.Fatalf("delegate = %v", dlg)
	}
	// 幂等重发返回同子女
	redlg := do(t, "POST", "/v1/intents", map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_lead"},
		"action": "delegate", "idempotency_key": "e2e-m6-dlg-1",
		"parent_subtask_id": leadID, "fencing_token": leadToken, "parent_version": leadVer,
		"task": map[string]any{"name": "research"},
	}, 200)
	if redlg["subtask_id"] != childID || redlg["deduplicated"] != true {
		t.Fatalf("delegate dedup = %v", redlg)
	}

	// 2) Lead 挂起精确等待该子女完成（where subtask_id）
	do(t, "POST", "/v1/subtasks/"+leadID+"/suspend", map[string]any{
		"agent_id": "agt_lead", "fencing_token": leadToken, "version": leadVer,
		"wake_on": map[string]any{
			"kind": "event",
			"event": map[string]any{"types": []string{"subtask.succeeded"},
				"where": map[string]any{"subtask_id": childID}},
			"deadline": time.Now().Add(30 * time.Second).UTC(),
		},
	}, 200)

	// 3) Worker 完成子女 → sweeper 唤醒 Lead
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offerC := findOffer(t, "agt_worker", "research")
	if offerC == nil {
		t.Fatal("no offer for research child")
	}
	cID, cToken, cVer := acceptAndStart(t, "agt_worker", offerC)
	if cID != childID {
		t.Fatalf("child id %s != delegated %s", cID, childID)
	}
	do(t, "POST", "/v1/subtasks/"+cID+"/complete", map[string]any{
		"agent_id": "agt_worker", "fencing_token": cToken, "version": cVer,
		"idempotency_key": "e2e-m6-c1", "result_ref": "artifact://draft1",
	}, 200)
	if err := svc.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if st := subState(t, mid, leadID); st != "READY" {
		t.Fatalf("lead = %s, want READY（子女完成精确唤醒）", st)
	}

	// 4) Lead 续跑验收：不通过 → rework（feedback + rework_of）
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offerL2 := findOffer(t, "agt_lead", "lead")
	if offerL2 == nil {
		t.Fatal("no offer for woken lead")
	}
	_, leadToken2, leadVer2 := acceptAndStart(t, "agt_lead", offerL2)
	rw := do(t, "POST", "/v1/intents", map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_lead"},
		"action": "delegate", "idempotency_key": "e2e-m6-dlg-2",
		"parent_subtask_id": leadID, "fencing_token": leadToken2, "parent_version": leadVer2,
		"task": map[string]any{
			"name": "research_v2", "required_skills": []string{"web.research"},
			"rework_of": childID, "feedback": "数据太旧，补充 2026 年数据",
		},
	}, 200)
	rwID := rw["subtask_id"].(string)
	if rwID == "" || rwID == childID {
		t.Fatalf("rework must spawn a new child: %v", rw)
	}
	// rework 链与 feedback 落 spec 可查
	midState := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	for _, x := range midState["subtasks"].([]any) {
		xs := x.(map[string]any)
		if xs["id"] == rwID {
			if xs["rework_of"] != childID {
				t.Fatalf("rework_of = %v", xs["rework_of"])
			}
			if fb, _ := xs["input"].(map[string]any)["feedback"]; fb != "数据太旧，补充 2026 年数据" {
				t.Fatalf("feedback = %v", fb)
			}
		}
	}

	// 5) Lead 再挂起等新子女 → Worker 完成 → Lead accept（完成自身）→ Mission SUCCEEDED
	do(t, "POST", "/v1/subtasks/"+leadID+"/suspend", map[string]any{
		"agent_id": "agt_lead", "fencing_token": leadToken2, "version": leadVer2,
		"wake_on": map[string]any{
			"kind": "event",
			"event": map[string]any{"types": []string{"subtask.succeeded"},
				"where": map[string]any{"subtask_id": rwID}},
			"deadline": time.Now().Add(30 * time.Second).UTC(),
		},
	}, 200)
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offerR := findOffer(t, "agt_worker", "research_v2")
	if offerR == nil {
		t.Fatal("no offer for rework child")
	}
	rID, rToken, rVer := acceptAndStart(t, "agt_worker", offerR)
	do(t, "POST", "/v1/subtasks/"+rID+"/complete", map[string]any{
		"agent_id": "agt_worker", "fencing_token": rToken, "version": rVer,
		"idempotency_key": "e2e-m6-c2", "result_ref": "artifact://final",
	}, 200)
	if err := svc.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offerL3 := findOffer(t, "agt_lead", "lead")
	if offerL3 == nil {
		t.Fatal("no offer for final lead resume")
	}
	fID, fToken, fVer := acceptAndStart(t, "agt_lead", offerL3)
	do(t, "POST", "/v1/subtasks/"+fID+"/complete", map[string]any{
		"agent_id": "agt_lead", "fencing_token": fToken, "version": fVer,
		"idempotency_key": "e2e-m6-lead", "result_ref": "artifact://report",
	}, 200)

	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("mission = %v", final["mission"])
	}
	fmt.Println("delegate → suspend(event where subtask_id) → wake → rework(feedback) → accept: ok")
}
