//go:build e2e

package e2e

// M7B 端到端验收：Lead 快照心跳 → child 结果入箱 → 显式 ingest →
// 心跳过期 fence → 健康继任者取得新租约与恢复上下文。

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"agenttroop/internal/api"
	"agenttroop/internal/clock"
	"agenttroop/internal/core"
	"agenttroop/internal/store/memory"
)

func TestE2ELeadInboxSnapshotTakeover(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC))
	svc := core.New(memory.New(), clk, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": "agt_lead", "name": "agt_lead", "platform": "custom",
		"capabilities":   []map[string]any{{"skill": "lead.coordinate", "level": 0.9}},
		"trigger_scopes": []string{"trigger.spawn_subtask"},
	}, 200)
	fakeAgent(t, "agt_worker", "work")
	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "recover lead",
		"tasks": []map[string]any{{"name": "lead", "kind": "agent",
			"required_skills": []string{"lead.coordinate"}}},
	}, 201)
	mid := created["id"].(string)
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule lead: %v", err)
	}
	offer := findOffer(t, "agt_lead", "lead")
	leadID, leadToken, leadVersion := acceptAndStart(t, "agt_lead", offer)

	snapshot := do(t, "POST", "/v1/subtasks/"+leadID+"/lead/heartbeat", map[string]any{
		"agent_id": "agt_lead", "fencing_token": leadToken, "expected_version": -1,
		"snapshot": map[string]any{"intent": "review child", "next": "ingest"},
	}, 200)
	if snapshot["version"] != float64(0) {
		t.Fatalf("snapshot=%v", snapshot)
	}
	dlg := do(t, "POST", "/v1/intents", map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_lead"},
		"action": "delegate", "idempotency_key": "e2e-m7-child",
		"parent_subtask_id": leadID, "fencing_token": leadToken, "parent_version": leadVersion,
		"task": map[string]any{"name": "child", "required_skills": []string{"work"}},
	}, 200)
	childID := dlg["subtask_id"].(string)
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule child: %v", err)
	}
	childOffer := findOffer(t, "agt_worker", "child")
	_, childToken, childVersion := acceptAndStart(t, "agt_worker", childOffer)
	do(t, "POST", "/v1/subtasks/"+childID+"/complete", map[string]any{
		"agent_id": "agt_worker", "fencing_token": childToken, "version": childVersion,
		"idempotency_key": "e2e-m7-result", "result_ref": "artifact://child-result",
	}, 200)

	inbox := do(t, "GET", "/v1/subtasks/"+leadID+"/lead/inbox?status=pending", nil, 200)
	items := inbox["items"].([]any)
	if len(items) != 1 {
		t.Fatalf("inbox=%v", inbox)
	}
	item := items[0].(map[string]any)
	itemID := item["id"].(string)
	do(t, "POST", "/v1/subtasks/"+leadID+"/lead/inbox/"+itemID+"/ingest", map[string]any{
		"agent_id": "agt_backup", "fencing_token": leadToken,
		"expected_version": int64(item["version"].(float64)), "mode": "summary",
	}, 403)
	do(t, "POST", "/v1/subtasks/"+leadID+"/lead/inbox/"+itemID+"/ingest", map[string]any{
		"agent_id": "agt_lead", "fencing_token": leadToken,
		"expected_version": int64(item["version"].(float64)), "mode": "summary",
	}, 200)
	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": "agt_backup", "name": "agt_backup", "platform": "custom",
		"capabilities":   []map[string]any{{"skill": "lead.coordinate", "level": 0.9}},
		"trigger_scopes": []string{"trigger.spawn_subtask"},
	}, 200)

	clk.Advance(core.DefaultConfig().LeadHeartbeatTTL + time.Second)
	if err := svc.SweepOnce(ctx); err != nil {
		t.Fatalf("takeover sweep: %v", err)
	}
	if st := subState(t, mid, leadID); st != "READY" {
		t.Fatalf("lead=%s after takeover", st)
	}
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule replacement: %v", err)
	}
	replacementOffer := findOffer(t, "agt_backup", "lead")
	if replacementOffer == nil {
		t.Fatal("backup did not receive lead")
	}
	acceptAndStart(t, "agt_backup", replacementOffer)
	recovery := do(t, "GET", "/v1/subtasks/"+leadID+"/lead/context", nil, 200)
	if recovery["snapshot"] == nil || len(recovery["inbox"].([]any)) != 1 {
		t.Fatalf("recovery=%v", recovery)
	}
	if recovery["inbox"].([]any)[0].(map[string]any)["status"] != "ingested" {
		t.Fatalf("recovery inbox lost ingest state: %v", recovery["inbox"])
	}
	value, _ := recovery["snapshot"].(map[string]any)["value"].(map[string]any)
	if value["intent"] != "review child" {
		t.Fatalf("snapshot JSON was not preserved: %v", recovery["snapshot"])
	}
}

func TestE2EBudgetHoldAndSettlement(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC))
	svc := core.New(memory.New(), clk, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": "agt_budget_lead", "name": "agt_budget_lead", "platform": "custom",
		"capabilities":   []map[string]any{{"skill": "lead.coordinate", "level": 0.9}},
		"trigger_scopes": []string{"trigger.spawn_subtask"},
	}, 200)
	fakeAgent(t, "agt_budget_worker", "work")
	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "budget hard cap", "budget_tokens": 100,
		"tasks": []map[string]any{{"name": "lead", "kind": "agent",
			"required_skills": []string{"lead.coordinate"}}},
	}, 201)
	mid := created["id"].(string)
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule lead: %v", err)
	}
	leadOffer := findOffer(t, "agt_budget_lead", "lead")
	leadID, leadToken, leadVersion := acceptAndStart(t, "agt_budget_lead", leadOffer)

	first := do(t, "POST", "/v1/intents", map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_budget_lead"},
		"action": "delegate", "idempotency_key": "e2e-budget-first",
		"parent_subtask_id": leadID, "fencing_token": leadToken, "parent_version": leadVersion,
		"task": map[string]any{"name": "first", "required_skills": []string{"work"}, "budget_tokens": 70},
	}, 200)
	childID := first["subtask_id"].(string)
	do(t, "POST", "/v1/intents", map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_budget_lead"},
		"action": "delegate", "idempotency_key": "e2e-budget-retry",
		"parent_subtask_id": leadID, "fencing_token": leadToken, "parent_version": leadVersion,
		"task": map[string]any{"name": "second", "budget_tokens": 40},
	}, 409)
	budget := do(t, "GET", "/v1/missions/"+mid+"/budget", nil, 200)
	account := budget["account"].(map[string]any)
	if account["held_tokens"] != float64(70) || account["available_tokens"] != float64(30) {
		t.Fatalf("held budget=%v", budget)
	}

	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule child: %v", err)
	}
	childOffer := findOffer(t, "agt_budget_worker", "first")
	_, childToken, childVersion := acceptAndStart(t, "agt_budget_worker", childOffer)
	do(t, "POST", "/v1/subtasks/"+childID+"/complete", map[string]any{
		"agent_id": "agt_budget_worker", "fencing_token": childToken, "version": childVersion,
		"idempotency_key": "e2e-budget-complete", "result_ref": "artifact://budget",
		"usage_tokens": 50,
	}, 200)
	budget = do(t, "GET", "/v1/missions/"+mid+"/budget", nil, 200)
	account = budget["account"].(map[string]any)
	if account["held_tokens"] != float64(0) || account["spent_tokens"] != float64(50) ||
		account["available_tokens"] != float64(50) {
		t.Fatalf("settled budget=%v", budget)
	}

	// 前一次预算拒绝没有消耗幂等键；退款后同键可成功落一个 50-token hold。
	do(t, "POST", "/v1/intents", map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_budget_lead"},
		"action": "delegate", "idempotency_key": "e2e-budget-retry",
		"parent_subtask_id": leadID, "fencing_token": leadToken, "parent_version": leadVersion,
		"task": map[string]any{"name": "second", "budget_tokens": 50},
	}, 200)
	budget = do(t, "GET", "/v1/missions/"+mid+"/budget", nil, 200)
	account = budget["account"].(map[string]any)
	if account["held_tokens"] != float64(50) || account["available_tokens"] != float64(0) {
		t.Fatalf("reused idempotency key budget=%v", budget)
	}
}

func TestE2EPermissionAttenuationAndContextPackage(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 8, 26, 11, 0, 0, 0, time.UTC))
	svc := core.New(memory.New(), clk, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	do(t, "POST", "/v1/agents/register", map[string]any{
		"id": "agt_context_lead", "name": "agt_context_lead", "platform": "custom",
		"capabilities":   []map[string]any{{"skill": "lead.coordinate", "level": 0.9}},
		"trigger_scopes": []string{"trigger.spawn_subtask"},
	}, 200)
	fakeAgent(t, "agt_context_worker", "work")
	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "least knowledge", "budget_tokens": 100,
		"tasks": []map[string]any{{
			"name": "lead", "kind": "agent", "required_skills": []string{"lead.coordinate"},
			"grants": map[string]any{
				"classification": "internal", "tool_scopes": []string{"search"},
				"board_views": []map[string]any{{
					"namespace": "shared", "keys": []string{"glossary"}, "mode": "rw",
				}},
			},
		}},
	}, 201)
	mid := created["id"].(string)
	do(t, "PUT", "/v1/missions/"+mid+"/board/shared/glossary", map[string]any{"term": "storage"}, 200)
	do(t, "PUT", "/v1/missions/"+mid+"/board/shared/secret", map[string]any{"token": "hidden"}, 200)
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatal(err)
	}
	leadOffer := findOffer(t, "agt_context_lead", "lead")
	leadPkg := leadOffer["context_package"].(map[string]any)
	if leadPkg["snapshot_hash"] == "" || len(leadPkg["board_views"].([]any)) != 1 {
		t.Fatalf("lead context=%v", leadPkg)
	}
	leadID, leadToken, leadVersion := acceptAndStart(t, "agt_context_lead", leadOffer)
	request := map[string]any{
		"source": map[string]any{"kind": "agent", "id": "agt_context_lead"},
		"action": "delegate", "idempotency_key": "e2e-context-child",
		"parent_subtask_id": leadID, "fencing_token": leadToken, "parent_version": leadVersion,
		"task": map[string]any{
			"name": "child", "required_skills": []string{"work"}, "budget_tokens": 20,
			"grants": map[string]any{"classification": "restricted"},
		},
	}
	do(t, "POST", "/v1/intents", request, 400)
	request["task"] = map[string]any{
		"name": "child", "required_skills": []string{"work"}, "budget_tokens": 20,
		"grants": map[string]any{
			"classification": "public", "tool_scopes": []string{"search"},
			"board_views": []map[string]any{{
				"namespace": "shared", "keys": []string{"glossary"}, "mode": "ro",
			}},
		},
	}
	child := do(t, "POST", "/v1/intents", request, 200)
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatal(err)
	}
	childOffer := findOffer(t, "agt_context_worker", "child")
	childPkg := childOffer["context_package"].(map[string]any)
	if childPkg["subtask_id"] != child["subtask_id"] || len(childPkg["board_views"].([]any)) != 1 ||
		childPkg["budget"].(map[string]any)["available_tokens"] != float64(80) {
		t.Fatalf("child context=%v", childPkg)
	}
	byLease := do(t, "GET", "/v1/leases/"+childOffer["lease_id"].(string)+"/context", nil, 200)
	if byLease["snapshot_hash"] != childPkg["snapshot_hash"] {
		t.Fatalf("offer context and lease context differ: %v %v", childPkg, byLease)
	}
}
