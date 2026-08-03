//go:build e2e

// M3 端到端验收（docs/plan/M3-sched-trigger.md §5）：
// Agent 执行中 suspend(timer) → sweeper 唤醒 → 换 Agent 凭 checkpoint 续跑完成；
// wake TTL 过期 → FAILED + Mission FAILED。
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

// TestE2ESuspendWakeResume 挂起-唤醒全链路：释放租约 → 定时唤醒 → 换 Agent 续跑。
func TestE2ESuspendWakeResume(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig()).
		WithStrategy(core.NewRoundRobin()) // 保证唤醒后派给另一个 Agent
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	fakeAgent(t, "agt_a", "web.research")
	fakeAgent(t, "agt_b", "web.research")

	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "挂起续跑",
		"tasks": []map[string]any{
			{"name": "long", "kind": "agent", "required_skills": []string{"web.research"}},
		},
	}, 201)
	mid := created["id"].(string)

	// agt_a 领取并启动
	_, _ = svc.ScheduleOnce(ctx)
	o := do(t, "GET", "/v1/agents/agt_a/offers", nil, 200)["offers"].([]any)[0].(map[string]any)
	sub := o["subtask"].(map[string]any)
	subID := sub["id"].(string)
	token := int64(o["fencing_token"].(float64))
	accepted := do(t, "POST", "/v1/leases/"+o["lease_id"].(string)+"/accept", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "subtask_version": int64(sub["version"].(float64)),
	}, 200)
	started := do(t, "POST", "/v1/subtasks/"+subID+"/start", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "version": int64(accepted["version"].(float64)),
	}, 200)

	// 挂起：300ms 后定时唤醒，TTL 30s，带检查点
	wakeAt := time.Now().Add(300 * time.Millisecond).UTC()
	ttl := time.Now().Add(30 * time.Second).UTC()
	do(t, "POST", "/v1/subtasks/"+subID+"/suspend", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "version": int64(started["version"].(float64)),
		"wake_on":    map[string]any{"kind": "timer", "at": wakeAt, "deadline": ttl},
		"checkpoint": map[string]any{"step": 1},
	}, 200)

	// 等待到点后 sweeper 唤醒（手动驱动，等价 troopd 后台循环）
	time.Sleep(400 * time.Millisecond)
	if err := svc.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	// round-robin：唤醒后派给 agt_b，且 offer 携带检查点
	if _, err := svc.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offersB := do(t, "GET", "/v1/agents/agt_b/offers", nil, 200)["offers"].([]any)
	if len(offersB) != 1 {
		t.Fatalf("agt_b offers = %d, want 1（换 Agent 续跑）", len(offersB))
	}
	subB := offersB[0].(map[string]any)["subtask"].(map[string]any)
	cp, ok := subB["checkpoint"].(map[string]any)
	if !ok || cp["step"].(float64) != 1 {
		t.Fatalf("resuming agent must see checkpoint, got %v", subB["checkpoint"])
	}

	// agt_b 续跑完成
	tokenB := int64(offersB[0].(map[string]any)["fencing_token"].(float64))
	accB := do(t, "POST", "/v1/leases/"+offersB[0].(map[string]any)["lease_id"].(string)+"/accept", map[string]any{
		"agent_id": "agt_b", "fencing_token": tokenB, "subtask_version": int64(subB["version"].(float64)),
	}, 200)
	stB := do(t, "POST", "/v1/subtasks/"+subID+"/start", map[string]any{
		"agent_id": "agt_b", "fencing_token": tokenB, "version": int64(accB["version"].(float64)),
	}, 200)
	do(t, "POST", "/v1/subtasks/"+subID+"/complete", map[string]any{
		"agent_id": "agt_b", "fencing_token": tokenB, "version": int64(stB["version"].(float64)),
		"idempotency_key": "e2e-m3-done", "result_ref": "artifact://final",
	}, 200)
	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "SUCCEEDED" {
		t.Fatalf("mission = %v", final["mission"])
	}
	fmt.Println("suspend(timer) → wake → resume on another agent with checkpoint: ok")
}

// TestE2EWakeTimeout wake TTL 过期：FAILED + 下游级联取消 + Mission FAILED。
func TestE2EWakeTimeout(t *testing.T) {
	svc := core.New(memory.New(), clock.RealClock{}, core.DefaultConfig())
	srv := httptest.NewServer(api.New(svc).Handler())
	defer srv.Close()
	base = srv.URL
	ctx := context.Background()

	fakeAgent(t, "agt_a", "web.research")
	created := do(t, "POST", "/v1/missions", map[string]any{
		"owner": "e2e", "goal": "唤醒超时",
		"tasks": []map[string]any{
			{"name": "long", "kind": "agent", "required_skills": []string{"web.research"}},
			{"name": "next", "kind": "agent", "depends_on": []string{"long"}},
		},
	}, 201)
	mid := created["id"].(string)

	_, _ = svc.ScheduleOnce(ctx)
	o := do(t, "GET", "/v1/agents/agt_a/offers", nil, 200)["offers"].([]any)[0].(map[string]any)
	sub := o["subtask"].(map[string]any)
	subID := sub["id"].(string)
	token := int64(o["fencing_token"].(float64))
	accepted := do(t, "POST", "/v1/leases/"+o["lease_id"].(string)+"/accept", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "subtask_version": int64(sub["version"].(float64)),
	}, 200)
	started := do(t, "POST", "/v1/subtasks/"+subID+"/start", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "version": int64(accepted["version"].(float64)),
	}, 200)

	// 挂起：at 200ms / TTL 400ms；sweeper 在 TTL 之后才跑 → 不唤醒，直接过期
	wakeAt := time.Now().Add(200 * time.Millisecond).UTC()
	ttl := time.Now().Add(400 * time.Millisecond).UTC()
	do(t, "POST", "/v1/subtasks/"+subID+"/suspend", map[string]any{
		"agent_id": "agt_a", "fencing_token": token, "version": int64(started["version"].(float64)),
		"wake_on": map[string]any{"kind": "timer", "at": wakeAt, "deadline": ttl},
	}, 200)
	time.Sleep(500 * time.Millisecond)
	if err := svc.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}

	final := do(t, "GET", "/v1/missions/"+mid, nil, 200)
	if final["mission"].(map[string]any)["status"] != "FAILED" {
		t.Fatalf("mission = %v, want FAILED (wake_timeout)", final["mission"])
	}
	for _, x := range final["subtasks"].([]any) {
		xs := x.(map[string]any)
		want := "CANCELLED" // next（下游级联）
		if strings.HasSuffix(xs["id"].(string), "_long") {
			want = "FAILED" // long（wake_timeout）
		}
		if xs["state"] != want {
			t.Fatalf("%s = %s, want %s", xs["id"], xs["state"], want)
		}
	}
	fmt.Println("wake TTL expiry → FAILED + cascade cancel: ok")
}
