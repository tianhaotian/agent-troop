package memory

import (
	"context"
	"errors"
	"testing"
	"time"

	"agenttroop/internal/clock"
	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

var (
	ctx  = context.Background()
	now  = time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	sys  = store.Actor{Kind: "system", ID: "test"}
	agtA = store.Actor{Kind: "agent", ID: "agt_a"}
)

func setup(t *testing.T) (*Store, *mission.Mission, []*mission.Subtask) {
	t.Helper()
	s := New()
	m := &mission.Mission{ID: "msn_1", Owner: "u1", Goal: "g", Status: mission.MissionActive}
	subs := []*mission.Subtask{
		{ID: "sub_a", MissionID: "msn_1", Kind: mission.KindAgent, State: mission.StatePending,
			Scheduling: mission.SchedulingSpec{Priority: 10}},
		{ID: "sub_b", MissionID: "msn_1", Kind: mission.KindAgent, State: mission.StatePending,
			DependsOn: []string{"sub_a"}, Scheduling: mission.SchedulingSpec{Priority: 5}},
	}
	if err := s.CreateMission(ctx, m, subs, sys, now); err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	return s, m, subs
}

func registerAgent(t *testing.T, s *Store, id string, skills ...string) {
	t.Helper()
	caps := make([]store.Capability, len(skills))
	for i, sk := range skills {
		caps[i] = store.Capability{Skill: sk, Level: 0.9}
	}
	if err := s.UpsertAgent(ctx, &store.Agent{
		ID: id, Name: id, Platform: "http-echo", Capabilities: caps,
		MaxConcurrency: 2, Health: "healthy",
	}, now); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
}

func TestTransitionCASAndEvents(t *testing.T) {
	s, _, _ := setup(t)

	// 正常迁移 PENDING → READY
	sub, err := s.TransitionSubtask(ctx, "sub_a", mission.EvDepsSatisfied, 0, sys, nil, now, nil)
	if err != nil || sub.State != mission.StateReady {
		t.Fatalf("transition: %v, state=%s", err, sub.State)
	}
	// 版本竞争：旧 version 写必须失败（乐观锁，§4.3）
	if _, err := s.TransitionSubtask(ctx, "sub_a", mission.EvLeaseOffered, 0, sys, nil, now, nil); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale version should conflict, got %v", err)
	}
	// 非法迁移：READY 不能直接 started
	if _, err := s.TransitionSubtask(ctx, "sub_a", mission.EvStarted, 1, sys, nil, now, nil); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("illegal transition should conflict, got %v", err)
	}
	// 事件已按序追加
	evs, _ := s.ListMissionEvents(ctx, "msn_1", 0, 100)
	if len(evs) != 4 { // mission.created + 2×subtask.created + deps_satisfied
		t.Fatalf("events = %d, want 4", len(evs))
	}
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq <= evs[i-1].Seq {
			t.Fatal("event seq must be strictly increasing")
		}
	}
}

func TestDequeueReadyOrdering(t *testing.T) {
	s, _, _ := setup(t)
	clk := clock.NewFake(now)
	dl := clk.Now().Add(time.Hour)
	// sub_b 依赖未满足仍为 PENDING；再造一个低优先级 READY
	s.TransitionSubtask(ctx, "sub_a", mission.EvDepsSatisfied, 0, sys, nil, clk.Now(), nil)
	s.CreateMission(ctx, &mission.Mission{ID: "msn_2", Owner: "u1", Goal: "g2", Status: mission.MissionActive},
		[]*mission.Subtask{{ID: "sub_c", MissionID: "msn_2", Kind: mission.KindAgent,
			State: mission.StateReady, Scheduling: mission.SchedulingSpec{Priority: 50, Deadline: &dl}}}, sys, now)

	ready, _ := s.DequeueReady(ctx, 10)
	if len(ready) != 2 || ready[0].ID != "sub_c" || ready[1].ID != "sub_a" {
		t.Fatalf("ordering wrong: %+v", ready)
	}
}

func TestLeaseLifecycleAndFencing(t *testing.T) {
	s, _, _ := setup(t)
	clk := clock.NewFake(now)
	registerAgent(t, s, "agt_a", "web.research")
	s.TransitionSubtask(ctx, "sub_a", mission.EvDepsSatisfied, 0, sys, nil, clk.Now(), nil)

	// 发放租约
	l1, err := s.OfferLease(ctx, "sub_a", "agt_a", 1, 30*time.Second, sys, clk.Now())
	if err != nil {
		t.Fatalf("OfferLease: %v", err)
	}
	// 同一子任务第二个活跃租约必须被拒（双发防护，§4.3）
	if _, err := s.OfferLease(ctx, "sub_a", "agt_a", 2, 30*time.Second, sys, clk.Now()); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second active lease must conflict, got %v", err)
	}
	// fencing token 错误：accept 拒绝
	if _, err := s.AcceptLease(ctx, l1.ID, l1.FencingToken+999, 2, agtA, clk.Now()); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("bad fencing token must be fenced, got %v", err)
	}
	// 正确 accept → start → complete
	sub, err := s.AcceptLease(ctx, l1.ID, l1.FencingToken, 2, agtA, clk.Now())
	if err != nil || sub.State != mission.StateLeased {
		t.Fatalf("AcceptLease: %v state=%s", err, sub.State)
	}
	if _, err := s.StartSubtask(ctx, "sub_a", l1.FencingToken, 3, agtA, clk.Now()); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	if _, err := s.CompleteSubtask(ctx, "sub_a", l1.FencingToken, "idem-1", "artifact://r1", 4, agtA, clk.Now()); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	// 幂等重放：同 key 返回成功结果 + ErrDuplicate（API 层译为 200）
	dup, err := s.CompleteSubtask(ctx, "sub_a", l1.FencingToken, "idem-1", "artifact://r1", 99, agtA, clk.Now())
	if !errors.Is(err, store.ErrDuplicate) || dup.State != mission.StateSucceeded {
		t.Fatalf("idempotent replay: %v state=%s", err, dup.State)
	}
	// 租约已释放，Agent 并发数归零
	a, _ := s.GetAgent(ctx, "agt_a")
	if a.Running != 0 {
		t.Fatalf("agent running = %d, want 0", a.Running)
	}
}

func TestExpireLeasesRecyclesToReady(t *testing.T) {
	s, _, _ := setup(t)
	clk := clock.NewFake(now)
	registerAgent(t, s, "agt_a", "web.research")
	s.TransitionSubtask(ctx, "sub_a", mission.EvDepsSatisfied, 0, sys, nil, clk.Now(), nil)
	l1, _ := s.OfferLease(ctx, "sub_a", "agt_a", 1, 30*time.Second, sys, clk.Now())

	clk.Advance(31 * time.Second) // 租约超时
	n, _ := s.ExpireLeases(ctx, clk.Now())
	if n != 1 {
		t.Fatalf("expired = %d, want 1", n)
	}
	subs, _ := s.ListSubtasks(ctx, "msn_1")
	if subs[0].State != mission.StateReady || subs[0].Assignee != "" {
		t.Fatalf("subtask should recycle to READY, got %s", subs[0].State)
	}
	// 过期租约的僵尸写入被拒（fencing，§4.3）
	cur, _ := s.ListSubtasks(ctx, "msn_1")
	if _, err := s.CompleteSubtask(ctx, "sub_a", l1.FencingToken, "zombie", "r", cur[0].Version, agtA, clk.Now()); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("zombie write must be fenced, got %v", err)
	}
	// 回收后可重新发放，fencing token 严格递增
	l2, err := s.OfferLease(ctx, "sub_a", "agt_a", cur[0].Version, 30*time.Second, sys, clk.Now())
	if err != nil || l2.FencingToken <= l1.FencingToken {
		t.Fatalf("re-offer: %v, token %d <= %d", err, l2.FencingToken, l1.FencingToken)
	}
}

func TestFailSubtaskReleasesLease(t *testing.T) {
	s, _, _ := setup(t)
	clk := clock.NewFake(now)
	registerAgent(t, s, "agt_a", "x")
	s.TransitionSubtask(ctx, "sub_a", mission.EvDepsSatisfied, 0, sys, nil, clk.Now(), nil)
	l1, _ := s.OfferLease(ctx, "sub_a", "agt_a", 1, 30*time.Second, sys, clk.Now())
	sub, _ := s.AcceptLease(ctx, l1.ID, l1.FencingToken, 2, agtA, clk.Now())
	s.StartSubtask(ctx, "sub_a", l1.FencingToken, sub.Version, agtA, clk.Now())

	cur, _ := s.ListSubtasks(ctx, "msn_1")
	failed, err := s.FailSubtask(ctx, "sub_a", l1.FencingToken, "boom", cur[0].Version, agtA, clk.Now())
	if err != nil || failed.State != mission.StateFailed {
		t.Fatalf("FailSubtask: %v state=%s", err, failed.State)
	}
	a, _ := s.GetAgent(ctx, "agt_a")
	if a.Running != 0 {
		t.Fatalf("lease not released, running=%d", a.Running)
	}
	// 失败后可显式重试回 READY（§5.4）
	if _, err := s.TransitionSubtask(ctx, "sub_a", mission.EvRetried, failed.Version, sys,
		map[string]any{"reason": "manual retry"}, clk.Now(), func(st *mission.Subtask) error {
			st.Attempt++
			return nil
		}); err != nil {
		t.Fatalf("retry: %v", err)
	}
}
