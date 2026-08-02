package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"agenttroop/internal/clock"
	"agenttroop/internal/mission"
	"agenttroop/internal/store"
	"agenttroop/internal/store/memory"
)

var (
	ctx  = context.Background()
	base = time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
)

func newService() (*Service, *memory.Store, *clock.FakeClock) {
	st := memory.New()
	clk := clock.NewFake(base)
	return New(st, clk, DefaultConfig()), st, clk
}

func mustRegister(t *testing.T, s *Service, id string, maxConc int, skills ...string) {
	t.Helper()
	caps := make([]store.Capability, len(skills))
	for i, sk := range skills {
		caps[i] = store.Capability{Skill: sk, Level: 0.9}
	}
	if err := s.RegisterAgent(ctx, &store.Agent{
		ID: id, Name: id, Platform: "http-echo", Capabilities: caps, MaxConcurrency: maxConc,
	}); err != nil {
		t.Fatalf("RegisterAgent: %v", err)
	}
}

// runOneTask 模拟 Agent 完成一个 OFFERED 任务（accept→start→complete）。
func runOneTask(t *testing.T, s *Service, agentID string, subs []*mission.Subtask) {
	t.Helper()
	offers, err := s.ListOffers(ctx, agentID)
	if err != nil {
		t.Fatalf("ListOffers: %v", err)
	}
	if len(offers) != 1 {
		t.Fatalf("agent %s offers = %d, want 1", agentID, len(offers))
	}
	sub := offers[0]
	sub, err = s.AcceptLease(ctx, sub.LeaseID, fenceOf(t, s, sub.LeaseID), sub.Version, agentID)
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	if _, err := s.StartSubtask(ctx, sub.ID, fenceOf(t, s, sub.LeaseID), sub.Version, agentID); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	cur := mustGet(t, s, sub.MissionID, sub.ID)
	if _, err := s.CompleteSubtask(ctx, sub.ID, fenceOf(t, s, sub.LeaseID),
		"idem-"+sub.ID, "artifact://echo/"+sub.ID, cur.Version, agentID); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
}

// fenceOf 测试辅助：查租约取 fencing token（模拟 Adapter 从 offer 响应获得）。
func fenceOf(t *testing.T, s *Service, leaseID string) int64 {
	t.Helper()
	l, err := s.GetLease(ctx, leaseID)
	if err != nil {
		t.Fatalf("GetLease(%s): %v", leaseID, err)
	}
	return l.FencingToken
}

func mustGet(t *testing.T, s *Service, missionID, subID string) *mission.Subtask {
	t.Helper()
	subs, err := s.ListSubtasks(ctx, missionID)
	if err != nil {
		t.Fatalf("ListSubtasks: %v", err)
	}
	for _, sub := range subs {
		if sub.ID == subID {
			return sub
		}
	}
	t.Fatalf("subtask %s not found", subID)
	return nil
}

func TestDAGValidation(t *testing.T) {
	s, _, _ := newService()
	// 环：a→b→a
	if _, err := s.CreateMission(ctx, "u1", "cyclic", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent, DependsOn: []string{"b"}},
		{Name: "b", Kind: mission.KindAgent, DependsOn: []string{"a"}},
	}); !errors.Is(err, ErrInvalidDAG) {
		t.Fatalf("cycle must be rejected, got %v", err)
	}
	// 未知依赖
	if _, err := s.CreateMission(ctx, "u1", "bad dep", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent, DependsOn: []string{"ghost"}},
	}); !errors.Is(err, ErrInvalidDAG) {
		t.Fatalf("unknown dep must be rejected, got %v", err)
	}
	// 重名
	if _, err := s.CreateMission(ctx, "u1", "dup", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent}, {Name: "a", Kind: mission.KindAgent},
	}); !errors.Is(err, ErrInvalidDAG) {
		t.Fatalf("dup name must be rejected, got %v", err)
	}
}

func TestChainMissionEndToEnd(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_research", 2, "web.research")
	mustRegister(t, s, "agt_write", 2, "write.zh")

	m, err := s.CreateMission(ctx, "u1", "chain", []TaskSpec{
		{Name: "collect", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "report", Kind: mission.KindAgent, RequiredSkills: []string{"write.zh"}, DependsOn: []string{"collect"}},
	})
	if err != nil {
		t.Fatalf("CreateMission: %v", err)
	}

	// 根节点已 READY，下游仍 PENDING
	subs, _ := s.ListSubtasks(ctx, m.ID)
	if subs[0].State != mission.StateReady && subs[1].State != mission.StateReady {
		t.Fatalf("root should be READY: %+v", subs)
	}

	// 调度：只有 collect 被放置，且只能给 research agent
	placed, _ := s.ScheduleOnce(ctx)
	if placed != 1 {
		t.Fatalf("placed = %d, want 1", placed)
	}
	offersW, _ := s.ListOffers(ctx, "agt_write")
	if len(offersW) != 0 {
		t.Fatal("write agent must not receive collect task (capability filter)")
	}

	runOneTask(t, s, "agt_research", nil)

	// 依赖传播：report 应已 READY
	report := mustGet(t, s, m.ID, subID(m.ID, "report"))
	if report.State != mission.StateReady {
		t.Fatalf("report should be READY after collect, got %s", report.State)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	runOneTask(t, s, "agt_write", nil)

	// Mission 终态
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionSucceeded {
		t.Fatalf("mission status = %s, want SUCCEEDED", final.Status)
	}
	// 事件序列含关键节点
	evs, _ := s.ListMissionEvents(ctx, m.ID, 0, 100)
	wantTypes := []string{
		"mission.created", "subtask.created", "subtask.deps_satisfied",
		"subtask.lease_offered", "subtask.lease_accepted", "subtask.started", "subtask.succeeded",
	}
	seen := map[string]bool{}
	for _, e := range evs {
		seen[e.Type] = true
	}
	for _, w := range wantTypes {
		if !seen[w] {
			t.Errorf("missing event type %s", w)
		}
	}
}

func TestSchedulerRespectsConcurrencyAndSkills(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_small", 1, "web.research") // 并发上限 1

	m, err := s.CreateMission(ctx, "u1", "parallel", []TaskSpec{
		{Name: "t1", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "t2", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "t3", Kind: mission.KindAgent, RequiredSkills: []string{"nonexistent"}},
	})
	if err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	placed, _ := s.ScheduleOnce(ctx)
	if placed != 1 { // 只放一个：并发上限 1；t3 无合格 Agent
		t.Fatalf("placed = %d, want 1 (concurrency cap)", placed)
	}
	// t3 留在 READY（无合格 Agent，等待下轮/人工干预）
	t3 := mustGet(t, s, m.ID, subID(m.ID, "t3"))
	if t3.State != mission.StateReady {
		t.Fatalf("t3 should stay READY, got %s", t3.State)
	}
}

func TestFailWithRetryThenMissionFailed(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")

	m, err := s.CreateMission(ctx, "u1", "retry", []TaskSpec{
		{Name: "flaky", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}, MaxAttempts: 1},
	})
	if err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	s.ScheduleOnce(ctx)
	offers, _ := s.ListOffers(ctx, "agt_a")
	sub := offers[0]
	token := fenceOf(t, s, sub.LeaseID)
	sub, _ = s.AcceptLease(ctx, sub.LeaseID, token, sub.Version, "agt_a")
	s.StartSubtask(ctx, sub.ID, token, sub.Version, "agt_a")
	cur := mustGet(t, s, m.ID, sub.ID)

	// 第一次失败 → 重试回 READY，attempt=1
	retried, err := s.FailSubtask(ctx, sub.ID, token, "boom", cur.Version, "agt_a")
	if err != nil {
		t.Fatalf("FailSubtask: %v", err)
	}
	if retried.State != mission.StateReady || retried.Attempt != 1 {
		t.Fatalf("should retry to READY attempt=1, got %s attempt=%d", retried.State, retried.Attempt)
	}

	// 再次执行并失败（MaxAttempts=1 已耗尽）→ FAILED → Mission FAILED
	s.ScheduleOnce(ctx)
	offers, _ = s.ListOffers(ctx, "agt_a")
	sub = offers[0]
	token = fenceOf(t, s, sub.LeaseID)
	sub, _ = s.AcceptLease(ctx, sub.LeaseID, token, sub.Version, "agt_a")
	s.StartSubtask(ctx, sub.ID, token, sub.Version, "agt_a")
	cur = mustGet(t, s, m.ID, sub.ID)
	if _, err := s.FailSubtask(ctx, sub.ID, token, "boom again", cur.Version, "agt_a"); err != nil {
		t.Fatalf("FailSubtask 2: %v", err)
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionFailed {
		t.Fatalf("mission = %s, want FAILED", final.Status)
	}
}

func TestCancelMissionCascades(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "cancel", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "b", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}, DependsOn: []string{"a"}},
	})
	s.ScheduleOnce(ctx)
	if err := s.CancelMission(ctx, m.ID, "u1"); err != nil {
		t.Fatalf("CancelMission: %v", err)
	}
	subs, _ := s.ListSubtasks(ctx, m.ID)
	for _, sub := range subs {
		if sub.State != mission.StateCancelled {
			t.Errorf("subtask %s = %s, want CANCELLED", sub.ID, sub.State)
		}
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionCancelled {
		t.Fatalf("mission = %s, want CANCELLED", final.Status)
	}
}

func TestSweeperRecyclesExpiredLease(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "sweep", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	s.ScheduleOnce(ctx)
	sub := mustGet(t, s, m.ID, subID(m.ID, "a"))
	if sub.State != mission.StateOffered {
		t.Fatalf("state = %s, want OFFERED", sub.State)
	}

	clk.Advance(31 * time.Second) // 超过 OfferTTL=30s
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	sub = mustGet(t, s, m.ID, subID(m.ID, "a"))
	if sub.State != mission.StateReady || sub.Assignee != "" {
		t.Fatalf("should recycle to READY, got %s assignee=%s", sub.State, sub.Assignee)
	}
	// 重调度可再放置
	placed, _ := s.ScheduleOnce(ctx)
	if placed != 1 {
		t.Fatalf("replaced = %d, want 1", placed)
	}
}

func TestAgentHealthMarkedSuspect(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	clk.Advance(91 * time.Second) // 超过 HeartbeatStale=90s
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	a, _ := s.GetAgent(ctx, "agt_a")
	if a.Health != "suspect" {
		t.Fatalf("health = %s, want suspect", a.Health)
	}
	// suspect 的 Agent 不再接收放置
	m, _ := s.CreateMission(ctx, "u1", "no-place", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	placed, _ := s.ScheduleOnce(ctx)
	if placed != 0 {
		t.Fatalf("placed = %d, want 0 for suspect agent", placed)
	}
	_ = m
}
