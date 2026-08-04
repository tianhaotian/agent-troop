package core

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// startOne 辅助：调度并把 agt 的一个 offer 推进到 RUNNING，返回 (sub, token)。
func startOne(t *testing.T, s *Service, agentID string) (*mission.Subtask, int64) {
	t.Helper()
	s.ScheduleOnce(ctx)
	offers, err := s.ListOffers(ctx, agentID)
	if err != nil || len(offers) == 0 {
		t.Fatalf("agent %s has no offer: %v", agentID, err)
	}
	sub := offers[0]
	token := fenceOf(t, s, sub.LeaseID)
	sub, err = s.AcceptLease(ctx, sub.LeaseID, token, sub.Version, agentID)
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	if _, err := s.StartSubtask(ctx, sub.ID, token, sub.Version, agentID); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	return mustGet(t, s, sub.MissionID, sub.ID), token
}

// ---- T4：timer 挂起 → 唤醒 → 换 Agent 凭检查点续跑（§7.3/§14.4 端到端语义） ----

func TestSuspendTimerWakeResume(t *testing.T) {
	s, _, clk := newService()
	s.WithStrategy(NewRoundRobin()) // 保证唤醒后派给另一个 Agent
	mustRegister(t, s, "agt_a", 2, "web.research")
	mustRegister(t, s, "agt_b", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "挂起续跑", []TaskSpec{
		{Name: "long", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	sub, token := startOne(t, s, "agt_a")

	// 挂起：10 分钟后唤醒，TTL 1 小时，带检查点
	wakeAt := clk.Now().Add(10 * time.Minute)
	ttl := clk.Now().Add(time.Hour)
	suspended, err := s.Suspend(ctx, sub.ID, token, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeTimer, At: &wakeAt, Deadline: &ttl},
		json.RawMessage(`{"progress": 42}`))
	if err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if suspended.State != mission.StateWaiting {
		t.Fatalf("state = %s, want WAITING", suspended.State)
	}
	// 租约已释放：agt_a 并发额度回收、subtask 无 lease
	if a, _ := s.GetAgent(ctx, "agt_a"); a.Running != 0 {
		t.Fatalf("lease should be released, running = %d", a.Running)
	}
	if _, err := s.GetLease(ctx, sub.LeaseID); err == nil {
		// LeaseID 已清空则 sub.LeaseID=="" 会 NotFound；原租约应 RELEASED
	}
	// 旧 token 已失效（fencing）：完成请求被拒
	if _, err := s.CompleteSubtask(ctx, sub.ID, token, "idem-x", "r", suspended.Version, "agt_a"); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("stale token after suspend must fence, got %v", err)
	}
	// 到期前 sweep 不唤醒
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m.ID, sub.ID); cur.State != mission.StateWaiting {
		t.Fatalf("premature wake: %s", cur.State)
	}
	// 到期 → sweep 唤醒 → READY
	clk.Advance(11 * time.Minute)
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	// 挂起期间 Agent 被标 suspect；心跳后恢复 healthy（存活证明）
	s.Heartbeat(ctx, "agt_a")
	s.Heartbeat(ctx, "agt_b")
	cur := mustGet(t, s, m.ID, sub.ID)
	if cur.State != mission.StateReady {
		t.Fatalf("after wake = %s, want READY", cur.State)
	}
	// 检查点与 wake 字段：checkpoint 保留供续跑，wake 注册已清空（一次性）
	if string(cur.Checkpoint) != `{"progress": 42}` {
		t.Fatalf("checkpoint = %s", cur.Checkpoint)
	}
	if cur.WakeKind != "" || cur.WakeAt != nil {
		t.Fatal("wake registration should be cleared after firing")
	}
	// 重新调度：round-robin 派给 agt_b（换 Agent 续跑）
	sub2, token2 := startOne(t, s, "agt_b")
	if string(sub2.Checkpoint) != `{"progress": 42}` {
		t.Fatalf("resuming agent must see checkpoint, got %s", sub2.Checkpoint)
	}
	if _, err := s.CompleteSubtask(ctx, sub2.ID, token2, "idem-done", "artifact://final", sub2.Version, "agt_b"); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionSucceeded {
		t.Fatalf("mission = %s, want SUCCEEDED", final.Status)
	}
}

// ---- T4：manual 唤醒 ----

func TestManualWake(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "人工唤醒", []TaskSpec{
		{Name: "long", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	sub, token := startOne(t, s, "agt_a")
	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, sub.ID, token, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeManual, Deadline: &ttl}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// manual 不被 sweeper 唤醒
	clk.Advance(30 * time.Minute)
	s.SweepOnce(ctx)
	if cur := mustGet(t, s, m.ID, sub.ID); cur.State != mission.StateWaiting {
		t.Fatalf("manual wake_on must not fire on timer, state = %s", cur.State)
	}
	// 人工唤醒
	woken, err := s.Wake(ctx, sub.ID, "lead")
	if err != nil {
		t.Fatalf("Wake: %v", err)
	}
	if woken.State != mission.StateReady {
		t.Fatalf("state = %s, want READY", woken.State)
	}
	// 重复唤醒 404（已不在 WAITING）
	if _, err := s.Wake(ctx, sub.ID, "lead"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("double wake = %v, want NotFound", err)
	}
}

// ---- T4：wake TTL 过期 → FAILED + 级联取消 ----

func TestWakeTimeoutFailsMission(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "唤醒超时", []TaskSpec{
		{Name: "long", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "next", Kind: mission.KindAgent, DependsOn: []string{"long"}},
	})
	sub, token := startOne(t, s, "agt_a")
	wakeAt := clk.Now().Add(10 * time.Minute)
	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, sub.ID, token, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeTimer, At: &wakeAt, Deadline: &ttl}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// timer 到期但 sweeper 未跑；直接越过 TTL → FAILED(wake_timeout)
	clk.Advance(61 * time.Minute)
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m.ID, sub.ID); cur.State != mission.StateFailed {
		t.Fatalf("state = %s, want FAILED (wake_timeout)", cur.State)
	}
	if next := mustGet(t, s, m.ID, subID(m.ID, "next")); next.State != mission.StateCancelled {
		t.Fatalf("downstream = %s, want CANCELLED（级联）", next.State)
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionFailed {
		t.Fatalf("mission = %s, want FAILED", final.Status)
	}
}

// ---- T3/T4：校验与检查点心跳 ----

func TestSuspendValidation(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	s.CreateMission(ctx, "u1", "校验", []TaskSpec{
		{Name: "long", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	sub, token := startOne(t, s, "agt_a")
	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, sub.ID, token, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeTimer, Deadline: &ttl}, nil); err == nil {
		t.Fatal("timer without at must error")
	}
	if _, err := s.Suspend(ctx, sub.ID, token, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeManual}, nil); err == nil {
		t.Fatal("missing TTL must error")
	}
	if _, err := s.Suspend(ctx, sub.ID, token, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeEvent, Deadline: &ttl}, nil); err == nil {
		t.Fatal("event wake without types must error")
	}
	if _, err := s.Suspend(ctx, sub.ID, token+1, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeManual, Deadline: &ttl}, nil); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("bad token must fence, got %v", err)
	}
}

func TestProgressCheckpoint(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "检查点心跳", []TaskSpec{
		{Name: "long", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	sub, token := startOne(t, s, "agt_a")
	if err := s.Progress(ctx, sub.ID, sub.LeaseID, token, json.RawMessage(`{"step": 3}`)); err != nil {
		t.Fatalf("Progress: %v", err)
	}
	if cur := mustGet(t, s, m.ID, sub.ID); string(cur.Checkpoint) != `{"step": 3}` {
		t.Fatalf("checkpoint = %s", cur.Checkpoint)
	}
	if err := s.Progress(ctx, sub.ID, sub.LeaseID, token+1, json.RawMessage(`{"step": 4}`)); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("bad token must fence, got %v", err)
	}
}
