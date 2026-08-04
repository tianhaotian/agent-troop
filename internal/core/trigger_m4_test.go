package core

import (
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// ---- G1：event 唤醒 ----

func TestEventWake(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	mustRegister(t, s, "agt_b", 2, "code.review")
	// A（research）与 B（review）并行；B 挂起等 A 的完成事件
	m, _ := s.CreateMission(ctx, "u1", "事件唤醒", []TaskSpec{
		{Name: "produce", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	_, tokenA := startOne(t, s, "agt_a")
	subB, tokenB := startOne(t, s, "agt_b")

	// B 挂起等"产物完成"事件（注册时水位线 = 当前最大 seq，此前事件不命中）
	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, subB.ID, tokenB, subB.Version, "agt_b",
		&mission.WakeSpec{Kind: mission.WakeEvent, Deadline: &ttl,
			Event: &mission.EventMatch{Types: []string{"subtask.succeeded"}}}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("old events must not fire, state = %s", cur.State)
	}

	// A 完成 → 新事件越过水位线 → sweeper 唤醒 B
	curA := mustGet(t, s, m.ID, subID(m.ID, "produce"))
	if _, err := s.CompleteSubtask(ctx, curA.ID, tokenA, "idem-a", "artifact://draft", curA.Version, "agt_a"); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateReady {
		t.Fatalf("event should wake B, state = %s", cur.State)
	}
}

func TestEventWakeWhereFilter(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "where 过滤", []TaskSpec{
		{Name: "produce", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	_, tokenA := startOne(t, s, "agt_a")
	subB, tokenB := startOne(t, s, "agt_b")

	ttl := clk.Now().Add(time.Hour)
	// 只关心 result_ref 为终稿的完成事件
	if _, err := s.Suspend(ctx, subB.ID, tokenB, subB.Version, "agt_b",
		&mission.WakeSpec{Kind: mission.WakeEvent, Deadline: &ttl,
			Event: &mission.EventMatch{Types: []string{"subtask.succeeded"},
				Where: map[string]any{"result_ref": "artifact://final"}}}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// A 以草稿完成 → where 不命中 → 不醒
	curA := mustGet(t, s, m.ID, subID(m.ID, "produce"))
	if _, err := s.CompleteSubtask(ctx, curA.ID, tokenA, "idem-a", "artifact://draft", curA.Version, "agt_a"); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	s.SweepOnce(ctx)
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("where mismatch must not wake, state = %s", cur.State)
	}
}

func TestMatchWhere(t *testing.T) {
	payload := map[string]any{"issue_id": "123", "meta": map[string]any{"prio": float64(2)}}
	if !matchWhere(payload, map[string]any{"issue_id": "123"}) {
		t.Fatal("exact match should hit")
	}
	if !matchWhere(payload, map[string]any{"meta.prio": float64(2)}) {
		t.Fatal("dot path should hit")
	}
	if matchWhere(payload, map[string]any{"meta.prio": float64(3)}) {
		t.Fatal("value mismatch must miss")
	}
	if matchWhere(payload, map[string]any{"meta.missing": 1}) {
		t.Fatal("missing key must miss")
	}
}

// ---- G2：condition 唤醒 ----

func TestConditionWakeBoardPut(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "条件唤醒", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")

	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, subB.ID, tokenB, subB.Version, "agt_b",
		&mission.WakeSpec{Kind: mission.WakeCondition, Deadline: &ttl,
			Condition: &mission.BoardCondition{Board: "shared/status", Op: mission.CondEquals, Value: "ready"}}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// 写其他键 / 不等的值 → 不醒（增量钩子已运行）
	if _, err := s.BoardPut(ctx, m.ID, "shared", "other", []byte(`"x"`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if _, err := s.BoardPut(ctx, m.ID, "shared", "status", []byte(`"draft"`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("non-matching writes must not wake, state = %s", cur.State)
	}
	// 命中值 → BoardPut 增量钩子直接唤醒（无需 sweep）
	if _, err := s.BoardPut(ctx, m.ID, "shared", "status", []byte(`"ready"`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateReady {
		t.Fatalf("matching board write should wake, state = %s", cur.State)
	}
}

func TestConditionWakeSweepFallback(t *testing.T) {
	s, st, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "sweep 兜底", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")

	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, subB.ID, tokenB, subB.Version, "agt_b",
		&mission.WakeSpec{Kind: mission.WakeCondition, Deadline: &ttl,
			Condition: &mission.BoardCondition{Board: "shared/go", Op: mission.CondExists}}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// 绕过 service 钩子直写 store（模拟增量漏更新）→ sweeper 全量兜底唤醒
	entry := &store.BoardEntry{MissionID: m.ID, Namespace: "shared", Key: "go", Value: []byte(`"1"`)}
	if _, err := st.BoardPut(ctx, entry, -1, clk.Now()); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateReady {
		t.Fatalf("anti-entropy sweep should wake, state = %s", cur.State)
	}
}
