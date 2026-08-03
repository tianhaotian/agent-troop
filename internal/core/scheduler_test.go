package core

import (
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// ---- T1：策略插件机制 ----

func TestNewStrategy(t *testing.T) {
	if ps, err := NewStrategy(""); err != nil || ps.Name() != "capability-first" {
		t.Fatalf("default strategy = %v %v", ps, err)
	}
	if _, err := NewStrategy("nope"); err == nil {
		t.Fatal("unknown strategy must error")
	}
}

func TestRoundRobinDistribution(t *testing.T) {
	s, _, _ := newService()
	s.WithStrategy(NewRoundRobin())
	mustRegister(t, s, "agt_a", 5, "web.research")
	mustRegister(t, s, "agt_b", 5, "web.research")

	if _, err := s.CreateMission(ctx, "u1", "轮转", []TaskSpec{
		{Name: "t1", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "t2", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	}); err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	if placed, _ := s.ScheduleOnce(ctx); placed != 2 {
		t.Fatalf("placed = %d, want 2", placed)
	}
	oa, _ := s.ListOffers(ctx, "agt_a")
	ob, _ := s.ListOffers(ctx, "agt_b")
	if len(oa) != 1 || len(ob) != 1 {
		t.Fatalf("round-robin should spread 2 tasks over 2 agents, got %d/%d", len(oa), len(ob))
	}
}

// ---- T2：deadline 紧迫度影响放置 ----

func TestDeadlineUrgencyPrefersIdleAgent(t *testing.T) {
	s, _, _ := newService()
	// busy 技能高但在途 1；idle 技能略低但空闲
	s.st.UpsertAgent(ctx, &store.Agent{
		ID: "agt_busy", Name: "agt_busy", Platform: "http-echo", MaxConcurrency: 5, Health: "healthy",
		Capabilities: []store.Capability{{Skill: "web.research", Level: 0.95}}, Running: 1,
	}, base)
	s.st.UpsertAgent(ctx, &store.Agent{
		ID: "agt_idle", Name: "agt_idle", Platform: "http-echo", MaxConcurrency: 5, Health: "healthy",
		Capabilities: []store.Capability{{Skill: "web.research", Level: 0.8}},
	}, base)

	urgent := base.Add(30 * time.Minute) // <1h → 紧迫，负载惩罚 ×2
	m, err := s.CreateMission(ctx, "u1", "紧迫度", []TaskSpec{
		{Name: "calm", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "urgent", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"},
			Deadline: &urgent},
	})
	if err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	if placed, _ := s.ScheduleOnce(ctx); placed != 2 {
		t.Fatalf("placed = %d, want 2", placed)
	}
	// 紧迫任务：busy 0.95−0.2×1=0.75 < idle 0.8 → idle
	// 普通任务：busy 0.95−0.1×1=0.85 > idle 0.8−0.1=0.7 → busy
	offers, _ := s.ListOffers(ctx, "agt_idle")
	if len(offers) != 1 || offers[0].ID != subID(m.ID, "urgent") {
		t.Fatalf("urgent task should go to idle agent, got %+v", offers)
	}
	if offers, _ := s.ListOffers(ctx, "agt_busy"); len(offers) != 1 {
		t.Fatalf("calm task should go to busy agent, got %+v", offers)
	}
}
