package core

import (
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// ---- G3：TaskIntent 准入管道 ----

func TestIntentCreateMissionIdempotent(t *testing.T) {
	s, _, _ := newService()
	in := Intent{
		Source:         store.Actor{Kind: "agent", ID: "agt_scout"},
		Action:         IntentCreateMission,
		IdempotencyKey: "scout-42",
		Owner:          "u1", Goal: "深挖新线索",
		Tasks: []TaskSpec{{Name: "dig", Kind: mission.KindAgent}},
	}
	r1, err := s.SubmitIntent(ctx, in)
	if err != nil {
		t.Fatalf("SubmitIntent: %v", err)
	}
	if r1.MissionID == "" || r1.Deduplicated {
		t.Fatalf("first submit: %+v", r1)
	}
	// 重发同键 → 返回同一 Mission，不新建
	r2, err := s.SubmitIntent(ctx, in)
	if err != nil {
		t.Fatalf("resubmit: %v", err)
	}
	if r2.MissionID != r1.MissionID || !r2.Deduplicated {
		t.Fatalf("dedup: %+v vs %+v", r1, r2)
	}
	// 触发者留痕：创建事件 actor = intent source
	evs, _ := s.ListMissionEvents(ctx, r1.MissionID, 0, 100)
	found := false
	for _, e := range evs {
		if e.Type == "mission.created" && e.Actor.Kind == "agent" && e.Actor.ID == "agt_scout" {
			found = true
		}
	}
	if !found {
		t.Fatal("mission.created event should carry intent source as actor")
	}
	// 校验失败不占用幂等键
	bad := in
	bad.IdempotencyKey = "scout-bad"
	bad.Tasks = []TaskSpec{{Name: "x", DependsOn: []string{"ghost"}}}
	if _, err := s.SubmitIntent(ctx, bad); err == nil {
		t.Fatal("invalid DAG must error")
	}
	bad.Tasks = []TaskSpec{{Name: "ok", Kind: mission.KindAgent}}
	if _, err := s.SubmitIntent(ctx, bad); err != nil {
		t.Fatalf("key must not be burned by validation failure: %v", err)
	}
}

func TestIntentWake(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "intent 唤醒", []TaskSpec{
		{Name: "long", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	sub, token := startOne(t, s, "agt_a")
	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, sub.ID, token, sub.Version, "agt_a",
		&mission.WakeSpec{Kind: mission.WakeManual, Deadline: &ttl}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// Agent 经准入管道唤醒（§7.1 T5：智能体主动触发）
	res, err := s.SubmitIntent(ctx, Intent{
		Source: store.Actor{Kind: "agent", ID: "agt_a"}, Action: IntentWake, SubtaskID: sub.ID,
	})
	if err != nil {
		t.Fatalf("SubmitIntent wake: %v", err)
	}
	if res.SubtaskID != sub.ID {
		t.Fatalf("res = %+v", res)
	}
	if cur := mustGet(t, s, m.ID, sub.ID); cur.State != mission.StateReady {
		t.Fatalf("state = %s, want READY", cur.State)
	}
}
