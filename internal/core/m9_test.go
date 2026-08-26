package core

import (
	"errors"
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

func TestM9QualityReputationAndMetering(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_producer", 1, "write")
	mustRegister(t, s, "agt_verifier", 1, "verify")
	m, err := s.CreateMission(ctx, "owner", "quality", []TaskSpec{{
		Name: "draft", Kind: mission.KindAgent, RequiredSkills: []string{"write"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatal(err)
	}
	offers, _ := s.ListOffers(ctx, "agt_producer")
	if len(offers) != 1 {
		t.Fatalf("offers=%d", len(offers))
	}
	offer := offers[0]
	token := fenceOf(t, s, offer.LeaseID)
	leased, err := s.AcceptLease(ctx, offer.LeaseID, token, offer.Version, "agt_producer")
	if err != nil {
		t.Fatal(err)
	}
	running, err := s.StartSubtask(ctx, leased.ID, token, leased.Version, "agt_producer")
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := s.PutArtifact(ctx, m.ID, running.ID, "schema://report/v1", []byte(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(1500 * time.Millisecond)
	if _, err := s.CompleteSubtaskWithUsage(ctx, running.ID, token, "m9-complete", "artifact://"+artifact.ID,
		120, running.Version, "agt_producer"); err != nil {
		t.Fatal(err)
	}

	input := VerifyArtifactInput{Score: 0.9, Confidence: 0.8, Verdict: store.QualityAccepted,
		Rubric: "rubric://report/v1", ContextHash: "ctx_hash"}
	if _, err := s.VerifyArtifact(ctx, artifact.ID, input,
		store.Actor{Kind: "agent", ID: "agt_producer"}); !errors.Is(err, ErrForbidden) {
		t.Fatalf("self verification=%v", err)
	}
	q, err := s.VerifyArtifact(ctx, artifact.ID, input,
		store.Actor{Kind: "agent", ID: "agt_verifier"})
	if err != nil {
		t.Fatal(err)
	}
	if !q.Layers["L0"].Pass || q.Score != 0.9 || q.ProducerAgentID != "agt_producer" {
		t.Fatalf("quality=%+v", q)
	}
	if _, err := s.VerifyArtifact(ctx, artifact.ID, input,
		store.Actor{Kind: "service", ID: "judge"}); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("duplicate verification=%v", err)
	}

	reps, err := s.GetReputations(ctx, "agt_producer")
	if err != nil || len(reps) != 1 {
		t.Fatalf("reputations=%+v err=%v", reps, err)
	}
	if reps[0].Skill != "write" || reps[0].Samples != 1.25 || reps[0].CompositeScore <= 0.5 {
		t.Fatalf("reputation=%+v", reps[0])
	}
	report, err := s.GetUsageReport(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if report.ByResource["artifact.byte"] != float64(len(`{"ok":true}`)) ||
		report.ByResource["token.reported"] != 120 ||
		report.ByResource["lease.wall_ms"] != 1500 || report.ByResource["verify.call"] != 1 {
		t.Fatalf("usage=%+v", report)
	}
}

func TestM9QualityValidation(t *testing.T) {
	s, _, _ := newService()
	m, err := s.CreateMission(ctx, "owner", "invalid quality", []TaskSpec{{Name: "a", Kind: mission.KindAgent}})
	if err != nil {
		t.Fatal(err)
	}
	a, err := s.PutArtifact(ctx, m.ID, "", "", []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.VerifyArtifact(ctx, a.ID, VerifyArtifactInput{Score: 0.5, Confidence: 1,
		Verdict: store.QualityRejected}, store.Actor{Kind: "service", ID: "judge"})
	if !errors.Is(err, ErrInvalidQuality) {
		t.Fatalf("missing failure class=%v", err)
	}
}

func TestM9ReputationAffectsPlacement(t *testing.T) {
	s, st, _ := newService()
	mustRegister(t, s, "agt_a", 1, "work")
	mustRegister(t, s, "agt_b", 1, "work")
	bad, good := false, true
	for i := 0; i < 8; i++ {
		if err := st.ApplyReputationSignal(ctx, store.ReputationSignal{ID: "bad:" + string(rune('a'+i)),
			AgentID: "agt_a", Skill: "work", Success: &bad, Reliable: &bad, Weight: 1}, base); err != nil {
			t.Fatal(err)
		}
		if err := st.ApplyReputationSignal(ctx, store.ReputationSignal{ID: "good:" + string(rune('a'+i)),
			AgentID: "agt_b", Skill: "work", Success: &good, Reliable: &good, Weight: 1}, base); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.CreateMission(ctx, "owner", "placement", []TaskSpec{{
		Name: "a", Kind: mission.KindAgent, RequiredSkills: []string{"work"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatal(err)
	}
	offers, _ := s.ListOffers(ctx, "agt_b")
	if len(offers) != 1 {
		t.Fatalf("reputable agent offers=%d", len(offers))
	}
}
