package core

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

func artifactIDFor(content []byte) string {
	sum := sha256.Sum256(content)
	return "art_" + hex.EncodeToString(sum[:])[:20]
}

func TestContextPackageAndPermissionAttenuation(t *testing.T) {
	s, _, clk := newService()
	other, err := s.CreateMission(ctx, "u2", "isolated", []TaskSpec{{
		Name: "x", Kind: mission.KindAgent, RequiredSkills: []string{"never.schedule"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	cross, err := s.PutArtifact(ctx, other.ID, subID(other.ID, "x"), "text/plain", []byte("cross mission"))
	if err != nil {
		t.Fatal(err)
	}

	allowedContent := []byte("allowed artifact")
	allowedID := artifactIDFor(allowedContent)
	rootGrants := mission.PermissionEnvelope{
		Classification: mission.ClassificationInternal,
		ToolScopes:     []string{"search", "publish"},
		ArtifactRefs:   []string{allowedID, cross.ID},
		BoardViews: []mission.BoardGrant{{
			Namespace: "shared", Keys: []string{"glossary", "style"}, Mode: mission.BoardModeReadWrite,
		}},
	}
	mustRegisterScopes(t, s, "agt_ctx_lead", []string{ScopeSpawnSubtask}, 1, "lead.coordinate")
	mustRegister(t, s, "agt_ctx_worker", 1, "work")
	m, err := s.CreateMissionWithBudget(ctx, "u1", "least knowledge", 100, []TaskSpec{{
		Name: "lead", Kind: mission.KindAgent, RequiredSkills: []string{"lead.coordinate"}, Grants: rootGrants,
	}})
	if err != nil {
		t.Fatal(err)
	}
	leadID := subID(m.ID, "lead")
	if _, err := s.PutArtifact(ctx, m.ID, leadID, "text/plain", allowedContent); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BoardPut(ctx, m.ID, "shared", "glossary", []byte(`{"term":"storage"}`), -1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BoardPut(ctx, m.ID, "shared", "secret", []byte(`{"token":"hidden"}`), -1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatal(err)
	}
	offers, _ := s.ListOffers(ctx, "agt_ctx_lead")
	if len(offers) != 1 {
		t.Fatalf("lead offers=%+v", offers)
	}
	leadOffer := offers[0]
	leadLease, _ := s.GetLease(ctx, leadOffer.LeaseID)
	leadPkg, err := s.GetContextPackage(ctx, leadLease.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(leadPkg.Artifacts) != 1 || leadPkg.Artifacts[0].ID != allowedID ||
		len(leadPkg.BoardViews) != 1 || leadPkg.BoardViews[0].Key != "glossary" ||
		leadPkg.Budget.Available != 100 || leadPkg.SnapshotHash == "" {
		t.Fatalf("lead context leaked or incomplete: %+v", leadPkg)
	}
	accepted, err := s.AcceptLease(ctx, leadLease.ID, leadLease.FencingToken, leadOffer.Version, "agt_ctx_lead")
	if err != nil {
		t.Fatal(err)
	}
	parent, err := s.StartSubtask(ctx, leadID, leadLease.FencingToken, accepted.Version, "agt_ctx_lead")
	if err != nil {
		t.Fatal(err)
	}
	intent := Intent{Source: store.Actor{Kind: "agent", ID: "agt_ctx_lead"}, Action: IntentDelegate,
		IdempotencyKey: "ctx-child", ParentSubtaskID: parent.ID, ParentVersion: parent.Version,
		FencingToken: leadLease.FencingToken, Task: &DelegateSpec{Name: "child", BudgetTokens: 20}}
	intent.Task.Grants = mission.PermissionEnvelope{Classification: mission.ClassificationRestricted}
	if _, err := s.SubmitIntent(ctx, intent); !errors.Is(err, ErrInvalidPermission) {
		t.Fatalf("classification escalation=%v", err)
	}
	intent.Task.RequiredSkills = []string{"work"}
	intent.Task.Grants = mission.PermissionEnvelope{
		Classification: mission.ClassificationPublic,
		ToolScopes:     []string{"search"},
		ArtifactRefs:   []string{allowedID, cross.ID},
		BoardViews: []mission.BoardGrant{{
			Namespace: "shared", Keys: []string{"glossary"}, Mode: mission.BoardModeReadOnly,
		}},
	}
	result, err := s.SubmitIntent(ctx, intent) // 权限拒绝未消耗同一幂等键或预算
	if err != nil {
		t.Fatalf("attenuated delegate: %v", err)
	}
	account, _, _ := s.GetMissionBudget(ctx, m.ID)
	if account.Held != 20 {
		t.Fatalf("unexpected hold after permission retry: %+v", account)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatal(err)
	}
	childOffers, _ := s.ListOffers(ctx, "agt_ctx_worker")
	if len(childOffers) != 1 || childOffers[0].ID != result.SubtaskID {
		t.Fatalf("child offers=%+v", childOffers)
	}
	firstLeaseID := childOffers[0].LeaseID
	firstPkg, err := s.GetContextPackage(ctx, firstLeaseID)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPkg.Artifacts) != 1 || firstPkg.Artifacts[0].ID != allowedID ||
		len(firstPkg.BoardViews) != 1 || firstPkg.BoardViews[0].Key != "glossary" ||
		firstPkg.BoardViews[0].Mode != mission.BoardModeReadOnly || firstPkg.Budget.Available != 80 {
		t.Fatalf("child context leaked or incomplete: %+v", firstPkg)
	}

	// offer 到期后重派会产生新的不可变 package；可见内容未变，因此 hash 稳定。
	clk.Advance(2 * s.cfg.OfferTTL)
	if _, err := s.st.ExpireLeases(ctx, clk.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatal(err)
	}
	childOffers, _ = s.ListOffers(ctx, "agt_ctx_worker")
	if len(childOffers) != 1 || childOffers[0].LeaseID == firstLeaseID {
		t.Fatalf("replacement offer=%+v", childOffers)
	}
	secondPkg, err := s.GetContextPackage(ctx, childOffers[0].LeaseID)
	if err != nil {
		t.Fatal(err)
	}
	if secondPkg.ID == firstPkg.ID || secondPkg.SnapshotHash != firstPkg.SnapshotHash {
		t.Fatalf("re-materialization first=%+v second=%+v", firstPkg, secondPkg)
	}
	events, _ := s.ListMissionEvents(ctx, m.ID, 0, 100)
	count := 0
	for _, event := range events {
		if event.Type == "context.materialized" && event.Payload["snapshot_hash"] != "" {
			count++
		}
	}
	if count < 3 { // Lead + child first offer + child replacement
		t.Fatalf("context audit events=%d", count)
	}
}

func TestCreateMissionRejectsInvalidPermissionEnvelope(t *testing.T) {
	s, _, _ := newService()
	if _, err := s.CreateMission(ctx, "u1", "bad grants", []TaskSpec{{
		Name: "a", Kind: mission.KindAgent,
		Grants: mission.PermissionEnvelope{BoardViews: []mission.BoardGrant{{Namespace: "x", Mode: "admin"}}},
	}}); !errors.Is(err, ErrInvalidPermission) {
		t.Fatalf("invalid grants=%v", err)
	}
}
