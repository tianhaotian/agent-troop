package core

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

func TestLeadInboxRequiresExplicitOwnedIngest(t *testing.T) {
	s, _, _ := newService()
	m, parent, leadToken := setupLead(t, s)
	mustRegister(t, s, "agt_worker", 1, "work")
	res, err := s.SubmitIntent(ctx, delegateIntent(parent, leadToken, "inbox-child", &DelegateSpec{
		Name: "child", RequiredSkills: []string{"work"},
	}))
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule child: %v", err)
	}
	offers, _ := s.ListOffers(ctx, "agt_worker")
	if len(offers) != 1 {
		t.Fatalf("worker offers=%d", len(offers))
	}
	childToken := fenceOf(t, s, offers[0].LeaseID)
	child, err := s.AcceptLease(ctx, offers[0].LeaseID, childToken, offers[0].Version, "agt_worker")
	if err != nil {
		t.Fatalf("accept child: %v", err)
	}
	child, err = s.StartSubtask(ctx, child.ID, childToken, child.Version, "agt_worker")
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	if _, err := s.CompleteSubtask(ctx, child.ID, childToken, "child-result", "artifact://result",
		child.Version, "agt_worker"); err != nil {
		t.Fatalf("complete child: %v", err)
	}

	items, err := s.ListLeadInbox(ctx, parent.ID, true)
	if err != nil || len(items) != 1 {
		t.Fatalf("pending inbox=%+v err=%v", items, err)
	}
	item := items[0]
	if item.SourceSubtaskID != res.SubtaskID || item.ResultRef != "artifact://result" ||
		item.Status != store.LeadInboxPending {
		t.Fatalf("inbox item=%+v", item)
	}
	// completion 幂等重放不得重复入箱。
	if _, err := s.CompleteSubtask(ctx, child.ID, childToken, "child-result", "artifact://result",
		child.Version, "agt_worker"); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("completion replay=%v", err)
	}
	if all, _ := s.ListLeadInbox(ctx, parent.ID, false); len(all) != 1 {
		t.Fatalf("inbox duplicated: %d", len(all))
	}
	if _, err := s.IngestLeadInbox(ctx, parent.ID, item.ID, "agt_intruder", leadToken,
		item.Version, store.LeadIngestSummary); !errors.Is(err, ErrForbidden) {
		t.Fatalf("foreign ingest must be forbidden: %v", err)
	}
	ingested, err := s.IngestLeadInbox(ctx, parent.ID, item.ID, "agt_lead", leadToken,
		item.Version, store.LeadIngestSummary)
	if err != nil || ingested.Status != store.LeadInboxIngested || ingested.IngestedBy != "agt_lead" {
		t.Fatalf("ingest=%+v err=%v", ingested, err)
	}
	if pending, _ := s.ListLeadInbox(ctx, parent.ID, true); len(pending) != 0 {
		t.Fatalf("pending after ingest=%d", len(pending))
	}
	if got, _ := s.GetMission(ctx, m.ID); got.Status != mission.MissionActive {
		t.Fatalf("lead still running, mission=%s", got.Status)
	}
}

func TestLeadSnapshotAndTakeoverRecovery(t *testing.T) {
	s, _, clk := newService()
	m, parent, leadToken := setupLead(t, s)
	mustRegister(t, s, "agt_worker", 1, "work")
	mustRegister(t, s, "agt_backup", 1, "lead.coordinate")

	// 保留一个 in-flight child，验证 takeover 不会级联 fence child。
	res, err := s.SubmitIntent(ctx, delegateIntent(parent, leadToken, "takeover-child", &DelegateSpec{
		Name: "child", RequiredSkills: []string{"work"},
	}))
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule child: %v", err)
	}
	offers, _ := s.ListOffers(ctx, "agt_worker")
	childToken := fenceOf(t, s, offers[0].LeaseID)
	child, _ := s.AcceptLease(ctx, offers[0].LeaseID, childToken, offers[0].Version, "agt_worker")
	child, _ = s.StartSubtask(ctx, child.ID, childToken, child.Version, "agt_worker")

	snapshot := json.RawMessage(`{"intent":"merge child result","next":"review"}`)
	entry, err := s.SaveLeadSnapshot(ctx, parent.ID, "agt_lead", leadToken, -1, snapshot)
	if err != nil || entry.Version != 0 {
		t.Fatalf("create snapshot=%+v err=%v", entry, err)
	}
	leaseAfterCreate, _ := s.GetLease(ctx, parent.LeaseID)
	if !leaseAfterCreate.ExpiresAt.Equal(clk.Now().Add(s.cfg.LeadHeartbeatTTL)) {
		t.Fatalf("lead expiry=%s", leaseAfterCreate.ExpiresAt)
	}
	if _, err := s.SaveLeadSnapshot(ctx, parent.ID, "agt_lead", leadToken, -1, snapshot); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("duplicate initial snapshot must conflict: %v", err)
	}
	clk.Advance(10 * time.Second)
	entry, err = s.SaveLeadSnapshot(ctx, parent.ID, "agt_lead", leadToken, entry.Version,
		json.RawMessage(`{"intent":"merge child result","next":"ingest"}`))
	if err != nil || entry.Version != 1 {
		t.Fatalf("update snapshot=%+v err=%v", entry, err)
	}
	if leadAgent, _ := s.GetAgent(ctx, "agt_lead"); !leadAgent.LastHeartbeat.Equal(clk.Now()) {
		t.Fatalf("lead heartbeat not updated: %+v", leadAgent)
	}

	// 越过最新 heartbeat TTL：旧 Lead 被 fence，协调任务回 READY。
	clk.Advance(s.cfg.LeadHeartbeatTTL + time.Second)
	if _, err := s.SaveLeadSnapshot(ctx, parent.ID, "agt_lead", leadToken, entry.Version,
		json.RawMessage(`{"intent":"too late"}`)); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("late heartbeat must be fenced: %v", err)
	}
	lateDelegate := delegateIntent(parent, leadToken, "late-delegate", &DelegateSpec{Name: "late"})
	if _, err := s.SubmitIntent(ctx, lateDelegate); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("late delegate must be fenced: %v", err)
	}
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	ready := mustGet(t, s, m.ID, parent.ID)
	if ready.State != mission.StateReady || ready.LeaseID != "" || ready.Assignee != "" {
		t.Fatalf("lead after takeover=%+v", ready)
	}
	oldLease, _ := s.GetLease(ctx, parent.LeaseID)
	if oldLease.State != store.LeaseFenced {
		t.Fatalf("old lease state=%s", oldLease.State)
	}
	oldAgent, _ := s.GetAgent(ctx, "agt_lead")
	if oldAgent.Health != "suspect" || oldAgent.Running != 0 {
		t.Fatalf("old agent=%+v", oldAgent)
	}
	if err := s.Progress(ctx, parent.ID, parent.LeaseID, leadToken, "agt_lead", nil); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("old lead write must be fenced: %v", err)
	}
	if curChild := mustGet(t, s, m.ID, res.SubtaskID); curChild.State != mission.StateRunning {
		t.Fatalf("in-flight child changed during takeover: %s", curChild.State)
	}
	// 已在运行的 child 仍可完成，结果进入等待继任者的 inbox。
	if _, err := s.CompleteSubtask(ctx, child.ID, childToken, "takeover-result", "artifact://late",
		child.Version, "agt_worker"); err != nil {
		t.Fatalf("in-flight child complete: %v", err)
	}

	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("schedule replacement: %v", err)
	}
	backupOffers, _ := s.ListOffers(ctx, "agt_backup")
	if len(backupOffers) != 1 || backupOffers[0].ID != parent.ID {
		t.Fatalf("backup offers=%+v", backupOffers)
	}
	backupToken := fenceOf(t, s, backupOffers[0].LeaseID)
	replacement, _ := s.AcceptLease(ctx, backupOffers[0].LeaseID, backupToken,
		backupOffers[0].Version, "agt_backup")
	if _, err := s.StartSubtask(ctx, replacement.ID, backupToken, replacement.Version, "agt_backup"); err != nil {
		t.Fatalf("start replacement: %v", err)
	}
	recovery, err := s.GetLeadContext(ctx, parent.ID)
	if err != nil || recovery.Snapshot == nil || recovery.Snapshot.Version != 1 || len(recovery.Inbox) != 1 {
		t.Fatalf("recovery=%+v err=%v", recovery, err)
	}
}
