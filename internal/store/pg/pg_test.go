package pg

// PG 实现的一致性测试：与 memory 实现跑同一关键路径（迁移 CAS、租约 fencing、
// 幂等去重、到期回收）。需要本地 PG：
//
//	docker compose up -d postgres
//	TROOP_TEST_PG=postgres://troop:troop@localhost:5432/troop go test ./internal/store/pg/
//
// 未设置 TROOP_TEST_PG 时跳过（CI 中由 compose 提供服务后启用）。
import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TROOP_TEST_PG")
	if dsn == "" {
		t.Skip("TROOP_TEST_PG not set; skipping PG conformance test")
	}
	ctx := context.Background()
	st, err := Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	applyMigrations(t, ctx, st)
	// 清场（测试库专用！）
	if _, err := st.pool.Exec(ctx,
		`TRUNCATE missions, subtasks, agents, leases, artifacts, decisions, idempotency_keys,
		 events, board_entries, lead_inbox, budget_holds, mission_budgets, context_packages`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func applyMigrations(t *testing.T, ctx context.Context, st *Store) {
	t.Helper()
	// 应用全部迁移（按文件名序）。迁移文件必须支持在已升级数据库重复执行。
	migs, err := filepath.Glob("../../../migrations/*.sql")
	if err != nil || len(migs) == 0 {
		t.Fatalf("glob migrations: %v", err)
	}
	sort.Strings(migs)
	for _, f := range migs {
		mig, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read migration %s: %v", f, err)
		}
		if _, err := st.pool.Exec(ctx, string(mig)); err != nil {
			t.Fatalf("migrate %s: %v", f, err)
		}
	}
}

func TestPGMigrationsIdempotent(t *testing.T) {
	st := testStore(t) // first application
	applyMigrations(t, context.Background(), st)
}

func TestPGBudgetHoldLifecycle(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	sys := store.Actor{Kind: "system", ID: "test"}
	leadActor := store.Actor{Kind: "agent", ID: "agt_budget_lead"}
	workerActor := store.Actor{Kind: "agent", ID: "agt_budget_worker"}
	m := &mission.Mission{
		ID: "msn_budget", Owner: "u1", Goal: "budget", BudgetTokens: 100, Status: mission.MissionActive,
	}
	parent := &mission.Subtask{ID: "sub_budget_lead", MissionID: m.ID, Kind: mission.KindAgent,
		State: mission.StateReady, Grants: mission.PermissionEnvelope{
			Classification: mission.ClassificationInternal,
			ArtifactRefs:   []string{"art_budget_context"},
			BoardViews: []mission.BoardGrant{{Namespace: "shared", Keys: []string{"allowed"},
				Mode: mission.BoardModeReadOnly}},
		}}
	if err := st.CreateMission(ctx, m, []*mission.Subtask{parent}, sys, now); err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	artifact := &store.Artifact{ID: "art_budget_context", SHA256: "abc123", MissionID: m.ID,
		ProducedBy: parent.ID, SchemaRef: "text/plain", Size: 3}
	if err := st.PutArtifact(ctx, artifact, now); err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}
	if _, err := st.BoardPut(ctx, &store.BoardEntry{MissionID: m.ID, Namespace: "shared",
		Key: "allowed", Value: []byte(`{"ok":true}`)}, -1, now); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	for _, a := range []*store.Agent{
		{ID: leadActor.ID, Name: "lead", Platform: "custom", MaxConcurrency: 1, Health: "healthy"},
		{ID: workerActor.ID, Name: "worker", Platform: "custom", MaxConcurrency: 1, Health: "healthy"},
	} {
		if err := st.UpsertAgent(ctx, a, now); err != nil {
			t.Fatalf("UpsertAgent: %v", err)
		}
	}
	leadLease, err := st.OfferLease(ctx, parent.ID, leadActor.ID, 0, time.Minute, sys, now)
	if err != nil {
		t.Fatalf("OfferLease lead: %v", err)
	}
	accepted, err := st.AcceptLease(ctx, leadLease.ID, leadLease.FencingToken, 1, leadActor, now)
	if err != nil {
		t.Fatalf("AcceptLease lead: %v", err)
	}
	runningLead, err := st.StartSubtask(ctx, parent.ID, leadLease.FencingToken, accepted.Version, leadActor, now)
	if err != nil {
		t.Fatalf("StartSubtask lead: %v", err)
	}
	child := &mission.Subtask{
		ID: "sub_budget_child", MissionID: m.ID, ParentID: parent.ID, Kind: mission.KindAgent,
		Scheduling: mission.SchedulingSpec{BudgetTokens: 60},
		Grants: mission.PermissionEnvelope{Classification: mission.ClassificationInternal,
			ArtifactRefs: []string{artifact.ID}, BoardViews: []mission.BoardGrant{{
				Namespace: "shared", Keys: []string{"allowed"}, Mode: mission.BoardModeReadOnly,
			}}},
	}
	if _, err := st.SpawnSubtask(ctx, "budget-child", parent.ID, leadLease.FencingToken,
		runningLead.Version, child, leadActor, now); err != nil {
		t.Fatalf("SpawnSubtask: %v", err)
	}
	tooLarge := &mission.Subtask{
		ID: "sub_budget_too_large", MissionID: m.ID, ParentID: parent.ID, Kind: mission.KindAgent,
		Scheduling: mission.SchedulingSpec{BudgetTokens: 50},
	}
	if _, err := st.SpawnSubtask(ctx, "budget-too-large", parent.ID, leadLease.FencingToken,
		runningLead.Version, tooLarge, leadActor, now); !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("concurrent capacity check: %v", err)
	}
	account, err := st.GetMissionBudget(ctx, m.ID)
	if err != nil || account.Held != 60 || account.Available != 40 {
		t.Fatalf("held account=%+v err=%v", account, err)
	}

	childLease, err := st.OfferLease(ctx, child.ID, workerActor.ID, 1, time.Minute, sys, now)
	if err != nil {
		t.Fatalf("OfferLease child: %v", err)
	}
	pkg, err := st.GetContextPackage(ctx, childLease.ID)
	if err != nil || len(pkg.Artifacts) != 1 || len(pkg.BoardViews) != 1 ||
		pkg.Budget.Held != 60 || pkg.SnapshotHash == "" {
		t.Fatalf("context package=%+v err=%v", pkg, err)
	}
	accepted, err = st.AcceptLease(ctx, childLease.ID, childLease.FencingToken, 2, workerActor, now)
	if err != nil {
		t.Fatalf("AcceptLease child: %v", err)
	}
	runningChild, err := st.StartSubtask(ctx, child.ID, childLease.FencingToken, accepted.Version, workerActor, now)
	if err != nil {
		t.Fatalf("StartSubtask child: %v", err)
	}
	if _, err := st.CompleteSubtaskWithUsage(ctx, child.ID, childLease.FencingToken,
		"budget-complete", "artifact://budget", 40, runningChild.Version, workerActor, now); err != nil {
		t.Fatalf("CompleteSubtaskWithUsage: %v", err)
	}
	account, _ = st.GetMissionBudget(ctx, m.ID)
	holds, _ := st.ListBudgetHolds(ctx, m.ID)
	if account.Held != 0 || account.Spent != 40 || account.Available != 60 ||
		len(holds) != 1 || holds[0].Status != store.BudgetHoldSettled || holds[0].Actual != 40 {
		t.Fatalf("settled account=%+v holds=%+v", account, holds)
	}
}

func TestPGConformance(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	if err := st.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	sys := store.Actor{Kind: "system", ID: "test"}
	agt := store.Actor{Kind: "agent", ID: "agt_a"}

	m := &mission.Mission{ID: "msn_pg1", Owner: "u1", Goal: "g", Status: mission.MissionActive}
	subs := []*mission.Subtask{
		{ID: "sub_pga", MissionID: m.ID, Kind: mission.KindAgent, State: mission.StatePending,
			RequiredSkills: []string{"web.research"}},
	}
	if err := st.CreateMission(ctx, m, subs, sys, now); err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	if got, err := st.GetSubtask(ctx, "sub_pga"); err != nil || got.MissionID != m.ID {
		t.Fatalf("GetSubtask: %+v err=%v", got, err)
	}
	if err := st.UpsertAgent(ctx, &store.Agent{ID: "agt_a", Name: "a", Platform: "http-echo",
		Capabilities:   []store.Capability{{Skill: "web.research", Level: 0.9}},
		MaxConcurrency: 1, Health: "healthy"}, now); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	heartbeatAt := now.Add(time.Second)
	if err := st.HeartbeatAgent(ctx, "agt_a", heartbeatAt); err != nil {
		t.Fatalf("HeartbeatAgent: %v", err)
	}
	if a, err := st.GetAgent(ctx, "agt_a"); err != nil || !a.LastHeartbeat.Equal(heartbeatAt) {
		t.Fatalf("heartbeat round trip: %+v err=%v", a, err)
	}

	sub, err := st.TransitionSubtask(ctx, "sub_pga", mission.EvDepsSatisfied, 0, sys, nil, now, nil)
	if err != nil || sub.State != mission.StateReady {
		t.Fatalf("transition: %v %s", err, sub.State)
	}
	// CAS 冲突
	if _, err := st.TransitionSubtask(ctx, "sub_pga", mission.EvDepsSatisfied, 0, sys, nil, now, nil); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("expect conflict, got %v", err)
	}

	l1, err := st.OfferLease(ctx, "sub_pga", "agt_a", 1, 30*time.Second, sys, now)
	if err != nil {
		t.Fatalf("OfferLease: %v", err)
	}
	// 唯一部分索引：第二个活跃租约被拒
	if _, err := st.OfferLease(ctx, "sub_pga", "agt_a", 2, 30*time.Second, sys, now); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("second active lease must conflict, got %v", err)
	}
	if _, err := st.AcceptLease(ctx, l1.ID, l1.FencingToken+1, 2, agt, now); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("bad token must fence, got %v", err)
	}
	sub, err = st.AcceptLease(ctx, l1.ID, l1.FencingToken, 2, agt, now)
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	if _, err := st.StartSubtask(ctx, "sub_pga", l1.FencingToken, sub.Version, agt, now); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	cur, _ := st.ListSubtasks(ctx, m.ID)
	if _, err := st.CompleteSubtask(ctx, "sub_pga", l1.FencingToken, "idem-pg-1", "artifact://r", cur[0].Version, agt, now); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	// 幂等重放
	if _, err := st.CompleteSubtask(ctx, "sub_pga", l1.FencingToken, "idem-pg-1", "artifact://r", 999, agt, now); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("idempotent replay: got %v", err)
	}
	// Agent 并发计数回落
	a, err := st.GetAgent(ctx, "agt_a")
	if err != nil || a.Running != 0 {
		t.Fatalf("running=%d err=%v", a.Running, err)
	}
	// 事件流完整且有序
	evs, err := st.ListMissionEvents(ctx, m.ID, 0, 100)
	if err != nil || len(evs) != 7 {
		t.Fatalf("events=%d err=%v, want 7", len(evs), err)
	}
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq <= evs[i-1].Seq {
			t.Fatal("seq not increasing")
		}
	}
}

// TestPGTriggerScopesRoundTrip M5-H2：trigger_scopes 列存取（0005 迁移）。
func TestPGTriggerScopesRoundTrip(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)

	// 未声明 → '[]'（默认收紧）
	if err := st.UpsertAgent(ctx, &store.Agent{ID: "agt_plain", Name: "p", Platform: "http-echo"}, now); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	a, err := st.GetAgent(ctx, "agt_plain")
	if err != nil || len(a.TriggerScopes) != 0 {
		t.Fatalf("default scopes must be empty: %+v err=%v", a.TriggerScopes, err)
	}
	// 声明 + upsert 覆盖
	if err := st.UpsertAgent(ctx, &store.Agent{ID: "agt_scoped", Name: "s", Platform: "http-echo",
		TriggerScopes: []string{"trigger.create_mission"}}, now); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	if err := st.UpsertAgent(ctx, &store.Agent{ID: "agt_scoped", Name: "s", Platform: "http-echo",
		TriggerScopes: []string{"trigger.wake"}}, now); err != nil {
		t.Fatalf("re-UpsertAgent: %v", err)
	}
	a, err = st.GetAgent(ctx, "agt_scoped")
	if err != nil || len(a.TriggerScopes) != 1 || a.TriggerScopes[0] != "trigger.wake" {
		t.Fatalf("scopes round trip: %+v err=%v", a.TriggerScopes, err)
	}
}

// TestPGSpawnSubtask M6-K1：pg 侧 delegate 原子落库一致性（幂等/fencing/RUNNING 校验）。
func TestPGSpawnSubtask(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	sys := store.Actor{Kind: "system", ID: "test"}
	agt := store.Actor{Kind: "agent", ID: "agt_a"}

	m := &mission.Mission{ID: "msn_pg2", Owner: "u1", Goal: "g", Status: mission.MissionActive}
	if err := st.CreateMission(ctx, m, []*mission.Subtask{
		{ID: "sub_pgp", MissionID: m.ID, Kind: mission.KindAgent, State: mission.StatePending},
	}, sys, now); err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	if err := st.UpsertAgent(ctx, &store.Agent{ID: "agt_a", Name: "a", Platform: "http-echo"}, now); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
	}
	if _, err := st.TransitionSubtask(ctx, "sub_pgp", mission.EvDepsSatisfied, 0, sys, nil, now, nil); err != nil {
		t.Fatalf("activate: %v", err)
	}
	l1, err := st.OfferLease(ctx, "sub_pgp", "agt_a", 1, 30*time.Second, sys, now)
	if err != nil {
		t.Fatalf("OfferLease: %v", err)
	}
	sub, err := st.AcceptLease(ctx, l1.ID, l1.FencingToken, 2, agt, now)
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	parent, err := st.StartSubtask(ctx, "sub_pgp", l1.FencingToken, sub.Version, agt, now)
	if err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	child := &mission.Subtask{ID: "sub_pgc1", MissionID: m.ID, ParentID: parent.ID,
		Kind: mission.KindAgent, Input: map[string]any{"topic": "t"}, ReworkOf: ""}
	if _, err := st.SpawnSubtask(ctx, "pg-dlg-1", parent.ID, l1.FencingToken+9, parent.Version,
		child, agt, now); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("bad fencing: %v", err)
	}
	if _, err := st.SpawnSubtask(ctx, "pg-dlg-1", parent.ID, l1.FencingToken, parent.Version,
		child, agt, now); err != nil {
		t.Fatalf("spawn: %v", err)
	}
	existing, err := st.SpawnSubtask(ctx, "pg-dlg-1", parent.ID, l1.FencingToken, parent.Version,
		&mission.Subtask{ID: "sub_pgcX", MissionID: m.ID}, agt, now)
	if !errors.Is(err, store.ErrDuplicate) || existing != "sub_pgc1" {
		t.Fatalf("dedup: existing=%s err=%v", existing, err)
	}
	n, _ := st.CountChildren(ctx, parent.ID)
	if n != 1 {
		t.Fatalf("children = %d, want 1", n)
	}
	// spec jsonb 内的 input/rework_of 回读一致
	subs, _ := st.ListSubtasks(ctx, m.ID)
	var got *mission.Subtask
	for _, x := range subs {
		if x.ID == "sub_pgc1" {
			got = x
		}
	}
	if got == nil || got.State != mission.StateReady || got.Input["topic"] != "t" || got.ParentID != parent.ID {
		t.Fatalf("child spec round trip: %+v", got)
	}
	// succeeded 载荷含 subtask_id
	cur, _ := st.ListSubtasks(ctx, m.ID)
	for _, x := range cur {
		if x.ID == parent.ID {
			if _, err := st.CompleteSubtask(ctx, parent.ID, l1.FencingToken, "pg-done", "r", x.Version, agt, now); err != nil {
				t.Fatalf("complete: %v", err)
			}
		}
	}
	evs, _ := st.ListMissionEvents(ctx, m.ID, 0, 100)
	found := false
	for _, e := range evs {
		if e.Type == string(mission.EvCompleted) && e.Payload["subtask_id"] == parent.ID {
			found = true
		}
	}
	if !found {
		t.Fatal("subtask.succeeded payload must carry subtask_id")
	}
}

func TestPGCreateDecisionAndBlock(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	sys := store.Actor{Kind: "system", ID: "hitl"}
	m := &mission.Mission{ID: "msn_pg_hitl", Owner: "u1", Goal: "approve", Status: mission.MissionActive}
	if err := st.CreateMission(ctx, m, []*mission.Subtask{
		{ID: "sub_pg_gate", MissionID: m.ID, Kind: mission.KindHumanApproval, State: mission.StatePending},
	}, sys, now); err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	ready, err := st.TransitionSubtask(ctx, "sub_pg_gate", mission.EvDepsSatisfied, 0, sys, nil, now, nil)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}
	d := &store.Decision{ID: "dec_pg_gate", MissionID: m.ID, SubtaskID: ready.ID,
		Kind: "approval", Question: "ok?", Options: []string{"approve", "reject"}}
	blocked, err := st.CreateDecisionAndBlock(ctx, d, ready.Version, nil, sys, now)
	if err != nil {
		t.Fatalf("CreateDecisionAndBlock: %v", err)
	}
	if blocked.State != mission.StateBlocked {
		t.Fatalf("state=%s, want BLOCKED", blocked.State)
	}
	pending, err := st.ListDecisions(ctx, m.ID, true)
	if err != nil || len(pending) != 1 || pending[0].DeciderID != "" {
		t.Fatalf("pending decision: %+v err=%v", pending, err)
	}
}

func TestPGLeadRecovery(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	now := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	sys := store.Actor{Kind: "system", ID: "test"}
	leadActor := store.Actor{Kind: "agent", ID: "agt_lead"}
	workerActor := store.Actor{Kind: "agent", ID: "agt_worker"}
	m := &mission.Mission{ID: "msn_pg_lead", Owner: "u1", Goal: "recover", Status: mission.MissionActive}
	if err := st.CreateMission(ctx, m, []*mission.Subtask{
		{ID: "sub_pg_lead", MissionID: m.ID, Kind: mission.KindAgent, State: mission.StatePending},
	}, sys, now); err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	for _, a := range []*store.Agent{
		{ID: "agt_lead", Name: "lead", Platform: "http-echo", Health: "healthy"},
		{ID: "agt_worker", Name: "worker", Platform: "http-echo", Health: "healthy"},
	} {
		if err := st.UpsertAgent(ctx, a, now); err != nil {
			t.Fatalf("UpsertAgent: %v", err)
		}
	}
	if _, err := st.TransitionSubtask(ctx, "sub_pg_lead", mission.EvDepsSatisfied, 0, sys, nil, now, nil); err != nil {
		t.Fatalf("activate lead: %v", err)
	}
	leadLease, err := st.OfferLease(ctx, "sub_pg_lead", "agt_lead", 1, 30*time.Second, sys, now)
	if err != nil {
		t.Fatalf("offer lead: %v", err)
	}
	lead, err := st.AcceptLease(ctx, leadLease.ID, leadLease.FencingToken, 2, leadActor, now)
	if err != nil {
		t.Fatalf("accept lead: %v", err)
	}
	lead, err = st.StartSubtask(ctx, lead.ID, leadLease.FencingToken, lead.Version, leadActor, now)
	if err != nil {
		t.Fatalf("start lead: %v", err)
	}
	snapshot, err := st.SaveLeadSnapshot(ctx, lead.ID, leadLease.FencingToken, -1,
		[]byte(`{"intent":"review"}`), time.Minute, leadActor, now)
	if err != nil || snapshot.Version != 0 {
		t.Fatalf("snapshot=%+v err=%v", snapshot, err)
	}
	if _, err := st.SaveLeadSnapshot(ctx, lead.ID, leadLease.FencingToken, -1,
		[]byte(`{"intent":"duplicate"}`), time.Minute, leadActor, now); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("snapshot create CAS: %v", err)
	}
	snapshot, err = st.SaveLeadSnapshot(ctx, lead.ID, leadLease.FencingToken, snapshot.Version,
		[]byte(`{"intent":"ingest"}`), time.Minute, leadActor, now)
	if err != nil || snapshot.Version != 1 {
		t.Fatalf("snapshot update=%+v err=%v", snapshot, err)
	}

	child := &mission.Subtask{ID: "sub_pg_lead_child", MissionID: m.ID, ParentID: lead.ID, Kind: mission.KindAgent}
	if _, err := st.SpawnSubtask(ctx, "pg-lead-child", lead.ID, leadLease.FencingToken,
		lead.Version, child, leadActor, now); err != nil {
		t.Fatalf("spawn child: %v", err)
	}
	childLease, err := st.OfferLease(ctx, child.ID, "agt_worker", 1, 30*time.Second, sys, now)
	if err != nil {
		t.Fatalf("offer child: %v", err)
	}
	child, err = st.AcceptLease(ctx, childLease.ID, childLease.FencingToken, 2, workerActor, now)
	if err != nil {
		t.Fatalf("accept child: %v", err)
	}
	child, err = st.StartSubtask(ctx, child.ID, childLease.FencingToken, child.Version, workerActor, now)
	if err != nil {
		t.Fatalf("start child: %v", err)
	}
	if _, err := st.CompleteSubtask(ctx, child.ID, childLease.FencingToken, "pg-lead-result",
		"artifact://result", child.Version, workerActor, now); err != nil {
		t.Fatalf("complete child: %v", err)
	}
	items, err := st.ListLeadInbox(ctx, lead.ID, true)
	if err != nil || len(items) != 1 || items[0].SourceSubtaskID != child.ID {
		t.Fatalf("inbox=%+v err=%v", items, err)
	}
	if _, err := st.IngestLeadInbox(ctx, items[0].ID, lead.ID, leadLease.FencingToken,
		items[0].Version, store.LeadIngestSummary, workerActor, now); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("foreign ingest: %v", err)
	}
	ingested, err := st.IngestLeadInbox(ctx, items[0].ID, lead.ID, leadLease.FencingToken,
		items[0].Version, store.LeadIngestSummary, leadActor, now)
	if err != nil || ingested.Status != store.LeadInboxIngested {
		t.Fatalf("ingest=%+v err=%v", ingested, err)
	}

	taken, err := st.TakeoverStaleLeads(ctx, now.Add(time.Minute+time.Second))
	if err != nil || len(taken) != 1 || taken[0].State != mission.StateReady {
		t.Fatalf("takeover=%+v err=%v", taken, err)
	}
	if oldLease, _ := st.GetLease(ctx, leadLease.ID); oldLease.State != store.LeaseFenced {
		t.Fatalf("old lease=%+v", oldLease)
	}
	if oldAgent, _ := st.GetAgent(ctx, "agt_lead"); oldAgent.Health != "suspect" || oldAgent.Running != 0 {
		t.Fatalf("old agent=%+v", oldAgent)
	}
	if gotChild, _ := st.GetSubtask(ctx, child.ID); gotChild.State != mission.StateSucceeded {
		t.Fatalf("child changed during takeover: %s", gotChild.State)
	}
}
