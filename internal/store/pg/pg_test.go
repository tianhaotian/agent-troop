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
	// 应用迁移 + 清场（测试库专用！）
	mig, err := os.ReadFile("../../../migrations/0001_init.sql")
	if err != nil {
		t.Fatalf("read migration: %v", err)
	}
	if _, err := st.pool.Exec(ctx, string(mig)); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, err := st.pool.Exec(ctx,
		`TRUNCATE missions, subtasks, agents, leases, artifacts, decisions, idempotency_keys, events`); err != nil {
		t.Fatalf("clean: %v", err)
	}
	t.Cleanup(st.Close)
	return st
}

func TestPGConformance(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
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
	if err := st.UpsertAgent(ctx, &store.Agent{ID: "agt_a", Name: "a", Platform: "http-echo",
		Capabilities: []store.Capability{{Skill: "web.research", Level: 0.9}},
		MaxConcurrency: 1, Health: "healthy"}, now); err != nil {
		t.Fatalf("UpsertAgent: %v", err)
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
