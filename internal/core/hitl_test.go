package core

import (
	"errors"
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// ---- H1/H2：human 审批节点全生命周期 ----

func TestHumanApprovalNodeFlow(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")

	m, err := s.CreateMission(ctx, "u1", "审批流水线", []TaskSpec{
		{Name: "draft", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "approve", Kind: mission.KindHumanApproval, DependsOn: []string{"draft"},
			Question: "发布该草稿？"},
		{Name: "publish", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"},
			DependsOn: []string{"approve"}},
	})
	if err != nil {
		t.Fatalf("CreateMission: %v", err)
	}

	// human 节点不得被派给 Agent
	s.ScheduleOnce(ctx)
	runOneTask(t, s, "agt_a", nil)
	s.ScheduleOnce(ctx)
	offers, _ := s.ListOffers(ctx, "agt_a")
	if len(offers) != 0 {
		t.Fatalf("human node must not be offered to agent, got %d", len(offers))
	}

	// 开工单：approve 节点 → BLOCKED + pending decision
	opened, _ := s.OpenHumanDecisions(ctx)
	if opened != 1 {
		t.Fatalf("opened = %d, want 1", opened)
	}
	approval := mustGet(t, s, m.ID, subID(m.ID, "approve"))
	if approval.State != mission.StateBlocked {
		t.Fatalf("approval = %s, want BLOCKED", approval.State)
	}
	pending, _ := s.ListDecisions(ctx, m.ID, true)
	if len(pending) != 1 || pending[0].Question != "发布该草稿？" {
		t.Fatalf("pending decisions: %+v", pending)
	}

	// 裁决通过 → SUCCEEDED → 下游 publish 就绪
	d := pending[0]
	if _, err := s.ResolveDecision(ctx, d.ID, "approve", "LGTM", "lead"); err != nil {
		t.Fatalf("ResolveDecision: %v", err)
	}
	approval = mustGet(t, s, m.ID, subID(m.ID, "approve"))
	if approval.State != mission.StateSucceeded {
		t.Fatalf("approval after approve = %s, want SUCCEEDED", approval.State)
	}
	publish := mustGet(t, s, m.ID, subID(m.ID, "publish"))
	if publish.State != mission.StateReady {
		t.Fatalf("publish = %s, want READY", publish.State)
	}

	// 重复裁决必须 409（审计不可篡改）
	if _, err := s.ResolveDecision(ctx, d.ID, "reject", "changed my mind", "lead"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("double resolve must conflict, got %v", err)
	}

	// 走完 publish → Mission SUCCEEDED
	s.ScheduleOnce(ctx)
	runOneTask(t, s, "agt_a", nil)
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionSucceeded {
		t.Fatalf("mission = %s, want SUCCEEDED", final.Status)
	}
}

func TestHumanApprovalReject(t *testing.T) {
	s, _, _ := newService()
	m, _ := s.CreateMission(ctx, "u1", "reject 路径", []TaskSpec{
		{Name: "approve", Kind: mission.KindHumanApproval, Question: "ok?"},
	})
	s.OpenHumanDecisions(ctx)
	pending, _ := s.ListDecisions(ctx, m.ID, true)
	if _, err := s.ResolveDecision(ctx, pending[0].ID, RejectChoice, "不合规", "compliance"); err != nil {
		t.Fatalf("ResolveDecision: %v", err)
	}
	sub := mustGet(t, s, m.ID, subID(m.ID, "approve"))
	if sub.State != mission.StateFailed {
		t.Fatalf("state = %s, want FAILED", sub.State)
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionFailed {
		t.Fatalf("mission = %s, want FAILED", final.Status)
	}
}

func TestRejectCancelsDownstream(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	// draft → gate(审批) → publish → archive：否决 gate 后 publish/archive 不可达
	m, err := s.CreateMission(ctx, "u1", "否决级联", []TaskSpec{
		{Name: "draft", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
		{Name: "gate", Kind: mission.KindHumanApproval, DependsOn: []string{"draft"}},
		{Name: "publish", Kind: mission.KindAgent, DependsOn: []string{"gate"}},
		{Name: "archive", Kind: mission.KindAgent, DependsOn: []string{"publish"}},
	})
	if err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	s.ScheduleOnce(ctx)
	runOneTask(t, s, "agt_a", nil)
	s.OpenHumanDecisions(ctx)
	pending, _ := s.ListDecisions(ctx, m.ID, true)
	if len(pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(pending))
	}
	if _, err := s.ResolveDecision(ctx, pending[0].ID, RejectChoice, "不合规", "compliance"); err != nil {
		t.Fatalf("ResolveDecision: %v", err)
	}
	for _, name := range []string{"publish", "archive"} {
		if sub := mustGet(t, s, m.ID, subID(m.ID, name)); sub.State != mission.StateCancelled {
			t.Fatalf("%s = %s, want CANCELLED（级联）", name, sub.State)
		}
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionFailed {
		t.Fatalf("mission = %s, want FAILED（否决即终态）", final.Status)
	}
}

// ---- H3：Agent 主动决策请求 ----

func TestAgentRequestDecision(t *testing.T) {
	s, _, _ := newService()
	mustRegister(t, s, "agt_a", 2, "web.research")
	m, _ := s.CreateMission(ctx, "u1", "agent 提问", []TaskSpec{
		{Name: "work", Kind: mission.KindAgent, RequiredSkills: []string{"web.research"}},
	})
	s.ScheduleOnce(ctx)
	offers, _ := s.ListOffers(ctx, "agt_a")
	sub := offers[0]
	token := fenceOf(t, s, sub.LeaseID)
	sub, _ = s.AcceptLease(ctx, sub.LeaseID, token, sub.Version, "agt_a")
	s.StartSubtask(ctx, sub.ID, token, sub.Version, "agt_a")
	cur := mustGet(t, s, m.ID, sub.ID)

	// 无 fencing 的请求被拒
	if _, err := s.RequestDecision(ctx, sub.ID, token+1, cur.Version, "agt_a", "选哪个方案？", []string{"A", "B"}); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("bad token must fence, got %v", err)
	}
	// 正常请求 → BLOCKED + 工单
	d, err := s.RequestDecision(ctx, sub.ID, token, cur.Version, "agt_a", "选哪个方案？", []string{"A", "B"})
	if err != nil {
		t.Fatalf("RequestDecision: %v", err)
	}
	if mustGet(t, s, m.ID, sub.ID).State != mission.StateBlocked {
		t.Fatal("subtask should be BLOCKED")
	}
	// 裁决选择 B（非否决值）→ 回 RUNNING 续跑，choice 可读回
	if _, err := s.ResolveDecision(ctx, d.ID, "B", "B 覆盖更全", "lead"); err != nil {
		t.Fatalf("ResolveDecision: %v", err)
	}
	resumed := mustGet(t, s, m.ID, sub.ID)
	if resumed.State != mission.StateRunning {
		t.Fatalf("state = %s, want RUNNING（续跑）", resumed.State)
	}
	// Agent 续跑并完成
	cur = mustGet(t, s, m.ID, sub.ID)
	if _, err := s.CompleteSubtask(ctx, sub.ID, token, "idem-dec", "artifact://planB", cur.Version, "agt_a"); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionSucceeded {
		t.Fatalf("mission = %s, want SUCCEEDED", final.Status)
	}
}

// ---- H6：决策超时自动裁决 ----

func TestDecisionTimeoutAutoReject(t *testing.T) {
	s, _, clk := newService()
	dl := clk.Now().Add(time.Hour)
	m, _ := s.CreateMission(ctx, "u1", "超时审批", []TaskSpec{
		{Name: "approve", Kind: mission.KindHumanApproval, Question: "ok?",
			Deadline: &dl, OnTimeout: "auto_reject"},
	})
	s.OpenHumanDecisions(ctx)
	clk.Advance(61 * time.Minute)
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	sub := mustGet(t, s, m.ID, subID(m.ID, "approve"))
	if sub.State != mission.StateFailed {
		t.Fatalf("state = %s, want FAILED (auto_reject)", sub.State)
	}
	final, _ := s.GetMission(ctx, m.ID)
	if final.Status != mission.MissionFailed {
		t.Fatalf("mission = %s, want FAILED", final.Status)
	}
}

// ---- H4/H5：黑板与 Artifact ----

func TestBoardCAS(t *testing.T) {
	s, _, _ := newService()
	m, _ := s.CreateMission(ctx, "u1", "board", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent},
	})
	// 盲写
	e1, err := s.BoardPut(ctx, m.ID, "shared", "glossary", []byte(`{"term":"x"}`), -1)
	if err != nil || e1.Version != 0 {
		t.Fatalf("first put: %v version=%d", err, e1.Version)
	}
	// CAS 匹配
	if _, err := s.BoardPut(ctx, m.ID, "shared", "glossary", []byte(`{"term":"y"}`), 0); err != nil {
		t.Fatalf("cas put: %v", err)
	}
	// CAS 不匹配 → 409
	if _, err := s.BoardPut(ctx, m.ID, "shared", "glossary", []byte(`{"term":"z"}`), 0); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale cas must conflict, got %v", err)
	}
	got, _ := s.BoardGet(ctx, m.ID, "shared", "glossary")
	if string(got.Value) != `{"term":"y"}` || got.Version != 1 {
		t.Fatalf("board value = %s version=%d", got.Value, got.Version)
	}
	// 命名空间隔离
	if _, err := s.BoardGet(ctx, m.ID, "other", "glossary"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("namespace isolation violated: %v", err)
	}
}

func TestArtifactRoundtrip(t *testing.T) {
	s, _, _ := newService()
	m, _ := s.CreateMission(ctx, "u1", "artifact", []TaskSpec{
		{Name: "a", Kind: mission.KindAgent},
	})
	content := []byte("# 研报正文\n储能板块…")
	a, err := s.PutArtifact(ctx, m.ID, subID(m.ID, "a"), "schema://report/v1", content)
	if err != nil {
		t.Fatalf("PutArtifact: %v", err)
	}
	data, meta, err := s.GetArtifactContent(ctx, a.ID)
	if err != nil || string(data) != string(content) {
		t.Fatalf("roundtrip: %v", err)
	}
	if meta.SHA256 == "" || meta.Size != int64(len(content)) {
		t.Fatalf("meta: %+v", meta)
	}
	// artifact.produced 事件已落（审计）
	evs, _ := s.ListMissionEvents(ctx, m.ID, 0, 100)
	found := false
	for _, e := range evs {
		if e.Type == "artifact.produced" {
			found = true
		}
	}
	if !found {
		t.Fatal("artifact.produced event missing")
	}
}
