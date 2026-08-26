package core

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// ---- M6：主子委托协议核心（docs/plan/M6-delegate.md §5） ----

// setupLead 建立"Lead 持 RUNNING 父任务"的委托前提，返回 (mission, parent, fencingToken)。
func setupLead(t *testing.T, s *Service) (*mission.Mission, *mission.Subtask, int64) {
	t.Helper()
	mustRegisterScopes(t, s, "agt_lead", []string{ScopeSpawnSubtask}, 2, "lead.coordinate")
	m, err := s.CreateMission(ctx, "u1", "主子委托", []TaskSpec{
		{Name: "lead", Kind: mission.KindAgent, RequiredSkills: []string{"lead.coordinate"}},
	})
	if err != nil {
		t.Fatalf("CreateMission: %v", err)
	}
	parent, token := startOne(t, s, "agt_lead")
	return m, parent, token
}

func delegateIntent(parent *mission.Subtask, token int64, key string, task *DelegateSpec) Intent {
	return Intent{
		Source: store.Actor{Kind: "agent", ID: "agt_lead"}, Action: IntentDelegate,
		IdempotencyKey: key, ParentSubtaskID: parent.ID,
		FencingToken: token, ParentVersion: parent.Version, Task: task,
	}
}

func TestDelegateHappyPath(t *testing.T) {
	s, _, _ := newService()
	m, parent, token := setupLead(t, s)

	res, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "dlg-1", &DelegateSpec{
		Name: "research", RequiredSkills: []string{"web.research"},
		Input: map[string]any{"topic": "储能"}, Priority: 5,
	}))
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	if res.SubtaskID == "" || res.MissionID != m.ID || res.Deduplicated {
		t.Fatalf("res = %+v", res)
	}
	// 子女即建即 READY、parent_id 因果链、载荷入 spec
	child := mustGet(t, s, m.ID, res.SubtaskID)
	if child.State != mission.StateReady || child.ParentID != parent.ID {
		t.Fatalf("child state=%s parent=%s", child.State, child.ParentID)
	}
	if child.Input["topic"] != "储能" || child.Scheduling.Priority != 5 {
		t.Fatalf("child spec = %+v", child)
	}
	// 因果留痕：创建事件带 parent_subtask_id，actor=Lead
	evs, _ := s.ListMissionEvents(ctx, m.ID, 0, 100)
	found := false
	for _, e := range evs {
		if e.AggregateID == child.ID && e.Type == string(mission.EvCreated) &&
			e.Payload["parent_subtask_id"] == parent.ID && e.Actor.ID == "agt_lead" {
			found = true
		}
	}
	if !found {
		t.Fatal("child creation event must carry parent_subtask_id with lead as actor")
	}
	// 幂等重发：同键返回原子女
	res2, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "dlg-1", &DelegateSpec{Name: "research"}))
	if err != nil || !res2.Deduplicated || res2.SubtaskID != res.SubtaskID {
		t.Fatalf("dedup: %+v, %v", res2, err)
	}
	// 子女可被正常调度
	mustRegister(t, s, "agt_w", 1, "web.research")
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offers, _ := s.ListOffers(ctx, "agt_w")
	if len(offers) != 1 || offers[0].ID != child.ID {
		t.Fatalf("delegated child must be schedulable, offers = %+v", offers)
	}
}

func TestDelegateRequiresParentLeaseOwner(t *testing.T) {
	s, _, _ := newService()
	_, parent, token := setupLead(t, s)
	mustRegisterScopes(t, s, "agt_intruder", []string{ScopeSpawnSubtask}, 1, "lead.coordinate")
	in := delegateIntent(parent, token, "dlg-owner", &DelegateSpec{Name: "foreign"})
	in.Source.ID = "agt_intruder"
	if _, err := s.SubmitIntent(ctx, in); !errors.Is(err, ErrForbidden) {
		t.Fatalf("foreign delegate must be forbidden, got %v", err)
	}
}

func TestDelegateScopeEnforced(t *testing.T) {
	s, _, _ := newService()
	// 无 spawn_subtask scope 的 Lead
	mustRegister(t, s, "agt_lead", 2, "lead.coordinate")
	s.CreateMission(ctx, "u1", "scope", []TaskSpec{
		{Name: "lead", Kind: mission.KindAgent, RequiredSkills: []string{"lead.coordinate"}},
	})
	parent, token := startOne(t, s, "agt_lead")
	if _, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "dlg-x", &DelegateSpec{})); !errors.Is(err, ErrForbidden) {
		t.Fatalf("delegate without scope must be forbidden, got %v", err)
	}
	// 未注册 Agent
	bad := delegateIntent(parent, token, "dlg-y", &DelegateSpec{})
	bad.Source.ID = "agt_ghost"
	if _, err := s.SubmitIntent(ctx, bad); !errors.Is(err, ErrForbidden) {
		t.Fatalf("unregistered delegate must be forbidden, got %v", err)
	}
}

func TestDelegateFencingAndState(t *testing.T) {
	s, _, _ := newService()
	_, parent, token := setupLead(t, s)

	// 错误 fencing token → ErrFenced
	in := delegateIntent(parent, token+99, "dlg-f1", &DelegateSpec{})
	if _, err := s.SubmitIntent(ctx, in); !errors.Is(err, store.ErrFenced) {
		t.Fatalf("bad fencing token must be fenced, got %v", err)
	}
	// 错误 version → ErrConflict
	in = delegateIntent(parent, token, "dlg-f2", &DelegateSpec{})
	in.ParentVersion = parent.Version + 1
	if _, err := s.SubmitIntent(ctx, in); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale version must conflict, got %v", err)
	}
	// 父任务非 RUNNING（挂起后）→ 拒
	ttl := time.Now().Add(time.Hour)
	cur := mustGet(t, s, parent.MissionID, parent.ID)
	if _, err := s.Suspend(ctx, parent.ID, token, cur.Version, "agt_lead",
		&mission.WakeSpec{Kind: mission.WakeManual, Deadline: &ttl}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	in = delegateIntent(parent, token, "dlg-f3", &DelegateSpec{})
	if _, err := s.SubmitIntent(ctx, in); err == nil {
		t.Fatal("delegate from non-RUNNING parent must be rejected")
	}
}

func TestDelegateDepthAndFanout(t *testing.T) {
	s, _, _ := newService()
	s.cfg.MaxDelegateDepth = 2
	s.cfg.MaxDelegateFanout = 2
	m, parent, token := setupLead(t, s)
	mustRegister(t, s, "agt_w", 8, "web.research")

	// fanout：第 3 个直接子女被拒
	for i := 1; i <= 2; i++ {
		if _, err := s.SubmitIntent(ctx, delegateIntent(parent, token, fmt.Sprintf("dlg-fan-%d", i),
			&DelegateSpec{RequiredSkills: []string{"web.research"}})); err != nil {
			t.Fatalf("delegate %d: %v", i, err)
		}
	}
	if _, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "dlg-fan-3",
		&DelegateSpec{RequiredSkills: []string{"web.research"}})); err == nil {
		t.Fatal("fanout beyond max must be rejected")
	}

	// depth：子女（depth1）再委托得 depth2 孙任务；孙任务再委托 depth3 超 max=2 被拒
	subs, _ := s.ListSubtasks(ctx, m.ID)
	var c1 *mission.Subtask
	for _, sub := range subs {
		if sub.ParentID == parent.ID {
			c1 = sub
			break
		}
	}
	// c1 调度给 worker 并启动（成为 RUNNING Lead-of-child）
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offers, _ := s.ListOffers(ctx, "agt_w")
	var offer *mission.Subtask
	for _, o := range offers {
		if o.ID == c1.ID {
			offer = o
		}
	}
	if offer == nil {
		t.Fatalf("no offer for %s", c1.ID)
	}
	token1 := fenceOf(t, s, offer.LeaseID)
	accepted1, err := s.AcceptLease(ctx, offer.LeaseID, token1, offer.Version, "agt_w")
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	if _, err := s.StartSubtask(ctx, c1.ID, token1, accepted1.Version, "agt_w"); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	// worker 无 scope——先补授权（recursive delegate 需要 spawn scope）
	mustRegisterScopes(t, s, "agt_w", []string{ScopeSpawnSubtask}, 8, "web.research")
	cur1 := mustGet(t, s, m.ID, c1.ID)
	grand, err := s.SubmitIntent(ctx, Intent{
		Source: store.Actor{Kind: "agent", ID: "agt_w"}, Action: IntentDelegate,
		IdempotencyKey: "dlg-depth-1", ParentSubtaskID: c1.ID,
		FencingToken: token1, ParentVersion: cur1.Version,
		Task: &DelegateSpec{RequiredSkills: []string{"web.research"}},
	})
	if err != nil {
		t.Fatalf("depth-2 delegate: %v", err)
	}
	// 孙任务 RUNNING 后再委托 → depth3 > max2 拒
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offers, _ = s.ListOffers(ctx, "agt_w")
	var g *mission.Subtask
	for _, o := range offers {
		if o.ID == grand.SubtaskID {
			g = o
		}
	}
	if g == nil {
		t.Fatal("no offer for grandchild")
	}
	token2 := fenceOf(t, s, g.LeaseID)
	accepted2, err := s.AcceptLease(ctx, g.LeaseID, token2, g.Version, "agt_w")
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	if _, err := s.StartSubtask(ctx, g.ID, token2, accepted2.Version, "agt_w"); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	curG := mustGet(t, s, m.ID, g.ID)
	if _, err := s.SubmitIntent(ctx, Intent{
		Source: store.Actor{Kind: "agent", ID: "agt_w"}, Action: IntentDelegate,
		IdempotencyKey: "dlg-depth-2", ParentSubtaskID: g.ID,
		FencingToken: token2, ParentVersion: curG.Version,
		Task: &DelegateSpec{},
	}); err == nil {
		t.Fatal("depth beyond max must be rejected")
	}
}

func TestDelegateRework(t *testing.T) {
	s, _, _ := newService()
	s.cfg.MaxRework = 2
	m, parent, token := setupLead(t, s)
	mustRegister(t, s, "agt_w", 2, "web.research")

	res, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "dlg-rw-1",
		&DelegateSpec{RequiredSkills: []string{"web.research"}}))
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	// 子女完成后 Lead 验收不通过 → rework（feedback 入新子女 input，rework_of 链）
	child := mustGet(t, s, m.ID, res.SubtaskID)
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offers, _ := s.ListOffers(ctx, "agt_w")
	if len(offers) != 1 {
		t.Fatalf("offers = %+v", offers)
	}
	wtoken := fenceOf(t, s, offers[0].LeaseID)
	acceptedW, err := s.AcceptLease(ctx, offers[0].LeaseID, wtoken, offers[0].Version, "agt_w")
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	if _, err := s.StartSubtask(ctx, child.ID, wtoken, acceptedW.Version, "agt_w"); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	cur := mustGet(t, s, m.ID, child.ID)
	if _, err := s.CompleteSubtask(ctx, child.ID, wtoken, "rw-c1", "artifact://draft1", cur.Version, "agt_w"); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	parentCur := mustGet(t, s, m.ID, parent.ID)
	rw, err := s.SubmitIntent(ctx, delegateIntent(parentCur, token, "dlg-rw-2", &DelegateSpec{
		RequiredSkills: []string{"web.research"},
		ReworkOf:       child.ID, Feedback: "数据来源不足，补充 2025 年数据",
	}))
	if err != nil {
		t.Fatalf("rework: %v", err)
	}
	rwChild := mustGet(t, s, m.ID, rw.SubtaskID)
	if rwChild.ReworkOf != child.ID || rwChild.Input["feedback"] != "数据来源不足，补充 2025 年数据" {
		t.Fatalf("rework child = %+v", rwChild)
	}
	// 不能返工别人派生的任务：伪造 rework_of 指向 parent 自己派生链之外
	if _, err := s.SubmitIntent(ctx, delegateIntent(parentCur, token, "dlg-rw-bad", &DelegateSpec{
		ReworkOf: parent.ID, // parent 的 ParentID 为空，不属于 parent 的子女
	})); err == nil {
		t.Fatal("rework of non-child must be rejected")
	}
	// 链长达 MaxRework=2：rwChild 完成后（模拟）再 rework 即拒——直接再派生一次到上限
	rw2, err := s.SubmitIntent(ctx, delegateIntent(parentCur, token, "dlg-rw-3", &DelegateSpec{
		ReworkOf: rwChild.ID, Feedback: "还不行",
	}))
	if err != nil {
		t.Fatalf("second rework: %v", err)
	}
	if _, err := s.SubmitIntent(ctx, delegateIntent(parentCur, token, "dlg-rw-4", &DelegateSpec{
		ReworkOf: rw2.SubtaskID, Feedback: "第三次",
	})); err == nil {
		t.Fatal("rework chain at max must be rejected")
	}
}

func TestDelegateEventWakePrecise(t *testing.T) {
	s, _, clk := newService()
	m, parent, token := setupLead(t, s)
	mustRegister(t, s, "agt_w", 2, "web.research")

	res, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "dlg-w-1",
		&DelegateSpec{RequiredSkills: []string{"web.research"}}))
	if err != nil {
		t.Fatalf("delegate: %v", err)
	}
	// Lead 挂起精确等待该子女完成（§15.1：delegate 后 suspend 不空占资源）
	ttl := clk.Now().Add(time.Hour)
	cur := mustGet(t, s, m.ID, parent.ID)
	if _, err := s.Suspend(ctx, parent.ID, token, cur.Version, "agt_lead",
		&mission.WakeSpec{Kind: mission.WakeEvent, Deadline: &ttl,
			Event: &mission.EventMatch{Types: []string{"subtask.succeeded"},
				Where: map[string]any{"subtask_id": res.SubtaskID}}}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// 子女完成 → where 精确命中 → Lead 醒
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offers, _ := s.ListOffers(ctx, "agt_w")
	if len(offers) != 1 {
		t.Fatalf("offers = %+v", offers)
	}
	wtoken := fenceOf(t, s, offers[0].LeaseID)
	acceptedW, err := s.AcceptLease(ctx, offers[0].LeaseID, wtoken, offers[0].Version, "agt_w")
	if err != nil {
		t.Fatalf("AcceptLease: %v", err)
	}
	if _, err := s.StartSubtask(ctx, res.SubtaskID, wtoken, acceptedW.Version, "agt_w"); err != nil {
		t.Fatalf("StartSubtask: %v", err)
	}
	childCur := mustGet(t, s, m.ID, res.SubtaskID)
	if _, err := s.CompleteSubtask(ctx, res.SubtaskID, wtoken, "w-c1", "artifact://done", childCur.Version, "agt_w"); err != nil {
		t.Fatalf("CompleteSubtask: %v", err)
	}
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m.ID, parent.ID); cur.State != mission.StateReady {
		t.Fatalf("lead must wake on child completion, state = %s", cur.State)
	}
}
