package core

import (
	"errors"
	"sync"
	"testing"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

func setupBudgetLead(t *testing.T, s *Service, total int64) (*mission.Mission, *mission.Subtask, int64) {
	t.Helper()
	mustRegisterScopes(t, s, "agt_lead", []string{ScopeSpawnSubtask}, 2, "lead.coordinate")
	m, err := s.CreateMissionWithBudget(ctx, "u1", "budgeted delegation", total, []TaskSpec{
		{Name: "lead", Kind: mission.KindAgent, RequiredSkills: []string{"lead.coordinate"}},
	})
	if err != nil {
		t.Fatalf("CreateMissionWithBudget: %v", err)
	}
	parent, token := startOne(t, s, "agt_lead")
	return m, parent, token
}

func startBudgetChild(t *testing.T, s *Service, childID, agentID string) (*mission.Subtask, int64) {
	t.Helper()
	if _, err := s.ScheduleOnce(ctx); err != nil {
		t.Fatalf("ScheduleOnce: %v", err)
	}
	offers, err := s.ListOffers(ctx, agentID)
	if err != nil {
		t.Fatalf("ListOffers: %v", err)
	}
	for _, offer := range offers {
		if offer.ID != childID {
			continue
		}
		token := fenceOf(t, s, offer.LeaseID)
		accepted, err := s.AcceptLease(ctx, offer.LeaseID, token, offer.Version, agentID)
		if err != nil {
			t.Fatalf("AcceptLease: %v", err)
		}
		running, err := s.StartSubtask(ctx, childID, token, accepted.Version, agentID)
		if err != nil {
			t.Fatalf("StartSubtask: %v", err)
		}
		return running, token
	}
	t.Fatalf("offer for child %s not found: %+v", childID, offers)
	return nil, 0
}

func TestBudgetHoldSettleAndIdempotency(t *testing.T) {
	s, _, _ := newService()
	m, parent, token := setupBudgetLead(t, s, 100)

	first, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "budget-first", &DelegateSpec{
		Name: "first", RequiredSkills: []string{"work"}, BudgetTokens: 60,
	}))
	if err != nil {
		t.Fatalf("first delegate: %v", err)
	}
	if _, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "budget-retry-key", &DelegateSpec{
		Name: "too-large", RequiredSkills: []string{"work"}, BudgetTokens: 50,
	})); !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("over budget delegate = %v", err)
	}
	account, holds, err := s.GetMissionBudget(ctx, m.ID)
	if err != nil || account.Held != 60 || account.Available != 40 || len(holds) != 1 {
		t.Fatalf("after hold account=%+v holds=%+v err=%v", account, holds, err)
	}

	mustRegister(t, s, "agt_worker", 1, "work")
	running, workerToken := startBudgetChild(t, s, first.SubtaskID, "agt_worker")
	if _, err := s.CompleteSubtaskWithUsage(ctx, running.ID, workerToken, "budget-complete",
		"artifact://first", 35, running.Version, "agt_worker"); err != nil {
		t.Fatalf("complete with usage: %v", err)
	}
	account, holds, _ = s.GetMissionBudget(ctx, m.ID)
	if account.Held != 0 || account.Spent != 35 || account.Available != 65 ||
		holds[0].Status != store.BudgetHoldSettled || holds[0].Actual != 35 {
		t.Fatalf("settled account=%+v holds=%+v", account, holds)
	}
	if _, err := s.CompleteSubtaskWithUsage(ctx, running.ID, workerToken, "budget-complete",
		"artifact://first", 99, 999, "agt_worker"); !errors.Is(err, store.ErrDuplicate) {
		t.Fatalf("duplicate completion = %v", err)
	}
	account, _, _ = s.GetMissionBudget(ctx, m.ID)
	if account.Spent != 35 || account.Held != 0 {
		t.Fatalf("duplicate changed budget: %+v", account)
	}

	// 失败请求没有消耗幂等键；释放出的 65 tokens 可用同键完整预占。
	second, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "budget-retry-key", &DelegateSpec{
		Name: "second", RequiredSkills: []string{"work"}, BudgetTokens: 65,
	}))
	if err != nil {
		t.Fatalf("retry delegate after budget rejection: %v", err)
	}
	running, workerToken = startBudgetChild(t, s, second.SubtaskID, "agt_worker")
	if _, err := s.CompleteSubtaskWithUsage(ctx, running.ID, workerToken, "budget-over-actual",
		"artifact://second", 66, running.Version, "agt_worker"); !errors.Is(err, store.ErrBudgetExceeded) {
		t.Fatalf("actual overrun = %v", err)
	}
	if cur := mustGet(t, s, m.ID, running.ID); cur.State != mission.StateRunning {
		t.Fatalf("overrun must roll back completion, state=%s", cur.State)
	}
	account, _, _ = s.GetMissionBudget(ctx, m.ID)
	if account.Held != 65 || account.Spent != 35 {
		t.Fatalf("overrun must roll back budget: %+v", account)
	}
	if _, err := s.CompleteSubtaskWithUsage(ctx, running.ID, workerToken, "budget-over-actual",
		"artifact://second", 65, running.Version, "agt_worker"); err != nil {
		t.Fatalf("completion at hard cap: %v", err)
	}
	account, _, _ = s.GetMissionBudget(ctx, m.ID)
	if account.Available != 0 || account.Held != 0 || account.Spent != 100 {
		t.Fatalf("hard cap account=%+v", account)
	}
	events, err := s.ListMissionEvents(ctx, m.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Type]++
	}
	if counts["budget.held"] != 2 || counts["budget.settled"] != 2 {
		t.Fatalf("budget lifecycle events=%v", counts)
	}
}

func TestBudgetConcurrentDelegateCannotOverspend(t *testing.T) {
	s, _, _ := newService()
	m, parent, token := setupBudgetLead(t, s, 100)

	start := make(chan struct{})
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, name := range []string{"a", "b"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			<-start
			_, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "concurrent-"+name, &DelegateSpec{
				Name: name, BudgetTokens: 60,
			}))
			errs <- err
		}(name)
	}
	close(start)
	wg.Wait()
	close(errs)
	succeeded, exceeded := 0, 0
	for err := range errs {
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, store.ErrBudgetExceeded):
			exceeded++
		default:
			t.Fatalf("unexpected delegate result: %v", err)
		}
	}
	account, holds, _ := s.GetMissionBudget(ctx, m.ID)
	if succeeded != 1 || exceeded != 1 || account.Held != 60 || len(holds) != 1 {
		t.Fatalf("succeeded=%d exceeded=%d account=%+v holds=%+v", succeeded, exceeded, account, holds)
	}
}

func TestBudgetRetryFinalFailureAndCancelRelease(t *testing.T) {
	s, _, _ := newService()
	m, parent, token := setupBudgetLead(t, s, 100)
	mustRegister(t, s, "agt_worker", 1, "work")

	child, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "failure-hold", &DelegateSpec{
		Name: "retry", RequiredSkills: []string{"work"}, BudgetTokens: 40, MaxAttempts: 1,
	}))
	if err != nil {
		t.Fatal(err)
	}
	running, workerToken := startBudgetChild(t, s, child.SubtaskID, "agt_worker")
	if _, err := s.FailSubtask(ctx, running.ID, workerToken, "retryable", running.Version, "agt_worker"); err != nil {
		t.Fatalf("retryable failure: %v", err)
	}
	account, _, _ := s.GetMissionBudget(ctx, m.ID)
	if account.Held != 40 {
		t.Fatalf("retryable failure released hold: %+v", account)
	}
	running, workerToken = startBudgetChild(t, s, child.SubtaskID, "agt_worker")
	if _, err := s.FailSubtask(ctx, running.ID, workerToken, "final", running.Version, "agt_worker"); err != nil {
		t.Fatalf("final failure: %v", err)
	}
	account, holds, _ := s.GetMissionBudget(ctx, m.ID)
	if account.Held != 0 || account.Available != 100 || holds[0].Status != store.BudgetHoldReleased {
		t.Fatalf("final failure did not release: account=%+v holds=%+v", account, holds)
	}

	second, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "cancel-hold", &DelegateSpec{
		Name: "cancelled", BudgetTokens: 50,
	}))
	if err != nil || second.SubtaskID == "" {
		t.Fatalf("second delegate: %+v %v", second, err)
	}
	if err := s.CancelMission(ctx, m.ID, "u1"); err != nil {
		t.Fatalf("CancelMission: %v", err)
	}
	account, holds, _ = s.GetMissionBudget(ctx, m.ID)
	allReleased := len(holds) == 2
	for _, hold := range holds {
		allReleased = allReleased && hold.Status == store.BudgetHoldReleased
	}
	if account.Held != 0 || account.Available != 100 || !allReleased {
		t.Fatalf("cancel did not release: account=%+v holds=%+v", account, holds)
	}
}

func TestBudgetedMissionRequiresDelegateSlice(t *testing.T) {
	s, _, _ := newService()
	_, parent, token := setupBudgetLead(t, s, 100)
	if _, err := s.SubmitIntent(ctx, delegateIntent(parent, token, "missing-budget", &DelegateSpec{
		Name: "unmetered-bypass",
	})); !errors.Is(err, store.ErrBudgetRequired) {
		t.Fatalf("missing slice = %v", err)
	}
}
