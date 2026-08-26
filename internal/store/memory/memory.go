// Package memory 提供 Store 的内存实现。
//
// 用途：单元测试（配合 clock.Fake 做确定性测试）、本地零依赖运行、
// e2e 测试。语义与 pg 实现对齐：单 mutex 串行化等价于 SKIP LOCKED 的
// 并发安全；fencing token 由内存序列单调分配。
package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

type Store struct {
	mu sync.Mutex

	missions    map[string]*mission.Mission
	subtasks    map[string]*mission.Subtask // by id
	agents      map[string]*store.Agent
	leases      map[string]*store.Lease // by id
	events      []*store.Event
	idem        map[string]string // key → result
	decisions   map[string]*store.Decision
	board       map[string]*store.BoardEntry // missionID/ns/key
	artifacts   map[string]*store.Artifact
	leadInbox   map[string]*store.LeadInboxItem
	budgets     map[string]*store.BudgetAccount    // by mission id; missing means unmetered
	budgetHolds map[string]*store.BudgetHold       // by subtask id
	contexts    map[string]*store.ContextPackage   // by lease id
	quality     map[string]*store.QualityRecord    // by artifact id
	appeals     map[string]*store.QualityAppeal    // by appeal id
	reputations map[string]*store.ReputationRecord // agent id + NUL + skill
	repSignals  map[string]struct{}
	meters      map[string]*store.MeterRecord

	eventSeq   int64
	fencingSeq int64
	leaseSeq   int64
}

func New() *Store {
	return &Store{
		missions:    map[string]*mission.Mission{},
		subtasks:    map[string]*mission.Subtask{},
		agents:      map[string]*store.Agent{},
		leases:      map[string]*store.Lease{},
		idem:        map[string]string{},
		decisions:   map[string]*store.Decision{},
		board:       map[string]*store.BoardEntry{},
		artifacts:   map[string]*store.Artifact{},
		leadInbox:   map[string]*store.LeadInboxItem{},
		budgets:     map[string]*store.BudgetAccount{},
		budgetHolds: map[string]*store.BudgetHold{},
		contexts:    map[string]*store.ContextPackage{},
		quality:     map[string]*store.QualityRecord{},
		appeals:     map[string]*store.QualityAppeal{},
		reputations: map[string]*store.ReputationRecord{},
		repSignals:  map[string]struct{}{},
		meters:      map[string]*store.MeterRecord{},
	}
}

func (s *Store) Ping(ctx context.Context) error {
	return ctx.Err()
}

// ---- 任务面 ----

func (s *Store) CreateMission(_ context.Context, m *mission.Mission, subs []*mission.Subtask, actor store.Actor, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.missions[m.ID]; ok {
		return store.ErrConflict
	}
	cp := *m
	s.missions[m.ID] = &cp
	if m.BudgetTokens > 0 {
		s.budgets[m.ID] = &store.BudgetAccount{
			MissionID: m.ID, Metered: true, Total: m.BudgetTokens,
			Available: m.BudgetTokens, UpdatedAt: now,
		}
	}
	s.appendEventLocked(m.ID, m.ID, "mission.created", map[string]any{
		"goal": m.Goal, "owner": m.Owner, "budget_tokens": m.BudgetTokens,
	}, actor, now)
	for _, sub := range subs {
		c := *sub
		s.subtasks[sub.ID] = &c
		s.appendEventLocked(sub.ID, m.ID, string(mission.EvCreated), map[string]any{"kind": string(sub.Kind)}, actor, now)
	}
	return nil
}

func (s *Store) GetMission(_ context.Context, id string) (*mission.Mission, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.missions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *m
	return &cp, nil
}

func (s *Store) GetMissionBudget(_ context.Context, missionID string) (*store.BudgetAccount, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.missions[missionID]; !ok {
		return nil, store.ErrNotFound
	}
	account, ok := s.budgets[missionID]
	if !ok {
		return &store.BudgetAccount{MissionID: missionID}, nil
	}
	cp := *account
	cp.Available = cp.Total - cp.Held - cp.Spent
	return &cp, nil
}

func (s *Store) ListBudgetHolds(_ context.Context, missionID string) ([]*store.BudgetHold, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.missions[missionID]; !ok {
		return nil, store.ErrNotFound
	}
	var out []*store.BudgetHold
	for _, hold := range s.budgetHolds {
		if hold.MissionID == missionID {
			cp := *hold
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *Store) GetSubtask(_ context.Context, id string) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	cp := *sub
	return &cp, nil
}

func (s *Store) ListSubtasks(_ context.Context, missionID string) ([]*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*mission.Subtask
	for _, sub := range s.subtasks {
		if sub.MissionID == missionID {
			c := *sub
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) ListSubtasksByState(_ context.Context, st mission.State) ([]*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*mission.Subtask
	for _, sub := range s.subtasks {
		if sub.State == st {
			c := *sub
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) SetMissionStatus(_ context.Context, id string, st mission.MissionStatus, expectedVersion int64, actor store.Actor, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.missions[id]
	if !ok {
		return store.ErrNotFound
	}
	if m.Version != expectedVersion {
		return store.ErrConflict
	}
	m.Status = st
	m.Version++
	s.appendEventLocked(m.ID, m.ID, "mission."+lowerStatus(st), map[string]any{"status": string(st)}, actor, now)
	return nil
}

func lowerStatus(st mission.MissionStatus) string {
	switch st {
	case mission.MissionSucceeded:
		return "succeeded"
	case mission.MissionFailed:
		return "failed"
	case mission.MissionCancelled:
		return "cancelled"
	}
	return "active"
}

func (s *Store) TransitionSubtask(_ context.Context, id string, ev mission.EventType, expectedVersion int64,
	actor store.Actor, payload map[string]any, now time.Time,
	mutate func(*mission.Subtask) error) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	next, err := mission.Apply(sub.State, ev)
	if err != nil {
		return nil, store.ErrConflict
	}
	sub.State = next
	if mutate != nil {
		if err := mutate(sub); err != nil {
			return nil, err
		}
	}
	sub.Version++
	if payload == nil {
		payload = map[string]any{}
	}
	payload["state"] = string(next)
	s.appendEventLocked(sub.ID, sub.MissionID, string(ev), payload, actor, now)
	c := *sub
	return &c, nil
}

// ---- 就绪队列 ----

func (s *Store) DequeueReady(_ context.Context, limit int) ([]*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ready []*mission.Subtask
	for _, sub := range s.subtasks {
		if sub.State == mission.StateReady {
			c := *sub
			ready = append(ready, &c)
		}
	}
	// priority 降序；deadline 升序（nil 最后）；稳定次序按 ID
	sort.Slice(ready, func(i, j int) bool {
		a, b := ready[i], ready[j]
		if a.Scheduling.Priority != b.Scheduling.Priority {
			return a.Scheduling.Priority > b.Scheduling.Priority
		}
		ad, bd := a.Scheduling.Deadline, b.Scheduling.Deadline
		if ad != nil && bd != nil && !ad.Equal(*bd) {
			return ad.Before(*bd)
		}
		if (ad == nil) != (bd == nil) {
			return ad != nil
		}
		return a.ID < b.ID
	})
	if len(ready) > limit {
		ready = ready[:limit]
	}
	return ready, nil
}

// ---- Agent 注册 ----

func (s *Store) UpsertAgent(_ context.Context, a *store.Agent, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a.LastHeartbeat = now
	s.agents[a.ID] = cloneAgent(a)
	return nil
}

func (s *Store) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return cloneAgent(a), nil
}

func (s *Store) ListAgents(_ context.Context) ([]*store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Agent
	for _, a := range s.agents {
		out = append(out, cloneAgent(a))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func cloneAgent(a *store.Agent) *store.Agent {
	c := *a
	c.Endpoint = cloneStringMap(a.Endpoint)
	c.Capabilities = append([]store.Capability(nil), a.Capabilities...)
	c.TriggerScopes = append([]string(nil), a.TriggerScopes...)
	c.Reputation = map[string]*store.ReputationRecord{}
	for skill, rep := range a.Reputation {
		rc := *rep
		c.Reputation[skill] = &rc
	}
	return &c
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (s *Store) HeartbeatAgent(_ context.Context, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return store.ErrNotFound
	}
	a.LastHeartbeat = now
	// 心跳即存活证明：suspect 自动恢复（down 为人工/熔断标记，不自动恢复）
	if a.Health == "" || a.Health == "suspect" {
		a.Health = "healthy"
	}
	return nil
}

func (s *Store) MarkAgentHealth(_ context.Context, id, health string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return store.ErrNotFound
	}
	a.Health = health
	return nil
}

func (s *Store) ListReputations(_ context.Context, agentID string) ([]*store.ReputationRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[agentID]; !ok {
		return nil, store.ErrNotFound
	}
	var out []*store.ReputationRecord
	for _, rep := range s.reputations {
		if rep.AgentID == agentID {
			cp := *rep
			cp.RefreshScores()
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Skill < out[j].Skill })
	return out, nil
}

func (s *Store) ApplyReputationSignal(_ context.Context, sig store.ReputationSignal, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.applyReputationSignalLocked(sig, now)
}

func (s *Store) applyReputationSignalLocked(sig store.ReputationSignal, now time.Time) error {
	if _, ok := s.repSignals[sig.ID]; ok {
		return store.ErrDuplicate
	}
	if _, ok := s.agents[sig.AgentID]; !ok {
		return store.ErrNotFound
	}
	key := sig.AgentID + "\x00" + sig.Skill
	rep := s.reputations[key]
	if rep == nil {
		rep = store.NewReputation(sig.AgentID, sig.Skill)
		s.reputations[key] = rep
	}
	store.ApplyReputationSignal(rep, sig, now)
	s.repSignals[sig.ID] = struct{}{}
	return nil
}

// ---- 租约 ----

func (s *Store) OfferLease(_ context.Context, subtaskID, agentID string, expectedVersion int64,
	ttl time.Duration, actor store.Actor, now time.Time) (*store.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[subtaskID]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if _, err := mission.Apply(sub.State, mission.EvLeaseOffered); err != nil {
		return nil, store.ErrConflict
	}
	// 同一子任务不得存在活跃租约（等价于 pg 的唯一部分索引兜底）
	for _, l := range s.leases {
		if l.SubtaskID == subtaskID && l.State == store.LeaseActive {
			return nil, store.ErrConflict
		}
	}
	s.fencingSeq++
	s.leaseSeq++
	lease := &store.Lease{
		ID:           leaseID(s.leaseSeq),
		SubtaskID:    subtaskID,
		AgentID:      agentID,
		FencingToken: s.fencingSeq,
		ExpiresAt:    now.Add(ttl),
		State:        store.LeaseActive,
		CreatedAt:    now,
	}
	pkg, err := s.buildContextPackageLocked(lease.ID, sub, now)
	if err != nil {
		return nil, err
	}
	s.leases[lease.ID] = lease
	s.contexts[lease.ID] = pkg
	sub.State = mission.StateOffered
	sub.Assignee = agentID
	sub.LeaseID = lease.ID
	sub.Version++
	if a, ok := s.agents[agentID]; ok {
		a.Running++
	}
	s.appendEventLocked(subtaskID, sub.MissionID, string(mission.EvLeaseOffered), map[string]any{
		"state":         string(mission.StateOffered),
		"agent_id":      agentID,
		"lease_id":      lease.ID,
		"fencing_token": lease.FencingToken,
	}, actor, now)
	s.appendEventLocked(subtaskID, sub.MissionID, "context.materialized", map[string]any{
		"context_package_id": pkg.ID, "lease_id": lease.ID, "snapshot_hash": pkg.SnapshotHash,
	}, store.Actor{Kind: "system", ID: "context-builder"}, now)
	c := *lease
	return &c, nil
}

func (s *Store) buildContextPackageLocked(leaseID string, sub *mission.Subtask, now time.Time) (*store.ContextPackage, error) {
	var artifacts []*store.Artifact
	for _, id := range sub.Grants.ArtifactRefs {
		if artifact := s.artifacts[id]; artifact != nil {
			artifacts = append(artifacts, artifact)
		}
	}
	views := map[string]*store.ContextBoardEntry{}
	for _, grant := range sub.Grants.BoardViews {
		allowed := map[string]struct{}{}
		for _, key := range grant.Keys {
			allowed[key] = struct{}{}
		}
		for _, entry := range s.board {
			if entry.MissionID != sub.MissionID || entry.Namespace != grant.Namespace {
				continue
			}
			if len(allowed) > 0 {
				if _, ok := allowed[entry.Key]; !ok {
					continue
				}
			}
			key := entry.Namespace + "\x00" + entry.Key
			current := views[key]
			if current == nil || current.Mode == mission.BoardModeReadOnly && grant.Mode == mission.BoardModeReadWrite {
				views[key] = &store.ContextBoardEntry{Namespace: entry.Namespace, Key: entry.Key,
					Value: append([]byte(nil), entry.Value...), Version: entry.Version, Mode: grant.Mode}
			}
		}
	}
	board := make([]*store.ContextBoardEntry, 0, len(views))
	for _, view := range views {
		board = append(board, view)
	}
	var decisions []*store.Decision
	for _, decision := range s.decisions {
		if decision.MissionID == sub.MissionID && decision.SubtaskID == sub.ID {
			decisions = append(decisions, decision)
		}
	}
	var budget *store.BudgetAccount
	if current := s.budgets[sub.MissionID]; current != nil {
		cp := *current
		cp.Available = cp.Total - cp.Held - cp.Spent
		budget = &cp
	}
	return store.BuildContextPackage(leaseID, sub, artifacts, board, decisions, budget, now)
}

func (s *Store) AcceptLease(_ context.Context, leaseID string, fencingToken int64, expectedSubVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[leaseID]
	if !ok {
		return nil, store.ErrNotFound
	}
	if l.State != store.LeaseActive || l.FencingToken != fencingToken {
		return nil, store.ErrFenced
	}
	sub := s.subtasks[l.SubtaskID]
	if sub == nil {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedSubVersion {
		return nil, store.ErrConflict
	}
	if _, err := mission.Apply(sub.State, mission.EvLeaseAccepted); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateLeased
	sub.Version++
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvLeaseAccepted), map[string]any{
		"state":    string(mission.StateLeased),
		"lease_id": leaseID,
	}, actor, now)
	c := *sub
	return &c, nil
}

func (s *Store) GetLease(_ context.Context, id string) (*store.Lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	c := *l
	return &c, nil
}

func (s *Store) GetContextPackage(_ context.Context, leaseID string) (*store.ContextPackage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pkg := s.contexts[leaseID]
	if pkg == nil {
		return nil, store.ErrNotFound
	}
	raw, err := json.Marshal(pkg)
	if err != nil {
		return nil, err
	}
	var cp store.ContextPackage
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func (s *Store) RenewLease(_ context.Context, leaseID string, fencingToken int64, ttl time.Duration, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, ok := s.leases[leaseID]
	if !ok {
		return store.ErrNotFound
	}
	if l.State != store.LeaseActive || l.FencingToken != fencingToken {
		return store.ErrFenced
	}
	l.ExpiresAt = now.Add(ttl)
	return nil
}

func (s *Store) ExpireLeases(_ context.Context, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, l := range s.leases {
		if l.State != store.LeaseActive || l.ExpiresAt.After(now) {
			continue
		}
		sub := s.subtasks[l.SubtaskID]
		// RUNNING 租约由 Lead takeover/后续执行策略处理；不能先置 EXPIRED 导致永久卡死。
		if sub == nil || (sub.State != mission.StateOffered && sub.State != mission.StateLeased) {
			continue
		}
		l.State = store.LeaseExpired
		if a, ok := s.agents[l.AgentID]; ok && a.Running > 0 {
			a.Running--
		}
		sub.State = mission.StateReady
		sub.Assignee, sub.LeaseID = "", ""
		sub.Version++
		s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvLeaseExpired), map[string]any{
			"state":    string(mission.StateReady),
			"lease_id": l.ID,
		}, store.Actor{Kind: "system", ID: "lease-sweeper"}, now)
		n++
	}
	return n, nil
}

// ---- 执行回调 ----

// checkFencingLocked 校验子任务的活跃租约与 fencing token。
func (s *Store) checkFencingLocked(sub *mission.Subtask, fencingToken int64) (*store.Lease, error) {
	if sub.LeaseID == "" {
		return nil, store.ErrFenced
	}
	l, ok := s.leases[sub.LeaseID]
	if !ok || l.State != store.LeaseActive || l.FencingToken != fencingToken {
		return nil, store.ErrFenced
	}
	return l, nil
}

// releaseLeaseLocked 释放租约并回收 Agent 并发计数。
func (s *Store) releaseLeaseLocked(l *store.Lease) {
	l.State = store.LeaseReleased
	if a, ok := s.agents[l.AgentID]; ok && a.Running > 0 {
		a.Running--
	}
}

func (s *Store) StartSubtask(_ context.Context, id string, fencingToken int64, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if _, err := s.checkFencingLocked(sub, fencingToken); err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvStarted); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateRunning
	sub.Version++
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvStarted),
		map[string]any{"state": string(mission.StateRunning), "attempt": sub.Attempt}, actor, now)
	c := *sub
	return &c, nil
}

func (s *Store) CompleteSubtask(_ context.Context, id string, fencingToken int64, idemKey, resultRef string,
	expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	return s.completeSubtask(id, fencingToken, idemKey, resultRef, 0, expectedVersion, actor, now)
}

func (s *Store) CompleteSubtaskWithUsage(_ context.Context, id string, fencingToken int64, idemKey, resultRef string,
	usageTokens, expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	return s.completeSubtask(id, fencingToken, idemKey, resultRef, usageTokens, expectedVersion, actor, now)
}

func (s *Store) completeSubtask(id string, fencingToken int64, idemKey, resultRef string,
	usageTokens, expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	// 幂等优先于版本校验：重复上报直接返回原结果，保证重试安全（§4.3）
	if _, dup := s.idem[idemKey]; dup {
		c := *sub
		return &c, store.ErrDuplicate
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	l, err := s.checkFencingLocked(sub, fencingToken)
	if err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvCompleted); err != nil {
		return nil, store.ErrConflict
	}
	if err := s.settleBudgetHoldLocked(sub, usageTokens, actor, now); err != nil {
		return nil, err
	}
	sub.State = mission.StateSucceeded
	sub.ResultRef = resultRef
	sub.Version++
	s.idem[idemKey] = resultRef
	s.putMeterLocked(&store.MeterRecord{ID: "meter:lease:" + l.ID, MissionID: sub.MissionID,
		SubtaskID: sub.ID, AgentID: l.AgentID, Resource: "lease.wall_ms",
		Quantity: float64(now.Sub(l.CreatedAt).Milliseconds()), Trust: store.MeterAuthoritative}, now)
	if usageTokens > 0 {
		s.putMeterLocked(&store.MeterRecord{ID: "meter:token:" + sub.ID + ":" + idemKey,
			MissionID: sub.MissionID, SubtaskID: sub.ID, AgentID: l.AgentID,
			Resource: "token.reported", Quantity: float64(usageTokens), Trust: store.MeterSelfReported}, now)
	}
	s.releaseLeaseLocked(l)
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvCompleted), map[string]any{
		"state":      string(mission.StateSucceeded),
		"subtask_id": sub.ID, // M6：Lead event 唤醒以 where 谓词精确等待特定子女
		"result_ref": resultRef,
	}, actor, now)
	if sub.ParentID != "" {
		itemID := store.LeadInboxID(sub.ID)
		if _, exists := s.leadInbox[itemID]; !exists {
			s.leadInbox[itemID] = &store.LeadInboxItem{
				ID: itemID, MissionID: sub.MissionID, LeadSubtaskID: sub.ParentID,
				SourceSubtaskID: sub.ID, Kind: "result", ResultRef: resultRef,
				Status: store.LeadInboxPending, CreatedAt: now,
			}
			s.appendEventLocked(sub.ParentID, sub.MissionID, "lead.inbox.enqueued", map[string]any{
				"item_id": itemID, "source_subtask_id": sub.ID, "result_ref": resultRef,
			}, store.Actor{Kind: "system", ID: "lead-inbox"}, now)
		}
	}
	c := *sub
	return &c, nil
}

func (s *Store) settleBudgetHoldLocked(sub *mission.Subtask, usageTokens int64, actor store.Actor, now time.Time) error {
	hold := s.budgetHolds[sub.ID]
	if hold == nil || hold.Status != store.BudgetHoldHeld {
		return nil
	}
	if usageTokens < 0 {
		return store.ErrBudgetExceeded
	}
	account := s.budgets[sub.MissionID]
	if account == nil {
		return store.ErrConflict
	}
	otherHeld := account.Held - hold.Amount
	if otherHeld < 0 || usageTokens > account.Total-account.Spent-otherHeld {
		return store.ErrBudgetExceeded
	}
	account.Held = otherHeld
	account.Spent += usageTokens
	account.Version++
	account.UpdatedAt = now
	account.Available = account.Total - account.Held - account.Spent
	hold.Actual = usageTokens
	hold.Status = store.BudgetHoldSettled
	hold.SettledAt = &now
	s.appendEventLocked(sub.ID, sub.MissionID, "budget.settled", map[string]any{
		"hold_id": hold.ID, "reserved_tokens": hold.Amount, "actual_tokens": usageTokens,
		"available_tokens": account.Available,
	}, actor, now)
	return nil
}

func (s *Store) releaseBudgetHoldLocked(sub *mission.Subtask, reason string, actor store.Actor, now time.Time) {
	hold := s.budgetHolds[sub.ID]
	if hold == nil || hold.Status != store.BudgetHoldHeld {
		return
	}
	account := s.budgets[sub.MissionID]
	if account == nil {
		return
	}
	account.Held -= hold.Amount
	account.Version++
	account.UpdatedAt = now
	account.Available = account.Total - account.Held - account.Spent
	hold.Status = store.BudgetHoldReleased
	hold.SettledAt = &now
	s.appendEventLocked(sub.ID, sub.MissionID, "budget.released", map[string]any{
		"hold_id": hold.ID, "amount_tokens": hold.Amount, "reason": reason,
		"available_tokens": account.Available,
	}, actor, now)
}

// ---- 主子委托（M6，§15.1） ----

// SpawnSubtask 原子完成：幂等去重 → 父任务 fencing + RUNNING 校验 → 子女插入并激活 READY。
func (s *Store) SpawnSubtask(_ context.Context, idemKey, parentID string, fencingToken, parentVersion int64,
	child *mission.Subtask, actor store.Actor, now time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 幂等优先：重复委托直接返回原子女（恰好一次，§4.3）
	if existing, dup := s.idem[idemKey]; dup {
		return existing, store.ErrDuplicate
	}
	parent, ok := s.subtasks[parentID]
	if !ok {
		return "", store.ErrNotFound
	}
	if parent.Version != parentVersion {
		return "", store.ErrConflict
	}
	lease, err := s.checkFencingLocked(parent, fencingToken)
	if err != nil {
		return "", err
	}
	if lease.AgentID != actor.ID || !lease.ExpiresAt.After(now) {
		return "", store.ErrFenced
	}
	if parent.State != mission.StateRunning {
		return "", store.ErrConflict // 只有 RUNNING 中的 Lead 能 delegate（§15.1 时序约束）
	}
	if !mission.PermissionEnvelopeSubset(parent.Grants, child.Grants) {
		return "", store.ErrPermissionExceeded
	}
	if _, exists := s.subtasks[child.ID]; exists {
		return "", store.ErrConflict
	}
	account := s.budgets[child.MissionID]
	if account != nil {
		amount := child.Scheduling.BudgetTokens
		if amount <= 0 {
			return "", store.ErrBudgetRequired
		}
		if account.Total-account.Held-account.Spent < amount {
			return "", store.ErrBudgetExceeded
		}
	}
	c := *child
	c.State = mission.StateReady
	c.Version = 1
	s.subtasks[child.ID] = &c
	s.idem[idemKey] = child.ID
	if account != nil {
		amount := child.Scheduling.BudgetTokens
		hold := &store.BudgetHold{
			ID: store.BudgetHoldID(child.ID), MissionID: child.MissionID, SubtaskID: child.ID,
			Attempt: child.Attempt, Amount: amount, Status: store.BudgetHoldHeld, CreatedAt: now,
		}
		s.budgetHolds[child.ID] = hold
		account.Held += amount
		account.Version++
		account.UpdatedAt = now
		account.Available = account.Total - account.Held - account.Spent
		s.appendEventLocked(child.ID, child.MissionID, "budget.held", map[string]any{
			"hold_id": hold.ID, "amount_tokens": amount, "available_tokens": account.Available,
		}, actor, now)
	}
	s.appendEventLocked(child.ID, child.MissionID, string(mission.EvCreated), map[string]any{
		"kind":              string(child.Kind),
		"parent_subtask_id": parentID,
		"rework_of":         child.ReworkOf,
	}, actor, now)
	s.appendEventLocked(child.ID, child.MissionID, string(mission.EvDepsSatisfied),
		map[string]any{"state": string(mission.StateReady)},
		store.Actor{Kind: "system", ID: "orchestrator"}, now)
	return "", nil
}

// CountChildren 直接子女计数（delegate fanout 校验）。
func (s *Store) CountChildren(_ context.Context, parentID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, sub := range s.subtasks {
		if sub.ParentID == parentID {
			n++
		}
	}
	return n, nil
}

func (s *Store) ListLeadInbox(_ context.Context, leadSubtaskID string, pendingOnly bool) ([]*store.LeadInboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.LeadInboxItem
	for _, item := range s.leadInbox {
		if item.LeadSubtaskID != leadSubtaskID || pendingOnly && item.Status != store.LeadInboxPending {
			continue
		}
		c := *item
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *Store) IngestLeadInbox(_ context.Context, itemID, leadSubtaskID string,
	fencingToken, expectedVersion int64, mode string, actor store.Actor, now time.Time) (*store.LeadInboxItem, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	item, ok := s.leadInbox[itemID]
	if !ok || item.LeadSubtaskID != leadSubtaskID {
		return nil, store.ErrNotFound
	}
	lead := s.subtasks[leadSubtaskID]
	if lead == nil {
		return nil, store.ErrNotFound
	}
	if lead.State != mission.StateRunning {
		return nil, store.ErrConflict
	}
	lease, err := s.checkFencingLocked(lead, fencingToken)
	if err != nil {
		return nil, err
	}
	if lease.AgentID != actor.ID || !lease.ExpiresAt.After(now) {
		return nil, store.ErrFenced
	}
	if item.Status != store.LeadInboxPending || item.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	item.Status, item.IngestMode, item.IngestedBy = store.LeadInboxIngested, mode, actor.ID
	item.IngestedAt = &now
	item.Version++
	s.appendEventLocked(lead.ID, lead.MissionID, "lead.inbox.ingested", map[string]any{
		"item_id": item.ID, "source_subtask_id": item.SourceSubtaskID, "mode": mode,
	}, actor, now)
	c := *item
	return &c, nil
}

func (s *Store) SaveLeadSnapshot(_ context.Context, leadSubtaskID string, fencingToken, expectedVersion int64,
	value []byte, leaseTTL time.Duration, actor store.Actor, now time.Time) (*store.BoardEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lead := s.subtasks[leadSubtaskID]
	if lead == nil {
		return nil, store.ErrNotFound
	}
	if lead.State != mission.StateRunning {
		return nil, store.ErrConflict
	}
	lease, err := s.checkFencingLocked(lead, fencingToken)
	if err != nil {
		return nil, err
	}
	if lease.AgentID != actor.ID || !lease.ExpiresAt.After(now) {
		return nil, store.ErrFenced
	}
	k := boardKey(lead.MissionID, "lead-plan", lead.ID)
	cur, exists := s.board[k]
	if expectedVersion == -1 {
		if exists {
			return nil, store.ErrConflict
		}
	} else if !exists || cur.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	version := int64(0)
	if exists {
		version = cur.Version + 1
	}
	entry := &store.BoardEntry{MissionID: lead.MissionID, Namespace: "lead-plan", Key: lead.ID,
		Value: append([]byte(nil), value...), Version: version, UpdatedAt: now}
	s.board[k] = entry
	lease.ExpiresAt = now.Add(leaseTTL)
	if agent := s.agents[actor.ID]; agent != nil {
		agent.LastHeartbeat = now
		if agent.Health == "suspect" {
			agent.Health = "healthy"
		}
	}
	s.appendEventLocked(lead.ID, lead.MissionID, "lead.snapshot.saved", map[string]any{
		"version": version,
	}, actor, now)
	c := *entry
	return &c, nil
}

func (s *Store) TakeoverStaleLeads(_ context.Context, now time.Time) ([]*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*mission.Subtask
	for _, lead := range s.subtasks {
		if lead.State != mission.StateRunning || lead.LeaseID == "" {
			continue
		}
		lease := s.leases[lead.LeaseID]
		if lease == nil || lease.State != store.LeaseActive || lease.ExpiresAt.After(now) {
			continue
		}
		isLead := s.board[boardKey(lead.MissionID, "lead-plan", lead.ID)] != nil
		if !isLead {
			for _, child := range s.subtasks {
				if child.ParentID == lead.ID {
					isLead = true
					break
				}
			}
		}
		if !isLead {
			continue
		}
		if _, err := mission.Apply(lead.State, mission.EvTakeover); err != nil {
			return out, store.ErrConflict
		}
		lease.State = store.LeaseFenced
		if agent := s.agents[lease.AgentID]; agent != nil {
			if agent.Running > 0 {
				agent.Running--
			}
			if agent.Health != "down" {
				agent.Health = "suspect"
			}
		}
		oldAgent, oldLease := lead.Assignee, lead.LeaseID
		lead.State, lead.Assignee, lead.LeaseID = mission.StateReady, "", ""
		lead.Version++
		s.appendEventLocked(lead.ID, lead.MissionID, string(mission.EvTakeover), map[string]any{
			"state": string(mission.StateReady), "fenced_lease_id": oldLease, "previous_agent_id": oldAgent,
		}, store.Actor{Kind: "system", ID: "lead-takeover"}, now)
		c := *lead
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) FailSubtask(_ context.Context, id string, fencingToken int64, reason string,
	expectedVersion int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	l, err := s.checkFencingLocked(sub, fencingToken)
	if err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvFailed); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateFailed
	sub.Version++
	s.putMeterLocked(&store.MeterRecord{ID: "meter:lease:" + l.ID, MissionID: sub.MissionID,
		SubtaskID: sub.ID, AgentID: l.AgentID, Resource: "lease.wall_ms",
		Quantity: float64(now.Sub(l.CreatedAt).Milliseconds()), Trust: store.MeterAuthoritative}, now)
	s.releaseLeaseLocked(l)
	if sub.Attempt >= sub.Retry.MaxAttempts {
		s.releaseBudgetHoldLocked(sub, "final_failure", actor, now)
	}
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvFailed), map[string]any{
		"state":  string(mission.StateFailed),
		"reason": reason,
	}, actor, now)
	c := *sub
	return &c, nil
}

// ---- 事件 ----

func (s *Store) ListMissionEvents(_ context.Context, missionID string, afterSeq int64, limit int) ([]*store.Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Event
	for _, e := range s.events {
		if e.MissionID == missionID && e.Seq > afterSeq {
			c := *e
			out = append(out, &c)
			if len(out) >= limit {
				break
			}
		}
	}
	return out, nil
}

func (s *Store) BlockSubtask(_ context.Context, id string, fencingToken int64, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if _, err := s.checkFencingLocked(sub, fencingToken); err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvBlocked); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateBlocked
	sub.Version++
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvBlocked),
		map[string]any{"state": string(mission.StateBlocked)}, actor, now)
	c := *sub
	return &c, nil
}

func (s *Store) CancelSubtask(_ context.Context, id string, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	if _, err := mission.Apply(sub.State, mission.EvCancelled); err != nil {
		return nil, store.ErrConflict
	}
	if sub.LeaseID != "" {
		if l, ok := s.leases[sub.LeaseID]; ok && l.State == store.LeaseActive {
			s.releaseLeaseLocked(l)
		}
	}
	sub.State = mission.StateCancelled
	sub.Assignee, sub.LeaseID = "", ""
	sub.Version++
	s.releaseBudgetHoldLocked(sub, "cancelled", actor, now)
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvCancelled),
		map[string]any{"state": string(mission.StateCancelled)}, actor, now)
	c := *sub
	return &c, nil
}

// ---- 挂起-唤醒（M3） ----

func (s *Store) SuspendSubtask(_ context.Context, id string, fencingToken int64, expectedVersion int64,
	wake *mission.WakeSpec, checkpoint []byte,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict
	}
	l, err := s.checkFencingLocked(sub, fencingToken)
	if err != nil {
		return nil, err
	}
	if _, err := mission.Apply(sub.State, mission.EvSuspended); err != nil {
		return nil, store.ErrConflict
	}
	spec, err := json.Marshal(wake)
	if err != nil {
		return nil, err
	}
	sub.State = mission.StateWaiting
	sub.WakeKind, sub.WakeAt, sub.WakeDeadline = wake.Kind, wake.At, wake.Deadline
	sub.WakeSpec = spec
	if len(checkpoint) > 0 {
		sub.Checkpoint = append([]byte(nil), checkpoint...)
	}
	sub.Version++
	s.releaseLeaseLocked(l)
	sub.Assignee, sub.LeaseID = "", ""
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvSuspended), map[string]any{
		"state":     string(mission.StateWaiting),
		"wake_kind": wake.Kind,
	}, actor, now)
	c := *sub
	return &c, nil
}

func (s *Store) WakeSubtask(_ context.Context, id string, expectedVersion int64,
	actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedVersion {
		return nil, store.ErrConflict // CAS 竞争：另一 sweeper/wake 已处理
	}
	if sub.WakeDeadline != nil && !sub.WakeDeadline.After(now) {
		return nil, store.ErrConflict
	}
	if _, err := mission.Apply(sub.State, mission.EvWoken); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateReady
	sub.WakeKind, sub.WakeAt, sub.WakeDeadline, sub.WakeSpec = "", nil, nil, nil // 一次性注册，清空
	sub.Version++
	s.putMeterLocked(&store.MeterRecord{ID: "meter:wake:" + sub.ID + ":" + fmt.Sprint(sub.Version),
		MissionID: sub.MissionID, SubtaskID: sub.ID, Resource: "wake.fire", Quantity: 1,
		Trust: store.MeterAuthoritative}, now)
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvWoken),
		map[string]any{"state": string(mission.StateReady)}, actor, now)
	c := *sub
	return &c, nil
}

func (s *Store) ListWaitingDue(_ context.Context, now time.Time) ([]*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*mission.Subtask
	for _, sub := range s.subtasks {
		if sub.State == mission.StateWaiting && sub.WakeKind == mission.WakeTimer &&
			sub.WakeAt != nil && !sub.WakeAt.After(now) &&
			(sub.WakeDeadline == nil || sub.WakeDeadline.After(now)) {
			c := *sub
			out = append(out, &c)
		}
	}
	return out, nil
}

func (s *Store) ExpireWakes(_ context.Context, now time.Time) ([]*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*mission.Subtask
	for _, sub := range s.subtasks {
		if sub.State != mission.StateWaiting || sub.WakeDeadline == nil || sub.WakeDeadline.After(now) {
			continue
		}
		sub.State = mission.StateFailed
		sub.WakeKind, sub.WakeAt, sub.WakeDeadline, sub.WakeSpec = "", nil, nil, nil
		sub.Version++
		s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvFailed), map[string]any{
			"state":  string(mission.StateFailed),
			"reason": "wake_timeout",
		}, store.Actor{Kind: "system", ID: "sweeper"}, now)
		c := *sub
		out = append(out, &c)
	}
	return out, nil
}

func (s *Store) SaveCheckpoint(_ context.Context, id string, fencingToken int64, checkpoint []byte, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subtasks[id]
	if !ok {
		return store.ErrNotFound
	}
	if _, err := s.checkFencingLocked(sub, fencingToken); err != nil {
		return err
	}
	if sub.State != mission.StateRunning && sub.State != mission.StateBlocked {
		return store.ErrConflict
	}
	sub.Checkpoint = append([]byte(nil), checkpoint...)
	return nil
}

func (s *Store) ListWaiting(_ context.Context, wakeKind string) ([]*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*mission.Subtask
	for _, sub := range s.subtasks {
		if sub.State == mission.StateWaiting && sub.WakeKind == wakeKind {
			c := *sub
			out = append(out, &c)
		}
	}
	return out, nil
}

func (s *Store) MaxEventSeq(_ context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.eventSeq, nil
}

// ---- 幂等键 ----

func (s *Store) PutIdempotent(_ context.Context, key, result string, _ time.Time) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, dup := s.idem[key]; dup {
		return existing, store.ErrDuplicate
	}
	s.idem[key] = result
	return "", nil
}

// ---- 决策 ----

func (s *Store) CreateDecision(_ context.Context, d *store.Decision, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.decisions[d.ID]; ok {
		return store.ErrConflict
	}
	c := *d
	c.Status = store.DecisionPending
	c.CreatedAt = now
	s.decisions[d.ID] = &c
	s.appendEventLocked(d.SubtaskID, d.MissionID, "decision.requested",
		map[string]any{"decision_id": d.ID, "question": d.Question, "options": d.Options},
		store.Actor{Kind: "system", ID: "hitl"}, now)
	return nil
}

func (s *Store) CreateDecisionAndBlock(_ context.Context, d *store.Decision, expectedSubVersion int64,
	fencingToken *int64, actor store.Actor, now time.Time) (*mission.Subtask, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.decisions[d.ID]; ok {
		return nil, store.ErrConflict
	}
	sub, ok := s.subtasks[d.SubtaskID]
	if !ok {
		return nil, store.ErrNotFound
	}
	if sub.Version != expectedSubVersion {
		return nil, store.ErrConflict
	}
	if fencingToken != nil {
		if _, err := s.checkFencingLocked(sub, *fencingToken); err != nil {
			return nil, err
		}
	}
	if _, err := mission.Apply(sub.State, mission.EvBlocked); err != nil {
		return nil, store.ErrConflict
	}
	sub.State = mission.StateBlocked
	sub.Version++
	c := *d
	c.Status = store.DecisionPending
	c.CreatedAt = now
	s.decisions[d.ID] = &c
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvBlocked),
		map[string]any{"state": string(mission.StateBlocked), "question": d.Question}, actor, now)
	s.appendEventLocked(d.SubtaskID, d.MissionID, "decision.requested",
		map[string]any{"decision_id": d.ID, "question": d.Question, "options": d.Options}, actor, now)
	out := *sub
	return &out, nil
}

func (s *Store) GetDecision(_ context.Context, id string) (*store.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.decisions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	c := *d
	return &c, nil
}

func (s *Store) ListDecisions(_ context.Context, missionID string, pendingOnly bool) ([]*store.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Decision
	for _, d := range s.decisions {
		if missionID != "" && d.MissionID != missionID {
			continue
		}
		if pendingOnly && d.Status != store.DecisionPending {
			continue
		}
		c := *d
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *Store) ResolveDecision(_ context.Context, id, choice, rationale, deciderID string, now time.Time) (*store.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.decisions[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	if d.Status != store.DecisionPending {
		return nil, store.ErrConflict // 重复裁决/已过期
	}
	d.Status = store.DecisionResolved
	d.Choice = choice
	d.Rationale = rationale
	d.DeciderID = deciderID
	d.ResolvedAt = &now
	s.appendEventLocked(d.SubtaskID, d.MissionID, "decision.resolved",
		map[string]any{"decision_id": d.ID, "choice": choice, "decider": deciderID},
		store.Actor{Kind: "human", ID: deciderID}, now)
	c := *d
	return &c, nil
}

func (s *Store) ExpireDecisions(_ context.Context, now time.Time) ([]*store.Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Decision
	for _, d := range s.decisions {
		if d.Status != store.DecisionPending || d.Deadline == nil || d.Deadline.After(now) {
			continue
		}
		switch d.OnTimeout {
		case "auto_approve", "auto_reject":
			d.Status = store.DecisionResolved
			if d.OnTimeout == "auto_approve" {
				d.Choice = "approve"
			} else {
				d.Choice = "reject"
			}
			d.DeciderID = "system:timeout"
			d.ResolvedAt = &now
		default:
			d.Status = store.DecisionExpired
		}
		s.appendEventLocked(d.SubtaskID, d.MissionID, "decision.expired",
			map[string]any{"decision_id": d.ID, "on_timeout": d.OnTimeout, "choice": d.Choice},
			store.Actor{Kind: "system", ID: "decision-sweeper"}, now)
		c := *d
		out = append(out, &c)
	}
	return out, nil
}

// ---- 黑板 ----

func boardKey(missionID, ns, key string) string { return missionID + "/" + ns + "/" + key }

func (s *Store) BoardPut(_ context.Context, e *store.BoardEntry, expectedVersion int64, now time.Time) (*store.BoardEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := boardKey(e.MissionID, e.Namespace, e.Key)
	cur, exists := s.board[k]
	if expectedVersion >= 0 {
		var curVer int64 = -1
		if exists {
			curVer = cur.Version
		}
		if curVer != expectedVersion {
			return nil, store.ErrConflict
		}
	}
	var newVer int64 = 0
	if exists {
		newVer = cur.Version + 1
	}
	c := *e
	c.Version = newVer
	c.UpdatedAt = now
	s.board[k] = &c
	out := c
	return &out, nil
}

func (s *Store) BoardGet(_ context.Context, missionID, ns, key string) (*store.BoardEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.board[boardKey(missionID, ns, key)]
	if !ok {
		return nil, store.ErrNotFound
	}
	c := *e
	return &c, nil
}

func (s *Store) BoardList(_ context.Context, missionID, ns string) ([]*store.BoardEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := missionID + "/"
	if ns != "" {
		prefix += ns + "/"
	} // 空 ns = 全命名空间（M5：CEL wildcard 注册的 board 视图）
	var out []*store.BoardEntry
	for k, e := range s.board {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			c := *e
			out = append(out, &c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// ---- Artifact ----

func (s *Store) PutArtifact(_ context.Context, a *store.Artifact, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.artifacts[a.ID]; ok {
		return store.ErrConflict
	}
	c := *a
	c.CreatedAt = now
	s.artifacts[a.ID] = &c
	s.putMeterLocked(&store.MeterRecord{ID: "meter:artifact:" + a.ID, MissionID: a.MissionID,
		SubtaskID: a.ProducedBy, Resource: "artifact.byte", Quantity: float64(a.Size),
		Trust: store.MeterAuthoritative}, now)
	s.appendEventLocked(a.ProducedBy, a.MissionID, "artifact.produced",
		map[string]any{"artifact_id": a.ID, "sha256": a.SHA256, "size": a.Size},
		store.Actor{Kind: "system", ID: "artifact-store"}, now)
	return nil
}

func (s *Store) GetArtifact(_ context.Context, id string) (*store.Artifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.artifacts[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	c := *a
	return &c, nil
}

func (s *Store) RecordQuality(_ context.Context, q *store.QualityRecord, signals []store.ReputationSignal,
	actor store.Actor, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.artifacts[q.ArtifactID]; !ok {
		return store.ErrNotFound
	}
	if _, ok := s.quality[q.ArtifactID]; ok {
		return store.ErrDuplicate
	}
	for _, sig := range signals {
		if _, ok := s.agents[sig.AgentID]; !ok {
			return store.ErrNotFound
		}
	}
	q.CreatedAt = now
	cp, err := cloneQuality(q)
	if err != nil {
		return err
	}
	s.quality[q.ArtifactID] = cp
	for _, sig := range signals {
		if err := s.applyReputationSignalLocked(sig, now); err != nil && err != store.ErrDuplicate {
			return err
		}
	}
	s.putMeterLocked(&store.MeterRecord{ID: "meter:verify:" + q.ArtifactID, MissionID: q.MissionID,
		SubtaskID: q.SubtaskID, Resource: "verify.call", Quantity: 1,
		Trust: store.MeterAuthoritative}, now)
	s.appendEventLocked(q.ArtifactID, q.MissionID, "artifact.verified", map[string]any{
		"artifact_id": q.ArtifactID, "score": q.Score, "confidence": q.Confidence,
		"verdict": q.Verdict, "failure_class": q.FailureClass,
	}, actor, now)
	return nil
}

func (s *Store) GetQuality(_ context.Context, artifactID string) (*store.QualityRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.quality[artifactID]
	if q == nil {
		return nil, store.ErrNotFound
	}
	return cloneQuality(q)
}

func (s *Store) CreateQualityAppeal(_ context.Context, a *store.QualityAppeal, actor store.Actor, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.quality[a.ArtifactID]
	if q == nil {
		return store.ErrNotFound
	}
	if _, exists := s.appeals[a.ID]; exists {
		return store.ErrDuplicate
	}
	cp := *a
	cp.MissionID, cp.Status, cp.CreatedAt = q.MissionID, store.AppealPending, now
	cp.EvidenceRefs = append([]string(nil), a.EvidenceRefs...)
	s.appeals[a.ID] = &cp
	*a = cp
	s.appendEventLocked(a.ID, a.MissionID, "quality.appeal.created", map[string]any{
		"appeal_id": a.ID, "artifact_id": a.ArtifactID, "appellant_id": a.AppellantID,
	}, actor, now)
	return nil
}

func (s *Store) ListQualityAppeals(_ context.Context, missionID string, pendingOnly bool) ([]*store.QualityAppeal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.QualityAppeal
	for _, a := range s.appeals {
		if (missionID == "" || a.MissionID == missionID) && (!pendingOnly || a.Status == store.AppealPending) {
			cp := *a
			cp.EvidenceRefs = append([]string(nil), a.EvidenceRefs...)
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

func (s *Store) ResolveQualityAppeal(_ context.Context, id, status, resolution, reviewerID string,
	actor store.Actor, now time.Time) (*store.QualityAppeal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.appeals[id]
	if a == nil {
		return nil, store.ErrNotFound
	}
	if a.Status != store.AppealPending {
		return nil, store.ErrConflict
	}
	a.Status, a.Resolution, a.ReviewerID, a.ResolvedAt = status, resolution, reviewerID, &now
	s.appendEventLocked(a.ID, a.MissionID, "quality.appeal.resolved", map[string]any{
		"appeal_id": a.ID, "artifact_id": a.ArtifactID, "status": status,
	}, actor, now)
	cp := *a
	cp.EvidenceRefs = append([]string(nil), a.EvidenceRefs...)
	return &cp, nil
}

func cloneQuality(q *store.QualityRecord) (*store.QualityRecord, error) {
	raw, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	var cp store.QualityRecord
	if err := json.Unmarshal(raw, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}

func (s *Store) PutMeterRecord(_ context.Context, m *store.MeterRecord, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.meters[m.ID]; ok {
		return store.ErrDuplicate
	}
	s.putMeterLocked(m, now)
	return nil
}

func (s *Store) putMeterLocked(m *store.MeterRecord, now time.Time) {
	if _, ok := s.meters[m.ID]; ok {
		return
	}
	cp := *m
	cp.RecordedAt = now
	store.PriceMeter(&cp)
	s.meters[m.ID] = &cp
}

func (s *Store) ListMeterRecords(_ context.Context, missionID string) ([]*store.MeterRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.missions[missionID]; !ok {
		return nil, store.ErrNotFound
	}
	var out []*store.MeterRecord
	for _, m := range s.meters {
		if m.MissionID == missionID {
			cp := *m
			out = append(out, &cp)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RecordedAt.Equal(out[j].RecordedAt) {
			return out[i].ID < out[j].ID
		}
		return out[i].RecordedAt.Before(out[j].RecordedAt)
	})
	return out, nil
}

func (s *Store) appendEventLocked(aggregateID, missionID, typ string, payload map[string]any, actor store.Actor, now time.Time) {
	s.eventSeq++
	s.events = append(s.events, &store.Event{
		Seq:         s.eventSeq,
		AggregateID: aggregateID,
		MissionID:   missionID,
		Type:        typ,
		Payload:     payload,
		Actor:       actor,
		Ts:          now,
	})
}

// AppendEvent 追加独立事件（M5：condition.cost_exceeded 等平台留痕）。
func (s *Store) AppendEvent(_ context.Context, e *store.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendEventLocked(e.AggregateID, e.MissionID, e.Type, e.Payload, e.Actor, e.Ts)
	return nil
}

func leaseID(n int64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	var buf [8]byte
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[n%36]
		n /= 36
	}
	return "les_" + string(buf[:])
}
