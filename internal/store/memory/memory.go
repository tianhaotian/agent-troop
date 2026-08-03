// Package memory 提供 Store 的内存实现。
//
// 用途：单元测试（配合 clock.Fake 做确定性测试）、本地零依赖运行、
// e2e 测试。语义与 pg 实现对齐：单 mutex 串行化等价于 SKIP LOCKED 的
// 并发安全；fencing token 由内存序列单调分配。
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

type Store struct {
	mu sync.Mutex

	missions  map[string]*mission.Mission
	subtasks  map[string]*mission.Subtask // by id
	agents    map[string]*store.Agent
	leases    map[string]*store.Lease // by id
	events    []*store.Event
	idem      map[string]string // key → result
	decisions map[string]*store.Decision
	board     map[string]*store.BoardEntry // missionID/ns/key
	artifacts map[string]*store.Artifact

	eventSeq     int64
	fencingSeq   int64
	leaseSeq     int64
}

func New() *Store {
	return &Store{
		missions: map[string]*mission.Mission{},
		subtasks: map[string]*mission.Subtask{},
		agents:   map[string]*store.Agent{},
		leases:   map[string]*store.Lease{},
		idem:      map[string]string{},
		decisions: map[string]*store.Decision{},
		board:     map[string]*store.BoardEntry{},
		artifacts: map[string]*store.Artifact{},
	}
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
	s.appendEventLocked(m.ID, m.ID, "mission.created", map[string]any{"goal": m.Goal, "owner": m.Owner}, actor, now)
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
	c := *a
	s.agents[a.ID] = &c
	return nil
}

func (s *Store) GetAgent(_ context.Context, id string) (*store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	c := *a
	return &c, nil
}

func (s *Store) ListAgents(_ context.Context) ([]*store.Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*store.Agent
	for _, a := range s.agents {
		c := *a
		out = append(out, &c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (s *Store) HeartbeatAgent(_ context.Context, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return store.ErrNotFound
	}
	a.LastHeartbeat = now
	if a.Health == "" {
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
	}
	s.leases[lease.ID] = lease
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
	c := *lease
	return &c, nil
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
		l.State = store.LeaseExpired
		sub := s.subtasks[l.SubtaskID]
		if a, ok := s.agents[l.AgentID]; ok && a.Running > 0 {
			a.Running--
		}
		if sub != nil && (sub.State == mission.StateOffered || sub.State == mission.StateLeased) {
			sub.State = mission.StateReady
			sub.Assignee, sub.LeaseID = "", ""
			sub.Version++
			s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvLeaseExpired), map[string]any{
				"state":    string(mission.StateReady),
				"lease_id": l.ID,
			}, store.Actor{Kind: "system", ID: "lease-sweeper"}, now)
			n++
		}
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
	sub.State = mission.StateSucceeded
	sub.ResultRef = resultRef
	sub.Version++
	s.idem[idemKey] = resultRef
	s.releaseLeaseLocked(l)
	s.appendEventLocked(sub.ID, sub.MissionID, string(mission.EvCompleted), map[string]any{
		"state":      string(mission.StateSucceeded),
		"result_ref": resultRef,
	}, actor, now)
	c := *sub
	return &c, nil
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
	s.releaseLeaseLocked(l)
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
	prefix := missionID + "/" + ns + "/"
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

func leaseID(n int64) string {
	const digits = "0123456789abcdefghijklmnopqrstuvwxyz"
	var buf [8]byte
	for i := len(buf) - 1; i >= 0; i-- {
		buf[i] = digits[n%36]
		n /= 36
	}
	return "les_" + string(buf[:])
}
