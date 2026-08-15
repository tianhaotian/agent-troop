// Package core 是控制平面的业务服务层：Mission 创建与 DAG 校验、依赖就绪传播、
// Capability-First 放置调度、租约生命周期、Agent 注册与健康、失败重试与取消级联。
// 对应设计 §5（调度）、§5.1（编排循环）、§4.3（一致性）。
package core

import (
	"context"
	"crypto/rand" // ID 生成专用；业务逻辑的随机仍须走 clock.Rand（ADR-8）
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"agenttroop/internal/clock"
	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// Config 服务参数。
type Config struct {
	OfferTTL       time.Duration // OFFERED 租约寿命（Agent 须在此时间内 accept）
	ScheduleBatch  int           // 单轮调度拉取的就绪任务数
	HeartbeatStale time.Duration // 超过该时长无心跳标记 suspect
	// 主子委托（M6，§15.1 结构校验）
	MaxDelegateDepth  int // 委托链最大深度（沿 parent_id 上溯）
	MaxDelegateFanout int // 单父任务最大直接子女数
	MaxRework         int // rework 链上限（到达后 Lead 应换方案/升级人决策）
}

func DefaultConfig() Config {
	return Config{OfferTTL: 30 * time.Second, ScheduleBatch: 100, HeartbeatStale: 90 * time.Second,
		MaxDelegateDepth: 4, MaxDelegateFanout: 8, MaxRework: 3}
}

type Service struct {
	st       store.Store
	clk      clock.Clock
	cfg      Config
	blob     BlobStore
	strategy PlacementStrategy
}

func New(st store.Store, clk clock.Clock, cfg Config) *Service {
	return &Service{st: st, clk: clk, cfg: cfg, blob: NewMemBlob(), strategy: CapabilityFirst{}}
}

// WithStrategy 替换放置策略（M3-T1；cmd/troopd 由 TROOP_SCHEDULER 解析）。
func (s *Service) WithStrategy(ps PlacementStrategy) *Service {
	s.strategy = ps
	return s
}

var (
	ErrInvalidDAG = errors.New("core: invalid DAG")
	ErrNoAgent    = errors.New("core: no eligible agent")
)

// ---- ID 生成（crypto/rand，非业务决策，不受 ADR-8 确定性约束） ----

func newID(prefix string) string {
	var b [10]byte
	_, _ = rand.Read(b[:])
	return prefix + "_" + hex.EncodeToString(b[:])
}

// ---- Mission 创建与 DAG ----

// TaskSpec 提交 Mission 时的子任务声明（M1：DAG 由调用方完整给出，即 Workflow 模式）。
type TaskSpec struct {
	Name           string            `json:"name"` // Mission 内唯一节点名
	Kind           mission.Kind      `json:"kind"`
	RequiredSkills []string          `json:"required_skills,omitempty"`
	DependsOn      []string          `json:"depends_on,omitempty"` // 其他节点的 name
	Input          map[string]any    `json:"input,omitempty"`
	Priority       int               `json:"priority,omitempty"`
	Deadline       *time.Time        `json:"deadline,omitempty"`
	MaxAttempts    int               `json:"max_attempts,omitempty"` // 0 = 不重试
	// human 节点（M2）：工单内容与超时策略
	Question       string            `json:"question,omitempty"`
	Options        []string          `json:"options,omitempty"`
	OnTimeout      string            `json:"on_timeout,omitempty"` // auto_approve | auto_reject
}

// validateDAG 校验：节点名唯一、依赖存在、无环（Kahn 拓扑）。
func validateDAG(tasks []TaskSpec) error {
	seen := map[string]bool{}
	indegree := map[string]int{}
	for _, t := range tasks {
		if t.Name == "" || seen[t.Name] {
			return fmt.Errorf("%w: duplicate or empty node name %q", ErrInvalidDAG, t.Name)
		}
		seen[t.Name] = true
	}
	for _, t := range tasks {
		for _, d := range t.DependsOn {
			if !seen[d] {
				return fmt.Errorf("%w: %q depends on unknown node %q", ErrInvalidDAG, t.Name, d)
			}
			if d == t.Name {
				return fmt.Errorf("%w: self dependency %q", ErrInvalidDAG, t.Name)
			}
			indegree[t.Name]++
		}
	}
	// Kahn
	queue := []string{}
	for _, t := range tasks {
		if indegree[t.Name] == 0 {
			queue = append(queue, t.Name)
		}
	}
	visited := 0
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		visited++
		for _, t := range tasks {
			for _, d := range t.DependsOn {
				if d == n {
					indegree[t.Name]--
					if indegree[t.Name] == 0 {
						queue = append(queue, t.Name)
					}
				}
			}
		}
	}
	if visited != len(tasks) {
		return fmt.Errorf("%w: cycle detected", ErrInvalidDAG)
	}
	return nil
}

// CreateMission 落库（全部 PENDING）后把根节点推进 READY。
func (s *Service) CreateMission(ctx context.Context, owner, goal string, tasks []TaskSpec) (*mission.Mission, error) {
	return s.createMission(ctx, newID("msn"), store.Actor{Kind: "human", ID: owner}, owner, goal, tasks)
}

// createMission 带预定 ID 与触发者的创建（M4 准入管道：幂等占位需先定 ID，
// actor 为 intent source，落创建事件留痕）。
func (s *Service) createMission(ctx context.Context, id string, actor store.Actor, owner, goal string, tasks []TaskSpec) (*mission.Mission, error) {
	if err := validateDAG(tasks); err != nil {
		return nil, err
	}
	now := s.clk.Now()
	m := &mission.Mission{ID: id, Owner: owner, Goal: goal, Status: mission.MissionActive}

	subs := make([]*mission.Subtask, len(tasks))
	for i, t := range tasks {
		depends := make([]string, len(t.DependsOn))
		for j, d := range t.DependsOn {
			depends[j] = subID(m.ID, d)
		}
		subs[i] = &mission.Subtask{
			ID:             subID(m.ID, t.Name),
			MissionID:      m.ID,
			Kind:           t.Kind,
			RequiredSkills: t.RequiredSkills,
			DependsOn:      depends,
			Scheduling:     mission.SchedulingSpec{Priority: t.Priority, Deadline: t.Deadline},
			Retry:          mission.RetryPolicy{MaxAttempts: t.MaxAttempts, OnFailure: "retry"},
			State:          mission.StatePending,
			Question:       t.Question,
			Options:        t.Options,
			OnTimeout:      t.OnTimeout,
		}
	}
	if err := s.st.CreateMission(ctx, m, subs, actor, now); err != nil {
		return nil, err
	}
	// 根节点（无依赖）推进 READY
	for _, sub := range subs {
		if len(sub.DependsOn) == 0 {
			if _, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvDepsSatisfied, 0, actor, nil, now, nil); err != nil {
				return nil, fmt.Errorf("activate root %s: %w", sub.ID, err)
			}
		}
	}
	return m, nil
}

func subID(missionID, name string) string { return "sub_" + missionID[4:] + "_" + name }

// GetMission / ListSubtasks / ListMissionEvents / ListAgents 查询直通。
func (s *Service) GetMission(ctx context.Context, id string) (*mission.Mission, error) {
	return s.st.GetMission(ctx, id)
}
func (s *Service) ListSubtasks(ctx context.Context, missionID string) ([]*mission.Subtask, error) {
	return s.st.ListSubtasks(ctx, missionID)
}
func (s *Service) ListMissionEvents(ctx context.Context, missionID string, afterSeq int64, limit int) ([]*store.Event, error) {
	return s.st.ListMissionEvents(ctx, missionID, afterSeq, limit)
}
func (s *Service) ListAgents(ctx context.Context) ([]*store.Agent, error) {
	return s.st.ListAgents(ctx)
}

// CancelMission 级联取消全部非终态子任务（§3.2）。
func (s *Service) CancelMission(ctx context.Context, id, owner string) error {
	now := s.clk.Now()
	actor := store.Actor{Kind: "human", ID: owner}
	subs, err := s.st.ListSubtasks(ctx, id)
	if err != nil {
		return err
	}
	for _, sub := range subs {
		if sub.State.Terminal() {
			continue
		}
		// 冲突忽略：并发下状态已变化则跳过（尽力而为级联）
		_, _ = s.st.TransitionSubtask(ctx, sub.ID, mission.EvCancelled, sub.Version, actor,
			map[string]any{"reason": "mission cancelled"}, now, nil)
	}
	m, err := s.st.GetMission(ctx, id)
	if err != nil {
		return err
	}
	return s.st.SetMissionStatus(ctx, id, mission.MissionCancelled, m.Version, actor, now)
}

// ---- 依赖传播与终态推导（编排器内核，ADR-1：自研 + 后续抽象 SPI） ----

// propagate 子任务完成后调用：推进就绪下游，推导 Mission 终态。
func (s *Service) propagate(ctx context.Context, completed *mission.Subtask, actor store.Actor) error {
	now := s.clk.Now()
	subs, err := s.st.ListSubtasks(ctx, completed.MissionID)
	if err != nil {
		return err
	}
	byID := map[string]*mission.Subtask{}
	for _, sub := range subs {
		byID[sub.ID] = sub
	}
	for _, sub := range subs {
		if sub.State != mission.StatePending {
			continue
		}
		ready := true
		for _, dep := range sub.DependsOn {
			if byID[dep] == nil || byID[dep].State != mission.StateSucceeded {
				ready = false
				break
			}
		}
		if ready {
			if _, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvDepsSatisfied, sub.Version,
				store.Actor{Kind: "system", ID: "orchestrator"}, nil, now, nil); err != nil {
				return err
			}
		}
	}
	return s.deriveMissionStatus(ctx, completed.MissionID)
}

// cancelUnreachable 子任务永久失败（重试耗尽/人工否决）后调用：
// 级联取消所有传递依赖它的 PENDING 下游（它们永远不可能就绪），
// 使 MissionStatusOf 能推导出 FAILED 终态。CANCELLED 的下游自身也作为级联源。
func (s *Service) cancelUnreachable(ctx context.Context, failed *mission.Subtask, actor store.Actor) error {
	subs, err := s.st.ListSubtasks(ctx, failed.MissionID)
	if err != nil {
		return err
	}
	dead := map[string]bool{failed.ID: true}
	now := s.clk.Now()
	for changed := true; changed; {
		changed = false
		for _, sub := range subs {
			if sub.State != mission.StatePending || dead[sub.ID] {
				continue
			}
			for _, dep := range sub.DependsOn {
				if dead[dep] {
					if _, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvCancelled, sub.Version,
						actor, map[string]any{"reason": "upstream failed: " + failed.ID}, now, nil); err != nil {
						return err
					}
					dead[sub.ID] = true
					changed = true
					break
				}
			}
		}
	}
	return nil
}

// deriveMissionStatus 由子任务集合推导并落库 Mission 终态。
func (s *Service) deriveMissionStatus(ctx context.Context, missionID string) error {
	subs, err := s.st.ListSubtasks(ctx, missionID)
	if err != nil {
		return err
	}
	states := make([]mission.State, len(subs))
	for i, sub := range subs {
		states[i] = sub.State
	}
	st := mission.MissionStatusOf(states)
	if st == mission.MissionActive {
		return nil
	}
	m, err := s.st.GetMission(ctx, missionID)
	if err != nil {
		return err
	}
	if m.Status != mission.MissionActive {
		return nil // 已有终态（并发/取消）
	}
	err = s.st.SetMissionStatus(ctx, missionID, st, m.Version,
		store.Actor{Kind: "system", ID: "orchestrator"}, s.clk.Now())
	if errors.Is(err, store.ErrConflict) {
		return nil // 并发推导，另一个写者已落定
	}
	return err
}

// ---- 放置调度（Capability-First，§5.3；Filter → Score → Offer） ----

// ---- Agent 注册 ----

func (s *Service) RegisterAgent(ctx context.Context, a *store.Agent) error {
	if a.ID == "" {
		a.ID = newID("agt")
	}
	if a.Health == "" {
		a.Health = "healthy"
	}
	return s.st.UpsertAgent(ctx, a, s.clk.Now())
}

func (s *Service) Heartbeat(ctx context.Context, agentID string) error {
	return s.st.HeartbeatAgent(ctx, agentID, s.clk.Now())
}

func (s *Service) GetAgent(ctx context.Context, id string) (*store.Agent, error) {
	return s.st.GetAgent(ctx, id)
}

// GetLease 查询租约（API 层向 Adapter 下发 offer 时附带 fencing token）。
func (s *Service) GetLease(ctx context.Context, id string) (*store.Lease, error) {
	return s.st.GetLease(ctx, id)
}

// ListOffers 返回派给某 Agent 的待确认租约任务（Adapter 轮询拉取，M1 拉模式）。
func (s *Service) ListOffers(ctx context.Context, agentID string) ([]*mission.Subtask, error) {
	offered, err := s.st.ListSubtasksByState(ctx, mission.StateOffered)
	if err != nil {
		return nil, err
	}
	var out []*mission.Subtask
	for _, sub := range offered {
		if sub.Assignee == agentID {
			out = append(out, sub)
		}
	}
	return out, nil
}

// ---- 执行回调（fencing 校验在 store 层原子完成） ----

func (s *Service) AcceptLease(ctx context.Context, leaseID string, fencingToken int64, subtaskVersion int64, agentID string) (*mission.Subtask, error) {
	return s.st.AcceptLease(ctx, leaseID, fencingToken, subtaskVersion,
		store.Actor{Kind: "agent", ID: agentID}, s.clk.Now())
}

func (s *Service) StartSubtask(ctx context.Context, subtaskID string, fencingToken int64, version int64, agentID string) (*mission.Subtask, error) {
	return s.st.StartSubtask(ctx, subtaskID, fencingToken, version,
		store.Actor{Kind: "agent", ID: agentID}, s.clk.Now())
}

// RenewLease progress 心跳续租。
func (s *Service) RenewLease(ctx context.Context, leaseID string, fencingToken int64) error {
	return s.st.RenewLease(ctx, leaseID, fencingToken, 2*s.cfg.OfferTTL, s.clk.Now())
}

// CompleteSubtask 完成回调：幂等/fencing 在 store 层；成功后传播依赖与终态。
func (s *Service) CompleteSubtask(ctx context.Context, subtaskID string, fencingToken int64, idemKey, resultRef string, version int64, agentID string) (*mission.Subtask, error) {
	actor := store.Actor{Kind: "agent", ID: agentID}
	sub, err := s.st.CompleteSubtask(ctx, subtaskID, fencingToken, idemKey, resultRef, version, actor, s.clk.Now())
	if err != nil {
		return sub, err
	}
	if err := s.propagate(ctx, sub, actor); err != nil {
		return sub, fmt.Errorf("propagate: %w", err)
	}
	return sub, nil
}

// FailSubtask 失败回调：按 retry 策略重试回 READY，否则推导 Mission 终态。
func (s *Service) FailSubtask(ctx context.Context, subtaskID string, fencingToken int64, reason string, version int64, agentID string) (*mission.Subtask, error) {
	actor := store.Actor{Kind: "agent", ID: agentID}
	now := s.clk.Now()
	sub, err := s.st.FailSubtask(ctx, subtaskID, fencingToken, reason, version, actor, now)
	if err != nil {
		return sub, err
	}
	if sub.Attempt < sub.Retry.MaxAttempts {
		// 可重试：回 READY，attempt+1（§5.4 指数退避在 M3 引入 jitter 时实现）
		retried, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvRetried, sub.Version,
			store.Actor{Kind: "system", ID: "orchestrator"},
			map[string]any{"reason": reason}, now, func(st *mission.Subtask) error {
				st.Attempt++
				st.Assignee, st.LeaseID = "", ""
				return nil
			})
		if err == nil {
			return retried, nil
		}
	}
	// 永久失败：级联取消不可达下游，再推导 Mission 终态
	if err := s.cancelUnreachable(ctx, sub, actor); err != nil {
		return sub, err
	}
	if err := s.deriveMissionStatus(ctx, sub.MissionID); err != nil {
		return sub, err
	}
	return sub, nil
}

// ---- 黑板（M2-H4） ----

// BoardPut 写黑板；expectedVersion<0 盲写，>=0 CAS。
func (s *Service) BoardPut(ctx context.Context, missionID, ns, key string, value []byte, expectedVersion int64) (*store.BoardEntry, error) {
	if ns == "" || key == "" {
		return nil, fmt.Errorf("core: board namespace/key required")
	}
	e := &store.BoardEntry{MissionID: missionID, Namespace: ns, Key: key, Value: value}
	entry, err := s.st.BoardPut(ctx, e, expectedVersion, s.clk.Now())
	if err != nil {
		return nil, err
	}
	// M4-G2：黑板写入增量评估条件唤醒（全量兜底在 sweeper；失败不影响写入本身）
	_ = s.evalConditionWakes(ctx, missionID, ns+"/"+key)
	return entry, nil
}

func (s *Service) BoardGet(ctx context.Context, missionID, ns, key string) (*store.BoardEntry, error) {
	return s.st.BoardGet(ctx, missionID, ns, key)
}

func (s *Service) BoardList(ctx context.Context, missionID, ns string) ([]*store.BoardEntry, error) {
	return s.st.BoardList(ctx, missionID, ns)
}

// ---- 清扫器 ----

// SweepOnce 回收到期租约、标记心跳过期 Agent、处理到期决策工单（M2-H6）。
func (s *Service) SweepOnce(ctx context.Context) error {
	now := s.clk.Now()
	if _, err := s.st.ExpireLeases(ctx, now); err != nil {
		return err
	}
	agents, err := s.st.ListAgents(ctx)
	if err != nil {
		return err
	}
	for _, a := range agents {
		if a.Health == "healthy" && now.Sub(a.LastHeartbeat) > s.cfg.HeartbeatStale {
			if err := s.st.MarkAgentHealth(ctx, a.ID, "suspect"); err != nil {
				return err
			}
		}
	}
	// 到期决策：auto_* 已由 store 落 choice，此处驱动子任务流转
	expired, err := s.st.ExpireDecisions(ctx, now)
	if err != nil {
		return err
	}
	for _, d := range expired {
		if d.Status == store.DecisionResolved {
			if err := s.applyDecisionOutcome(ctx, d); err != nil {
				return err
			}
		}
		// status==expired（无 on_timeout 动作）：保持 BLOCKED 等人工干预（M2 边界）
	}
	// M3：挂起任务的 timer 唤醒与 wake TTL 回收
	return s.sweepWakes(ctx)
}
