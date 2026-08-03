// Package store 定义控制平面的存储抽象（PG-first，设计 §4 / §22.4）。
//
// 原子性约定：每个方法封装一个原子操作——状态迁移（乐观锁 CAS）与
// 事件追加（Outbox）在同一事务内完成，调用方不得拆开使用（§4.3）。
package store

import (
	"context"
	"errors"
	"time"

	"agenttroop/internal/mission"
)

// 并发/冲突错误：调用方（调度器、回调）遇 Conflict 应重读或放弃本次操作。
var (
	ErrConflict   = errors.New("store: version conflict or illegal transition")
	ErrNotFound   = errors.New("store: not found")
	ErrFenced     = errors.New("store: fencing token stale or lease not active")
	ErrDuplicate  = errors.New("store: duplicate idempotency key")
)

// Actor 事件行为者（审计三元组之一，§10）。
type Actor struct {
	Kind string `json:"kind"` // human | agent | system
	ID   string `json:"id"`
}

// Event 事件日志条目（只追加，§4.3）。
type Event struct {
	Seq         int64          `json:"seq"`
	AggregateID string         `json:"aggregate_id"` // subtask / mission / agent id
	MissionID   string         `json:"mission_id,omitempty"`
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload,omitempty"`
	Actor       Actor          `json:"actor"`
	Ts          time.Time      `json:"ts"`
}

// Capability Agent 能力画像条目（§3.1）。
type Capability struct {
	Skill string  `json:"skill"`
	Level float64 `json:"level"` // 0~1
}

// Agent 注册信息。
type Agent struct {
	ID             string            `json:"id"`
	Name           string            `json:"name"`
	Platform       string            `json:"platform"`
	Endpoint       map[string]string `json:"endpoint"`
	Capabilities   []Capability      `json:"capabilities"`
	MaxConcurrency int               `json:"max_concurrency"`
	Health         string            `json:"health"` // healthy | suspect | down
	LastHeartbeat  time.Time         `json:"last_heartbeat"`
	Running        int               `json:"running"` // 在途租约数（放置调度用）
}

// Lease 执行租约（§4.3：fencing token 单调递增防僵尸写入）。
type Lease struct {
	ID           string    `json:"id"`
	SubtaskID    string    `json:"subtask_id"`
	AgentID      string    `json:"agent_id"`
	FencingToken int64     `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	State        string    `json:"state"` // ACTIVE | EXPIRED | RELEASED | FENCED
}

const (
	LeaseActive   = "ACTIVE"
	LeaseExpired  = "EXPIRED"
	LeaseReleased = "RELEASED"
	LeaseFenced   = "FENCED"
)

// Decision 决策工单（§8.2：审批门 / 决策点；裁决留痕审计）。
type Decision struct {
	ID         string     `json:"id"`
	MissionID  string     `json:"mission_id"`
	SubtaskID  string     `json:"subtask_id"`
	Kind       string     `json:"kind"` // approval | decision
	Question   string     `json:"question"`
	Options    []string   `json:"options"`
	Status     string     `json:"status"` // pending | resolved | expired
	Choice     string     `json:"choice,omitempty"`
	Rationale  string     `json:"rationale,omitempty"`
	DeciderID  string     `json:"decider_id,omitempty"`
	Deadline   *time.Time `json:"deadline,omitempty"`
	OnTimeout  string     `json:"on_timeout,omitempty"` // auto_approve | auto_reject | ""(none)
	CreatedAt  time.Time  `json:"created_at"`
	ResolvedAt *time.Time `json:"resolved_at,omitempty"`
}

const (
	DecisionPending  = "pending"
	DecisionResolved = "resolved"
	DecisionExpired  = "expired"
)

// BoardEntry 黑板条目（§6.2：Mission 级共享上下文，CAS 版本防脏写）。
type BoardEntry struct {
	MissionID string    `json:"mission_id"`
	Namespace string    `json:"namespace"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Artifact 产物注册项（§4.1：内容寻址，blob 本体在 BlobStore）。
type Artifact struct {
	ID         string    `json:"id"`
	SHA256     string    `json:"sha256"`
	MissionID  string    `json:"mission_id"`
	ProducedBy string    `json:"produced_by,omitempty"` // subtask id
	SchemaRef  string    `json:"schema_ref,omitempty"`
	Size       int64     `json:"size"`
	CreatedAt  time.Time `json:"created_at"`
}

// Store 存储接口。memory 实现用于测试与本地零依赖运行；pg 实现用于真实部署。
type Store interface {
	// ---- 任务面 ----
	// CreateMission 在一个事务内写入 Mission、全部 Subtask（PENDING）与创建事件。
	CreateMission(ctx context.Context, m *mission.Mission, subs []*mission.Subtask, actor Actor, now time.Time) error
	GetMission(ctx context.Context, id string) (*mission.Mission, error)
	ListSubtasks(ctx context.Context, missionID string) ([]*mission.Subtask, error)
	// ListSubtasksByState 按状态扫描（OFFERED 拉取等；pg 走部分索引）。
	ListSubtasksByState(ctx context.Context, st mission.State) ([]*mission.Subtask, error)
	SetMissionStatus(ctx context.Context, id string, st mission.MissionStatus, expectedVersion int64, actor Actor, now time.Time) error

	// TransitionSubtask 加载子任务 → mission.Apply 校验迁移 → mutate 修改字段 →
	// CAS 更新（version 不匹配返回 ErrConflict）→ 同事务追加事件。
	TransitionSubtask(ctx context.Context, id string, ev mission.EventType, expectedVersion int64,
		actor Actor, payload map[string]any, now time.Time,
		mutate func(*mission.Subtask) error) (*mission.Subtask, error)

	// ---- 就绪队列 ----
	// DequeueReady 按 priority 降序、deadline 升序返回 READY 候选（不加锁；
	// 放置以 OfferLease 的 CAS 为准，多副本调度器竞争安全，§5.1）。
	DequeueReady(ctx context.Context, limit int) ([]*mission.Subtask, error)

	// ---- Agent 注册 ----
	UpsertAgent(ctx context.Context, a *Agent, now time.Time) error
	GetAgent(ctx context.Context, id string) (*Agent, error)
	ListAgents(ctx context.Context) ([]*Agent, error)
	HeartbeatAgent(ctx context.Context, id string, now time.Time) error
	MarkAgentHealth(ctx context.Context, id, health string) error

	// ---- 租约（fencing） ----
	// OfferLease 原子完成：READY→OFFERED 迁移 + 租约插入（fencing token 取全局单调序列）。
	OfferLease(ctx context.Context, subtaskID, agentID string, expectedVersion int64,
		ttl time.Duration, actor Actor, now time.Time) (*Lease, error)
	// AcceptLease 校验租约活跃且 token 匹配，OFFERED→LEASED。
	AcceptLease(ctx context.Context, leaseID string, fencingToken int64, expectedSubVersion int64,
		actor Actor, now time.Time) (*mission.Subtask, error)
	// GetLease 查询租约（Adapter 拉取 offer 时需连同 fencing token 下发）。
	GetLease(ctx context.Context, id string) (*Lease, error)
	// RenewLease 延长活跃租约（progress 心跳驱动）。
	RenewLease(ctx context.Context, leaseID string, fencingToken int64, ttl time.Duration, now time.Time) error
	// ExpireLeases 回收到期活跃租约：LEASED/OFFERED 态子任务回 READY。返回回收数量。
	ExpireLeases(ctx context.Context, now time.Time) (int, error)

	// ---- 执行回调（fencing + 幂等 + 迁移原子完成，§4.3） ----
	// StartSubtask 校验 fencing token 后 LEASED→RUNNING。
	StartSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
		actor Actor, now time.Time) (*mission.Subtask, error)
	// CompleteSubtask 校验 fencing token 与幂等键（重复键返回 ErrDuplicate），
	// RUNNING→SUCCEEDED、写 result_ref、释放租约。
	CompleteSubtask(ctx context.Context, id string, fencingToken int64, idemKey, resultRef string,
		expectedVersion int64, actor Actor, now time.Time) (*mission.Subtask, error)
	// FailSubtask 校验 fencing token，RUNNING→FAILED、记录原因、释放租约。
	FailSubtask(ctx context.Context, id string, fencingToken int64, reason string,
		expectedVersion int64, actor Actor, now time.Time) (*mission.Subtask, error)
	// BlockSubtask 校验 fencing token，RUNNING→BLOCKED（Agent 主动请求人决策，M2-H3）。
	// 注意：BLOCKED 不释放租约——裁决批准后原 Agent 续跑。
	BlockSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
		actor Actor, now time.Time) (*mission.Subtask, error)

	// ---- 事件 ----
	// ListMissionEvents 返回 Mission 及子任务相关事件（SSE 驱动，按 seq 递增）。
	ListMissionEvents(ctx context.Context, missionID string, afterSeq int64, limit int) ([]*Event, error)

	// ---- 决策（M2） ----
	CreateDecision(ctx context.Context, d *Decision, now time.Time) error
	GetDecision(ctx context.Context, id string) (*Decision, error)
	ListDecisions(ctx context.Context, missionID string, pendingOnly bool) ([]*Decision, error)
	// ResolveDecision 裁决（CAS：仅 pending 可裁决，重复裁决返回 ErrConflict）。
	ResolveDecision(ctx context.Context, id, choice, rationale, deciderID string, now time.Time) (*Decision, error)
	// ExpireDecisions 到期未裁决工单按 on_timeout 处理，返回处理数。
	ExpireDecisions(ctx context.Context, now time.Time) ([]*Decision, error)

	// ---- 黑板（M2） ----
	// BoardPut 写黑板；expectedVersion<0 盲写覆盖，>=0 时 CAS（不匹配返回 ErrConflict）。
	BoardPut(ctx context.Context, e *BoardEntry, expectedVersion int64, now time.Time) (*BoardEntry, error)
	BoardGet(ctx context.Context, missionID, ns, key string) (*BoardEntry, error)
	BoardList(ctx context.Context, missionID, ns string) ([]*BoardEntry, error)

	// ---- Artifact（M2） ----
	PutArtifact(ctx context.Context, a *Artifact, now time.Time) error
	GetArtifact(ctx context.Context, id string) (*Artifact, error)
}
