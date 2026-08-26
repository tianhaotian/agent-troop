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
	ErrConflict           = errors.New("store: version conflict or illegal transition")
	ErrNotFound           = errors.New("store: not found")
	ErrFenced             = errors.New("store: fencing token stale or lease not active")
	ErrDuplicate          = errors.New("store: duplicate idempotency key")
	ErrBudgetRequired     = errors.New("store: budget slice required")
	ErrBudgetExceeded     = errors.New("store: mission budget exceeded")
	ErrPermissionExceeded = errors.New("store: delegated permission envelope exceeded")
)

// Actor 事件行为者（审计三元组之一，§10）。
type Actor struct {
	Kind string `json:"kind"` // human | agent | service | system
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
	// TriggerScopes 触发授权（M5-H2，§7.4：默认收紧、按授权放开）。
	// 注册时显式声明，缺省 []——即默认不能经 /v1/intents create_mission/wake。
	TriggerScopes []string `json:"trigger_scopes,omitempty"`
	// AuthSubject 是外部身份令牌的稳定 subject。为空时兼容地回退为 Agent ID。
	AuthSubject string `json:"auth_subject,omitempty"`
	// Reputation 是调度查询时装载的按 skill 信誉快照，不由注册请求覆盖。
	Reputation map[string]*ReputationRecord `json:"reputation,omitempty"`
}

// Lease 执行租约（§4.3：fencing token 单调递增防僵尸写入）。
type Lease struct {
	ID           string    `json:"id"`
	SubtaskID    string    `json:"subtask_id"`
	AgentID      string    `json:"agent_id"`
	FencingToken int64     `json:"fencing_token"`
	ExpiresAt    time.Time `json:"expires_at"`
	State        string    `json:"state"` // ACTIVE | EXPIRED | RELEASED | FENCED
	CreatedAt    time.Time `json:"created_at"`
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

// LeadInboxItem 是 delegated child 向父 Lead 交付的显式上下文条目（§15.1）。
type LeadInboxItem struct {
	ID              string     `json:"id"`
	MissionID       string     `json:"mission_id"`
	LeadSubtaskID   string     `json:"lead_subtask_id"`
	SourceSubtaskID string     `json:"source_subtask_id"`
	Kind            string     `json:"kind"`
	ResultRef       string     `json:"result_ref,omitempty"`
	Status          string     `json:"status"`                // pending | ingested
	IngestMode      string     `json:"ingest_mode,omitempty"` // summary | full
	IngestedBy      string     `json:"ingested_by,omitempty"`
	Version         int64      `json:"version"`
	CreatedAt       time.Time  `json:"created_at"`
	IngestedAt      *time.Time `json:"ingested_at,omitempty"`
}

const (
	LeadInboxPending  = "pending"
	LeadInboxIngested = "ingested"
	LeadIngestSummary = "summary"
	LeadIngestFull    = "full"
)

func LeadInboxID(sourceSubtaskID string) string { return "lin_" + sourceSubtaskID }

// BudgetAccount 是 Mission 级 token 硬顶账户。Metered=false 表示兼容历史 Mission，
// 不执行预算限制；Available 由 total-held-spent 派生并随查询返回。
type BudgetAccount struct {
	MissionID string    `json:"mission_id"`
	Metered   bool      `json:"metered"`
	Total     int64     `json:"total_tokens"`
	Held      int64     `json:"held_tokens"`
	Spent     int64     `json:"spent_tokens"`
	Available int64     `json:"available_tokens"`
	Version   int64     `json:"version"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// BudgetHold 记录一次 delegated subtask 的预估与实际用量。
type BudgetHold struct {
	ID        string     `json:"id"`
	MissionID string     `json:"mission_id"`
	SubtaskID string     `json:"subtask_id"`
	Attempt   int        `json:"attempt"`
	Amount    int64      `json:"amount_tokens"`
	Actual    int64      `json:"actual_tokens"`
	Status    string     `json:"status"` // HELD | SETTLED | RELEASED
	CreatedAt time.Time  `json:"created_at"`
	SettledAt *time.Time `json:"settled_at,omitempty"`
}

const (
	BudgetHoldHeld     = "HELD"
	BudgetHoldSettled  = "SETTLED"
	BudgetHoldReleased = "RELEASED"
)

func BudgetHoldID(subtaskID string) string { return "bhd_" + subtaskID }

// ContextBoardEntry 是授权黑板切片在 dispatch 时的不可变视图。
type ContextBoardEntry struct {
	Namespace string `json:"namespace"`
	Key       string `json:"key"`
	Value     []byte `json:"value"`
	Version   int64  `json:"version"`
	Mode      string `json:"mode"`
}

type ContextDecision struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Question string `json:"question"`
	Status   string `json:"status"`
	Choice   string `json:"choice,omitempty"`
}

type ContextTask struct {
	ID             string                     `json:"id"`
	MissionID      string                     `json:"mission_id"`
	ParentID       string                     `json:"parent_id,omitempty"`
	Kind           mission.Kind               `json:"kind"`
	RequiredSkills []string                   `json:"required_skills,omitempty"`
	Scheduling     mission.SchedulingSpec     `json:"scheduling"`
	Retry          mission.RetryPolicy        `json:"retry"`
	Input          map[string]any             `json:"input,omitempty"`
	ReworkOf       string                     `json:"rework_of,omitempty"`
	Grants         mission.PermissionEnvelope `json:"grants"`
	Checkpoint     []byte                     `json:"checkpoint,omitempty"`
	WakeKind       string                     `json:"wake_kind,omitempty"`
	WakeAt         *time.Time                 `json:"wake_at,omitempty"`
	WakeDeadline   *time.Time                 `json:"wake_deadline,omitempty"`
	WakeSpec       []byte                     `json:"wake_spec,omitempty"`
}

// ContextPackage 是按 lease 物化的最小知情快照；SnapshotHash 不含 lease/时间元数据。
type ContextPackage struct {
	ID           string               `json:"id"`
	LeaseID      string               `json:"lease_id"`
	MissionID    string               `json:"mission_id"`
	SubtaskID    string               `json:"subtask_id"`
	Task         ContextTask          `json:"task_spec"`
	Artifacts    []*Artifact          `json:"artifacts"`
	BoardViews   []*ContextBoardEntry `json:"board_views"`
	Decisions    []*ContextDecision   `json:"decisions_digest"`
	Budget       *BudgetAccount       `json:"budget"`
	SnapshotHash string               `json:"snapshot_hash"`
	CreatedAt    time.Time            `json:"created_at"`
}

// Store 存储接口。memory 实现用于测试与本地零依赖运行；pg 实现用于真实部署。
type Store interface {
	// Ping 验证当前存储依赖可用；供 readiness 使用，不执行写操作。
	Ping(ctx context.Context) error

	// ---- 任务面 ----
	// CreateMission 在一个事务内写入 Mission、全部 Subtask（PENDING）与创建事件。
	CreateMission(ctx context.Context, m *mission.Mission, subs []*mission.Subtask, actor Actor, now time.Time) error
	GetMission(ctx context.Context, id string) (*mission.Mission, error)
	GetMissionBudget(ctx context.Context, missionID string) (*BudgetAccount, error)
	ListBudgetHolds(ctx context.Context, missionID string) ([]*BudgetHold, error)
	GetSubtask(ctx context.Context, id string) (*mission.Subtask, error)
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
	ListReputations(ctx context.Context, agentID string) ([]*ReputationRecord, error)
	ApplyReputationSignal(ctx context.Context, sig ReputationSignal, now time.Time) error

	// ---- 租约（fencing） ----
	// OfferLease 原子完成：READY→OFFERED 迁移 + 租约插入（fencing token 取全局单调序列）。
	OfferLease(ctx context.Context, subtaskID, agentID string, expectedVersion int64,
		ttl time.Duration, actor Actor, now time.Time) (*Lease, error)
	// AcceptLease 校验租约活跃且 token 匹配，OFFERED→LEASED。
	AcceptLease(ctx context.Context, leaseID string, fencingToken int64, expectedSubVersion int64,
		actor Actor, now time.Time) (*mission.Subtask, error)
	// GetLease 查询租约（Adapter 拉取 offer 时需连同 fencing token 下发）。
	GetLease(ctx context.Context, id string) (*Lease, error)
	GetContextPackage(ctx context.Context, leaseID string) (*ContextPackage, error)
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
	// CompleteSubtaskWithUsage 与 CompleteSubtask 相同，并在同一事务内按实际 token 结算预算 hold。
	CompleteSubtaskWithUsage(ctx context.Context, id string, fencingToken int64, idemKey, resultRef string,
		usageTokens, expectedVersion int64, actor Actor, now time.Time) (*mission.Subtask, error)
	// FailSubtask 校验 fencing token，RUNNING→FAILED、记录原因、释放租约。
	FailSubtask(ctx context.Context, id string, fencingToken int64, reason string,
		expectedVersion int64, actor Actor, now time.Time) (*mission.Subtask, error)
	// BlockSubtask 校验 fencing token，RUNNING→BLOCKED（Agent 主动请求人决策，M2-H3）。
	// 注意：BLOCKED 不释放租约——裁决批准后原 Agent 续跑。
	BlockSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
		actor Actor, now time.Time) (*mission.Subtask, error)
	// CancelSubtask 原子完成状态取消、活跃租约释放与取消事件追加。
	CancelSubtask(ctx context.Context, id string, expectedVersion int64,
		actor Actor, now time.Time) (*mission.Subtask, error)

	// ---- 主子委托（M6，§15.1） ----
	// SpawnSubtask 原子完成（CompleteSubtask 同构）：幂等键撞键返回 ErrDuplicate +
	// existingID（原子女 ID）；否则校验父任务 fencing token + RUNNING 态 + version，
	// 插入子女并原子激活为 READY（parent_id 因果链），追加创建/就绪事件。
	SpawnSubtask(ctx context.Context, idemKey, parentID string, fencingToken, parentVersion int64,
		child *mission.Subtask, actor Actor, now time.Time) (existingID string, err error)
	// CountChildren 统计某子任务的直接子女数（delegate fanout 校验）。
	CountChildren(ctx context.Context, parentID string) (int, error)

	// ---- Lead 恢复闭环（M7B，§15.1/§15.2/§15.4） ----
	ListLeadInbox(ctx context.Context, leadSubtaskID string, pendingOnly bool) ([]*LeadInboxItem, error)
	// IngestLeadInbox 在活跃 Lead 租约保护下 CAS 标记显式摄入。
	IngestLeadInbox(ctx context.Context, itemID, leadSubtaskID string, fencingToken, expectedVersion int64,
		mode string, actor Actor, now time.Time) (*LeadInboxItem, error)
	// SaveLeadSnapshot 原子完成 owner/fencing 校验、lead-plan CAS 快照写入与租约续期。
	// expectedVersion=-1 表示仅首次创建；>=0 表示更新指定版本。
	SaveLeadSnapshot(ctx context.Context, leadSubtaskID string, fencingToken, expectedVersion int64,
		value []byte, leaseTTL time.Duration, actor Actor, now time.Time) (*BoardEntry, error)
	// TakeoverStaleLeads fence 到期 RUNNING Lead，将协调任务回 READY；不影响其 child。
	TakeoverStaleLeads(ctx context.Context, now time.Time) ([]*mission.Subtask, error)

	// ---- 挂起-唤醒（M3/M4，§7.3/§14.4） ----
	// SuspendSubtask 校验 fencing token，RUNNING→WAITING 并**释放租约**
	// （与 BLOCKED 不同：唤醒后重新调度，可换 Agent 凭 checkpoint 续跑）。
	// wake 为完整唤醒注册（kind/at/deadline 冗余为顶层字段供索引查询）；
	// checkpoint 非空时一并保存。
	SuspendSubtask(ctx context.Context, id string, fencingToken int64, expectedVersion int64,
		wake *mission.WakeSpec, checkpoint []byte,
		actor Actor, now time.Time) (*mission.Subtask, error)
	// WakeSubtask CAS 唤醒 WAITING→READY 并清空 wake 字段（一次性注册语义；
	// checkpoint 保留供续跑）。版本不匹配返回 ErrConflict（多 sweeper 竞争安全）。
	WakeSubtask(ctx context.Context, id string, expectedVersion int64,
		actor Actor, now time.Time) (*mission.Subtask, error)
	// ListWaitingDue 返回 timer 到期的 WAITING 子任务（wake_at<=now 且未过 TTL）。
	ListWaitingDue(ctx context.Context, now time.Time) ([]*mission.Subtask, error)
	// ExpireWakes 将 wake_deadline<=now 的 WAITING 子任务置 FAILED(reason=wake_timeout)，
	// 返回受影响子任务（级联取消与 Mission 终态推导由调用方负责）。
	ExpireWakes(ctx context.Context, now time.Time) ([]*mission.Subtask, error)
	// SaveCheckpoint progress 心跳携带检查点落库（fencing 校验；不迁移状态、不记事件）。
	SaveCheckpoint(ctx context.Context, id string, fencingToken int64, checkpoint []byte, now time.Time) error
	// ListWaiting 按唤醒类型扫描 WAITING 子任务（event/condition 求值用，M4）。
	ListWaiting(ctx context.Context, wakeKind string) ([]*mission.Subtask, error)
	// MaxEventSeq 当前事件最大 seq（suspend 注册 event 唤醒的水位线，M4）。
	MaxEventSeq(ctx context.Context) (int64, error)

	// ---- 幂等键（M4 准入管道复用） ----
	// PutIdempotent 写入幂等键；键已存在返回 ErrDuplicate（result 为既有值）。
	PutIdempotent(ctx context.Context, key, result string, now time.Time) (existing string, err error)

	// ---- 事件 ----
	// ListMissionEvents 返回 Mission 及子任务相关事件（SSE 驱动，按 seq 递增）。
	ListMissionEvents(ctx context.Context, missionID string, afterSeq int64, limit int) ([]*Event, error)
	// AppendEvent 追加一条独立事件（M5：condition.cost_exceeded 等平台留痕；
	// 不与状态迁移同事务时使用本方法）。
	AppendEvent(ctx context.Context, e *Event) error

	// ---- 决策（M2） ----
	CreateDecision(ctx context.Context, d *Decision, now time.Time) error
	// CreateDecisionAndBlock 原子完成子任务 BLOCKED 迁移与决策工单创建。
	// fencingToken=nil 用于 READY human 节点；非 nil 用于 RUNNING Agent 主动请求。
	CreateDecisionAndBlock(ctx context.Context, d *Decision, expectedSubVersion int64,
		fencingToken *int64, actor Actor, now time.Time) (*mission.Subtask, error)
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
	RecordQuality(ctx context.Context, q *QualityRecord, signals []ReputationSignal, actor Actor, now time.Time) error
	GetQuality(ctx context.Context, artifactID string) (*QualityRecord, error)
	CreateQualityAppeal(ctx context.Context, a *QualityAppeal, actor Actor, now time.Time) error
	ListQualityAppeals(ctx context.Context, missionID string, pendingOnly bool) ([]*QualityAppeal, error)
	ResolveQualityAppeal(ctx context.Context, id, status, resolution, reviewerID string, actor Actor, now time.Time) (*QualityAppeal, error)

	// ---- 权威计量（M9） ----
	PutMeterRecord(ctx context.Context, m *MeterRecord, now time.Time) error
	ListMeterRecords(ctx context.Context, missionID string) ([]*MeterRecord, error)
}
