// Package mission 定义任务模型（Mission / Subtask）与生命周期状态机。
// 对应设计文档 §3.2（任务模型）与 §7.3（WAITING 挂起-唤醒路径）。
package mission

import (
	"encoding/json"
	"time"
)

// State 子任务状态。终态：SUCCEEDED / FAILED / CANCELLED。
type State string

const (
	StatePending   State = "PENDING"   // 已创建，依赖未满足
	StateReady     State = "READY"     // 就绪，待调度
	StateOffered   State = "OFFERED"   // 已发放租约，待 Agent 确认
	StateLeased    State = "LEASED"    // Agent 已确认租约
	StateRunning   State = "RUNNING"   // 执行中
	StateWaiting   State = "WAITING"   // 挂起：等待定时/事件/条件唤醒（§7.3）
	StateBlocked   State = "BLOCKED"   // 等待人决策（§8）
	StateSucceeded State = "SUCCEEDED" // 终态
	StateFailed    State = "FAILED"    // 终态
	StateCancelled State = "CANCELLED" // 终态
)

// Terminal 判断是否为终态。
func (s State) Terminal() bool {
	switch s {
	case StateSucceeded, StateFailed, StateCancelled:
		return true
	}
	return false
}

// Kind 子任务类型（§3.2）。
type Kind string

const (
	KindAgent          Kind = "agent"
	KindHumanApproval  Kind = "human_approval"
	KindHumanDecision  Kind = "human_decision"
	KindAggregation    Kind = "aggregation"
	KindCondition      Kind = "condition"
)

// MissionStatus 使命状态。
type MissionStatus string

const (
	MissionActive    MissionStatus = "ACTIVE"
	MissionSucceeded MissionStatus = "SUCCEEDED"
	MissionFailed    MissionStatus = "FAILED"
	MissionCancelled MissionStatus = "CANCELLED"
)

// RetryPolicy 重试策略（§3.2）。
type RetryPolicy struct {
	MaxAttempts int    `json:"max_attempts"`
	Backoff     string `json:"backoff"`    // none | linear | exponential
	OnFailure   string `json:"on_failure"` // retry | replan | escalate
}

// SchedulingSpec 调度约束（§3.2）。
type SchedulingSpec struct {
	Priority     int        `json:"priority"`
	Deadline     *time.Time `json:"deadline,omitempty"`
	BudgetTokens int64      `json:"budget_tokens,omitempty"`
	Exclusive    bool       `json:"exclusive,omitempty"`
}

// Subtask 可执行最小单元。
type Subtask struct {
	ID         string          `json:"id"`
	MissionID  string          `json:"mission_id"`
	ParentID   string          `json:"parent_id,omitempty"`
	Kind       Kind            `json:"kind"`
	RequiredSkills []string    `json:"required_skills,omitempty"`
	DependsOn  []string        `json:"depends_on,omitempty"`
	Scheduling SchedulingSpec  `json:"scheduling"`
	Retry      RetryPolicy     `json:"retry"`
	State      State           `json:"state"`
	Assignee   string          `json:"assignee_agent_id,omitempty"`
	LeaseID    string          `json:"lease_id,omitempty"`
	Attempt    int             `json:"attempt"`
	ResultRef  string          `json:"result_ref,omitempty"`
	Version    int64           `json:"version"` // 乐观锁（§4.3）
	// human 节点（M2）：裁决工单的内容与超时策略
	Question   string          `json:"question,omitempty"`
	Options    []string        `json:"options,omitempty"`
	OnTimeout  string          `json:"on_timeout,omitempty"` // auto_approve | auto_reject | ""
	// Continuation（M3/M4，§7.3）：挂起-唤醒与检查点续跑
	Checkpoint   json.RawMessage `json:"checkpoint,omitempty"`    // 透明载荷（≤64KB），续跑 Agent 自行解释
	WakeKind     string          `json:"wake_kind,omitempty"`     // timer | manual | event | condition
	WakeAt       *time.Time      `json:"wake_at,omitempty"`       // timer 唤醒时刻
	WakeDeadline *time.Time      `json:"wake_deadline,omitempty"` // 唤醒注册 TTL（必填，过期 FAILED）
	WakeSpec     json.RawMessage `json:"wake_spec,omitempty"`     // 完整唤醒注册（event/condition 参数，求值用）
}

// Wake 类型常量（§7.3）。
const (
	WakeTimer     = "timer"
	WakeManual    = "manual"
	WakeEvent     = "event"     // M4：事件唤醒（§14.2 单事件模式）
	WakeCondition = "condition" // M4：条件唤醒（§14.3 结构化谓词，CEL 槽位预留）
)

// WakeSpec 一次性唤醒注册（§7.3/§14.4：level-triggered、edge-fired、TTL 必填）。
type WakeSpec struct {
	Kind      string          `json:"kind"` // timer | manual | event | condition
	At        *time.Time      `json:"at,omitempty"`
	Deadline  *time.Time      `json:"deadline"` // TTL（防永久悬挂）
	Event     *EventMatch     `json:"event,omitempty"`
	Condition *BoardCondition `json:"condition,omitempty"`
}

// EventMatch 单事件模式（§14.2 规则 1）：类型过滤 + 载荷子集等值谓词。
type EventMatch struct {
	Types    []string       `json:"types"`
	Where    map[string]any `json:"where,omitempty"` // 点路径下钻载荷，全部键等值才命中
	AfterSeq int64          `json:"after_seq"`       // 水位线：注册时平台写入，只匹配其后到达的事件
}

// BoardCondition 黑板条件谓词（M4 MVP；CEL 内核经 ConditionEvaluator 槽位替换）。
type BoardCondition struct {
	Board string `json:"board"` // "namespace/key"
	Op    string `json:"op"`    // exists | equals
	Value any    `json:"value,omitempty"`
}

// 条件操作常量。
const (
	CondExists = "exists"
	CondEquals = "equals"
)

// Mission 顶层目标。
type Mission struct {
	ID        string         `json:"id"`
	Owner     string         `json:"owner"`
	Goal      string         `json:"goal"`
	Status    MissionStatus  `json:"status"`
	Version   int64          `json:"version"`
}
