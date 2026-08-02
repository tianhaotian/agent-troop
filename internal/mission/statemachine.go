package mission

import "fmt"

// EventType 子任务生命周期事件（事件溯源，§4.3：状态迁移一律事件先行）。
type EventType string

const (
	EvCreated           EventType = "subtask.created"
	EvDepsSatisfied     EventType = "subtask.deps_satisfied"
	EvLeaseOffered      EventType = "subtask.lease_offered"
	EvLeaseAccepted     EventType = "subtask.lease_accepted"
	EvLeaseExpired      EventType = "subtask.lease_expired"   // 租约超时回收 → 回 READY
	EvStarted           EventType = "subtask.started"
	EvSuspended         EventType = "subtask.suspended"       // Agent 主动挂起（Continuation，§7.3）
	EvWoken             EventType = "subtask.woken"           // Trigger Service 唤醒
	EvBlocked           EventType = "subtask.blocked"         // 发起人工决策请求
	EvDecisionApproved  EventType = "subtask.decision_approved"
	EvDecisionRejected  EventType = "subtask.decision_rejected"
	EvCompleted         EventType = "subtask.succeeded"
	EvFailed            EventType = "subtask.failed"
	EvRetried           EventType = "subtask.retried"         // 失败后重试 → 回 READY
	EvWithdrawn         EventType = "subtask.withdrawn"       // 无人认领 → 策略重评估后回 READY
	EvCancelled         EventType = "subtask.cancelled"
)

// transitions 合法迁移表：state → event → next。
// 对应设计 §3.2 状态机（含 WAITING 挂起路径与 BLOCKED 人决策路径）。
var transitions = map[State]map[EventType]State{
	StatePending: {
		EvDepsSatisfied: StateReady,
		EvCancelled:     StateCancelled,
	},
	StateReady: {
		EvLeaseOffered: StateOffered,
		EvSuspended:    StateWaiting, // 就绪前被挂起（如等待外部条件的计划性挂起）
		EvCancelled:    StateCancelled,
	},
	StateOffered: {
		EvLeaseAccepted: StateLeased,
		EvLeaseExpired:  StateReady, // EXPIRED：租约超时未确认
		EvWithdrawn:     StateReady, // WITHDRAWN：重评估后重新入队
		EvCancelled:     StateCancelled,
	},
	StateLeased: {
		EvStarted:      StateRunning,
		EvLeaseExpired: StateReady, // 确认后迟迟未启动，回收
		EvCancelled:    StateCancelled,
	},
	StateRunning: {
		EvCompleted: StateSucceeded,
		EvFailed:    StateFailed,
		EvSuspended: StateWaiting, // Continuation：suspend + wake_on
		EvBlocked:   StateBlocked, // 请求人工决策
		EvCancelled: StateCancelled,
	},
	StateWaiting: {
		EvWoken:     StateReady, // 唤醒后重新调度（可换 Agent 续跑，§14.4）
		EvCancelled: StateCancelled,
	},
	StateBlocked: {
		EvDecisionApproved: StateRunning,
		EvDecisionRejected: StateFailed,
		EvCancelled:        StateCancelled,
	},
	// FAILED 允许经显式 retried 事件回 READY（Orchestrator 判定 attempt < max_attempts 后发出）
	StateFailed: {
		EvRetried:   StateReady,
		EvCancelled: StateCancelled, // 允许对失败子树做显式清理标记
	},
}

// Apply 校验并返回 (state, event) 的下一状态。
// 纯函数：不做任何 IO，不读时钟——迁移的持久化与事件落库由 store 层负责。
func Apply(from State, ev EventType) (State, error) {
	nexts, ok := transitions[from]
	if !ok {
		// FAILED 是半终态（允许 retried/cancelled），在迁移表中有条目；
		// SUCCEEDED/CANCELLED 无条目，走到这里说明是真正终态。
		if from.Terminal() {
			return "", fmt.Errorf("mission: subtask in terminal state %s rejects event %s", from, ev)
		}
		return "", fmt.Errorf("mission: unknown state %s", from)
	}
	next, ok := nexts[ev]
	if !ok {
		return "", fmt.Errorf("mission: illegal transition %s --%s--> ?", from, ev)
	}
	return next, nil
}

// MustApply 同 Apply，非法迁移 panic（仅用于编排器内部已校验路径与测试）。
func MustApply(from State, ev EventType) State {
	next, err := Apply(from, ev)
	if err != nil {
		panic(err)
	}
	return next
}

// MissionStatusOf 由子任务集合推导 Mission 终态（§5.1：S5 编排使用）。
// 规则：全部 SUCCEEDED → SUCCEEDED；存在 FAILED（且不可重试由调用方保证）→ FAILED；
// 全部终态且含 CANCELLED → CANCELLED；否则仍 ACTIVE。
func MissionStatusOf(states []State) MissionStatus {
	if len(states) == 0 {
		return MissionActive
	}
	succeeded, failed, cancelled, terminal := 0, 0, 0, 0
	for _, s := range states {
		switch s {
		case StateSucceeded:
			succeeded++
			terminal++
		case StateFailed:
			failed++
			terminal++
		case StateCancelled:
			cancelled++
			terminal++
		}
	}
	switch {
	case terminal < len(states):
		return MissionActive
	case failed > 0:
		return MissionFailed
	case cancelled > 0:
		return MissionCancelled
	default:
		_ = succeeded
		return MissionSucceeded
	}
}
