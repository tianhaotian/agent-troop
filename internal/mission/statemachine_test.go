package mission

import "testing"

// 全量迁移矩阵：合法路径断言目标状态，非法路径断言报错。
// 覆盖设计 §3.2 状态机的全部边（含 WAITING/BLOCKED/重试/取消级联）。

func TestApply_LegalTransitions(t *testing.T) {
	cases := []struct {
		from State
		ev   EventType
		want State
	}{
		// 主链路：PENDING → READY → OFFERED → LEASED → RUNNING → SUCCEEDED
		{StatePending, EvDepsSatisfied, StateReady},
		{StateReady, EvLeaseOffered, StateOffered},
		{StateOffered, EvLeaseAccepted, StateLeased},
		{StateLeased, EvStarted, StateRunning},
		{StateRunning, EvCompleted, StateSucceeded},
		{StateRunning, EvTakeover, StateReady},

		// 租约回收：OFFERED/LEASED 超时回 READY；OFFERED 撤回重评估
		{StateOffered, EvLeaseExpired, StateReady},
		{StateLeased, EvLeaseExpired, StateReady},
		{StateOffered, EvWithdrawn, StateReady},

		// 挂起-唤醒（Continuation，§7.3）
		{StateRunning, EvSuspended, StateWaiting},
		{StateReady, EvSuspended, StateWaiting},
		{StateWaiting, EvWoken, StateReady},

		// 人工决策（§8）：阻塞 → 批准续跑 / 拒绝失败
		{StateRunning, EvBlocked, StateBlocked},
		{StateReady, EvBlocked, StateBlocked}, // human 节点就绪即阻塞（M2-H1）
		{StateBlocked, EvDecisionApproved, StateRunning},
		{StateBlocked, EvDecisionRejected, StateFailed},

		// 失败重试回 READY（§5.4）
		{StateRunning, EvFailed, StateFailed},
		{StateFailed, EvRetried, StateReady},
	}
	for _, c := range cases {
		got, err := Apply(c.from, c.ev)
		if err != nil {
			t.Errorf("Apply(%s, %s) unexpected error: %v", c.from, c.ev, err)
			continue
		}
		if got != c.want {
			t.Errorf("Apply(%s, %s) = %s, want %s", c.from, c.ev, got, c.want)
		}
	}
}

func TestApply_CancelFromAnyNonTerminal(t *testing.T) {
	// 取消路径：任意非终态 → CANCELLED（§3.2 级联取消）
	nonTerminal := []State{
		StatePending, StateReady, StateOffered,
		StateLeased, StateRunning, StateWaiting, StateBlocked, StateFailed,
	}
	for _, s := range nonTerminal {
		got, err := Apply(s, EvCancelled)
		if err != nil || got != StateCancelled {
			t.Errorf("Apply(%s, cancelled) = %s, %v; want CANCELLED, nil", s, got, err)
		}
	}
}

func TestApply_TerminalStatesRejectEverything(t *testing.T) {
	allEvents := []EventType{
		EvCreated, EvDepsSatisfied, EvLeaseOffered, EvLeaseAccepted, EvLeaseExpired,
		EvStarted, EvTakeover, EvSuspended, EvWoken, EvBlocked, EvDecisionApproved,
		EvDecisionRejected, EvCompleted, EvFailed, EvRetried, EvWithdrawn, EvCancelled,
	}
	for _, s := range []State{StateSucceeded, StateCancelled} {
		for _, ev := range allEvents {
			if _, err := Apply(s, ev); err == nil {
				t.Errorf("Apply(%s, %s) should fail on terminal state", s, ev)
			}
		}
	}
	// FAILED 半终态：仅允许 retried / cancelled
	if _, err := Apply(StateFailed, EvCompleted); err == nil {
		t.Error("FAILED --completed--> should be illegal")
	}
	if _, err := Apply(StateFailed, EvLeaseOffered); err == nil {
		t.Error("FAILED --lease_offered--> should be illegal")
	}
}

func TestApply_IllegalTransitions(t *testing.T) {
	cases := []struct {
		from State
		ev   EventType
	}{
		{StatePending, EvLeaseOffered}, // 未就绪不可派租
		{StatePending, EvStarted},      // 未执行不可启动
		{StateReady, EvStarted},        // 跳过租约直接启动
		{StateReady, EvCompleted},      // 未运行不可完成
		{StateOffered, EvStarted},      // 未确认租约不可启动
		{StateWaiting, EvCompleted},    // 挂起中不可直接完成（须先唤醒）
		{StateWaiting, EvLeaseOffered}, // 挂起中须唤醒回 READY 再调度
		{StateBlocked, EvCompleted},    // 阻塞中须先获决策
		{StateBlocked, EvSuspended},    // 阻塞与挂起不可互换
		{StateLeased, EvCompleted},     // 未 started 不可完成
	}
	for _, c := range cases {
		if _, err := Apply(c.from, c.ev); err == nil {
			t.Errorf("Apply(%s, %s) should be illegal but succeeded", c.from, c.ev)
		}
	}
}

func TestMissionStatusOf(t *testing.T) {
	cases := []struct {
		name   string
		states []State
		want   MissionStatus
	}{
		{"empty is active", nil, MissionActive},
		{"all succeeded", []State{StateSucceeded, StateSucceeded}, MissionSucceeded},
		{"partial still active", []State{StateSucceeded, StateRunning}, MissionActive},
		{"any failed → failed", []State{StateSucceeded, StateFailed, StateCancelled}, MissionFailed},
		{"cancelled only", []State{StateSucceeded, StateCancelled}, MissionCancelled},
	}
	for _, c := range cases {
		if got := MissionStatusOf(c.states); got != c.want {
			t.Errorf("%s: MissionStatusOf = %s, want %s", c.name, got, c.want)
		}
	}
}
