// 挂起-唤醒（M3/M4，§7.3/§14.2-14.4）与检查点续跑（§5.4）。
// 语义决策见 docs/plan/M3-sched-trigger.md §3 与 M4-trigger-pipeline.md §3：
// - WAITING 释放租约（区别于 BLOCKED 保租约），唤醒后重新调度、可换 Agent 续跑；
// - wake_on 必带 TTL，过期 FAILED(wake_timeout) + 级联取消下游（所有 kind 统一）；
// - 唤醒一次性：CAS WAITING→READY，醒后注册清空，再等待须重新 suspend；
// - event 唤醒以事件 seq 水位线界定"注册之后"（重启安全、无时间戳歧义）。
package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// MaxCheckpointSize 检查点载荷上限（§3.4：平台只透传不解释）。
const MaxCheckpointSize = 64 << 10

func validateWake(w *mission.WakeSpec) error {
	switch w.Kind {
	case mission.WakeTimer:
		if w.At == nil {
			return fmt.Errorf("core: timer wake requires at")
		}
	case mission.WakeManual:
	case mission.WakeEvent:
		if w.Event == nil || len(w.Event.Types) == 0 {
			return fmt.Errorf("core: event wake requires event.types")
		}
	case mission.WakeCondition:
		if err := validateCondition(w.Condition); err != nil {
			return err
		}
	default:
		return fmt.Errorf("core: unsupported wake kind %q", w.Kind)
	}
	if w.Deadline == nil {
		return fmt.Errorf("core: wake deadline (TTL) is required")
	}
	if w.Kind == mission.WakeTimer && !w.Deadline.After(*w.At) {
		return fmt.Errorf("core: wake deadline must be after at")
	}
	return nil
}

func validateCondition(c *mission.BoardCondition) error {
	if c == nil {
		return fmt.Errorf("core: condition wake requires condition")
	}
	// M5 §3.1：expr 与结构化谓词互斥——同现或同缺均拒绝
	hasExpr := c.Expr != ""
	hasStruct := c.Board != "" || c.Op != ""
	if hasExpr && hasStruct {
		return fmt.Errorf("%w: expr and board/op are mutually exclusive", ErrInvalidCondition)
	}
	if hasExpr {
		cc, err := compileConditionExpr(c.Expr)
		if err != nil {
			return err
		}
		// 引用键集随 wake_spec 持久化（增量过滤与可观测性用）
		c.Refs, c.RefsWildcard = cc.refs, cc.wildcard
		return nil
	}
	if !hasStruct {
		return fmt.Errorf("core: condition requires expr or board/op")
	}
	parts := strings.SplitN(c.Board, "/", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return fmt.Errorf("core: condition.board must be \"namespace/key\"")
	}
	switch c.Op {
	case mission.CondExists:
	case mission.CondEquals:
		if c.Value == nil {
			return fmt.Errorf("core: equals condition requires value")
		}
	default:
		return fmt.Errorf("core: unsupported condition op %q (exists|equals)", c.Op)
	}
	return nil
}

// Suspend Agent 挂起自身：fencing 校验，RUNNING→WAITING，释放租约。
// event 唤醒在此写入水位线（after_seq = 当前最大事件 seq）。
func (s *Service) Suspend(ctx context.Context, subtaskID string, fencingToken, version int64,
	agentID string, wake *mission.WakeSpec, checkpoint json.RawMessage) (*mission.Subtask, error) {
	if err := validateWake(wake); err != nil {
		return nil, err
	}
	if len(checkpoint) > MaxCheckpointSize {
		return nil, fmt.Errorf("core: checkpoint exceeds %d bytes", MaxCheckpointSize)
	}
	if wake.Kind == mission.WakeEvent {
		seq, err := s.st.MaxEventSeq(ctx)
		if err != nil {
			return nil, err
		}
		wake.Event.AfterSeq = seq
	}
	now := s.clk.Now()
	wake.RegisteredAt = &now // CEL elapsed() 基准（§14.3 逻辑时钟注入）
	return s.st.SuspendSubtask(ctx, subtaskID, fencingToken, version, wake, checkpoint,
		store.Actor{Kind: "agent", ID: agentID}, now)
}

// Wake 人工唤醒 WAITING 子任务（M3 无鉴权；scope 授权在 M5 准入管道引入）。
func (s *Service) Wake(ctx context.Context, subtaskID, actorID string) (*mission.Subtask, error) {
	sub, err := s.st.ListSubtasksByState(ctx, mission.StateWaiting)
	if err != nil {
		return nil, err
	}
	for _, x := range sub {
		if x.ID == subtaskID {
			return s.st.WakeSubtask(ctx, subtaskID, x.Version,
				store.Actor{Kind: "human", ID: actorID}, s.clk.Now())
		}
	}
	return nil, store.ErrNotFound
}

// Progress progress 心跳：续租 + 检查点落库（fencing 校验在 store 层）。
func (s *Service) Progress(ctx context.Context, subtaskID, leaseID string, fencingToken int64,
	checkpoint json.RawMessage) error {
	if len(checkpoint) > MaxCheckpointSize {
		return fmt.Errorf("core: checkpoint exceeds %d bytes", MaxCheckpointSize)
	}
	if err := s.st.RenewLease(ctx, leaseID, fencingToken, 2*s.cfg.OfferTTL, s.clk.Now()); err != nil {
		return err
	}
	if len(checkpoint) == 0 {
		return nil
	}
	return s.st.SaveCheckpoint(ctx, subtaskID, fencingToken, checkpoint, s.clk.Now())
}

// ---- 求值器（G1/G2） ----

// wakeSpecOf 解码子任务的唤醒注册。
func wakeSpecOf(sub *mission.Subtask) *mission.WakeSpec {
	if len(sub.WakeSpec) == 0 {
		return nil
	}
	var w mission.WakeSpec
	if err := json.Unmarshal(sub.WakeSpec, &w); err != nil {
		return nil
	}
	return &w
}

// matchWhere 子集等值匹配：where 的每个点路径键在载荷中存在且 JSON 等值。
func matchWhere(payload map[string]any, where map[string]any) bool {
	for path, want := range where {
		cur := any(payload)
		for _, seg := range strings.Split(path, ".") {
			m, ok := cur.(map[string]any)
			if !ok {
				return false
			}
			if cur, ok = m[seg]; !ok {
				return false
			}
		}
		if !reflect.DeepEqual(cur, want) {
			return false
		}
	}
	return true
}

// evalEventWakes 事件唤醒评估（edge-fired）：扫描 event 等待的注册，
// 只匹配水位线之后到达的同 Mission 事件；命中即 CAS 唤醒（恰好一次由 CAS 仲裁）。
func (s *Service) evalEventWakes(ctx context.Context) error {
	waiting, err := s.st.ListWaiting(ctx, mission.WakeEvent)
	if err != nil {
		return err
	}
	for _, sub := range waiting {
		w := wakeSpecOf(sub)
		if w == nil || w.Event == nil {
			continue
		}
		evs, err := s.st.ListMissionEvents(ctx, sub.MissionID, w.Event.AfterSeq, 200)
		if err != nil {
			return err
		}
		for _, e := range evs {
			matched := false
			for _, typ := range w.Event.Types {
				if e.Type == typ {
					matched = true
					break
				}
			}
			if !matched || !matchWhere(e.Payload, w.Event.Where) {
				continue
			}
			// CAS 竞争失败说明已被另一路唤醒，直接看下一个注册
			_, _ = s.st.WakeSubtask(ctx, sub.ID, sub.Version,
				store.Actor{Kind: "system", ID: "trigger"}, s.clk.Now())
			break
		}
	}
	return nil
}

// evalCondition 条件求值分发（M5-H1）：expr 走 CEL 内核，否则走结构化谓词
//（M4 路径，行为逐字节不变）。
func (s *Service) evalCondition(ctx context.Context, sub *mission.Subtask, w *mission.WakeSpec) (bool, error) {
	if w.Condition.Expr != "" {
		return s.evalConditionExpr(ctx, sub, w)
	}
	return s.evalStructuredCondition(ctx, sub.MissionID, w.Condition)
}

// evalStructuredCondition 结构化谓词求值（exists/equals，M4）。
func (s *Service) evalStructuredCondition(ctx context.Context, missionID string, c *mission.BoardCondition) (bool, error) {
	parts := strings.SplitN(c.Board, "/", 2)
	entry, err := s.st.BoardGet(ctx, missionID, parts[0], parts[1])
	if err != nil {
		if err == store.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	if c.Op == mission.CondExists {
		return true, nil
	}
	// equals：JSON 规范化比较（两侧重新编码，map 键排序后字节等值）
	want, err := json.Marshal(c.Value)
	if err != nil {
		return false, err
	}
	var gotNorm any
	if err := json.Unmarshal(entry.Value, &gotNorm); err != nil {
		return false, nil // 非 JSON 值不等于任何结构化期望值
	}
	got, err := json.Marshal(gotNorm)
	if err != nil {
		return false, err
	}
	return bytes.Equal(want, got), nil
}

// conditionRefsChanged 增量过滤（M5 §3.6）：changedBoard 是否影响该注册。
// 结构化谓词按键等值比对；CEL 注册按静态引用键集比对，wildcard/提取失败恒真。
// 只引用 mission.*/subtask.*/时钟函数的注册键集为空——BoardPut 不改变这些输入，
// 增量评估跳过，由 sweeper 全量兜底（§14.4 level-triggered 语义由兜底保证）。
func conditionRefsChanged(c *mission.BoardCondition, changedBoard string) bool {
	if c.Expr == "" {
		return c.Board == changedBoard
	}
	cc, err := compileConditionExpr(c.Expr)
	if err != nil {
		return true // 无法判定：宁多评不漏评
	}
	return cc.referencesBoard(changedBoard)
}

// evalConditionWakes 条件唤醒评估。changedBoard 非空时为增量模式
// （BoardPut 钩子：只评估该 Mission 内引用该 ns/key 的注册）；为空为全量兜底（sweeper）。
func (s *Service) evalConditionWakes(ctx context.Context, missionID, changedBoard string) error {
	waiting, err := s.st.ListWaiting(ctx, mission.WakeCondition)
	if err != nil {
		return err
	}
	for _, sub := range waiting {
		w := wakeSpecOf(sub)
		if w == nil || w.Condition == nil {
			continue
		}
		if changedBoard != "" && (sub.MissionID != missionID || !conditionRefsChanged(w.Condition, changedBoard)) {
			continue
		}
		ok, err := s.evalCondition(ctx, sub, w)
		if errors.Is(err, errConditionCostExceeded) {
			// §14.3"超限视为 false 并告警"：不唤醒，落事件留痕
			_ = s.st.AppendEvent(ctx, &store.Event{
				AggregateID: sub.ID, MissionID: sub.MissionID,
				Type: "condition.cost_exceeded",
				Payload: map[string]any{
					"subtask_id": sub.ID, "expr": w.Condition.Expr,
					"limit": MaxConditionRuntimeCost,
				},
				Actor: store.Actor{Kind: "system", ID: "trigger"},
				Ts:    s.clk.Now(),
			})
			continue
		}
		if err != nil {
			return err
		}
		if ok {
			_, _ = s.st.WakeSubtask(ctx, sub.ID, sub.Version,
				store.Actor{Kind: "system", ID: "trigger"}, s.clk.Now())
		}
	}
	return nil
}

// sweepWakes SweepOnce 的唤醒段：timer 到期唤醒 + event/condition 全量兜底评估 +
// TTL 过期置 FAILED 并级联取消下游、推导 Mission 终态。所有 CAS 竞争安全。
func (s *Service) sweepWakes(ctx context.Context) error {
	now := s.clk.Now()
	due, err := s.st.ListWaitingDue(ctx, now)
	if err != nil {
		return err
	}
	for _, sub := range due {
		// 冲突忽略：另一 sweeper 已唤醒（恰好一次由 CAS 保证）
		_, _ = s.st.WakeSubtask(ctx, sub.ID, sub.Version,
			store.Actor{Kind: "system", ID: "trigger"}, now)
	}
	if err := s.evalEventWakes(ctx); err != nil {
		return err
	}
	if err := s.evalConditionWakes(ctx, "", ""); err != nil { // anti-entropy 兜底
		return err
	}
	expired, err := s.st.ExpireWakes(ctx, now)
	if err != nil {
		return err
	}
	actor := store.Actor{Kind: "system", ID: "sweeper"}
	for _, sub := range expired {
		if err := s.cancelUnreachable(ctx, sub, actor); err != nil {
			return err
		}
		if err := s.deriveMissionStatus(ctx, sub.MissionID); err != nil {
			return err
		}
	}
	return nil
}
