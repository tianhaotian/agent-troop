// 挂起-唤醒（M3-T4，§7.3/§14.4）与检查点续跑（M3-T3，§5.4）。
// 语义决策见 docs/plan/M3-sched-trigger.md §3：
// - WAITING 释放租约（区别于 BLOCKED 保租约），唤醒后重新调度、可换 Agent 续跑；
// - wake_on 必带 TTL，过期 FAILED(wake_timeout) + 级联取消下游；
// - 唤醒一次性：CAS WAITING→READY，醒后注册清空，再等待须重新 suspend。
package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// MaxCheckpointSize 检查点载荷上限（§3.4：平台只透传不解释）。
const MaxCheckpointSize = 64 << 10

// WakeSpec suspend 请求的唤醒注册。
type WakeSpec struct {
	Kind     string     `json:"kind"` // timer | manual（event/condition 在 M4）
	At       *time.Time `json:"at,omitempty"`
	Deadline *time.Time `json:"deadline"` // TTL 必填
}

func validateWake(w WakeSpec) error {
	switch w.Kind {
	case mission.WakeTimer:
		if w.At == nil {
			return fmt.Errorf("core: timer wake requires at")
		}
	case mission.WakeManual:
	default:
		return fmt.Errorf("core: unsupported wake kind %q (m3: timer|manual)", w.Kind)
	}
	if w.Deadline == nil {
		return fmt.Errorf("core: wake deadline (TTL) is required")
	}
	if w.Kind == mission.WakeTimer && !w.Deadline.After(*w.At) {
		return fmt.Errorf("core: wake deadline must be after at")
	}
	return nil
}

// Suspend Agent 挂起自身：fencing 校验，RUNNING→WAITING，释放租约。
func (s *Service) Suspend(ctx context.Context, subtaskID string, fencingToken, version int64,
	agentID string, wake WakeSpec, checkpoint json.RawMessage) (*mission.Subtask, error) {
	if err := validateWake(wake); err != nil {
		return nil, err
	}
	if len(checkpoint) > MaxCheckpointSize {
		return nil, fmt.Errorf("core: checkpoint exceeds %d bytes", MaxCheckpointSize)
	}
	return s.st.SuspendSubtask(ctx, subtaskID, fencingToken, version,
		wake.Kind, wake.At, wake.Deadline, checkpoint,
		store.Actor{Kind: "agent", ID: agentID}, s.clk.Now())
}

// Wake 人工唤醒 WAITING 子任务（M3 无鉴权；scope 授权在 M4 准入管道引入）。
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

// sweepWakes SweepOnce 的唤醒段：timer 到期唤醒（CAS，多 sweeper 竞争安全）+
// TTL 过期置 FAILED 并级联取消下游、推导 Mission 终态。
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
