package core

// 人在回路（M2：H1–H3、H6）。语义决策见 docs/plan/M2-hitl.md §3：
// - 审批节点的"执行"即人的裁决：approve 经 RUNNING 落 SUCCEEDED（两步两事件，审计完整）；
// - choice 约定：唯一否决值 "reject"，其余任何 choice 视为批准并作为决策内容下发；
// - Agent 决策请求 BLOCKED 不释放租约，批准后续跑。

import (
	"context"
	"fmt"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// RejectChoice 唯一否决值（其余 choice 均视为批准）。
const RejectChoice = "reject"

// isHumanKind 判断子任务是否需要人执行。
func isHumanKind(k mission.Kind) bool {
	return k == mission.KindHumanApproval || k == mission.KindHumanDecision
}

// OpenHumanDecisions 扫描 READY 的 human 节点：创建裁决工单并置 BLOCKED。
// 在 ScheduleOnce 每轮调用（human 节点不参与 Agent 放置）。
func (s *Service) OpenHumanDecisions(ctx context.Context) (int, error) {
	ready, err := s.st.ListSubtasksByState(ctx, mission.StateReady)
	if err != nil {
		return 0, err
	}
	now := s.clk.Now()
	opened := 0
	for _, sub := range ready {
		if !isHumanKind(sub.Kind) {
			continue
		}
		kind := "approval"
		if sub.Kind == mission.KindHumanDecision {
			kind = "decision"
		}
		question := sub.Question
		if question == "" {
			question = fmt.Sprintf("Approve subtask %s?", sub.ID)
		}
		options := sub.Options
		if len(options) == 0 {
			options = []string{"approve", RejectChoice}
		}
		d := &store.Decision{
			ID:        newID("dec"),
			MissionID: sub.MissionID,
			SubtaskID: sub.ID,
			Kind:      kind,
			Question:  question,
			Options:   options,
			Deadline:  sub.Scheduling.Deadline,
			OnTimeout: sub.OnTimeout,
		}
		// 先迁移 READY→BLOCKED（CAS 保证单胜者），再落工单
		if _, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvBlocked, sub.Version,
			store.Actor{Kind: "system", ID: "hitl"},
			map[string]any{"question": question}, now, nil); err != nil {
			continue // 并发下已被处理
		}
		if err := s.st.CreateDecision(ctx, d, now); err != nil {
			return opened, err
		}
		opened++
	}
	return opened, nil
}

// RequestDecision Agent 主动请求人决策（fencing 校验；RUNNING→BLOCKED，租约保留）。
func (s *Service) RequestDecision(ctx context.Context, subtaskID string, fencingToken, version int64,
	agentID, question string, options []string) (*store.Decision, error) {
	now := s.clk.Now()
	sub, err := s.st.BlockSubtask(ctx, subtaskID, fencingToken, version,
		store.Actor{Kind: "agent", ID: agentID}, now)
	if err != nil {
		return nil, err
	}
	if len(options) == 0 {
		options = []string{"approve", RejectChoice}
	}
	d := &store.Decision{
		ID:        newID("dec"),
		MissionID: sub.MissionID,
		SubtaskID: sub.ID,
		Kind:      "decision",
		Question:  question,
		Options:   options,
	}
	if err := s.st.CreateDecision(ctx, d, now); err != nil {
		return nil, err
	}
	return d, nil
}

// ResolveDecision 人裁决工单并驱动子任务流转。
func (s *Service) ResolveDecision(ctx context.Context, decisionID, choice, rationale, deciderID string) (*store.Decision, error) {
	d, err := s.st.ResolveDecision(ctx, decisionID, choice, rationale, deciderID, s.clk.Now())
	if err != nil {
		return nil, err
	}
	return d, s.applyDecisionOutcome(ctx, d)
}

// applyDecisionOutcome 按裁决结果流转子任务（裁决已落库后调用；超时自动裁决也走这里）。
func (s *Service) applyDecisionOutcome(ctx context.Context, d *store.Decision) error {
	now := s.clk.Now()
	actor := store.Actor{Kind: "human", ID: d.DeciderID}
	subs, err := s.st.ListSubtasks(ctx, d.MissionID)
	if err != nil {
		return err
	}
	var sub *mission.Subtask
	for _, x := range subs {
		if x.ID == d.SubtaskID {
			sub = x
			break
		}
	}
	if sub == nil {
		return store.ErrNotFound
	}
	if sub.State != mission.StateBlocked {
		return nil // 已被并发路径处理
	}

	if d.Choice == RejectChoice {
		rejected, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvDecisionRejected, sub.Version,
			actor, map[string]any{"choice": d.Choice, "rationale": d.Rationale}, now, nil)
		if err != nil {
			return err
		}
		if err := s.cancelUnreachable(ctx, rejected, actor); err != nil {
			return err
		}
		return s.deriveMissionStatus(ctx, d.MissionID)
	}

	// 批准：BLOCKED → RUNNING
	approved, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvDecisionApproved, sub.Version,
		actor, map[string]any{"choice": d.Choice, "rationale": d.Rationale}, now, nil)
	if err != nil {
		return err
	}
	// human 节点：裁决即完成，再落 completed → SUCCEEDED（审计两步，见计划 §3.1）
	if isHumanKind(sub.Kind) {
		done, err := s.st.TransitionSubtask(ctx, sub.ID, mission.EvCompleted, approved.Version,
			actor, map[string]any{"via": "human_decision"}, now, func(st *mission.Subtask) error {
				st.ResultRef = "decision://" + d.ID
				return nil
			})
		if err != nil {
			return err
		}
		return s.propagate(ctx, done, actor)
	}
	return nil // agent 节点：回 RUNNING 续跑
}

// GetDecision / ListDecisions 查询直通。
func (s *Service) GetDecision(ctx context.Context, id string) (*store.Decision, error) {
	return s.st.GetDecision(ctx, id)
}

func (s *Service) ListDecisions(ctx context.Context, missionID string, pendingOnly bool) ([]*store.Decision, error) {
	return s.st.ListDecisions(ctx, missionID, pendingOnly)
}
