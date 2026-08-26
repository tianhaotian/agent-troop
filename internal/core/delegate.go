// 主子委托（M6，§6.4.2/§15.1）：Lead Agent 在 RUNNING 中派生子女任务。
// 语义决策见 docs/plan/M6-delegate.md §3：
//   - 委托关系用 parent_id 表达（非 DAG 依赖），子女即建即 READY 参与调度；
//   - fencing 即委托权：只有持父任务活跃租约的 Lead 能 delegate；
//   - 幂等 + fencing + 落库在 store.SpawnSubtask 一个原子操作内（CompleteSubtask 同构）；
//   - rework 为链式重派（rework_of + feedback），不改状态机终态不可逆假设。
package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// IntentDelegate 主子委托动作（intent action=delegate）。
const IntentDelegate = "delegate"

// DelegateSpec delegate 的子女任务声明（TaskSpec 的子集 + rework 链）。
type DelegateSpec struct {
	Name           string                     `json:"name,omitempty"` // 缺省平台生成
	RequiredSkills []string                   `json:"required_skills,omitempty"`
	Input          map[string]any             `json:"input,omitempty"`
	Priority       int                        `json:"priority,omitempty"`
	Deadline       *time.Time                 `json:"deadline,omitempty"`
	MaxAttempts    int                        `json:"max_attempts,omitempty"`
	BudgetTokens   int64                      `json:"budget_tokens,omitempty"` // Mission 账户中的原子预占切片
	Grants         mission.PermissionEnvelope `json:"grants,omitempty"`
	// rework（M6-K2）：对 Lead 自己派生的子女验收不通过时的链式重派
	ReworkOf string `json:"rework_of,omitempty"`
	Feedback string `json:"feedback,omitempty"` // 结构化反馈，入子女 input.feedback
}

// intentDelegate 委托编排水线：core 校验（父任务/Mission/depth/fanout/rework 链，
// 早失败不耗键）→ store.SpawnSubtask 原子落库并推进 READY。
func (s *Service) intentDelegate(ctx context.Context, in Intent) (*IntentResult, error) {
	if in.IdempotencyKey == "" {
		return nil, fmt.Errorf("core: delegate intent requires idempotency_key")
	}
	if in.ParentSubtaskID == "" || in.Task == nil {
		return nil, fmt.Errorf("core: delegate intent requires parent_subtask_id and task")
	}
	if in.Source.Kind != "agent" {
		return nil, fmt.Errorf("%w: delegate requires an agent source", ErrForbidden)
	}
	if in.Task.BudgetTokens < 0 {
		return nil, fmt.Errorf("%w: task.budget_tokens must be >= 0", ErrInvalidBudget)
	}
	grants, err := mission.NormalizePermissionEnvelope(in.Task.Grants)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidPermission, err)
	}
	// 父任务须 RUNNING（委托发生于 Lead 执行中）；找不到即非 RUNNING 或不存在
	parent, err := s.findRunning(ctx, in.ParentSubtaskID)
	if err != nil {
		return nil, err
	}
	if _, err := s.authorizeSubtaskLeaseOwner(ctx, parent.ID, in.FencingToken, in.Source.ID, true); err != nil {
		return nil, err
	}
	m, err := s.st.GetMission(ctx, parent.MissionID)
	if err != nil {
		return nil, err
	}
	if m.Status != mission.MissionActive {
		return nil, fmt.Errorf("core: delegate into terminal mission %s rejected", m.Status)
	}
	if !mission.PermissionEnvelopeSubset(parent.Grants, grants) {
		return nil, fmt.Errorf("%w: delegated grants exceed parent envelope", ErrInvalidPermission)
	}
	subs, err := s.st.ListSubtasks(ctx, parent.MissionID)
	if err != nil {
		return nil, err
	}
	if err := s.checkDelegateLimits(ctx, parent, in.Task, subs); err != nil {
		return nil, err
	}
	child := &mission.Subtask{
		ID:             newID("sub"),
		MissionID:      parent.MissionID,
		ParentID:       parent.ID,
		Kind:           mission.KindAgent,
		RequiredSkills: in.Task.RequiredSkills,
		Input:          in.Task.Input,
		ReworkOf:       in.Task.ReworkOf,
		Grants:         grants,
		Scheduling: mission.SchedulingSpec{
			Priority: in.Task.Priority, Deadline: in.Task.Deadline, BudgetTokens: in.Task.BudgetTokens,
		},
		Retry: mission.RetryPolicy{MaxAttempts: in.Task.MaxAttempts, OnFailure: "retry"},
	}
	if in.Task.Name != "" {
		child.ID = subID(parent.MissionID, in.Task.Name) // 与 Mission 创建命名一致
	}
	if in.Task.Feedback != "" {
		if child.Input == nil {
			child.Input = map[string]any{}
		}
		child.Input["feedback"] = in.Task.Feedback
	}
	// 独立幂等前缀：与 create_mission 的键空间隔离（同键跨动作不会互相侧漏）
	existing, err := s.st.SpawnSubtask(ctx, "intent-delegate-"+in.IdempotencyKey,
		parent.ID, in.FencingToken, in.ParentVersion, child,
		store.Actor{Kind: in.Source.Kind, ID: in.Source.ID}, s.clk.Now())
	if errors.Is(err, store.ErrDuplicate) {
		return &IntentResult{MissionID: parent.MissionID, SubtaskID: existing, Deduplicated: true}, nil
	}
	if err != nil {
		return nil, err
	}
	// SpawnSubtask 在同一事务内创建并激活子女，避免 PENDING 孤儿窗口。
	return &IntentResult{MissionID: parent.MissionID, SubtaskID: child.ID}, nil
}

// findRunning 按 ID 定位 RUNNING 子任务（委托父任务前置校验）。
func (s *Service) findRunning(ctx context.Context, subtaskID string) (*mission.Subtask, error) {
	running, err := s.st.ListSubtasksByState(ctx, mission.StateRunning)
	if err != nil {
		return nil, err
	}
	for _, sub := range running {
		if sub.ID == subtaskID {
			return sub, nil
		}
	}
	return nil, fmt.Errorf("core: parent subtask %s not RUNNING (delegate requires active execution)", subtaskID)
}

// checkDelegateLimits 结构校验：depth（沿 parent_id 链）/ fanout / rework 链上限。
func (s *Service) checkDelegateLimits(ctx context.Context, parent *mission.Subtask, task *DelegateSpec,
	subs []*mission.Subtask) error {
	byID := map[string]*mission.Subtask{}
	for _, sub := range subs {
		byID[sub.ID] = sub
	}
	// depth：父链深度 + 1 超限即拒（上溯步数封顶防环——环本身意味着数据损坏）
	depth := 1
	for cur, steps := parent, 0; cur.ParentID != "" && steps <= s.cfg.MaxDelegateDepth+1; steps++ {
		depth++
		cur = byID[cur.ParentID]
		if cur == nil {
			break
		}
	}
	if depth > s.cfg.MaxDelegateDepth {
		return fmt.Errorf("core: delegate depth %d exceeds max %d", depth, s.cfg.MaxDelegateDepth)
	}
	// fanout：同父直接子女计数
	fanout, err := s.st.CountChildren(ctx, parent.ID)
	if err != nil {
		return err
	}
	if fanout >= s.cfg.MaxDelegateFanout {
		return fmt.Errorf("core: delegate fanout %d reaches max %d", fanout, s.cfg.MaxDelegateFanout)
	}
	// rework 链：只能返工自己派生的子女；链长达上限即拒（Lead 应换方案/升级人决策）
	if task.ReworkOf != "" {
		origin := byID[task.ReworkOf]
		if origin == nil {
			return fmt.Errorf("core: rework_of %s not found in mission", task.ReworkOf)
		}
		if origin.ParentID != parent.ID {
			return fmt.Errorf("core: rework_of %s is not delegated by this parent", task.ReworkOf)
		}
		// chain = 新节点的返工序号（origin 的链位 + 1）：原子任务为 0，首次返工为 1
		chain := 1
		for cur := origin; cur.ReworkOf != ""; {
			next := byID[cur.ReworkOf]
			if next == nil {
				break
			}
			chain++
			cur = next
		}
		if chain > s.cfg.MaxRework {
			return fmt.Errorf("core: rework chain %d exceeds max %d", chain, s.cfg.MaxRework)
		}
	}
	return nil
}
