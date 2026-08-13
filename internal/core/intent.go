// TaskIntent 准入管道（M4-G3 + M5-H2，§7.1/§7.2）：归一化人/Agent 触发入口。
// 鉴权（agent source 强制 scope）→ 校验 → 幂等去重 → 实例化 → 事件留痕（actor=source）。
// 管道顺序对齐 §7.2：鉴权在去重之前——未授权重复提交不得经 deduplicated 侧漏
// Mission 存在性，且未授权请求不消耗幂等键去重窗口（M5 §3.7/3.8）。
package core

import (
	"context"
	"errors"
	"fmt"

	"agenttroop/internal/store"
)

// Intent 动作常量。
const (
	IntentCreateMission = "create_mission"
	IntentWake          = "wake"
)

// 触发 scope 常量（§7.4 能力分级授权；默认收紧、按授权放开）。
const (
	ScopeCreateMission = "trigger.create_mission"
	ScopeWake          = "trigger.wake"
	ScopeSpawnSubtask  = "trigger.spawn_subtask" // M6 预留：依赖主子委托协议（§15）
)

// ErrForbidden 触发鉴权失败（API 映射 403）：未注册 Agent 或缺少所需 scope。
var ErrForbidden = errors.New("core: forbidden")

// Intent 统一触发意图（API 触发 / Agent 主动触发归一化后的形态）。
type Intent struct {
	Source         store.Actor `json:"source"`           // 触发者（kind: human|agent）
	Action         string      `json:"action"`           // create_mission | wake
	IdempotencyKey string      `json:"idempotency_key"`  // create_mission 必填
	Owner          string      `json:"owner,omitempty"`  // create_mission
	Goal           string      `json:"goal,omitempty"`   // create_mission
	Tasks          []TaskSpec  `json:"tasks,omitempty"`  // create_mission
	SubtaskID      string      `json:"subtask_id,omitempty"` // wake
}

// IntentResult 准入结果。
type IntentResult struct {
	MissionID     string `json:"mission_id,omitempty"`
	SubtaskID     string `json:"subtask_id,omitempty"`
	Deduplicated  bool   `json:"deduplicated,omitempty"`
}

// SubmitIntent 准入管道：鉴权（agent source）→ 校验 → 幂等 → 实例化。
func (s *Service) SubmitIntent(ctx context.Context, in Intent) (*IntentResult, error) {
	if in.Source.ID == "" || in.Source.Kind == "" {
		return nil, fmt.Errorf("core: intent source (kind/id) required")
	}
	var need string
	switch in.Action {
	case IntentCreateMission:
		need = ScopeCreateMission
	case IntentWake:
		need = ScopeWake
	default:
		return nil, fmt.Errorf("core: unsupported intent action %q", in.Action)
	}
	// ① 鉴权先于去重（§7.2）：agent source 强制 scope；human 不鉴权（M5 §3.7，
	// 全库无认证定位不变，human 越权防护等 SSO/RBAC）
	if in.Source.Kind == "agent" {
		if err := s.authorizeAgentTrigger(ctx, in.Source.ID, need); err != nil {
			return nil, err
		}
	}
	switch in.Action {
	case IntentCreateMission:
		return s.intentCreateMission(ctx, in)
	case IntentWake:
		if in.SubtaskID == "" {
			return nil, fmt.Errorf("core: wake intent requires subtask_id")
		}
		if _, err := s.Wake(ctx, in.SubtaskID, in.Source.ID); err != nil {
			return nil, err
		}
		return &IntentResult{SubtaskID: in.SubtaskID}, nil
	}
	return nil, fmt.Errorf("core: unsupported intent action %q", in.Action)
}

// authorizeAgentTrigger 校验 Agent 已注册且持有所需触发 scope（§7.4：默认收紧）。
func (s *Service) authorizeAgentTrigger(ctx context.Context, agentID, scope string) error {
	a, err := s.st.GetAgent(ctx, agentID)
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("%w: unregistered agent %q", ErrForbidden, agentID)
	}
	if err != nil {
		return err
	}
	for _, sc := range a.TriggerScopes {
		if sc == scope {
			return nil
		}
	}
	return fmt.Errorf("%w: agent %q lacks scope %q", ErrForbidden, agentID, scope)
}

func (s *Service) intentCreateMission(ctx context.Context, in Intent) (*IntentResult, error) {
	if in.IdempotencyKey == "" {
		return nil, fmt.Errorf("core: create_mission intent requires idempotency_key")
	}
	if in.Owner == "" || in.Goal == "" || len(in.Tasks) == 0 {
		return nil, fmt.Errorf("core: owner/goal/tasks required")
	}
	if err := validateDAG(in.Tasks); err != nil { // 早失败：不占用幂等键
		return nil, err
	}
	// 预生成 ID 先落幂等键（result=missionID）：并发/重发同键只有一个赢家，
	// 其余直接返回已落键的 Mission，不产生孤儿 Mission。
	id := newID("msn")
	existing, err := s.st.PutIdempotent(ctx, "intent-"+in.IdempotencyKey, id, s.clk.Now())
	if errors.Is(err, store.ErrDuplicate) {
		return &IntentResult{MissionID: existing, Deduplicated: true}, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err := s.createMission(ctx, id, in.Source, in.Owner, in.Goal, in.Tasks); err != nil {
		return nil, err
	}
	return &IntentResult{MissionID: id}, nil
}
