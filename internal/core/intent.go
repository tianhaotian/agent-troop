// TaskIntent 准入管道（M4-G3，§7.1/§7.2）：归一化人/Agent 触发入口。
// 校验 → 幂等去重 → 实例化（create_mission / wake）→ 事件留痕（actor=source）。
// scope 分级授权（§7.2）在 M5 鉴权引入后强制，M4 仅记录 source。
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

// SubmitIntent 准入管道：校验 → 幂等 → 实例化。
func (s *Service) SubmitIntent(ctx context.Context, in Intent) (*IntentResult, error) {
	if in.Source.ID == "" || in.Source.Kind == "" {
		return nil, fmt.Errorf("core: intent source (kind/id) required")
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
	default:
		return nil, fmt.Errorf("core: unsupported intent action %q", in.Action)
	}
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
