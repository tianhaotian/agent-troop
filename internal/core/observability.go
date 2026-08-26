package core

import (
	"context"
	"time"

	"agenttroop/internal/mission"
)

type OperationalSnapshot struct {
	Subtasks         map[mission.State]int `json:"subtasks"`
	AgentsByHealth   map[string]int        `json:"agents_by_health"`
	PendingDecisions int                   `json:"pending_decisions"`
	CapturedAt       time.Time             `json:"captured_at"`
}

func (s *Service) ObservabilitySnapshot(ctx context.Context) (*OperationalSnapshot, error) {
	out := &OperationalSnapshot{Subtasks: map[mission.State]int{}, AgentsByHealth: map[string]int{},
		CapturedAt: s.clk.Now()}
	states := []mission.State{mission.StatePending, mission.StateReady, mission.StateOffered,
		mission.StateLeased, mission.StateRunning, mission.StateWaiting, mission.StateBlocked,
		mission.StateSucceeded, mission.StateFailed, mission.StateCancelled}
	for _, state := range states {
		subs, err := s.st.ListSubtasksByState(ctx, state)
		if err != nil {
			return nil, err
		}
		out.Subtasks[state] = len(subs)
	}
	agents, err := s.st.ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	for _, agent := range agents {
		health := agent.Health
		if health == "" {
			health = "unknown"
		}
		out.AgentsByHealth[health]++
	}
	decisions, err := s.st.ListDecisions(ctx, "", true)
	if err != nil {
		return nil, err
	}
	out.PendingDecisions = len(decisions)
	return out, nil
}
