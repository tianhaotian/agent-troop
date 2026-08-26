package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"agenttroop/internal/store"
)

var ErrInvalidProductInput = errors.New("core: invalid product input")

type SimulationInput struct {
	Scenario     string  `json:"scenario"`
	Seed         uint64  `json:"seed"`
	Tasks        int     `json:"tasks"`
	Agents       int     `json:"agents"`
	FailureRate  float64 `json:"failure_rate,omitempty"`
	ChaosRate    float64 `json:"chaos_rate,omitempty"`
	ExpectedCost float64 `json:"expected_cost,omitempty"`
}

type SimulationReport struct {
	Scenario         string  `json:"scenario"`
	Seed             uint64  `json:"seed"`
	Tasks            int     `json:"tasks"`
	Agents           int     `json:"agents"`
	Succeeded        int     `json:"succeeded"`
	Failed           int     `json:"failed"`
	SuccessRate      float64 `json:"success_rate"`
	P50LatencyMS     int     `json:"p50_latency_ms"`
	P95LatencyMS     int     `json:"p95_latency_ms"`
	LeaseExpirations int     `json:"lease_expirations"`
	MeanRecoveryMS   float64 `json:"mean_recovery_ms"`
	ActualCost       float64 `json:"actual_cost"`
	CostDeviation    float64 `json:"cost_deviation"`
	LoadGini         float64 `json:"load_gini"`
	StateHash        string  `json:"state_hash"`
}

// RunSimulation is intentionally pure: the same input always yields the same report.
func RunSimulation(in SimulationInput) (*SimulationReport, error) {
	if in.Scenario == "" {
		in.Scenario = "harness"
	}
	if in.Scenario != "harness" && in.Scenario != "shadow" && in.Scenario != "load" && in.Scenario != "chaos" {
		return nil, fmt.Errorf("%w: unsupported scenario", ErrInvalidProductInput)
	}
	if in.Tasks < 1 || in.Tasks > 100000 || in.Agents < 1 || in.Agents > 10000 ||
		in.FailureRate < 0 || in.FailureRate > 1 || in.ChaosRate < 0 || in.ChaosRate > 1 || in.ExpectedCost < 0 {
		return nil, fmt.Errorf("%w: tasks/agents/rates/cost out of range", ErrInvalidProductInput)
	}
	if in.Seed == 0 {
		in.Seed = 1
	}
	rng := xorshift64{state: in.Seed}
	latencies := make([]int, in.Tasks)
	loads := make([]int, in.Agents)
	recovery, cost := 0, 0.0
	report := &SimulationReport{Scenario: in.Scenario, Seed: in.Seed, Tasks: in.Tasks, Agents: in.Agents}
	for i := 0; i < in.Tasks; i++ {
		agent := int(rng.next() % uint64(in.Agents))
		loads[agent]++
		latency := 40 + int(rng.next()%961)
		chaos := float64(rng.next()%1000000)/1000000 < in.ChaosRate
		failed := float64(rng.next()%1000000)/1000000 < in.FailureRate
		if in.Scenario == "chaos" && chaos {
			report.LeaseExpirations++
			delta := 250 + int(rng.next()%1751)
			recovery += delta
			latency += delta
		}
		latencies[i] = latency
		cost += float64(latency)*0.000001 + float64(200+rng.next()%1801)*0.000002
		if failed {
			report.Failed++
		} else {
			report.Succeeded++
		}
	}
	sort.Ints(latencies)
	report.SuccessRate = float64(report.Succeeded) / float64(in.Tasks)
	report.P50LatencyMS = percentile(latencies, 0.50)
	report.P95LatencyMS = percentile(latencies, 0.95)
	if report.LeaseExpirations > 0 {
		report.MeanRecoveryMS = float64(recovery) / float64(report.LeaseExpirations)
	}
	report.ActualCost = round6(cost)
	if in.ExpectedCost > 0 {
		report.CostDeviation = round6((report.ActualCost - in.ExpectedCost) / in.ExpectedCost)
	}
	report.LoadGini = round6(gini(loads))
	canonical, _ := json.Marshal(struct {
		Input     SimulationInput `json:"input"`
		Success   int             `json:"success"`
		Failure   int             `json:"failure"`
		Latencies []int           `json:"latencies"`
		Loads     []int           `json:"loads"`
		Expired   int             `json:"expired"`
	}{in, report.Succeeded, report.Failed, latencies, loads, report.LeaseExpirations})
	sum := sha256.Sum256(canonical)
	report.StateHash = hex.EncodeToString(sum[:])
	return report, nil
}

type xorshift64 struct{ state uint64 }

func (x *xorshift64) next() uint64 {
	x.state ^= x.state << 13
	x.state ^= x.state >> 7
	x.state ^= x.state << 17
	return x.state
}
func percentile(v []int, p float64) int { return v[int(math.Ceil(float64(len(v))*p))-1] }
func round6(v float64) float64          { return math.Round(v*1e6) / 1e6 }
func gini(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	v := append([]int(nil), values...)
	sort.Ints(v)
	var sum, weighted float64
	for i, n := range v {
		sum += float64(n)
		weighted += float64((i + 1) * n)
	}
	if sum == 0 {
		return 0
	}
	return 2*weighted/(float64(len(v))*sum) - float64(len(v)+1)/float64(len(v))
}

type MarketplaceQuery struct {
	Skill, Platform string
	HealthyOnly     bool
	MinReputation   float64
}

func (s *Service) DiscoverAgents(ctx context.Context, q MarketplaceQuery) ([]*store.Agent, error) {
	if q.MinReputation < 0 || q.MinReputation > 1 {
		return nil, fmt.Errorf("%w: min_reputation out of range", ErrInvalidProductInput)
	}
	agents, err := s.st.ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	var out []*store.Agent
	for _, a := range agents {
		if q.Platform != "" && a.Platform != q.Platform || q.HealthyOnly && a.Health != "healthy" {
			continue
		}
		level, matched := 0.0, q.Skill == ""
		for _, c := range a.Capabilities {
			if q.Skill == "" || c.Skill == q.Skill {
				matched = true
				if c.Level > level {
					level = c.Level
				}
			}
		}
		if !matched {
			continue
		}
		reps, err := s.st.ListReputations(ctx, a.ID)
		if err != nil {
			return nil, err
		}
		a.Reputation = map[string]*store.ReputationRecord{}
		score := 0.5
		for _, rep := range reps {
			a.Reputation[rep.Skill] = rep
			if rep.Skill == q.Skill || q.Skill == "" && rep.CompositeScore > score {
				score = rep.CompositeScore
			}
		}
		if q.MinReputation > 0 && score < q.MinReputation {
			continue
		}
		if q.Skill != "" && level == 0 {
			continue
		}
		out = append(out, a)
	}
	sort.SliceStable(out, func(i, j int) bool {
		si, sj := marketplaceScore(out[i], q.Skill), marketplaceScore(out[j], q.Skill)
		if si == sj {
			return out[i].ID < out[j].ID
		}
		return si > sj
	})
	return out, nil
}

func marketplaceScore(a *store.Agent, skill string) float64 {
	capScore := 0.0
	for _, c := range a.Capabilities {
		if skill == "" || c.Skill == skill {
			capScore = math.Max(capScore, c.Level)
		}
	}
	rep := 0.5
	for k, r := range a.Reputation {
		if k == skill || skill == "" && r.CompositeScore > rep {
			rep = r.CompositeScore
		}
	}
	return 0.6*capScore + 0.4*rep
}

type CanaryInput struct {
	ID, VerifierAgentID, Skill, ExpectedVerdict, ActualVerdict string
	ExpectedScore, ActualScore                                 float64
}
type CanaryResult struct {
	ID         string  `json:"id"`
	Match      bool    `json:"match"`
	ScoreDelta float64 `json:"score_delta"`
	SignalID   string  `json:"signal_id"`
}

func (s *Service) EvaluateCanary(ctx context.Context, in CanaryInput) (*CanaryResult, error) {
	if strings.TrimSpace(in.ID) == "" || strings.TrimSpace(in.VerifierAgentID) == "" || in.ExpectedScore < 0 || in.ExpectedScore > 1 || in.ActualScore < 0 || in.ActualScore > 1 {
		return nil, fmt.Errorf("%w: invalid canary", ErrInvalidProductInput)
	}
	match := in.ExpectedVerdict == in.ActualVerdict && math.Abs(in.ExpectedScore-in.ActualScore) <= 0.1
	quality := 1 - math.Min(1, math.Abs(in.ExpectedScore-in.ActualScore))
	skill := in.Skill
	if skill == "" {
		skill = "verify"
	}
	sigID := "canary:" + in.ID + ":" + in.VerifierAgentID
	sig := store.ReputationSignal{ID: sigID, AgentID: in.VerifierAgentID, Skill: skill, Success: &match, Reliable: &match, Quality: &quality, Weight: 0.1, EventRef: "canary:" + in.ID}
	if err := s.st.ApplyReputationSignal(ctx, sig, s.clk.Now()); err != nil && !errors.Is(err, store.ErrDuplicate) {
		return nil, err
	}
	s.invalidateReputation(in.VerifierAgentID)
	return &CanaryResult{ID: in.ID, Match: match, ScoreDelta: round6(in.ActualScore - in.ExpectedScore), SignalID: sigID}, nil
}

func (s *Service) CreateQualityAppeal(ctx context.Context, artifactID, appellantID, reason string, refs []string, actor store.Actor) (*store.QualityAppeal, error) {
	if strings.TrimSpace(appellantID) == "" || strings.TrimSpace(reason) == "" {
		return nil, fmt.Errorf("%w: appellant_id and reason required", ErrInvalidProductInput)
	}
	a := &store.QualityAppeal{ID: newID("apl"), ArtifactID: artifactID, AppellantID: appellantID, Reason: reason, EvidenceRefs: refs}
	if err := s.st.CreateQualityAppeal(ctx, a, actor, s.clk.Now()); err != nil {
		return nil, err
	}
	return a, nil
}
func (s *Service) ListQualityAppeals(ctx context.Context, missionID string, pendingOnly bool) ([]*store.QualityAppeal, error) {
	return s.st.ListQualityAppeals(ctx, missionID, pendingOnly)
}
func (s *Service) ResolveQualityAppeal(ctx context.Context, id, status, resolution, reviewer string, actor store.Actor) (*store.QualityAppeal, error) {
	if status != store.AppealUpheld && status != store.AppealOverturned || strings.TrimSpace(reviewer) == "" || strings.TrimSpace(resolution) == "" {
		return nil, fmt.Errorf("%w: status, reviewer and resolution required", ErrInvalidProductInput)
	}
	a, err := s.st.ResolveQualityAppeal(ctx, id, status, resolution, reviewer, actor, s.clk.Now())
	if err != nil {
		return nil, err
	}
	if status == store.AppealOverturned {
		q, err := s.st.GetQuality(ctx, a.ArtifactID)
		if err == nil && q.ProducerAgentID != "" {
			sub, _ := s.st.GetSubtask(ctx, q.SubtaskID)
			skills := []string{"*"}
			if sub != nil {
				skills = reputationSkills(sub.RequiredSkills)
			}
			positive, perfect := true, 1.0
			for _, skill := range skills {
				_ = s.st.ApplyReputationSignal(ctx, store.ReputationSignal{ID: "appeal:" + a.ID + ":" + skill, AgentID: q.ProducerAgentID, Skill: skill, Success: &positive, Quality: &perfect, Reliable: &positive, Weight: 1, EventRef: "appeal:" + a.ID}, s.clk.Now())
			}
			s.invalidateReputation(q.ProducerAgentID)
		}
	}
	return a, nil
}

type GatewayUsage struct {
	ID, MissionID, SubtaskID, AgentID, Provider, Model string
	InputTokens, OutputTokens                          int64
}

func (s *Service) RecordGatewayUsage(ctx context.Context, in GatewayUsage) ([]*store.MeterRecord, error) {
	if strings.TrimSpace(in.ID) == "" || strings.TrimSpace(in.MissionID) == "" || in.InputTokens < 0 || in.OutputTokens < 0 || in.InputTokens+in.OutputTokens == 0 {
		return nil, fmt.Errorf("%w: id/mission and positive token usage required", ErrInvalidProductInput)
	}
	metadata := map[string]any{"provider": in.Provider, "model": in.Model}
	candidates := []*store.MeterRecord{{ID: "gateway:" + in.ID + ":input", MissionID: in.MissionID, SubtaskID: in.SubtaskID, AgentID: in.AgentID, Resource: "token.input", Quantity: float64(in.InputTokens), Trust: store.MeterAuthoritative, Metadata: metadata}, {ID: "gateway:" + in.ID + ":output", MissionID: in.MissionID, SubtaskID: in.SubtaskID, AgentID: in.AgentID, Resource: "token.output", Quantity: float64(in.OutputTokens), Trust: store.MeterAuthoritative, Metadata: metadata}}
	var records []*store.MeterRecord
	for _, r := range candidates {
		if r.Quantity == 0 {
			continue
		}
		if err := s.st.PutMeterRecord(ctx, r, s.clk.Now()); err != nil {
			return nil, err
		}
		records = append(records, r)
	}
	return records, nil
}
