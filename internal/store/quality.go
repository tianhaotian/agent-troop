package store

import (
	"math"
	"time"
)

const (
	QualityAccepted = "accepted"
	QualityRework   = "rework"
	QualityRejected = "rejected"

	MeterAuthoritative = "authoritative"
	MeterSelfReported  = "self_reported"
	PriceBookV1        = "credits-v1"
	AppealPending      = "pending"
	AppealUpheld       = "upheld"
	AppealOverturned   = "overturned"
)

// QualityLayer 保存某一验收层的机器可读结果与证据。
type QualityLayer struct {
	Pass       bool             `json:"pass"`
	Score      *float64         `json:"score,omitempty"`
	Confidence *float64         `json:"confidence,omitempty"`
	Violations []map[string]any `json:"violations,omitempty"`
	Evidence   map[string]any   `json:"evidence,omitempty"`
}

// QualityRecord 是 Artifact 的最终验收事实。ArtifactID 唯一，因此重复提交可安全识别。
type QualityRecord struct {
	ArtifactID       string                  `json:"artifact_id"`
	MissionID        string                  `json:"mission_id"`
	SubtaskID        string                  `json:"subtask_id,omitempty"`
	ProducerAgentID  string                  `json:"producer_agent_id,omitempty"`
	ProducerPlatform string                  `json:"producer_platform,omitempty"`
	Attempt          int                     `json:"attempt"`
	Layers           map[string]QualityLayer `json:"layers"`
	Score            float64                 `json:"score"`
	Confidence       float64                 `json:"confidence"`
	Verdict          string                  `json:"verdict"`
	FailureClass     string                  `json:"failure_class,omitempty"`
	Rubric           string                  `json:"rubric,omitempty"`
	ContextHash      string                  `json:"context_hash,omitempty"`
	VerifiedBy       []Actor                 `json:"verified_by"`
	CreatedAt        time.Time               `json:"created_at"`
}

// QualityAppeal 是对不可变 QualityRecord 的审计式异议；裁决不覆盖原事实。
type QualityAppeal struct {
	ID               string     `json:"id"`
	ArtifactID       string     `json:"artifact_id"`
	MissionID        string     `json:"mission_id"`
	AppellantID      string     `json:"appellant_id"`
	Reason           string     `json:"reason"`
	EvidenceRefs     []string   `json:"evidence_refs,omitempty"`
	Status           string     `json:"status"`
	Resolution       string     `json:"resolution,omitempty"`
	ReviewerID       string     `json:"reviewer_id,omitempty"`
	CorrectionSignal string     `json:"correction_signal,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	ResolvedAt       *time.Time `json:"resolved_at,omitempty"`
}

// ReputationSignal 是信誉投影的幂等输入事实。
type ReputationSignal struct {
	ID          string   `json:"id"`
	AgentID     string   `json:"agent_id"`
	Skill       string   `json:"skill"`
	Success     *bool    `json:"success,omitempty"`
	Quality     *float64 `json:"quality,omitempty"`
	Reliable    *bool    `json:"reliable,omitempty"`
	LatencyMS   float64  `json:"latency_ms,omitempty"`
	CostCredits float64  `json:"cost_credits,omitempty"`
	Weight      float64  `json:"weight"`
	EventRef    string   `json:"event_ref,omitempty"`
}

// ReputationRecord 是 (agent, skill) 的可解释信誉投影。
type ReputationRecord struct {
	AgentID          string    `json:"agent_id"`
	Skill            string    `json:"skill"`
	SuccessAlpha     float64   `json:"success_alpha"`
	SuccessBeta      float64   `json:"success_beta"`
	QualityEWMA      float64   `json:"quality_ewma"`
	QualitySamples   float64   `json:"quality_samples"`
	ReliabilityAlpha float64   `json:"reliability_alpha"`
	ReliabilityBeta  float64   `json:"reliability_beta"`
	LatencyEWMAms    float64   `json:"latency_ewma_ms"`
	CostEfficiency   float64   `json:"cost_efficiency_ewma"`
	Samples          float64   `json:"samples"`
	UpdatedAt        time.Time `json:"updated_at"`
	SuccessScore     float64   `json:"success_score"`
	ReliabilityScore float64   `json:"reliability_score"`
	CompositeScore   float64   `json:"composite_score"`
}

func NewReputation(agentID, skill string) *ReputationRecord {
	return &ReputationRecord{
		AgentID: agentID, Skill: skill,
		SuccessAlpha: 2, SuccessBeta: 2, QualityEWMA: 0.5,
		ReliabilityAlpha: 2, ReliabilityBeta: 2,
	}
}

// ApplyReputationSignal 更新投影。EWMA 单次最多移动 0.1，避免单样本操纵。
func ApplyReputationSignal(r *ReputationRecord, sig ReputationSignal, now time.Time) {
	weight := sig.Weight
	if weight <= 0 {
		weight = 1
	}
	if weight > 1 {
		weight = 1
	}
	if sig.Success != nil {
		if *sig.Success {
			r.SuccessAlpha += weight
		} else {
			r.SuccessBeta += weight
		}
	}
	if sig.Reliable != nil {
		if *sig.Reliable {
			r.ReliabilityAlpha += weight
		} else {
			r.ReliabilityBeta += weight
		}
	}
	alpha := math.Min(0.2*weight, 0.1)
	if sig.Quality != nil {
		q := clamp01(*sig.Quality)
		r.QualityEWMA += alpha * (q - r.QualityEWMA)
		r.QualitySamples += weight
	}
	if sig.LatencyMS > 0 {
		if r.LatencyEWMAms == 0 {
			r.LatencyEWMAms = sig.LatencyMS
		} else {
			r.LatencyEWMAms += alpha * (sig.LatencyMS - r.LatencyEWMAms)
		}
	}
	if sig.CostCredits > 0 && sig.Quality != nil {
		eff := clamp01(*sig.Quality) / sig.CostCredits
		if r.CostEfficiency == 0 {
			r.CostEfficiency = eff
		} else {
			r.CostEfficiency += alpha * (eff - r.CostEfficiency)
		}
	}
	r.Samples += weight
	r.UpdatedAt = now
	r.RefreshScores()
}

func (r *ReputationRecord) RefreshScores() {
	r.SuccessScore = ratio(r.SuccessAlpha, r.SuccessBeta)
	r.ReliabilityScore = ratio(r.ReliabilityAlpha, r.ReliabilityBeta)
	r.CompositeScore = clamp01(0.4*r.SuccessScore + 0.4*r.QualityEWMA + 0.2*r.ReliabilityScore)
}

func ratio(a, b float64) float64 {
	if a+b == 0 {
		return 0.5
	}
	return a / (a + b)
}

func clamp01(v float64) float64 {
	if v < 0 {
		return 0
	}
	if v > 1 {
		return 1
	}
	return v
}

// MeterRecord 是不可变的内部用量账条目。
type MeterRecord struct {
	ID         string         `json:"id"`
	MissionID  string         `json:"mission_id"`
	SubtaskID  string         `json:"subtask_id,omitempty"`
	AgentID    string         `json:"agent_id,omitempty"`
	Resource   string         `json:"resource"`
	Quantity   float64        `json:"quantity"`
	Unit       string         `json:"unit"`
	Trust      string         `json:"trust"`
	PriceBook  string         `json:"price_book"`
	UnitPrice  float64        `json:"unit_price"`
	Credits    float64        `json:"credits"`
	Metadata   map[string]any `json:"metadata,omitempty"`
	RecordedAt time.Time      `json:"recorded_at"`
}

type UsageReport struct {
	MissionID  string             `json:"mission_id"`
	Records    []*MeterRecord     `json:"records"`
	ByResource map[string]float64 `json:"quantity_by_resource"`
	ByTrust    map[string]float64 `json:"credits_by_trust"`
	Credits    float64            `json:"total_credits"`
}

func PriceMeter(m *MeterRecord) {
	m.PriceBook = PriceBookV1
	switch m.Resource {
	case "lease.wall_ms":
		m.Unit, m.UnitPrice = "ms", 0.000001
	case "artifact.byte":
		m.Unit, m.UnitPrice = "byte", 0.000000001
	case "verify.call":
		m.Unit, m.UnitPrice = "call", 0.01
	case "wake.fire":
		m.Unit, m.UnitPrice = "fire", 0.001
	case "token.reported":
		m.Unit, m.UnitPrice = "token", 0 // 未经网关仲裁不结算
	case "token.input":
		m.Unit, m.UnitPrice = "token", 0.000002
	case "token.output":
		m.Unit, m.UnitPrice = "token", 0.000008
	}
	m.Credits = m.Quantity * m.UnitPrice
}
