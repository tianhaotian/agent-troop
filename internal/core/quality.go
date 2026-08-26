package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

var ErrInvalidQuality = errors.New("core: invalid quality record")

var qualityFailureClasses = map[string]bool{
	"schema_invalid": true, "fact_conflict": true, "incomplete": true,
	"style": true, "policy_violation": true, "judge_rejected": true,
}

type VerifyArtifactInput struct {
	Layers       map[string]store.QualityLayer `json:"layers,omitempty"`
	Score        float64                       `json:"score"`
	Confidence   float64                       `json:"confidence"`
	Verdict      string                        `json:"verdict"`
	FailureClass string                        `json:"failure_class,omitempty"`
	Rubric       string                        `json:"rubric,omitempty"`
	ContextHash  string                        `json:"context_hash,omitempty"`
}

func (s *Service) VerifyArtifact(ctx context.Context, artifactID string, in VerifyArtifactInput,
	verifier store.Actor) (*store.QualityRecord, error) {
	if verifier.ID == "" || verifier.Kind == "" {
		return nil, fmt.Errorf("%w: verifier identity required", ErrInvalidQuality)
	}
	if math.IsNaN(in.Score) || math.IsInf(in.Score, 0) || in.Score < 0 || in.Score > 1 ||
		math.IsNaN(in.Confidence) || math.IsInf(in.Confidence, 0) || in.Confidence < 0 || in.Confidence > 1 {
		return nil, fmt.Errorf("%w: score/confidence must be finite values in [0,1]", ErrInvalidQuality)
	}
	if in.Verdict != store.QualityAccepted && in.Verdict != store.QualityRework && in.Verdict != store.QualityRejected {
		return nil, fmt.Errorf("%w: invalid verdict", ErrInvalidQuality)
	}
	if in.Verdict == store.QualityAccepted && in.FailureClass != "" {
		return nil, fmt.Errorf("%w: accepted verdict cannot have failure_class", ErrInvalidQuality)
	}
	if in.Verdict != store.QualityAccepted && !qualityFailureClasses[in.FailureClass] {
		return nil, fmt.Errorf("%w: structured failure_class required", ErrInvalidQuality)
	}
	content, artifact, err := s.GetArtifactContent(ctx, artifactID)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(content)
	l0Pass := int64(len(content)) == artifact.Size && hex.EncodeToString(digest[:]) == artifact.SHA256
	if !l0Pass {
		return nil, fmt.Errorf("%w: artifact content hash/size mismatch", ErrInvalidQuality)
	}
	layers := make(map[string]store.QualityLayer, len(in.Layers)+1)
	for name, layer := range in.Layers {
		if name != "L1" && name != "L2" && name != "L3" {
			return nil, fmt.Errorf("%w: unsupported layer %q", ErrInvalidQuality, name)
		}
		if layer.Score != nil && (*layer.Score < 0 || *layer.Score > 1) {
			return nil, fmt.Errorf("%w: layer %s score out of range", ErrInvalidQuality, name)
		}
		if layer.Confidence != nil && (*layer.Confidence < 0 || *layer.Confidence > 1) {
			return nil, fmt.Errorf("%w: layer %s confidence out of range", ErrInvalidQuality, name)
		}
		layers[name] = layer
	}
	layers["L0"] = store.QualityLayer{Pass: true, Evidence: map[string]any{
		"sha256": artifact.SHA256, "size": artifact.Size,
	}}

	record := &store.QualityRecord{ArtifactID: artifact.ID, MissionID: artifact.MissionID,
		SubtaskID: artifact.ProducedBy, Layers: layers, Score: in.Score, Confidence: in.Confidence,
		Verdict: in.Verdict, FailureClass: in.FailureClass, Rubric: in.Rubric,
		ContextHash: in.ContextHash, VerifiedBy: []store.Actor{verifier}}
	var producer *mission.Subtask
	if artifact.ProducedBy != "" {
		producer, err = s.st.GetSubtask(ctx, artifact.ProducedBy)
		if err != nil {
			return nil, err
		}
		record.Attempt = producer.Attempt
		record.ProducerAgentID = producer.Assignee
	}
	if verifier.Kind == "agent" && verifier.ID == record.ProducerAgentID {
		return nil, fmt.Errorf("%w: producer cannot verify its own artifact", ErrForbidden)
	}
	if record.ProducerAgentID != "" {
		if a, err := s.st.GetAgent(ctx, record.ProducerAgentID); err == nil {
			record.ProducerPlatform = a.Platform
		}
	}

	var signals []store.ReputationSignal
	if producer != nil && record.ProducerAgentID != "" {
		success := in.Verdict == store.QualityAccepted
		quality := in.Score * in.Confidence
		for _, skill := range reputationSkills(producer.RequiredSkills) {
			signals = append(signals, store.ReputationSignal{ID: "quality:" + artifact.ID + ":" + skill,
				AgentID: record.ProducerAgentID, Skill: skill, Success: &success, Quality: &quality,
				Weight: 1, EventRef: "artifact:" + artifact.ID})
		}
	}
	if err := s.st.RecordQuality(ctx, record, signals, verifier, s.clk.Now()); err != nil {
		return nil, err
	}
	s.invalidateReputation(record.ProducerAgentID)
	return record, nil
}

func (s *Service) GetQuality(ctx context.Context, artifactID string) (*store.QualityRecord, error) {
	return s.st.GetQuality(ctx, artifactID)
}

func (s *Service) GetReputations(ctx context.Context, agentID string) ([]*store.ReputationRecord, error) {
	return s.st.ListReputations(ctx, agentID)
}

func (s *Service) GetUsageReport(ctx context.Context, missionID string) (*store.UsageReport, error) {
	records, err := s.st.ListMeterRecords(ctx, missionID)
	if err != nil {
		return nil, err
	}
	report := &store.UsageReport{MissionID: missionID, Records: records,
		ByResource: map[string]float64{}, ByTrust: map[string]float64{}}
	for _, m := range records {
		report.ByResource[m.Resource] += m.Quantity
		report.ByTrust[m.Trust] += m.Credits
		report.Credits += m.Credits
	}
	return report, nil
}

func reputationSkills(skills []string) []string {
	if len(skills) == 0 {
		return []string{"*"}
	}
	return skills
}

func (s *Service) recordOutcome(ctx context.Context, sub *mission.Subtask, success bool,
	agentID string, latencyMS float64) {
	if sub == nil || agentID == "" {
		return
	}
	for _, skill := range reputationSkills(sub.RequiredSkills) {
		reliable := success
		sig := store.ReputationSignal{ID: fmt.Sprintf("outcome:%s:%d:%t:%s", sub.ID, sub.Attempt, success, skill),
			AgentID: agentID, Skill: skill, Success: &success, Reliable: &reliable,
			LatencyMS: latencyMS, Weight: 0.25, EventRef: "subtask:" + sub.ID}
		if err := s.st.ApplyReputationSignal(ctx, sig, s.clk.Now()); err != nil && !errors.Is(err, store.ErrDuplicate) {
			continue // 幂等补偿由 sweeper 重放；执行终态不能被派生投影阻塞
		}
		s.invalidateReputation(agentID)
	}
}

type reputationCacheEntry struct {
	records   []*store.ReputationRecord
	expiresAt time.Time
}

func (s *Service) cachedReputations(ctx context.Context, agentID string, now time.Time) ([]*store.ReputationRecord, error) {
	s.reputationMu.Lock()
	entry, ok := s.reputationCache[agentID]
	if ok && now.Before(entry.expiresAt) {
		records := cloneReputations(entry.records)
		s.reputationMu.Unlock()
		return records, nil
	}
	s.reputationMu.Unlock()
	records, err := s.st.ListReputations(ctx, agentID)
	if err != nil {
		return nil, err
	}
	ttl := s.cfg.ReputationCacheTTL
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	s.reputationMu.Lock()
	s.reputationCache[agentID] = reputationCacheEntry{records: cloneReputations(records), expiresAt: now.Add(ttl)}
	s.reputationMu.Unlock()
	return records, nil
}

func (s *Service) invalidateReputation(agentID string) {
	if agentID == "" {
		return
	}
	s.reputationMu.Lock()
	delete(s.reputationCache, agentID)
	s.reputationMu.Unlock()
}

func cloneReputations(in []*store.ReputationRecord) []*store.ReputationRecord {
	out := make([]*store.ReputationRecord, len(in))
	for i, rep := range in {
		cp := *rep
		out[i] = &cp
	}
	return out
}
