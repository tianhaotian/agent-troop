package store

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"time"

	"agenttroop/internal/mission"
)

// BuildContextPackage 过滤跨 Mission artifact、排序并生成稳定的内容指纹。
func BuildContextPackage(leaseID string, sub *mission.Subtask, artifacts []*Artifact,
	board []*ContextBoardEntry, decisions []*Decision, budget *BudgetAccount, now time.Time) (*ContextPackage, error) {
	grants, err := mission.NormalizePermissionEnvelope(sub.Grants)
	if err != nil {
		return nil, err
	}
	task := ContextTask{ID: sub.ID, MissionID: sub.MissionID, ParentID: sub.ParentID,
		Kind: sub.Kind, RequiredSkills: append([]string(nil), sub.RequiredSkills...),
		Scheduling: sub.Scheduling, Retry: sub.Retry, Input: sub.Input, ReworkOf: sub.ReworkOf,
		Grants: grants, Checkpoint: append([]byte(nil), sub.Checkpoint...), WakeKind: sub.WakeKind,
		WakeAt: sub.WakeAt, WakeDeadline: sub.WakeDeadline, WakeSpec: append([]byte(nil), sub.WakeSpec...)}
	filteredArtifacts := make([]*Artifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		if artifact != nil && artifact.MissionID == sub.MissionID {
			cp := *artifact
			filteredArtifacts = append(filteredArtifacts, &cp)
		}
	}
	sort.Slice(filteredArtifacts, func(i, j int) bool { return filteredArtifacts[i].ID < filteredArtifacts[j].ID })
	sort.Slice(board, func(i, j int) bool {
		if board[i].Namespace != board[j].Namespace {
			return board[i].Namespace < board[j].Namespace
		}
		return board[i].Key < board[j].Key
	})
	digest := make([]*ContextDecision, 0, len(decisions))
	for _, decision := range decisions {
		if decision != nil && decision.SubtaskID == sub.ID {
			digest = append(digest, &ContextDecision{ID: decision.ID, Kind: decision.Kind,
				Question: decision.Question, Status: decision.Status, Choice: decision.Choice})
		}
	}
	sort.Slice(digest, func(i, j int) bool { return digest[i].ID < digest[j].ID })
	if budget == nil {
		budget = &BudgetAccount{MissionID: sub.MissionID}
	} else {
		cp := *budget
		budget = &cp
	}
	pkg := &ContextPackage{ID: "ctx_" + leaseID, LeaseID: leaseID, MissionID: sub.MissionID,
		SubtaskID: sub.ID, Task: task, Artifacts: filteredArtifacts, BoardViews: board,
		Decisions: digest, Budget: budget, CreatedAt: now}
	hashInput := struct {
		Task      ContextTask          `json:"task_spec"`
		Artifacts []*Artifact          `json:"artifacts"`
		Board     []*ContextBoardEntry `json:"board_views"`
		Decisions []*ContextDecision   `json:"decisions_digest"`
		Budget    *BudgetAccount       `json:"budget"`
	}{task, filteredArtifacts, board, digest, budget}
	raw, err := json.Marshal(hashInput)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	pkg.SnapshotHash = "sha256:" + hex.EncodeToString(sum[:])
	return pkg, nil
}
