package api

import (
	"net/http"
	"strconv"

	"agenttroop/internal/core"
	"agenttroop/internal/store"
)

func (s *Server) runSimulation(w http.ResponseWriter, r *http.Request) {
	var in core.SimulationInput
	if !decodeLimited(w, r, &in, 64<<10) {
		return
	}
	report, err := core.RunSimulation(in)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) marketplaceAgents(w http.ResponseWriter, r *http.Request) {
	minRep, err := strconv.ParseFloat(defaultValue(r.URL.Query().Get("min_reputation"), "0"), 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad min_reputation"})
		return
	}
	agents, err := s.svc.DiscoverAgents(r.Context(), core.MarketplaceQuery{
		Skill: r.URL.Query().Get("skill"), Platform: r.URL.Query().Get("platform"),
		HealthyOnly: r.URL.Query().Get("healthy") != "false", MinReputation: minRep,
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"agents": agents, "count": len(agents)})
}

func (s *Server) evaluateCanary(w http.ResponseWriter, r *http.Request) {
	var in core.CanaryInput
	if !decodeLimited(w, r, &in, 64<<10) {
		return
	}
	result, err := s.svc.EvaluateCanary(r.Context(), in)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *Server) createAppeal(w http.ResponseWriter, r *http.Request) {
	var in struct {
		AppellantID  string   `json:"appellant_id"`
		Reason       string   `json:"reason"`
		EvidenceRefs []string `json:"evidence_refs,omitempty"`
	}
	if !decodeLimited(w, r, &in, 64<<10) {
		return
	}
	a, err := s.svc.CreateQualityAppeal(r.Context(), pv(r, "id"), in.AppellantID, in.Reason,
		in.EvidenceRefs, requestActor(r, store.Actor{Kind: "human", ID: in.AppellantID}))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, a)
}

func (s *Server) listAppeals(w http.ResponseWriter, r *http.Request) {
	appeals, err := s.svc.ListQualityAppeals(r.Context(), r.URL.Query().Get("mission_id"), r.URL.Query().Get("pending") == "true")
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"appeals": appeals, "count": len(appeals)})
}

func (s *Server) resolveAppeal(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Status     string `json:"status"`
		Resolution string `json:"resolution"`
		ReviewerID string `json:"reviewer_id"`
	}
	if !decodeLimited(w, r, &in, 64<<10) {
		return
	}
	a, err := s.svc.ResolveQualityAppeal(r.Context(), pv(r, "id"), in.Status, in.Resolution,
		in.ReviewerID, requestActor(r, store.Actor{Kind: "human", ID: in.ReviewerID}))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) gatewayMeter(w http.ResponseWriter, r *http.Request) {
	var in struct {
		ID           string `json:"id"`
		MissionID    string `json:"mission_id"`
		SubtaskID    string `json:"subtask_id,omitempty"`
		AgentID      string `json:"agent_id,omitempty"`
		Provider     string `json:"provider,omitempty"`
		Model        string `json:"model,omitempty"`
		InputTokens  int64  `json:"input_tokens"`
		OutputTokens int64  `json:"output_tokens"`
	}
	if !decodeLimited(w, r, &in, 64<<10) {
		return
	}
	records, err := s.svc.RecordGatewayUsage(r.Context(), core.GatewayUsage{ID: in.ID, MissionID: in.MissionID,
		SubtaskID: in.SubtaskID, AgentID: in.AgentID, Provider: in.Provider, Model: in.Model,
		InputTokens: in.InputTokens, OutputTokens: in.OutputTokens})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"records": records})
}

func defaultValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}
