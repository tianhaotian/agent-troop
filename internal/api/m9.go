package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"

	"agenttroop/internal/auth"
	"agenttroop/internal/core"
	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

type verifyArtifactRequest struct {
	core.VerifyArtifactInput
	VerifierAgentID string `json:"verifier_agent_id,omitempty"`
}

func (s *Server) verifyArtifact(w http.ResponseWriter, r *http.Request) {
	var req verifyArtifactRequest
	if !decodeLimited(w, r, &req, 1<<20) {
		return
	}
	actor := store.Actor{Kind: "service", ID: "anonymous-verifier"}
	if s.auth != nil {
		identity, _ := auth.FromContext(r.Context())
		if identity.Kind == "agent" {
			if req.VerifierAgentID == "" || !s.requireAgentIdentity(w, r, req.VerifierAgentID) {
				return
			}
			actor = store.Actor{Kind: "agent", ID: req.VerifierAgentID}
		} else {
			actor = store.Actor{Kind: identity.Kind, ID: identity.Subject}
		}
	} else if req.VerifierAgentID != "" {
		actor = store.Actor{Kind: "agent", ID: req.VerifierAgentID}
	}
	q, err := s.svc.VerifyArtifact(r.Context(), pv(r, "id"), req.VerifyArtifactInput, actor)
	if err != nil {
		writeAuthOrCoreError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, q)
}

func (s *Server) getQuality(w http.ResponseWriter, r *http.Request) {
	q, err := s.svc.GetQuality(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, q)
}

func (s *Server) getReputation(w http.ResponseWriter, r *http.Request) {
	agentID := pv(r, "id")
	if !s.requireAgentOrPrivileged(w, r, agentID) {
		return
	}
	reps, err := s.svc.GetReputations(r.Context(), agentID)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"agent_id": agentID, "skills": reps})
}

func (s *Server) getUsage(w http.ResponseWriter, r *http.Request) {
	report, err := s.svc.GetUsageReport(r.Context(), pv(r, "id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func (s *Server) observabilitySnapshot(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.svc.ObservabilitySnapshot(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (s *Server) prometheusMetrics(w http.ResponseWriter, r *http.Request) {
	snapshot, err := s.svc.ObservabilitySnapshot(r.Context())
	if err != nil {
		writeErr(w, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	fmt.Fprintln(w, "# HELP troop_subtasks Current subtasks by state.")
	fmt.Fprintln(w, "# TYPE troop_subtasks gauge")
	states := make([]string, 0, len(snapshot.Subtasks))
	for state := range snapshot.Subtasks {
		states = append(states, string(state))
	}
	sort.Strings(states)
	for _, state := range states {
		fmt.Fprintf(w, "troop_subtasks{state=%q} %d\n", state, snapshot.Subtasks[mission.State(state)])
	}
	fmt.Fprintln(w, "# HELP troop_agents Current agents by health.")
	fmt.Fprintln(w, "# TYPE troop_agents gauge")
	healths := make([]string, 0, len(snapshot.AgentsByHealth))
	for health := range snapshot.AgentsByHealth {
		healths = append(healths, health)
	}
	sort.Strings(healths)
	for _, health := range healths {
		fmt.Fprintf(w, "troop_agents{health=%q} %d\n", health, snapshot.AgentsByHealth[health])
	}
	fmt.Fprintf(w, "# HELP troop_pending_decisions Current unresolved decisions.\n")
	fmt.Fprintf(w, "# TYPE troop_pending_decisions gauge\n")
	fmt.Fprintf(w, "troop_pending_decisions %d\n", snapshot.PendingDecisions)
	s.metrics.writePrometheus(w)
}

type requestMetric struct {
	Count       uint64
	DurationSec float64
}

type httpMetrics struct {
	mu       sync.Mutex
	requests map[string]requestMetric
}

func newHTTPMetrics() *httpMetrics { return &httpMetrics{requests: map[string]requestMetric{}} }

type metricWriter struct {
	http.ResponseWriter
	status int
}

func (w *metricWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *metricWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

func (w *metricWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent, traceID := ensureTraceparent(r.Header.Get("traceparent"))
		r.Header.Set("traceparent", traceparent)
		w.Header().Set("traceparent", traceparent)
		w.Header().Set("X-Trace-ID", traceID)
		start := s.svc.Now()
		mw := &metricWriter{ResponseWriter: w}
		next.ServeHTTP(mw, r)
		status := mw.status
		if status == 0 {
			status = http.StatusOK
		}
		key := fmt.Sprintf("%s\x00%s\x00%d", r.Method, metricPath(r.URL.Path), status)
		s.metrics.mu.Lock()
		metric := s.metrics.requests[key]
		metric.Count++
		metric.DurationSec += s.svc.Now().Sub(start).Seconds()
		s.metrics.requests[key] = metric
		s.metrics.mu.Unlock()
	})
}

func (m *httpMetrics) writePrometheus(w http.ResponseWriter) {
	m.mu.Lock()
	defer m.mu.Unlock()
	fmt.Fprintln(w, "# HELP troop_http_requests_total HTTP requests by method, route and status.")
	fmt.Fprintln(w, "# TYPE troop_http_requests_total counter")
	fmt.Fprintln(w, "# HELP troop_http_request_duration_seconds_sum HTTP request duration sum.")
	fmt.Fprintln(w, "# TYPE troop_http_request_duration_seconds_sum counter")
	keys := make([]string, 0, len(m.requests))
	for key := range m.requests {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts := strings.Split(key, "\x00")
		metric := m.requests[key]
		fmt.Fprintf(w, "troop_http_requests_total{method=%q,route=%q,status=%q} %d\n",
			parts[0], parts[1], parts[2], metric.Count)
		fmt.Fprintf(w, "troop_http_request_duration_seconds_sum{method=%q,route=%q,status=%q} %.9f\n",
			parts[0], parts[1], parts[2], metric.DurationSec)
	}
}

func ensureTraceparent(value string) (string, string) {
	parts := strings.Split(value, "-")
	if len(parts) == 4 && parts[0] == "00" && len(parts[1]) == 32 && len(parts[2]) == 16 && len(parts[3]) == 2 {
		if _, err := hex.DecodeString(parts[1] + parts[2] + parts[3]); err == nil &&
			parts[1] != strings.Repeat("0", 32) && parts[2] != strings.Repeat("0", 16) {
			return value, parts[1]
		}
	}
	var trace [16]byte
	var span [8]byte
	_, _ = rand.Read(trace[:])
	_, _ = rand.Read(span[:])
	traceID, spanID := hex.EncodeToString(trace[:]), hex.EncodeToString(span[:])
	return "00-" + traceID + "-" + spanID + "-01", traceID
}

func metricPath(path string) string {
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) >= 3 && parts[0] == "v1" {
		switch parts[1] {
		case "missions", "agents", "leases", "subtasks", "decisions", "artifacts":
			parts[2] = ":id"
		}
	}
	return "/" + strings.Join(parts, "/")
}
