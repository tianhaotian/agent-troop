package api

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"agenttroop/internal/auth"
	"agenttroop/internal/core"
	"agenttroop/internal/store"
)

const maxArtifactURLTTL = 15 * time.Minute

func requestActor(r *http.Request, fallback store.Actor) store.Actor {
	if identity, ok := auth.FromContext(r.Context()); ok {
		return store.Actor{Kind: identity.Kind, ID: identity.Subject}
	}
	return fallback
}

func (s *Server) authenticate(next http.Handler) http.Handler {
	if s.auth == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if publicRoute(r) {
			next.ServeHTTP(w, r)
			return
		}
		identity, err := s.auth.FromRequest(r)
		if err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return
		}
		if !routeAllowed(identity, r) {
			writeJSON(w, http.StatusForbidden, map[string]string{"error": auth.ErrForbidden.Error()})
			return
		}
		next.ServeHTTP(w, r.WithContext(auth.WithIdentity(r.Context(), identity)))
	})
}

func publicRoute(r *http.Request) bool {
	if r.Method == http.MethodGet && (r.URL.Path == "/" || r.URL.Path == "/healthz" ||
		r.URL.Path == "/readyz" || r.URL.Path == "/.well-known/agent-card.json") {
		return true
	}
	if r.Method == http.MethodPost && r.URL.Path == "/v1/auth/tokens" {
		return true
	}
	// Content authorizes in-handler: either privileged Bearer or a bound signed URL.
	return r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/artifacts/") &&
		strings.HasSuffix(r.URL.Path, "/content")
}

func routeAllowed(identity auth.Identity, r *http.Request) bool {
	if identity.Privileged() {
		return true
	}
	if identity.Kind != "agent" {
		return false
	}
	p := r.URL.Path
	if p == "/v1/agents/register" || strings.HasSuffix(p, "/wake") {
		return false
	}
	if strings.HasPrefix(p, "/v1/agents/") || strings.HasPrefix(p, "/v1/leases/") ||
		strings.HasPrefix(p, "/v1/subtasks/") || p == "/v1/intents" || p == "/v1/artifacts" ||
		strings.HasSuffix(p, "/signed-url") {
		return true
	}
	return false
}

func (s *Server) requireAgentIdentity(w http.ResponseWriter, r *http.Request, agentID string) bool {
	if s.auth == nil {
		return true
	}
	identity, ok := auth.FromContext(r.Context())
	if !ok || identity.Kind != "agent" || agentID == "" {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": auth.ErrForbidden.Error()})
		return false
	}
	agent, err := s.svc.GetAgent(r.Context(), agentID)
	if err != nil {
		writeErr(w, err)
		return false
	}
	expected := agent.AuthSubject
	if expected == "" {
		expected = agent.ID
	}
	if identity.Subject != expected {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "authenticated subject does not own agent"})
		return false
	}
	return true
}

func (s *Server) requireAgentOrPrivileged(w http.ResponseWriter, r *http.Request, agentID string) bool {
	if s.auth == nil {
		return true
	}
	if identity, ok := auth.FromContext(r.Context()); ok && identity.Privileged() {
		return true
	}
	return s.requireAgentIdentity(w, r, agentID)
}

func (s *Server) requireSubtaskIdentity(w http.ResponseWriter, r *http.Request, subtaskID string) bool {
	if s.auth == nil {
		return true
	}
	sub, err := s.svc.GetSubtask(r.Context(), subtaskID)
	if err != nil {
		writeErr(w, err)
		return false
	}
	return s.requireAgentIdentity(w, r, sub.Assignee)
}

func (s *Server) requireSubtaskOrPrivileged(w http.ResponseWriter, r *http.Request, subtaskID string) bool {
	if s.auth == nil {
		return true
	}
	if identity, ok := auth.FromContext(r.Context()); ok && identity.Privileged() {
		return true
	}
	return s.requireSubtaskIdentity(w, r, subtaskID)
}

type tokenRequest struct {
	Subject    string   `json:"subject"`
	Kind       string   `json:"kind"`
	Scopes     []string `json:"scopes,omitempty"`
	TTLSeconds int64    `json:"ttl_seconds,omitempty"`
}

func (s *Server) issueToken(w http.ResponseWriter, r *http.Request) {
	if !s.auth.CheckBootstrap(r.Header.Get("X-Troop-Bootstrap-Token")) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid bootstrap token"})
		return
	}
	var req tokenRequest
	if !decodeLimited(w, r, &req, 64<<10) {
		return
	}
	if req.TTLSeconds == 0 {
		req.TTLSeconds = 3600
	}
	token, err := s.auth.Issue(req.Subject, req.Kind, req.Scopes, time.Duration(req.TTLSeconds)*time.Second)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"access_token": token, "token_type": "Bearer", "expires_in": req.TTLSeconds,
	})
}

type signArtifactRequest struct {
	AgentID   string `json:"agent_id,omitempty"`
	LeaseID   string `json:"lease_id,omitempty"`
	ExpiresIn int64  `json:"expires_in,omitempty"`
}

func (s *Server) signArtifactURL(w http.ResponseWriter, r *http.Request) {
	if s.auth == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "artifact signing requires TROOP_AUTH_SECRET"})
		return
	}
	var req signArtifactRequest
	if !decodeLimited(w, r, &req, 64<<10) {
		return
	}
	identity, _ := auth.FromContext(r.Context())
	if req.ExpiresIn == 0 {
		req.ExpiresIn = 300
	}
	ttl := time.Duration(req.ExpiresIn) * time.Second
	if ttl <= 0 || ttl > maxArtifactURLTTL {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "expires_in must be between 1 and 900 seconds"})
		return
	}
	if identity.Kind == "agent" {
		if !s.requireAgentIdentity(w, r, req.AgentID) {
			return
		}
		if req.LeaseID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "lease_id required for agent"})
			return
		}
		if err := s.svc.AuthorizeArtifactAccess(r.Context(), pv(r, "id"), req.LeaseID, req.AgentID); err != nil {
			writeErr(w, err)
			return
		}
	} else if !identity.Privileged() {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": auth.ErrForbidden.Error()})
		return
	}
	if _, err := s.svc.GetArtifact(r.Context(), pv(r, "id")); err != nil {
		writeErr(w, err)
		return
	}
	expires := s.auth.Now().Add(ttl)
	query := url.Values{}
	query.Set("expires", strconv.FormatInt(expires.Unix(), 10))
	query.Set("subject", identity.Subject)
	query.Set("lease_id", req.LeaseID)
	query.Set("sig", s.auth.SignArtifact(pv(r, "id"), identity.Subject, req.LeaseID, expires))
	path := fmt.Sprintf("/v1/artifacts/%s/content?%s", url.PathEscape(pv(r, "id")), query.Encode())
	writeJSON(w, http.StatusOK, map[string]any{"url": path, "expires_at": expires.UTC()})
}

func (s *Server) authorizeArtifactDownload(w http.ResponseWriter, r *http.Request, artifactID string) bool {
	if s.auth == nil {
		return true
	}
	q := r.URL.Query()
	if q.Get("sig") != "" {
		if err := s.auth.VerifyArtifact(artifactID, q.Get("subject"), q.Get("lease_id"), q.Get("expires"), q.Get("sig")); err != nil {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
			return false
		}
		return true
	}
	identity, err := s.auth.FromRequest(r)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return false
	}
	if !identity.Privileged() {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": auth.ErrForbidden.Error()})
		return false
	}
	return true
}

func writeAuthOrCoreError(w http.ResponseWriter, err error) {
	if errors.Is(err, core.ErrForbidden) || errors.Is(err, auth.ErrForbidden) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": err.Error()})
		return
	}
	writeErr(w, err)
}
