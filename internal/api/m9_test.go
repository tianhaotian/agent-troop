package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestM9QualityUsageMetricsAndTrace(t *testing.T) {
	h := testHandler()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/v1/missions", strings.NewReader(
		`{"owner":"owner","goal":"quality","tasks":[{"name":"draft","kind":"agent"}]}`)))
	if w.Code != http.StatusCreated {
		t.Fatalf("create=%d body=%s", w.Code, w.Body.String())
	}
	var mission struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &mission)

	r := httptest.NewRequest(http.MethodPost, "/v1/artifacts", strings.NewReader(`{"answer":42}`))
	r.Header.Set("X-Mission-ID", mission.ID)
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusCreated {
		t.Fatalf("artifact=%d body=%s", w.Code, w.Body.String())
	}
	var artifact struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &artifact)

	r = httptest.NewRequest(http.MethodPost, "/v1/artifacts/"+artifact.ID+"/verify", strings.NewReader(
		`{"score":0.95,"confidence":0.9,"verdict":"accepted","rubric":"rubric://api/v1"}`))
	r.Header.Set("traceparent", "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusCreated || !strings.Contains(w.Body.String(), `"L0"`) {
		t.Fatalf("verify=%d body=%s", w.Code, w.Body.String())
	}
	if got := w.Header().Get("X-Trace-ID"); got != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("trace id=%q", got)
	}

	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/artifacts/"+artifact.ID+"/quality", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"verdict":"accepted"`) {
		t.Fatalf("quality=%d body=%s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/missions/"+mission.ID+"/usage", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"artifact.byte"`) ||
		!strings.Contains(w.Body.String(), `"verify.call"`) {
		t.Fatalf("usage=%d body=%s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "troop_subtasks") ||
		!strings.Contains(w.Body.String(), "troop_http_requests_total") {
		t.Fatalf("metrics=%d body=%s", w.Code, w.Body.String())
	}
}
