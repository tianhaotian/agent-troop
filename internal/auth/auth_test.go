package auth

import (
	"net/http/httptest"
	"testing"
	"time"

	"agenttroop/internal/clock"
)

func TestTokenLifecycle(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC))
	m, err := New("0123456789abcdef0123456789abcdef", clk)
	if err != nil {
		t.Fatal(err)
	}
	token, err := m.Issue("agent@example", "agent", []string{"tasks.execute", "tasks.execute"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	identity, err := m.Verify(token)
	if err != nil || identity.Subject != "agent@example" || len(identity.Scopes) != 1 {
		t.Fatalf("identity=%+v err=%v", identity, err)
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	if fromRequest, err := m.FromRequest(r); err != nil || fromRequest.Subject != identity.Subject {
		t.Fatalf("from request=%+v err=%v", fromRequest, err)
	}
	if _, err := m.Verify(token + "x"); err != ErrInvalidToken {
		t.Fatalf("tampered err=%v", err)
	}
	clk.Advance(time.Minute)
	if _, err := m.Verify(token); err != ErrExpiredToken {
		t.Fatalf("expired err=%v", err)
	}
}

func TestArtifactSignatureBinding(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC))
	m, _ := New("0123456789abcdef0123456789abcdef", clk)
	expires := clk.Now().Add(time.Minute)
	sig := m.SignArtifact("art_a", "agent@example", "les_a", expires)
	if err := m.VerifyArtifact("art_a", "agent@example", "les_a", "1787731260", sig); err != nil {
		t.Fatal(err)
	}
	if err := m.VerifyArtifact("art_b", "agent@example", "les_a", "1787731260", sig); err != ErrInvalidToken {
		t.Fatalf("cross-artifact err=%v", err)
	}
	clk.Advance(time.Minute)
	if err := m.VerifyArtifact("art_a", "agent@example", "les_a", "1787731260", sig); err != ErrExpiredToken {
		t.Fatalf("expired err=%v", err)
	}
}
