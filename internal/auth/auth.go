// Package auth implements the M8 edge identity token and signed-resource primitives.
// Tokens are deliberately stateless: an upstream OIDC/API gateway authenticates users,
// then exchanges that identity for a short-lived platform token.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"agenttroop/internal/clock"
)

var (
	ErrMissingToken = errors.New("auth: bearer token required")
	ErrInvalidToken = errors.New("auth: invalid token")
	ErrExpiredToken = errors.New("auth: token expired")
	ErrForbidden    = errors.New("auth: forbidden")
)

const maxTokenTTL = 24 * time.Hour

type Identity struct {
	Subject string   `json:"sub"`
	Kind    string   `json:"kind"` // human | agent | service
	Scopes  []string `json:"scopes,omitempty"`
	Issued  int64    `json:"iat"`
	Expires int64    `json:"exp"`
}

func (i Identity) HasScope(scope string) bool {
	for _, current := range i.Scopes {
		if current == "*" || current == scope {
			return true
		}
	}
	return false
}

func (i Identity) Privileged() bool { return i.Kind == "human" || i.Kind == "service" }

type Manager struct {
	secret []byte
	clk    clock.Clock
	oidc   *OIDCVerifier
}

func (m *Manager) WithOIDC(verifier *OIDCVerifier) *Manager { m.oidc = verifier; return m }

func New(secret string, clk clock.Clock) (*Manager, error) {
	if len(secret) < 32 {
		return nil, errors.New("auth: TROOP_AUTH_SECRET must contain at least 32 bytes")
	}
	if clk == nil {
		return nil, errors.New("auth: clock required")
	}
	return &Manager{secret: []byte(secret), clk: clk}, nil
}

func (m *Manager) Issue(subject, kind string, scopes []string, ttl time.Duration) (string, error) {
	if strings.TrimSpace(subject) == "" {
		return "", errors.New("auth: subject required")
	}
	if kind != "human" && kind != "agent" && kind != "service" {
		return "", errors.New("auth: kind must be human, agent or service")
	}
	if ttl <= 0 || ttl > maxTokenTTL {
		return "", fmt.Errorf("auth: ttl must be between 1s and %s", maxTokenTTL)
	}
	now := m.clk.Now()
	claims := Identity{Subject: subject, Kind: kind, Scopes: normalizeScopes(scopes),
		Issued: now.Unix(), Expires: now.Add(ttl).Unix()}
	header, _ := json.Marshal(map[string]string{"alg": "HS256", "typ": "TROOP"})
	payload, _ := json.Marshal(claims)
	unsigned := encode(header) + "." + encode(payload)
	return unsigned + "." + encode(m.mac(unsigned)), nil
}

func (m *Manager) Verify(token string) (Identity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, ErrInvalidToken
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Identity{}, ErrInvalidToken
	}
	var header map[string]string
	if json.Unmarshal(headerBytes, &header) != nil {
		return Identity{}, ErrInvalidToken
	}
	if header["alg"] == "RS256" && m.oidc != nil {
		return m.oidc.Verify(token)
	}
	unsigned := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || !hmac.Equal(sig, m.mac(unsigned)) {
		return Identity{}, ErrInvalidToken
	}
	if header["alg"] != "HS256" || header["typ"] != "TROOP" {
		return Identity{}, ErrInvalidToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Identity{}, ErrInvalidToken
	}
	var claims Identity
	if json.Unmarshal(payload, &claims) != nil || claims.Subject == "" || claims.Expires <= claims.Issued {
		return Identity{}, ErrInvalidToken
	}
	if claims.Kind != "human" && claims.Kind != "agent" && claims.Kind != "service" {
		return Identity{}, ErrInvalidToken
	}
	if m.clk.Now().Unix() >= claims.Expires {
		return Identity{}, ErrExpiredToken
	}
	claims.Scopes = normalizeScopes(claims.Scopes)
	return claims, nil
}

func (m *Manager) FromRequest(r *http.Request) (Identity, error) {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") || strings.TrimSpace(strings.TrimPrefix(header, "Bearer ")) == "" {
		return Identity{}, ErrMissingToken
	}
	return m.Verify(strings.TrimSpace(strings.TrimPrefix(header, "Bearer ")))
}

func (m *Manager) CheckBootstrap(candidate string) bool {
	return hmac.Equal([]byte(candidate), m.secret)
}

func (m *Manager) Now() time.Time { return m.clk.Now() }

// SignArtifact returns an opaque query signature bound to artifact, caller, lease and expiry.
func (m *Manager) SignArtifact(artifactID, subject, leaseID string, expires time.Time) string {
	return encode(m.mac(artifactMessage(artifactID, subject, leaseID, expires.Unix())))
}

func (m *Manager) VerifyArtifact(artifactID, subject, leaseID, expiresRaw, signature string) error {
	expires, err := strconv.ParseInt(expiresRaw, 10, 64)
	if err != nil || expires <= m.clk.Now().Unix() {
		return ErrExpiredToken
	}
	provided, err := base64.RawURLEncoding.DecodeString(signature)
	if err != nil || !hmac.Equal(provided, m.mac(artifactMessage(artifactID, subject, leaseID, expires))) {
		return ErrInvalidToken
	}
	return nil
}

func artifactMessage(artifactID, subject, leaseID string, expires int64) string {
	return "artifact\n" + artifactID + "\n" + subject + "\n" + leaseID + "\n" + strconv.FormatInt(expires, 10)
}

func (m *Manager) mac(value string) []byte {
	h := hmac.New(sha256.New, m.secret)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}

func encode(value []byte) string { return base64.RawURLEncoding.EncodeToString(value) }

func normalizeScopes(scopes []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(scopes))
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope != "" && !seen[scope] {
			seen[scope] = true
			out = append(out, scope)
		}
	}
	return out
}

type identityKey struct{}

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, identity)
}

func FromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey{}).(Identity)
	return identity, ok
}
