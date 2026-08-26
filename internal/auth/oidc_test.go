package auth

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"agenttroop/internal/clock"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestOIDCVerifierRS256(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	n := base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes())
	e := base64.RawURLEncoding.EncodeToString([]byte{1, 0, 1})
	jwks, _ := json.Marshal(map[string]any{"keys": []map[string]string{{"kid": "k1", "kty": "RSA", "alg": "RS256", "n": n, "e": e}}})
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(jwks)), Header: make(http.Header)}, nil
	})}
	clk := clock.NewFake(time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC))
	v, err := NewOIDCVerifier("https://issuer.example", "troop", "https://issuer.example/jwks", clk)
	if err != nil {
		t.Fatal(err)
	}
	v.Client = client
	token := signedRS256(t, key, map[string]any{"alg": "RS256", "kid": "k1"}, map[string]any{"sub": "operator", "iss": "https://issuer.example", "aud": []string{"other", "troop"}, "iat": clk.Now().Unix(), "exp": clk.Now().Add(time.Hour).Unix(), "kind": "service", "scope": "missions.read metering.write"})
	id, err := v.Verify(token)
	if err != nil {
		t.Fatal(err)
	}
	if id.Subject != "operator" || id.Kind != "service" || !id.HasScope("metering.write") {
		t.Fatalf("identity=%+v", id)
	}
}

func signedRS256(t *testing.T, key *rsa.PrivateKey, header, claims any) string {
	h, _ := json.Marshal(header)
	c, _ := json.Marshal(claims)
	unsigned := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(c)
	digest := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, digest[:])
	if err != nil {
		t.Fatal(err)
	}
	return unsigned + "." + base64.RawURLEncoding.EncodeToString(sig)
}
