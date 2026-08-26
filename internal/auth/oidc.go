package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"

	"agenttroop/internal/clock"
)

// OIDCVerifier validates RS256 access tokens against a rotating JWKS endpoint.
type OIDCVerifier struct {
	Issuer, Audience, JWKSURL string
	Client                    *http.Client
	Clock                     clock.Clock
	CacheTTL                  time.Duration
	mu                        sync.Mutex
	keys                      map[string]*rsa.PublicKey
	expires                   time.Time
}

func NewOIDCVerifier(issuer, audience, jwksURL string, clk clock.Clock) (*OIDCVerifier, error) {
	if strings.TrimRight(issuer, "/") == "" || audience == "" || jwksURL == "" || clk == nil {
		return nil, errors.New("auth: OIDC issuer, audience, JWKS URL and clock required")
	}
	return &OIDCVerifier{Issuer: strings.TrimRight(issuer, "/"), Audience: audience, JWKSURL: jwksURL, Clock: clk, CacheTTL: time.Hour, keys: map[string]*rsa.PublicKey{}}, nil
}

func (v *OIDCVerifier) Verify(token string) (Identity, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Identity{}, ErrInvalidToken
	}
	var header struct{ Alg, Kid string }
	if !decodeJWT(parts[0], &header) || header.Alg != "RS256" || header.Kid == "" {
		return Identity{}, ErrInvalidToken
	}
	key, err := v.key(header.Kid, false)
	if err != nil {
		return Identity{}, ErrInvalidToken
	}
	hash := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, hash[:], sig) != nil {
		key, err = v.key(header.Kid, true)
		if err != nil || rsa.VerifyPKCS1v15(key, crypto.SHA256, hash[:], sig) != nil {
			return Identity{}, ErrInvalidToken
		}
	}
	var claims struct {
		Sub, Iss, Kind string
		Aud            any
		Scope          string
		Scopes         []string
		Iat, Exp       int64
	}
	if !decodeJWT(parts[1], &claims) || claims.Sub == "" || claims.Iss != v.Issuer || !audienceContains(claims.Aud, v.Audience) || claims.Exp <= claims.Iat {
		return Identity{}, ErrInvalidToken
	}
	if v.Clock.Now().Unix() >= claims.Exp {
		return Identity{}, ErrExpiredToken
	}
	kind := claims.Kind
	if kind == "" {
		kind = "human"
	}
	if kind != "human" && kind != "agent" && kind != "service" {
		return Identity{}, ErrInvalidToken
	}
	scopes := append([]string(nil), claims.Scopes...)
	scopes = append(scopes, strings.Fields(claims.Scope)...)
	return Identity{Subject: claims.Sub, Kind: kind, Scopes: normalizeScopes(scopes), Issued: claims.Iat, Expires: claims.Exp}, nil
}

func (v *OIDCVerifier) key(kid string, force bool) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !force && v.Clock.Now().Before(v.expires) {
		if key := v.keys[kid]; key != nil {
			return key, nil
		}
	}
	client := v.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, v.JWKSURL, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, ErrInvalidToken
	}
	var set struct {
		Keys []struct{ Kid, Kty, Alg, N, E string } `json:"keys"`
	}
	if json.NewDecoder(resp.Body).Decode(&set) != nil {
		return nil, ErrInvalidToken
	}
	keys := map[string]*rsa.PublicKey{}
	for _, jwk := range set.Keys {
		if jwk.Kty != "RSA" || jwk.Alg != "RS256" {
			continue
		}
		n, e := decodeRSA(jwk.N, jwk.E)
		if n != nil && e > 0 {
			keys[jwk.Kid] = &rsa.PublicKey{N: n, E: e}
		}
	}
	v.keys, v.expires = keys, v.Clock.Now().Add(v.CacheTTL)
	if key := keys[kid]; key != nil {
		return key, nil
	}
	return nil, ErrInvalidToken
}

func decodeJWT(part string, out any) bool {
	data, err := base64.RawURLEncoding.DecodeString(part)
	return err == nil && json.Unmarshal(data, out) == nil
}
func decodeRSA(nRaw, eRaw string) (*big.Int, int) {
	nBytes, nErr := base64.RawURLEncoding.DecodeString(nRaw)
	eBytes, eErr := base64.RawURLEncoding.DecodeString(eRaw)
	if nErr != nil || eErr != nil || len(eBytes) == 0 || len(eBytes) > 4 {
		return nil, 0
	}
	var padded [4]byte
	copy(padded[4-len(eBytes):], eBytes)
	return new(big.Int).SetBytes(nBytes), int(binary.BigEndian.Uint32(padded[:]))
}
func audienceContains(raw any, want string) bool {
	switch value := raw.(type) {
	case string:
		return value == want
	case []any:
		for _, item := range value {
			if item == want {
				return true
			}
		}
	}
	return false
}
