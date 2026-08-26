package core

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

// S3Blob is a dependency-free S3-compatible content-addressed store using SigV4.
type S3Blob struct {
	Endpoint, Bucket, Region, AccessKey, SecretKey, SessionToken string
	KMSKeyID                                                     string
	Client                                                       *http.Client
	Now                                                          func() time.Time
}

func (s S3Blob) Put(hash string, data []byte) error {
	req, err := s.request(http.MethodPut, hash, data)
	if err != nil {
		return err
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		return fmt.Errorf("s3 put: status %d: %s", resp.StatusCode, body)
	}
	return nil
}

func (s S3Blob) Get(hash string) ([]byte, error) {
	req, err := s.request(http.MethodGet, hash, nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("blob: %s not found", hash)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("s3 get: status %d", resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 128<<20))
}

func (s S3Blob) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return &http.Client{Timeout: 2 * time.Minute}
}

func (s S3Blob) request(method, hash string, body []byte) (*http.Request, error) {
	if s.Endpoint == "" || s.Bucket == "" || s.Region == "" || s.AccessKey == "" || s.SecretKey == "" {
		return nil, errors.New("s3: endpoint, bucket, region and credentials required")
	}
	base, err := url.Parse(strings.TrimRight(s.Endpoint, "/"))
	if err != nil {
		return nil, err
	}
	base.Path = path.Join(base.Path, s.Bucket, hash[:2], hash)
	payloadHash := sha256Hex(body)
	req, err := http.NewRequest(method, base.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if s.Now != nil {
		now = s.Now().UTC()
	}
	amzDate, shortDate := now.Format("20060102T150405Z"), now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	if s.SessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", s.SessionToken)
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/octet-stream")
	}
	if s.KMSKeyID != "" {
		req.Header.Set("X-Amz-Server-Side-Encryption", "aws:kms")
		req.Header.Set("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id", s.KMSKeyID)
	}
	signedHeaders, canonicalHeaders := canonicalS3Headers(req)
	canonical := method + "\n" + req.URL.EscapedPath() + "\n" + req.URL.Query().Encode() + "\n" + canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash
	scope := shortDate + "/" + s.Region + "/s3/aws4_request"
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonical))
	dateKey := hmacSHA([]byte("AWS4"+s.SecretKey), shortDate)
	regionKey := hmacSHA(dateKey, s.Region)
	serviceKey := hmacSHA(regionKey, "s3")
	signingKey := hmacSHA(serviceKey, "aws4_request")
	signature := hex.EncodeToString(hmacSHA(signingKey, stringToSign))
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential="+s.AccessKey+"/"+scope+", SignedHeaders="+signedHeaders+", Signature="+signature)
	return req, nil
}

func canonicalS3Headers(req *http.Request) (string, string) {
	entries := []string{"host:" + req.URL.Host, "x-amz-content-sha256:" + req.Header.Get("X-Amz-Content-Sha256"), "x-amz-date:" + req.Header.Get("X-Amz-Date")}
	names := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if v := req.Header.Get("X-Amz-Security-Token"); v != "" {
		entries = append(entries, "x-amz-security-token:"+v)
		names = append(names, "x-amz-security-token")
	}
	if v := req.Header.Get("X-Amz-Server-Side-Encryption"); v != "" {
		entries = append(entries, "x-amz-server-side-encryption:"+v)
		names = append(names, "x-amz-server-side-encryption")
	}
	if v := req.Header.Get("X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id"); v != "" {
		entries = append(entries, "x-amz-server-side-encryption-aws-kms-key-id:"+v)
		names = append(names, "x-amz-server-side-encryption-aws-kms-key-id")
	}
	return strings.Join(names, ";"), strings.Join(entries, "\n") + "\n"
}
func sha256Hex(data []byte) string { sum := sha256.Sum256(data); return hex.EncodeToString(sum[:]) }
func hmacSHA(key []byte, value string) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write([]byte(value))
	return h.Sum(nil)
}
