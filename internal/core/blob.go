package core

// Artifact blob 存储（M2-H5）：内容寻址（sha256 为 key）。
// FS 实现落本地目录（TROOP_BLOB_DIR）；MemBlob 供测试。
// S3/MinIO 实现在 M3 按同一接口接入（设计 §4.1）。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"agenttroop/internal/store"
)

// BlobStore 产物本体存储接口。
type BlobStore interface {
	Put(hash string, data []byte) error
	Get(hash string) ([]byte, error)
}

// FSBlob 本地文件系统 blob。
type FSBlob struct{ Dir string }

func (f FSBlob) path(hash string) string { return filepath.Join(f.Dir, hash[:2], hash) }

func (f FSBlob) Put(hash string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(f.path(hash)), 0o755); err != nil {
		return err
	}
	return os.WriteFile(f.path(hash), data, 0o644)
}

func (f FSBlob) Get(hash string) ([]byte, error) { return os.ReadFile(f.path(hash)) }

// MemBlob 内存 blob（测试用）。
type MemBlob struct {
	mu sync.Mutex
	m  map[string][]byte
}

func NewMemBlob() *MemBlob { return &MemBlob{m: map[string][]byte{}} }

func (m *MemBlob) Put(hash string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.m[hash] = append([]byte(nil), data...)
	return nil
}

func (m *MemBlob) Get(hash string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.m[hash]
	if !ok {
		return nil, fmt.Errorf("blob: %s not found", hash)
	}
	return append([]byte(nil), b...), nil
}

// WithBlob 注入 blob 存储（默认内存实现，生产经 FSBlob/S3）。
func (s *Service) WithBlob(b BlobStore) *Service {
	s.blob = b
	return s
}

// PutArtifact 上传产物：sha256 内容寻址 + 注册表登记 + artifact.produced 事件。
func (s *Service) PutArtifact(ctx context.Context, missionID, producedBy, schemaRef string, content []byte) (*store.Artifact, error) {
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	if err := s.blob.Put(hash, content); err != nil {
		return nil, err
	}
	a := &store.Artifact{
		ID:         "art_" + hash[:20],
		SHA256:     hash,
		MissionID:  missionID,
		ProducedBy: producedBy,
		SchemaRef:  schemaRef,
		Size:       int64(len(content)),
	}
	if err := s.st.PutArtifact(ctx, a, s.clk.Now()); err != nil {
		return nil, err
	}
	return a, nil
}

// GetArtifact 取产物元数据。
func (s *Service) GetArtifact(ctx context.Context, id string) (*store.Artifact, error) {
	return s.st.GetArtifact(ctx, id)
}

// GetArtifactContent 取产物本体。
func (s *Service) GetArtifactContent(ctx context.Context, id string) ([]byte, *store.Artifact, error) {
	a, err := s.st.GetArtifact(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	data, err := s.blob.Get(a.SHA256)
	if err != nil {
		return nil, nil, err
	}
	return data, a, nil
}
