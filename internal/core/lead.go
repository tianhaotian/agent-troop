package core

// Lead 恢复闭环（M7B，设计 §15.1/§15.2/§15.4）：显式结果摄入、
// 版本化计划快照，以及由 sweeper 驱动的 fencing takeover。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"agenttroop/internal/store"
)

const MaxLeadSnapshotSize = 256 << 10

var ErrInvalidLeadInput = errors.New("core: invalid lead input")

type LeadSnapshot struct {
	MissionID string          `json:"mission_id"`
	SubtaskID string          `json:"subtask_id"`
	Value     json.RawMessage `json:"value"`
	Version   int64           `json:"version"`
	UpdatedAt time.Time       `json:"updated_at"`
}

type LeadContext struct {
	Snapshot *LeadSnapshot          `json:"snapshot,omitempty"`
	Inbox    []*store.LeadInboxItem `json:"inbox"`
}

func leadSnapshot(entry *store.BoardEntry) *LeadSnapshot {
	if entry == nil {
		return nil
	}
	return &LeadSnapshot{MissionID: entry.MissionID, SubtaskID: entry.Key,
		Value: json.RawMessage(entry.Value), Version: entry.Version, UpdatedAt: entry.UpdatedAt}
}

func (s *Service) ListLeadInbox(ctx context.Context, leadSubtaskID string,
	pendingOnly bool) ([]*store.LeadInboxItem, error) {
	if _, err := s.st.GetSubtask(ctx, leadSubtaskID); err != nil {
		return nil, err
	}
	items, err := s.st.ListLeadInbox(ctx, leadSubtaskID, pendingOnly)
	if items == nil {
		items = []*store.LeadInboxItem{}
	}
	return items, err
}

func (s *Service) IngestLeadInbox(ctx context.Context, leadSubtaskID, itemID, agentID string,
	fencingToken, expectedVersion int64, mode string) (*store.LeadInboxItem, error) {
	if mode != store.LeadIngestSummary && mode != store.LeadIngestFull {
		return nil, fmt.Errorf("%w: ingest mode must be %q or %q",
			ErrInvalidLeadInput, store.LeadIngestSummary, store.LeadIngestFull)
	}
	if _, err := s.authorizeSubtaskLeaseOwner(ctx, leadSubtaskID, fencingToken, agentID, true); err != nil {
		return nil, err
	}
	return s.st.IngestLeadInbox(ctx, itemID, leadSubtaskID, fencingToken, expectedVersion,
		mode, store.Actor{Kind: "agent", ID: agentID}, s.clk.Now())
}

func (s *Service) SaveLeadSnapshot(ctx context.Context, leadSubtaskID, agentID string,
	fencingToken, expectedVersion int64, snapshot json.RawMessage) (*LeadSnapshot, error) {
	if len(snapshot) == 0 || !json.Valid(snapshot) {
		return nil, fmt.Errorf("%w: lead snapshot must be valid JSON", ErrInvalidLeadInput)
	}
	if len(snapshot) > MaxLeadSnapshotSize {
		return nil, fmt.Errorf("%w: lead snapshot exceeds %d bytes", ErrInvalidLeadInput, MaxLeadSnapshotSize)
	}
	if expectedVersion < -1 {
		return nil, fmt.Errorf("%w: snapshot expected_version must be >= -1", ErrInvalidLeadInput)
	}
	if _, err := s.authorizeSubtaskLeaseOwner(ctx, leadSubtaskID, fencingToken, agentID, true); err != nil {
		return nil, err
	}
	entry, err := s.st.SaveLeadSnapshot(ctx, leadSubtaskID, fencingToken, expectedVersion, snapshot,
		s.cfg.LeadHeartbeatTTL, store.Actor{Kind: "agent", ID: agentID}, s.clk.Now())
	return leadSnapshot(entry), err
}

func (s *Service) GetLeadContext(ctx context.Context, leadSubtaskID string) (*LeadContext, error) {
	lead, err := s.st.GetSubtask(ctx, leadSubtaskID)
	if err != nil {
		return nil, err
	}
	snapshot, err := s.st.BoardGet(ctx, lead.MissionID, "lead-plan", lead.ID)
	if errors.Is(err, store.ErrNotFound) {
		snapshot = nil
	} else if err != nil {
		return nil, err
	}
	// 恢复视图包含全部条目及其 ingest 状态，避免“刚 ingest、尚未来得及更新快照”
	// 的崩溃窗口让继任者误以为结果从未存在。
	inbox, err := s.st.ListLeadInbox(ctx, lead.ID, false)
	if err != nil {
		return nil, err
	}
	if inbox == nil {
		inbox = []*store.LeadInboxItem{}
	}
	return &LeadContext{Snapshot: leadSnapshot(snapshot), Inbox: inbox}, nil
}
