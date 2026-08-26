package mission

import (
	"fmt"
	"sort"
	"strings"
)

const (
	ClassificationPublic       = "public"
	ClassificationInternal     = "internal"
	ClassificationConfidential = "confidential"
	ClassificationRestricted   = "restricted"
	BoardModeReadOnly          = "ro"
	BoardModeReadWrite         = "rw"
)

var classificationRank = map[string]int{
	ClassificationPublic: 0, ClassificationInternal: 1,
	ClassificationConfidential: 2, ClassificationRestricted: 3,
}

// NormalizePermissionEnvelope 校验并规范化权限集合，使子集判断和快照 hash 确定。
func NormalizePermissionEnvelope(in PermissionEnvelope) (PermissionEnvelope, error) {
	out := in
	out.BoardViews = make([]BoardGrant, len(in.BoardViews))
	for i, view := range in.BoardViews {
		out.BoardViews[i] = view
		out.BoardViews[i].Keys = append([]string(nil), view.Keys...)
	}
	if out.Classification == "" {
		out.Classification = ClassificationPublic
	}
	if _, ok := classificationRank[out.Classification]; !ok {
		return PermissionEnvelope{}, fmt.Errorf("unknown classification %q", out.Classification)
	}
	out.ToolScopes = uniqueSorted(out.ToolScopes)
	out.ArtifactRefs = uniqueSorted(out.ArtifactRefs)
	for i := range out.BoardViews {
		view := &out.BoardViews[i]
		if view.Namespace == "" {
			return PermissionEnvelope{}, fmt.Errorf("board namespace required")
		}
		if view.Mode == "" {
			view.Mode = BoardModeReadOnly
		}
		if view.Mode != BoardModeReadOnly && view.Mode != BoardModeReadWrite {
			return PermissionEnvelope{}, fmt.Errorf("board mode must be ro or rw")
		}
		view.Keys = uniqueSorted(view.Keys)
	}
	sort.Slice(out.BoardViews, func(i, j int) bool {
		if out.BoardViews[i].Namespace != out.BoardViews[j].Namespace {
			return out.BoardViews[i].Namespace < out.BoardViews[j].Namespace
		}
		if out.BoardViews[i].Mode != out.BoardViews[j].Mode {
			return out.BoardViews[i].Mode < out.BoardViews[j].Mode
		}
		return strings.Join(out.BoardViews[i].Keys, "\x00") < strings.Join(out.BoardViews[j].Keys, "\x00")
	})
	return out, nil
}

func uniqueSorted(in []string) []string {
	seen := map[string]struct{}{}
	for _, value := range in {
		if value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// PermissionEnvelopeSubset 判断 child 是否没有超出 parent 的密级、工具或数据视图。
func PermissionEnvelopeSubset(parent, child PermissionEnvelope) bool {
	p, err := NormalizePermissionEnvelope(parent)
	if err != nil {
		return false
	}
	c, err := NormalizePermissionEnvelope(child)
	if err != nil || classificationRank[c.Classification] > classificationRank[p.Classification] {
		return false
	}
	if !stringSubset(p.ToolScopes, c.ToolScopes) || !stringSubset(p.ArtifactRefs, c.ArtifactRefs) {
		return false
	}
	for _, childView := range c.BoardViews {
		covered := false
		for _, parentView := range p.BoardViews {
			if boardGrantCovers(parentView, childView) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func stringSubset(parent, child []string) bool {
	allowed := map[string]struct{}{}
	for _, value := range parent {
		allowed[value] = struct{}{}
	}
	for _, value := range child {
		if _, ok := allowed[value]; !ok {
			return false
		}
	}
	return true
}

func boardGrantCovers(parent, child BoardGrant) bool {
	if parent.Namespace != child.Namespace || parent.Mode == BoardModeReadOnly && child.Mode == BoardModeReadWrite {
		return false
	}
	if len(child.Keys) == 0 {
		return len(parent.Keys) == 0
	}
	if len(parent.Keys) == 0 {
		return true
	}
	return stringSubset(parent.Keys, child.Keys)
}
