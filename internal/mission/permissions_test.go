package mission

import "testing"

func TestPermissionEnvelopeSubset(t *testing.T) {
	parent := PermissionEnvelope{
		Classification: ClassificationConfidential,
		ToolScopes:     []string{"search", "publish"},
		ArtifactRefs:   []string{"art_a", "art_b"},
		BoardViews: []BoardGrant{
			{Namespace: "shared", Keys: []string{"a", "b"}, Mode: BoardModeReadWrite},
			{Namespace: "public", Mode: BoardModeReadOnly},
		},
	}
	valid := PermissionEnvelope{
		Classification: ClassificationInternal, ToolScopes: []string{"search"},
		ArtifactRefs: []string{"art_a"},
		BoardViews:   []BoardGrant{{Namespace: "shared", Keys: []string{"a"}, Mode: BoardModeReadOnly}},
	}
	if !PermissionEnvelopeSubset(parent, valid) {
		t.Fatal("narrower envelope must be accepted")
	}
	cases := map[string]PermissionEnvelope{
		"classification escalation": {Classification: ClassificationRestricted},
		"new tool":                  {ToolScopes: []string{"admin"}},
		"new artifact":              {ArtifactRefs: []string{"art_c"}},
		"new board key": {BoardViews: []BoardGrant{
			{Namespace: "shared", Keys: []string{"c"}, Mode: BoardModeReadOnly},
		}},
		"ro to rw": {BoardViews: []BoardGrant{
			{Namespace: "public", Keys: []string{"x"}, Mode: BoardModeReadWrite},
		}},
		"key list to wildcard": {BoardViews: []BoardGrant{
			{Namespace: "shared", Mode: BoardModeReadOnly},
		}},
	}
	for name, child := range cases {
		t.Run(name, func(t *testing.T) {
			if PermissionEnvelopeSubset(parent, child) {
				t.Fatalf("escalated child accepted: %+v", child)
			}
		})
	}
}

func TestNormalizePermissionEnvelope(t *testing.T) {
	got, err := NormalizePermissionEnvelope(PermissionEnvelope{
		ToolScopes: []string{"b", "a", "a"},
		BoardViews: []BoardGrant{{Namespace: "shared", Keys: []string{"z", "a"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Classification != ClassificationPublic || got.BoardViews[0].Mode != BoardModeReadOnly ||
		len(got.ToolScopes) != 2 || got.ToolScopes[0] != "a" || got.BoardViews[0].Keys[0] != "a" {
		t.Fatalf("normalized=%+v", got)
	}
	if _, err := NormalizePermissionEnvelope(PermissionEnvelope{Classification: "top-secret"}); err == nil {
		t.Fatal("unknown classification must be rejected")
	}
	if _, err := NormalizePermissionEnvelope(PermissionEnvelope{
		BoardViews: []BoardGrant{{Namespace: "shared", Mode: "admin"}},
	}); err == nil {
		t.Fatal("unknown board mode must be rejected")
	}
}
