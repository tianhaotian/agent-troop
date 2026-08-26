package pg

import "testing"

func TestTextArrayNormalizesNil(t *testing.T) {
	got := textArray(nil)
	if got == nil || len(got) != 0 {
		t.Fatalf("textArray(nil) = %#v, want non-nil empty slice", got)
	}

	in := []string{"sub_a"}
	got = textArray(in)
	if len(got) != 1 || got[0] != "sub_a" {
		t.Fatalf("textArray(%#v) = %#v", in, got)
	}
}
