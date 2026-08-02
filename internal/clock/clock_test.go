package clock

import (
	"testing"
	"time"
)

func TestFakeClockAdvance(t *testing.T) {
	base := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	f := NewFake(base)
	f.Advance(90 * time.Second)
	if got := f.Now(); !got.Equal(base.Add(90*time.Second)) {
		t.Errorf("FakeClock.Now() = %v, want %v", got, base.Add(90*time.Second))
	}
}

func TestRandDeterministic(t *testing.T) {
	// ADR-8：相同种子必须产生相同序列（确定性回放前提）
	a, b := NewRand(42), NewRand(42)
	for i := 0; i < 100; i++ {
		if x, y := a.Float64(), b.Float64(); x != y {
			t.Fatalf("seeded rand diverged at %d: %v != %v", i, x, y)
		}
	}
}
