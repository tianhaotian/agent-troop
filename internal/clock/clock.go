// Package clock 提供注入式时钟与种子化随机源。
//
// ADR-8（设计文档 §22.8）：全代码库禁止裸调用 time.Now() / math/rand，
// 一切时间与随机必须经本包注入——这是 §20 确定性回放与虚拟时钟的前置。
package clock

import (
	"math/rand"
	"sync"
	"time"
)

// Clock 是可注入的时间源。
type Clock interface {
	Now() time.Time
}

// RealClock 生产环境时钟。
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// FakeClock 测试/仿真用手动推进时钟。
type FakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func NewFake(t time.Time) *FakeClock { return &FakeClock{t: t} }

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.t
}

// Advance 推进时钟（仅测试/仿真使用）。
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.t = f.t.Add(d)
}

// Rand 种子化随机源包装（退避抖动、探索流量抽样等场景使用）。
type Rand struct{ r *rand.Rand }

// NewRand 以显式种子构造，保证相同种子下行为可复现。
func NewRand(seed int64) *Rand { return &Rand{r: rand.New(rand.NewSource(seed))} }

func (s *Rand) Int63n(n int64) int64  { return s.r.Int63n(n) }
func (s *Rand) Float64() float64      { return s.r.Float64() }
func (s *Rand) Perm(n int) []int      { return s.r.Perm(n) }
