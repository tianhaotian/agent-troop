package core

import (
	"errors"
	"strings"
	"testing"
	"time"

	"agenttroop/internal/mission"
)

// ---- H1：CEL 条件内核（docs/plan/M5-cel-scope.md §5） ----

// suspendCEL 以 CEL 条件挂起子任务（注册期校验在此触发）。
func suspendCEL(t *testing.T, s *Service, sub *mission.Subtask, token int64, expr string, ttl time.Time) error {
	t.Helper()
	_, err := s.Suspend(ctx, sub.ID, token, sub.Version, sub.Assignee,
		&mission.WakeSpec{Kind: mission.WakeCondition, Deadline: &ttl,
			Condition: &mission.BoardCondition{Expr: expr}}, nil)
	return err
}

func TestCELCompileErrorRejected(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "编译拒绝", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)

	// 语法错误
	if err := suspendCEL(t, s, subB, tokenB, `board.shared.count >=`, ttl); !errors.Is(err, ErrInvalidCondition) {
		t.Fatalf("syntax error must be rejected with ErrInvalidCondition, got %v", err)
	}
	// 未声明变量
	if err := suspendCEL(t, s, subB, tokenB, `unknown.var == 1`, ttl); !errors.Is(err, ErrInvalidCondition) {
		t.Fatalf("undeclared var must be rejected, got %v", err)
	}
	// 结果非 bool/dyn
	if err := suspendCEL(t, s, subB, tokenB, `mission.id + "x"`, ttl); !errors.Is(err, ErrInvalidCondition) {
		t.Fatalf("non-bool result must be rejected, got %v", err)
	}
	// 校验失败不迁移状态：仍 RUNNING
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateRunning {
		t.Fatalf("rejected suspend must not transition, state = %s", cur.State)
	}
}

func TestCELConditionMutualExclusion(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	_, _ = s.CreateMission(ctx, "u1", "互斥校验", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)

	// expr 与结构化谓词同现 → 拒
	_, err := s.Suspend(ctx, subB.ID, tokenB, subB.Version, "agt_b",
		&mission.WakeSpec{Kind: mission.WakeCondition, Deadline: &ttl,
			Condition: &mission.BoardCondition{Expr: `true`, Board: "shared/x", Op: mission.CondExists}}, nil)
	if !errors.Is(err, ErrInvalidCondition) {
		t.Fatalf("expr+board must be rejected, got %v", err)
	}
	// 同缺 → 拒
	_, err = s.Suspend(ctx, subB.ID, tokenB, subB.Version, "agt_b",
		&mission.WakeSpec{Kind: mission.WakeCondition, Deadline: &ttl,
			Condition: &mission.BoardCondition{}}, nil)
	if err == nil {
		t.Fatal("empty condition must be rejected")
	}
}

func TestCELStaticCostRejected(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	s.CreateMission(ctx, "u1", "静态 cost", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)

	// 嵌套 comprehension：静态估算（未知规模按上限假设）即超限（§14.3 注册边界拒绝）
	nested := `board.a.l.exists(x, board.b.l.exists(y, board.c.l.exists(z, x + y + z > 100)))`
	if err := suspendCEL(t, s, subB, tokenB, nested, ttl); !errors.Is(err, ErrInvalidCondition) {
		t.Fatalf("static cost overflow must be rejected, got %v", err)
	}
}

func TestCELConditionWakeBoardPut(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "CEL 条件唤醒", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")

	ttl := clk.Now().Add(time.Hour)
	if err := suspendCEL(t, s, subB, tokenB, `board.shared.count >= 2`, ttl); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// 注册引用键集随 wake_spec 持久化
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("state = %s, want WAITING", cur.State)
	}
	// 未命中值 → 不醒（增量评估为 false）
	if _, err := s.BoardPut(ctx, m.ID, "shared", "count", []byte(`1`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("count=1 must not wake, state = %s", cur.State)
	}
	// 命中 → BoardPut 增量钩子直接唤醒（无需 sweep）
	if _, err := s.BoardPut(ctx, m.ID, "shared", "count", []byte(`2`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateReady {
		t.Fatalf("count=2 should wake, state = %s", cur.State)
	}
}

func TestCELConditionRefsPersisted(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "引用键集", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)
	if err := suspendCEL(t, s, subB, tokenB,
		`board.shared.count >= 2 && board["cfg.ns"]["mode"] == "x"`, ttl); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	cur := mustGet(t, s, m.ID, subB.ID)
	w := wakeSpecOf(cur)
	if w == nil || w.Condition == nil {
		t.Fatal("wake spec must persist")
	}
	want := map[string]bool{"shared/count": true, "cfg.ns/mode": true}
	if len(w.Condition.Refs) != len(want) {
		t.Fatalf("refs = %v", w.Condition.Refs)
	}
	for _, r := range w.Condition.Refs {
		if !want[r] {
			t.Fatalf("unexpected ref %q in %v", r, w.Condition.Refs)
		}
	}
	if w.Condition.RefsWildcard {
		t.Fatal("static expr must not be wildcard")
	}
	if w.RegisteredAt == nil {
		t.Fatal("registered_at must be stamped at suspend")
	}
}

func TestCELConditionDynamicIndexWildcard(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "动态下标", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)
	// 动态下标（mission.owner 为变量）→ refs=*，任意 BoardPut 都评估
	if err := suspendCEL(t, s, subB, tokenB, `board[mission.owner].ready == true`, ttl); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); wakeSpecOf(cur).Condition.RefsWildcard != true {
		t.Fatal("dynamic index must mark refs wildcard")
	}
	// 命中键经动态下标解析（owner=u1）：静态键集为空也能被增量评估命中
	if _, err := s.BoardPut(ctx, m.ID, "u1", "ready", []byte(`true`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateReady {
		t.Fatalf("wildcard registration must be evaluated on any BoardPut, state = %s", cur.State)
	}
}

func TestCELRuntimeCostExceeded(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "运行时 cost", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)
	// 静态估算对 dyn 输入按默认规模假设 → 通过注册；运行时面对大载荷超限
	if err := suspendCEL(t, s, subB, tokenB, `board.shared.items.exists(x, x > 0)`, ttl); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// 200k 元素全不命中：exists 全扫描，运行时 cost 超上限 → 视为 false + 留痕
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < 200000; i++ {
		b.WriteString("0,")
	}
	b.WriteString("0]")
	if _, err := s.BoardPut(ctx, m.ID, "shared", "items", []byte(b.String()), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("cost-exceeded must be treated as false, state = %s", cur.State)
	}
	evs, err := s.ListMissionEvents(ctx, m.ID, 0, 100)
	if err != nil {
		t.Fatalf("ListMissionEvents: %v", err)
	}
	found := false
	for _, e := range evs {
		if e.Type == "condition.cost_exceeded" {
			found = true
		}
	}
	if !found {
		t.Fatal("condition.cost_exceeded event must be recorded (§14.3 超限告警)")
	}
}

func TestCELClockSweepFallback(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "时钟函数", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")

	// deadline 1h 后 → deadline_in() < 2h 立即为真
	ttl := clk.Now().Add(time.Hour)
	if err := suspendCEL(t, s, subB, tokenB, `deadline_in() < duration("2h")`, ttl); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// 表达式不引用 board（键集为空）：BoardPut 增量评估跳过（M5 §3.6）
	if _, err := s.BoardPut(ctx, m.ID, "shared", "noise", []byte(`1`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("non-board expr must be skipped in incremental eval, state = %s", cur.State)
	}
	// sweeper 全量兜底负责（§14.4 level-triggered 由兜底保证）
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateReady {
		t.Fatalf("sweeper fallback should wake, state = %s", cur.State)
	}
}

func TestCELCrossMissionIsolation(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m1, _ := s.CreateMission(ctx, "u1", "M1", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)
	if err := suspendCEL(t, s, subB, tokenB, `board.shared.go == true`, ttl); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	// 另一个 Mission 写同名黑板键：增量（missionID 过滤）与全量（数据视图隔离）都不得命中
	m2, _ := s.CreateMission(ctx, "u2", "M2", []TaskSpec{
		{Name: "other", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	if _, err := s.BoardPut(ctx, m2.ID, "shared", "go", []byte(`true`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if err := s.SweepOnce(ctx); err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if cur := mustGet(t, s, m1.ID, subB.ID); cur.State != mission.StateWaiting {
		t.Fatalf("cross-mission board must not satisfy condition, state = %s", cur.State)
	}
}

// TestCELStructuredRegression M4 结构化谓词与 CEL 双路径共存回归。
func TestCELStructuredRegression(t *testing.T) {
	s, _, clk := newService()
	mustRegister(t, s, "agt_b", 2, "code.review")
	m, _ := s.CreateMission(ctx, "u1", "双路径回归", []TaskSpec{
		{Name: "waiter", Kind: mission.KindAgent, RequiredSkills: []string{"code.review"}},
	})
	subB, tokenB := startOne(t, s, "agt_b")
	ttl := clk.Now().Add(time.Hour)
	if _, err := s.Suspend(ctx, subB.ID, tokenB, subB.Version, "agt_b",
		&mission.WakeSpec{Kind: mission.WakeCondition, Deadline: &ttl,
			Condition: &mission.BoardCondition{Board: "shared/flag", Op: mission.CondExists}}, nil); err != nil {
		t.Fatalf("Suspend: %v", err)
	}
	if _, err := s.BoardPut(ctx, m.ID, "shared", "flag", []byte(`1`), -1); err != nil {
		t.Fatalf("BoardPut: %v", err)
	}
	if cur := mustGet(t, s, m.ID, subB.ID); cur.State != mission.StateReady {
		t.Fatalf("structured predicate path must keep working, state = %s", cur.State)
	}
}

// TestExtractBoardRefs 静态引用提取单元测试（含 ns 通配与动态形态）。
func TestExtractBoardRefs(t *testing.T) {
	cases := []struct {
		expr     string
		refs     []string
		wildcard bool
	}{
		{`board.shared.count >= 2`, []string{"shared/count"}, false},
		{`board["cfg.ns"]["mode"] == "x"`, []string{"cfg.ns/mode"}, false},
		{`board.shared.deep.inner.x == 1`, []string{"shared/deep"}, false}, // 深路径归并到 ns/key
		{`board.shared == 1`, []string{"shared/*"}, false},                 // 仅 ns
		{`board[subtask.id].x == 1`, nil, true},                            // 动态下标
		{`board == board`, nil, true},                                    // 裸 board
		{`mission.owner == "u1" && elapsed() > duration("0s")`, nil, false},
		{`board.a.x == 1 && board.b.y == 2`, []string{"a/x", "b/y"}, false},
	}
	for _, tc := range cases {
		cc, err := compileConditionExpr(tc.expr)
		if err != nil {
			t.Fatalf("%s: compile: %v", tc.expr, err)
		}
		if cc.wildcard != tc.wildcard {
			t.Fatalf("%s: wildcard = %v, want %v", tc.expr, cc.wildcard, tc.wildcard)
		}
		if len(cc.refs) != len(tc.refs) {
			t.Fatalf("%s: refs = %v, want %v", tc.expr, cc.refs, tc.refs)
		}
		for i, r := range cc.refs {
			if r != tc.refs[i] {
				t.Fatalf("%s: refs = %v, want %v", tc.expr, cc.refs, tc.refs)
			}
		}
	}
}
