// CEL 条件内核（M5-H1，§14.3）：wake_on.condition.expr 的编译/校验/求值。
// 语义决策见 docs/plan/M5-cel-scope.md §3：
//   - 注册时编译 + 类型检查 + 静态 cost 估算拒超（400）；运行时 CostLimit 中断按
//     false 处理并落 condition.cost_exceeded 事件（"超限视为 false 并告警"）；
//   - 数据模型 board.<ns>.<key> / mission.* / subtask.* / elapsed() / deadline_in()，
//     时钟函数只读注入的逻辑时钟，表达式内无裸系统时钟/随机源；
//   - 静态引用提取（board ns/key 键集）供 BoardPut 增量评估过滤；遇动态下标等
//     不可判定形态置 wildcard——宁多评不漏评，sweeper 全量兜底语义不变。
package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// ErrInvalidCondition CEL 条件注册校验失败（编译错误/结果非布尔多态/静态 cost 超限），
// API 映射 400（§14.3：非法表达式在注册边界拒绝，不进求值链路）。
var ErrInvalidCondition = errors.New("core: invalid condition")

// errConditionCostExceeded 运行时 cost 超限（视为 false + 事件留痕，区别于校验错误）。
var errConditionCostExceeded = errors.New("core: condition runtime cost limit exceeded")

// cost 双闸（§14.3 护栏）。静态估算按最坏输入规模拒明显失控的表达式；
// 运行时上限中断依赖大载荷数据的求值（静态估算对 dyn 输入按默认规模假设）。
const (
	MaxConditionStaticCost  = 10_000
	MaxConditionRuntimeCost = 100_000
)

// celEnv CEL 环境单例：声明数据模型与时钟函数签名（不含绑定——绑定随每次求值
// 注入当前逻辑时钟值，见 evalConditionExpr）。
var celEnv = mustCELEnv()

func mustCELEnv() *cel.Env {
	env, err := cel.NewEnv(
		// board: ns → key → JSON 解码值（点路径与下标写法等价；ns/key 含点号须用下标）
		cel.Variable("board", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("mission", cel.MapType(cel.StringType, cel.DynType)),
		cel.Variable("subtask", cel.MapType(cel.StringType, cel.DynType)),
		cel.Function("elapsed", cel.Overload("elapsed_duration", []*cel.Type{}, cel.DurationType)),
		cel.Function("deadline_in", cel.Overload("deadline_in_duration", []*cel.Type{}, cel.DurationType)),
	)
	if err != nil {
		panic("core: CEL env init: " + err.Error())
	}
	return env
}

// compiledCondition 编译产物缓存（按 expr 文本；不持久化，进程级）。
type compiledCondition struct {
	ast      *cel.Ast
	refs     []string // 静态 board 引用键集："ns/key"；"ns/*" 表整命名空间
	wildcard bool     // 引用不可静态判定（动态下标等）
}

var celCompileCache sync.Map // expr -> *compiledCondition（或 error 值缓存不必，注册边界低频）

// assumedMaxCollectionSize 静态估算对 dyn 输入（board 载荷）的规模假设上限。
// cel-go 默认把未知规模视为无限——任何 comprehension 都会被静态闸误杀；
// 有界假设让静态闸专注"结构上失控"的表达式（嵌套循环等），真实规模由运行时
// CostLimit 收口（M5 §3.4 cost 双闸的分工）。
const assumedMaxCollectionSize = 100

// boundedCostEstimator 未知规模按 assumedMaxCollectionSize 估算（内联字面量规模已知，
// cel-go 不会回调本估算器）。
type boundedCostEstimator struct{}

func (boundedCostEstimator) EstimateSize(checker.AstNode) *checker.SizeEstimate {
	return &checker.SizeEstimate{Min: 0, Max: assumedMaxCollectionSize}
}
func (boundedCostEstimator) EstimateCallCost(string, string, *checker.AstNode, []checker.AstNode) *checker.CallEstimate {
	return nil
}

// compileConditionExpr 注册期编译：解析 + 类型检查 + 结果类型约束 + 静态 cost 闸 +
// 引用提取。错误均包装 ErrInvalidCondition（API 400）。
func compileConditionExpr(expr string) (*compiledCondition, error) {
	if v, ok := celCompileCache.Load(expr); ok {
		return v.(*compiledCondition), nil
	}
	ast, iss := celEnv.Compile(expr)
	if iss.Err() != nil {
		return nil, fmt.Errorf("%w: compile: %v", ErrInvalidCondition, iss.Err())
	}
	// 结果须为 bool 或 dyn（board 载荷未类型化，flag 直取为 dyn，运行时收口）
	rt := ast.ResultType()
	prim, isPrim := rt.TypeKind.(*exprpb.Type_Primitive)
	isBool := isPrim && prim.Primitive == exprpb.Type_BOOL
	_, isDyn := rt.TypeKind.(*exprpb.Type_Dyn)
	if !isBool && !isDyn {
		return nil, fmt.Errorf("%w: expr must evaluate to bool, got %v", ErrInvalidCondition, rt)
	}
	est, err := celEnv.EstimateCost(ast, boundedCostEstimator{})
	if err != nil {
		return nil, fmt.Errorf("%w: cost estimate: %v", ErrInvalidCondition, err)
	}
	if est.Max > MaxConditionStaticCost {
		return nil, fmt.Errorf("%w: static cost %d exceeds limit %d", ErrInvalidCondition, est.Max, MaxConditionStaticCost)
	}
	refs, wildcard := extractBoardRefs(ast)
	cc := &compiledCondition{ast: ast, refs: refs, wildcard: wildcard}
	celCompileCache.Store(expr, cc)
	return cc, nil
}

// referencesBoard 增量过滤判定：changedBoard（"ns/key"）是否可能影响该注册。
// wildcard 或未持久化键集的旧注册恒真（宁多评不漏评）。
func (c *compiledCondition) referencesBoard(changedBoard string) bool {
	if c.wildcard {
		return true
	}
	for _, r := range c.refs {
		if r == changedBoard {
			return true
		}
		if strings.HasSuffix(r, "/*") && strings.HasPrefix(changedBoard, r[:len(r)-1]) {
			return true
		}
	}
	return false
}

// evalConditionExpr CEL 路径求值。求值错误（缺键/类型不符等）按 false 处理
// （与 exists 缺键为假的语义一致）；仅运行时 cost 超限返回 errConditionCostExceeded。
func (s *Service) evalConditionExpr(ctx context.Context, sub *mission.Subtask, w *mission.WakeSpec) (bool, error) {
	cc, err := compileConditionExpr(w.Condition.Expr)
	if err != nil {
		// 注册时已校验，走到这里说明存储中的 expr 被绕过校验写入——按 false 处理并显式报错
		return false, err
	}
	board, err := s.celBoardView(ctx, sub.MissionID, cc)
	if err != nil {
		return false, err
	}
	m, err := s.st.GetMission(ctx, sub.MissionID)
	if err != nil {
		return false, err
	}
	now := s.clk.Now()
	registeredAt := now
	if w.RegisteredAt != nil {
		registeredAt = *w.RegisteredAt
	}
	deadline := now
	if w.Deadline != nil {
		deadline = *w.Deadline
	}
	// 时钟函数绑定注入逻辑时钟（ADR-8：表达式内无裸 time.Now）
	ext, err := celEnv.Extend(
		cel.Function("elapsed", cel.Overload("elapsed_duration", []*cel.Type{}, cel.DurationType,
			cel.FunctionBinding(func(...ref.Val) ref.Val {
				return types.Duration{Duration: now.Sub(registeredAt)}
			}))),
		cel.Function("deadline_in", cel.Overload("deadline_in_duration", []*cel.Type{}, cel.DurationType,
			cel.FunctionBinding(func(...ref.Val) ref.Val {
				return types.Duration{Duration: deadline.Sub(now)}
			}))),
	)
	if err != nil {
		return false, err
	}
	prg, err := ext.Program(cc.ast, cel.CostLimit(MaxConditionRuntimeCost), cel.EvalOptions(cel.OptTrackCost))
	if err != nil {
		return false, err
	}
	out, _, err := prg.Eval(map[string]any{
		"board":   board,
		"mission": map[string]any{"id": m.ID, "owner": m.Owner, "goal": m.Goal, "status": string(m.Status)},
		"subtask": map[string]any{
			"id": sub.ID, "kind": string(sub.Kind), "state": string(sub.State),
			"attempt": int64(sub.Attempt), "assignee": sub.Assignee,
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "cost limit") {
			return false, errConditionCostExceeded
		}
		return false, nil // 缺键/类型不符等：条件不成立
	}
	v, ok := out.Value().(bool)
	return ok && v, nil
}

// celBoardView 构建 board 数据视图（ns → key → JSON 解码值）。
// 只加载引用涉及的命名空间；wildcard 注册加载全量。
func (s *Service) celBoardView(ctx context.Context, missionID string, cc *compiledCondition) (map[string]any, error) {
	nss := map[string]bool{}
	if !cc.wildcard {
		for _, r := range cc.refs {
			ns, _, _ := strings.Cut(r, "/")
			nss[ns] = true
		}
	}
	board := map[string]any{}
	put := func(entries []*store.BoardEntry) {
		for _, e := range entries {
			g, _ := board[e.Namespace].(map[string]any)
			if g == nil {
				g = map[string]any{}
				board[e.Namespace] = g
			}
			g[e.Key] = jsonToCEL(e.Value)
		}
	}
	if cc.wildcard {
		entries, err := s.st.BoardList(ctx, missionID, "") // 空 ns = 全量
		if err != nil {
			return nil, err
		}
		put(entries)
		return board, nil
	}
	for ns := range nss {
		entries, err := s.st.BoardList(ctx, missionID, ns)
		if err != nil {
			return nil, err
		}
		put(entries)
	}
	return board, nil
}

// jsonToCEL JSON 解码值转 CEL 友好的 Go 值：整数值的 float64 归一为 int64
//（CEL 不做隐式数值转换，`board.ns.count >= 2` 依赖 int 比较）。
func jsonToCEL(raw []byte) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw) // 非 JSON 载荷按字符串暴露
	}
	return normCEL(v)
}

func normCEL(v any) any {
	switch t := v.(type) {
	case float64:
		if t == math.Trunc(t) && t >= math.MinInt64 && t <= math.MaxInt64 {
			return int64(t)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = normCEL(e)
		}
		return t
	case map[string]any:
		for k, e := range t {
			t[k] = normCEL(e)
		}
		return t
	default:
		return v
	}
}

// ---- 静态引用提取（M5 §3.5） ----

// extractBoardRefs 遍历 checked AST 提取 board 的静态引用键集。
// 只关心 ns/key 粒度（更深路径归并到 ns/key）；任何动态形态（变量下标、
// board 整体出现在非常量链中）置 wildcard。
func extractBoardRefs(ast *cel.Ast) ([]string, bool) {
	checked, err := cel.AstToCheckedExpr(ast)
	if err != nil {
		return nil, true
	}
	x := &refExtractor{seen: map[string]bool{}}
	x.walk(checked.Expr)
	return x.refs, x.wildcard
}

type refExtractor struct {
	refs     []string
	seen     map[string]bool
	wildcard bool
}

func (x *refExtractor) add(ref string) {
	if !x.seen[ref] {
		x.seen[ref] = true
		x.refs = append(x.refs, ref)
	}
}

// boardChain 还原 "board" 为根的 select/index 常量链。
// 返回 segs（[ns, key, ...]）；dynamic 表链中有非常量段（此时该引用按 wildcard 处理）；
// ok 表整条链以 board 为根。
func boardChain(e *exprpb.Expr) (segs []string, dynamic, ok bool) {
	switch k := e.ExprKind.(type) {
	case *exprpb.Expr_IdentExpr:
		return nil, false, k.IdentExpr.Name == "board"
	case *exprpb.Expr_SelectExpr:
		segs, dynamic, ok = boardChain(k.SelectExpr.Operand)
		if !ok {
			return nil, false, false
		}
		return append(segs, k.SelectExpr.Field), dynamic, true
	case *exprpb.Expr_CallExpr:
		if k.CallExpr.Function == "_[_]" && len(k.CallExpr.Args) == 2 {
			segs, dynamic, ok = boardChain(k.CallExpr.Args[0])
			if !ok {
				return nil, false, false
			}
			c, isConst := k.CallExpr.Args[1].ExprKind.(*exprpb.Expr_ConstExpr)
			var str *exprpb.Constant_StringValue
			var isStr bool
			if isConst {
				str, isStr = c.ConstExpr.ConstantKind.(*exprpb.Constant_StringValue)
			}
			if !isConst || !isStr {
				return segs, true, true // 变量/非常量下标：不可静态判定
			}
			return append(segs, str.StringValue), dynamic, true
		}
	}
	return nil, false, false
}

func (x *refExtractor) walk(e *exprpb.Expr) {
	if e == nil || x.wildcard {
		return
	}
	switch k := e.ExprKind.(type) {
	case *exprpb.Expr_IdentExpr:
		if k.IdentExpr.Name == "board" {
			x.record(nil, false) // 裸 board 整体引用：无法静态判定键集 → wildcard
		}
	case *exprpb.Expr_SelectExpr:
		if segs, dynamic, ok := boardChain(e); ok {
			x.record(segs, dynamic)
			return // 链内节点已处理，不再下钻
		}
		x.walk(k.SelectExpr.Operand)
	case *exprpb.Expr_CallExpr:
		if k.CallExpr.Function == "_[_]" {
			if segs, dynamic, ok := boardChain(e); ok {
				x.record(segs, dynamic)
				return
			}
		}
		if k.CallExpr.Target != nil {
			x.walk(k.CallExpr.Target)
		}
		for _, a := range k.CallExpr.Args {
			x.walk(a)
		}
	case *exprpb.Expr_ListExpr:
		for _, el := range k.ListExpr.Elements {
			x.walk(el)
		}
	case *exprpb.Expr_StructExpr:
		for _, en := range k.StructExpr.Entries {
			x.walk(en.GetMapKey())
			x.walk(en.Value)
		}
	case *exprpb.Expr_ComprehensionExpr:
		c := k.ComprehensionExpr
		x.walk(c.IterRange)
		x.walk(c.AccuInit)
		x.walk(c.LoopCondition)
		x.walk(c.LoopStep)
		x.walk(c.Result)
	}
}

// record 归并一条 board 引用到键集：动态段→wildcard；board 裸引用→wildcard；
// 仅 ns→"ns/*"；ns+key→"ns/key"（更深路径归并）。
func (x *refExtractor) record(segs []string, dynamic bool) {
	switch {
	case dynamic || len(segs) == 0:
		x.wildcard = true
	case len(segs) == 1:
		x.add(segs[0] + "/*")
	default:
		x.add(segs[0] + "/" + segs[1])
	}
}

