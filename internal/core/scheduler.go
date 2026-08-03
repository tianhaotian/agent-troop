// 放置调度（M3-T1/T2，§5.2）：策略插件化。
// 调度循环固定（取就绪 → 策略打分 → OfferLease CAS），策略只负责
// "这个子任务给这个 Agent 打多少分、是否合格"，便于插拔与 A/B。
package core

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"agenttroop/internal/mission"
	"agenttroop/internal/store"
)

// PlacementStrategy 放置策略（Filter 并入 Score：不合格返回 ok=false）。
type PlacementStrategy interface {
	Name() string
	// Score 为 (子任务, Agent) 打分；ok=false 表示该 Agent 不合格（健康/技能/并发）。
	Score(sub *mission.Subtask, a *store.Agent, now time.Time) (score float64, ok bool)
}

// PlacementObserver 可选钩子：策略需要感知放置结果时实现（如轮询游标）。
type PlacementObserver interface {
	OnPlaced(agentID string)
}

// NewStrategy 按名构造策略（TROOP_SCHEDULER；未知名报错，启动期 fail-fast）。
func NewStrategy(name string) (PlacementStrategy, error) {
	switch name {
	case "", "capability-first":
		return CapabilityFirst{}, nil
	case "round-robin":
		return NewRoundRobin(), nil
	default:
		return nil, fmt.Errorf("core: unknown scheduler strategy %q", name)
	}
}

// ---- 内置策略：capability-first（默认，M1 语义 + T2 紧迫度负载惩罚） ----

// CapabilityFirst 技能契合优先：score = 技能木桶分 − 负载惩罚。
// 负载惩罚随任务紧迫度放大（T2）：紧迫任务更偏向空闲 Agent。
type CapabilityFirst struct{}

func (CapabilityFirst) Name() string { return "capability-first" }

func (CapabilityFirst) Score(sub *mission.Subtask, a *store.Agent, now time.Time) (float64, bool) {
	if !eligible(a, sub) {
		return 0, false
	}
	level, _ := matchSkills(a, sub.RequiredSkills)
	return level - loadPenalty(sub, now)*float64(a.Running), true
}

// loadPenalty 基础 0.1，按优先级与 deadline 紧迫度放大（最高 ×3）。
func loadPenalty(sub *mission.Subtask, now time.Time) float64 {
	boost := 0.0
	if p := sub.Scheduling.Priority; p > 0 {
		boost += math.Min(float64(p), 10) / 10 // 优先级 0~10 → +0~1
	}
	if dl := sub.Scheduling.Deadline; dl != nil {
		switch left := dl.Sub(now); {
		case left <= time.Hour: // 含已逾期
			boost += 1.0
		case left <= 6*time.Hour:
			boost += 0.5
		case left <= 24*time.Hour:
			boost += 0.2
		}
	}
	return 0.1 * (1 + math.Min(boost, 2))
}

// ---- 内置策略：round-robin（均匀分散，验证插件机制） ----

// RoundRobin 偏好最久未获派的 Agent（合格性过滤与默认策略一致）。
type RoundRobin struct {
	seq  int64
	last map[string]int64
}

func NewRoundRobin() *RoundRobin { return &RoundRobin{last: map[string]int64{}} }

func (r *RoundRobin) Name() string { return "round-robin" }

func (r *RoundRobin) Score(sub *mission.Subtask, a *store.Agent, _ time.Time) (float64, bool) {
	if !eligible(a, sub) {
		return 0, false
	}
	return -float64(r.last[a.ID]), true // 从未获派者并列 0，按 Agent ID 序取先
}

func (r *RoundRobin) OnPlaced(agentID string) {
	r.seq++
	r.last[agentID] = r.seq
}

// ---- 调度循环（策略无关） ----

// ScheduleOnce 单轮调度：取就绪任务（priority desc / deadline asc，store 层排序）
// → 策略打分 → 发放租约。高优先级任务先抢容量（本轮内 Running 本地视图递增）。
// 多副本安全：竞争同一任务时 OfferLease 的 CAS 保证只有一个赢家。
func (s *Service) ScheduleOnce(ctx context.Context) (int, error) {
	ready, err := s.st.DequeueReady(ctx, s.cfg.ScheduleBatch)
	if err != nil {
		return 0, err
	}
	agents, err := s.st.ListAgents(ctx)
	if err != nil {
		return 0, err
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].ID < agents[j].ID }) // 确定性打分次序
	now := s.clk.Now()
	placed := 0
	for _, sub := range ready {
		if isHumanKind(sub.Kind) {
			continue // human 节点由 OpenHumanDecisions 处理，不参与 Agent 放置
		}
		best := s.pickAgent(agents, sub, now)
		if best == nil {
			continue // 无合格 Agent：留在 READY 等下轮
		}
		if _, err := s.st.OfferLease(ctx, sub.ID, best.ID, sub.Version, s.cfg.OfferTTL,
			store.Actor{Kind: "system", ID: "scheduler"}, now); err != nil {
			continue // CAS 竞争失败/状态已变，下轮再来
		}
		best.Running++ // 本轮内的本地视图修正
		if obs, ok := s.strategy.(PlacementObserver); ok {
			obs.OnPlaced(best.ID)
		}
		placed++
	}
	return placed, nil
}

// pickAgent 用当前策略选出最高分合格 Agent。
func (s *Service) pickAgent(agents []*store.Agent, sub *mission.Subtask, now time.Time) *store.Agent {
	var best *store.Agent
	bestScore := 0.0
	for _, a := range agents {
		score, ok := s.strategy.Score(sub, a, now)
		if !ok {
			continue
		}
		if best == nil || score > bestScore {
			best, bestScore = a, score
		}
	}
	return best
}

// eligible 合格性过滤：健康 ∧ 并发余量 ∧ 技能齐全（各策略共用）。
func eligible(a *store.Agent, sub *mission.Subtask) bool {
	if a.Health != "" && a.Health != "healthy" {
		return false
	}
	if a.MaxConcurrency > 0 && a.Running >= a.MaxConcurrency {
		return false
	}
	_, ok := matchSkills(a, sub.RequiredSkills)
	return ok
}

// matchSkills 返回匹配技能的最低 level（木桶原则）；缺技能返回 false。
func matchSkills(a *store.Agent, required []string) (float64, bool) {
	if len(required) == 0 {
		return 0.5, true // 无技能要求：任何健康 Agent 皆可，基础分
	}
	levels := map[string]float64{}
	for _, c := range a.Capabilities {
		levels[c.Skill] = c.Level
	}
	min := 1.0
	for _, sk := range required {
		lv, ok := levels[sk]
		if !ok {
			return 0, false
		}
		if lv < min {
			min = lv
		}
	}
	return min, true
}
