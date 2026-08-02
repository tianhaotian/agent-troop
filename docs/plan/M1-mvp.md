# M1 MVP 实现计划

> 对应设计文档 §12.2 M1。目标：**两个 Agent（一个经通用 HTTP Adapter 接入的回显 Agent + 一个模拟 Agent）接力完成一次 Mission，全程状态可查、事件可回放。**
> 流程约定：本计划先行；每个切片实现后 `go test ./...` 全绿才标记完成。

## 1. 范围（In Scope）

| # | 切片 | 内容 | 对应设计章节 |
|---|------|------|-------------|
| S1 | 任务模型与状态机 | Mission/Subtask 类型、状态机迁移（含 WAITING/BLOCKED/CANCELLED 路径）、事件定义与 `Apply(event)` | §3.2 |
| S2 | PG schema 与存储层 | migrations/0001：missions/subtasks/agents/leases/artifacts/events/decisions；store 包：条件更新（乐观锁 version）、Outbox 同事务写事件 | §4.2, §4.3 |
| S3 | 就绪队列 | `SELECT ... FOR UPDATE SKIP LOCKED` 出队、按 priority/deadline 排序 | §4.1, §5.1 |
| S4 | 租约与 Fencing | 租约发放/续期/过期回收、fencing token 单调校验、幂等键去重 | §4.3 |
| S5 | DAG 编排 | 依赖就绪传播（上游 SUCCEEDED → 下游 READY）、Mission 完成/失败终态判定 | §5.1 |
| S6 | Capability-First 调度 | Filter：能力匹配 ∧ 健康 ∧ 并发余量；Score：技能契合 + 负载；暂不接信誉 | §5.2, §5.3 |
| S7 | Agent 注册与心跳 | register/heartbeat/deregister，健康状态标记 | §3.1 |
| S8 | 北向 API | `POST /v1/missions`、查询、取消；`GET /v1/missions/{id}/events`（SSE） | §9 |
| S9 | 执行面回调 | leases accept、progress、artifacts 登记、complete/fail（fencing + 幂等校验） | §9 |
| S10 | 通用 HTTP Adapter 参考实现 | `adapters/http-echo`：按协议拉活/收推、回显结果，用于端到端自测 | §6.3 |
| S11 | 最小 Console | 静态页：Mission DAG 状态 + 事件时间线（SSE 驱动），只读 | §11 |

## 2. 明确不做（Out of Scope, M1）

- 主子模式动态委托 / Team 模式（§6.4，M2+）
- 触发体系（定时/事件/条件唤醒、主动触发准入，§7，M3）
- Verifier / 信誉 / 计量（§18–§21，M2 后）
- 审批门与人回路（M2）
- Temporal 替换（ADR-1：自研 + Execution Engine SPI 抽象在 S5 落地）

## 3. 技术约束（红线）

- **ADR-8**：全代码库禁止 `time.Now()` / `math/rand` 裸调用——`internal/clock` 提供注入时钟与种子化随机源（S1 一并交付）；
- 状态迁移只经事件 + 条件更新，禁止绕过 store 层直接 UPDATE；
- 无外部 Go 依赖起步（stdlib only）；首个依赖引入需在 PR 说明理由。

## 4. 测试方案

| 层 | 内容 | 工具 |
|----|------|------|
| 单元 | 状态机全迁移矩阵（合法/非法/取消级联）、fencing 单调性、幂等去重、调度 Filter/Score | `go test`，纯内存 |
| 存储 | schema 迁移幂等、乐观锁冲突、SKIP LOCKED 并发出队不重复 | dockertest / 本地 PG（`TROOP_TEST_PG` 环境变量启用，CI 用 compose） |
| 端到端 | 两个 http-echo Agent 接力完成 Mission：断言最终状态、事件序列、无僵尸租约 | `go test -tags e2e` |

**验收标准（DoD）**：
1. `go test ./...` 全绿（含 e2e tag）；
2. 演示脚本：提交 3 节点链式 Mission → 两 Agent 接力 → Console/SSE 可见全程状态迁移；
3. README 快速开始步骤可复现。

## 5. 排期与顺序

S1 → S2 → S3/S4 → S5 → S6/S7 → S8/S9 → S10 → S11 → e2e 收口。
每个切片一个 commit，测试随切片交付。
