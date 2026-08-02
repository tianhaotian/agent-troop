# 多智能体协同平台（Agent Troop）系统功能与方案设计

> 版本：v0.1 草案　日期：2026-08-02
> 目标读者：架构 / 平台工程 / 多智能体系统研发

---

## 1. 背景与目标

### 1.1 问题陈述

当前 Agent 生态高度碎片化：OpenClaw、Hermes、Claude Code、LangGraph、CrewAI、AutoGen、Dify、Coze 等平台各自为政，Agent 的能力描述、任务状态、上下文、产物格式互不兼容。当一项复杂工作（如"市场调研 → 数据分析 → 报告撰写 → 合规审查 → 发布"）需要多个不同平台 / 不同部署位置的 Agent 接力完成时，缺乏一个**中立的协调层**来承担：

- 任务的统一建模、拆解与编排；
- 跨平台 Agent 的发现、调度与生命周期管理；
- 全局状态与元数据的中心化管理（谁在做、做到哪、依赖谁、产出了什么）；
- 人在回路中的观察、干预与决策（Watcher / Decision Maker）；
- 失败恢复、审计与可观测性。

### 1.2 设计目标

| # | 目标 | 说明 |
|---|------|------|
| G1 | **异构接入** | 不同 Agent 平台（OpenClaw、Hermes 等）以及同一平台内的多智能体，均可通过统一协议接入；Agent 不必本地化，可为远端部署的服务 |
| G2 | **状态中心化** | 任务状态、Agent 注册信息、协作元数据、事件日志集中于控制平面存储，作为唯一事实源（Single Source of Truth） |
| G3 | **调度可插拔** | 调度策略与机制可配置、可扩展（能力匹配 / 成本 / 时延 / 负载 / 优先级 / 抢占） |
| G4 | **任务协作与拆解** | 支持任务 DAG 拆解、子任务委托（delegation）、并行/汇聚、结果合并与冲突消解 |
| G5 | **人在回路** | 人可作为 Watcher（订阅观察、只读）或 Decision Maker（审批、仲裁、接管）参与 |
| G6 | **可靠与可审计** | 任务至少一次执行、断点续跑、幂等、全链路事件溯源与审计 |

### 1.3 非目标（Out of Scope）

- 不实现 Agent 自身的推理 / 规划能力（那是各 Agent 平台的事）；
- 不做模型推理网关（可对接现有 LLM Gateway，但不是本平台核心）；
- 不替代各平台内部的单 Agent 运行时，只做**平台间 / 智能体间**的协调。

---

## 2. 总体架构

### 2.1 分层视图

```
┌────────────────────────────────────────────────────────────────────┐
│                        接入层（Experience）                          │
│   Web Console │ CLI │ Open API/SDK │ Webhook │ IM Bot（Slack/飞书） │
└───────────────┬────────────────────────────────────────────────────┘
                │
┌───────────────▼────────────────────────────────────────────────────┐
│                     控制平面（Control Plane）★中心化★                 │
│                                                                    │
│  ┌─────────────┐ ┌──────────────┐ ┌───────────────┐ ┌───────────┐ │
│  │ Task Service│ │ Orchestrator │ │   Scheduler   │ │ HumanLoop │ │
│  │ (任务建模/   │ │ (DAG执行引擎, │ │ (策略引擎+放置) │ │ Service   │ │
│  │  拆解/合并)  │ │  状态机驱动)  │ │               │ │ (审批/通知)│ │
│  └─────────────┘ └──────────────┘ └───────────────┘ └───────────┘ │
│  ┌─────────────┐ ┌──────────────┐ ┌───────────────┐ ┌───────────┐ │
│  │Agent Registry│ │Context/Artifact│ │ Policy & Auth │ │ Audit &   │ │
│  │ (注册/发现/   │ │ Store (上下文, │ │ (RBAC/配额/    │ │ Telemetry │ │
│  │  健康/能力)  │ │  产物/黑板)   │ │  数据边界)    │ │ (事件溯源) │ │
│  └─────────────┘ └──────────────┘ └───────────────┘ └───────────┘ │
│                ┌──────────────────────────┐                        │
│                │  State Store (元数据 DB)  │  + Event Bus + Queue   │
│                └──────────────────────────┘                        │
└───────────────┬────────────────────────────────────────────────────┘
                │  统一 Agent 接入协议（Control Channel + Data Channel）
┌───────────────▼────────────────────────────────────────────────────┐
│                  数据平面（Data Plane / Agent 侧）                    │
│                                                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ Adapter:     │  │ Adapter:     │  │ Adapter:     │   …          │
│  │ OpenClaw     │  │ Hermes       │  │ 通用 HTTP/   │              │
│  │ (远端部署)    │  │ (远端部署)    │  │ A2A / MCP    │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│  ┌──────────────┐  ┌──────────────┐                                │
│  │ Sidecar/Agent │  │ Local Runner │  （Agent 不必本地，仅要求      │
│  │ (嵌入Agent进程)│  │ (本地Agent托管)│   能被 Adapter 触达）        │
│  └──────────────┘  └──────────────┘                                │
└────────────────────────────────────────────────────────────────────┘
```

**核心原则：控制平面中心化、数据平面去中心化。** 所有状态与元数据写入控制平面；Agent 可以部署在任意位置（云端、IDC、边缘、用户笔记本），只要能与控制平面建立控制通道即可。

### 2.2 关键组件职责

| 组件 | 职责 |
|------|------|
| **Task Service** | 接收任务请求，任务建模（DSL/API），调用拆解器生成子任务 DAG，维护任务生命周期状态机 |
| **Orchestrator** | DAG 执行引擎：解析依赖、驱动状态迁移、处理回调、失败重试/补偿、结果汇聚与冲突消解 |
| **Scheduler** | 将"就绪子任务"匹配到具体 Agent：策略引擎（规则 + 打分）+ 放置决策 + 租约发放 |
| **Trigger Service** | 统一触发入口：API / 定时 / 事件 / 条件 / 智能体主动触发的归一化与准入控制；评估唤醒条件，将 WAITING 任务激活为 READY（见 §7） |
| **Agent Registry** | Agent 注册、能力画像（Capability Profile）、健康检查、心跳、版本管理 |
| **Context & Artifact Store** | 共享上下文（黑板）、任务产物（文件/结构化数据）的版本化存储与引用 |
| **Human-in-Loop Service** | 订阅/通知、审批工单、决策点路由、人工接管会话、超时升级策略 |
| **Policy & Auth** | 身份（人/Agent/服务）、RBAC、数据边界（哪些上下文可给哪些 Agent）、配额 |
| **Audit & Telemetry** | 事件溯源（Event Sourcing）、指标、分布式追踪、回放 |

---

## 3. 统一抽象模型

### 3.1 Agent 抽象（跨平台抹平差异）

每个接入的 Agent（无论来自 OpenClaw、Hermes 还是自研）在平台内被抽象为：

```yaml
Agent:
  agent_id: "agt_01H..."            # 平台内唯一 ID
  name: "market-researcher"
  platform: "openclaw"               # openclaw | hermes | claude-code | langgraph | custom
  endpoint:
    type: remote                     # remote | local-managed | embedded
    uri: "https://openclaw.internal/api/v1/agents/mr-7"
    auth_ref: "secret://vault/openclaw-token"
  capabilities:                      # 能力画像，调度的依据
    - { skill: "web.research",       level: 0.9 }
    - { skill: "summarize.zh",       level: 0.8 }
  io_contract:                       # 输入输出契约（JSON Schema 引用）
    input_schema:  "schema://research-task/v2"
    output_schema: "schema://research-report/v1"
  constraints:
    max_concurrency: 4
    cost_per_1k_tokens: 0.012
    avg_latency_p50_ms: 8000
    data_boundary: ["public", "internal"]   # 可接触的数据密级
  health: { status: healthy, last_heartbeat: "..." }
```

要点：
- **能力画像**既用于调度匹配，也用于任务拆解时"该拆给谁"；
- **IO 契约**保证跨平台协作时上下游能互相理解——这是异构协作的关键，平台提供 Schema Registry 与自动适配（见 §6.3）；
- Agent 状态（空闲/忙碌/熔断）由控制平面中心化维护，Agent 自身无需感知其他 Agent。

### 3.2 任务模型（Mission → Task → Subtask）

三层模型：

```
Mission（使命）: 人/系统提交的顶层目标，含约束（预算、期限、密级、人工介入策略）
  └── Task DAG: 拆解后的有向无环图，节点为 Subtask，边为依赖
        └── Subtask（子任务）: 可分派给单个 Agent 执行的最小单元，带 IO 契约与验收标准
```

```yaml
Subtask:
  subtask_id: "sub_..."
  mission_id: "msn_..."
  kind: agent | human_approval | human_decision | aggregation | condition
  spec:
    required_skills: ["web.research"]
    input:  { ... }                  # 或引用上游产物 artifact://...
    output_schema: "schema://..."
    acceptance:                      # 验收标准（可被 Verifier 检查）
      - type: schema_valid
      - type: llm_judge, rubric: "覆盖至少5个竞品"
  scheduling:
    priority: 50
    deadline: "2026-08-03T10:00:00Z"
    budget_tokens: 200000
    affinity: { platform: ["openclaw", "hermes"] }
    exclusive: false                 # 是否要求独占/互斥执行
  retry: { max_attempts: 3, backoff: exponential, on_failure: retry|replan|escalate }
  state: PENDING
```

**Subtask 状态机**（中心化状态存储的核心实体之一）：

```
PENDING → READY → OFFERED → LEASED → RUNNING → SUCCEEDED
                    │          │         │
                    │          │         ├→ FAILED → (retry) → READY
                    │          │         ├→ BLOCKED(等待人决策) → APPROVED → RUNNING / REJECTED → FAILED
                    │          │         └→ SUSPECT(心跳丢失) → 回收重调度
                    │          └→ EXPIRED(租约超时未确认) → READY
                    └→ WITHDRAWN(无人认领/超时) → 策略重评估
挂起路径：READY/RUNNING → WAITING(挂起：等待定时/事件/条件/外部信号)
          WAITING → READY(Trigger Service 唤醒，见 §7.3)
阻塞路径：RUNNING → BLOCKED(等待人决策) → APPROVED → RUNNING / REJECTED → FAILED
取消路径：任意状态 → CANCELLED（级联取消下游）
```

### 3.3 协作元数据（Metadata）

中心存储的不只是状态，还有协作语义：

- **Handoff（交接）**：上游产物 → 下游输入的映射记录（哪个 artifact、做了何种转换）；
- **Claim / Lease（租约）**：Scheduler 将子任务租给某 Agent，带 TTL，防止脑裂与双执行；
- **Decision Record（决策记录）**：人或 Agent 做出的关键决策（审批意见、方案选择、理由）；
- **Message Thread（消息线索）**：Agent 间协商对话的持久化记录（见 §6.2）；
- **Reputation（信誉）**：每个 Agent 在每种 skill 上的历史成功率/质量分/时延，反哺调度。

---

## 4. 中心化状态与元数据存储

### 4.1 存储选型

| 数据类别 | 存储 | 理由 |
|---------|------|------|
| 实体状态（Mission/Subtask/Agent/Lease） | PostgreSQL（或兼容分布式 SQL） | 强一致、事务、复杂查询；状态迁移走条件更新（乐观锁 `version` 字段） |
| 事件日志（Event Sourcing） | 追加式日志（Kafka / Redpanda）+ 物化读模型 | 审计、回放、状态重建；事件是唯一事实源，表是投影 |
| 共享上下文 / 黑板 | Redis（热）+ 对象存储（冷） | 高频读写的中间上下文走 Redis，大产物走 S3 并仅存引用 |
| 产物 Artifact | S3 / MinIO（内容寻址，SHA256 为 key） | 不可变、去重、可签名 URL 分发 |
| 队列（就绪任务） | Postgres SKIP LOCKED 或 Redis Stream | 规模小时用 PG 即可，避免过度设计 |
| Schema Registry | PG 表 + 缓存 | IO 契约版本化 |

### 4.2 核心表结构（简化）

```sql
missions(id, owner, goal, constraints_json, status, version, created_at, ...)
subtasks(id, mission_id, parent_id, kind, spec_json, scheduling_json,
         state, assignee_agent_id, lease_id, attempt, result_ref, version, ...)
agents(id, platform, endpoint_json, capabilities_json, constraints_json,
       health_json, reputation_json, ...)
leases(id, subtask_id, agent_id, expires_at, fencing_token, ...)
artifacts(id, sha256, uri, schema_ref, produced_by, mission_id, meta_json, ...)
events(seq, aggregate_id, type, payload_json, actor, ts, trace_id, ...)
decisions(id, mission_id, subtask_id, decider_type, decider_id,
          question, options_json, choice, rationale, ts, ...)
```

### 4.3 一致性设计要点

1. **租约 + Fencing Token**：Scheduler 发放租约时附带单调递增 token；Agent 回写结果时必须携带，控制平面校验 token 为最新才接受写入——防止租约过期后旧 Agent 的"僵尸写入"。
2. **状态迁移全部走事件**：`SubtaskLeased` / `SubtaskStarted` / `ArtifactProduced` / `SubtaskSucceeded` 等事件先落日志，再投影到状态表；任何状态都可由事件重放重建。
3. **幂等键**：Agent 上报结果携带 `(subtask_id, attempt, idempotency_key)`，重复上报去重。
4. **Outbox 模式**：状态表更新与事件发布同事务，避免双写不一致。

---

## 5. 调度策略与机制

调度分为两级：**任务级（DAG 内何时就绪）** 与 **放置级（派给哪个 Agent）**。

### 5.1 调度循环

```
Orchestrator 推进 DAG → 依赖满足的 Subtask 置 READY，入就绪队列
   ↓
Scheduler 拉取 READY 任务（按优先级/截止时间排序）
   ↓
候选过滤（Filter）：能力匹配 ∧ 数据边界 ∧ 平台亲和 ∧ 健康 ∧ 并发余量
   ↓
打分排序（Score）：多目标加权打分（见 5.2）
   ↓
发放租约（Lease）→ 推送任务给 Agent Adapter → 等待确认/心跳
   ↓
租约过期 / 心跳丢失 / 失败 → 回收，按 retry 策略重调度 / 重规划 / 升级给人
```

### 5.2 打分模型（可插拔）

```
score(agent, subtask) =
    w1 * skill_match(agent, subtask.required_skills)      # 能力契合（含历史质量分）
  + w2 * reputation(agent, skill)                          # 历史成功率/评分
  + w3 * (1 - normalized_cost(agent))                      # 成本
  + w4 * (1 - normalized_eta(agent))                       # 预计完成时间（负载+历史时延）
  + w5 * locality_bonus(agent, subtask.input_artifacts)    # 数据局部性（产物距 Agent 近）
  - penalty(cold_start, quota_pressure, ...)
```

权重 `w1..w5` 按 Mission 的偏好配置（省钱模式 / 赶时间模式 / 质量优先模式），策略以插件形式注册（Rego / CEL / 自定义 WASM 均可）。

### 5.3 内置调度策略

| 策略 | 说明 | 适用场景 |
|------|------|---------|
| **Capability-First** | 严格按能力画像与契约匹配，默认策略 | 通用 |
| **Cost-Aware** | 满足质量下限前提下选最便宜 Agent | 批量、预算敏感任务 |
| **Deadline-Aware (EDF)** | 按截止时间最早优先，倒排关键路径预留缓冲 | 有 SLA 的 Mission |
| **Load-Balancing** | 均匀分散到同能力 Agent，避免热点 | 同构 Agent 池 |
| **Sticky / Affinity** | 同一 Mission 的关联子任务倾向同一 Agent（上下文复用、缓存命中） | 强上下文连续的任务链 |
| **Gang Scheduling** | 需要多 Agent 同时协作的子任务组要么全部就位要么不启动 | 辩论/红蓝对抗/会诊类 |
| **Auction/Bidding（可选）** | 就绪任务向候选 Agent 广播"招标"，Agent 按自身负载/成本报价，价优者得 | 开放市场式多租户 |
| **Preemption** | 高优先级 Mission 可抢占低优先级任务的租约（Agent 收到取消+补偿上下文） | 紧急插单 |

### 5.4 可靠性机制

- **重试**：指数退避 + 抖动；同一 Subtask 可切换不同 Agent 重试（避免同一平台故障反复踩坑）。
- **熔断**：某 Agent/平台连续失败 → Registry 标记熔断，Scheduler 暂时过滤。
- **检查点**：长任务要求 Agent 周期性上报 progress + 中间产物，崩溃后新 Agent 可从最近检查点续跑（`resume_from: artifact://...`）。
- **超时与升级**：每级超时可配置动作链：`retry → replan（让拆解器换方案）→ escalate（升级给 Decision Maker）`。
- **死信**：最终失败的子任务进 DLQ，人可在 Console 中手动改派/跳过/终止整个 Mission。

### 5.5 模式感知的调度建议

不同协作拓扑（Workflow / 主子 / Team，见 §6.4）对调度器的要求不同，建议内置以下横切规则：

1. **优先级 / 预算 / 期限沿委托链继承**：master 委派的子任务默认继承 Mission 的 priority，预算从 Mission 预算池统一扣减——防止委托链上的**优先级反转**（低优先级子任务阻塞高优先级主线）。
2. **期限倒排分解（Deadline Propagation）**：Mission 的 deadline 沿关键路径倒排，按 slack 分配到每个子任务的 `scheduling.deadline`；某节点延误吃掉 slack 超过阈值时，自动提升其下游所有节点的调度优先级。
3. **Workflow 模式**：Sticky 亲和 + 关键路径优先（关键路径上的就绪任务插队）。
4. **主子模式**：委派任务的放置决策**仍由 Scheduler 做出**——master 只表达"需求与偏好"（required_skills、排除/偏好名单），不直接点名 Agent。master 掌握语义，但看不到全局负载与成本，直接点名会造成局部最优和热点。
5. **Team 模式**：team 级子队列 + 数据局部性权重上调 + Gang 调度；standing team（常驻团队）的空闲容量允许被全局 work-stealing 回收，避免资源闲置。
6. **抢占级联**：高优 Mission 抢占某 Agent 时，被抢占子任务的 Mission 自动获得一次"补偿性优先重调度"，减少抢占的尾延迟放大。

---

## 6. 任务协作与拆解

### 6.1 任务拆解（Decomposition）

三种模式，可组合：

1. **预定义模板（Workflow Template）**：领域专家预编 DAG 模板，参数化实例化。确定性最强，适合标准化流程（如"内容发布流水线"）。
2. **Planner Agent 动态拆解**：把一个 Meta-Subtask（kind=agent）派给"规划型 Agent"，输入目标 + 当前可用 Agent 能力清单（从 Registry 取），输出子任务 DAG 草案 → 平台校验（DAG 合法性、契约连通性、权限）→ 落库执行。
3. **人在回路拆解**：Planner 产出草案后，先给 Decision Maker 审批/编辑（拖拽 DAG 编辑器），批准后执行。

**运行时重规划（Replan）**：执行中子任务失败或产出不符合验收时，Orchestrator 可触发局部重规划——只替换失败子树，已完成的兄弟分支与产物保留复用（以 artifact 引用保证）。

**拆解输出契约校验**：
- 每个节点的 `input` 必须能由上游 `output_schema` 满足（Schema 兼容性检查，含版本）；
- 无环校验、可达性校验、预算/期限的可行性校验（关键路径 ETA ≤ deadline）。

### 6.2 协作机制

| 机制 | 描述 |
|------|------|
| **黑板（Blackboard）** | Mission 级共享上下文区（分命名空间与密级），Agent 可读订阅范围内的上下文与产物索引，写需鉴权。适合松耦合信息共享 |
| **结构化 Handoff** | 上下游通过 Artifact + Schema 传递结果，平台负责引用解析与格式适配。默认协作方式 |
| **消息通道（Agent-to-Agent）** | 需要协商时（如两个 Agent 对结论有分歧），平台建立持久化 Message Thread（兼容 Google A2A 协议语义），消息落中心存储，人可围观 |
| **会诊 / 投票（Ensemble）** | 同一子任务并行派给 N 个 Agent，Aggregator 节点按规则合并：多数投票 / LLM 裁判 / 置信度加权 / 人工裁决 |
| **委托（Delegation）** | Agent 执行中可申请"再拆解"——向平台提交子任务请求，经配额与深度限制校验后挂入 DAG（防止无限递归：max_depth、token 预算硬顶） |
| **冲突消解** | 多 Agent 写同一黑板键/产出冲突结论 → 按策略：版本仲裁、指定 Verifier Agent 裁决、或升级人决策 |

### 6.3 跨平台协议适配

- **Adapter 层**：每个平台一个适配器，负责协议翻译（OpenClaw API / Hermes API / A2A / MCP / 通用 Webhook）与语义映射（平台的 task/run 概念 ↔ 平台 Subtask/Lease）。
- **出站**（控制平面 → Agent）：优先长连接（gRPC stream / WebSocket）推送；不支持主动连接的远端平台用轮询或 Webhook 回调。
- **入站**（Agent → 控制平面）：统一 REST/gRPC 上报接口（心跳、进度、产物引用、结果、日志）。
- **Schema 适配器**：上下游契约不一致时，可插入一个轻量"翻译子任务"（小模型做格式转换），在 DAG 中以普通节点存在，可见可审计。
- **建议以 [A2A 协议](https://google.github.io/A2A/) 作为跨平台互操作的基准协议，MCP 作为工具/资源接入协议**，自研 Adapter 向两者对齐，避免发明私有协议造成的二次锁定。

### 6.4 协作拓扑模式：Workflow / 主子 / Team

**核心设计决策：不为每种模式建独立引擎。** 所有模式都编译到同一个执行底座（Subtask DAG + 租约 + Fencing + Artifact 引用 + 事件溯源），模式的差异只体现在三个维度：

| 维度 | 含义 |
|------|------|
| DAG 书写权 | 模板预定义 / Planner 一次性规划 / Lead Agent 运行时动态追加 / 团队成员认领 |
| 路由决策权 | 平台 Scheduler 全权 / Lead Agent 给需求、Scheduler 定人选 / Team 内部自治 |
| 上下文作用域 | 全局黑板 / 委托链继承视图 / Team 持久命名空间（分场景细化见 §16） |

这样审计、失败恢复、调度优化、人工介入在所有模式下行为一致。

#### 6.4.1 Workflow 模式（平台编排）

- DAG 由模板或 Planner 事先完整定义，Orchestrator 驱动，**确定性最强**；
- 适合标准化流水线（发布、审批、ETL 类）；人以内嵌 gate 节点参与；
- 失败语义简单：失败节点重试/重规划，不影响图结构（除非显式 replan）。

#### 6.4.2 主子模式（Master–Sub，分层委托）

- Mission 派给一个 **Lead Agent**，它持有一个 `kind=coordinator` 的长租约，运行时可动态向 DAG 追加子任务（委托），并消费子任务结果继续规划；
- **两种委派变体**：
  - **平台中介委派（推荐，默认）**：Lead 提交"子任务需求 + 偏好/排除名单"，Scheduler 决定人选。Lead 无全局负载视图，直接点名会造成热点与局部最优；
  - **指定委派**：Lead 直接指定 `assignee_agent_id`，平台只做校验（权限、密级、配额）不优化。仅用于强语义绑定场景（如"必须由撰写该草稿的同一个 Agent 修订"）；
- **Lead 容错**：Lead 须周期心跳并提交"计划快照"（当前 DAG 意图 + 中间结论落黑板）。Lead 失联时按策略：① 另选 Agent 以最近快照继任（checkpoint takeover）；② 平台冻结该子树并升级人决策；
- **护栏**：`max_depth`（委托深度）、`max_fanout`（单节点扇出）、预算池扣减、子任务权限不超过父任务的权限包络（permission attenuation，权限只能收窄不能放大）。（委托协议的完整消息时序与容错见 §15）

#### 6.4.3 Team 模式（对等团队）

- 一组有角色的 Agent 组成具名 Team，拥有**持久黑板命名空间**和团队收件箱；
- 两种生命周期：**临时团队**（随 Mission 组建，Mission 结束解散）与**常驻团队**（standing team，长期存在，如"运维值班组""内容工作室"，承接源源不断的任务）；
- 两种内部路由：
  - **认领制（Claim-based）**：任务张贴到团队任务板，成员按自身能力与负载主动认领（带租约抢占保护，防重复认领）——自组织、容错性好；
  - **协调员制**：Team 内设 coordinator（Agent 或规则），统一分发——可控性强；
- 团队内部消息线程为一等公民；团队对平台外部表现为一个"复合 Agent"（有自己的能力画像 = 成员能力的并集 + 协作加成系数），因此 Team 可以嵌套参与更高层协作；
- 与主子模式的区别：主子是**权力层级**（Lead 有规划与验收权），Team 是**对等协作 + 共享记忆**，没有天然的上级。

#### 6.4.4 Ensemble 会诊 / Swarm 市场

- Ensemble 见 §6.2：同任务 N 路并行 + 聚合节点，调度上用 Gang；
- Swarm（开放竞价、无中心规划）只建议用于探索性任务，且必须套预算硬顶与最大步数，产物必须经 Verifier 才允许进入下游——确定性最低，不作为默认模式。

#### 6.4.5 模式选择与混合

```yaml
orchestration:
  mode: workflow            # workflow | master_sub | team | ensemble
  master_sub: { lead: "agent:planner-pro", max_depth: 3, max_fanout: 8 }
```

**推荐混合用法**：Workflow 骨架 + 局部主子节点。即顶层用模板保证主干确定性与关键审批节点位置固定，骨架中某些 `kind=agent, can_delegate=true` 的节点允许 Lead Agent 在局部动态展开子 DAG——兼顾可控与灵活，这是实践中成功率最高的形态。

---

## 7. 触发体系与主动唤醒

任务不应只有"人提交 API"一个入口。平台将所有触发源归一为 **TaskIntent（任务意图）**，经统一准入管道处理后实例化为 Mission/Subtask 或唤醒挂起任务。

### 7.1 触发源全景

| # | 触发源 | 说明 | 示例 |
|---|--------|------|------|
| T1 | **API 触发** | 人/外部系统显式提交 | `POST /v1/missions`、CI 流水线回调 |
| T2 | **定时触发** | cron 计划 / 一次性定时器 | "每周一 9 点生成周报" |
| T3 | **事件触发** | 订阅事件流/外部 Webhook，按规则匹配 | "新 issue 打上 `bug` 标签 → 启动分诊 Mission"、"artifact X 产出 → 启动下游" |
| T4 | **依赖触发** | DAG 内部就绪传播（平台内部机制，非外部入口） | 上游 SUCCEEDED → 下游 READY |
| T5 | **智能体主动触发** | Agent 运行中发起：委派子任务、创建新 Mission、唤醒挂起任务、注册自我唤醒（见 7.3/7.4） | 研究 Agent 发现新线索，追加一个深挖子任务 |
| T6 | **人工触发** | IM 对话、Console 操作、决策完成隐式唤醒 BLOCKED 任务 | 审批通过 → 发布子任务 READY |

### 7.2 统一准入管道（Admission Pipeline）

无论触发源是人是机，一律走同一条管道，保证治理无旁路：

```
Trigger Source → TaskIntent{what, why(triggered_by), payload, idempotency_key}
   → ① 认证鉴权（谁有权触发什么：人→SSO/RBAC；Agent→能力授权 scope）
   → ② 去重（idempotency_key + 去重窗口，防 webhook 重投/Agent 重试造成重复 Mission）
   → ③ 配额与预算校验（预算池归属、并发上限）
   → ④ 循环检测（触发因果图上成环或扇出风暴 → 熔断，见 7.4）
   → ⑤ 优先级赋值与限流
   → ⑥ 实例化（新建 Mission / 挂入既有 DAG / 唤醒 WAITING）→ 事件落日志
```

### 7.3 WAITING 状态与唤醒语义

长任务 Agent 不应空占计算资源等待外部条件，平台支持**挂起-唤醒（Continuation）**模式：

- Agent 执行中可不返回 complete，而返回 `suspend + wake_on`，平台将子任务置 **WAITING**，释放租约与计算资源，Agent 的上下文以检查点形式持久化；
- `wake_on` 支持四类条件，可组合：
  - `timer`: 时间点/相对时长（"2 小时后再看实验结果"）；
  - `event`: 事件模式匹配（"当 artifact:experiment-42 产出时"）；
  - `condition`: 对黑板/上下文的表达式（"当 blackboard.approval_count >= 2"）；
  - `manual`: 仅允许显式唤醒（人或授权 Agent 调用 `:wake`）；
- Trigger Service 由时间轮 + 事件流匹配器 + 条件求值器三部分实现；条件满足 → 子任务回 READY → Scheduler 重新调度（**可换 Agent 续跑**，凭检查点恢复，不要求原 Agent 在线）→ 全程事件留痕。（表达式语言与匹配语义细化见 §14）

### 7.4 智能体主动触发的治理（自治的安全边界）

Agent 主动触发是能力也是最大风险源，默认收紧、按授权放开：

1. **能力分级授权**：`trigger.spawn_subtask`（Mission 内委派）/ `trigger.create_mission`（开新使命）/ `trigger.wake`（唤醒他人任务）三个独立 scope，按 Agent 逐个授予，默认仅开第一个；
2. **预算归属不可洗白**：Agent 触发的一切工作默认计入其来源 Mission 的预算池（或显式做预算划转并留痕），杜绝通过开新 Mission 绕过预算顶；
3. **权限包络衰减**：被触发任务的权限/数据密级不得超过触发者自身的包络；
4. **循环与风暴防护**：维护全局触发因果图（triggered_by 链）；检测到 A→B→A 环、单 Agent 触发速率超阈值、或扇出系数超顶 → 熔断该 Agent 的触发权限并升级人决策；
5. **自治度门控**：Mission 自治级 L0 下 Agent 开新 Mission 自由；L1 下超成本/风险阈值的新 Mission 先落 `PENDING_APPROVAL` 由人批准；L2 下一切主动触发均需人确认；
6. **全链路因果审计**：每个 Mission/Subtask 记录 `triggered_by`（事件/决策/Agent/人），可从任一产物反查完整触发链。

### 7.5 触发通道（出站）

- Agent 侧：经 Adapter 推送（长连接优先，Webhook 兜底）；
- 外部系统：出站 Webhook（Mission 状态变化通知业务系统，支持签名与重投）；
- 人：IM / 邮件 / Console 待办（见 §8）。

---

## 8. 人在回路（Human-in-the-Loop）

### 8.1 两种角色

**Watcher（观察者）**：
- 按 Mission / Agent / 标签订阅事件流（SSE / WebSocket / IM 推送）；
- 只读视图：DAG 实时进度图、Agent 对话线程、产物预览、成本与时延仪表；
- 可随时"举手"升级为介入（watcher → intervenor）。

**Decision Maker（决策者）**：
- **审批门（Approval Gate）**：DAG 中的 `human_approval` 节点——如"发布前合规确认"。携带上下文摘要 + 产物 + 建议选项，推送到人的待办；
- **决策点（Decision Point）**：Agent 主动发起 `request_decision(question, options, context, deadline)`——如"两个方案选哪个"；
- **仲裁**：冲突消解升级、Verifier 判定存疑时路由给人；
- **接管（Takeover）**：人直接进入某 Agent 的会话接管输入，或手动修改子任务参数后重跑。

### 8.2 决策路由与超时策略

```yaml
human_policy:
  route:                        # 决策路由：按技能/值班表/负载
    approvers: ["role:compliance-lead", "user:alice"]
    quorum: 1                   # 需要几人批准
  timeout:
    after: "2h"
    on_timeout: auto_approve | auto_reject | escalate_to: ["user:bob"] | pause_mission
  notification: ["slack:#agent-ops", "email"]
```

- 所有人工决策写入 `decisions` 表（谁、何时、选了什么、理由）——既为审计，也可后续蒸馏成自动策略（如"这类审批历史上 98% 通过 → 建议改为自动 + 抽样复核"）。
- **自治度分级**：Mission 可声明 autonomy level：`L0 全自动 / L1 关键节点审批 / L2 每步确认`，平台按级别在拆解时自动插入人工节点。

### 8.3 安全前提

- 人看到的上下文经**脱敏过滤**（数据边界策略同样适用于人：不是每个 watcher 都能看全部产物）；
- 决策操作需强认证（SSO + MFA 可选）+ 操作审计；
- Agent 不得伪造/诱导审批：决策请求中 Agent 提交的"建议"需明确标注来源，Verifier 可对建议做偏见/操纵检测。

---

## 9. 接口设计（核心 API 摘要）

```http
# ---- 任务面 ----
POST   /v1/missions                      # 提交使命（goal + constraints + autonomy）
GET    /v1/missions/{id}                 # 状态 + DAG 快照
POST   /v1/missions/{id}:cancel
POST   /v1/subtasks/{id}:retry | :skip | :reassign

# ---- Agent 面 ----
POST   /v1/agents:register               # 注册/更新能力画像
POST   /v1/agents/{id}/heartbeat
POST   /v1/agents/{id}:deregister        # 优雅下线（拒收新租约，排空在途）

# ---- 执行面（Agent 回调） ----
POST   /v1/leases/{id}:accept | :reject
POST   /v1/subtasks/{id}/progress        # 心跳 + 进度 + 检查点
POST   /v1/subtasks/{id}/artifacts       # 产物登记（返回签名上传 URL）
POST   /v1/subtasks/{id}:complete        # 结果 + idempotency_key + fencing_token
POST   /v1/subtasks/{id}:fail            # 结构化错误（可重试性分类）
POST   /v1/subtasks/{id}:request_decision

# ---- 触发面（统一准入，见 §7） ----
POST   /v1/intents                       # Agent/系统提交任务意图（spawn/wake/new-mission）
POST   /v1/triggers                      # 注册触发规则（cron / event 模式 / 条件表达式）
DELETE /v1/triggers/{id}
POST   /v1/subtasks/{id}:suspend         # Agent 挂起自身：{ wake_on: {timer|event|condition|manual} }
POST   /v1/subtasks/{id}:wake            # 授权唤醒 WAITING 任务（人或 Agent，按 scope 鉴权）

# ---- 人工面 ----
GET    /v1/inbox?assignee=me             # 我的待办（审批/决策）
POST   /v1/decisions/{id}:resolve        # 提交决策
GET    /v1/missions/{id}/events          # SSE 订阅（Watcher）

# ---- 元数据 ----
GET    /v1/schemas/{ref}                 # Schema Registry
GET    /v1/artifacts/{id}                # 产物元数据 + 签名下载 URL
```

Agent 侧 SDK 提供最小接入封装：`receive(task) → your_agent.run() → report(result)`，隐藏租约/心跳/幂等细节。对 OpenClaw / Hermes 这类已有运行时的平台，提供**托管 Adapter**：平台侧部署一个连接器进程，把 Subtask 翻译成该平台原生的 run/job 调用并回传状态。

---

## 10. 安全、权限与数据治理

1. **身份三元组**：人（SSO/OIDC）、Agent（mTLS 证书或签名 token，注册时签发）、平台服务（服务账户）。所有事件带 actor。
2. **RBAC + 能力标签**：谁能提交 Mission、谁能审批、Agent 能读哪些黑板命名空间。
3. **数据边界**：Artifact/上下文带密级标签；Scheduler 过滤阶段硬性排除密级不匹配的 Agent（例如涉内数据不派给公网部署的 OpenClaw 实例）；跨边界传输需审批且留痕。
4. **审计**：事件溯源天然形成不可篡改审计链；支持按 Mission 导出完整证据包（DAG、决策、产物哈希、对话记录）。
5. **防滥用**：Mission 级 token/成本预算硬顶、委托深度限制、速率限制、Prompt 注入防护（对入站的上游产物做内容安检后再派给下游 Agent）。

---

## 11. 可观测性

- **追踪**：OpenTelemetry 贯穿 Mission → Subtask → Lease → Agent 内部 span（Adapter 注入 traceparent）。
- **指标**：就绪队列深度、调度延迟、租约过期率、每 Agent 成功率/时延/成本、人工决策平均等待时长、Mission 端到端时长。
- **实时视图**：DAG 甘特图 + 事件时间线 + Agent 间消息流图（Console 三大核心页面）。
- **回放**：给定 Mission ID，从事件日志重放完整执行过程，用于调试与复盘。

---

## 12. 部署形态与演进路线

### 12.1 部署

- **单二进制起步**（控制平面 all-in-one：API + Orchestrator + Scheduler + 内嵌 PG/Redis 依赖外部化可配），Docker Compose 即可跑；
- **规模化拆分**：Scheduler / Orchestrator 无状态水平扩展（基于队列与租约天然支持多副本竞争消费）；事件总线换 Kafka。
- Agent 侧：Adapter 可集中部署（平台托管）或分布式部署（贴近 Agent 就近接入）。

### 12.2 里程碑

| 阶段 | 范围 | 验收 |
|------|------|------|
| **M1（4–6 周）MVP** | 任务模型 + PG 中心存储 + 简单 DAG 编排 + 通用 HTTP Adapter + Capability-First 调度 + 基础 Console（DAG 视图 + SSE 事件流） | 两个真实平台 Agent（如一个 OpenClaw 远端实例 + 一个本地 Agent）接力完成一次 Mission，全程状态可查 |
| **M2（+4 周）协作与人回路** | 审批门/决策点 + Watcher 订阅 + 黑板 + Artifact Store + 重试/租约/Fencing | "生成→审查→人工批准→发布"流水线含 1 个人工节点跑通；故障注入后自动恢复 |
| **M3（+6 周）高级调度与可靠性** | 打分策略插件化、Deadline/Cost 策略、熔断、检查点续跑、Ensemble 会诊、触发体系（定时/事件触发 + WAITING 挂起唤醒 + 准入管道） | 压测：100 并发 Mission 下调度正确性与 SLA；策略 A/B 对比报告；Agent 挂起-唤醒端到端跑通 |
| **M4（持续）生态** | A2A/MCP 原生适配、OpenClaw/Hermes 托管 Adapter、决策数据回流优化策略、Marketplace 式 Agent 发现与竞价 | 第三方平台仅靠文档自助接入 |

### 12.3 关键风险与对策

| 风险 | 对策 |
|------|------|
| 异构契约不齐，协作鸡同鸭讲 | Schema Registry + 强制契约声明 + 翻译子任务兜底 |
| 中心存储成为瓶颈/单点 | 读写分离（事件日志 + 物化视图）、PG 分区表、后续可换分布式 SQL；状态热路径走 Redis |
| 调度脑裂（多副本重复派发） | 租约 + fencing token + 条件更新，任何时刻一个 Subtask 只有一个有效租约 |
| Agent 不可信（谎报结果/注入攻击） | Verifier 抽检、信誉系统、产物内容安检、权限最小化 |
| 人成为瓶颈 | 自治度分级、超时默认动作、决策数据回流逐步自动化 |
| Agent 主动触发失控（循环触发、预算洗白、扇出风暴） | 统一准入管道：触发因果图环检测、预算池强制归属、触发 scope 分级授权、速率限制与熔断（§7.4） |
| Lead Agent 单点（主子模式） | 计划快照落黑板 + 心跳，失联后 checkpoint takeover 或冻结升级（§6.4.2） |

---

## 13. 一个端到端示例

> Mission：「调研 A 股储能板块，产出 3000 字中文研报并发布到内部 Wiki」，autonomy=L1，预算 5M tokens，期限 4 小时。

1. 用户提交 Mission → Planner Agent 拆解为 DAG：`数据采集(并行×3) → 数据清洗 → 分析建模 → 初稿撰写 → 事实核查(Verifier) → 合规审批(人) → 发布`；
2. Scheduler 按能力+成本把采集任务派给两个 OpenClaw 远端 Agent 和一个 Hermes Agent（Gang 就绪后并行启动）；
3. 产物陆续写入 Artifact Store，黑板更新"已完成 2/3 采集"——Watcher 在 Console 实时看到 DAG 变绿；
4. 撰写 Agent 引用上游产物写初稿；Verifier 发现两处数据矛盾 → 发起 Agent 间消息线程协商 → 未收敛，升级为 Decision Maker 裁决，人 10 分钟内裁定采纳来源 B；
5. 合规审批节点推送 Slack，lead 一键批准；发布 Agent 调用 Wiki API 完成发布；
6. 全程事件落日志：耗时 2h41m，花费 1.9M tokens，人工介入 2 次；复盘报告自动生成。

---

## 14. 专题深化 A：Trigger Service 表达式语言与事件模式匹配

### 14.1 规范化事件信封

所有内部状态变化与外部入站信号统一归一为事件信封（写入事件日志后才能参与匹配——**先落日志，后可匹配**，保证可回放）：

```json
{
  "event_id": "evt_01J...",
  "type": "artifact.produced",
  "ts": "2026-08-02T09:31:07.312Z",
  "actor": { "kind": "agent", "id": "agt_01H..." },
  "scope": { "mission_id": "msn_01H...", "team_id": null },
  "refs":  { "subtask_id": "sub_...", "artifact_id": "art_..." },
  "payload": { "schema_ref": "schema://research-report/v1", "sha256": "..." },
  "labels": { "severity": "info", "boundary": "internal" },
  "trace_id": "..."
}
```

**主题分类法**（`domain.entity.action` 层级命名，支持后缀通配订阅）：

| 域 | 主题示例 |
|----|---------|
| 任务 | `mission.created`、`mission.state.changed`、`subtask.ready`、`subtask.succeeded`、`subtask.failed`、`subtask.decision.requested` |
| 产物 | `artifact.produced`、`artifact.approved`、`artifact.rejected` |
| Agent | `agent.registered`、`agent.health.changed`、`agent.quota.exceeded` |
| 黑板 | `blackboard.key.changed`（payload 含 namespace、key、writer、version） |
| 决策 | `decision.resolved`、`decision.expired` |
| 外部 | `external.<source>.<...>`——Webhook/轮询入站后由 Adapter 归一化（如 `external.github.issue.labeled`），与内部事件走同一管道 |

### 14.2 事件模式匹配语义

触发规则定义：

```yaml
trigger:
  name: bug-triage-on-label
  on:
    event_pattern:
      type: "external.github.issue.labeled"
      where: 'payload.label == "bug" && payload.repo.startsWith("agent-troop/")'
      correlation_key: "payload.issue_id"
  debounce: "30s"                    # 同 correlation_key 去抖
  max_fires: "10/h"                  # 触发频率上限
  action:
    create_mission:
      template: "bug-triage"
      input_mapping: { issue_id: "${payload.issue_id}" }
```

语义规则（明确定义，避免各实现漂移）：

1. **单事件模式**：`type` 过滤 + `where` 谓词（表达式语言见 §14.3）。
2. **复合模式（CEP）**：支持 `all(...)`（窗口内全部到达，无序）、`sequence(...)`（严格按序）、`any(...)`、`not(...)`（窗口内缺席——如"产物产出后 30 分钟内无 `artifact.approved` 则催办"）。
3. **窗口语义**：滑动（sliding）/滚动（tumbling）窗口 + **watermark 与 allowed lateness**：迟到超过宽限的事件默认丢弃并计数（指标 `trigger.late_events`），可配置进死信。
4. **关联键**：`correlation_key` 将事件流按键分区维护匹配状态；不同 key 的匹配互不干扰。
5. **恰好一次实例化**：触发动作执行前写 firing 记录 `(trigger_id, correlation_key, window_id)`，唯一约束兜底——事件重复投递（总线至少一次语义）不会产生重复 Mission。
6. **顺序保证**：仅保证同一聚合（同一 mission / 同一 subtask）内事件有序；跨聚合不保证，表达式语义不得依赖跨聚合顺序。
7. **背压隔离**：匹配引擎消费事件流，触发动作写入持久化"触发动作队列"异步执行；动作执行慢不阻塞匹配，匹配积压超阈值告警并自动采样降级。

### 14.3 条件表达式语言（`wake_on.condition` 与 `where`）

**选型：嵌入 CEL（Google Common Expression Language）**。理由：沙箱执行无副作用、强类型、官方支持求值成本上限（cost budgeting）、可静态分析表达式引用的变量——最后一点是增量求值的基础。不选自研 JSON 谓词语法（表达力天花板低），也不选 Lua/WASM（治理面过重）。

**表达式可见的数据模型**（按任务的作用域视图裁剪，越权字段在求值上下文中根本不存在，而非求值后过滤）：

```
mission.*                     # 所属 Mission 字段（goal、constraints、state…）
subtask.*                     # 所属 Subtask（state、attempt、result_ref…）
board.<namespace>.<key>       # 黑板读视图（仅限有读权限的命名空间）
artifacts["<id>"].*           # 产物元数据（不含内容体）
agents["<id>"].health         # Agent 健康摘要
elapsed() / deadline_in()     # 注入的逻辑时钟函数（不暴露裸系统时钟，防时钟回拨作弊）
```

示例——"两位评审批准且终稿通过校验，或超过截止时间"：

```yaml
wake_on:
  any:
    - all:
        - condition: 'board.review.approval_count >= 2'
        - condition: 'artifacts["draft-final"].status == "verified"'
    - condition: 'deadline_in() < duration("0s")'
  ttl: "24h"                    # 唤醒注册本身有寿命，过期升级人处理
```

**安全与成本护栏**：注册时静态校验（类型检查、禁用函数表、引用键提取）；求值设 cost 上限（默认 1e6 CEL cost units），超限视为false并告警；表达式纯函数、无 IO、无系统时钟/随机源。

**求值策略（增量 + 兜底）**：
- 注册时提取引用键集（如 `board.review.approval_count`），建立**倒排索引** `key → [condition_id]`；
- 黑板写入/事件到达时，只重评估受影响条件，避免全量扫描；
- 条件为真 → 以**乐观锁 CAS 状态迁移**（`WAITING→READY`，校验 expected version）保证只唤醒一次，多副本评估器竞争安全；
- 低频全量复核扫描（anti-entropy sweep，默认 60s）兜底索引漏更新或事件丢失。

### 14.4 唤醒语义（精确约定）

- **Level-triggered，edge-fired**：条件首次由假变真时唤醒一次；若任务被唤醒后再次 suspend 且条件仍为真，立即再次唤醒（不等待新的"边沿"），避免死等。
- **恰好一次**：firing 记录 + CAS 状态迁移双重保证；唤醒后原唤醒注册失效（一次性），Agent 若需再次等待须重新 suspend。
- **TTL 与永久悬挂防护**：`wake_on` 必须带 TTL（平台设上限），到期未触发 → 任务置 FAILED( reason=wake_timeout) 并按策略升级。
- **唤醒≠原地复活**：唤醒只让任务重新进入调度，可被放置到任意健康 Agent（凭检查点续跑）；语义上 Agent 不得假设自己仍在原进程/原机器。

---

## 15. 专题深化 B：主子模式委托协议消息时序

参与者：**Lead**（协调者 Agent）、**Platform**（Task Service / Scheduler / Registry 合体视角）、**Sub**（被委派 Agent）。所有消息携带 fencing token 与幂等键；Platform 是唯一的仲裁者与状态持有者，Lead 与 Sub **互不直连**（保证可审计、可换人）。

### 15.1 正常委派时序（Happy Path）

```
Lead                     Platform                          Sub
 │── acquire_coordinator ─►│                                │
 │◄─ lease(L1) + 上下文包 ──│                                │
 │                                （上下文包构造见 §16.2）    │
 │── delegate(spec, prefs, ─►│ 校验: max_depth / max_fanout /
 │    budget_slice, grants)  │ 预算池原子预扣(hold) /        │
 │◄── sub_7 (PENDING) ──────│ 权限包络衰减                  │
 │                           │ Scheduler 放置               │
 │                           │── dispatch(lease S1, ctx) ──►│
 │                           │◄──── accept(S1) ─────────────│
 │── guidance(可选消息) ────►│── 投递到 Sub 收件箱 ────────►│
 │                           │◄──── progress/checkpoint ────│
 │                           │◄──── complete(result, S1) ───│
 │◄── notify(result 就绪) ──│ 结果入 Lead 收件箱            │
 │   (Lead 若 WAITING → 唤醒) │ 预算 hold 结算                │
 │── review: accept ───────►│ sub_7 → SUCCEEDED             │
 │   或 rework(feedback) ──►│ sub_7 → READY (attempt+1,     │
 │                           │   可换 Sub, 上限 max_rework)  │
```

要点：
- **结果不自动注入 Lead 上下文**：先进 Lead 收件箱，Lead 显式 `ingest`（可只取摘要或按需 drill-down）——这是上下文预算控制的关键闸门；
- **rework 闭环**：Lead 验收不通过给结构化反馈，子任务带反馈重派（默认可换 Sub 执行，避免同一失败模式重复）；`max_rework` 到达后升级；
- Lead 发送 `delegate` 后自身可 `suspend`（`wake_on: sub_7 succeeded/failed`），不空占资源——与 §7.3 的 Continuation 无缝组合。

### 15.2 Lead 心跳与计划快照

- Lead 每 `heartbeat_interval`（默认 30s）上报心跳 + **计划快照**：当前 DAG 意图、各子任务状态认知、关键中间结论的 artifact 引用、下一步计划；
- 快照版本化写入黑板 `lead-plan` 命名空间——这是 Lead 可被替换的前提（状态外置）；
- 快照也是 Watcher 观察"Lead 在想什么"的窗口（按角色脱敏）。

### 15.3 Sub 澄清与升级时序

```
Sub ── request_clarification(question, ctx) ──► Platform
      Platform 路由至 Lead 收件箱（Lead 若挂起则唤醒）
      Lead ── answer / 追加 grant（补授 artifact 引用）──► Sub
      Lead 答不了 ── escalate ──► 人（Decision Maker）
      超时（默认可配）──► 视为 Lead 无响应 ──► 走 15.4 失联流程
```

### 15.4 Lead 失联容错时序

```
心跳丢失 → 宽限期(grace, 默认 2×interval) → fence L1（token 失效，后续写入被拒）
  → 冻结子树：in-flight Sub 允许跑到完成（结果入收件箱暂存），新 delegate 拒绝
  → takeover 策略（按 Mission 配置）：
    a) 继任：Scheduler 选新 Lead → 以最近计划快照 + 收件箱 + 产物引用重建上下文
       → 新 Lead 审阅快照后可确认续行 / 局部改派 / 宣布 replan
    b) 无候选 → 升级人决策：指定新 Lead / 终止子树（触发取消传播）/ 平台托管
       （按快照机械等待在途 Sub 完成后停摆待命）
  → 旧 Lead 复活：持已失效 token 的一切写入被拒；可转为只读旁观者申请重新接管
    （需现任 Lead 或人批准，防双主脑裂）
```

### 15.5 预算与递归委托的原子性

- `delegate` 时预算池**原子预扣**（hold），完成结算多退少补，取消/失败释放 hold——预扣与校验在同一个 DB 事务内，防并发委托超支竞态；
- 递归委托（Sub 也有 `can_delegate`）时同样从 Mission 预算池扣减且深度+1 校验，`max_depth` 命中即拒绝并返回结构化错误，由 Lead 决定换方案。

### 15.6 取消传播

Mission 取消 → 按逆拓扑序级联：先停新调度，再向 RUNNING 子任务发 `cancel`（Sub 可返回 partial result 与补偿动作），最后 fence Lead 租约；所有 hold 预算释放。Lead 也可发起**局部取消**（仅取消其委托的子树）。

---

## 16. 专题深化 C：上下文作用域与视图模型

### 16.1 作用域层级（Scope Lattice）

```
global（平台级只读公共知识）
 └─ team（常驻团队持久记忆）
     └─ mission（使命级黑板，Mission 结束归档）
         └─ subtree（委托链子树视图）
             └─ agent-private（Agent 私有暂存，随租约销毁）
```

正交维度：**human-only 标注**（某些决策理由、凭据材料只对人可见，对任何 Agent 不可见）与**密级标签**（§10 数据边界，与作用域取交集生效）。每个作用域声明：读写 ACL、生命周期（归档/销毁时机）、存储配额。

### 16.2 上下文包（Context Package）——核心机制

Agent 被调度时**不是"自己到处去读"**，而是平台在 dispatch 时按作用域策略**物化一份上下文包**随任务下发：

```yaml
context_package:
  task_spec: { ... }                    # 本任务规格与验收标准
  inputs: [ artifact://sha256:abc... ]  # 显式授予的产物引用（签名 URL，限时）
  board_views:                          # 黑板切片授权
    - { ns: "mission.shared", keys: ["glossary", "style-guide"], mode: ro }
    - { ns: "subtask.scratch", mode: rw }
  budget: { tokens_remaining: 180000 }
  wake_conditions: [ ... ]
  decisions_digest: "artifact://..."    # 相关历史决策摘要（引用而非内联）
  snapshot_hash: "sha256:..."           # 物化时刻的上下文指纹，记入事件日志
```

`snapshot_hash` 落审计：事后可精确回答"**这个 Agent 在做出该行为时看到了什么**"——这是调试幻觉、归责与合规的关键能力。

### 16.3 分场景视图策略

| 场景 | 视图策略 | 设计理由 |
|------|---------|---------|
| Workflow 上下游接力 | 下游只拿到上游**声明的产物**（按 schema），默认不给上游的中间推理痕迹/对话 | 最小信息面，减少错误传播与注入面 |
| 主子：Lead 视图 | 子树 **rollup**：各 Sub 的状态 + 结构化摘要 + 产物引用；完整对话需 drill-down 且留痕 | Lead 上下文窗口是稀缺资源；同时保留可追溯性 |
| 主子：Sub 视图 | 仅本任务切片 + Lead 显式 grant 的引用；**看不到兄弟任务** | 防越权感知任务全貌（最小知情） |
| Team 共享记忆 | 持久命名空间分两层：**episodic**（事件流，带作者标签，TTL + 滚动摘要压缩）与 **semantic**（提炼后的知识条目，CAS 版本化写入，冲突走仲裁/人审）；成员平等读写但受密级约束 | 长期协作需要沉淀，但要防黑板无限膨胀与"脏写" |
| Ensemble 会诊 | 成员之间**互不可见**（防锚定效应），仅 aggregator 见全部 | 保证意见独立性 |
| Watcher（人） | 按角色脱敏的实时视图；decision 请求中 Agent 的"建议"必须标注来源 | 见 §8.3 |

### 16.4 上下文预算与分层

- 三级热度：**hot**（当前工作窗口内联内容）→ **warm**（摘要 + 可检索索引）→ **cold**（artifact 引用，按需拉回）；
- 平台内置 `kind=summarize` 与 `kind=retrieve` 两类系统子任务，供编排器/Lead 显式调用做上下文压缩与召回；
- 上下文 token 消耗计入 Mission 预算——上下文膨胀直接体现为成本，倒逼显式治理。

### 16.5 跨 Mission 上下文共享

默认完全隔离。共享必须显式：`publish` 产物到 **Artifact Catalog**（带 schema、密级、所有者、有效期），消费方以引用引入并经密级校验（必要时审批）。禁止"读别人 Mission 的黑板"这类隐式通道。

---

## 17. 后续设计议题清单（按优先级）

| # | 议题 | 关键问题 | 建议时机 |
|---|------|---------|---------|
| 1 | **Verifier 与质量体系** | ✅ 已细化，见 §18（分层管线、judge 防操纵、质量数据模型） | M2 后展开 |
| 2 | **信誉系统与调度反馈闭环** | ✅ 已细化，见 §19（Beta/EWMA 更新、探索流量、金丝雀复测） | 与 §18 同期 |
| 3 | **计量计费与多租户配额** | ✅ 已细化，见 §21（权威计量 vs 上报、三级交叉验证、hold/settle） | M3 |
| 4 | **仿真与回放测试床** | ✅ 已细化，见 §20（四种模式、虚拟时钟、确定性回放、场景 DSL） | M3 压测前置依赖 |
| 5 | **Prompt 注入与内容安全管线** | 入站产物安检、跨密级流动检测、工具调用白名单 | 开放平台接入前（M4 前必须） |
| 6 | **Console 详设** | DAG 编辑器、时间线/甘特、干预操作台、决策收件箱 UX | M2 迭代中细化 |
| 7 | **SDK / Adapter 认证计划** | OpenClaw/Hermes 托管 Adapter 的兼容性测试套件（契约、心跳、断连恢复用例） | M4 |
| 8 | **数据生命周期与合规** | 删除权 vs 事件溯源的矛盾 → crypto-shredding（按主体加密，删钥即删数据）；保留策略 | 上线前 |
| 9 | **多 Region 与容灾** | 控制平面单 Region 强一致起步；跨 Region 只做 Agent 就近接入 + 异步复制 | 规模化后 |
| 10 | **经济机制** | Agent 竞价的市场出清规则、内部记账与反投机 | 开放生态阶段 |

---

## 18. 专题深化 D：Verifier 与质量体系

### 18.1 分层验收管线

验收不是单一判定，而是**由廉到奢的层级管线**，每一层通过才进入下一层（或按风险路由跳层）：

| 层 | 机制 | 成本 | 说明 |
|----|------|------|------|
| L0 | **结构校验**：output_schema 验证、必填字段、引用完整性 | 极低 | 纯确定性，失败即 rework，不消耗 judge 预算 |
| L1 | **规则检查**：约束断言、合规词表、事实核对（与来源 artifact 比对）、数值一致性 | 低 | 确定性规则引擎；规则按 schema_ref 版本化挂载 |
| L2 | **模型裁判**：LLM judge 按 rubric 打分 / 多方案 pairwise 比较 / 参照物对比 | 中 | 详见 18.2 防操纵设计 |
| L3 | **人工抽样复核**：按比例或按风险抽样送 Decision Maker | 高 | 抽样率按 Mission 自治度与历史质量动态调整 |

- **风险路由**：产物按 `risk = f(密级, 下游影响面, Mission 重要度)` 决定验收深度——低风险 L0+L1 即放行，高风险强制到 L2/L3；
- **验收预算上限**：单任务验收成本不超过任务本身成本的固定比例（默认 20%），超限升级人定夺，防"验证比生产还贵"；
- **结构化失败分类**：Verifier 输出必须带机器可读的失败原因分类（`schema_invalid / fact_conflict / incomplete / style / policy_violation / judge_rejected`），这是 Lead 有效 rework 和信誉归因的前提。

### 18.2 Judge 防操纵设计

LLM judge 是整个质量体系的信任根，必须假设它会被博弈：

1. **利益隔离**：Verifier 不得与生产者同 Agent；尽量不同 platform / 不同租户；平台维护"生产者-裁判"配对记录，检测固定配对勾结（pairing entropy 过低告警）；
2. **盲评**：judge 上下文中隐去生产者身份与信誉信息（防"名人效应"偏袒）；多方案比较时随机洗牌顺序（防位置偏置）；
3. **多裁判聚合**：高风险产物 ≥2 个独立 judge，取中位数或 Borda 计数；分歧超阈值自动升级第三裁判或人；
4. **金丝雀校准（Seeded Defects）**：平台持续向验收管线注入**已知缺陷的标准产物**（金丝雀样本），监测各 judge 的检出率——judge 本身也积累信誉分，检出率下滑的 judge 被降权或淘汰。这是体系不自欺的关键；
5. **Rubric 版本化**：评分细则进 Schema Registry，评分结果记录 rubric 版本，防止"换 rubric 刷分"且保证跨时间可比；
6. **judge 输入留痕**：judge 收到的完整上下文包 hash 落审计，评分可复核。

### 18.3 质量数据模型

```yaml
quality_record:
  artifact_id: "art_..."
  producer: { agent_id, platform, attempt }
  layers:                            # 每层结果与证据
    L0: { pass: true }
    L1: { pass: false, violations: [{rule: "fact-consistency#17", detail: ...}] }
    L2: { score: 0.82, confidence: 0.9, judges: [...], rubric: "rubric://report/v3" }
  final: { score: 0.82, verdict: accepted | rework | rejected, failure_class: null }
  verified_by: [ ... ]
  ts: ...
```

- 质量分规范化到 [0,1] 且带置信度——**置信度低的高分不应被当作高分使用**（调度与信誉按 `score × confidence` 计）；
- 全部经 `artifact.verified` 事件进入事件流，成为信誉闭环与抽样的数据源。

---

## 19. 专题深化 E：信誉反馈闭环

### 19.1 信誉模型

信誉按 **(agent_id, skill)** 细粒度维护，并聚合出 (agent_id) 与 (platform) 两级粗粒度视图（粗粒度用于冷启动回退与熔断）：

| 维度 | 来源 | 更新方式 |
|------|------|---------|
| 成功率 | `subtask.succeeded/failed`（区分可重试失败与质量失败） | Beta 后验（见 19.2） |
| 质量分 | Verifier 的 `score × confidence` | EWMA（半衰期可配，默认 7 天） |
| 时延 | 租约 wall-clock（平台权威计量，§21） | 分位数在线估计（P50/P95，t-digest） |
| 成本效率 | 结算成本 / 质量分 | EWMA |
| 可靠度 | 租约丢失率、心跳稳定性、拒绝率 | Beta 后验 |

### 19.2 更新算法与冷启动

- **成功率用 Beta-Binomial 后验**：`score = (α + success) / (α + β + total)`，先验 (α,β) 来自同 platform 同 skill 的群体均值——**新 Agent 天然向群体均值收缩（shrinkage）**，样本少时不敢给极端分，样本多了让数据说话；
- **时延/质量用 EWMA + 半衰期**：近期表现权重高，Agent 能力变化（升级/劣化）能被快速反映；长期无活动的信誉按时间衰减回先验（陈旧折扣）；
- **只有经 Verifier 确认的结果才全额计入**；未验证的快速通道结果按折扣权重计入（防"跳过验收刷成功率"）。

### 19.3 闭环注入调度

```
artifact.verified / subtask.failed 事件
   → Reputation Updater（流处理，幂等消费）
   → 信誉表（PG）+ Scheduler 本地缓存（秒级 TTL）
   → 调度打分 w2 项（§5.2）与熔断判定
```

- **探索-利用平衡**：纯按信誉调度会让新 Agent 永无机会（冷启动死锁）。Scheduler 以 ε-greedy / Thompson Sampling 留出**探索流量**（默认 5–10% 的非关键任务派给低样本 Agent），把调度形式化为多臂老虎机问题；
- **金丝雀复测**：信誉跌破阈值的 Agent 不直接永久拉黑，而是定期收到**合成探针任务**（已知标准答案，来自测试床 §20）——凭复测成绩恢复或确认淘汰，避免"一次事故终身判刑"，也防信誉表被旧数据锁死；
- **防刷分**：信誉变化速率限幅（单次任务对分数影响封顶）；同一 Mission 内大量自产自验的配对进入勾结检测；新注册 Agent 有押金/配额冷启动期。

### 19.4 对 Agent 运营方的透明性

信誉对运营方可查可解释（各维度分数、关键影响事件链接），并提供申诉通道（误判的验收结果经人复核后可撤销并回补信誉）——信誉黑箱会迫使优质 Agent 离开平台。

---

## 20. 专题深化 F：仿真 / 回放测试床

### 20.1 目标与定位

控制平面（Orchestrator/Scheduler/Trigger）的正确性与性能必须在**无真实 Agent** 的环境中可验证：回归测试、调度策略 A/B、混沌容错、容量规划。测试床同时是 §19 金丝雀探针任务的来源。

### 20.2 四种模式

| 模式 | 说明 | 用途 |
|------|------|------|
| **Harness（单元级）** | 进程内假 Adapter，实现完整租约/心跳/上报协议的可编程 Agent | CI 回归：状态机、幂等、fencing、委托护栏 |
| **Shadow（影子模式）** | 新版 Scheduler 与生产并行跑，消费相同就绪队列快照，**只记录决策不执行**，与生产决策比对 | 新策略上线前的决策一致性验证 |
| **Load/Chaos（压力混沌）** | 合成 Agent 舰队（数百~数千）+ 故障注入 | M3 验收压测、熔断/抢占/恢复验证 |
| **What-if（反事实回放）** | 录制生产事件流，用不同策略权重重放调度决策，对比 SLA/成本/利用率 | 策略调参依据，替代拍脑袋 |

### 20.3 合成 Agent（Sim Agent）

实现标准 Adapter 协议的程序化 Agent，行为由画像驱动：

```yaml
sim_agent:
  skills: ["web.research"]
  latency: { dist: lognormal, p50: "8s", p95: "30s" }
  failure: { rate: 0.05, classes: { timeout: 0.6, bad_output: 0.3, lease_loss: 0.1 } }
  quality: { dist: beta, mean: 0.8 }       # 与测试床答案库联动产出"相应质量"的结果
  cost_per_1k: 0.01
  behavior_scripts:                          # 剧本化故障
    - { at: "T+5m", action: "partition", duration: "2m" }
    - { at: "T+10m", action: "degrade", latency_multiplier: 3 }
```

### 20.4 确定性回放与虚拟时钟

- **虚拟时钟**：控制平面所有时间读取走注入时钟（设计时即禁止裸 `now()`——与 §14.3 表达式时钟同一约束），仿真可 100× 加速跑完 4 小时的 Mission 生命周期；
- **确定性**：随机源全部种子化；相同事件流 + 相同种子 → 逐事件一致的最终状态（状态哈希比对作为 CI 断言）；
- **回放保真**：生产事件流录制（含间隔分布），回放时按原相对时序驱动；调度器的每次决策输入（就绪队列、信誉快照、负载快照）随事件一并录制，保证决策可复现；
- **场景 DSL**：声明式定义 Mission 混合比、Agent 舰队、故障剧本、断言（如"SLA 达成率 ≥ 99%"、"无僵尸租约"、"无重复执行"），断言失败即 CI 红灯。

### 20.5 指标门

每次回放/压测产出标准指标集：SLA 达成率、调度延迟分布、租约过期率、恢复时长（MTTR）、成本偏差、公平性（各 Agent 负载基尼系数）。纳入 CI 回归门，策略 PR 必须附 What-if 对比报告。

---

## 21. 专题深化 G：计量计费口径

### 21.1 核心问题：上报不可信

Agent/Adapter 自报的 token 与成本是**利益相关数据**，不能直接作为计费依据。计量分两类口径：

| 口径 | 内容 | 信任级 |
|------|------|--------|
| **平台权威计量** | 租约 wall-clock、排队时长、重试次数、artifact 存储字节与时长、Verifier 消耗、触发/唤醒次数、上下文包字节 | 权威，平台直接测量 |
| **Agent 侧用量（LLM tokens 等）** | 模型调用 token 数 | 不可信，需交叉验证 |

### 21.2 Token 用量的交叉验证（三级，按部署条件选用）

1. **网关仲裁（首选）**：有条件时 Agent 的模型调用经平台 LLM Gateway 中转，token 计数权威、可按 subtask 归属——同时解决密钥托管与数据边界；
2. **旁路估算**：平台对流经自身的输入（上下文包）与输出（产物文本）用对应 tokenizer 估算，与上报值比对，偏差超阈值（默认 ±25%）进入异常检测；
3. **统计审计**：对上报序列做异常检测（同 skill 群体分布的离群点、单位质量成本异常低→可能虚报高质量，异常高→可能成本注水），命中即降信誉并触发人工审计。

计费争议以平台权威计量为准，Agent 侧用量仅在有网关或估算佐证时入账。

### 21.3 内部记账模型

- **记账单位**：内部 credit，价格表（price book）按资源类型版本化（`compute.sec / token.in / token.out / storage.GB-day / verify.call / wake.fire`…），调价不影响历史账单；
- **生命周期**（与 §15.5 衔接）：`delegate/调度 → hold（预扣估计值）→ 完成/失败 → settle（按权威计量实际值，幂等键 (subtask_id, attempt)）→ 取消 → release`；hold 过期未 settle 自动释放并告警（防泄漏）；
- **多粒度归属**：credit 消耗同时挂 mission / tenant / team / agent 四个维度，支持成本报表与 chargeback；
- **配额与熔断**：tenant/user/team 三级配额（速率 + 总额）；预算池剩余低于在途 hold 总额时拒绝新触发（§7.2 准入管道第③步的数据来源）。

### 21.4 结算与报表

- 结算由完成事件驱动、幂等；事件溯源保证账单可从事件流重算（对账 = 重放）；
- 产出三类报表：**Mission 成本明细**（给人看的"这单钱花哪了"）、**Agent 成本效率榜**（喂给调度 w3）、**tenant 用量账**（配额与计费）；
- 低成本起步：MVP 阶段只做 hold/settle 记账与报表，不接真实支付；真实计费在网关仲裁落地后再开启。

---

## 22. 工程落地前：技术方案与架构选型分析

### 22.1 选型原则

1. **Boring Technology 优先**：控制平面的正确性（状态机、租约、fencing、恰好一次）是本项目最大风险，组件越无聊越成熟，调试面越小；
2. **PG-first，延迟拆分**：PostgreSQL 一个组件先承担实体存储 + 队列（SKIP LOCKED）+ 触发规则 + Schema Registry，事件日志也先落 PG 表；每个组件都预设"何时换"的触发条件（见 22.4），但不为想象中的规模提前付复杂度税；
3. **Build vs Buy 的判定标准**：只自研构成**差异化**的部分（调度器、信誉、上下文包物化、触发准入管道），商品化的部分（工作流持久执行、API 网关、可观测栈）一律用现成的；
4. **一切时间读取注入化、一切随机种子化**——这是 §20 仿真回放的前置，必须从第一行代码就立规矩，事后改造代价巨大。

### 22.2 最大决策：编排引擎自研 vs  Temporal

这是落地前唯一可能推翻 §1–§21 部分实现的决策，正面分析：

| 维度 | 自研（PG 状态机 + 队列驱动） | 基于 Temporal |
|------|------------------------------|----------------|
| 长挂起任务（WAITING/人工审批数小时） | 状态在 PG，天然无成本挂起 | Durable timer + Signal，原生擅长 |
| 主子委托（动态追加子任务） | 自己写 DAG 追加逻辑 | Child Workflow 语义高度契合 |
| 重试/补偿/超时 | 自己实现（§5.4 全部手写） | 内建，经过大规模验证 |
| **放置调度**（§5 打分、租约、fencing、Gang、抢占） | 自研——本就是核心差异化 | **仍需自研**，Temporal 不管"派给谁" |
| **信誉/上下文包/触发准入** | 自研 | 仍需自研 |
| 事件模型 | 自己的事件溯源，语义完全可控 | Temporal History 是另一套事件模型，与 §4.2 审计/回放模型**语义重叠且不同构**，双事件源是长期心智负担 |
| 仿真回放（§20 虚拟时钟、确定性） | 完全自控 | Temporal 有测试框架但时钟/确定性模型受其约束，What-if 反事实回放难做 |
| 团队心智成本 | 状态机+队列，普通后端技能 | 整套 durable execution 心智模型 + 运维一个 Temporal 集群 |
| 风险 | 编排引擎的边角案例（崩溃恢复、重入）自己踩 | 被框架语义束缚，核心差异化层反而难贴合 |

**结论：MVP 自研，但把编排内核抽象为 Execution Engine SPI**（接口：`apply(event) → transition`、`due_timers()`、`ready_queue()` 等约 10 个方法）。理由：差异化层（调度/信誉/上下文/触发）无论选什么都得自研，而自研状态机的风险主要集中在崩溃恢复与重入——用**事件溯源 + 投影重建**（§4.3，本来就要做）正好兜底：引擎崩溃后从事件日志重建内存态。Temporal 保留为 M3 之后、若编排复杂度真的失控时的替换选项，SPI 保证替换成本可控。

**明确不做的反面**：不要为了"分布式"上 etcd/Raft 自研协调——PG 的事务与唯一约束就是本规模下最好的协调原语。

### 22.3 语言与运行时

| 层 | 推荐 | 理由 |
|----|------|------|
| 控制平面（Task/Orchestrator/Scheduler/Trigger/Registry/HITL） | **Go** | 高并发 IO 密集场景的部署与运维成本最低；`pgx`、`cel-go`、OPA 生态成熟；单二进制交付契合 §12.1 部署形态 |
| Agent SDK | **Python**（主）+ TypeScript（次） | Agent 生态在 Python；SDK 只做协议封装，薄 |
| Adapter（OpenClaw/Hermes 托管连接器） | 跟随目标平台语言，协议面用 Go 写的参考实现做兼容性基准 | — |
| Console | TypeScript + React，DAG 渲染用 React Flow（+ ELK 自动布局），时间线自绘 | 生态成熟，无特殊要求 |

### 22.4 组件选型与替换触发条件

| 组件 | MVP 选择 | 何时替换 / 换成什么 |
|------|---------|---------------------|
| 实体存储 + 队列 + 触发规则 + Schema Registry | **PostgreSQL 16**（`SKIP LOCKED` 队列、`LISTEN/NOTIFY` 做轻量唤醒提示、逻辑复制备审计导出） | 写入 >5k/s 或单库 >2TB：事件日志先拆 Kafka/Redpanda；再不够换分布式 SQL（CockroachDB） |
| 事件日志 | **PG `events` 表**（分区 + 只追加 + 定期归档对象存储） | 多消费者订阅需求出现（报表/信誉/审计各自消费）→ NATS JetStream（轻）或 Kafka（重） |
| 黑板热数据 / 调度器缓存 | **Redis**（或 ValKey） | 一般不用换；黑板冷数据一开始就在 S3 |
| 产物存储 | **S3 / MinIO**，内容寻址（§4.1） | 不换 |
| 表达式引擎 | **cel-go**（§14.3） | 不换 |
| 策略引擎（调度策略、数据边界） | **嵌入式 CEL/Rego 规则 + 配置**，先不上 OPA sidecar | 策略需要跨团队自治管理时 → OPA |
| LLM Gateway（§21 网关仲裁） | MVP 不做，旁路估算先行 | 开启真实计费时 → 接 LiteLLM / 自研薄网关 |
| 可观测 | **OpenTelemetry + Prometheus + Grafana + Tempo**；Console 内嵌 trace 链接 | 不换 |
| 认证 | **OIDC**（对接 Dex/Keycloak 或企业 IdP）+ Agent 用签名 token（Ed25519） | mTLS 在服务网格化后再议 |

**刻意不选**：Kafka（MVP 杀鸡用牛刀）、Kubernetes Operator 模式（先于有 K8s 之前不抽象）、服务网格、独立 API Gateway（Go 服务内嵌中间件足够）。

### 22.5 API 与通道

- 北向（人/外部系统）：**REST + OpenAPI 3.1**，事件订阅用 **SSE**（比 WebSocket 简单、自动重连、过代理友好；决策收件箱等交互仍走普通 HTTP）；
- 南向（Agent/Adapter）：**REST + Webhook 回调**起步（远端平台最通用），预留 gRPC streaming 升级位（协议从第一天就按"请求/事件/确认"三元组建模，迁移不改语义）；
- 出站 Webhook：签名（HMAC）+ 指数退避重投 + 死信。

### 22.6 量级估算（论证 PG-first 够用）

取一个偏激进的目标规模估算：**1,000 并发 Mission × 平均 20 子任务 × 每子任务全生命周期 ~15 个事件 ≈ 30 万事件/波次**；假设波次在 1 小时内完成，稳态事件写入 **<100/s，峰值 ~500/s**；就绪队列出队 <50/s；信誉更新 <200/s。

对照经验值：PG 单节点简单行写入 1–2 万/s、`SKIP LOCKED` 出队数千/s、CEL 求值微秒级。**余量 1–2 个数量级**。真正的扩展瓶颈会先到 **Adapter 长连接数与产物存储带宽**，而不是控制平面——所以投资顺序是：连接层水平扩展 > 存储拆分 > 引擎拆分。

### 22.7 演进路径（与 §12.1 对齐的落地形态）

```
阶段0（M1）：单 Go 二进制 + PG + Redis + MinIO，docker-compose 一把起
阶段1（M2-M3）：控制平面无状态多副本（PG 做协调），事件消费组拆出 Reputation/Billing worker
阶段2（M4+）：事件日志外迁 NATS/Kafka，Adapter 连接层独立部署，按租户单元化（cell）
```

### 22.8 架构决策记录（ADR 摘要）

| # | 决策 | 备选 | 依据 |
|---|------|------|------|
| ADR-1 | 编排引擎自研 + Execution Engine SPI | Temporal、Camunda | 见 22.2；差异化层必须自研，事件模型须与审计/仿真同构 |
| ADR-2 | PG-first 单库承担四角色 | 起步即 Kafka+独立队列 | 22.6 量级估算；替换触发条件见 22.4 |
| ADR-3 | 控制平面 Go，SDK Python/TS | Rust（过重）、Java（部署重） | 团队效率与交付形态 |
| ADR-4 | CEL 作为唯一表达式语言（触发/条件/策略复用） | JSONLogic、Lua | §14.3：静态可分析 + 成本上限 |
| ADR-5 | 状态迁移一律事件先行 + Outbox | 直接写库 + 异步发消息 | §4.3：审计、回放、投影重建三位一体 |
| ADR-6 | 调度器多副本竞争消费队列，无 leader | leader 选举 | 租约+fencing 已防脑裂，省掉协调层 |
| ADR-7 | Agent 间不直连，一切经平台中转 | 点对点 A2A 直连 | §15：可审计、可换人、可仲裁；A2A 仅作协议语义参考 |
| ADR-8 | 全平台禁止裸时钟/裸随机 | — | §20 确定性回放前置 |

### 22.9 工程风险与缓解（落地视角）

| 风险 | 早期信号 | 缓解 |
|------|---------|------|
| 状态机边角案例（重入、崩溃中途） | 测试中出现"幽灵租约" | 事件溯源重建 + §20 Harness 混沌用例从第一周就上，不留到 M3 |
| PG 队列热点（就绪队列行锁竞争） | p99 调度延迟爬升 | 队列按 priority 分桶多表/分区；出队批量化 |
| CEL 表达式拖慢调度热路径 | 求值 p99 上升 | 表达式编译缓存 + 求值 cost 上限（§14.3）+ 热路径只走倒排索引增量求值 |
| Adapter 协议演进撕裂已有接入方 | 第三方接入后出现 breaking change 诉求 | 协议版本号 + 兼容性测试套件（§17 #7）随 M1 就建立最小骨架 |
| 事件表膨胀拖垮 PG | vacuum 压力、查询变慢 | 分区 + 7 天热存 + 归档对象存储；归档作业从第一天就有 |

---

## 附录 A：术语表

- **Mission / Task / Subtask**：目标 / 拆解后的图 / 可执行最小单元
- **Lease / Fencing Token**：执行租约 / 防僵尸写入的单调令牌
- **Blackboard**：Mission 内共享上下文区
- **Adapter**：平台协议适配器
- **Watcher / Decision Maker**：观察型 / 决策型人类角色
- **Verifier**：负责验收与质检的 Agent 或规则引擎
- **TaskIntent / Trigger**：归一化的任务意图 / 触发规则（定时、事件模式、条件）
- **Continuation**：Agent 挂起-唤醒模式（suspend + wake_on，凭检查点续跑）
- **Correlation Key / Watermark**：事件匹配的关联分区键 / 迟到事件容忍水位
- **Context Package / Snapshot Hash**：dispatch 时物化的上下文包 / 其内容指纹（审计用）
- **Rollup**：Lead 对子树的摘要式视图（状态+摘要+引用，不含完整对话）
- **Permission Attenuation**：委托链上权限只能收窄不能放大的原则
- **Hold（预算预扣）**：delegate 时原子冻结预算，完成结算、取消释放
- **Seeded Defects / 金丝雀样本**：注入验收管线的已知缺陷产物，用于校准 judge 检出率
- **Shrinkage（收缩）**：小样本信誉向群体先验均值回缩的贝叶斯处理
- **探索流量（ε-greedy / Thompson Sampling）**：调度器留给低样本 Agent 的试用份额
- **Shadow / What-if**：影子并行决策比对 / 录制事件流的反事实策略回放
- **虚拟时钟**：控制平面统一注入的逻辑时钟，支撑确定性回放与加速仿真
- **Price Book / Credit**：版本化资源价格表 / 平台内部记账单位
- **权威计量 vs 上报计量**：平台直接测量的可信数据 vs Agent 自报的需交叉验证数据
- **Execution Engine SPI**：编排内核的抽象接口层，隔离自研状态机与未来替换（如 Temporal）的成本
- **ADR**：架构决策记录，固化关键选型及其依据
- **PG-first**：以 PostgreSQL 单组件优先承担多角色、按触发条件延迟拆分的选型策略
