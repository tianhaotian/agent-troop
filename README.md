# Agent Troop

多智能体协同平台：跨 Agent 平台（OpenClaw / Hermes / 自研等）的任务拆解、中心化状态管理、调度与人在回路协作。

- **设计文档**：[docs/design/multi-agent-collab-platform-design.md](docs/design/multi-agent-collab-platform-design.md)（22 节 + 附录，含架构、调度、触发体系、协作模式、质量/信誉/仿真/计量专题与技术选型 ADR）
- **实现计划**：[docs/plan/](docs/plan/)（按里程碑拆分，已交付：[M1 MVP](docs/plan/M1-mvp.md)、[M2 人在回路](docs/plan/M2-hitl.md)、[M3 调度与挂起-唤醒](docs/plan/M3-sched-trigger.md)、[M4 触发管道](docs/plan/M4-trigger-pipeline.md)、[M5 CEL 与授权](docs/plan/M5-cel-scope.md)、[M6 主子委托](docs/plan/M6-delegate.md)、[M7A 生产可靠性基线](docs/plan/M7A-production-baseline.md)、[M7B Lead 恢复闭环](docs/plan/M7B-lead-recovery.md)、[M7C 预算预占与结算](docs/plan/M7C-budget-holds.md)、[M7D 权限与上下文包](docs/plan/M7D-context-permissions.md)）
- **License**：[Apache 2.0](LICENSE)

## 开发流程约定（重要）

1. **README + Plan 先行**：每个里程碑/功能先在 `docs/plan/` 落实现计划（范围、验收标准、测试方案），再动手写代码；
2. **测试通过才算交付**：功能实现后必须有对应测试且全部通过（`go test ./...`），否则不算完成；
3. **工程红线**（来自设计文档 ADR-8、§22.9）：
   - 全代码库**禁止裸时钟 / 裸随机**——时间与随机源一律经 `internal/clock`（待建）注入，为确定性回放预留；
   - 状态迁移**事件先行 + Outbox**，不直接双写；
   - 混沌/状态机测试随功能同步交付，不留到后期。

## 仓库结构

```
cmd/troopd/          控制平面单二进制（API + Orchestrator + Scheduler + Trigger）
internal/
  mission/           任务模型与状态机（Mission / Subtask / 事件）
  store/             PostgreSQL 访问层（实体 + SKIP LOCKED 队列 + 事件日志）
  orchestrator/      DAG 执行引擎
  scheduler/         放置调度（Filter → Score → Lease + Fencing）
  registry/          Agent 注册、能力画像、健康
  trigger/           触发准入管道、定时/事件/条件唤醒（M3）
  api/               REST/OpenAPI 北向接口 + SSE
migrations/          SQL 迁移（PG-first，见设计 §22.4）
adapters/http-echo/  通用 HTTP Adapter 参考实现（回显 Agent，用于端到端自测）
sdk/python/          Agent SDK（Python，薄协议封装）
web/console/         Web 控制台（DAG 视图 + 事件时间线）
docs/design/         系统设计文档
docs/plan/           里程碑实现计划
```

## 快速开始

```bash
# 依赖：Go 1.21+（生产部署另需 Docker 提供 PostgreSQL 16）
go test ./...                  # 交付门槛：全绿
go test -tags e2e ./e2e/       # 端到端验收

# 本地零依赖体验（内存存储）：
go run ./cmd/troopd &                                    # 控制平面 :8080，Console 在 http://localhost:8080/
go run ./adapters/http-echo -id echo1 -skills web.research &
curl -X POST localhost:8080/v1/missions -d '{
  "owner": "me", "goal": "demo",
  "tasks": [{"name":"collect","kind":"agent","required_skills":["web.research"]}]
}'

# 持久化（PG-first）：
docker compose up -d postgres
psql postgres://troop:troop@localhost:5432/troop -f migrations/0001_init.sql \
                                               -f migrations/0002_hitl_board.sql \
                                               -f migrations/0003_m3.sql \
                                               -f migrations/0004_m4.sql \
                                               -f migrations/0005_m5.sql \
                                               -f migrations/0006_reliability.sql \
                                               -f migrations/0007_lead_recovery.sql \
                                               -f migrations/0008_budget.sql \
                                               -f migrations/0009_context_packages.sql
TROOP_PG_DSN=postgres://troop:troop@localhost:5432/troop go run ./cmd/troopd

# 探针：healthz 只检查进程；readyz 会真实检查 Store（PG 不可用时返回 503）
curl -f localhost:8080/healthz
curl -f localhost:8080/readyz

# 可选环境变量：TROOP_SCHEDULER=capability-first|round-robin（放置策略，M3）
#               TROOP_BLOB_DIR=./data/artifacts（Artifact blob 目录）
```

## M3 API 示例（挂起-唤醒 / 检查点续跑）

```bash
# 1) progress 心跳携带检查点（fencing 校验；≤64KB 透明载荷，崩溃/换 Agent 后续跑用）
curl -X POST localhost:8080/v1/subtasks/sub_xxx/progress -d '{
  "agent_id":"echo1", "lease_id":"lea_xxx", "fencing_token":3,
  "checkpoint":{"step":3,"partial":"..."}
}'

# 2) Agent 挂起自身（RUNNING→WAITING，释放租约；wake_on 必带 TTL 防永久悬挂）
curl -X POST localhost:8080/v1/subtasks/sub_xxx/suspend -d '{
  "agent_id":"echo1", "fencing_token":3, "version":4,
  "wake_on":{"kind":"timer", "at":"2026-08-05T09:00:00Z", "deadline":"2026-08-06T09:00:00Z"},
  "checkpoint":{"step":3}
}'
# timer 到期由 sweeper 唤醒回 READY 重新调度（可换 Agent，offer 里带 checkpoint）；
# TTL 过期未唤醒 → FAILED(reason=wake_timeout) + 下游级联取消

# 3) 人工唤醒（manual 挂起只能这样醒）
curl -X POST localhost:8080/v1/subtasks/sub_xxx/wake -d '{"actor_id":"lead"}'
```

## M4 API 示例（event/condition 唤醒 / 触发准入）

```bash
# 1) event 唤醒：挂起等"某类事件到达"（types 过滤 + where 载荷子集等值谓词）
#    水位线语义：suspend 时平台记录 after_seq，只匹配注册之后到达的同 Mission 事件
curl -X POST localhost:8080/v1/subtasks/sub_xxx/suspend -d '{
  "agent_id":"echo1", "fencing_token":3, "version":4,
  "wake_on":{
    "kind":"event",
    "event":{"types":["subtask.succeeded"], "where":{"result_ref":"artifact://a-done"}},
    "deadline":"2026-08-06T09:00:00Z"
  }
}'
# 事件到达后由 BoardPut 钩子/sweeper 增量评估，命中即 CAS 唤醒（恰好一次）

# 2) condition 唤醒：挂起等黑板条件成立（结构化谓词，CEL 内核槽位预留）
curl -X POST localhost:8080/v1/subtasks/sub_xxx/suspend -d '{
  "agent_id":"echo1", "fencing_token":3, "version":4,
  "wake_on":{
    "kind":"condition",
    "condition":{"board":"shared/glossary", "op":"exists"},
    "deadline":"2026-08-06T09:00:00Z"
  }
}'
# op: exists（键存在即真）| equals（值 JSON 等值，须带 value）；
# BoardPut 命中即醒，sweeper 全量兜底（anti-entropy）

# 3) 触发准入管道：人/Agent 归一化触发入口（create_mission 幂等去重）
curl -X POST localhost:8080/v1/intents -d '{
  "source":{"kind":"agent", "id":"agt_ext"},
  "action":"create_mission", "idempotency_key":"req-42",
  "owner":"me", "goal":"demo",
  "tasks":[{"name":"collect","kind":"agent"}]
}'
# → {"mission_id":"msn_xxx"}；重发同键 → 200 + 原 mission_id + "deduplicated":true
# source 落入 mission.created 事件 actor 留痕；action=wake 可唤醒 WAITING 子任务：
curl -X POST localhost:8080/v1/intents -d '{
  "source":{"kind":"human", "id":"lead"}, "action":"wake", "subtask_id":"sub_xxx"
}'
```

## M5 API 示例（CEL 条件内核 / scope 触发授权）

```bash
# 1) condition 唤醒的 CEL 形态：condition.expr 与 M4 结构化谓词（board/op）互斥
curl -X POST localhost:8080/v1/subtasks/sub_xxx/suspend -d '{
  "agent_id":"echo1", "fencing_token":3, "version":4,
  "wake_on":{
    "kind":"condition",
    "condition":{"expr":"board.shared.input_ready == true && board.shared.count >= 2"},
    "deadline":"2026-08-20T09:00:00Z"
  }
}'
# 数据模型：board.<ns>.<key>（ns/key 含点号时用下标 board["cfg.ns"]["k"]）、
#   mission.{id,owner,goal,status}、subtask.{id,kind,state,attempt,assignee}、
#   elapsed()（自挂起注册起）/ deadline_in()（距唤醒 TTL）——只读逻辑时钟，无裸系统时钟
# 安全护栏（§14.3）：
#   - 注册时编译 + 类型检查 + 静态 cost 估算，超限直接 400 拒绝；
#   - 运行时 cost 上限，超限视为 false 并落 condition.cost_exceeded 事件告警；
#   - 静态引用提取（board 键集随 wake_spec 持久化）：BoardPut 只增量评估引用
#     该键的注册；动态下标等不可判定形态按通配处理（宁多评不漏评），sweeper 全量兜底

# 2) scope 触发授权（§7.4 默认收紧）：Agent 注册时显式声明 trigger_scopes
curl -X POST localhost:8080/v1/agents/register -d '{
  "id":"agt_ext", "name":"ext", "platform":"custom",
  "capabilities":[{"skill":"web.research","level":0.9}],
  "trigger_scopes":["trigger.create_mission","trigger.wake"]
}'
# source.kind=agent 的 /v1/intents 强制校验：create_mission 需 trigger.create_mission、
# wake 需 trigger.wake；未注册或未授权 → 403 且不消耗幂等键（鉴权先于去重）；
# 缺省 trigger_scopes=[] 即默认不能触发；human source 不鉴权（SSO/RBAC 后续）
```

## M6 API 示例（主子委托：delegate / rework）

```bash
# 1) Lead 在执行中（RUNNING、持租约）派生子女任务——fencing 即委托权
#    需 trigger.spawn_subtask scope；幂等键必填（重发返回原子女 + deduplicated:true）
curl -X POST localhost:8080/v1/intents -d '{
  "source":{"kind":"agent", "id":"agt_lead"},
  "action":"delegate", "idempotency_key":"dlg-42",
  "parent_subtask_id":"sub_xxx", "fencing_token":3, "parent_version":4,
  "task":{"name":"research", "required_skills":["web.research"],
          "input":{"topic":"储能行业"}, "priority":5}
}'
# → {"mission_id":"msn_xxx", "subtask_id":"sub_xxx_research"}
# 子女即建即 READY 参与调度（parent_id 因果链，不占 DAG 依赖位）；
# max_depth / max_fanout 结构校验（Config.MaxDelegateDepth=4 / MaxDelegateFanout=8）

# 2) Lead 挂起精确等待该子女完成（subtask.succeeded 载荷含 subtask_id），不空占资源
curl -X POST localhost:8080/v1/subtasks/sub_xxx/suspend -d '{
  "agent_id":"agt_lead", "fencing_token":3, "version":4,
  "wake_on":{"kind":"event",
    "event":{"types":["subtask.succeeded"], "where":{"subtask_id":"sub_xxx_research"}},
    "deadline":"2026-08-20T09:00:00Z"}
}'

# 3) 验收不通过 → rework：链式重派新子女（rework_of 因果链 + feedback 入 input），
#    链长达 Config.MaxRework=3 即拒，Lead 应换方案或升级人决策
curl -X POST localhost:8080/v1/intents -d '{
  "source":{"kind":"agent", "id":"agt_lead"},
  "action":"delegate", "idempotency_key":"dlg-43",
  "parent_subtask_id":"sub_xxx", "fencing_token":5, "parent_version":6,
  "task":{"name":"research_v2", "required_skills":["web.research"],
          "rework_of":"sub_xxx_research", "feedback":"数据太旧，补充 2026 年数据"}
}'
```

## M7B API 示例（Lead 快照 / 收件箱 / takeover）

```bash
# 1) Lead 心跳与计划快照原子提交：首次 expected_version=-1，之后使用响应 version 做 CAS
curl -X POST localhost:8080/v1/subtasks/sub_lead/lead/heartbeat -d '{
  "agent_id":"agt_lead", "fencing_token":5, "expected_version":-1,
  "snapshot":{"dag_intent":"汇总调研结果","known":{"sub_research":"RUNNING"},
              "artifacts":[],"next":"等待并验收"}
}'

# 2) child 完成后结果原子进入 Lead inbox；查询不会自动消费
curl 'localhost:8080/v1/subtasks/sub_lead/lead/inbox?status=pending'
# Lead 显式选择摄入粒度（summary|full），并以 inbox item version 做 CAS
curl -X POST localhost:8080/v1/subtasks/sub_lead/lead/inbox/lin_sub_research/ingest -d '{
  "agent_id":"agt_lead", "fencing_token":5, "expected_version":0, "mode":"summary"
}'

# 3) 继任 Lead 获取最近计划快照 + 完整收件箱（含 pending/ingested 状态），重建执行上下文
curl localhost:8080/v1/subtasks/sub_lead/lead/context
# Lead heartbeat 租约到期后，sweeper 会 fence 旧租约并把 Lead 任务送回 READY；
# in-flight child 不受影响，新 Lead 经正常 Scheduler/lease 流程接管并获得新 fencing token。
```

## M7C API 示例（预算 hold / settle / release）

```bash
# 1) 创建带 100k token 硬顶的 Mission；不传 budget_tokens 则保持 unmetered
curl -X POST localhost:8080/v1/missions -d '{
  "owner":"me", "goal":"预算内完成调研", "budget_tokens":100000,
  "tasks":[{"name":"lead","kind":"agent","required_skills":["lead.coordinate"]}]
}'

# 2) budgeted Mission 的 delegate 必须声明正数切片；创建 child 与 60k hold 同事务
curl -X POST localhost:8080/v1/intents -d '{
  "source":{"kind":"agent","id":"agt_lead"},
  "action":"delegate", "idempotency_key":"dlg-budget-1",
  "parent_subtask_id":"sub_lead", "fencing_token":5, "parent_version":6,
  "task":{"name":"research","required_skills":["web.research"],"budget_tokens":60000}
}'

# 3) child 完成时上报实际用量：hold 释放，45k 计入 spent，15k 自动退回 available
curl -X POST localhost:8080/v1/subtasks/sub_research/complete -d '{
  "agent_id":"agt_worker", "fencing_token":8, "version":4,
  "idempotency_key":"complete-budget-1", "result_ref":"artifact://research",
  "usage_tokens":45000
}'

# 4) 查询账户与全部 hold 明细；最终失败或取消会把 HELD 原子变为 RELEASED
curl localhost:8080/v1/missions/msn_xxx/budget
```

## M7D API 示例（权限衰减 / 上下文包）

```bash
# 1) 根任务声明权限包络；未声明 grants 的旧任务保持最小权限（public、无数据视图）
curl -X POST localhost:8080/v1/missions -d '{
  "owner":"me", "goal":"最小知情调研",
  "tasks":[{"name":"lead","kind":"agent","required_skills":["lead.coordinate"],
    "grants":{"classification":"internal","tool_scopes":["search"],
      "artifact_refs":["art_xxx"],
      "board_views":[{"namespace":"shared","keys":["glossary"],"mode":"rw"}]}}]
}'

# 2) delegate grants 必须是 parent 的子集：public ≤ internal、ro ≤ rw，且不能新增 key/tool/artifact
curl -X POST localhost:8080/v1/intents -d '{
  "source":{"kind":"agent","id":"agt_lead"},
  "action":"delegate", "idempotency_key":"dlg-context-1",
  "parent_subtask_id":"sub_lead", "fencing_token":5, "parent_version":6,
  "task":{"name":"research","required_skills":["web.research"],
    "grants":{"classification":"public","tool_scopes":["search"],
      "artifact_refs":["art_xxx"],
      "board_views":[{"namespace":"shared","keys":["glossary"],"mode":"ro"}]}}
}'

# 3) offer 已携带不可变 context_package；也可按 lease 读取同一快照
curl localhost:8080/v1/agents/agt_worker/offers
curl localhost:8080/v1/leases/les_xxx/context
# snapshot_hash 记入 context.materialized 事件，可审计该 Agent 当时看到的精确视图。
```

## M2 API 示例（人在回路 / 黑板 / Artifact）

```bash
# 1) 含审批门的 Mission：agent 生成 → human 审批 → agent 发布
curl -X POST localhost:8080/v1/missions -d '{
  "owner": "me", "goal": "发布研报",
  "tasks": [
    {"name":"draft",   "kind":"agent",          "required_skills":["web.research"]},
    {"name":"gate",    "kind":"human_approval", "depends_on":["draft"],
     "question":"发布该草稿？", "on_timeout":"auto_reject"},
    {"name":"publish", "kind":"agent",          "depends_on":["gate"]}
  ]
}'
# draft 完成后 gate 自动生成裁决工单（子任务置 BLOCKED）；reject 会级联取消下游并使 Mission FAILED

# 2) 决策收件箱：拉取待裁决 → 裁决（"reject" 为唯一否决值，其余均视为批准）
curl 'localhost:8080/v1/decisions?status=pending'
curl -X POST localhost:8080/v1/decisions/dec_xxx/resolve -d '{
  "choice":"approve", "rationale":"LGTM", "decider_id":"lead"
}'

# 3) Agent 执行中主动请求人决策（RUNNING→BLOCKED 挂起，批准后原 Agent 续跑）
curl -X POST localhost:8080/v1/subtasks/sub_xxx/request_decision -d '{
  "agent_id":"echo1", "fencing_token":3, "version":4,
  "question":"选哪个方案？", "options":["A","B"]
}'

# 4) 黑板（Mission 级 KV，X-Expected-Version 做 CAS，不匹配 409）
curl -X PUT localhost:8080/v1/missions/msn_xxx/board/shared/glossary -d '{"term":"储能"}'
curl -X PUT -H 'X-Expected-Version: 0' \
  localhost:8080/v1/missions/msn_xxx/board/shared/glossary -d '{"term":"储能2.0"}'

# 5) Artifact（sha256 内容寻址；元数据在 PG，blob 在 TROOP_BLOB_DIR）
curl -X POST -H "X-Mission-ID: msn_xxx" -H "X-Produced-By: sub_xxx" \
  --data-binary @report.md localhost:8080/v1/artifacts
curl localhost:8080/v1/artifacts/art_xxx/content   # 响应头带 X-Artifact-SHA256
```

## 路线图（详见设计文档 §12.2）

- **M1 ✅**：任务模型 + PG 中心存储 + DAG 编排 + 通用 HTTP Adapter + Capability-First 调度 + 基础事件流
- **M2 ✅**：审批门/决策点 + Agent 决策请求 + 决策超时 + 黑板 + Artifact Store（[计划](docs/plan/M2-hitl.md)）
- **M3 ✅**：调度策略插件化（capability-first/round-robin）+ Deadline/Priority 打分 + WAITING 挂起-唤醒（timer/manual + TTL）+ 检查点续跑（[计划](docs/plan/M3-sched-trigger.md)）
- **M4 ✅**：event/condition 唤醒（水位线语义 + 增量评估 + sweeper 兜底）+ TaskIntent 准入管道（/v1/intents 幂等 create_mission / wake）（[计划](docs/plan/M4-trigger-pipeline.md)）
- **M5 ✅**：CEL 条件内核（cel-go：静态/运行时 cost 双闸 + 静态引用提取 + 逻辑时钟函数）+ scope 三级授权（trigger_scopes 默认收紧，鉴权先于去重）（[计划](docs/plan/M5-cel-scope.md)）
- **M6 ✅**：主子委托协议核心（delegate 准入 + fencing 委托权 + 幂等派生 + depth/fanout 校验 + rework 链式重派 + 子女完成精确唤醒）（[计划](docs/plan/M6-delegate.md)）
- **M7A ✅**：生产可靠性基线（readiness、Agent/租约归属校验、PG CI 门禁与迁移重放）（[计划](docs/plan/M7A-production-baseline.md)）
- **M7B ✅**：Lead 收件箱/计划快照/失联 takeover（[计划](docs/plan/M7B-lead-recovery.md)）
- **M7C ✅**：Mission 预算池、delegate 原子 hold 与完成/失败/取消结算（[计划](docs/plan/M7C-budget-holds.md)）
- **M7D ✅ / M7 完成**：权限包络衰减、lease 级不可变上下文包、最小知情视图与 SHA-256 审计（[计划](docs/plan/M7D-context-permissions.md)）
- **M8（下一阶段）**：A2A/MCP 协议适配、外部身份认证、签名资源 URL 与托管 Adapter
