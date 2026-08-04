# Agent Troop

多智能体协同平台：跨 Agent 平台（OpenClaw / Hermes / 自研等）的任务拆解、中心化状态管理、调度与人在回路协作。

- **设计文档**：[docs/design/multi-agent-collab-platform-design.md](docs/design/multi-agent-collab-platform-design.md)（22 节 + 附录，含架构、调度、触发体系、协作模式、质量/信誉/仿真/计量专题与技术选型 ADR）
- **实现计划**：[docs/plan/](docs/plan/)（按里程碑拆分，已交付：[M1 MVP](docs/plan/M1-mvp.md)、[M2 人在回路](docs/plan/M2-hitl.md)、[M3 调度与挂起-唤醒](docs/plan/M3-sched-trigger.md)、[M4 触发管道](docs/plan/M4-trigger-pipeline.md)）
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
                                               -f migrations/0004_m4.sql
TROOP_PG_DSN=postgres://troop:troop@localhost:5432/troop go run ./cmd/troopd

# 可选环境变量：TROOP_SCHEDULER=capability-first|round-robin（放置策略，M3）
#               TROOP_BLOB_DIR=./data/artifacts（Artifact blob 目录）
```

## M3 API 示例（挂起-唤醒 / 检查点续跑）

```bash
# 1) progress 心跳携带检查点（fencing 校验；≤64KB 透明载荷，崩溃/换 Agent 后续跑用）
curl -X POST localhost:8080/v1/subtasks/sub_xxx/progress -d '{
  "lease_id":"lea_xxx", "fencing_token":3, "checkpoint":{"step":3,"partial":"..."}
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
- **M5（当前）**：CEL 条件内核（cel-go，cost 上限 + 静态引用提取）、scope 三级授权、A2A/MCP 适配、OpenClaw/Hermes 托管 Adapter、Agent 生态接入
