# Agent Troop

多智能体协同平台：跨 Agent 平台（OpenClaw / Hermes / 自研等）的任务拆解、中心化状态管理、调度与人在回路协作。

- **设计文档**：[docs/design/multi-agent-collab-platform-design.md](docs/design/multi-agent-collab-platform-design.md)（22 节 + 附录，含架构、调度、触发体系、协作模式、质量/信誉/仿真/计量专题与技术选型 ADR）
- **实现计划**：[docs/plan/](docs/plan/)（按里程碑拆分，当前：[M1 MVP](docs/plan/M1-mvp.md)）

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
# 依赖：Go 1.21+，Docker（PostgreSQL 16 / Redis / MinIO 经 docker-compose 提供）
docker compose up -d postgres
go run ./cmd/troopd            # 控制平面，默认 :8080
go test ./...                  # 交付门槛：全绿
```

## 路线图（详见设计文档 §12.2）

- **M1（当前）**：任务模型 + PG 中心存储 + DAG 编排 + 通用 HTTP Adapter + Capability-First 调度 + 基础事件流
- **M2**：审批门/决策点 + Watcher 订阅 + 黑板 + Artifact Store + 租约重试
- **M3**：策略插件化、Deadline/Cost 调度、检查点续跑、触发体系（挂起-唤醒）
- **M4**：A2A/MCP 适配、OpenClaw/Hermes 托管 Adapter、Agent 生态接入
