# M10 产品化能力

M10 将 M1–M9 的控制平面能力包装成可部署、可校准、可运营的产品闭环。实现保持 provider-neutral，默认配置可在本地零凭据运行，生产环境通过标准 OIDC、S3-compatible 和 OpenAI-compatible 协议接入。

## 交付范围

- **确定性测试床**：按 seed 运行 harness / shadow / load / chaos 场景，输出成功率、延迟、租约过期、恢复时间、成本偏差、负载 Gini 与稳定 state hash；相同输入必须字节级可回放。
- **质量治理**：金丝雀样本校准 Verifier，并提供 Artifact 质量申诉的创建、查询和人工裁决；所有裁决写审计事件，推翻时写入纠偏信誉信号。
- **Marketplace**：按 skill、platform、health、最低信誉发现 Agent，返回可解释的能力与信誉排序。
- **原生 Adapter**：managed adapter 支持 generic、Hermes 与 OpenClaw profiles；后两者使用 `/v1/responses` 兼容面并采集 token usage。
- **生产集成**：RS256 OIDC/JWKS 验证（含 key rotation refresh）、S3-compatible SigV4 Artifact 存储（含 SSE-KMS headers）、权威 token metering gateway。
- **产品 Console 与 SDK**：Console 提供 Overview、Marketplace、Simulation、Quality Appeals、Usage 工作区；REST 与 Python SDK 同步覆盖。

## API

- `POST /v1/simulations/run`
- `POST /v1/canaries/evaluate`
- `GET /v1/marketplace/agents`
- `POST /v1/artifacts/{id}/appeals`
- `GET /v1/appeals`
- `POST /v1/appeals/{id}/resolve`
- `POST /v1/metering/gateway`

## 验收标准

1. 同一 simulation 请求重复执行得到相同 `state_hash` 与报告。
2. 金丝雀命中/偏离期望结果均可解释，并以低权重更新 Verifier 信誉。
3. 申诉只有 `pending` 可裁决，重复裁决冲突；推翻产生审计事件和纠偏信号。
4. Marketplace 结果稳定排序且健康、技能、平台、信誉过滤有效。
5. OIDC 拒绝错误 issuer/audience/alg/过期 token，未知 `kid` 触发一次 JWKS 刷新。
6. S3 请求使用 AWS Signature V4，读写保持内容寻址，KMS 配置产生正确 headers。
7. 权威计量拒绝负数与空 mission，token 输入/输出分别定价、幂等落账。
8. Go test/vet/race、Python SDK 测试、PostgreSQL migrations 与 E2E 全部通过。

## 非目标

- 不绑定特定云厂商计费、KMS SDK 或 IdP 管理 API。
- 不替代 OpenClaw 的 canonical WebSocket RPC；本 Adapter 使用其公开的 OpenAI-compatible HTTP compatibility surface。
- 不在控制平面保存 OIDC 客户端密钥或 S3 secret；全部由部署环境注入。
