# Agent Troop Python SDK（M2 规划，M1 仅占位）

薄协议封装：`receive(task) → your_agent.run() → report(result)`，
隐藏租约 / 心跳 / 幂等 / fencing 细节（设计 §9）。M1 阶段 Agent 直接按
[adapters/http-echo](../../adapters/http-echo/README.md) 的 HTTP 协议接入即可。
