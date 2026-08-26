# Agent Troop Python SDK

零第三方依赖的 Agent/业务客户端，封装 Bearer、注册、offer、租约、fencing、心跳、
幂等完成、Artifact 签名 URL、Verifier 质量提交、信誉和权威用量查询。

```python
from agent_troop import AgentWorker, Client

client = Client("http://localhost:8080", token="...")
worker = AgentWorker(client, "python-1")

def run(context_package):
    return {"result_ref": "artifact://answer", "usage_tokens": 1200}

while True:
    worker.run_once(run)
```

Verifier/运营接口：

```python
client.verify_artifact("art_...", score=0.92, confidence=0.88,
                       verdict="accepted", rubric="rubric://report/v3",
                       verifier_agent_id="agt_judge")
quality = client.artifact_quality("art_...")
reputation = client.reputation("agt_writer")
usage = client.mission_usage("msn_...")
snapshot = client.observability_snapshot()
```

开发安装与测试：

```bash
python3 -m pip install -e sdk/python
python3 -m unittest discover -s sdk/python/tests
```
