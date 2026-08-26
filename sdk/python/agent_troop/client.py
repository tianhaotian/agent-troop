from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional


class ProtocolError(RuntimeError):
    def __init__(self, status: int, message: str):
        super().__init__(f"Agent Troop status {status}: {message}")
        self.status = status
        self.message = message


class Client:
    def __init__(self, base_url: str, token: str = "", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self.timeout = timeout

    def request(self, method: str, path: str, body: Optional[Any] = None,
                headers: Optional[Dict[str, str]] = None) -> Any:
        encoded = None if body is None else json.dumps(body).encode("utf-8")
        request_headers = {"Accept": "application/json"}
        if encoded is not None:
            request_headers["Content-Type"] = "application/json"
        if self.token:
            request_headers["Authorization"] = f"Bearer {self.token}"
        request_headers.update(headers or {})
        request = urllib.request.Request(
            self.base_url + path, data=encoded, headers=request_headers, method=method
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                data = response.read()
                return None if not data else json.loads(data)
        except urllib.error.HTTPError as error:
            data = error.read().decode("utf-8", errors="replace")
            try:
                message = json.loads(data).get("error", data)
            except json.JSONDecodeError:
                message = data
            raise ProtocolError(error.code, message) from error

    def register_agent(self, agent_id: str, platform: str, skills: List[str], *,
                       auth_subject: str = "", trigger_scopes: Optional[List[str]] = None,
                       max_concurrency: int = 1) -> Dict[str, Any]:
        return self.request("POST", "/v1/agents/register", {
            "id": agent_id, "name": agent_id, "platform": platform,
            "auth_subject": auth_subject,
            "capabilities": [{"skill": skill, "level": 0.9} for skill in skills],
            "trigger_scopes": trigger_scopes or [], "max_concurrency": max_concurrency,
        })

    def heartbeat(self, agent_id: str) -> Dict[str, Any]:
        return self.request("POST", f"/v1/agents/{agent_id}/heartbeat", {})

    def offers(self, agent_id: str) -> List[Dict[str, Any]]:
        return self.request("GET", f"/v1/agents/{agent_id}/offers")["offers"]

    def accept(self, agent_id: str, offer: Dict[str, Any]) -> Dict[str, Any]:
        return self.request("POST", f"/v1/leases/{offer['lease_id']}/accept", {
            "agent_id": agent_id, "fencing_token": offer["fencing_token"],
            "subtask_version": offer["subtask"]["version"],
        })

    def start(self, agent_id: str, offer: Dict[str, Any], version: int) -> Dict[str, Any]:
        return self.request("POST", f"/v1/subtasks/{offer['subtask']['id']}/start", {
            "agent_id": agent_id, "fencing_token": offer["fencing_token"], "version": version,
        })

    def progress(self, agent_id: str, offer: Dict[str, Any], checkpoint: Any = None) -> Dict[str, Any]:
        body = {"agent_id": agent_id, "lease_id": offer["lease_id"],
                "fencing_token": offer["fencing_token"]}
        if checkpoint is not None:
            body["checkpoint"] = checkpoint
        return self.request("POST", f"/v1/subtasks/{offer['subtask']['id']}/progress", body)

    def complete(self, agent_id: str, offer: Dict[str, Any], version: int,
                 result_ref: str, usage_tokens: int = 0,
                 idempotency_key: str = "") -> Dict[str, Any]:
        subtask = offer["subtask"]
        key = idempotency_key or f"sdk-{subtask['id']}-{subtask.get('attempt', 0)}"
        return self.request("POST", f"/v1/subtasks/{subtask['id']}/complete", {
            "agent_id": agent_id, "fencing_token": offer["fencing_token"],
            "version": version, "idempotency_key": key,
            "result_ref": result_ref, "usage_tokens": usage_tokens,
        })

    def fail(self, agent_id: str, offer: Dict[str, Any], version: int,
             reason: str) -> Dict[str, Any]:
        return self.request("POST", f"/v1/subtasks/{offer['subtask']['id']}/fail", {
            "agent_id": agent_id, "fencing_token": offer["fencing_token"],
            "version": version, "reason": reason,
        })

    def signed_artifact_url(self, artifact_id: str, *, agent_id: str = "",
                            lease_id: str = "", expires_in: int = 300) -> str:
        result = self.request("POST",
                              f"/v1/artifacts/{urllib.parse.quote(artifact_id)}/signed-url",
                              {"agent_id": agent_id, "lease_id": lease_id,
                               "expires_in": expires_in})
        return urllib.parse.urljoin(self.base_url + "/", result["url"])

    def verify_artifact(self, artifact_id: str, *, score: float, confidence: float,
                        verdict: str, failure_class: str = "", rubric: str = "",
                        context_hash: str = "", verifier_agent_id: str = "",
                        layers: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return self.request("POST",
                            f"/v1/artifacts/{urllib.parse.quote(artifact_id)}/verify", {
            "score": score, "confidence": confidence, "verdict": verdict,
            "failure_class": failure_class, "rubric": rubric,
            "context_hash": context_hash, "verifier_agent_id": verifier_agent_id,
            "layers": layers or {},
        })

    def artifact_quality(self, artifact_id: str) -> Dict[str, Any]:
        return self.request("GET",
                            f"/v1/artifacts/{urllib.parse.quote(artifact_id)}/quality")

    def reputation(self, agent_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/v1/agents/{urllib.parse.quote(agent_id)}/reputation")

    def mission_usage(self, mission_id: str) -> Dict[str, Any]:
        return self.request("GET", f"/v1/missions/{urllib.parse.quote(mission_id)}/usage")

    def observability_snapshot(self) -> Dict[str, Any]:
        return self.request("GET", "/v1/observability/snapshot")

    def run_simulation(self, **scenario: Any) -> Dict[str, Any]:
        return self.request("POST", "/v1/simulations/run", scenario)

    def marketplace(self, *, skill: str = "", platform: str = "",
                    min_reputation: float = 0.0, healthy: bool = True) -> List[Dict[str, Any]]:
        query = urllib.parse.urlencode({"skill": skill, "platform": platform,
                                        "min_reputation": min_reputation,
                                        "healthy": str(healthy).lower()})
        return self.request("GET", f"/v1/marketplace/agents?{query}")["agents"]

    def evaluate_canary(self, **sample: Any) -> Dict[str, Any]:
        return self.request("POST", "/v1/canaries/evaluate", sample)

    def create_appeal(self, artifact_id: str, appellant_id: str, reason: str,
                      evidence_refs: Optional[List[str]] = None) -> Dict[str, Any]:
        return self.request("POST", f"/v1/artifacts/{urllib.parse.quote(artifact_id)}/appeals", {
            "appellant_id": appellant_id, "reason": reason,
            "evidence_refs": evidence_refs or [],
        })

    def appeals(self, *, mission_id: str = "", pending: bool = False) -> List[Dict[str, Any]]:
        query = urllib.parse.urlencode({"mission_id": mission_id, "pending": str(pending).lower()})
        return self.request("GET", f"/v1/appeals?{query}")["appeals"]

    def resolve_appeal(self, appeal_id: str, status: str, resolution: str,
                       reviewer_id: str) -> Dict[str, Any]:
        return self.request("POST", f"/v1/appeals/{urllib.parse.quote(appeal_id)}/resolve", {
            "status": status, "resolution": resolution, "reviewer_id": reviewer_id,
        })

    def record_gateway_usage(self, record_id: str, mission_id: str, *,
                             input_tokens: int, output_tokens: int,
                             subtask_id: str = "", agent_id: str = "",
                             provider: str = "", model: str = "") -> Dict[str, Any]:
        return self.request("POST", "/v1/metering/gateway", {
            "id": record_id, "mission_id": mission_id, "subtask_id": subtask_id,
            "agent_id": agent_id, "provider": provider, "model": model,
            "input_tokens": input_tokens, "output_tokens": output_tokens,
        })


@dataclass
class AgentWorker:
    client: Client
    agent_id: str

    def run_once(self, handler: Callable[[Dict[str, Any]], Dict[str, Any]]) -> int:
        completed = 0
        for offer in self.client.offers(self.agent_id):
            accepted = self.client.accept(self.agent_id, offer)
            started = self.client.start(self.agent_id, offer, accepted["version"])
            try:
                result = handler(offer["context_package"])
                self.client.complete(
                    self.agent_id, offer, started["version"],
                    result.get("result_ref", f"sdk://{self.agent_id}/{offer['subtask']['id']}"),
                    int(result.get("usage_tokens", 0)),
                )
                completed += 1
            except Exception as error:
                self.client.fail(self.agent_id, offer, started["version"], str(error))
        return completed
