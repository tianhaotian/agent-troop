"""Small, dependency-free Agent Troop protocol client."""

from .client import AgentWorker, Client, ProtocolError

__all__ = ["AgentWorker", "Client", "ProtocolError"]
__version__ = "0.8.0"
