"""Minimal Streamable-HTTP MCP client.

Some corroborator sources have no credential fskb can hold directly -- the
authenticated access lives in a house MCP server (e.g. Horizon via
``horizon-mcp``). This client reaches those servers over their HTTP transport so
the monitor can pull independent evidence without duplicating credentials.

Deliberately tiny: initialize a session, call one tool, read structuredContent.
"""

from __future__ import annotations

import json
import ssl
import urllib.request
from typing import Any, Dict, Optional


class MCPError(RuntimeError):
    pass


class MCPClient:
    def __init__(self, url: str, token: str, verify_ssl: bool = True, timeout: int = 30):
        self.url = url
        self.token = token
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self._sid: Optional[str] = None

    def _ctx(self):  # noqa: ANN202
        if self.verify_ssl:
            return None
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self._sid:
            headers["mcp-session-id"] = self._sid
        req = urllib.request.Request(
            self.url, data=json.dumps(payload).encode(), headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=self.timeout, context=self._ctx()) as resp:
            self._sid = resp.headers.get("mcp-session-id") or self._sid
            raw = resp.read().decode()
        return _parse(raw)

    def initialize(self) -> None:
        self._post(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "fskb-monitor", "version": "0"},
                },
            }
        )
        try:  # notification; harmless if the server ignores it
            self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        except Exception:
            pass

    def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if self._sid is None:
            self.initialize()
        resp = self._post(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments or {}},
            }
        )
        if "error" in resp:
            raise MCPError(str(resp["error"]))
        result = resp.get("result", {}) or {}
        if result.get("structuredContent") is not None:
            return result["structuredContent"]
        for c in result.get("content", []) or []:
            if c.get("type") == "text":
                try:
                    return json.loads(c["text"])
                except Exception:
                    return {"text": c["text"]}
        return result


def _parse(raw: str) -> Dict[str, Any]:
    """Accept either a JSON body or a Server-Sent-Events frame."""

    text = raw.lstrip()
    if text.startswith("{"):
        return json.loads(raw)
    for line in raw.splitlines():
        if line.startswith("data:"):
            return json.loads(line[5:].strip())
    return {}
