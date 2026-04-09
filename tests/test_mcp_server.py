"""Tests for MCP server registration and entrypoint behavior."""
from __future__ import annotations

from typing import Any

from app.mcp import server as mcp_server


class FakeFastMCP:
    def __init__(self, name: str, **kwargs: Any) -> None:
        self.name = name
        self.kwargs = kwargs
        self.tools: list[dict[str, Any]] = []
        self.run_calls: list[dict[str, Any]] = []

    def tool(self):
        def decorator(func):
            self.tools.append(
                {
                    "name": func.__name__,
                    "doc": func.__doc__,
                    "func": func,
                }
            )
            return func

        return decorator

    def run(self, **kwargs: Any) -> None:
        self.run_calls.append(kwargs)


def test_build_server_registers_sql_tools() -> None:
    server = mcp_server.build_server(fastmcp_cls=FakeFastMCP)

    assert server.name == "MarketLens AI"
    assert server.kwargs["json_response"] is True
    assert "validated, read-only SQL" in server.kwargs["instructions"]
    assert [tool["name"] for tool in server.tools] == [
        "run_sql",
        "run_canonical_query",
    ]


def test_main_uses_configured_transport(monkeypatch) -> None:
    fake_server = FakeFastMCP("test")

    monkeypatch.setenv("MCP_TRANSPORT", "streamable-http")
    monkeypatch.setattr(mcp_server, "build_server", lambda: fake_server)

    mcp_server.main()

    assert fake_server.run_calls == [{"transport": "streamable-http"}]


def test_main_defaults_to_direct_run(monkeypatch) -> None:
    fake_server = FakeFastMCP("test")

    monkeypatch.delenv("MCP_TRANSPORT", raising=False)
    monkeypatch.setattr(mcp_server, "build_server", lambda: fake_server)

    mcp_server.main()

    assert fake_server.run_calls == [{}]
