"""MCP server entry point."""
from __future__ import annotations

import os
from typing import Any

from app.mcp.tools.sql_tools import execute_canonical_query, execute_sql
from app.utils.logger import configure_logging

SERVER_NAME = "MarketLens AI"
SERVER_INSTRUCTIONS = (
    "Use these tools to query the MarketLens analytics database with validated, "
    "read-only SQL. Prefer the canonical query tool when the request fits its "
    "structured shape."
)


def _require_fastmcp():
    try:
        from mcp.server.fastmcp import FastMCP
    except ImportError as exc:
        raise RuntimeError(
            "The MCP SDK is not installed. Add it to the environment with "
            "`pip install -r requirements.txt`."
        ) from exc
    return FastMCP


def _serialize_execution_result(result) -> dict[str, Any]:
    return {
        "sql": result.sql,
        "referenced_tables": list(result.referenced_tables),
        "applied_limit": result.applied_limit,
        "row_count": result.row_count,
        "rows": [dict(row) for row in result.rows],
    }


def build_server(*, fastmcp_cls: type[Any] | None = None) -> Any:
    fastmcp_cls = fastmcp_cls or _require_fastmcp()
    server = fastmcp_cls(
        SERVER_NAME,
        instructions=SERVER_INSTRUCTIONS,
        json_response=True,
    )

    @server.tool()
    def run_sql(
        sql: str,
        default_limit: int = 100,
        max_limit: int = 1000,
    ) -> dict[str, Any]:
        """Validate and execute a read-only SQL query against approved tables."""
        result = execute_sql(
            sql,
            default_limit=default_limit,
            max_limit=max_limit,
        )
        return _serialize_execution_result(result)

    @server.tool()
    def run_canonical_query(
        query: dict[str, Any],
        default_limit: int = 100,
        max_limit: int = 1000,
    ) -> dict[str, Any]:
        """Render and execute a structured read-only SQL query against approved tables."""
        result = execute_canonical_query(
            query,
            default_limit=default_limit,
            max_limit=max_limit,
        )
        return _serialize_execution_result(result)

    return server


def main() -> None:
    configure_logging()
    transport = os.getenv("MCP_TRANSPORT")
    server = build_server()
    if transport:
        server.run(transport=transport)
        return
    server.run()


if __name__ == "__main__":
    main()
