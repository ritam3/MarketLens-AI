"""Agent package."""
from __future__ import annotations

from importlib import import_module
from typing import Any


def __getattr__(name: str) -> Any:
    if name in {"CrewSQLOrchestrator", "build_schema_context"}:
        module = import_module("app.agent.orchestrator")
        return getattr(module, name)
    if name == "SQLCritique":
        module = import_module("app.agent.sql_critique")
        return getattr(module, name)
    if name == "SQLAnswer":
        module = import_module("app.agent.sql_answer")
        return getattr(module, name)
    if name == "SQLDraft":
        module = import_module("app.agent.sql_draft")
        return getattr(module, name)
    if name == "SQLWorkflowResult":
        module = import_module("app.agent.sql_workflow_result")
        return getattr(module, name)
    if name == "TableSelection":
        module = import_module("app.agent.table_selection")
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "CrewSQLOrchestrator",
    "SQLAnswer",
    "SQLCritique",
    "SQLDraft",
    "SQLWorkflowResult",
    "TableSelection",
    "build_schema_context",
]
