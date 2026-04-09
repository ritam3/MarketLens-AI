"""Structured result model for an end-to-end SQL workflow run."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel

from app.agent.sql_answer import SQLAnswer
from app.agent.sql_critique import SQLCritique
from app.agent.table_selection import TableSelection


class SQLWorkflowResult(BaseModel):
    question: str
    table_selection: TableSelection
    schema_context: str
    generated_sql: str
    validated_sql: str
    critique: SQLCritique
    answer: SQLAnswer
    execution: dict[str, Any]
    repair_attempts: int = 0
