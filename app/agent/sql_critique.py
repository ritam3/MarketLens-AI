"""Structured output model for SQL review feedback."""
from __future__ import annotations

from pydantic import BaseModel, Field


class SQLCritique(BaseModel):
    approved: bool
    issues: list[str] = Field(default_factory=list)
    suggested_sql: str | None = None
    rationale: str = ""
