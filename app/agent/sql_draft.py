"""Structured output model for generated or repaired SQL."""
from __future__ import annotations

from pydantic import BaseModel, Field


class SQLDraft(BaseModel):
    sql: str
    reasoning: str = ""
    assumptions: list[str] = Field(default_factory=list)
