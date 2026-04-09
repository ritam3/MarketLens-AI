"""Structured natural-language answer synthesized from query results."""
from __future__ import annotations

from pydantic import BaseModel


class SQLAnswer(BaseModel):
    answer: str
