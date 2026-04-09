"""Structured output model for the table-selection stage."""
from __future__ import annotations

from pydantic import BaseModel, Field, model_validator

from app.agent.prompts import TABLE_CATALOG


class TableSelection(BaseModel):
    primary_tables: list[str] = Field(default_factory=list)
    supporting_tables: list[str] = Field(default_factory=list)
    expected_grain: str = ""
    reasoning: str = ""

    @model_validator(mode="after")
    def validate_tables(self) -> "TableSelection":
        unknown_tables = [
            table
            for table in self.all_tables
            if table not in TABLE_CATALOG
        ]
        if unknown_tables:
            raise ValueError(f"Unsupported tables selected: {', '.join(unknown_tables)}")
        return self

    @property
    def all_tables(self) -> list[str]:
        return list(dict.fromkeys([*self.primary_tables, *self.supporting_tables]))

    def summary(self) -> str:
        return "\n".join(
            (
                f"Primary tables: {', '.join(self.primary_tables) or 'none'}",
                f"Supporting tables: {', '.join(self.supporting_tables) or 'none'}",
                f"Expected grain: {self.expected_grain or 'unspecified'}",
                f"Reasoning: {self.reasoning or 'none provided'}",
            )
        )
