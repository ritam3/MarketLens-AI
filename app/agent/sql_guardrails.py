"""SQL validation and canonical safety guardrails."""
from __future__ import annotations

import re
from dataclasses import dataclass

ALLOWED_TABLES = {
    "instruments",
    "market_bars_daily",
    "macro_series",
    "macro_observations",
    "fundamentals_quarterly",
    "market_metrics_daily",
}

FORBIDDEN_KEYWORDS = {
    "insert",
    "update",
    "delete",
    "drop",
    "alter",
    "truncate",
    "create",
    "grant",
    "revoke",
    "commit",
    "rollback",
    "vacuum",
    "copy",
}

TABLE_PATTERN = re.compile(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)\b", re.IGNORECASE)
LIMIT_PATTERN = re.compile(r"\blimit\s+(\d+)\b", re.IGNORECASE)


@dataclass(frozen=True)
class SQLValidationResult:
    sql: str
    referenced_tables: tuple[str, ...]
    applied_limit: int


def normalize_sql(sql: str) -> str:
    normalized = sql.strip()
    if normalized.endswith(";"):
        normalized = normalized[:-1]
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def validate_sql(sql: str, *, default_limit: int = 50, max_limit: int = 100) -> SQLValidationResult:
    normalized = normalize_sql(sql)

    if not normalized:
        raise ValueError("SQL is empty")
    if "--" in normalized or "/*" in normalized or "*/" in normalized:
        raise ValueError("SQL comments are not allowed")
    if ";" in normalized:
        raise ValueError("Only a single SQL statement is allowed")

    first_keyword = normalized.split(" ", 1)[0].lower()
    if first_keyword not in {"select", "with"}:
        raise ValueError("Only read-only SELECT/WITH queries are allowed")

    lowered = normalized.lower()
    for keyword in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{keyword}\b", lowered):
            raise ValueError(f"Forbidden SQL keyword detected: {keyword}")

    referenced_tables = tuple(dict.fromkeys(match.group(1) for match in TABLE_PATTERN.finditer(normalized)))
    unknown_tables = [table for table in referenced_tables if table not in ALLOWED_TABLES]
    if unknown_tables:
        raise ValueError(f"Query references unsupported tables: {', '.join(unknown_tables)}")

    limited_sql, applied_limit = enforce_limit(normalized, default_limit=default_limit, max_limit=max_limit)
    return SQLValidationResult(
        sql=limited_sql,
        referenced_tables=referenced_tables,
        applied_limit=applied_limit,
    )


def enforce_limit(sql: str, *, default_limit: int = 100, max_limit: int = 1000) -> tuple[str, int]:
    normalized = normalize_sql(sql)
    match = LIMIT_PATTERN.search(normalized)

    if match is None:
        applied_limit = min(default_limit, max_limit)
        return f"{normalized} limit {applied_limit}", applied_limit

    requested_limit = int(match.group(1))
    applied_limit = min(requested_limit, max_limit)
    limited_sql = LIMIT_PATTERN.sub(f"limit {applied_limit}", normalized, count=1)
    return limited_sql, applied_limit
