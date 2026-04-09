"""Structured helpers for canonical SQL formation."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Iterable

from sqlalchemy import text

from app.agent.sql_guardrails import validate_sql

IDENTIFIER_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")
ALLOWED_OPERATORS = {
    "=",
    "!=",
    ">",
    ">=",
    "<",
    "<=",
    "in",
    "not in",
    "like",
    "ilike",
    "is",
    "is not",
    "between",
}


def _is_valid_identifier(value: str) -> bool:
    return (
        bool(value)
        and (value[0].isalpha() or value[0] == "_")
        and all(char in IDENTIFIER_CHARS for char in value)
    )


def _validate_identifier(value: str, label: str) -> str:
    if not _is_valid_identifier(value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def quote_sql_literal(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, Decimal)):
        return str(value)
    if isinstance(value, (date, datetime)):
        return f"'{value.isoformat()}'"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


@dataclass(frozen=True)
class SqlFilter:
    column: str
    operator: str
    value: Any

    def render(self) -> str:
        column = _validate_identifier(self.column, "filter column")
        operator = self.operator.lower().strip()
        if operator not in ALLOWED_OPERATORS:
            raise ValueError(f"Unsupported SQL operator: {self.operator!r}")

        if operator in {"in", "not in"}:
            if not isinstance(self.value, (list, tuple, set)):
                raise ValueError(f"Operator {self.operator!r} requires a sequence value")
            values = list(self.value)
            if not values:
                raise ValueError(f"Operator {self.operator!r} does not allow an empty sequence")
            rendered_values = ", ".join(quote_sql_literal(item) for item in values)
            return f"{column} {operator} ({rendered_values})"

        if operator == "between":
            if not isinstance(self.value, (list, tuple)) or len(self.value) != 2:
                raise ValueError("Operator 'between' requires exactly two values")
            lower_bound, upper_bound = self.value
            return (
                f"{column} between {quote_sql_literal(lower_bound)} "
                f"and {quote_sql_literal(upper_bound)}"
            )

        return f"{column} {operator} {quote_sql_literal(self.value)}"


@dataclass(frozen=True)
class SqlOrderBy:
    column: str
    direction: str = "asc"

    def render(self) -> str:
        column = _validate_identifier(self.column, "order column")
        direction = self.direction.lower().strip()
        if direction not in {"asc", "desc"}:
            raise ValueError(f"Unsupported order direction: {self.direction!r}")
        return f"{column} {direction}"


@dataclass(frozen=True)
class CanonicalQuery:
    table: str
    columns: tuple[str, ...]
    filters: tuple[SqlFilter, ...] = field(default_factory=tuple)
    order_by: tuple[SqlOrderBy, ...] = field(default_factory=tuple)
    limit: int = 100


@dataclass(frozen=True)
class QueryExecutionResult:
    sql: str
    referenced_tables: tuple[str, ...]
    applied_limit: int
    row_count: int
    rows: tuple[dict[str, Any], ...]


def _render_select_columns(columns: Iterable[str]) -> str:
    rendered = []
    for column in columns:
        if column == "*":
            rendered.append(column)
            continue
        rendered.append(_validate_identifier(column, "select column"))
    if not rendered:
        raise ValueError("Canonical queries must select at least one column")
    return ",\n    ".join(rendered)


def render_canonical_sql(query: CanonicalQuery) -> str:
    table = _validate_identifier(query.table, "table")
    if query.limit <= 0:
        raise ValueError("Query limit must be positive")

    sql_parts = [
        "select",
        f"    {_render_select_columns(query.columns)}",
        f"from {table}",
    ]

    if query.filters:
        where_lines = [query.filters[0].render()]
        where_lines.extend(filter_.render() for filter_ in query.filters[1:])
        sql_parts.append("where " + where_lines[0])
        for line in where_lines[1:]:
            sql_parts.append(f"  and {line}")

    if query.order_by:
        order_lines = [order.render() for order in query.order_by]
        sql_parts.append("order by " + order_lines[0])
        for line in order_lines[1:]:
            sql_parts.append(f"       , {line}")

    sql_parts.append(f"limit {query.limit}")
    return "\n".join(sql_parts)


def canonical_query_from_dict(payload: dict[str, Any]) -> CanonicalQuery:
    filters = tuple(
        SqlFilter(
            column=item["column"],
            operator=item["operator"],
            value=item["value"],
        )
        for item in payload.get("filters", [])
    )
    order_by = tuple(
        SqlOrderBy(
            column=item["column"],
            direction=item.get("direction", "asc"),
        )
        for item in payload.get("order_by", [])
    )

    return CanonicalQuery(
        table=payload["table"],
        columns=tuple(payload["columns"]),
        filters=filters,
        order_by=order_by,
        limit=int(payload.get("limit", 100)),
    )


def _json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _result_rows_to_dicts(result) -> tuple[dict[str, Any], ...]:
    rows = []
    for row in result:
        rows.append({key: _json_safe_value(value) for key, value in row._mapping.items()})
    return tuple(rows)


def execute_sql(
    sql: str,
    *,
    default_limit: int = 100,
    max_limit: int = 1000,
) -> QueryExecutionResult:
    from app.data.db.session import SessionLocal

    validation = validate_sql(sql, default_limit=default_limit, max_limit=max_limit)

    with SessionLocal() as db:
        result = db.execute(text(validation.sql))
        rows = _result_rows_to_dicts(result)

    return QueryExecutionResult(
        sql=validation.sql,
        referenced_tables=validation.referenced_tables,
        applied_limit=validation.applied_limit,
        row_count=len(rows),
        rows=rows,
    )


def execute_canonical_query(
    query: CanonicalQuery | dict[str, Any],
    *,
    default_limit: int = 100,
    max_limit: int = 1000,
) -> QueryExecutionResult:
    canonical_query = canonical_query_from_dict(query) if isinstance(query, dict) else query
    sql = render_canonical_sql(canonical_query)
    return execute_sql(sql, default_limit=default_limit, max_limit=max_limit)


def tool_definitions() -> tuple[dict[str, Any], ...]:
    return (
        {
            "name": "run_canonical_query",
            "description": "Render and execute a structured read-only SQL query against approved analytics tables.",
        },
        {
            "name": "run_sql",
            "description": "Validate and execute a raw read-only SQL query against approved analytics tables.",
        },
    )
