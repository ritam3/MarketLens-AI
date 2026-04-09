from app.agent.sql_guardrails import enforce_limit, validate_sql
from app.mcp.tools import sql_tools
from app.mcp.tools.sql_tools import (
    CanonicalQuery,
    QueryExecutionResult,
    SqlFilter,
    SqlOrderBy,
    canonical_query_from_dict,
    execute_canonical_query,
    render_canonical_sql,
    tool_definitions,
)


def test_render_canonical_sql_builds_expected_query() -> None:
    sql = render_canonical_sql(
        CanonicalQuery(
            table="market_bars_daily",
            columns=("instrument_id", "price_date", "close"),
            filters=(
                SqlFilter("instrument_id", "=", 42),
                SqlFilter("price_date", ">=", "2025-01-01"),
            ),
            order_by=(SqlOrderBy("price_date", "desc"),),
            limit=50,
        )
    )

    assert sql == (
        "select\n"
        "    instrument_id,\n"
        "    price_date,\n"
        "    close\n"
        "from market_bars_daily\n"
        "where instrument_id = 42\n"
        "  and price_date >= '2025-01-01'\n"
        "order by price_date desc\n"
        "limit 50"
    )


def test_render_canonical_sql_supports_in_filters() -> None:
    sql = render_canonical_sql(
        CanonicalQuery(
            table="instruments",
            columns=("symbol", "asset_type"),
            filters=(SqlFilter("asset_type", "in", ["equity", "etf"]),),
            order_by=(SqlOrderBy("symbol", "asc"),),
            limit=10,
        )
    )

    assert "asset_type in ('equity', 'etf')" in sql


def test_validate_sql_rejects_mutating_queries() -> None:
    try:
        validate_sql("delete from instruments")
    except ValueError as exc:
        assert "Only read-only SELECT/WITH queries are allowed" in str(exc)
    else:
        raise AssertionError("Expected validate_sql to reject mutating SQL")


def test_validate_sql_applies_default_limit_and_tracks_tables() -> None:
    result = validate_sql("select symbol from instruments", default_limit=25, max_limit=100)

    assert result.sql == "select symbol from instruments limit 25"
    assert result.referenced_tables == ("instruments",)
    assert result.applied_limit == 25


def test_enforce_limit_caps_large_limits() -> None:
    sql, applied_limit = enforce_limit("select * from market_bars_daily limit 5000", max_limit=1000)

    assert sql == "select * from market_bars_daily limit 1000"
    assert applied_limit == 1000


def test_canonical_query_from_dict_builds_query_objects() -> None:
    query = canonical_query_from_dict(
        {
            "table": "instruments",
            "columns": ["symbol", "asset_type"],
            "filters": [{"column": "asset_type", "operator": "=", "value": "equity"}],
            "order_by": [{"column": "symbol", "direction": "asc"}],
            "limit": 5,
        }
    )

    assert query == CanonicalQuery(
        table="instruments",
        columns=("symbol", "asset_type"),
        filters=(SqlFilter("asset_type", "=", "equity"),),
        order_by=(SqlOrderBy("symbol", "asc"),),
        limit=5,
    )


def test_execute_canonical_query_delegates_to_execute_sql(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_execute_sql(sql: str, *, default_limit: int = 100, max_limit: int = 1000) -> QueryExecutionResult:
        captured["sql"] = sql
        captured["default_limit"] = default_limit
        captured["max_limit"] = max_limit
        return QueryExecutionResult(
            sql=sql,
            referenced_tables=("instruments",),
            applied_limit=5,
            row_count=1,
            rows=({"symbol": "AAPL"},),
        )

    monkeypatch.setattr(sql_tools, "execute_sql", fake_execute_sql)

    result = execute_canonical_query(
        {
            "table": "instruments",
            "columns": ["symbol"],
            "filters": [{"column": "symbol", "operator": "=", "value": "AAPL"}],
            "limit": 5,
        }
    )

    assert "from instruments" in str(captured["sql"])
    assert captured["default_limit"] == 100
    assert captured["max_limit"] == 1000
    assert result.row_count == 1
    assert result.rows == ({"symbol": "AAPL"},)


def test_tool_definitions_expose_sql_tool_names() -> None:
    definitions = tool_definitions()

    assert definitions[0]["name"] == "run_canonical_query"
    assert definitions[1]["name"] == "run_sql"
