from pathlib import Path


SCHEMA_PATH = Path(__file__).resolve().parents[1] / "app" / "data" / "db" / "schema.sql"


def test_schema_defines_core_tables() -> None:
    schema = SCHEMA_PATH.read_text()

    expected_tables = (
        "create table if not exists instruments",
        "create table if not exists market_bars_daily",
        "create table if not exists market_bars_intraday",
        "create table if not exists macro_series",
        "create table if not exists macro_observations",
        "create table if not exists market_events",
        "create table if not exists fundamentals_quarterly",
        "create table if not exists market_metrics_daily",
    )

    for table_ddl in expected_tables:
        assert table_ddl in schema


def test_schema_defines_key_indexes_and_trigger() -> None:
    schema = SCHEMA_PATH.read_text()

    expected_statements = (
        "create index if not exists idx_instruments_symbol on instruments(symbol);",
        "create index if not exists idx_market_events_payload_gin",
        "create trigger trg_instruments_set_updated_at",
        "execute function set_updated_at();",
    )

    for statement in expected_statements:
        assert statement in schema
