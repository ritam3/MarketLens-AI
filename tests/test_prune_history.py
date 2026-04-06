from app.data.ingest.prune_history import (
    DAILY_BARS_PRUNE_TARGET,
    MACRO_OBSERVATIONS_PRUNE_TARGET,
    build_entity_prune_query,
)


def test_daily_bars_prune_target_uses_instrument_id_and_row_limit() -> None:
    assert DAILY_BARS_PRUNE_TARGET.entity_column == "instrument_id"
    assert DAILY_BARS_PRUNE_TARGET.order_column == "price_date"
    assert DAILY_BARS_PRUNE_TARGET.keep_rows == 750


def test_macro_observations_prune_target_uses_series_id() -> None:
    assert MACRO_OBSERVATIONS_PRUNE_TARGET.entity_column == "series_id"
    assert MACRO_OBSERVATIONS_PRUNE_TARGET.order_column == "obs_date"


def test_build_entity_prune_query_contains_entity_scoped_delete() -> None:
    sql = str(build_entity_prune_query(DAILY_BARS_PRUNE_TARGET))

    assert "delete from market_bars_daily" in sql
    assert "where instrument_id = :entity_value" in sql
    assert "limit :keep_rows" in sql
