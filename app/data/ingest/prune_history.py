"""Entity-scoped pruning helpers for bounded history tables."""
from __future__ import annotations

import os
from dataclasses import dataclass

from sqlalchemy import text


@dataclass(frozen=True)
class EntityPruneTarget:
    table_name: str
    entity_column: str
    order_column: str
    keep_rows: int


def _env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


DAILY_BARS_PRUNE_TARGET = EntityPruneTarget(
    table_name="market_bars_daily",
    entity_column="instrument_id",
    order_column="price_date",
    keep_rows=_env_int("MAX_DAILY_BARS_PER_INSTRUMENT", 750),
)

MACRO_OBSERVATIONS_PRUNE_TARGET = EntityPruneTarget(
    table_name="macro_observations",
    entity_column="series_id",
    order_column="obs_date",
    keep_rows=_env_int("MAX_MACRO_OBSERVATIONS_PER_SERIES", 750),
)

FUNDAMENTALS_PRUNE_TARGET = EntityPruneTarget(
    table_name="fundamentals_quarterly",
    entity_column="instrument_id",
    order_column="fiscal_period_end",
    keep_rows=_env_int("MAX_FUNDAMENTALS_ROWS_PER_INSTRUMENT", 16),
)

METRICS_PRUNE_TARGET = EntityPruneTarget(
    table_name="market_metrics_daily",
    entity_column="instrument_id",
    order_column="metric_date",
    keep_rows=_env_int("MAX_METRICS_ROWS_PER_INSTRUMENT", 750),
)


def build_entity_prune_query(target: EntityPruneTarget):
    return text(
        f"""
        with deleted as (
            delete from {target.table_name}
            where {target.entity_column} = :entity_value
              and {target.order_column} not in (
                  select {target.order_column}
                  from {target.table_name}
                  where {target.entity_column} = :entity_value
                  order by {target.order_column} desc
                  limit :keep_rows
              )
            returning 1
        )
        select count(*) as deleted_count
        from deleted
        """
    )


def prune_entity_history(db, target: EntityPruneTarget, entity_value) -> int:
    if target.keep_rows <= 0:
        return 0

    return db.execute(
        build_entity_prune_query(target),
        {
            "entity_value": entity_value,
            "keep_rows": target.keep_rows,
        },
    ).scalar_one()
