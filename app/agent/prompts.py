"""Prompt builders and schema semantics for the SQL agent workflow."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class TableMetadata:
    name: str
    purpose: str
    grain: str
    key_columns: tuple[str, ...]
    semantic_notes: tuple[str, ...]
    join_hints: tuple[str, ...]
    useful_for: tuple[str, ...]


TABLE_CATALOG: dict[str, TableMetadata] = {
    "instruments": TableMetadata(
        name="instruments",
        purpose="Master instrument lookup table for symbols, names, and market metadata.",
        grain="One row per instrument.",
        key_columns=(
            "instrument_id",
            "symbol",
            "name",
            "asset_type",
            "exchange",
            "currency",
            "country",
            "sector",
            "industry",
            "market_cap_class",
            "is_active",
        ),
        semantic_notes=(
            "Use this table to resolve ticker symbols, company names, sectors, industries, or active status.",
            "Join other market and fundamentals tables through instrument_id.",
        ),
        join_hints=(
            "Join market_bars_daily on instruments.instrument_id = market_bars_daily.instrument_id.",
            "Join fundamentals_quarterly on instruments.instrument_id = fundamentals_quarterly.instrument_id.",
            "Join market_metrics_daily on instruments.instrument_id = market_metrics_daily.instrument_id.",
        ),
        useful_for=(
            "symbol lookup",
            "company metadata",
            "sector and industry filters",
            "active instrument filtering",
        ),
    ),
    "market_bars_daily": TableMetadata(
        name="market_bars_daily",
        purpose="Raw daily OHLCV price history by instrument and trading date.",
        grain="One row per instrument per trading day.",
        key_columns=(
            "instrument_id",
            "price_date",
            "open",
            "high",
            "low",
            "close",
            "adjusted_close",
            "volume",
            "vwap",
            "source",
        ),
        semantic_notes=(
            "adjusted_close may be null when the upstream source does not provide it.",
            "Use adjusted_close for split-aware performance when available; otherwise fall back to close.",
            "vwap may be null.",
        ),
        join_hints=(
            "Usually join to instruments for symbol filters or labels.",
        ),
        useful_for=(
            "daily price history",
            "raw OHLCV analysis",
            "date-based price trends",
        ),
    ),
    "macro_series": TableMetadata(
        name="macro_series",
        purpose="Metadata for macroeconomic series sourced from FRED.",
        grain="One row per macro series.",
        key_columns=(
            "series_id",
            "title",
            "units",
            "frequency",
            "seasonal_adjustment",
            "notes",
            "source",
            "last_updated",
        ),
        semantic_notes=(
            "Use this table to discover or label FRED series before joining observations.",
        ),
        join_hints=(
            "Join macro_observations on macro_series.series_id = macro_observations.series_id.",
        ),
        useful_for=(
            "macro series lookup",
            "macro metadata",
        ),
    ),
    "macro_observations": TableMetadata(
        name="macro_observations",
        purpose="Historical observed values for macroeconomic series.",
        grain="One row per macro series per observation date.",
        key_columns=(
            "series_id",
            "obs_date",
            "value",
            "raw_value",
        ),
        semantic_notes=(
            "Join to macro_series when the question references macro titles or units rather than series_id.",
            "raw_value preserves source text; numeric analysis should use value.",
        ),
        join_hints=(
            "Join to macro_series for series metadata.",
        ),
        useful_for=(
            "macro trend analysis",
            "inflation, rates, and economic time series",
        ),
    ),
    "fundamentals_quarterly": TableMetadata(
        name="fundamentals_quarterly",
        purpose="Quarterly company fundamentals merged from SEC filing facts.",
        grain="One row per instrument per fiscal period end.",
        key_columns=(
            "instrument_id",
            "fiscal_period_end",
            "fiscal_year",
            "fiscal_quarter",
            "revenue",
            "gross_profit",
            "operating_income",
            "net_income",
            "eps",
            "ebitda",
            "free_cash_flow",
            "total_assets",
            "total_liabilities",
            "shareholders_equity",
            "shares_outstanding",
            "source",
        ),
        semantic_notes=(
            "Data is quarterly and filing-derived, so comparisons should respect fiscal quarter timing.",
            "shares_outstanding may be missing for some filings or periods.",
        ),
        join_hints=(
            "Usually join to instruments for ticker filters or company labels.",
        ),
        useful_for=(
            "quarterly revenue and EPS trends",
            "balance sheet analysis",
            "cash flow analysis",
            "shares outstanding from filings",
        ),
    ),
    "market_metrics_daily": TableMetadata(
        name="market_metrics_daily",
        purpose="Derived daily analytics built from market bars and available fundamentals.",
        grain="One row per instrument per trading day.",
        key_columns=(
            "instrument_id",
            "metric_date",
            "market_cap",
            "shares_outstanding",
            "daily_return_pct",
            "return_30d_pct",
            "return_90d_pct",
            "volatility_20d_pct",
            "avg_20d_volume",
            "abnormal_volume_ratio",
            "source",
        ),
        semantic_notes=(
            "Prefer this table when the user asks for rolling returns, volatility, abnormal volume, or derived market cap.",
            "market_cap and shares_outstanding may be null when fundamentals data is missing.",
        ),
        join_hints=(
            "Usually join to instruments for symbol filters or labels.",
        ),
        useful_for=(
            "30d and 90d return questions",
            "volatility questions",
            "abnormal volume questions",
            "derived market-cap screens",
        ),
    ),
}


TABLE_SELECTOR_ROLE = "SQL Table Selector"
TABLE_SELECTOR_GOAL = "Choose the smallest set of database tables needed to answer a market or macro question."
TABLE_SELECTOR_BACKSTORY = (
    "You are a disciplined analytics planner. You know the data model well and prefer the most direct, "
    "semantically correct source of truth instead of selecting every plausible table."
)

SQL_GENERATOR_ROLE = "Postgres SQL Analyst"
SQL_GENERATOR_GOAL = "Write accurate, compact Postgres read-only SQL using only the approved tables and schema context."
SQL_GENERATOR_BACKSTORY = (
    "You are a senior analytics engineer who writes clear SQL, respects data grain, and avoids unnecessary complexity."
)

SQL_CRITIC_ROLE = "SQL Critic"
SQL_CRITIC_GOAL = "Review whether a SQL query answers the user's question correctly and flag likely semantic mistakes."
SQL_CRITIC_BACKSTORY = (
    "You are a careful reviewer focused on correctness, data grain, and using the right source table for the question."
)

SQL_REPAIR_ROLE = "SQL Repair Specialist"
SQL_REPAIR_GOAL = "Repair SQL using concrete validation or execution feedback while keeping the query read-only and minimal."
SQL_REPAIR_BACKSTORY = (
    "You are excellent at making precise fixes to SQL without changing the user's requested intent."
)

SQL_ANSWER_ROLE = "Analytics Answer Writer"
SQL_ANSWER_GOAL = "Turn validated SQL results into a concise, accurate natural-language answer grounded in the returned rows."
SQL_ANSWER_BACKSTORY = (
    "You are a careful analyst who answers questions directly from the query result, avoids inventing facts, "
    "and clearly states when the result is empty or partial."
)


def format_table_catalog(table_names: Iterable[str] | None = None) -> str:
    names = tuple(table_names) if table_names is not None else tuple(TABLE_CATALOG)
    blocks = []
    for name in names:
        metadata = TABLE_CATALOG[name]
        blocks.append(
            "\n".join(
                (
                    f"Table: {metadata.name}",
                    f"Purpose: {metadata.purpose}",
                    f"Grain: {metadata.grain}",
                    f"Columns: {', '.join(metadata.key_columns)}",
                    "Semantic notes:",
                    *(f"- {note}" for note in metadata.semantic_notes),
                    "Join hints:",
                    *(f"- {hint}" for hint in metadata.join_hints),
                    "Useful for:",
                    *(f"- {item}" for item in metadata.useful_for),
                )
            )
        )
    return "\n\n".join(blocks)


def build_table_selection_prompt(question: str) -> str:
    return f"""
Choose the minimum sufficient database tables needed to answer the user's question.

Rules:
- Prefer the most direct source of truth.
- Use `market_metrics_daily` instead of recomputing rolling returns or volatility from raw bars when possible.
- Include `instruments` when the query needs ticker, company name, sector, industry, or active-status resolution.
- Do not select tables that are not necessary.
- The selected tables must come only from the catalog below.

Database catalog:
{format_table_catalog()}

User question:
{question}
""".strip()


def build_sql_generation_prompt(
    *,
    question: str,
    schema_context: str,
    selection_summary: str,
) -> str:
    return f"""
Write one Postgres query that answers the user's question.

Rules:
- Return a single read-only `SELECT` or `WITH` query.
- Use only the selected tables described below.
- Respect the table grain and semantic notes.
- Prefer explicit joins.
- Do not include comments or a trailing semicolon.
- Keep the query as simple as possible while still answering the question.

Selected table summary:
{selection_summary}

Schema context:
{schema_context}

User question:
{question}
""".strip()


def build_sql_critic_prompt(
    *,
    question: str,
    schema_context: str,
    selection_summary: str,
    sql: str,
) -> str:
    return f"""
Review whether the SQL answers the user's question correctly.

Focus on:
- wrong table choice
- wrong data grain
- missing join needed for symbol or label resolution
- using raw values when a derived table is the better source
- returning history when the user asked for latest values
- missing aggregation, ordering, or filtering

If the query is correct, approve it.
If there is a likely issue, explain it clearly and suggest a corrected query if the fix is straightforward.

Selected table summary:
{selection_summary}

Schema context:
{schema_context}

User question:
{question}

SQL to review:
{sql}
""".strip()


def build_sql_repair_prompt(
    *,
    question: str,
    schema_context: str,
    selection_summary: str,
    sql: str,
    feedback: str,
) -> str:
    return f"""
Repair the SQL query using the feedback below.

Rules:
- Keep it as a single read-only `SELECT` or `WITH` query.
- Use only the selected tables.
- Do not include comments or a trailing semicolon.
- Preserve the user's original intent.

Selected table summary:
{selection_summary}

Schema context:
{schema_context}

User question:
{question}

Current SQL:
{sql}

Feedback:
{feedback}
""".strip()


def build_sql_answer_prompt(
    *,
    question: str,
    sql: str,
    execution_summary: str,
) -> str:
    return f"""
Answer the user's question using only the SQL result below.

Rules:
- Respond in natural language, not as JSON or a table.
- Ground every claim in the returned rows.
- If the result is empty, say that no matching rows were found.
- Mention important numbers or labels directly when they answer the question.
- Do not mention internal workflow details unless they materially help the user.

User question:
{question}

Validated SQL:
{sql}

Execution result:
{execution_summary}
""".strip()
