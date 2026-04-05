-- Active PostgreSQL schema for the current MarketLens ingestion pipeline.

create or replace function set_updated_at()
returns trigger
language plpgsql
as $$
begin
    new.updated_at = now();
    return new;
end;
$$;

create table if not exists instruments (
    instrument_id bigserial primary key,
    symbol text not null unique,
    name text,
    asset_type text not null check (asset_type in ('equity', 'etf', 'index')),
    exchange text,
    currency text,
    country text,
    sector text,
    industry text,
    market_cap_class text check (
        market_cap_class in ('mega', 'large', 'mid', 'small', 'micro')
        or market_cap_class is null
    ),
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists idx_instruments_symbol on instruments(symbol);
create index if not exists idx_instruments_sector on instruments(sector);
create index if not exists idx_instruments_industry on instruments(industry);
create index if not exists idx_instruments_asset_type on instruments(asset_type);

drop trigger if exists trg_instruments_set_updated_at on instruments;
create trigger trg_instruments_set_updated_at
before update on instruments
for each row
execute function set_updated_at();

create table if not exists market_bars_daily (
    instrument_id bigint not null references instruments(instrument_id) on delete cascade,
    price_date date not null,
    open numeric(18, 6),
    high numeric(18, 6),
    low numeric(18, 6),
    close numeric(18, 6),
    adjusted_close numeric(18, 6),
    volume bigint,
    vwap numeric(18, 6),
    source text not null default 'unknown',
    inserted_at timestamptz not null default now(),
    primary key (instrument_id, price_date)
);

create index if not exists idx_market_bars_daily_instrument_date
on market_bars_daily(instrument_id, price_date desc);

create index if not exists idx_market_bars_daily_price_date
on market_bars_daily(price_date desc);

create table if not exists macro_series (
    series_id text primary key,
    title text not null,
    units text,
    frequency text,
    seasonal_adjustment text,
    notes text,
    source text not null default 'fred',
    last_updated timestamptz,
    inserted_at timestamptz not null default now()
);

create table if not exists macro_observations (
    series_id text not null references macro_series(series_id) on delete cascade,
    obs_date date not null,
    value numeric(18, 6),
    raw_value text,
    inserted_at timestamptz not null default now(),
    primary key (series_id, obs_date)
);

create index if not exists idx_macro_observations_series_date
on macro_observations(series_id, obs_date desc);

create index if not exists idx_macro_observations_obs_date
on macro_observations(obs_date desc);

create table if not exists fundamentals_quarterly (
    instrument_id bigint not null references instruments(instrument_id) on delete cascade,
    fiscal_period_end date not null,
    fiscal_year int,
    fiscal_quarter int check (fiscal_quarter between 1 and 4 or fiscal_quarter is null),
    revenue numeric(20, 2),
    gross_profit numeric(20, 2),
    operating_income numeric(20, 2),
    net_income numeric(20, 2),
    eps numeric(18, 6),
    ebitda numeric(20, 2),
    free_cash_flow numeric(20, 2),
    total_assets numeric(20, 2),
    total_liabilities numeric(20, 2),
    shareholders_equity numeric(20, 2),
    shares_outstanding numeric(20, 2),
    source text not null default 'unknown',
    inserted_at timestamptz not null default now(),
    primary key (instrument_id, fiscal_period_end)
);

create index if not exists idx_fundamentals_quarterly_instrument_period
on fundamentals_quarterly(instrument_id, fiscal_period_end desc);

create table if not exists market_metrics_daily (
    instrument_id bigint not null references instruments(instrument_id) on delete cascade,
    metric_date date not null,
    market_cap numeric(20, 2),
    shares_outstanding numeric(20, 2),
    daily_return_pct numeric(12, 6),
    return_30d_pct numeric(12, 6),
    return_90d_pct numeric(12, 6),
    volatility_20d_pct numeric(12, 6),
    avg_20d_volume numeric(20, 2),
    abnormal_volume_ratio numeric(12, 6),
    source text not null default 'derived',
    inserted_at timestamptz not null default now(),
    primary key (instrument_id, metric_date)
);

create index if not exists idx_market_metrics_daily_instrument_date
on market_metrics_daily(instrument_id, metric_date desc);

create index if not exists idx_market_metrics_daily_metric_date
on market_metrics_daily(metric_date desc);
