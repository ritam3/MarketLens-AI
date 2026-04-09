# MarketLens AI

MarketLens AI is an agentic analytics system that turns natural-language financial questions into validated SQL, executes them against a curated market and macroeconomic warehouse, and returns grounded natural-language answers.

The project is built to showcase an end-to-end agent workflow rather than a single prompt-to-SQL demo. Instead of relying on one model call, MarketLens decomposes the problem into planning, generation, critique, repair, execution, and explanation stages. That structure makes the system easier to debug, safer to run against a database, and more convincing as an engineering project on a resume.

## What The Project Does

- Accepts natural-language questions about:
  - company metadata
  - market prices and derived market metrics
  - quarterly fundamentals
  - macroeconomic series and observations
- Selects the minimum relevant tables for the question
- Generates SQL with a role-specific agent
- Validates the SQL with local guardrails before execution
- Critiques the query with a second agent
- Repairs bad SQL iteratively when needed
- Executes only read-only SQL against approved tables
- Converts the result rows into a user-facing natural-language answer

## Why This Project Stands Out

This is not a thin wrapper around an LLM. The core value of the project is the agentic structure:

1. A table-selection agent narrows the schema scope.
2. A SQL-generation agent writes the query.
3. Local guardrails enforce read-only behavior, allowed tables, and row limits.
4. A critique agent checks semantic correctness.
5. A repair agent retries when validation, critique, or execution fails.
6. An answer agent turns result rows into a concise explanation for the user.

That staged design gives you:

- better reliability than single-shot prompt engineering
- explicit safety boundaries before the database is touched
- clear observability through logs and intermediate artifacts
- a workflow that is easy to explain in interviews and on a resume

## Architecture

### Agent Pipeline

The main workflow lives in [`app/agent/orchestrator.py`](./app/agent/orchestrator.py).

At a high level:

1. **Table Selection**
   The system chooses the smallest set of tables needed for the question.

2. **Schema Context Construction**
   It builds a targeted schema description including table purposes, grain, key columns, and join hints.

3. **SQL Generation**
   A specialized SQL agent writes a read-only query.

4. **SQL Guardrails**
   [`app/agent/sql_guardrails.py`](./app/agent/sql_guardrails.py) enforces:
   - single-statement SQL
   - `SELECT` / `WITH` only
   - no comments
   - no mutating keywords
   - only approved tables
   - automatic row limits

5. **Scope Enforcement**
   The orchestrator ensures the generated SQL stays within the tables selected for the question.

6. **SQL Critique**
   A second agent reviews the SQL for semantic issues like wrong table choice, wrong grain, missing joins, or incorrect “latest” logic.

7. **Repair Loop**
   If validation, critique, or execution fails, a repair agent rewrites the SQL using structured feedback.

8. **Execution**
   The validated SQL runs through SQLAlchemy against the configured warehouse.

9. **Natural-Language Answering**
   A final agent converts the result rows into a direct, grounded answer.

### PostgreSQL Warehouse

MarketLens uses PostgreSQL as its analytics warehouse. The schema is defined in [`app/data/db/schema.sql`](./app/data/db/schema.sql) and is designed specifically for agent-driven analytical querying.

Instead of exposing raw API payloads directly to the LLM, the project reshapes external data into a compact warehouse with stable semantics, explicit join paths, and predictable grain. That is important for agentic systems: a smaller, more intentional schema gives the planning and critique agents a much better chance of generating correct SQL.

The warehouse has three conceptual layers:

1. **Entity / reference tables**
   - `instruments`
   - `macro_series`

2. **Fact tables**
   - `market_bars_daily`
   - `fundamentals_quarterly`
   - `macro_observations`

3. **Derived analytics table**
   - `market_metrics_daily`

This layered design lets the agent answer both raw-data questions and derived analytics questions without recomputing everything on every request.

### Warehouse Tables

#### `instruments`

This is the anchor table for company and symbol metadata.

It stores:
- ticker symbol
- company name
- asset type
- exchange
- currency
- country
- sector
- industry
- market-cap classification
- active/inactive status

This table is the main join hub for:
- `market_bars_daily`
- `fundamentals_quarterly`
- `market_metrics_daily`

#### `market_bars_daily`

Stores daily end-of-day market data:
- open
- high
- low
- close
- adjusted close
- volume
- VWAP

Grain:
- one row per instrument per trading day

This is the raw price-history fact table used for trend and history questions.

#### `fundamentals_quarterly`

Stores filing-derived quarterly financials:
- revenue
- gross profit
- operating income
- net income
- EPS
- EBITDA
- free cash flow
- total assets
- total liabilities
- shareholders' equity
- shares outstanding

Grain:
- one row per instrument per fiscal period end

This table is built from SEC EDGAR company facts and normalized into a single quarterly warehouse shape.

#### `market_metrics_daily`

Stores derived daily metrics rather than raw vendor payloads.

It includes:
- market cap
- shares outstanding
- daily return
- 30-day return
- 90-day return
- 20-day rolling volatility
- 20-day average volume
- abnormal volume ratio

Grain:
- one row per instrument per trading day

This table is especially important because it keeps the LLM from having to synthesize rolling analytics from scratch at query time.

#### `macro_series`

Stores metadata for macroeconomic series:
- series id
- title
- units
- frequency
- seasonal adjustment
- notes

#### `macro_observations`

Stores time-series observations for macroeconomic series:
- observation date
- parsed numeric value
- raw source value

Grain:
- one row per macro series per observation date

### Join Structure

The warehouse currently supports these direct relationships:

- `instruments.instrument_id = market_bars_daily.instrument_id`
- `instruments.instrument_id = fundamentals_quarterly.instrument_id`
- `instruments.instrument_id = market_metrics_daily.instrument_id`
- `macro_series.series_id = macro_observations.series_id`

There is intentionally no direct bridge between the macro tables and the company / market tables. That keeps the schema easier for the table-selection and SQL-critique agents to reason about.

### Data Domains

The current warehouse supports these table families:

- `instruments`
- `market_bars_daily`
- `market_metrics_daily`
- `fundamentals_quarterly`
- `macro_series`
- `macro_observations`

Important join paths:

- `instruments.instrument_id = market_bars_daily.instrument_id`
- `instruments.instrument_id = market_metrics_daily.instrument_id`
- `instruments.instrument_id = fundamentals_quarterly.instrument_id`
- `macro_series.series_id = macro_observations.series_id`

## Tech Stack

- **Language:** Python 3.12
- **Agent orchestration:** CrewAI
- **LLM provider:** Google GenAI models via CrewAI
- **Validation / schemas:** Pydantic v2
- **Warehouse:** PostgreSQL
- **Database access:** SQLAlchemy
- **Database driver:** psycopg / psycopg2
- **UI:** Gradio
- **Protocol server:** MCP (Model Context Protocol)
- **Testing:** pytest

## Data Sources And APIs

MarketLens populates the PostgreSQL warehouse from multiple external APIs, each mapped into a specific part of the schema.

### Financial Modeling Prep (FMP)

Client: [`app/data/clients/fmp_client.py`](./app/data/clients/fmp_client.py)

Used for:
- company profile metadata
- historical end-of-day prices

Tables populated:
- `instruments`
- `market_bars_daily`

Examples of the endpoints wrapped in the client:
- `company_profile(symbol)`
- `historical_price_eod_full(symbol, from_date, to_date)`

### FRED API

Client: [`app/data/clients/fred_client.py`](./app/data/clients/fred_client.py)

Used for:
- macro series metadata
- macro observations

Tables populated:
- `macro_series`
- `macro_observations`

Examples of the endpoints wrapped in the client:
- `series(series_id)`
- `observations(series_id, observation_start, observation_end)`

### SEC EDGAR

Client: [`app/data/clients/sec_edgar_client.py`](./app/data/clients/sec_edgar_client.py)

Used for:
- ticker-to-CIK resolution
- company XBRL facts

Tables populated:
- `fundamentals_quarterly`

This ingestion path is more than a raw API dump. It selects specific accounting concepts, aligns them by fiscal period, and normalizes them into a compact quarterly warehouse table the agent can query safely.

## Ingestion And Metric-Building Pipeline

The ingestion logic lives in [`app/data/ingest`](./app/data/ingest).

### `seed_instruments.py`

Seeds and updates a curated symbol universe in `instruments`.

Source:
- FMP company profiles

Adds:
- names
- exchange and country
- sector and industry
- asset type
- market-cap class

### `sync_daily_bars.py`

Loads daily market history into `market_bars_daily`.

Source:
- FMP historical EOD prices

### `sync_fundamentals.py`

Extracts quarterly fundamentals from SEC EDGAR company facts and upserts them into `fundamentals_quarterly`.

Source:
- SEC EDGAR XBRL company facts

Mapped measures include:
- revenue
- gross profit
- operating income
- net income
- EPS
- free cash flow
- assets and liabilities
- shareholders' equity
- shares outstanding

### `sync_macro.py`

Loads macro series metadata and observations into:
- `macro_series`
- `macro_observations`

Source:
- FRED

### `build_metrics.py`

Builds the derived analytics table `market_metrics_daily` from warehouse facts.

This step uses PostgreSQL window functions to compute:
- daily return
- 30-day return
- 90-day return
- rolling 20-day volatility
- rolling 20-day average volume
- abnormal volume ratio
- market cap from price × shares outstanding

### `prune_history.py`

Prunes entity-scoped history so each table remains bounded.

Examples:
- max daily bars per instrument
- max macro observations per series
- max fundamentals rows per instrument
- max metrics rows per instrument

## Repo Structure

```text
app/
  agent/
    orchestrator.py         # multi-stage agent workflow
    prompts.py              # role prompts, schema metadata, join hints
    sql_guardrails.py       # local SQL safety checks
    sql_draft.py            # generated / repaired SQL model
    sql_critique.py         # critique output model
    sql_answer.py           # natural-language answer model
    table_selection.py      # selected-table model
    sql_workflow_result.py  # end-to-end workflow output
  data/
    db/
      session.py            # SQLAlchemy session + engine
      schema.sql            # warehouse DDL
    ingest/                 # ingestion and metric-building scripts
  mcp/
    server.py               # MCP server exposing SQL tools
    tools/
      sql_tools.py          # validated raw SQL + canonical query tools
  ui/
    gradio_app.py           # web interface
  main.py                   # application entrypoint

tests/
  test_agent.py
  test_canonical_sql.py
  test_sql_generation.py
  ...
```

## Setup

### 1. Clone The Repository

```bash
git clone <your-repo-url>
cd marketlens-ai
```

### 2. Create And Activate An Environment

Using Conda:

```bash
conda create -n marketlens python=3.12 -y
conda activate marketlens
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Create a `.env` file in the project root.

Minimum variables:

```env
CREWAI_MODEL=gemma-4-31b-it
GOOGLE_API_KEY=your_google_api_key
DATABASE_URL=your_postgres_connection_string
FMP_API_KEY=your_fmp_api_key
FRED_API_KEY=your_fred_api_key
SEC_EDGAR_USER_AGENT=YourName your_email@example.com
LOG_LEVEL=INFO
```

Optional:

```env
GRADIO_SERVER_NAME=127.0.0.1
GRADIO_SERVER_PORT=7860
GRADIO_SHARE=false
MCP_TRANSPORT=streamable-http
GOOGLE_GENAI_USE_VERTEXAI=false
GOOGLE_CLOUD_PROJECT=
GOOGLE_CLOUD_LOCATION=
```

Notes:

- `CREWAI_MODEL` is the primary model selector.
- `GOOGLE_MODEL` can also be used as a fallback if `CREWAI_MODEL` is not set.
- `DATABASE_URL` must point to a PostgreSQL instance that will host the MarketLens warehouse.
- `SEC_EDGAR_USER_AGENT` should be set to a valid identifier for SEC requests.

## Running The Project

### 0. Initialize The PostgreSQL Schema

Before launching the app, create the warehouse schema:

```bash
psql "$DATABASE_URL" -f app/data/db/schema.sql
```

### 1. Populate The Warehouse

Run the ingestion jobs in order:

```bash
python -m app.data.ingest.seed_instruments
python -m app.data.ingest.sync_daily_bars
python -m app.data.ingest.sync_fundamentals
python -m app.data.ingest.sync_macro
python -m app.data.ingest.build_metrics
```

Why this order matters:

- `seed_instruments` creates the reference universe
- `sync_daily_bars` loads raw price history
- `sync_fundamentals` loads quarterly filing facts
- `sync_macro` builds the macro side of the warehouse
- `build_metrics` depends on the raw warehouse tables to compute derived analytics

### 2. Launch The Gradio App

```bash
python -m app.main
```

Then open the local Gradio URL shown in the terminal.

### 3. Run The MCP Server

```bash
python -m app.mcp.server
```

If you want HTTP transport:

```bash
MCP_TRANSPORT=streamable-http python -m app.mcp.server
```

## Example Questions

- Which company has the highest market cap?
- Show the 10 companies with the largest latest market caps.
- Which stocks have the strongest latest 30-day returns?
- Show the latest 10 companies with market cap, 30-day return, and 20-day volatility, sorted by market cap.
- Show the latest quarterly revenue, net income, and EPS for Apple, Microsoft, and Amazon.
- What macro series are available in the database?
- Show the recent observations for the federal funds rate.

## Safety And Guardrails

The project intentionally combines LLM reasoning with hard local constraints.

The SQL layer:

- rejects non-read-only statements
- rejects comments and multi-statement SQL
- blocks mutating keywords
- blocks unapproved tables
- applies row limits automatically
- validates that generated SQL stays inside the selected table scope

This is important because it demonstrates a practical pattern for production-oriented LLM systems: use agents for planning and reasoning, but use deterministic code for safety and policy enforcement.

## Logging And Debugging

The orchestrator logs each stage of the workflow, including:

- question preview
- selected tables
- generated SQL preview
- validated SQL preview
- critique status
- repair attempts
- row count
- final answer preview

Set:

```env
LOG_LEVEL=INFO
```

or:

```env
LOG_LEVEL=DEBUG
```

to control verbosity.

## Testing

Run the focused test suite:

```bash
pytest -q
```

Useful files:

- [`tests/test_agent.py`](./tests/test_agent.py)
- [`tests/test_canonical_sql.py`](./tests/test_canonical_sql.py)
- [`tests/test_sql_generation.py`](./tests/test_sql_generation.py)

## Future Extensions

- richer MCP tools beyond SQL execution
- better UI polish and result visualizations
- stronger schema introspection and metadata retrieval
- caching and evaluation benchmarks
- support for broader analytics datasets
