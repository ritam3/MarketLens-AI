# MarketLens AI

MarketLens AI is an agentic analytics system that answers natural-language financial questions by planning over a curated PostgreSQL warehouse, generating validated SQL, executing it, and returning a grounded natural-language response.

This project is built to demonstrate a production-style multi-agent workflow rather than a single prompt-to-SQL demo. The core idea is simple: let agents handle planning and reasoning, but use deterministic code for database safety, schema control, execution, and observability.

## Demo video


https://github.com/user-attachments/assets/0abc9cea-7a35-4a89-999e-91dfcfce6aa7




## Why This Project Matters

- It uses a staged agent pipeline instead of one-shot prompt engineering.
- It queries a real PostgreSQL warehouse backed by external market and macro APIs.
- It enforces local SQL guardrails before execution.
- It critiques and repairs generated SQL when needed.
- It turns result rows back into a user-facing answer.

It combines:
- agent orchestration
- warehouse design
- API ingestion
- SQL validation
- UI + MCP integration

## Agentic Workflow

The main workflow lives in [`app/agent/orchestrator.py`](./app/agent/orchestrator.py).

For each user question, the system runs these stages:

1. **Table selection**
   A planning agent chooses the minimum set of relevant tables.

2. **Schema context building**
   The system constructs targeted schema context with table purpose, grain, and join hints.

3. **SQL generation**
   A SQL agent writes a read-only query.

4. **Guardrails**
   [`app/agent/sql_guardrails.py`](./app/agent/sql_guardrails.py) enforces:
   - single-statement SQL
   - `SELECT` / `WITH` only
   - no comments
   - no mutating keywords
   - approved tables only
   - automatic limit enforcement

5. **Critique**
   A second agent reviews the SQL for semantic correctness.

6. **Repair loop**
   If validation, critique, or execution fails, a repair agent rewrites the query using structured feedback.

7. **Execution**
   Validated SQL executes against PostgreSQL through SQLAlchemy.

8. **Answer synthesis**
   A final agent turns the returned rows into a concise natural-language answer.

## PostgreSQL Warehouse

The warehouse schema is defined in [`app/data/db/schema.sql`](./app/data/db/schema.sql).

Instead of exposing raw API payloads directly to the model, MarketLens reshapes upstream data into a compact analytics warehouse with clear grain and predictable joins. That is important for the agent system because smaller, semantically stable schemas produce better SQL.

### Core Tables

- `instruments`
  Master entity table for symbols, company names, exchange, country, sector, industry, and active status.

- `market_bars_daily`
  Raw daily OHLCV market history.

- `fundamentals_quarterly`
  Normalized quarterly company fundamentals built from SEC filing facts.

- `market_metrics_daily`
  Derived daily analytics such as market cap, rolling returns, volatility, average volume, and abnormal volume.

- `macro_series`
  Metadata for macroeconomic time series.

- `macro_observations`
  Historical macro observations by series and date.

### Main Join Paths

- `instruments.instrument_id = market_bars_daily.instrument_id`
- `instruments.instrument_id = fundamentals_quarterly.instrument_id`
- `instruments.instrument_id = market_metrics_daily.instrument_id`
- `macro_series.series_id = macro_observations.series_id`

There is intentionally no direct bridge between macro tables and company tables. That keeps the schema easier for the planning and critique agents to reason about.

## Data Sources And Ingestion

The warehouse is populated from three upstream APIs.

### Financial Modeling Prep (FMP)

Client: [`app/data/clients/fmp_client.py`](./app/data/clients/fmp_client.py)

Used for:
- company profile metadata
- historical daily price data

Populates:
- `instruments`
- `market_bars_daily`

### FRED API

Client: [`app/data/clients/fred_client.py`](./app/data/clients/fred_client.py)

Used for:
- macro series metadata
- macro observations

Populates:
- `macro_series`
- `macro_observations`

### SEC EDGAR

Client: [`app/data/clients/sec_edgar_client.py`](./app/data/clients/sec_edgar_client.py)

Used for:
- ticker-to-CIK lookup
- XBRL company facts

Populates:
- `fundamentals_quarterly`

### Derived Metrics Pipeline

[`app/data/ingest/build_metrics.py`](./app/data/ingest/build_metrics.py) computes `market_metrics_daily` from raw bars and shares data using PostgreSQL window functions.

Derived metrics include:
- daily return
- 30-day return
- 90-day return
- 20-day rolling volatility
- 20-day average volume
- abnormal volume ratio
- market cap

### Ingestion Order

Run ingestion in this order:

```bash
python -m app.data.ingest.seed_instruments
python -m app.data.ingest.sync_daily_bars
python -m app.data.ingest.sync_fundamentals
python -m app.data.ingest.sync_macro
python -m app.data.ingest.build_metrics
```

Why this order:
- instruments create the entity backbone
- bars and fundamentals populate raw facts
- macro builds a separate macro subgraph
- derived metrics depend on the raw warehouse tables

## Tech Stack

- Python 3.12
- CrewAI for multi-agent orchestration
- Google GenAI models via CrewAI
- Pydantic v2 for structured models
- PostgreSQL as the warehouse
- SQLAlchemy + psycopg for database access
- Gradio for the UI
- MCP for tool/server integration
- pytest for testing

## Setup

### 1. Clone And Create Environment

```bash
git clone <your-repo-url>
cd marketlens-ai
conda create -n marketlens python=3.12 -y
conda activate marketlens
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure `.env`

Create a `.env` file in the project root:

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
GOOGLE_MODEL=
GRADIO_SERVER_NAME=127.0.0.1
GRADIO_SERVER_PORT=7860
GRADIO_SHARE=false
MCP_TRANSPORT=streamable-http
GOOGLE_GENAI_USE_VERTEXAI=false
GOOGLE_CLOUD_PROJECT=
GOOGLE_CLOUD_LOCATION=
```

Notes:
- `DATABASE_URL` should point to PostgreSQL.
- `SEC_EDGAR_USER_AGENT` should be a valid identifier for SEC requests.
- `CREWAI_MODEL` is the primary model selector.

### 4. Initialize PostgreSQL Schema

```bash
psql "$DATABASE_URL" -f app/data/db/schema.sql
```

### 5. Populate The Warehouse

```bash
python -m app.data.ingest.seed_instruments
python -m app.data.ingest.sync_daily_bars
python -m app.data.ingest.sync_fundamentals
python -m app.data.ingest.sync_macro
python -m app.data.ingest.build_metrics
```

## Running The Project

### Launch The Gradio App

```bash
python -m app.main
```

### Launch The MCP Server

```bash
python -m app.mcp.server
```

For HTTP transport:

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

## Safety And Observability

MarketLens intentionally combines agent reasoning with hard local controls.

Safety:
- read-only SQL only
- approved tables only
- single statement only
- automatic limits
- selected-table scope enforcement

Observability:
- orchestrator stage logging
- SQL validation logs
- critique and repair logs
- execution summaries

## Testing

```bash
pytest -q
```

Key tests:
- [`tests/test_agent.py`](./tests/test_agent.py)
- [`tests/test_canonical_sql.py`](./tests/test_canonical_sql.py)
- [`tests/test_sql_generation.py`](./tests/test_sql_generation.py)
