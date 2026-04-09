"""Tests for agent orchestration behavior."""
from __future__ import annotations

from types import SimpleNamespace
from types import MethodType

import pytest

from app.agent import orchestrator as orchestrator_module
from app.agent.orchestrator import (
    CrewSQLOrchestrator,
    SQLAnswer,
    SQLCritique,
    SQLDraft,
    TableSelection,
    _coerce_structured_output,
    build_schema_context,
)
from app.mcp.tools.sql_tools import QueryExecutionResult


def test_build_schema_context_includes_relationships() -> None:
    context = build_schema_context(["instruments", "market_metrics_daily"])

    assert "Table: instruments" in context
    assert "Table: market_metrics_daily" in context
    assert "instruments.instrument_id = market_metrics_daily.instrument_id" in context
    assert "derived market-cap screens" in context


def test_orchestrator_repairs_invalid_sql_before_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    orchestrator = CrewSQLOrchestrator(max_repair_attempts=1)
    selection = TableSelection(
        primary_tables=["instruments"],
        supporting_tables=[],
        expected_grain="One row per instrument.",
        reasoning="Need instrument metadata only.",
    )

    orchestrator.select_tables = MethodType(lambda self, question: selection, orchestrator)
    orchestrator.generate_sql = MethodType(
        lambda self, **kwargs: SQLDraft(sql="delete from instruments", reasoning="bad first draft"),
        orchestrator,
    )
    orchestrator.critique_sql = MethodType(
        lambda self, **kwargs: SQLCritique(approved=True, issues=[], rationale="Looks good"),
        orchestrator,
    )
    orchestrator.repair_sql = MethodType(
        lambda self, **kwargs: SQLDraft(
            sql="select instrument_id, symbol from instruments order by instrument_id desc limit 5",
            reasoning="Repaired after validation failure",
        ),
        orchestrator,
    )
    orchestrator.answer_results = MethodType(
        lambda self, **kwargs: SQLAnswer(answer="The latest symbol is AAPL."),
        orchestrator,
    )

    def fake_execute_sql(sql: str, *, default_limit: int, max_limit: int) -> QueryExecutionResult:
        return QueryExecutionResult(
            sql=sql,
            referenced_tables=("instruments",),
            applied_limit=5,
            row_count=1,
            rows=({"instrument_id": 1, "symbol": "AAPL"},),
        )

    monkeypatch.setattr("app.agent.orchestrator.execute_sql", fake_execute_sql)

    result = orchestrator.run("Show me the latest 5 symbols")

    assert result.repair_attempts == 1
    assert result.validated_sql == "select instrument_id, symbol from instruments order by instrument_id desc limit 5"
    assert result.answer.answer == "The latest symbol is AAPL."
    assert result.execution["row_count"] == 1
    assert result.execution["rows"][0]["symbol"] == "AAPL"


def test_orchestrator_rejects_sql_outside_selected_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    orchestrator = CrewSQLOrchestrator(max_repair_attempts=0)
    selection = TableSelection(
        primary_tables=["instruments"],
        supporting_tables=[],
        expected_grain="One row per instrument.",
        reasoning="Need only instruments.",
    )

    orchestrator.select_tables = MethodType(lambda self, question: selection, orchestrator)
    orchestrator.generate_sql = MethodType(
        lambda self, **kwargs: SQLDraft(sql="select series_id from macro_series limit 5"),
        orchestrator,
    )

    with pytest.raises(ValueError, match="outside the selected scope"):
        orchestrator.run("List a few instruments")


def test_orchestrator_builds_gemini_llm_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeLLM:
        def __init__(self, **kwargs):
            captured["llm_kwargs"] = kwargs
            self.model = kwargs["model"]

    class FakeAgent:
        def __init__(self, **kwargs):
            captured["agent_kwargs"] = kwargs
            self.llm = kwargs.get("llm")

    class FakeTask:
        def __init__(self, **kwargs):
            captured["task_kwargs"] = kwargs

    class FakeCrew:
        def __init__(self, **kwargs):
            captured["crew_kwargs"] = kwargs

        def kickoff(self, inputs=None):
            captured["inputs"] = inputs
            return {
                "primary_tables": ["instruments"],
                "supporting_tables": [],
                "expected_grain": "One row per instrument.",
                "reasoning": "Need instrument metadata.",
            }

    fake_crewai = SimpleNamespace(
        LLM=FakeLLM,
        Agent=FakeAgent,
        Task=FakeTask,
        Crew=FakeCrew,
        Process=SimpleNamespace(sequential="sequential"),
    )

    monkeypatch.setattr(orchestrator_module, "_require_crewai", lambda: fake_crewai)
    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")

    orchestrator = CrewSQLOrchestrator(
        model="gemini/gemini-2.0-flash",
        llm_config={"temperature": 0.2},
    )

    result = orchestrator.select_tables("List instrument metadata")

    assert result.primary_tables == ["instruments"]
    assert captured["llm_kwargs"] == {
        "model": "gemini/gemini-2.0-flash",
        "temperature": 0.2,
        "api_key": "test-google-key",
    }
    assert isinstance(captured["agent_kwargs"]["llm"], FakeLLM)
    assert captured["task_kwargs"]["output_pydantic"] is TableSelection


def test_orchestrator_builds_gemma_llm_from_google_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeLLM:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_crewai = SimpleNamespace(LLM=FakeLLM)

    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")

    orchestrator = CrewSQLOrchestrator(model="gemma-3-27b")

    llm = orchestrator._build_llm(fake_crewai)

    assert llm.kwargs == {
        "model": "gemini/gemma-3-27b",
        "api_key": "test-google-key",
    }


def test_orchestrator_adds_vertex_settings_for_gemini(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeLLM:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    fake_crewai = SimpleNamespace(LLM=FakeLLM)

    monkeypatch.setenv("GOOGLE_GENAI_USE_VERTEXAI", "true")
    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "marketlens-dev")
    monkeypatch.setenv("GOOGLE_CLOUD_LOCATION", "us-central1")

    orchestrator = CrewSQLOrchestrator(model="gemini/gemini-2.5-flash")

    llm = orchestrator._build_llm(fake_crewai)

    assert llm.kwargs["project"] == "marketlens-dev"
    assert llm.kwargs["location"] == "us-central1"


def test_orchestrator_reads_default_model_at_init_time(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CREWAI_MODEL", "gemini/gemini-2.5-flash")

    orchestrator = CrewSQLOrchestrator()

    assert orchestrator.model == "gemini/gemini-2.5-flash"


def test_orchestrator_reads_google_model_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CREWAI_MODEL", raising=False)
    monkeypatch.setenv("GOOGLE_MODEL", "gemini/gemini-2.5-flash")

    orchestrator = CrewSQLOrchestrator()

    assert orchestrator.model == "gemini/gemini-2.5-flash"


def test_orchestrator_normalizes_raw_gemma_model_names() -> None:
    orchestrator = CrewSQLOrchestrator(model="gemma-3-27b-it")

    assert orchestrator.model == "gemini/gemma-3-27b-it"


def test_orchestrator_answers_from_execution_results(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeLLM:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeAgent:
        def __init__(self, **kwargs):
            captured["agent_kwargs"] = kwargs

    class FakeTask:
        def __init__(self, **kwargs):
            captured["task_kwargs"] = kwargs

    class FakeCrew:
        def __init__(self, **kwargs):
            captured["crew_kwargs"] = kwargs

        def kickoff(self, inputs=None):
            captured["inputs"] = inputs
            return {"answer": "NVIDIA Corporation has the highest market cap."}

    fake_crewai = SimpleNamespace(
        LLM=FakeLLM,
        Agent=FakeAgent,
        Task=FakeTask,
        Crew=FakeCrew,
        Process=SimpleNamespace(sequential="sequential"),
    )

    monkeypatch.setattr(orchestrator_module, "_require_crewai", lambda: fake_crewai)

    orchestrator = CrewSQLOrchestrator(model="gemini/gemini-2.0-flash")
    execution = {
        "sql": "select name from instruments limit 1",
        "referenced_tables": ["instruments"],
        "applied_limit": 1,
        "row_count": 1,
        "rows": [{"name": "NVIDIA Corporation"}],
    }

    answer = orchestrator.answer_results(
        question="Which company has the highest market cap?",
        sql="select name from instruments limit 1",
        execution=execution,
    )

    assert answer.answer == "NVIDIA Corporation has the highest market cap."
    assert captured["inputs"] == {
        "question": "Which company has the highest market cap?",
        "sql": "select name from instruments limit 1",
        "execution": execution,
    }
    assert "NVIDIA Corporation" in captured["task_kwargs"]["description"]


def test_coerce_structured_output_accepts_fenced_json() -> None:
    raw = """```json
{
  "sql": "select symbol from instruments limit 5",
  "reasoning": "Use the instrument table.",
  "assumptions": []
}
```"""

    result = _coerce_structured_output(raw, SQLDraft)

    assert result.sql == "select symbol from instruments limit 5"
