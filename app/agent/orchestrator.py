"""CrewAI-based workflow for schema-aware SQL generation and execution."""
from __future__ import annotations

import importlib
import json
import logging
import os
import time
from typing import Any, Iterable
from uuid import uuid4

from pydantic import BaseModel

from app.agent.prompts import (
    SQL_ANSWER_BACKSTORY,
    SQL_ANSWER_GOAL,
    SQL_ANSWER_ROLE,
    SQL_CRITIC_BACKSTORY,
    SQL_CRITIC_GOAL,
    SQL_CRITIC_ROLE,
    SQL_GENERATOR_BACKSTORY,
    SQL_GENERATOR_GOAL,
    SQL_GENERATOR_ROLE,
    SQL_REPAIR_BACKSTORY,
    SQL_REPAIR_GOAL,
    SQL_REPAIR_ROLE,
    TABLE_CATALOG,
    TABLE_SELECTOR_BACKSTORY,
    TABLE_SELECTOR_GOAL,
    TABLE_SELECTOR_ROLE,
    build_sql_answer_prompt,
    build_sql_critic_prompt,
    build_sql_generation_prompt,
    build_sql_repair_prompt,
    build_table_selection_prompt,
    format_table_catalog,
)
from app.agent.sql_answer import SQLAnswer
from app.agent.sql_critique import SQLCritique
from app.agent.sql_draft import SQLDraft
from app.agent.sql_guardrails import validate_sql
from app.agent.sql_workflow_result import SQLWorkflowResult
from app.agent.table_selection import TableSelection
from app.mcp.tools.sql_tools import QueryExecutionResult, execute_sql

logger = logging.getLogger(__name__)

def _default_crewai_model() -> str | None:
    return os.getenv("CREWAI_MODEL") or os.getenv("GOOGLE_MODEL")


DEFAULT_CREWAI_MODEL = _default_crewai_model()


def _normalize_model_name(model: str) -> str:
    normalized = model.strip()
    lowered = normalized.lower()
    if lowered.startswith("gemma-"):
        return f"gemini/{normalized}"
    return normalized


def build_schema_context(table_names: Iterable[str]) -> str:
    normalized_names = list(dict.fromkeys(table_names))
    if not normalized_names:
        raise ValueError("At least one table must be selected")

    unknown_tables = [table for table in normalized_names if table not in TABLE_CATALOG]
    if unknown_tables:
        raise ValueError(f"Unsupported tables requested: {', '.join(unknown_tables)}")

    relationship_lines = []
    selected_set = set(normalized_names)
    if {"instruments", "market_bars_daily"} <= selected_set:
        relationship_lines.append(
            "- instruments.instrument_id = market_bars_daily.instrument_id"
        )
    if {"instruments", "fundamentals_quarterly"} <= selected_set:
        relationship_lines.append(
            "- instruments.instrument_id = fundamentals_quarterly.instrument_id"
        )
    if {"instruments", "market_metrics_daily"} <= selected_set:
        relationship_lines.append(
            "- instruments.instrument_id = market_metrics_daily.instrument_id"
        )
    if {"macro_series", "macro_observations"} <= selected_set:
        relationship_lines.append(
            "- macro_series.series_id = macro_observations.series_id"
        )

    relationship_block = (
        "Relationships:\n" + "\n".join(relationship_lines)
        if relationship_lines
        else "Relationships:\n- No direct join hints required for the selected tables."
    )
    return f"{format_table_catalog(normalized_names)}\n\n{relationship_block}"


def _require_crewai():
    try:
        return importlib.import_module("crewai")
    except ImportError as exc:
        raise RuntimeError(
            "CrewAI is not installed. Add it to the environment with `pip install -r requirements.txt`."
        ) from exc


def _is_google_genai_model(model: str) -> bool:
    normalized = model.strip().lower()
    return (
        normalized.startswith("gemini/")
        or normalized.startswith("google/")
        or normalized.startswith("gemma-")
    )


def _json_string_candidates(raw: str) -> list[str]:
    candidates: list[str] = []
    stripped = raw.strip()
    if stripped:
        candidates.append(stripped)

    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if len(lines) >= 3 and lines[-1].strip() == "```":
            fenced_body = "\n".join(lines[1:-1]).strip()
            if fenced_body:
                candidates.append(fenced_body)

    start = stripped.find("{")
    end = stripped.rfind("}")
    if start != -1 and end != -1 and end > start:
        embedded_object = stripped[start : end + 1].strip()
        if embedded_object:
            candidates.append(embedded_object)

    deduped: list[str] = []
    for candidate in candidates:
        if candidate not in deduped:
            deduped.append(candidate)
    return deduped


def _coerce_structured_output(result: Any, output_model: type[BaseModel]) -> BaseModel:
    if isinstance(result, output_model):
        return result

    pydantic_output = getattr(result, "pydantic", None)
    if isinstance(pydantic_output, output_model):
        return pydantic_output
    if pydantic_output is not None:
        return output_model.model_validate(pydantic_output)

    json_dict = getattr(result, "json_dict", None)
    if json_dict is not None:
        return output_model.model_validate(json_dict)

    raw = getattr(result, "raw", result)
    if isinstance(raw, str):
        last_error: Exception | None = None
        for candidate in _json_string_candidates(raw):
            try:
                return output_model.model_validate_json(candidate)
            except ValueError as exc:
                last_error = exc
                try:
                    return output_model.model_validate(json.loads(candidate))
                except ValueError as json_exc:
                    last_error = json_exc
        if last_error is not None:
            logger.warning(
                "Failed to coerce structured output for model=%s",
                output_model.__name__,
            )
            raise last_error

    return output_model.model_validate(raw)


class CrewSQLOrchestrator:
    """Run the SQL workflow as staged CrewAI tasks around local validation and execution."""

    def __init__(
        self,
        *,
        model: str | None = None,
        llm_config: dict[str, Any] | None = None,
        verbose: bool = False,
        default_limit: int = 50,
        max_limit: int = 100,
        max_repair_attempts: int = 2,
    ) -> None:
        resolved_model = _default_crewai_model() if model is None else model
        self.model = _normalize_model_name(resolved_model) if resolved_model else None
        self.llm_config = dict(llm_config or {})
        self.verbose = verbose
        self.default_limit = default_limit
        self.max_limit = max_limit
        self.max_repair_attempts = max_repair_attempts

    def _question_preview(self, question: str, limit: int = 160) -> str:
        compact = " ".join(question.split())
        if len(compact) <= limit:
            return compact
        return compact[: limit - 3] + "..."

    def _sql_preview(self, sql: str, limit: int = 180) -> str:
        compact = " ".join(sql.split())
        if len(compact) <= limit:
            return compact
        return compact[: limit - 3] + "..."

    def _build_llm(self, crewai: Any) -> Any | None:
        if not self.model:
            logger.info("CrewAI LLM not configured; provider defaults will apply")
            return None

        llm_kwargs: dict[str, Any] = {"model": self.model, **self.llm_config}
        if _is_google_genai_model(self.model):
            api_key = os.getenv("GOOGLE_API_KEY") or os.getenv("GEMINI_API_KEY")
            if api_key and "api_key" not in llm_kwargs:
                llm_kwargs["api_key"] = api_key

            use_vertex = (os.getenv("GOOGLE_GENAI_USE_VERTEXAI") or "").lower() == "true"
            if use_vertex:
                project = os.getenv("GOOGLE_CLOUD_PROJECT")
                location = os.getenv("GOOGLE_CLOUD_LOCATION")
                if project and "project" not in llm_kwargs:
                    llm_kwargs["project"] = project
                if location and "location" not in llm_kwargs:
                    llm_kwargs["location"] = location

        logger.info(
            "Building CrewAI LLM model=%s google_genai=%s vertex=%s",
            llm_kwargs["model"],
            _is_google_genai_model(self.model),
            "project" in llm_kwargs,
        )
        return crewai.LLM(**llm_kwargs)

    def _run_structured_stage(
        self,
        *,
        stage_name: str,
        role: str,
        goal: str,
        backstory: str,
        description: str,
        expected_output: str,
        output_model: type[BaseModel],
        inputs: dict[str, Any] | None = None,
    ) -> BaseModel:
        stage_start = time.perf_counter()
        logger.info(
            "Starting stage=%s role=%s output_model=%s provider_side_structured_output=%s",
            stage_name,
            role,
            output_model.__name__,
            True,
        )
        crewai = _require_crewai()
        agent_kwargs: dict[str, Any] = {
            "role": role,
            "goal": goal,
            "backstory": backstory,
            "verbose": self.verbose,
            "allow_delegation": False,
        }
        llm = self._build_llm(crewai)
        if llm is not None:
            agent_kwargs["llm"] = llm

        agent = crewai.Agent(**agent_kwargs)
        task = crewai.Task(
            description=description,
            expected_output=expected_output,
            agent=agent,
            output_pydantic=output_model,
        )
        crew = crewai.Crew(
            agents=[agent],
            tasks=[task],
            process=crewai.Process.sequential,
            verbose=self.verbose,
        )
        try:
            result = crew.kickoff(inputs=inputs or {})
            coerced = _coerce_structured_output(result, output_model)
            logger.info(
                "Completed stage=%s output_model=%s duration_ms=%s",
                stage_name,
                output_model.__name__,
                round((time.perf_counter() - stage_start) * 1000, 1),
            )
            return coerced
        except Exception:
            logger.exception(
                "Stage failed stage=%s output_model=%s duration_ms=%s",
                stage_name,
                output_model.__name__,
                round((time.perf_counter() - stage_start) * 1000, 1),
            )
            raise

    def select_tables(self, question: str) -> TableSelection:
        return self._run_structured_stage(
            stage_name="select_tables",
            role=TABLE_SELECTOR_ROLE,
            goal=TABLE_SELECTOR_GOAL,
            backstory=TABLE_SELECTOR_BACKSTORY,
            description=build_table_selection_prompt(question),
            expected_output=(
                "Return a JSON object with primary_tables, supporting_tables, expected_grain, and reasoning."
            ),
            output_model=TableSelection,
            inputs={"question": question},
        )

    def generate_sql(
        self,
        *,
        question: str,
        schema_context: str,
        selection: TableSelection,
    ) -> SQLDraft:
        return self._run_structured_stage(
            stage_name="generate_sql",
            role=SQL_GENERATOR_ROLE,
            goal=SQL_GENERATOR_GOAL,
            backstory=SQL_GENERATOR_BACKSTORY,
            description=build_sql_generation_prompt(
                question=question,
                schema_context=schema_context,
                selection_summary=selection.summary(),
            ),
            expected_output="Return a JSON object with sql, reasoning, and assumptions.",
            output_model=SQLDraft,
            inputs={
                "question": question,
                "schema_context": schema_context,
                "selection_summary": selection.summary(),
            },
        )

    def critique_sql(
        self,
        *,
        question: str,
        schema_context: str,
        selection: TableSelection,
        sql: str,
    ) -> SQLCritique:
        return self._run_structured_stage(
            stage_name="critique_sql",
            role=SQL_CRITIC_ROLE,
            goal=SQL_CRITIC_GOAL,
            backstory=SQL_CRITIC_BACKSTORY,
            description=build_sql_critic_prompt(
                question=question,
                schema_context=schema_context,
                selection_summary=selection.summary(),
                sql=sql,
            ),
            expected_output=(
                "Return a JSON object with approved, issues, suggested_sql, and rationale."
            ),
            output_model=SQLCritique,
            inputs={
                "question": question,
                "schema_context": schema_context,
                "selection_summary": selection.summary(),
                "sql": sql,
            },
        )

    def repair_sql(
        self,
        *,
        question: str,
        schema_context: str,
        selection: TableSelection,
        sql: str,
        feedback: str,
    ) -> SQLDraft:
        return self._run_structured_stage(
            stage_name="repair_sql",
            role=SQL_REPAIR_ROLE,
            goal=SQL_REPAIR_GOAL,
            backstory=SQL_REPAIR_BACKSTORY,
            description=build_sql_repair_prompt(
                question=question,
                schema_context=schema_context,
                selection_summary=selection.summary(),
                sql=sql,
                feedback=feedback,
            ),
            expected_output="Return a JSON object with sql, reasoning, and assumptions.",
            output_model=SQLDraft,
            inputs={
                "question": question,
                "schema_context": schema_context,
                "selection_summary": selection.summary(),
                "sql": sql,
                "feedback": feedback,
            },
        )

    def _assert_selected_tables(self, referenced_tables: Iterable[str], selection: TableSelection) -> None:
        allowed_tables = set(selection.all_tables)
        unexpected_tables = [table for table in referenced_tables if table not in allowed_tables]
        if unexpected_tables:
            raise ValueError(
                "SQL used tables outside the selected scope: "
                + ", ".join(unexpected_tables)
            )

    def _serialize_execution(self, execution: QueryExecutionResult) -> dict[str, Any]:
        return {
            "sql": execution.sql,
            "referenced_tables": list(execution.referenced_tables),
            "applied_limit": execution.applied_limit,
            "row_count": execution.row_count,
            "rows": [dict(row) for row in execution.rows],
        }

    def answer_results(
        self,
        *,
        question: str,
        sql: str,
        execution: dict[str, Any],
    ) -> SQLAnswer:
        return self._run_structured_stage(
            stage_name="answer_results",
            role=SQL_ANSWER_ROLE,
            goal=SQL_ANSWER_GOAL,
            backstory=SQL_ANSWER_BACKSTORY,
            description=build_sql_answer_prompt(
                question=question,
                sql=sql,
                execution_summary=json.dumps(execution, indent=2, sort_keys=True),
            ),
            expected_output="Return a JSON object with answer.",
            output_model=SQLAnswer,
            inputs={
                "question": question,
                "sql": sql,
                "execution": execution,
            },
        )

    def run(self, question: str) -> SQLWorkflowResult:
        workflow_id = uuid4().hex[:8]
        logger.info(
            "Starting workflow id=%s model=%s default_limit=%s max_limit=%s max_repairs=%s question=%s",
            workflow_id,
            self.model,
            self.default_limit,
            self.max_limit,
            self.max_repair_attempts,
            self._question_preview(question),
        )
        selection = self.select_tables(question)
        logger.info(
            "Workflow id=%s selected primary_tables=%s supporting_tables=%s",
            workflow_id,
            selection.primary_tables,
            selection.supporting_tables,
        )
        schema_context = build_schema_context(selection.all_tables)
        draft = self.generate_sql(
            question=question,
            schema_context=schema_context,
            selection=selection,
        )
        logger.info(
            "Workflow id=%s generated_sql=%s",
            workflow_id,
            self._sql_preview(draft.sql),
        )
        current_sql = draft.sql
        repair_attempts = 0
        critique = SQLCritique(approved=True)

        while True:
            try:
                logger.info(
                    "Workflow id=%s validating repair_attempt=%s sql=%s",
                    workflow_id,
                    repair_attempts,
                    self._sql_preview(current_sql),
                )
                validation = validate_sql(
                    current_sql,
                    default_limit=self.default_limit,
                    max_limit=self.max_limit,
                )
                logger.info(
                    "Workflow id=%s validated tables=%s applied_limit=%s sql=%s",
                    workflow_id,
                    list(validation.referenced_tables),
                    validation.applied_limit,
                    self._sql_preview(validation.sql),
                )
                self._assert_selected_tables(validation.referenced_tables, selection)

                critique = self.critique_sql(
                    question=question,
                    schema_context=schema_context,
                    selection=selection,
                    sql=validation.sql,
                )
                logger.info(
                    "Workflow id=%s critique approved=%s issue_count=%s issues=%s rationale=%s suggested_sql=%s",
                    workflow_id,
                    critique.approved,
                    len(critique.issues),
                    critique.issues,
                    self._question_preview(critique.rationale or "none", limit=180),
                    self._sql_preview(critique.suggested_sql or "none"),
                )
                if not critique.approved:
                    logger.warning(
                        "Workflow id=%s critique_rejected issues=%s suggested_sql=%s",
                        workflow_id,
                        critique.issues,
                        self._sql_preview(critique.suggested_sql or ""),
                    )
                    raise ValueError("; ".join(critique.issues) or critique.rationale or "Critic rejected SQL")

                logger.info("Workflow id=%s executing_sql", workflow_id)
                execution = execute_sql(
                    validation.sql,
                    default_limit=self.default_limit,
                    max_limit=self.max_limit,
                )
                serialized_execution = self._serialize_execution(execution)
                logger.info(
                    "Workflow id=%s execution_complete row_count=%s tables=%s",
                    workflow_id,
                    serialized_execution["row_count"],
                    serialized_execution["referenced_tables"],
                )
                answer = self.answer_results(
                    question=question,
                    sql=validation.sql,
                    execution=serialized_execution,
                )
                logger.info(
                    "Workflow id=%s answer=%s",
                    workflow_id,
                    self._question_preview(answer.answer, limit=120),
                )
                logger.info(
                    "Completed workflow id=%s repair_attempts=%s",
                    workflow_id,
                    repair_attempts,
                )
                return SQLWorkflowResult(
                    question=question,
                    table_selection=selection,
                    schema_context=schema_context,
                    generated_sql=draft.sql,
                    validated_sql=validation.sql,
                    critique=critique,
                    answer=answer,
                    execution=serialized_execution,
                    repair_attempts=repair_attempts,
                )
            except Exception as exc:
                logger.warning(
                    "Workflow id=%s iteration_failed repair_attempt=%s error=%s",
                    workflow_id,
                    repair_attempts,
                    exc,
                )
                if repair_attempts >= self.max_repair_attempts:
                    logger.exception(
                        "Workflow id=%s exhausted_repairs repair_attempts=%s",
                        workflow_id,
                        repair_attempts,
                    )
                    raise

                feedback_parts = [str(exc)]
                if critique.issues:
                    feedback_parts.append("Critic issues: " + "; ".join(critique.issues))
                if critique.suggested_sql:
                    feedback_parts.append("Suggested SQL: " + critique.suggested_sql)
                feedback = "\n".join(part for part in feedback_parts if part)

                logger.info(
                    "Workflow id=%s attempting_repair next_repair_attempt=%s",
                    workflow_id,
                    repair_attempts + 1,
                )
                repaired = self.repair_sql(
                    question=question,
                    schema_context=schema_context,
                    selection=selection,
                    sql=current_sql,
                    feedback=feedback,
                )
                current_sql = repaired.sql
                repair_attempts += 1
                logger.info(
                    "Workflow id=%s repair_complete repair_attempts=%s sql=%s",
                    workflow_id,
                    repair_attempts,
                    self._sql_preview(current_sql),
                )


__all__ = [
    "CrewSQLOrchestrator",
    "DEFAULT_CREWAI_MODEL",
    "SQLAnswer",
    "SQLCritique",
    "SQLDraft",
    "SQLWorkflowResult",
    "TableSelection",
    "build_schema_context",
]
