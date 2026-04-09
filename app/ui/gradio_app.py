"""Gradio chat interface."""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any

from dotenv import load_dotenv
load_dotenv()

import gradio as gr

from app.agent.orchestrator import CrewSQLOrchestrator

logger = logging.getLogger(__name__)

APP_TITLE = "MarketLens AI"
APP_DESCRIPTION = (
    "Ask natural-language questions about instruments, derived market metrics, "
    "and macro data. The app returns a direct answer and keeps the SQL and raw "
    "rows available for inspection."
)
EXAMPLE_QUESTIONS = [
    ["Which company has the highest market cap?", True, False],
    ["What are the latest 5 instrument symbols in the database?", True, True],
    ["Which 5 symbols have the highest latest market capitalization?", True, True],
    ["List the latest 5 macro series titles.", True, False],
]
CUSTOM_CSS = """
:root {
  --ml-bg: linear-gradient(135deg, #f7f3eb 0%, #efe6d4 45%, #dde7df 100%);
  --ml-card: rgba(255, 252, 246, 0.88);
  --ml-border: rgba(78, 61, 32, 0.12);
  --ml-ink: #1f2a1f;
  --ml-muted: #5f6d60;
  --ml-accent: #9c5b2e;
  --ml-accent-soft: rgba(156, 91, 46, 0.12);
}

.gradio-container {
  background: var(--ml-bg);
  color: var(--ml-ink);
  font-family: ui-serif, Georgia, Cambria, "Times New Roman", serif;
}

#ml-shell {
  max-width: 1320px;
  margin: 0 auto;
  padding: 24px 18px 36px;
}

#ml-hero {
  background:
    radial-gradient(circle at top left, rgba(189, 129, 67, 0.18), transparent 34%),
    linear-gradient(140deg, rgba(255, 249, 240, 0.98), rgba(244, 238, 228, 0.9));
  border: 1px solid var(--ml-border);
  border-radius: 28px;
  padding: 28px 30px 24px;
  box-shadow: 0 18px 48px rgba(66, 52, 28, 0.08);
}

#ml-hero h1 {
  margin: 0;
  font-size: 2.6rem;
  line-height: 1;
  letter-spacing: -0.03em;
}

#ml-hero p {
  margin: 10px 0 0;
  max-width: 760px;
  color: var(--ml-muted);
  font-size: 1.06rem;
}

#ml-chat-panel,
#ml-side-panel {
  background: var(--ml-card);
  border: 1px solid var(--ml-border);
  border-radius: 26px;
  box-shadow: 0 14px 40px rgba(66, 52, 28, 0.07);
}

#ml-side-panel {
  padding: 10px;
}

.ml-section-note {
  color: var(--ml-muted);
  font-size: 0.95rem;
}

.ml-chip {
  display: inline-block;
  padding: 5px 10px;
  margin: 4px 8px 0 0;
  border-radius: 999px;
  background: var(--ml-accent-soft);
  color: var(--ml-accent);
  border: 1px solid rgba(156, 91, 46, 0.14);
  font-size: 0.88rem;
}

.ml-footer {
  margin-top: 14px;
  color: var(--ml-muted);
  font-size: 0.9rem;
}
"""


@lru_cache(maxsize=1)
def get_orchestrator() -> CrewSQLOrchestrator:
    return CrewSQLOrchestrator(verbose=False)


def _summarize_execution(execution: dict[str, Any], *, include_rows: bool) -> dict[str, Any]:
    summary = {
        "row_count": execution.get("row_count", 0),
        "referenced_tables": execution.get("referenced_tables", []),
        "applied_limit": execution.get("applied_limit"),
    }
    if include_rows:
        summary["rows"] = execution.get("rows", [])
    return summary


def run_marketlens_query(
    message: str,
    history: list[dict[str, Any]] | None,
    show_sql: bool,
    show_rows: bool,
) -> tuple[str, str, dict[str, Any]]:
    del history  # ChatInterface supplies this, but the current workflow is stateless.

    if not message.strip():
        return (
            "Ask a market or macro question to get started.",
            "```sql\n-- no query executed\n```",
            {"row_count": 0, "referenced_tables": [], "rows": []},
        )

    logger.info("Received UI query")
    try:
        result = get_orchestrator().run(message)
    except Exception as exc:
        logger.exception("UI query failed")
        return (
            f"I couldn't answer that request because the workflow failed: {exc}",
            "```sql\n-- query generation did not complete\n```",
            {"error": str(exc)},
        )

    logger.info("UI query completed successfully")
    sql_panel = (
        f"```sql\n{result.validated_sql}\n```"
        if show_sql
        else "_Validated SQL hidden. Toggle it on in the side panel._"
    )
    execution_panel = _summarize_execution(result.execution, include_rows=show_rows)
    return result.answer.answer, sql_panel, execution_panel


def build_demo() -> gr.Blocks:
    theme = gr.themes.Base(
        primary_hue="amber",
        secondary_hue="emerald",
        neutral_hue="stone",
    )
    with gr.Blocks(
        title=APP_TITLE,
        fill_width=True,
        fill_height=True,
    ) as demo:
        with gr.Column(elem_id="ml-shell"):
            gr.Markdown(
                f"""
<div id="ml-hero">
  <div class="ml-chip">Schema-aware SQL</div>
  <div class="ml-chip">Natural-language answers</div>
  <div class="ml-chip">Inspectable execution trail</div>
  <h1>{APP_TITLE}</h1>
  <p>{APP_DESCRIPTION}</p>
</div>
""".strip()
            )

            with gr.Row(equal_height=True):
                with gr.Column(scale=7, elem_id="ml-chat-panel"):
                    chatbot = gr.Chatbot(
                        label="Conversation",
                        height=620,
                        layout="bubble",
                        placeholder="Ask about companies, returns, volatility, macro data, or the latest records in the warehouse.",
                        avatar_images=(None, None),
                    )
                    textbox = gr.Textbox(
                        placeholder="Try: Which company has the highest market cap?",
                        show_label=False,
                        container=False,
                        lines=2,
                    )

                with gr.Column(scale=4, min_width=320, elem_id="ml-side-panel"):
                    gr.Markdown(
                        """
### Query Lens
Use the toggles below to control how much debugging detail appears beside the answer.

The main chat always returns a natural-language response grounded in the executed rows.
""".strip(),
                        elem_classes=["ml-section-note"],
                    )
                    show_sql = gr.Checkbox(label="Show validated SQL", value=True)
                    show_rows = gr.Checkbox(label="Show raw rows", value=True)
                    sql_view = gr.Markdown(label="Validated SQL")
                    execution_view = gr.JSON(label="Execution snapshot")
                    gr.Markdown(
                        "Questions run through table selection, SQL generation, SQL critique, execution, and a final answer-writing agent.",
                        elem_classes=["ml-footer"],
                    )

            gr.ChatInterface(
                fn=run_marketlens_query,
                chatbot=chatbot,
                textbox=textbox,
                additional_inputs=[show_sql, show_rows],
                additional_outputs=[sql_view, execution_view],
                examples=EXAMPLE_QUESTIONS,
                autofocus=True,
                save_history=True,
                fill_height=True,
                show_progress="minimal",
            )
    demo.theme = theme
    demo.css = CUSTOM_CSS
    return demo


def launch_app() -> gr.Blocks:
    demo = build_demo()
    server_name = os.getenv("GRADIO_SERVER_NAME", "127.0.0.1")
    server_port = int(os.getenv("GRADIO_SERVER_PORT", "7860"))
    share = os.getenv("GRADIO_SHARE", "false").lower() == "true"
    demo.queue(default_concurrency_limit=1)
    demo.launch(
        server_name=server_name,
        server_port=server_port,
        share=share,
        show_error=True,
        theme=demo.theme,
        css=demo.css,
    )
    return demo


def main() -> None:
    launch_app()


__all__ = [
    "APP_TITLE",
    "build_demo",
    "get_orchestrator",
    "launch_app",
    "main",
    "run_marketlens_query",
]
