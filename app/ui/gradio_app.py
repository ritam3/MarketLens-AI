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
QUESTION_CATEGORIES: dict[str, list[str]] = {
    "Company and market": [
        "Which company has the highest market cap?",
        "Show the 10 companies with the largest latest market caps.",
        "Which stocks have the strongest latest 30-day returns?",
        "Which symbols had the highest abnormal volume ratio on the latest date?",
    ],
    "Join-heavy": [
        "Show the latest 10 companies with market cap, 30-day return, and 20-day volatility, sorted by market cap.",
        "Which 10 companies have the highest latest market cap, along with sector and industry?",
        "For the latest date available, list the top 10 companies by market cap with symbol, name, sector, and industry.",
        "Which sectors contain the most companies in the top 50 by latest market cap?",
    ],
    "Price history": [
        "What were the latest closing prices for AAPL, MSFT, and NVDA?",
        "Show the last 20 trading days of close and volume for SPY.",
        "Which instruments had the highest daily return on the latest trading date?",
        "Which stocks had the biggest 30-day gains?",
    ],
    "Fundamentals": [
        "Which companies had the highest latest quarterly revenue?",
        "Show the latest quarterly revenue, net income, and EPS for Apple, Microsoft, and Amazon.",
        "Which companies had negative net income in the latest reported quarter?",
        "Show the latest free cash flow leaders.",
    ],
    "Macro": [
        "What macro series are available in the database?",
        "Show the latest 10 macro series titles.",
        "What is the latest value for CPI-related series?",
        "Show the recent observations for the federal funds rate.",
    ],
}
CUSTOM_CSS = """
:root {
  --ml-bg: #f1eadf;
  --ml-card: #fffaf2;
  --ml-border: rgba(78, 61, 32, 0.12);
  --ml-ink: #1f2a1f;
  --ml-muted: #5f6d60;
  --ml-accent: #9c5b2e;
  --ml-accent-soft: rgba(156, 91, 46, 0.12);
  --ml-panel: #fffaf2;
}

.gradio-container {
  background: var(--ml-bg);
  color: var(--ml-ink);
  color-scheme: light;
  font-family: ui-serif, Georgia, Cambria, "Times New Roman", serif;
}

.gradio-container .prose,
.gradio-container .prose p,
.gradio-container .prose li,
.gradio-container .prose strong,
.gradio-container .prose h1,
.gradio-container .prose h2,
.gradio-container .prose h3,
.gradio-container .prose h4,
.gradio-container .prose code,
.gradio-container label,
.gradio-container .message,
.gradio-container .message p,
.gradio-container .message strong,
.gradio-container .message code,
.gradio-container textarea,
.gradio-container input,
.gradio-container button,
.gradio-container select,
.gradio-container .toast-body,
.gradio-container .generating,
.gradio-container .json-container,
.gradio-container .json-container *,
.gradio-container [role="listbox"],
.gradio-container [role="option"],
.gradio-container [role="combobox"],
.gradio-container [data-testid="dropdown"] *,
.gradio-container .chatbot *,
.gradio-container .message-wrap *,
.gradio-container .md *,
.gradio-container .wrap *,
.gradio-container .label-wrap * {
  color: var(--ml-ink) !important;
}

.gradio-container .block,
.gradio-container .panel,
.gradio-container .form,
.gradio-container .form > *,
.gradio-container .wrap,
.gradio-container .message,
.gradio-container .message-wrap,
.gradio-container .md,
.gradio-container .json-container,
.gradio-container textarea,
.gradio-container input,
.gradio-container button,
.gradio-container select,
.gradio-container [role="listbox"],
.gradio-container [role="option"],
.gradio-container [role="combobox"],
.gradio-container [data-testid="dropdown"],
.gradio-container [data-testid="dropdown"] > div,
.gradio-container .chatbot,
.gradio-container .chatbot .message,
.gradio-container .chatbot .message-wrap,
.gradio-container .prose {
  background: var(--ml-panel) !important;
}

.gradio-container textarea,
.gradio-container input,
.gradio-container button,
.gradio-container select,
.gradio-container .message,
.gradio-container .json-container,
.gradio-container [role="combobox"],
.gradio-container [role="listbox"],
.gradio-container .block,
.gradio-container .panel,
.gradio-container .chatbot {
  border-color: rgba(78, 61, 32, 0.18) !important;
}

.gradio-container .placeholder,
.gradio-container ::placeholder,
.gradio-container .prose em,
.gradio-container .metadata,
.gradio-container .message-wrap .icon-button,
.gradio-container .form label span,
.gradio-container .secondary-text {
  color: var(--ml-muted) !important;
}

.gradio-container .prose {
  max-width: none;
}

#ml-shell {
  max-width: 1320px;
  margin: 0 auto;
  padding: 24px 18px 36px;
}

#ml-hero {
  background: var(--ml-card);
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

.gradio-container details,
.gradio-container summary,
.gradio-container summary *,
.gradio-container summary svg,
.gradio-container button[aria-expanded],
.gradio-container button[aria-expanded] *,
.gradio-container button[aria-expanded] svg {
  color: var(--ml-ink) !important;
  fill: var(--ml-ink) !important;
  stroke: var(--ml-ink) !important;
}

.gradio-container input[type="checkbox"] {
  appearance: auto !important;
  accent-color: var(--ml-accent) !important;
  background: var(--ml-panel) !important;
  border: 1px solid rgba(78, 61, 32, 0.35) !important;
}

.gradio-container .form input[type="checkbox"],
.gradio-container .form label input[type="checkbox"],
.gradio-container label input[type="checkbox"] {
  opacity: 1 !important;
  visibility: visible !important;
}

.gradio-container .form label,
.gradio-container .form label *,
.gradio-container label,
.gradio-container label *,
.gradio-container .checkbox,
.gradio-container .checkbox * {
  color: var(--ml-ink) !important;
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


def _select_example_question(selected_question: str | None) -> str:
    return selected_question or ""


def _submit_marketlens_query(
    message: str,
    history: list[dict[str, Any]] | None,
    show_sql: bool,
    show_rows: bool,
) -> tuple[list[dict[str, Any]], str, str, dict[str, Any]]:
    existing_history = list(history or [])
    if not message.strip():
        return existing_history, "", "```sql\n-- no query executed\n```", {
            "row_count": 0,
            "referenced_tables": [],
            "rows": [],
        }

    answer, sql_panel, execution_panel = run_marketlens_query(
        message,
        existing_history,
        show_sql,
        show_rows,
    )
    existing_history.extend(
        [
            {"role": "user", "content": message},
            {"role": "assistant", "content": answer},
        ]
    )
    return existing_history, "", sql_panel, execution_panel


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
                    history_state = gr.State([])
                    chatbot = gr.Chatbot(
                        label="Conversation",
                        height=620,
                        layout="bubble",
                        placeholder="Ask about companies, returns, volatility, macro data, or the latest records in the warehouse.",
                        avatar_images=(None, None),
                    )
                    with gr.Row():
                        textbox = gr.Textbox(
                            placeholder="Try: Which company has the highest market cap?",
                            show_label=False,
                            container=False,
                            lines=2,
                            scale=8,
                        )
                        send_button = gr.Button("Ask", scale=1, variant="primary")
                    clear_button = gr.Button("Clear conversation", variant="secondary")

                with gr.Column(scale=4, min_width=320, elem_id="ml-side-panel"):
                    with gr.Accordion("Query Lens", open=True):
                        gr.Markdown(
                            """
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
                    with gr.Accordion("Browse By Category", open=True):
                        for category, questions in QUESTION_CATEGORIES.items():
                            dropdown = gr.Dropdown(
                                label=category,
                                choices=questions,
                                value=None,
                                allow_custom_value=False,
                                interactive=True,
                            )
                            dropdown.change(
                                fn=_select_example_question,
                                inputs=dropdown,
                                outputs=textbox,
                            )

            send_event = textbox.submit(
                fn=_submit_marketlens_query,
                inputs=[textbox, history_state, show_sql, show_rows],
                outputs=[chatbot, textbox, sql_view, execution_view],
            )
            send_button_event = send_button.click(
                fn=_submit_marketlens_query,
                inputs=[textbox, history_state, show_sql, show_rows],
                outputs=[chatbot, textbox, sql_view, execution_view],
            )
            send_event.then(
                fn=lambda history: history,
                inputs=chatbot,
                outputs=history_state,
            )
            send_button_event.then(
                fn=lambda history: history,
                inputs=chatbot,
                outputs=history_state,
            )
            clear_button.click(
                fn=lambda: ([], [], "", "```sql\n-- no query executed\n```", {}),
                outputs=[chatbot, history_state, textbox, sql_view, execution_view],
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
