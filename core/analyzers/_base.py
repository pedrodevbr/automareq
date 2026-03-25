"""
_base.py — Shared helpers for analysis stages.

Provides:
  - step_header:      Console output for stage start
  - update_row:       Append log messages + set AI decision on a row
  - save_checkpoint:  Export DataFrame per responsável
  - init_analysis_columns: Ensure flag columns exist
"""

from __future__ import annotations

import logging

import pandas as pd

from utils.export_module import export_by_responsavel

logger = logging.getLogger(__name__)


def step_header(step_num: int, title: str, description: str = "") -> None:
    """Print a formatted stage header."""
    bar = "─" * 56
    print(f"\n┌{bar}┐")
    print(f"│  [{step_num}] {title:<50}│")
    if description:
        print(f"│      {description:<50}│")
    print(f"└{bar}┘")


def update_row(row: pd.Series, logs: list[str], ai_decisao: str | None = None) -> pd.Series:
    """Append log messages to pre_analise and optionally set Analise_AI."""
    if logs:
        curr = str(row.get("pre_analise", ""))
        sep = " | " if curr else ""
        row["pre_analise"] = curr + sep + " | ".join(logs)
    if ai_decisao:
        row["Analise_AI"] = ai_decisao
    return row


def save_checkpoint(df: pd.DataFrame, step_name: str) -> None:
    """Export current state per responsible analyst."""
    export_by_responsavel(df, filename=f"0{step_name}")


def init_analysis_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all analysis flag columns exist."""
    for col in ["needs_ai", "needs_market_search", "needs_jira_search", "sugestao_jira_frac"]:
        if col not in df.columns:
            df[col] = False
    return df
