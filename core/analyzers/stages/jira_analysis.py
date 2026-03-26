"""
jira_analysis.py — JIRA comment analysis stage.

Reads ALL comments from ALL JIRA tickets for each material,
sends them to the LLM for structured analysis, and populates:
  - jira_historico_resumo:  LLM summary of all JIRA comments
  - jira_acao_sugerida:     LLM-suggested action
  - jira_tickets_count:     Number of related tickets
  - jira_status_atual:      Current ticket status
  - jira_ultimo_comentario: Most recent comment text
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pandas as pd
from tqdm import tqdm

from core.analyzers._base import save_checkpoint, step_header

logger = logging.getLogger(__name__)

# Columns added by this stage
JIRA_ANALYSIS_COLUMNS = [
    "jira_historico_resumo",
    "jira_acao_sugerida",
    "jira_tickets_count",
    "jira_status_atual",
    "jira_ultimo_comentario",
]


def _read_all_comments(jira, code: str, max_tickets: int = 5) -> dict[str, Any]:
    """Fetch ALL comments from ALL tickets matching a material code."""
    issues = jira.search_tickets(code, max_results=max_tickets)
    if not issues:
        return {
            "tickets": [],
            "comments": [],
            "count": 0,
            "status": "",
            "last_comment": "",
        }

    all_comments = []
    statuses = []

    for issue in issues:
        status = getattr(issue.fields.status, "name", str(issue.fields.status))
        statuses.append(f"{issue.key}: {status}")
        comments = jira.read_comments(issue)
        for c in comments:
            c["ticket_key"] = issue.key
            c["ticket_status"] = status
        all_comments.extend(comments)

    last_comment = all_comments[-1]["body"] if all_comments else ""

    return {
        "tickets": [i.key for i in issues],
        "comments": all_comments,
        "count": len(issues),
        "status": "; ".join(statuses),
        "last_comment": last_comment,
    }


def _format_comments_for_llm(comments: list[dict]) -> str:
    """Format all comments into a structured text block for the LLM."""
    if not comments:
        return "(Sem comentários)"

    lines = []
    for c in comments:
        ticket = c.get("ticket_key", "?")
        author = c.get("author", "?")
        date = c.get("created", "?")
        body = c.get("body", "")
        lines.append(f"[{ticket}] {date} — {author}:\n{body}\n")
    return "\n".join(lines)


def _analyze_jira_with_llm(row: pd.Series, jira, llm_model: str) -> dict[str, str]:
    """Read JIRA comments for one material and analyze with LLM."""
    from config.prompts import JIRA_ANALYSIS_SYSTEM_PROMPT, JIRA_ANALYSIS_USER_TEMPLATE
    from core.validators._base import LLMRunner

    code = str(row.get("Codigo_Material", ""))
    if not code:
        return {col: "" for col in JIRA_ANALYSIS_COLUMNS}

    # Fetch all comments
    data = _read_all_comments(jira, code)

    result = {
        "jira_tickets_count": data["count"],
        "jira_status_atual": data["status"],
        "jira_ultimo_comentario": data["last_comment"][:500] if data["last_comment"] else "",
        "jira_historico_resumo": "",
        "jira_acao_sugerida": "",
    }

    # If no tickets, nothing to analyze
    if data["count"] == 0:
        result["jira_historico_resumo"] = "Sem tickets JIRA encontrados"
        result["jira_acao_sugerida"] = "Nenhuma ação pendente no JIRA"
        return result

    # Format comments and send to LLM
    comments_text = _format_comments_for_llm(data["comments"])
    user_msg = JIRA_ANALYSIS_USER_TEMPLATE.format(
        codigo=code,
        descricao=row.get("Texto_Breve_Material", ""),
        grupo_mrp=row.get("Grupo_MRP", ""),
        estoque=row.get("Estoque_Total", ""),
        saldo=row.get("Saldo_Virtual", ""),
        ultimo_consumo=row.get("Data_Ultimo_Consumo", ""),
        num_tickets=data["count"],
        comentarios=comments_text[:3000],  # Limit to avoid token overflow
    )

    try:
        response_text = LLMRunner.chat(
            llm_model,
            JIRA_ANALYSIS_SYSTEM_PROMPT,
            user_msg,
            temperature=0.1,
        )
        parsed = json.loads(response_text)
        result["jira_historico_resumo"] = parsed.get("resumo", "")
        result["jira_acao_sugerida"] = parsed.get("acao_sugerida", "")
    except Exception as exc:
        logger.warning("JIRA LLM analysis failed for %s: %s", code, exc)
        result["jira_historico_resumo"] = f"Erro LLM: {exc}"
        result["jira_acao_sugerida"] = "Revisar manualmente"

    return result


def run_jira_analysis(
    df: pd.DataFrame,
    jira,
    llm_model: str = "google/gemini-2.0-flash-001",
    max_workers: int = 3,
) -> pd.DataFrame:
    """
    JIRA Analysis stage — reads all comments and analyzes with LLM.

    Runs for ALL materials (not just SMIT/FRAC). Populates JIRA columns
    that will be used by downstream AI analysis for context.
    """
    step_header(0, "JIRA Analysis", "Lendo comentários e analisando com LLM")

    # Initialize columns
    for col in JIRA_ANALYSIS_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    if df.empty or jira is None:
        return df

    print(f"   Analisando JIRA para {len(df)} materiais ({max_workers} threads)...")

    def process_row(args):
        idx, row = args
        try:
            return idx, _analyze_jira_with_llm(row, jira, llm_model)
        except Exception as exc:
            logger.error("JIRA analysis error for idx %s: %s", idx, exc)
            return idx, {col: "" for col in JIRA_ANALYSIS_COLUMNS}

    items = list(df.iterrows())

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(tqdm(
            executor.map(process_row, items),
            total=len(items),
            desc="JIRA Analysis",
        ))

    for idx, result_dict in results:
        for col, val in result_dict.items():
            df.at[idx, col] = val

    # Summary
    with_tickets = (df["jira_tickets_count"].astype(int) > 0).sum()
    print(f"   {with_tickets}/{len(df)} materiais com tickets JIRA encontrados.")

    save_checkpoint(df, "JIRA_Analysis")
    return df
