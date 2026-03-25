"""
smit.py — SMIT stage: JIRA read + enrichment.

Materials in Grupo_MRP == 'SMIT' need specialist consultation via JIRA.
This stage reads existing JIRA tickets and enriches the DataFrame.
"""

from __future__ import annotations

import logging

import pandas as pd
from tqdm import tqdm

from core.analyzers._base import save_checkpoint

logger = logging.getLogger(__name__)


def run_smit(df: pd.DataFrame, jira) -> pd.DataFrame:
    """
    Process SMIT materials: read JIRA tickets and enrich with last comment.

    Args:
        df:   Full DataFrame (only SMIT rows are processed).
        jira: JiraModule instance (or None to skip).

    Returns the modified DataFrame.
    """
    mask = df["Grupo_MRP"].astype(str).str.upper() == "SMIT"
    n = mask.sum()
    print(f"   SMIT: {n} materiais para consultar no Jira")

    if not mask.any():
        return df

    # Set flags
    df.loc[mask, "needs_jira_search"] = True
    df.loc[mask, "Analise_AI"] = "NAO_REPOR"

    if "SMIT_texto" not in df.columns:
        df["SMIT_texto"] = ""

    if jira is None:
        print("   Jira desativado — pulando leitura de tickets.")
        return df

    for idx, row in tqdm(df[mask].iterrows(), total=n, desc="SMIT Jira Read"):
        try:
            comments, key = jira.find_last_comment(row["Codigo_Material"])
            if comments:
                last = comments[-1]["body"]
                df.at[idx, "SMIT_texto"] = (
                    f"{str(df.at[idx, 'SMIT_texto'])}\nMsg ({key}): {last}"
                )
                df.at[idx, "pre_analise"] = (
                    f"{str(df.at[idx, 'pre_analise'])} | Ticket: {key}"
                )
        except Exception as e:
            logger.error("Jira Read Error %s: %s", row["Codigo_Material"], e)

    save_checkpoint(df, "SMIT")
    return df
