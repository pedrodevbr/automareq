"""
ana.py — ANA stage: manual analysis flag.

Materials in Grupo_MRP == 'ANA' require manual analysis by a specialist.
"""

from __future__ import annotations

import pandas as pd

from core.analyzers._base import save_checkpoint


def run_ana(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process ANA materials: flag for manual analysis.

    Args:
        df: Full DataFrame (only ANA rows are processed).

    Returns the modified DataFrame.
    """
    mask = df["Grupo_MRP"].astype(str).str.upper() == "ANA"
    n = mask.sum()
    print(f"   ANA: {n} materiais para análise manual")

    if not mask.any():
        return df

    df.loc[mask, "pre_analise"] = (
        df.loc[mask, "pre_analise"].fillna("").astype(str) + " | ANALISE MANUAL"
    )
    df.loc[mask, "Analise_AI"] = "NAO_REPOR"

    save_checkpoint(df, "ANA")
    return df
