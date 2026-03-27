"""
reference.py — Market reference validation stage.

Stage 4 of the validation pipeline: delegates to ReferenceValidator service.
"""

from __future__ import annotations

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def run_reference_validation(
    df: pd.DataFrame,
    max_workers: int = 5,
) -> pd.DataFrame:
    """
    For rows where obs_pesquisa_vale != 'NAO', calls ReferenceValidator.
    Merges ref_* columns back into df.
    """
    from services.search_service import ReferenceValidator

    skip_mask = df.get("obs_pesquisa_vale", pd.Series("SIM", index=df.index)) == "NAO"
    df_to_search = df[~skip_mask]

    if skip_mask.any():
        print(f"   [SKIP]  {skip_mask.sum()} itens ignorados na pesquisa (obs_pesquisa_vale=NAO).")

    validator = ReferenceValidator()
    ref_df = validator.run_batch(df_to_search, max_workers=max_workers)

    for col in ref_df.columns:
        if col not in df.columns:
            df[col] = ""
    df.update(ref_df)

    if "ref_validation_issues" in df.columns and "pre_analise" in df.columns:
        has_issue = df["ref_validation_issues"].fillna("").astype(str).ne("")
        df.loc[has_issue, "pre_analise"] = (
            df.loc[has_issue, "pre_analise"].fillna("").astype(str)
            + "\n[REF] " + df.loc[has_issue, "ref_validation_issues"]
        )

    return df
