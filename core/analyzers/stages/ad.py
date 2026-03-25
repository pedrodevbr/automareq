"""
ad.py — AD stage: auto-decision + supplier quote text.

Materials in Grupo_MRP == 'AD' are automatically set to REPOR
with a pre-built supplier quote request.
"""

from __future__ import annotations

import pandas as pd

from core.analyzers._base import save_checkpoint


def run_ad(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process AD materials: set REPOR and generate supplier quote text.

    Args:
        df: Full DataFrame (only AD rows are processed).

    Returns the modified DataFrame.
    """
    mask = df["Grupo_MRP"].astype(str).str.upper() == "AD"
    n = mask.sum()
    print(f"   AD: {n} materiais para cotação")

    if not mask.any():
        return df

    if "AD_texto" not in df.columns:
        df["AD_texto"] = ""

    for idx, row in df[mask].iterrows():
        df.at[idx, "Analise_AI"] = "REPOR"
        df.at[idx, "AD_texto"] = (
            f"Prezado Fornecedor,\n\n"
            f"Favor cotar {row.get('Quantidade_Ordem', 0)} unidades do material "
            f"{row.get('Texto_Breve_Material', 'N/A')}:\n"
            f"- A Itaipu Binacional é isenta de impostos.\n"
            f"- Frete CIF.\n\n"
            f"Descrição:\n{row.get('Texto_PT', 'N/A')}\n\n"
            f"Referência:\n{row.get('Numero_Peca_Fabricante', 'N/A')}\n\n"
            f"Observações:\n{row.get('Texto_Observacao_PT', 'N/A')}\n"
        )

    save_checkpoint(df, "AD")
    return df
