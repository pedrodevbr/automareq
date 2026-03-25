"""
frac.py — FRAC stage: JIRA ticket creation + SAP MRP change.

Materials in Grupo_MRP == 'FRAC' had a desert bidding (licitação deserta).
If LMR qty > 0, creates a JIRA ticket and changes SAP MRP type to SMIT.
"""

from __future__ import annotations

import logging

import pandas as pd

from core.analyzers._base import save_checkpoint, update_row

logger = logging.getLogger(__name__)


def _rule_frac(row: pd.Series, jira, sap) -> pd.Series:
    """Apply FRAC business logic to a single row."""
    log: list[str] = []
    try:
        row["needs_market_search"] = True
        qtde_lmr = float(row.get("Quantidade_LMR", 0) or 0)

        if qtde_lmr > 0:
            data_pedido = row.get("Data_Ultimo_Pedido")
            try:
                data_str = data_pedido.strftime("%d/%m/%Y") if pd.notna(data_pedido) else "Nunca comprou"
            except Exception:
                data_str = "Nunca comprou"

            texto = (
                f"Prezados,\nA licitação do código {row.get('Codigo_Material')} "
                f"resultou deserta.\n"
                f"Referência: {row.get('Numero_Peca_Fabricante')}\n"
                f"Data da ultima compra: {data_str}\n"
                f"Favor indicar uma referencia substituta\n"
            )
            row["FRAC_texto"] = texto
            log.append("Sugerido abrir JIRA FRAC")
            row["sugestao_jira_frac"] = True
            row["Analise_AI"] = "NAO_REPOR"

            # Create JIRA ticket and change SAP MRP type
            if jira is not None:
                try:
                    if not jira.verificar_consultas_abertas(row["Codigo_Material"]):
                        key = jira.create_frac_ticket(
                            code=row["Codigo_Material"],
                            short_text=row["Texto_Breve_Material"],
                            text=texto,
                            saldo_virtual=row.get("Saldo_Virtual", 0),
                        )
                        log.append(f"FRAC criado: {key}")
                        if sap is not None:
                            try:
                                sap.change_tipo_mrp(row["Codigo_Material"], "SMIT")
                            except Exception as e:
                                logger.error("SAP MRP change error %s: %s", row["Codigo_Material"], e)
                except Exception as e:
                    logger.error("Jira Create Error %s: %s", row["Codigo_Material"], e)
        else:
            log.append("Encontrar outra referência")
    except Exception as e:
        log.append(f"Erro FRAC: {e}")

    return update_row(row, log)


def run_frac(df: pd.DataFrame, jira, sap) -> pd.DataFrame:
    """
    Process FRAC materials: create JIRA tickets and change SAP MRP type.

    Args:
        df:   Full DataFrame (only FRAC rows are processed).
        jira: JiraModule instance (or None to skip JIRA operations).
        sap:  SapManager instance (or None to skip SAP operations).

    Returns the modified DataFrame.
    """
    mask = df["Grupo_MRP"].astype(str).str.upper() == "FRAC"
    n = mask.sum()
    print(f"   FRAC: {n} materiais para processar")

    if not mask.any():
        return df

    if "FRAC_texto" not in df.columns:
        df["FRAC_texto"] = ""

    for idx in df[mask].index:
        df.loc[idx] = _rule_frac(df.loc[idx].copy(), jira, sap)

    save_checkpoint(df, "FRAC")
    return df
