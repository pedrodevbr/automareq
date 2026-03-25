"""
zstk.py — ZSTK stage: business rules + AI analysis + market search.

Default stage for materials in Grupo_MRP == 'ZSTK' or any group not handled
by SMIT, FRAC, AD, or ANA.
"""

from __future__ import annotations

import logging
import time

import pandas as pd

from config.business import ALTO_VALOR, ALTO_VOLUME, ANOS_SEM_OC
from core.analyzers._base import save_checkpoint, update_row

logger = logging.getLogger(__name__)

# Groups handled by other stages
_OTHER_GROUPS = {"FRAC", "SMIT", "AD", "ANA"}


def _rule_zstk(row: pd.Series) -> pd.Series:
    """Apply ZSTK business logic to a single row."""
    log: list[str] = []
    try:
        val = lambda k: float(row.get(k, 0) or 0)

        v_total = val("Valor_Total_Ordem")
        vol_op = val("Volume_OP")
        virt = val("Saldo_Virtual")
        rtp3 = val("RTP3")
        rtp6 = val("RTP6")
        pr = val("PR_Atual")
        max_at = val("MAX_Atual")
        op = val("Quantidade_Ordem")
        anos_oc = float(
            row.get("Anos_Ultima_Compra", -1)
            if pd.notna(row.get("Anos_Ultima_Compra"))
            else -1
        )

        needs_mkt = False

        # Consumo zero check
        if str(row.get("Classificacao", "")).strip().upper() == "CONSUMO ZERO":
            log.append("CONSUMO ZERO")
            return update_row(row, log, ai_decisao="NAO_REPOR")

        if v_total > ALTO_VALOR:
            log.append("Verificar demanda (alto valor)")
        if vol_op > ALTO_VOLUME:
            log.append("Verificar entrega parcelada")
        if virt + rtp3 > pr:
            log.append("Saldo Virtual + RTP3 > PR")
        if virt + op > max_at:
            row["pos_analise"] = "Incluir (DEMPRO) na requisição"

        # Market search triggers
        if anos_oc < 0:
            log.append("Nunca comprou")
            needs_mkt = True
        elif anos_oc > ANOS_SEM_OC:
            log.append(f"Preço desatualizado (anos sem compra: {anos_oc})")
            needs_mkt = True

        if needs_mkt:
            row["needs_market_search"] = True

        # AI analysis trigger
        if row.get("Demanda_Programada") is True or rtp6 > 0:
            row["needs_ai"] = True
        else:
            row["needs_ai"] = False
            row["Analise_AI"] = "REPOR"

    except Exception as e:
        log.append(f"Erro regra ZSTK: {e}")

    return update_row(row, log)


def run_zstk(df: pd.DataFrame, ai_module=None, search_service=None) -> pd.DataFrame:
    """
    Process ZSTK materials (and any group not in FRAC/SMIT/AD/ANA).

    Steps:
      1. Apply business rules (_rule_zstk)
      2. Run AI analysis on needs_ai=True rows
      3. Run market search on needs_market_search=True rows

    Args:
        df:             Full DataFrame.
        ai_module:      AIModule instance (or None to skip AI).
        search_service: ReferenceValidator instance (or None to skip search).

    Returns the modified DataFrame.
    """
    grp = df["Grupo_MRP"].astype(str).str.upper()
    mask = (grp == "ZSTK") | (~grp.isin(_OTHER_GROUPS))
    n = mask.sum()
    print(f"   ZSTK: {n} materiais para analisar")

    if not mask.any():
        return df

    # Step 1: Business rules
    for idx in df[mask].index:
        df.loc[idx] = _rule_zstk(df.loc[idx].copy())

    grp_counts = df.loc[mask].groupby("Grupo_MRP").size().to_dict() if "Grupo_MRP" in df.columns else {}
    needs_ai = int((df.loc[mask, "needs_ai"] == True).sum())
    needs_mkt = int((df.loc[mask, "needs_market_search"] == True).sum())
    print(f"   Grupos: {grp_counts}")
    print(f"   Necessitam IA: {needs_ai} | Pesquisa de mercado: {needs_mkt}")

    # Step 2: AI analysis
    if ai_module is not None and needs_ai > 0:
        ai_mask = (mask) & (df["needs_ai"] == True)
        t0 = time.time()
        df_ai = ai_module.analyze_batch(df.loc[ai_mask].copy(), max_workers=3)
        if not df_ai.empty:
            df.update(df_ai)
        decisions = df.loc[ai_mask, "Analise_AI"].value_counts().to_dict() if "Analise_AI" in df.columns else {}
        print(f"   IA concluída em {time.time() - t0:.1f}s | Decisões: {decisions}")

    # Step 3: Market search
    if search_service is not None and needs_mkt > 0:
        mkt_mask = (mask) & (df["needs_market_search"] == True)
        cols_search = [
            "produto_identificado", "preco_unitario_estimado", "moeda",
            "url_fonte", "disponibilidade", "analise_confianca", "fornecedor_principal",
        ]
        for col in cols_search:
            if col not in df.columns:
                df[col] = None

        t0 = time.time()
        df_search = search_service.run_analysis_search(df.loc[mkt_mask].copy(), max_workers=3)
        if not df_search.empty:
            df.update(df_search[cols_search])
        print(f"   Pesquisa concluída em {time.time() - t0:.1f}s")

    save_checkpoint(df, "ZSTK")
    return df
