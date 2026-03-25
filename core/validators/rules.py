"""
rules.py — Pure pandas/rule-based validation stages.

No AI, no API key required. Safe to import and test without external services.

Contains:
  - validate_lead_time          (Stage 1)
  - validate_grpm_format        (Stage 2a)
  - validate_text_fields_static (Stage 3c — static text checks)
  - validate_ref_in_obs         (Stage 5.5)
  - consolidate_validation_summary
  - Pydantic schemas: MaterialClassification, ClassificationResponse
"""

from __future__ import annotations

import logging
from typing import List, Tuple

import pandas as pd
from pydantic import BaseModel, Field

from core.validators._base import check_pn_in_obs_static

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pydantic schemas (used by GRPM AI stage, defined here to avoid circular deps)
# ---------------------------------------------------------------------------

class MaterialClassification(BaseModel):
    id: int
    code: str = Field(description="Planner code, e.g. S21, U09")


class ClassificationResponse(BaseModel):
    items: List[MaterialClassification]


# ===========================================================================
# 1. LEAD TIME VALIDATION
# ===========================================================================

def validate_lead_time(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rules:
      - Prazo_Entrega_Previsto must be > 0
      - Must be a multiple of 30 (days)
      - > 720 days flagged as warning

    Adds columns: leadtime_invalido (bool), leadtime_obs (str).
    Appends flags to pre_analise.
    Returns (df_with_flag, invalid_subset_df).
    """
    col = "Prazo_Entrega_Previsto"
    if col not in df.columns:
        df["leadtime_invalido"] = False
        df["leadtime_obs"] = ""
        return df, pd.DataFrame()

    lt = pd.to_numeric(df[col], errors="coerce").fillna(0)

    mask_zero = lt == 0
    mask_not_mult = (lt > 0) & (lt % 30 != 0)
    mask_high = lt > 720
    mask_invalid = mask_zero | mask_not_mult

    df["leadtime_invalido"] = mask_invalid
    df["leadtime_obs"] = ""

    df.loc[mask_zero, "leadtime_obs"] = "Lead time zerado"
    df.loc[mask_not_mult, "leadtime_obs"] = (
        "Lead time não é múltiplo de 30 dias: " + lt[mask_not_mult].astype(str) + "d"
    )
    df.loc[mask_high & ~mask_invalid, "leadtime_obs"] = (
        "Lead time acima de 720 dias (atenção): " + lt[mask_high & ~mask_invalid].astype(str) + "d"
    )

    if "pre_analise" in df.columns:
        df.loc[mask_invalid, "pre_analise"] = (
            df.loc[mask_invalid, "pre_analise"].fillna("").astype(str)
            + "\n[LT] " + df.loc[mask_invalid, "leadtime_obs"]
        )

    invalid_df = df[mask_invalid].copy()
    if not invalid_df.empty:
        logger.info("Lead time inválido em %d materiais.", len(invalid_df))

    return df, invalid_df


# ===========================================================================
# 2a. GRPM FORMAT VALIDATION
# ===========================================================================

def validate_grpm_format(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Grupo_Mercadoria must be >= 4 numeric digits and must NOT equal '99'.

    Adds column grpm_formato_invalido (bool).
    Returns (df, invalid_subset_df).
    """
    col = "Grupo_Mercadoria"
    if col not in df.columns:
        df["grpm_formato_invalido"] = False
        return df, pd.DataFrame()

    grp = df[col].astype(str).str.strip().fillna("")
    mask_invalid = ~grp.str.match(r"^\d{4,}$") | (grp == "99")

    df["grpm_formato_invalido"] = mask_invalid

    if "pre_analise" in df.columns:
        df.loc[mask_invalid, "pre_analise"] = (
            df.loc[mask_invalid, "pre_analise"].fillna("").astype(str)
            + "\n[GRPM] Formato inválido: " + grp[mask_invalid]
        )

    return df, df[mask_invalid].copy()


# ===========================================================================
# 3c. STATIC TEXT FIELD RULES
# ===========================================================================

def validate_text_fields_static(row: dict) -> str:
    """
    Rule-based checks (no AI):
      - Observacao PT and ES must both be filled or both empty.
      - Dados_Basicos PT and ES must both be filled or both empty.
      - Observacao must not equal Dados_Basicos in the same language.
    """
    issues: list[str] = []

    def empty(v) -> bool:
        return pd.isna(v) or str(v).strip() == ""

    obs_pt = row.get("Texto_Observacao_PT", "")
    obs_es = row.get("Texto_Observacao_ES", "")
    dados_pt = row.get("Texto_Dados_Basicos_PT", "")
    dados_es = row.get("Texto_Dados_Basicos_ES", "")

    if empty(obs_pt) != empty(obs_es):
        issues.append("Observacao_PT e Observacao_ES devem estar ambos preenchidos ou ambos vazios.")

    if empty(dados_pt) != empty(dados_es):
        issues.append("Dados_Basicos_PT e Dados_Basicos_ES devem estar ambos preenchidos ou ambos vazios.")

    if not empty(obs_pt) and not empty(dados_pt) and str(obs_pt).strip() == str(dados_pt).strip():
        issues.append("Observacao_PT não pode ser igual a Dados_Basicos_PT.")

    if not empty(obs_es) and not empty(dados_es) and str(obs_es).strip() == str(dados_es).strip():
        issues.append("Observacao_ES não pode ser igual a Dados_Basicos_ES.")

    return "\n".join(f"==> {i}" for i in issues)


# ===========================================================================
# 5.5. REFERENCE-IN-OBS VALIDATION
# ===========================================================================

def validate_ref_in_obs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates whether a material reference is properly documented.

    Logic:
      1. If Texto_Dados_Basicos_PT has content -> OK.
      2. Else:
         a. PN missing from Numero_Peca_Fabricante -> flag
         b. PN missing from Texto_Observacao_PT    -> flag
    """
    def _empty(val) -> bool:
        return pd.isna(val) or str(val).strip() == ""

    flags: list[str] = []

    for idx, row in df.iterrows():
        if not _empty(row.get("Texto_Dados_Basicos_PT")):
            flags.append("")
            continue

        row_flags: list[str] = []
        pn = str(row.get("Numero_Peca_Fabricante", "") or "").strip()
        obs = str(row.get("Texto_Observacao_PT", "") or "").strip()

        if _empty(pn):
            row_flags.append("[REF] PN ausente no campo Numero_Peca_Fabricante")

        if not _empty(pn) and not check_pn_in_obs_static(pn, obs):
            row_flags.append("[REF] PN ausente em Texto_Observacao_PT")

        combined = "\n".join(row_flags)
        flags.append(combined)

        if combined and "pre_analise" in df.columns:
            df.at[idx, "pre_analise"] = (
                str(df.at[idx, "pre_analise"]).rstrip() + f"\n{combined}"
            )

    df["ref_obs_flag"] = flags
    flagged = sum(bool(f) for f in flags)
    if flagged:
        logger.info("ref_obs_flag: %d materials with reference documentation issues.", flagged)

    return df


# ===========================================================================
# VALIDATION CONSOLIDATION — Human-oriented summary
# ===========================================================================

def consolidate_validation_summary(df: pd.DataFrame) -> pd.DataFrame:
    """
    Consolidates ALL validation flags into structured columns:
      - resumo_validacao:         multi-line text organised by category tag
      - classificacao_validacao:  OK | REVISAR
    """
    resumos: list[str] = []
    classifs: list[str] = []

    for _, row in df.iterrows():
        issues: list[str] = []

        # [LT] Lead Time
        if row.get("leadtime_invalido", False):
            obs = str(row.get("leadtime_obs", "")).strip()
            issues.append(f"[LT] Ajustar Prazo_Entrega_Previsto: {obs if obs else 'Inválido'}")

        # [GRPM] Grupo Mercadoria
        if row.get("grpm_formato_invalido", False):
            issues.append("[GRPM] Corrigir formato numérico do Grupo_Mercadoria")

        decisao = str(row.get("grpm_decisao_llm", "")).upper()
        if decisao == "TROCAR":
            novo = row.get("grpm_novo_codigo", "")
            desc = row.get("grpm_novo_descricao", "")
            just = row.get("grpm_justificativa", "")
            issues.append(f"[GRPM] Alterar para {novo} - {desc}: {just}")
        elif decisao == "INCERTO":
            just = row.get("grpm_justificativa", "")
            issues.append(f"[GRPM] Revisar adequação do grupo atual: {just}")

        # [TXT] Textos PT/ES
        text_analysis = str(row.get("Text_Analysis", "")).strip()
        if text_analysis:
            txt_msg = f"[TXT] Padronizar tradução Texto PT/ES {text_analysis}"
            sug = str(row.get("Texto_Sugerido", "")).strip()
            if sug:
                txt_msg += f" (Sugestão: {sug[:80]}...)"
            issues.append(txt_msg)

        # [OBS] Observação / PN
        if row.get("obs_pn_presente") is False or row.get("obs_pn_presente") == False:
            pn = str(row.get("Numero_Peca_Fabricante", "")).strip()
            if pn:
                sug = str(row.get("obs_sugestao_texto", "")).strip()
                msg = "[OBS] Igualar PN e Observação"
                if sug:
                    msg += f" (Sugestão: {sug[:80]}"
                issues.append(msg)

        pesq = str(row.get("obs_pesquisa_vale", "")).upper()
        if pesq == "NAO":
            motivo = str(row.get("obs_motivo", "")).strip()
            issues.append(f"[OBS] Melhorar descrição para pesquisa: {motivo}")

        # [REF] Referência de mercado
        ref_issues = str(row.get("ref_validation_issues", "")).strip()
        if ref_issues:
            issues.append(f"[REF] Resolver divergência de mercado: {ref_issues}")

        ref_cov = str(row.get("ref_text_coverage", "")).upper()
        if ref_cov and ref_cov not in ("", "TOTAL", "COMPLETA", "OK"):
            gaps = str(row.get("ref_coverage_gaps", "")).strip()
            issues.append(f"[REF] Adicionar especificação em falta: {ref_cov}: {gaps}")

        # [IMG] Imagens
        img_q = str(row.get("img_qualidade", "")).upper()
        if img_q == "SUBSTITUIR":
            motivo = str(row.get("img_motivo", "")).strip()
            issues.append(f"[IMG] Substituir imagem de catálogo: {motivo}")
        elif img_q == "NAO_VERIFICADA":
            issues.append("[IMG] Anexar imagem padronizada (arquivo ausente)")

        # [DOC] Referência na documentação
        ref_obs = str(row.get("ref_obs_flag", "")).strip()
        if ref_obs:
            clean_ref = ref_obs.replace("[REF] ", "")
            issues.append(f"[DOC] Corrigir cadastro de PN: {clean_ref}")

        resumo = "\n".join(issues) if issues else ""
        classif = "✅ OK" if not issues else "⚠️ REVISAR"

        resumos.append(resumo)
        classifs.append(classif)

    df["resumo_validacao"] = resumos
    df["classificacao_validacao"] = classifs

    if "score_validacao" in df.columns:
        df.drop(columns=["score_validacao"], inplace=True)
    if "pre_analise" in df.columns:
        df.drop(columns=["pre_analise"], inplace=True)

    total = len(df)
    ok_count = sum(1 for c in classifs if "OK" in c)
    revisar_count = sum(1 for c in classifs if "REVISAR" in c)

    print(f"\n{'=' * 60}")
    print(f"[RESUMO VALIDAÇÃO] Total analisado: {total}")
    if total:
        print(f"   ✅ OK       : {ok_count:>4d} : {100 * ok_count / total:.0f}%)")
        print(f"   ⚠️ REVISAR  : {revisar_count:>4d} : {100 * revisar_count / total:.0f}%)")
    print(f"{'=' * 60}\n")

    return df
