"""
orchestrator.py — Validation pipeline orchestration.

Contains:
  - run_stage_* wrappers (error handling + export per stage)
  - _ALL_STAGES registry
  - run_validations() — main entry point
  - _print_summary() helper
"""

from __future__ import annotations

import logging
from typing import Optional

import pandas as pd

from core.validators._base import MAX_AUDIT_WORKERS
from utils.export_module import export_by_responsavel, export_debug

logger = logging.getLogger(__name__)


# ===========================================================================
# Per-stage wrappers
# ===========================================================================

def run_stage_leadtime(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 1 — Lead Time validation."""
    from core.validators.rules import validate_lead_time

    print("\n[1]  Stage 1 — Lead Time")
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    try:
        df, lt_invalid = validate_lead_time(df)
        if not lt_invalid.empty:
            cols = [c for c in [
                "Codigo_Material", "Texto_Breve_Material",
                "Prazo_Entrega_Previsto", "leadtime_obs", "Responsavel",
            ] if c in df.columns]
            export_by_responsavel(lt_invalid[cols], filename="Validation_LeadTime")
            print(f"   [!]  {len(lt_invalid)} materiais com lead time inválido -> relatório exportado.")
    except Exception as exc:
        logger.error("Stage 1 failed: %s", exc)
        for col in ["leadtime_invalido", "leadtime_obs"]:
            if col not in df.columns:
                df[col] = False if col == "leadtime_invalido" else ""
    return df


def run_stage_grpm(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 2 — GRPM format + ML suggestion + LLM decision."""
    from core.validators.rules import validate_grpm_format
    from core.validators.ai_stages import suggest_grpm_ml, decide_grpm_llm

    print("\n[2]  Stage 2 — GRPM")
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    try:
        df, grpm_fmt_invalid = validate_grpm_format(df)
        if not grpm_fmt_invalid.empty:
            print(f"   [!]  {len(grpm_fmt_invalid)} materiais com formato de grupo inválido.")
    except Exception as exc:
        logger.error("Stage 2 — format check failed: %s", exc)
        if "grpm_formato_invalido" not in df.columns:
            df["grpm_formato_invalido"] = False

    try:
        df = suggest_grpm_ml(df)
    except Exception as exc:
        logger.error("Stage 2 — ML suggestion failed: %s", exc)
        for col in ["Grupo_Sugerido", "Descricao_Grupo_Atual"]:
            if col not in df.columns:
                df[col] = ""

    try:
        df = decide_grpm_llm(df, max_workers=MAX_AUDIT_WORKERS)
    except Exception as exc:
        logger.error("Stage 2 — LLM decision failed: %s", exc)
        for col in ["grpm_decisao_llm", "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa"]:
            if col not in df.columns:
                df[col] = ""

    grpm_report_mask = df.get("grpm_decisao_llm", pd.Series("MANTER")).isin(["TROCAR", "INCERTO"])
    if grpm_report_mask.any():
        cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material",
            "Grupo_Mercadoria", "Descricao_Grupo_Atual", "Grupo_Sugerido",
            "grpm_decisao_llm", "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa",
            "Texto_PT", "Texto_ES", "Responsavel",
        ] if c in df.columns]
        export_by_responsavel(df.loc[grpm_report_mask, cols], filename="Validation_GRPM")
        print(f"   [!]  {grpm_report_mask.sum()} materiais com decisão GRPM (TROCAR/INCERTO) -> relatório exportado.")
    return df


def run_stage_texts(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 3 — Texts PT/ES similarity + AI audit."""
    from core.validators.ai_stages import calculate_text_similarity_batch, run_text_audit

    print("\n[3]  Stage 3 — Textos PT/ES")
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    try:
        df = calculate_text_similarity_batch(df)
    except Exception as exc:
        logger.error("Stage 3 — embeddings failed: %s", exc)
        df["similarity_score"] = 0.0

    try:
        df = run_text_audit(df)
    except Exception as exc:
        logger.error("Stage 3 — text audit failed: %s", exc)
        df["Text_Analysis"] = ""
        df["Texto_Sugerido"] = ""

    text_issues = df["Text_Analysis"].fillna("").astype(str).ne("")
    if text_issues.any():
        cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material",
            "Texto_PT", "Texto_ES", "similarity_score", "Text_Analysis", "Texto_Sugerido", "Responsavel",
        ] if c in df.columns]
        export_by_responsavel(df.loc[text_issues, cols], filename="Validation_Texts")
        print(f"   [!]  {text_issues.sum()} materiais com problemas de texto -> relatório exportado.")
    return df


def run_stage_obs(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 3.5 — OBS Pre-Check."""
    from core.validators.ai_stages import run_obs_precheck

    print("\n[3.5]  Stage 3.5 — Observações / PN Pre-Check")
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    try:
        df = run_obs_precheck(df, max_workers=MAX_AUDIT_WORKERS)
    except Exception as exc:
        logger.error("Stage 3.5 failed: %s", exc)
        for col in ["obs_pn_presente", "obs_referencia_extraida",
                     "obs_pesquisa_vale", "obs_motivo", "obs_sugestao_texto"]:
            if col not in df.columns:
                df[col] = ""
    return df


def run_stage_reference(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 4 — Market Reference validation."""
    from core.validators.ai_stages import run_reference_validation

    print("\n[4]  Stage 4 — Referências de Mercado")
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    try:
        df = run_reference_validation(df)
    except Exception as exc:
        logger.error("Stage 4 failed: %s", exc)
        for col in ["ref_reference_found", "ref_part_number_confirmed",
                     "ref_text_coverage", "ref_validation_issues"]:
            if col not in df.columns:
                df[col] = ""

    ref_issues = df.get("ref_validation_issues", pd.Series()).fillna("").astype(str).ne("")
    if ref_issues.any():
        ref_cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material",
            "Numero_Peca_Fabricante", "Texto_PT", "Texto_ES",
            "ref_reference_found", "ref_supplier",
            "ref_url", "ref_search_links",
            "ref_price_estimated", "ref_currency", "ref_availability",
            "ref_part_number_confirmed", "ref_part_number_note",
            "ref_text_coverage", "ref_coverage_gaps", "ref_validation_issues",
            "obs_referencia_extraida", "obs_pesquisa_vale",
            "Texto_Observacao_PT", "Texto_Observacao_ES",
            "Responsavel",
        ] if c in df.columns]
        export_by_responsavel(df.loc[ref_issues, ref_cols], filename="Validation_Reference")
        print(f"   [!]  {ref_issues.sum()} materiais com problemas de referência -> relatório exportado.")
    return df


def run_stage_images(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 5 — Image Validation."""
    from core.validators.ai_stages import run_image_validation

    print("\n[5]  Stage 5 — Imagens")
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    try:
        df = run_image_validation(df)
    except Exception as exc:
        logger.error("Stage 5 failed: %s", exc)
        for col in ["img_path", "img_qualidade", "img_motivo", "img_substituir"]:
            if col not in df.columns:
                df[col] = "" if col != "img_substituir" else False
    return df


def run_stage_ref_obs(df: pd.DataFrame) -> pd.DataFrame:
    """Stage 5.5 — Reference-in-OBS Validation."""
    from core.validators.rules import validate_ref_in_obs

    print("\n[5.5]  Stage 5.5 — Reference-in-OBS")
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    try:
        df = validate_ref_in_obs(df)
    except Exception as exc:
        logger.error("Stage 5.5 failed: %s", exc)
        if "ref_obs_flag" not in df.columns:
            df["ref_obs_flag"] = ""
    return df


# ===========================================================================
# Summary printer
# ===========================================================================

def _print_summary(df: pd.DataFrame) -> None:
    """Prints a consolidated summary of all validation flags."""
    print("\n" + "=" * 60)
    print("[OK]  PIPELINE CONCLUÍDO")
    print(f"   Lead time inválido    : {df.get('leadtime_invalido', pd.Series(False)).sum()}")
    print(f"   GRPM formato inválido : {df.get('grpm_formato_invalido', pd.Series(False)).sum()}")
    print(f"   GRPM TROCAR (LLM)     : {(df.get('grpm_decisao_llm', pd.Series()) == 'TROCAR').sum()}")
    print(f"   GRPM INCERTO (LLM)    : {(df.get('grpm_decisao_llm', pd.Series()) == 'INCERTO').sum()}")
    print(f"   Textos c/ problemas   : {df.get('Text_Analysis', pd.Series('')).fillna('').astype(str).ne('').sum()}")
    print(f"   PN ausente em OBS     : {(~df.get('obs_pn_presente', pd.Series(True))).sum()}")
    print(f"   Pesquisas ignoradas   : {(df.get('obs_pesquisa_vale', pd.Series('SIM')) == 'NAO').sum()}")
    print(f"   Referências c/ issue  : {df.get('ref_validation_issues', pd.Series('')).fillna('').astype(str).ne('').sum()}")
    print(f"   Imagens p/ substituir : {df.get('img_substituir', pd.Series(False)).astype(bool).sum()}")
    print(f"   Imagens nao encontrad.: {(df.get('img_qualidade', pd.Series()) == 'NAO_VERIFICADA').sum()}")
    print(f"   Ref-in-OBS issues     : {df.get('ref_obs_flag', pd.Series('')).fillna('').astype(str).ne('').sum()}")
    print("=" * 60)


# ===========================================================================
# Stage registry and main pipeline
# ===========================================================================

_ALL_STAGES: list[tuple[str, callable]] = [
    ("leadtime", run_stage_leadtime),
    ("grpm", run_stage_grpm),
    ("texts", run_stage_texts),
    ("obs", run_stage_obs),
    ("reference", run_stage_reference),
    # ("images",    run_stage_images),
    ("ref_obs", run_stage_ref_obs),
]


def run_validations(
    df: pd.DataFrame,
    stages: Optional[list[str]] = None,
    export_debug_sheet: bool = True,
) -> pd.DataFrame:
    """
    Executes the full (or partial) validation pipeline.

    Args:
        df:                 Input DataFrame.
        stages:             Optional list of stage names to run.
                            Valid: 'leadtime', 'grpm', 'texts', 'obs',
                                   'reference', 'images', 'ref_obs'.
                            If None, all stages run in order.
        export_debug_sheet: Whether to save DEBUG_Full.xlsx at the end.

    Returns the enriched DataFrame.
    """
    from core.validators.rules import consolidate_validation_summary

    print("=" * 60)
    print("[START]  INICIANDO PIPELINE DE VALIDAÇÕES")
    if stages:
        print(f"         Stages selecionados: {', '.join(stages)}")
    print("=" * 60)

    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""

    active = {name for name in (stages or [n for n, _ in _ALL_STAGES])}
    invalid = active - {n for n, _ in _ALL_STAGES}
    if invalid:
        raise ValueError(
            f"Stage(s) desconhecido(s): {invalid}. "
            f"Válidos: {[n for n, _ in _ALL_STAGES]}"
        )

    for name, fn in _ALL_STAGES:
        if name in active:
            df = fn(df)

    # Consolidate all validation flags
    print("\n[6]  Consolidação — Resumo de Validação")
    try:
        df = consolidate_validation_summary(df)
    except Exception as exc:
        logger.error("Consolidation failed: %s", exc)
        for col in ["resumo_validacao", "score_validacao", "classificacao_validacao"]:
            if col not in df.columns:
                df[col] = "" if col != "score_validacao" else 100

    _print_summary(df)

    if export_debug_sheet:
        print("\n[DEBUG] Exportando planilha completa para debug...")
        try:
            debug_path = export_debug(df)
            print(f"   -> {debug_path}")
        except Exception as exc:
            logger.error("Debug export failed: %s", exc)

    return df
