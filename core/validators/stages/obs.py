"""
obs.py — OBS/PN pre-check validation stage.

Stage 3.5 of the validation pipeline: extracts references from observations
using LLM and checks PN presence.
"""

from __future__ import annotations

import json
import logging

import pandas as pd

from config.ai import ai_model_analysis
from config.personnel import country_for_responsavel
from core.validators._base import (
    MAX_AUDIT_WORKERS,
    LLMRunner,
    check_pn_in_obs_static,
    lang_instruction,
    run_llm_parallel,
    strip_json_fences,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# Prompts
# ===========================================================================

_OBS_SYSTEM = """\
Você é um especialista em dados mestre de materiais SAP.

Analise os campos fornecidos e retorne APENAS um JSON válido com estas chaves exatas:
{
  "obs_referencia_extraida": "<melhor string de referência identificada na observação para usar numa pesquisa de mercado, ex: modelo, catálogo, part number alternativo — ou string vazia se não houver nada útil>",
  "obs_pesquisa_vale": "<SIM se há referência suficiente para uma pesquisa de mercado eficaz, NAO se os dados são insuficientes ou genéricos demais, INCERTO se há dúvida>",
  "obs_motivo": "<justificativa objetiva da decisão em 1-2 frases>",
  "obs_sugestao_texto": "<texto sugerido para Texto_Observacao_PT incluindo o PN do fabricante corretamente formatado, ou string vazia se o PN já está presente>"
}

REGRAS:
- Textos em CAIXA ALTA sem acentuação é padrão de sistema legado — ignore isso.
- A referência extraída deve ser o identificador mais específico encontrado na observação.
- A pesquisa NÃO vale se: o texto da observação for vazio, genérico demais, ou não contiver nenhuma referência técnica identificável.
- A sugestão de texto só é necessária quando o PN do fabricante NÃO estiver na observação.
- Sem preâmbulo, sem markdown.
- Responda no idioma indicado na mensagem.
"""

_OBS_USER = """\
MATERIAL: {codigo} — {texto_breve}
Numero_Peca_Fabricante : {pn}
Texto_Observacao_PT    : {obs_pt}
Texto_Observacao_ES    : {obs_es}
Texto_PT               : {texto_pt}

{lang}"""

_OBS_EMPTY = {
    "obs_referencia_extraida": "",
    "obs_pesquisa_vale": "INCERTO",
    "obs_motivo": "Erro na análise",
    "obs_sugestao_texto": "",
}


# ===========================================================================
# Single-row processor
# ===========================================================================

def _obs_precheck_single(row: dict) -> dict:
    """Runs the LLM pre-check for one row."""
    codigo = str(row.get("Codigo_Material", ""))
    texto_breve = str(row.get("Texto_Breve_Material", ""))
    pn = str(row.get("Numero_Peca_Fabricante", "") or "").strip()
    obs_pt = str(row.get("Texto_Observacao_PT", "") or "").strip()
    obs_es = str(row.get("Texto_Observacao_ES", "") or "").strip()
    texto_pt = str(row.get("Texto_PT", "") or "")
    responsavel = str(row.get("Responsavel", ""))

    country = country_for_responsavel(responsavel)
    user = _OBS_USER.format(
        codigo=codigo, texto_breve=texto_breve,
        pn=pn or "—", obs_pt=obs_pt or "—", obs_es=obs_es or "—",
        texto_pt=texto_pt, lang=lang_instruction(country),
    )

    try:
        raw = LLMRunner.chat(ai_model_analysis, _OBS_SYSTEM, user)
        data = json.loads(strip_json_fences(raw))
    except Exception as exc:
        logger.error("OBS pre-check error for %s: %s", codigo, exc)
        data = _OBS_EMPTY.copy()
        data["obs_motivo"] = f"Erro API: {exc}"

    return {
        "obs_referencia_extraida": str(data.get("obs_referencia_extraida", "")),
        "obs_pesquisa_vale": str(data.get("obs_pesquisa_vale", "INCERTO")).upper(),
        "obs_motivo": str(data.get("obs_motivo", "")),
        "obs_sugestao_texto": str(data.get("obs_sugestao_texto", "")),
    }


# ===========================================================================
# Batch runner
# ===========================================================================

def run_obs_precheck(
    df: pd.DataFrame,
    max_workers: int = 8,
) -> pd.DataFrame:
    """
    Stage 3.5 — PN / Observation Pre-Check.
    Static PN presence check + LLM reference extraction.
    Adds: obs_pn_presente, obs_referencia_extraida, obs_pesquisa_vale,
          obs_motivo, obs_sugestao_texto.
    """
    from utils.export_core import export_by_responsavel

    # 1. Static PN presence check
    df["obs_pn_presente"] = df.apply(
        lambda r: check_pn_in_obs_static(
            str(r.get("Numero_Peca_Fabricante", "") or ""),
            str(r.get("Texto_Observacao_PT", "") or ""),
        ),
        axis=1,
    )

    absent_mask = ~df["obs_pn_presente"] & df["Numero_Peca_Fabricante"].fillna("").astype(str).ne("")
    if absent_mask.any() and "pre_analise" in df.columns:
        flag = "[OBS] Incluir Numero_Peca_Fabricante em Texto_Observacao_PT"
        for idx in df[absent_mask].index:
            df.at[idx, "pre_analise"] = str(df.at[idx, "pre_analise"]).rstrip() + f"\n{flag}"

    # 2. LLM analysis (all rows)
    output_cols = ["obs_referencia_extraida", "obs_pesquisa_vale", "obs_motivo", "obs_sugestao_texto"]
    all_mask = pd.Series(True, index=df.index)

    print(f"   [3.5]  Analisando observações e PNs para {len(df)} itens...")
    df = run_llm_parallel(
        df, all_mask, _obs_precheck_single,
        output_cols, _OBS_EMPTY,
        max_workers=max_workers, desc="OBS Pre-Check",
    )

    # 3. Export report
    needs_attention = (
        absent_mask
        | (df["obs_pesquisa_vale"] == "NAO")
        | df["obs_sugestao_texto"].fillna("").astype(str).ne("")
    )
    if needs_attention.any():
        report_cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material",
            "Numero_Peca_Fabricante",
            "Texto_Observacao_PT", "Texto_Observacao_ES",
            "obs_pn_presente", "obs_referencia_extraida",
            "obs_pesquisa_vale", "obs_motivo", "obs_sugestao_texto",
            "Texto_PT", "Texto_ES", "Responsavel",
        ] if c in df.columns]
        export_by_responsavel(df.loc[needs_attention, report_cols], filename="Validation_OBS_PreCheck")
        print(f"   [!]  {needs_attention.sum()} itens requerem atenção nas observações -> relatório exportado.")

    return df
