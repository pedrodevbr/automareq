"""
ai_stages.py — All LLM-powered validation stages.

Contains:
  Stage 2b-c: GRPM ML suggestion + LLM decision + Planner classification
  Stage 3:    Text similarity (embeddings) + AI audit
  Stage 3.5:  OBS pre-check (PN extraction via LLM)
  Stage 4:    Reference validation (delegates to ReferenceValidator)
  Stage 5:    Image validation (vision LLM)
"""

from __future__ import annotations

import base64
import json
import logging
import os
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
from numpy.linalg import norm
from tqdm import tqdm

from config.ai import ai_model_analysis, ai_model_text
from config.personnel import PLANEJADORES, country_for_responsavel

from core.validators._base import (
    EMBED_BATCH_SIZE,
    EMBED_THRESHOLD,
    EMBEDDING_MODEL,
    MAX_AUDIT_WORKERS,
    TEXT_COLUMN,
    LLMRunner,
    check_pn_in_obs_static,
    lang_instruction,
    run_llm_parallel,
    strip_json_fences,
)
from core.validators.rules import (
    ClassificationResponse,
    MaterialClassification,
    validate_text_fields_static,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# 2b. GRPM — ML-based group suggestion
# ===========================================================================

def _obter_descricao(grupos_mercadoria: Optional[pd.DataFrame], codigo: str) -> str:
    if grupos_mercadoria is None:
        return "Desconhecido"
    match = grupos_mercadoria[grupos_mercadoria["codigo_grupo"] == str(codigo)]["descricao_grupo"]
    return match.values[0] if not match.empty else "Desconhecido"


def suggest_grpm_ml(
    df: pd.DataFrame,
    model_path: str = "model/modelo_svc_dados_treino.joblib",
) -> pd.DataFrame:
    """
    Uses a local SVC/pipeline to predict top-10 Grupo_Mercadoria candidates.

    Adds columns: Grupo_Sugerido, Descricao_Grupo_Atual.
    """
    import joblib

    df["Grupo_Sugerido"] = ""
    df["Descricao_Grupo_Atual"] = ""

    try:
        data = joblib.load(model_path)
    except FileNotFoundError:
        logger.warning("ML model not found at '%s'. Skipping GRPM suggestion.", model_path)
        return df

    pipeline = data.get("pipeline")
    grupos_mercadoria = data.get("grupos_mercadoria")

    if isinstance(grupos_mercadoria, pd.DataFrame):
        grupos_mercadoria["codigo_grupo"] = (
            grupos_mercadoria["codigo_grupo"].astype(str).apply(
                lambda x: x.zfill(len(x) + 1) if len(x) % 2 != 0 else x
            )
        )

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="GRPM ML"):
        texto = str(row.get(TEXT_COLUMN, "")).lower()
        grupo_atual = str(row.get("Grupo_Mercadoria", "")).strip()

        if grupo_atual.isdigit() and len(grupo_atual) % 2 != 0:
            grupo_atual = grupo_atual.zfill(len(grupo_atual) + 1)
            df.at[idx, "Grupo_Mercadoria"] = grupo_atual

        df.at[idx, "Descricao_Grupo_Atual"] = _obter_descricao(grupos_mercadoria, grupo_atual)

        try:
            if hasattr(pipeline, "decision_function"):
                scores = pipeline.decision_function([texto])[0]
            else:
                scores = pipeline.predict_proba([texto])[0]

            classes = pipeline.classes_
            top10_idx = scores.argsort()[-10:][::-1]
            top10 = [str(classes[i]) for i in top10_idx]
        except Exception as exc:
            logger.error("ML pipeline error at index %s: %s", idx, exc)
            continue

        top3 = top10[:3]
        is_valid = any(
            grupo_atual == g or (grupo_atual and g.startswith(grupo_atual))
            for g in top3
        )

        if is_valid:
            df.at[idx, "Grupo_Sugerido"] = "ok"
        else:
            suggestions = "\n".join(
                f"{str(classes[i])} | {_obter_descricao(grupos_mercadoria, str(classes[i]))} | {round(scores[i], 2)}"
                for i in top10_idx
            )
            df.at[idx, "Grupo_Sugerido"] = suggestions

    return df


# ===========================================================================
# 2c. GRPM — LLM decision layer
# ===========================================================================

_GRPM_DECISION_SYSTEM = """\
Você é um especialista em classificação de materiais SAP com profundo conhecimento de grupos de mercadoria.

Dado um material com grupo atual, descrição do grupo atual, sugestões do modelo ML e o texto descritivo,
decida se vale a pena substituir o grupo de mercadoria registrado.

CRITÉRIOS:
- TROCAR: o grupo atual claramente não corresponde à natureza técnica do material E existe
  uma sugestão ML com alta confiança que se encaixa muito melhor.
- MANTER: o grupo atual é adequado, mesmo que não seja o top-1 do ML. Diferenças semânticas
  menores não justificam troca.
- INCERTO: há dúvida razoável; recomenda revisão humana.

REGRAS:
- Textos em CAIXA ALTA sem acentuação são padrão de sistema legado — ignore isso.
- Prefira MANTER em caso de dúvida para evitar mudanças desnecessárias.
- Se TROCAR, escolha o grupo mais específico dentre as sugestões (maior número de dígitos).

Responda APENAS com JSON válido — sem preâmbulo, sem markdown.
Schema:
{
  "grpm_decisao_llm":    "<TROCAR|MANTER|INCERTO>",
  "grpm_novo_codigo":    "<código do grupo sugerido, ou string vazia se MANTER/INCERTO>",
  "grpm_novo_descricao": "<descrição do novo grupo, ou string vazia>",
  "grpm_justificativa":  "<justificativa objetiva em 1-2 frases>"
}
"""

_GRPM_DECISION_USER = """\
MATERIAL: {codigo} — {texto_breve}

Grupo atual         : {grupo_atual}
Descrição atual     : {descricao_atual}
Texto PT            : {texto_pt}
Texto ES            : {texto_es}

Sugestões ML (código | descrição | confiança):
{sugestoes}

{lang}"""

_GRPM_DECISION_EMPTY = {
    "grpm_decisao_llm": "INCERTO",
    "grpm_novo_codigo": "",
    "grpm_novo_descricao": "",
    "grpm_justificativa": "Erro na análise",
}


def _grpm_decide_single(row: dict) -> dict:
    """Calls the LLM to decide whether to change Grupo_Mercadoria for one row."""
    codigo = str(row.get("Codigo_Material", ""))
    texto_breve = str(row.get("Texto_Breve_Material", ""))
    grupo_atual = str(row.get("Grupo_Mercadoria", ""))
    descricao_atual = str(row.get("Descricao_Grupo_Atual", ""))
    texto_pt = str(row.get("Texto_PT", "") or "")
    texto_es = str(row.get("Texto_ES", "") or "")
    sugestoes = str(row.get("Grupo_Sugerido", "") or "")
    responsavel = str(row.get("Responsavel", ""))

    country = country_for_responsavel(responsavel)
    user = _GRPM_DECISION_USER.format(
        codigo=codigo, texto_breve=texto_breve,
        grupo_atual=grupo_atual, descricao_atual=descricao_atual,
        texto_pt=texto_pt, texto_es=texto_es,
        sugestoes=sugestoes if sugestoes else "—",
        lang=lang_instruction(country),
    )

    try:
        raw = LLMRunner.chat(ai_model_analysis, _GRPM_DECISION_SYSTEM, user)
        data = json.loads(strip_json_fences(raw))
        decisao = str(data.get("grpm_decisao_llm", "INCERTO")).upper()
        if decisao not in ("TROCAR", "MANTER", "INCERTO"):
            decisao = "INCERTO"
        return {
            "grpm_decisao_llm": decisao,
            "grpm_novo_codigo": str(data.get("grpm_novo_codigo", "") or ""),
            "grpm_novo_descricao": str(data.get("grpm_novo_descricao", "") or ""),
            "grpm_justificativa": str(data.get("grpm_justificativa", "") or ""),
        }
    except Exception as exc:
        logger.error("GRPM LLM decision error for %s: %s", codigo, exc)
        result = _GRPM_DECISION_EMPTY.copy()
        result["grpm_justificativa"] = f"Erro API: {exc}"
        return result


def decide_grpm_llm(
    df: pd.DataFrame,
    max_workers: int = 6,
) -> pd.DataFrame:
    """
    LLM GRPM Decision Layer. Runs only on rows where Grupo_Sugerido != 'ok'.
    Adds: grpm_decisao_llm, grpm_novo_codigo, grpm_novo_descricao, grpm_justificativa.
    """
    output_cols = ["grpm_decisao_llm", "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa"]
    for col in output_cols:
        df[col] = ""
    df.loc[df.get("Grupo_Sugerido", pd.Series("ok")) == "ok", "grpm_decisao_llm"] = "MANTER"

    mask_diverge = df.get("Grupo_Sugerido", pd.Series("ok", index=df.index)) != "ok"

    if not mask_diverge.any():
        logger.info("GRPM LLM: todos os grupos estão validados pelo ML. Nada a processar.")
        return df

    print(f"   [LLM]  LLM decidindo sobre {mask_diverge.sum()} grupos divergentes...")
    df = run_llm_parallel(
        df, mask_diverge, _grpm_decide_single,
        output_cols, _GRPM_DECISION_EMPTY,
        max_workers=max_workers, desc="GRPM LLM Decision",
    )

    # Propagate TROCAR flag to pre_analise
    trocar_mask = df["grpm_decisao_llm"] == "TROCAR"
    if trocar_mask.any() and "pre_analise" in df.columns:
        for idx, row in df[trocar_mask].iterrows():
            flag = (f"[GRPM] Alterar Grupo_Mercadoria: {row.get('Grupo_Mercadoria')} → "
                    f"{row.get('grpm_novo_codigo')} ({row.get('grpm_novo_descricao')}). "
                    f"Motivo: {row.get('grpm_justificativa')}")
            df.at[idx, "pre_analise"] = str(df.at[idx, "pre_analise"]).rstrip() + f"\n{flag}"

    trocar_count = trocar_mask.sum()
    incerto_count = (df["grpm_decisao_llm"] == "INCERTO").sum()
    logger.info("GRPM LLM: TROCAR=%d | INCERTO=%d | MANTER=%d",
                trocar_count, incerto_count,
                (df["grpm_decisao_llm"] == "MANTER").sum())

    return df


# ===========================================================================
# 2d. PLANNER CLASSIFICATION
# ===========================================================================

def _build_planner_system_prompt() -> str:
    rules = json.dumps(PLANEJADORES, indent=2, ensure_ascii=False)
    schema = json.dumps(ClassificationResponse.model_json_schema())
    return (
        "You are a SAP Material Master Data Expert.\n"
        f"Classify materials into 'Planejador MRP' codes:\n{rules}\n\n"
        f"Return JSON matching this schema:\n{schema}"
    )


def _classify_planner_batch(batch_df: pd.DataFrame) -> List[MaterialClassification]:
    items = [
        {"id": idx, "description": str(row[TEXT_COLUMN])}
        for idx, row in batch_df.iterrows()
    ]
    if not items:
        return []
    try:
        raw = LLMRunner.chat(ai_model_analysis, _build_planner_system_prompt(),
                             f"Classify: {json.dumps(items)}")
        validated = ClassificationResponse.model_validate_json(raw)
        return validated.items
    except Exception as exc:
        logger.error("Planner classification error: %s", exc)
    return []


def validate_planner(df: pd.DataFrame, max_workers: int = 3) -> pd.DataFrame:
    """Classifies each material into a Planejador_MRP code using AI."""
    from utils.export_module import export_by_responsavel

    if df.empty:
        return df

    df["Planejador_Sugerido"] = None
    mask = df[TEXT_COLUMN].notna()
    to_proc = df[mask]
    batches = [to_proc.iloc[i:i + 1] for i in range(len(to_proc))]
    results: List[MaterialClassification] = []

    logger.info("Classifying %d items for planner...", len(to_proc))

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_classify_planner_batch, b): b for b in batches}
        for fut in tqdm(as_completed(futures), total=len(batches), desc="Planejador AI"):
            try:
                results.extend(fut.result())
            except Exception as exc:
                logger.error("Planner thread error: %s", exc)

    code_map = {item.id: item.code for item in results if item.code in PLANEJADORES}
    for idx, code in code_map.items():
        df.at[idx, "Planejador_Sugerido"] = code

    diff_mask = df["Planejador_Sugerido"].notna() & (df["Planejador_Sugerido"] != df.get("Planejador_MRP"))
    if diff_mask.any():
        cols = ["Codigo_Material", "Texto_Breve_Material", "Planejador_MRP", "Planejador_Sugerido", "Responsavel"]
        export_by_responsavel(df.loc[diff_mask, [c for c in cols if c in df.columns]], filename="Planner_validation")

    return df


# ===========================================================================
# 3. TEXT VALIDATION (PT / ES equivalence)
# ===========================================================================

def _cosine(v1: np.ndarray, v2: np.ndarray) -> float:
    n1, n2 = norm(v1), norm(v2)
    return float(np.dot(v1, v2) / (n1 * n2)) if n1 and n2 else 0.0


def calculate_text_similarity_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates embeddings for Texto_PT and Texto_ES in batch,
    computes cosine similarity. Only processes rows where Grupo_MRP == 'ZSTK'.
    Adds column: similarity_score.
    """
    df["similarity_score"] = 0.0

    mask = (
        (df.get("Grupo_MRP") == "ZSTK")
        & df["Texto_PT"].fillna("").astype(str).ne("")
        & df["Texto_ES"].fillna("").astype(str).ne("")
    )
    sub = df[mask].copy()
    if sub.empty:
        logger.info("No rows to embed (mask empty).")
        return df

    texts_pt = sub["Texto_PT"].fillna("").astype(str).tolist()
    texts_es = sub["Texto_ES"].fillna("").astype(str).tolist()

    def _batch_embed(texts: list, label: str) -> list:
        vecs = []
        for i in tqdm(range(0, len(texts), EMBED_BATCH_SIZE), desc=f"Embeddings {label}"):
            chunk = texts[i: i + EMBED_BATCH_SIZE]
            vecs.extend(LLMRunner.embed(EMBEDDING_MODEL, chunk))
        return vecs

    try:
        logger.info("Generating embeddings for %d rows...", len(sub))
        vecs_pt = _batch_embed(texts_pt, "PT")
        vecs_es = _batch_embed(texts_es, "ES")
    except Exception as exc:
        logger.error("Embedding API error: %s", exc)
        return df

    scores = [_cosine(np.array(p), np.array(e)) for p, e in zip(vecs_pt, vecs_es)]
    df.loc[mask, "similarity_score"] = scores
    return df


# --- AI audit for low-similarity rows ---

_AUDIT_SYSTEM = """\
Você é um auditor técnico de materiais para uma empresa binacional.
Analise a equivalência entre Texto PT e Texto ES.

REGRA: textos em CAIXA ALTA sem acentuação é padrão de sistema legado — NÃO aponte isso como erro.

CRITÉRIOS DE REPROVAÇÃO:
1. Contaminação de idioma: palavra de uma língua inserida na outra (ex: 'PARAFUSO' no ES em vez de 'TORNILLO').
2. Erros técnicos: números, referências ou unidades divergentes.
3. Falsos cognatos que mudam o sentido técnico.

Responda APENAS com um JSON válido:
{
  "status": "<OK|REPROVADO>",
  "motivo": "<Se REPROVADO, explicação objetiva. Se OK, string vazia>",
  "sugestao": "<Se REPROVADO, o texto original corrigido (PT ou ES) para sanar o erro. Se OK, string vazia>"
}
"""


def _audit_single_row(row: dict) -> dict:
    """Runs AI audit for a single row with low embedding similarity."""
    score = row.get("similarity_score", 0.0)
    pt_text = row.get("Texto_PT", "")
    es_text = row.get("Texto_ES", "")
    resp_key = (row.get("Responsavel") or "MTSE").strip().upper()
    country = country_for_responsavel(resp_key)

    issues = ""
    sugestao = ""

    if score < EMBED_THRESHOLD:
        user = f"PT: {pt_text}\nES: {es_text}\n\n{lang_instruction(country)}"
        try:
            raw = LLMRunner.chat(ai_model_analysis, _AUDIT_SYSTEM, user)
            data = json.loads(strip_json_fences(raw))
            if str(data.get("status", "OK")).upper() != "OK":
                issues = str(data.get("motivo", ""))
                sugestao = str(data.get("sugestao", ""))
        except Exception as exc:
            issues = f"Erro API: {exc}"

    # Always apply static checks on top
    static = validate_text_fields_static(row)
    if static:
        issues = (issues + "\n" if issues else "") + static

    return {"issues": issues, "sugestao": sugestao}


def run_text_audit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs text audit on rows with Grupo_MRP == 'ZSTK', Dias_Em_OP <= 7,
    and similarity_score < EMBED_THRESHOLD.
    All other rows receive only the static rule check.
    Adds: Text_Analysis, Texto_Sugerido.
    """
    df["Text_Analysis"] = ""
    df["Texto_Sugerido"] = ""

    mask_audit = (
        (df.get("Grupo_MRP") == "ZSTK")
        & (df.get("Dias_Em_OP", 99).fillna(99) <= 7)
        & (df.get("similarity_score", 0.0) < EMBED_THRESHOLD)
    )
    rows_to_audit = df[mask_audit]

    logger.info("Text AI audit: %d rows below threshold.", len(rows_to_audit))

    results_map: dict[int, dict] = {}

    from concurrent.futures import ThreadPoolExecutor, as_completed
    with ThreadPoolExecutor(max_workers=MAX_AUDIT_WORKERS) as pool:
        future_to_idx = {
            pool.submit(_audit_single_row, row): idx
            for idx, row in rows_to_audit.iterrows()
        }
        for fut in tqdm(as_completed(future_to_idx), total=len(rows_to_audit), desc="Text Audit AI"):
            idx = future_to_idx[fut]
            try:
                results_map[idx] = fut.result()
            except Exception as exc:
                results_map[idx] = {"issues": f"Erro system: {exc}", "sugestao": ""}

    for idx, row in df.iterrows():
        if idx in results_map:
            df.at[idx, "Text_Analysis"] = results_map[idx]["issues"]
            df.at[idx, "Texto_Sugerido"] = results_map[idx]["sugestao"]
        else:
            df.at[idx, "Text_Analysis"] = validate_text_fields_static(row)

    return df


# ===========================================================================
# 3.5  PN / OBSERVATION PRE-CHECK
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
    from utils.export_module import export_by_responsavel

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


# ===========================================================================
# 4. REFERENCE VALIDATION (web search + part number + text coverage)
# ===========================================================================

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


# ===========================================================================
# 5. IMAGE VALIDATION
# ===========================================================================

IMAGE_BASE_PATH = Path(r"P:\Mfotos\Padronizadas")
IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"]
IMAGE_VISION_MODEL = "google/gemini-3.1-flash-lite-preview"

_IMG_SYSTEM = """\
Você é um especialista em qualidade de imagens de catálogo industrial.

Analise a foto do material fornecida e responda APENAS com um JSON válido:
{
  "img_qualidade":  "<BOA|ACEITAVEL|SUBSTITUIR>",
  "img_motivo":     "<justificativa objetiva em 1-2 frases>"
}

CRITÉRIOS:
- BOA:        Imagem nítida, boa iluminação, fundo limpo, produto claramente identificável.
- ACEITAVEL:  Qualidade razoável com pequenos defeitos (leve desfoque, fundo levemente sujo).
- SUBSTITUIR: Imagem muito antiga, extremamente desfocada, baixa resolução, produto mal visível,
              foto amarelada/desbotada, ou que claramente não representa o material adequadamente.

Sem preâmbulo, sem markdown. Responda no idioma indicado na mensagem.
"""

_IMG_USER = "Material: {codigo} — {texto_breve}. {lang}"

_IMG_EMPTY = {
    "img_path": "",
    "img_qualidade": "NAO_VERIFICADA",
    "img_motivo": "Imagem não encontrada",
    "img_substituir": False,
}


def _resolve_image_path(codigo: str) -> Optional[Path]:
    for ext in IMAGE_EXTENSIONS:
        candidate = IMAGE_BASE_PATH / f"{codigo}(A){ext}"
        if candidate.exists():
            return candidate
    return None


def _encode_image_base64(path: Path) -> Tuple[str, str]:
    ext_to_mime = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".bmp": "image/bmp",
        ".tif": "image/tiff", ".tiff": "image/tiff",
    }
    mime = ext_to_mime.get(path.suffix.lower(), "image/jpeg")
    data = base64.b64encode(path.read_bytes()).decode("utf-8")
    return data, mime


def _validate_image_single(row: dict) -> dict:
    """Runs LLM vision analysis on one row's image."""
    codigo = str(row.get("Codigo_Material", ""))
    texto_breve = str(row.get("Texto_Breve_Material", ""))
    responsavel = str(row.get("Responsavel", ""))
    country = country_for_responsavel(responsavel)
    img_path = _resolve_image_path(codigo)

    if img_path is None:
        return {
            "img_path": str(IMAGE_BASE_PATH / f"{codigo}(A).jpg"),
            "img_qualidade": "NAO_VERIFICADA",
            "img_motivo": "Arquivo de imagem não encontrado",
            "img_substituir": False,
        }

    user = _IMG_USER.format(codigo=codigo, texto_breve=texto_breve, lang=lang_instruction(country))

    try:
        img_data, mime_type = _encode_image_base64(img_path)

        messages = [
            {"role": "system", "content": _IMG_SYSTEM},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime_type};base64,{img_data}"},
                    },
                    {"type": "text", "text": user},
                ],
            },
        ]

        raw = LLMRunner.chat_raw(IMAGE_VISION_MODEL, messages)
        data = json.loads(strip_json_fences(raw))
        qualidade = str(data.get("img_qualidade", "NAO_VERIFICADA")).upper()
        if qualidade not in ("BOA", "ACEITAVEL", "SUBSTITUIR"):
            qualidade = "NAO_VERIFICADA"

        return {
            "img_path": str(img_path),
            "img_qualidade": qualidade,
            "img_motivo": str(data.get("img_motivo", "")),
            "img_substituir": qualidade == "SUBSTITUIR",
        }

    except json.JSONDecodeError:
        logger.warning("Image LLM JSON error for %s", codigo)
        return {
            "img_path": str(img_path),
            "img_qualidade": "NAO_VERIFICADA",
            "img_motivo": "Erro: resposta JSON invalida do modelo",
            "img_substituir": False,
        }
    except Exception as exc:
        logger.error("Image validation error for %s: %s", codigo, exc)
        return {
            "img_path": str(img_path),
            "img_qualidade": "NAO_VERIFICADA",
            "img_motivo": f"Erro API: {exc}",
            "img_substituir": False,
        }


def run_image_validation(
    df: pd.DataFrame,
    max_workers: int = 4,
) -> pd.DataFrame:
    """
    Stage 5 — Image Validation. LLM vision analysis of material photos.
    Adds: img_path, img_qualidade, img_motivo, img_substituir.
    """
    from utils.export_module import export_by_responsavel

    for col in ["img_path", "img_qualidade", "img_motivo", "img_substituir"]:
        df[col] = "" if col != "img_substituir" else False

    all_mask = pd.Series(True, index=df.index)
    img_output_cols = ["img_path", "img_qualidade", "img_motivo", "img_substituir"]

    print(f"   [IMG]  Validando imagens para {len(df)} materiais...")
    df = run_llm_parallel(
        df, all_mask, _validate_image_single,
        img_output_cols, _IMG_EMPTY,
        max_workers=max_workers, desc="Image Validation",
    )

    # Propagate substitution flag
    subst_mask = df["img_substituir"].astype(bool)
    if subst_mask.any() and "pre_analise" in df.columns:
        for idx, row in df[subst_mask].iterrows():
            flag = f"[IMG] Substituir imagem — {row.get('img_motivo', '')}"
            df.at[idx, "pre_analise"] = str(df.at[idx, "pre_analise"]).rstrip() + f"\n{flag}"

    # Export report
    not_found_mask = df["img_qualidade"] == "NAO_VERIFICADA"
    report_mask = subst_mask | not_found_mask
    if report_mask.any():
        img_cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material",
            "img_path", "img_qualidade", "img_motivo", "img_substituir",
            "Texto_PT", "Texto_ES", "Responsavel",
        ] if c in df.columns]
        export_by_responsavel(df.loc[report_mask, img_cols], filename="Validation_Images")
        print(f"   [!]  {subst_mask.sum()} imagens para substituir, "
              f"{not_found_mask.sum()} nao encontradas -> relatorio exportado.")

    return df
