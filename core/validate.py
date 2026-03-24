"""
validate.py
===========
Material Master Data Validation Pipeline

Validation stages (run in order):
  1. Lead Time      – Prazo_Entrega_Previsto must be > 0 and a multiple of 30.
  2. GRPM           – Grupo_Mercadoria format + ML-based suggestion + LLM decision.
  3. Texts          – PT / ES equivalence via Qwen embeddings -> AI audit fallback.
  3.5 OBS Pre-Check – PN presence in Texto_Observacao_PT + reference extraction + search worthiness.
  4. Reference      – Web-search reference vs Texto_PT + part-number confirmation.
  5. Image          – LLM vision analysis of material photo; flags outdated images.

Entry point: run_validations(df) -> pd.DataFrame
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Optional, Tuple

# ---------------------------------------------------------------------------
# Windows cp1252 safety: force stdout/stderr to UTF-8 before any print/log
# ---------------------------------------------------------------------------
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import joblib
import numpy as np
import pandas as pd
from numpy.linalg import norm
from openai import OpenAI
from pydantic import BaseModel, Field, ValidationError
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from config.config import (
    PLANEJADORES,
    RESPONSAVEIS,
    ai_model_analysis,
    ai_model_text,
    country_for_responsavel,
)
from services.ai_service import AIModule
from services.search_service import ReferenceValidator
from utils.export_module import export_by_responsavel, export_dashboard_data, export_debug

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EMBEDDING_MODEL   = ai_model_text
TEXT_COLUMN       = "Texto_PT"
EMBED_THRESHOLD   = 0.99      # cosine similarity above which texts are approved without AI
EMBED_BATCH_SIZE  = 50
MAX_AUDIT_WORKERS = 8

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _strip_json_fences(text: str) -> str:
    """Remove ```json ... ``` fences that some models add."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def _lang_instruction(country: str) -> str:
    """Returns a language directive to append to user messages."""
    return "Responda em português." if country == "BR" else "Responda en español."

# ---------------------------------------------------------------------------
# Shared clients
# ---------------------------------------------------------------------------

def _openrouter_client() -> OpenAI:
    key = os.getenv("OPENROUTER_API_KEY")
    if not key:
        raise ValueError("OPENROUTER_API_KEY missing from environment / .env")
    return OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)


_client       = _openrouter_client()
_ai_module    = AIModule(ai_model_analysis)

# ---------------------------------------------------------------------------
# Pydantic schemas for GRPM classification
# ---------------------------------------------------------------------------

class MaterialClassification(BaseModel):
    id:   int
    code: str = Field(description="Planner code, e.g. S21, U09")

class ClassificationResponse(BaseModel):
    items: List[MaterialClassification]

# ===========================================================================
# 1. LEAD TIME VALIDATION
# ===========================================================================

def validate_lead_time(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Rules:
      • Prazo_Entrega_Previsto must be > 0
      • Must be a multiple of 30 (days)
      • Should not exceed 720 days (24 months) — flagged as a warning

    Adds column ``leadtime_invalido`` (bool) and appends a message to
    ``pre_analise`` for each flagged row.

    Returns (df_with_flag, invalid_subset_df).
    """
    col = "Prazo_Entrega_Previsto"
    if col not in df.columns:
        df["leadtime_invalido"] = False
        df["leadtime_obs"]      = ""
        return df, pd.DataFrame()

    lt = pd.to_numeric(df[col], errors="coerce").fillna(0)

    mask_zero     = lt == 0
    mask_not_mult = (lt > 0) & (lt % 30 != 0)
    mask_high     = lt > 720
    mask_invalid  = mask_zero | mask_not_mult

    df["leadtime_invalido"] = mask_invalid
    df["leadtime_obs"]      = ""

    df.loc[mask_zero,     "leadtime_obs"] = "Lead time zerado"
    df.loc[mask_not_mult, "leadtime_obs"] = (
        "Lead time não é múltiplo de 30 dias: " + lt[mask_not_mult].astype(str) + "d"
    )
    df.loc[mask_high & ~mask_invalid, "leadtime_obs"] = (
        "Lead time acima de 720 dias (atenção): " + lt[mask_high & ~mask_invalid].astype(str) + "d"
    )

    # Propagate to pre_analise
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
# 2. GRPM – GRUPO MERCADORIA VALIDATION
# ===========================================================================

# --- 2a. Format check ---

def validate_grpm_format(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Grupo_Mercadoria must be ≥ 4 numeric digits and must NOT equal '99'.

    Adds column ``grpm_formato_invalido`` (bool).
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


# --- 2b. ML-based group suggestion ---

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
    Uses a local SVC/pipeline to predict the top-10 Grupo_Mercadoria candidates.

    Adds columns:
      ``Grupo_Sugerido``      – 'ok' if current group is in top-3, else ranked suggestions.
      ``Descricao_Grupo_Atual`` – human-readable description of the current group.

    Normalises odd-digit group codes by zero-padding (e.g. '401' -> '0401').
    """
    df["Grupo_Sugerido"]       = ""
    df["Descricao_Grupo_Atual"] = ""

    try:
        data = joblib.load(model_path)
    except FileNotFoundError:
        logger.warning("ML model not found at '%s'. Skipping GRPM suggestion.", model_path)
        return df

    pipeline          = data.get("pipeline")
    grupos_mercadoria = data.get("grupos_mercadoria")

    if isinstance(grupos_mercadoria, pd.DataFrame):
        grupos_mercadoria["codigo_grupo"] = (
            grupos_mercadoria["codigo_grupo"].astype(str).apply(
                lambda x: x.zfill(len(x) + 1) if len(x) % 2 != 0 else x
            )
        )

    for idx, row in tqdm(df.iterrows(), total=len(df), desc="GRPM ML"):
        texto       = str(row.get(TEXT_COLUMN, "")).lower()
        grupo_atual = str(row.get("Grupo_Mercadoria", "")).strip()

        # Normalise zero-pad
        if grupo_atual.isdigit() and len(grupo_atual) % 2 != 0:
            grupo_atual = grupo_atual.zfill(len(grupo_atual) + 1)
            df.at[idx, "Grupo_Mercadoria"] = grupo_atual

        df.at[idx, "Descricao_Grupo_Atual"] = _obter_descricao(grupos_mercadoria, grupo_atual)

        try:
            if hasattr(pipeline, "decision_function"):
                scores = pipeline.decision_function([texto])[0]
            else:
                scores = pipeline.predict_proba([texto])[0]

            classes   = pipeline.classes_
            top10_idx = scores.argsort()[-10:][::-1]
            top10     = [str(classes[i]) for i in top10_idx]

        except Exception as exc:
            logger.error("ML pipeline error at index %s: %s", idx, exc)
            continue

        top3     = top10[:3]
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


# --- 2c. LLM decision layer: should we actually change the group? ---

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
    "grpm_decisao_llm":    "INCERTO",
    "grpm_novo_codigo":    "",
    "grpm_novo_descricao": "",
    "grpm_justificativa":  "Erro na análise",
}


def _grpm_decide_single(row: dict) -> dict:
    """Calls the LLM to decide whether to change Grupo_Mercadoria for one row."""
    codigo        = str(row.get("Codigo_Material", ""))
    texto_breve   = str(row.get("Texto_Breve_Material", ""))
    grupo_atual   = str(row.get("Grupo_Mercadoria", ""))
    descricao_atual = str(row.get("Descricao_Grupo_Atual", ""))
    texto_pt      = str(row.get("Texto_PT", "") or "")
    texto_es      = str(row.get("Texto_ES", "") or "")
    sugestoes     = str(row.get("Grupo_Sugerido", "") or "")
    responsavel   = str(row.get("Responsavel", ""))

    country = country_for_responsavel(responsavel)
    system  = _GRPM_DECISION_SYSTEM
    user    = _GRPM_DECISION_USER.format(
        codigo=codigo, texto_breve=texto_breve,
        grupo_atual=grupo_atual, descricao_atual=descricao_atual,
        texto_pt=texto_pt, texto_es=texto_es,
        sugestoes=sugestoes if sugestoes else "—",
        lang=_lang_instruction(country),
    )

    try:
        resp = _client.chat.completions.create(
            model=ai_model_analysis,
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        raw  = resp.choices[0].message.content or ""
        data = json.loads(_strip_json_fences(raw))
        decisao = str(data.get("grpm_decisao_llm", "INCERTO")).upper()
        if decisao not in ("TROCAR", "MANTER", "INCERTO"):
            decisao = "INCERTO"
        return {
            "grpm_decisao_llm":    decisao,
            "grpm_novo_codigo":    str(data.get("grpm_novo_codigo", "") or ""),
            "grpm_novo_descricao": str(data.get("grpm_novo_descricao", "") or ""),
            "grpm_justificativa":  str(data.get("grpm_justificativa", "") or ""),
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
    Stage 2b — LLM GRPM Decision Layer.

    Runs only on rows where ``Grupo_Sugerido`` != 'ok' (i.e. the ML model
    disagrees with the current group). For each such row, the LLM receives:
      • Current group code + description
      • ML top-10 suggestions with confidence scores
      • Texto_PT and Texto_ES

    And returns:
      ``grpm_decisao_llm``    – TROCAR | MANTER | INCERTO
      ``grpm_novo_codigo``    – suggested new group code (when TROCAR)
      ``grpm_novo_descricao`` – description of the new group
      ``grpm_justificativa``  – reasoning

    Rows where the LLM says TROCAR also get a flag appended to ``pre_analise``.
    Returns the enriched DataFrame.
    """
    # Initialise columns for all rows
    for col in ["grpm_decisao_llm", "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa"]:
        df[col] = ""
    df.loc[df.get("Grupo_Sugerido", pd.Series("ok")) == "ok", "grpm_decisao_llm"] = "MANTER"

    # Only process rows where ML disagreed
    mask_diverge = df.get("Grupo_Sugerido", pd.Series("ok", index=df.index)) != "ok"
    to_process   = df[mask_diverge]

    if to_process.empty:
        logger.info("GRPM LLM: todos os grupos estão validados pelo ML. Nada a processar.")
        return df

    print(f"   [LLM]  LLM decidindo sobre {len(to_process)} grupos divergentes...")
    records = to_process.to_dict("records")
    results: list[dict] = [{}] * len(records)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_grpm_decide_single, rec): i for i, rec in enumerate(records)}
        for fut in tqdm(as_completed(future_map), total=len(records), desc="GRPM LLM Decision"):
            idx = future_map[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                results[idx] = {**_GRPM_DECISION_EMPTY, "grpm_justificativa": f"Erro: {exc}"}

    # Write results back using positional index alignment
    proc_indices = to_process.index.tolist()
    for pos, row_result in enumerate(results):
        df_idx = proc_indices[pos]
        for col, val in row_result.items():
            df.at[df_idx, col] = val

    # Propagate TROCAR flag to pre_analise
    trocar_mask = df["grpm_decisao_llm"] == "TROCAR"
    if trocar_mask.any() and "pre_analise" in df.columns:
        for idx, row in df[trocar_mask].iterrows():
            flag = (f"[GRPM] Alterar Grupo_Mercadoria: {row.get('Grupo_Mercadoria')} → "
                    f"{row.get('grpm_novo_codigo')} ({row.get('grpm_novo_descricao')}). "
                    f"Motivo: {row.get('grpm_justificativa')}")
            df.at[idx, "pre_analise"] = str(df.at[idx, "pre_analise"]).rstrip() + f"\n{flag}"

    trocar_count  = trocar_mask.sum()
    incerto_count = (df["grpm_decisao_llm"] == "INCERTO").sum()
    logger.info("GRPM LLM: TROCAR=%d | INCERTO=%d | MANTER=%d",
                trocar_count, incerto_count,
                (df["grpm_decisao_llm"] == "MANTER").sum())

    return df

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
        resp = _client.chat.completions.create(
            model=ai_model_analysis,
            messages=[
                {"role": "system", "content": _build_planner_system_prompt()},
                {"role": "user",   "content": f"Classify: {json.dumps(items)}"},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        content  = resp.choices[0].message.content or ""
        validated = ClassificationResponse.model_validate_json(content)
        return validated.items
    except ValidationError as ve:
        logger.warning("Planner classification schema error: %s", ve)
    except Exception as exc:
        logger.error("Planner classification API error: %s", exc)
    return []


def validate_planner(df: pd.DataFrame, max_workers: int = 3) -> pd.DataFrame:
    """
    Classifies each material into a Planejador_MRP code using AI.
    Adds column ``Planejador_Sugerido``.
    Exports diff report when suggestion ≠ current value.
    """
    if df.empty:
        return df

    df["Planejador_Sugerido"] = None
    mask     = df[TEXT_COLUMN].notna()
    to_proc  = df[mask]
    batches  = [to_proc.iloc[i:i+1] for i in range(len(to_proc))]   # batch-of-1 for accuracy
    results: List[MaterialClassification] = []

    logger.info("Classifying %d items for planner...", len(to_proc))

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
# 3. TEXT VALIDATION  (PT / ES equivalence)
# ===========================================================================

# --- 3a. Cosine similarity helper ---

def _cosine(v1: np.ndarray, v2: np.ndarray) -> float:
    n1, n2 = norm(v1), norm(v2)
    return float(np.dot(v1, v2) / (n1 * n2)) if n1 and n2 else 0.0


# --- 3b. Qwen embedding batch ---

def calculate_text_similarity_batch(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates Qwen embeddings for Texto_PT and Texto_ES in batch,
    computes cosine similarity, and stores it in column ``similarity_score``.

    Only processes rows where Grupo_MRP == 'ZSTK' with non-empty PT and ES texts.
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
            chunk = texts[i : i + EMBED_BATCH_SIZE]
            resp  = _client.embeddings.create(model=EMBEDDING_MODEL, input=chunk)
            vecs.extend(e.embedding for e in resp.data)
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


# --- 3c. Static text field rules ---

def _validate_text_fields_static(row: dict) -> str:
    """
    Rule-based checks (no AI):
      • Observacao PT and ES must both be filled or both empty.
      • Dados_Basicos PT and ES must both be filled or both empty.
      • Observacao must not equal Dados_Basicos in the same language.
    """
    issues: list[str] = []

    def empty(v) -> bool:
        return pd.isna(v) or str(v).strip() == ""

    obs_pt   = row.get("Texto_Observacao_PT", "")
    obs_es   = row.get("Texto_Observacao_ES", "")
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


# --- 3d. AI audit for low-similarity rows ---

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
    """
    Runs AI audit for a single row with low embedding similarity.
    Returns a dict with 'issues' and 'sugestao'.
    """
    score    = row.get("similarity_score", 0.0)
    pt_text  = row.get("Texto_PT", "")
    es_text  = row.get("Texto_ES", "")
    resp_key = (row.get("Responsavel") or "MTSE").strip().upper()
    country  = country_for_responsavel(resp_key)

    issues = ""
    sugestao = ""

    if score < EMBED_THRESHOLD:
        user = f"PT: {pt_text}\nES: {es_text}\n\n{_lang_instruction(country)}"
        try:
            resp  = _client.chat.completions.create(
                model=ai_model_analysis,
                messages=[
                    {"role": "system", "content": _AUDIT_SYSTEM},
                    {"role": "user",   "content": user},
                ],
                temperature=0.1,
                response_format={"type": "json_object"},
            )
            raw  = resp.choices[0].message.content or ""
            data = json.loads(_strip_json_fences(raw))
            if str(data.get("status", "OK")).upper() != "OK":
                issues = str(data.get("motivo", ""))
                sugestao = str(data.get("sugestao", ""))
        except Exception as exc:
            issues = f"Erro API: {exc}"

    # Always apply static checks on top
    static = _validate_text_fields_static(row)
    if static:
        issues = (issues + "\n" if issues else "") + static

    return {"issues": issues, "sugestao": sugestao}


def run_text_audit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Runs text audit only on rows that:
      • Have Grupo_MRP == 'ZSTK'
      • Have Dias_Em_OP ≤ 7
      • Have similarity_score < EMBED_THRESHOLD

    All other rows receive only the static rule check.
    Adds 'Text_Analysis' and 'Texto_Sugerido' directly into df.
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
            df.at[idx, "Text_Analysis"] = _validate_text_fields_static(row)

    return df


# ===========================================================================
# 3.5  PN / OBSERVATION PRE-CHECK
# ===========================================================================
# Before spending API credits on a web search we:
#   a) Check statically whether Numero_Peca_Fabricante appears in Texto_Observacao_PT.
#   b) Ask the LLM to:
#       • Extract the best searchable reference from Texto_Observacao_PT.
#       • Decide if a web search is worthwhile (SIM / NAO / INCERTO).
#       • If the PN is absent from the observation, suggest the corrected text.
#
# New columns produced (prefix ``obs_``):
#   obs_pn_presente        bool   – static: PN found verbatim in observation
#   obs_referencia_extraida str   – reference the LLM extracted from observation
#   obs_pesquisa_vale       str   – SIM | NAO | INCERTO
#   obs_motivo              str   – reason for search decision
#   obs_sugestao_texto      str   – suggested corrected Texto_Observacao_PT (when PN absent)
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
    "obs_pesquisa_vale":       "INCERTO",
    "obs_motivo":              "Erro na análise",
    "obs_sugestao_texto":      "",
}


def _check_pn_in_obs_static(pn: str, obs: str) -> bool:
    """Case-insensitive, ignores hyphens and spaces."""
    if not pn or not obs:
        return False
    normalize = lambda s: re.sub(r"[\s\-]", "", s).upper()
    return normalize(pn) in normalize(obs)


def _obs_precheck_single(row: dict) -> dict:
    """
    Runs the LLM pre-check for one row.
    Returns a dict with obs_* keys ready to be written back to the DataFrame.
    """
    codigo      = str(row.get("Codigo_Material", ""))
    texto_breve = str(row.get("Texto_Breve_Material", ""))
    pn          = str(row.get("Numero_Peca_Fabricante", "") or "").strip()
    obs_pt      = str(row.get("Texto_Observacao_PT", "") or "").strip()
    obs_es      = str(row.get("Texto_Observacao_ES", "") or "").strip()
    texto_pt    = str(row.get("Texto_PT", "") or "")
    responsavel = str(row.get("Responsavel", ""))

    country = country_for_responsavel(responsavel)
    user    = _OBS_USER.format(
        codigo=codigo, texto_breve=texto_breve,
        pn=pn or "—", obs_pt=obs_pt or "—", obs_es=obs_es or "—",
        texto_pt=texto_pt, lang=_lang_instruction(country),
    )

    try:
        resp  = _client.chat.completions.create(
            model=ai_model_analysis,
            messages=[
                {"role": "system", "content": _OBS_SYSTEM},
                {"role": "user",   "content": user},
            ],
            temperature=0.1,
            response_format={"type": "json_object"},
        )
        raw   = resp.choices[0].message.content or ""
        data  = json.loads(_strip_json_fences(raw))
    except Exception as exc:
        logger.error("OBS pre-check error for %s: %s", codigo, exc)
        data = _OBS_EMPTY.copy()
        data["obs_motivo"] = f"Erro API: {exc}"

    return {
        "obs_referencia_extraida": str(data.get("obs_referencia_extraida", "")),
        "obs_pesquisa_vale":       str(data.get("obs_pesquisa_vale", "INCERTO")).upper(),
        "obs_motivo":              str(data.get("obs_motivo", "")),
        "obs_sugestao_texto":      str(data.get("obs_sugestao_texto", "")),
    }


def run_obs_precheck(
    df: pd.DataFrame,
    max_workers: int = 8,
) -> pd.DataFrame:
    """
    Stage 3.5 — PN / Observation Pre-Check.

    For every row:
      1. Static check: is Numero_Peca_Fabricante present in Texto_Observacao_PT?
         -> ``obs_pn_presente`` (bool)
         -> Flags rows needing correction in ``pre_analise``
      2. LLM analysis (parallel):
         -> ``obs_referencia_extraida``: best reference string for web search
         -> ``obs_pesquisa_vale``: SIM | NAO | INCERTO
         -> ``obs_motivo``: reason
         -> ``obs_sugestao_texto``: suggested corrected observation (when PN absent)

    Returns enriched df. Exports a report of rows needing attention.
    """
    import re as _re

    # ── 1. Static PN presence check ──────────────────────────────────────
    df["obs_pn_presente"] = df.apply(
        lambda r: _check_pn_in_obs_static(
            str(r.get("Numero_Peca_Fabricante", "") or ""),
            str(r.get("Texto_Observacao_PT", "") or ""),
        ),
        axis=1,
    )

    # Propagate absence flag to pre_analise
    absent_mask = ~df["obs_pn_presente"] & df["Numero_Peca_Fabricante"].fillna("").astype(str).ne("")
    if absent_mask.any() and "pre_analise" in df.columns:
        flag = "[OBS] Incluir Numero_Peca_Fabricante em Texto_Observacao_PT"
        for idx in df[absent_mask].index:
            df.at[idx, "pre_analise"] = str(df.at[idx, "pre_analise"]).rstrip() + f"\n{flag}"

    # ── 2. LLM analysis (all rows in parallel) ────────────────────────────
    # Initialise columns so they always exist even on error
    for col in ["obs_referencia_extraida", "obs_pesquisa_vale", "obs_motivo", "obs_sugestao_texto"]:
        df[col] = ""

    records = df.to_dict("records")
    results: list[dict] = [{}] * len(records)

    print(f"   [3.5]  Analisando observações e PNs para {len(records)} itens...")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_obs_precheck_single, rec): i for i, rec in enumerate(records)}
        for fut in tqdm(as_completed(future_map), total=len(records), desc="OBS Pre-Check"):
            idx = future_map[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                results[idx] = {**_OBS_EMPTY, "obs_motivo": f"Erro: {exc}"}

    for i, row_result in enumerate(results):
        for col, val in row_result.items():
            df.iat[i, df.columns.get_loc(col)] = val

    # ── 3. Export report ──────────────────────────────────────────────────
    needs_attention = (
        absent_mask |
        (df["obs_pesquisa_vale"] == "NAO") |
        df["obs_sugestao_texto"].fillna("").astype(str).ne("")
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
# 4. REFERENCE VALIDATION  (web search + part number + text coverage)
# ===========================================================================

def run_reference_validation(
    df: pd.DataFrame,
    max_workers: int = 5,
) -> pd.DataFrame:
    """
    For every row where ``obs_pesquisa_vale`` != 'NAO', calls ReferenceValidator which:
      1. Searches the web using ``obs_referencia_extraida`` (preferred) or Numero_Peca_Fabricante.
      2. Confirms whether the part number is present in the found reference.
      3. Checks if the reference COMPLETELY satisfies Texto_PT.

    Rows where ``obs_pesquisa_vale`` == 'NAO' are skipped (saves API credits).
    Merges ``ref_*`` columns back into *df* and propagates issues to ``pre_analise``.
    """
    # Determine which rows to search
    skip_mask = df.get("obs_pesquisa_vale", pd.Series("SIM", index=df.index)) == "NAO"
    df_to_search = df[~skip_mask]

    if skip_mask.any():
        print(f"   [SKIP]  {skip_mask.sum()} itens ignorados na pesquisa (obs_pesquisa_vale=NAO).")

    validator = ReferenceValidator()
    ref_df    = validator.run_batch(df_to_search, max_workers=max_workers)

    # Initialise ref columns for skipped rows as empty
    for col in ref_df.columns:
        if col not in df.columns:
            df[col] = ""
    df.update(ref_df)

    # Propagate reference issues to pre_analise
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
# For every material:
#   a) Build the image path: P:\Mfotos\Padronizadas\{Codigo_Material}
#      (tries common image extensions; stores the first match)
#   b) If the file exists, encode it as base64 and send to a vision LLM.
#   c) LLM decides:
#       • img_qualidade: BOA | ACEITAVEL | SUBSTITUIR
#       • img_motivo:    brief justification
#       • img_substituir: True when quality == SUBSTITUIR
#
# New columns:
#   img_path          str   – resolved file path (or "NAO_ENCONTRADA")
#   img_qualidade     str   – BOA | ACEITAVEL | SUBSTITUIR | NAO_VERIFICADA
#   img_motivo        str   – LLM justification
#   img_substituir    bool  – True when image should be replaced
# ===========================================================================

IMAGE_BASE_PATH = Path(r"P:\Mfotos\Padronizadas")
IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"]

# Vision model — must support image input via base64
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
    "img_qualidade": "NAO_VERIFICADA",
    "img_motivo":    "Imagem não encontrada",
    "img_substituir": False,
}


def _resolve_image_path(codigo: str) -> Optional[Path]:
    """
    Searches for the material image under IMAGE_BASE_PATH.
    Tries each extension in IMAGE_EXTENSIONS.
    Returns the first match, or None if not found.
    """
    for ext in IMAGE_EXTENSIONS:
        candidate = IMAGE_BASE_PATH / f"{codigo}(A){ext}"
        if candidate.exists():
            return candidate
    return None


def _encode_image_base64(path: Path) -> tuple[str, str]:
    """
    Returns (base64_data, media_type) for the given image file.
    """
    ext_to_mime = {
        ".jpg":  "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png":  "image/png",
        ".bmp":  "image/bmp",
        ".tif":  "image/tiff",
        ".tiff": "image/tiff",
    }
    mime = ext_to_mime.get(path.suffix.lower(), "image/jpeg")
    data = base64.b64encode(path.read_bytes()).decode("utf-8")
    return data, mime


def _validate_image_single(row: dict) -> dict:
    """
    Runs LLM vision analysis on one row's image.
    Returns a dict with img_path, img_qualidade, img_motivo, img_substituir.
    """
    codigo      = str(row.get("Codigo_Material", ""))
    texto_breve = str(row.get("Texto_Breve_Material", ""))
    responsavel = str(row.get("Responsavel", ""))
    country  = country_for_responsavel(responsavel)
    img_path = _resolve_image_path(codigo)

    if img_path is None:
        return {
            "img_path":       str(IMAGE_BASE_PATH / f"{codigo}(A).jpg"),
            "img_qualidade":  "NAO_VERIFICADA",
            "img_motivo":     "Arquivo de imagem não encontrado",
            "img_substituir": False,
        }

    user = _IMG_USER.format(codigo=codigo, texto_breve=texto_breve, lang=_lang_instruction(country))

    try:
        img_data, mime_type = _encode_image_base64(img_path)

        resp = _client.chat.completions.create(
            model=IMAGE_VISION_MODEL,
            messages=[
                {"role": "system", "content": _IMG_SYSTEM},
                {
                    "role": "user",
                    "content": [
                        {
                            "type":  "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{img_data}"
                            },
                        },
                        {"type": "text", "text": user},
                    ],
                },
            ],
            temperature=0.1,
        )

        raw  = resp.choices[0].message.content or ""
        data = json.loads(_strip_json_fences(raw))
        qualidade = str(data.get("img_qualidade", "NAO_VERIFICADA")).upper()
        if qualidade not in ("BOA", "ACEITAVEL", "SUBSTITUIR"):
            qualidade = "NAO_VERIFICADA"

        return {
            "img_path":       str(img_path),
            "img_qualidade":  qualidade,
            "img_motivo":     str(data.get("img_motivo", "")),
            "img_substituir": qualidade == "SUBSTITUIR",
        }

    except json.JSONDecodeError:
        logger.warning("Image LLM JSON error for %s", codigo)
        return {
            "img_path":       str(img_path),
            "img_qualidade":  "NAO_VERIFICADA",
            "img_motivo":     "Erro: resposta JSON invalida do modelo",
            "img_substituir": False,
        }
    except Exception as exc:
        logger.error("Image validation error for %s: %s", codigo, exc)
        return {
            "img_path":       str(img_path),
            "img_qualidade":  "NAO_VERIFICADA",
            "img_motivo":     f"Erro API: {exc}",
            "img_substituir": False,
        }


def run_image_validation(
    df: pd.DataFrame,
    max_workers: int = 4,
) -> pd.DataFrame:
    """
    Stage 5 — Image Validation.

    For every row:
      1. Adds ``img_path`` column: P:\\Mfotos\\Padronizadas\\{Codigo_Material}
      2. If file exists, sends base64-encoded image to a vision LLM.
      3. Populates ``img_qualidade``, ``img_motivo``, ``img_substituir``.

    Rows with img_substituir == True also get a flag appended to ``pre_analise``.
    Exports a report of images flagged for replacement.
    Returns the enriched DataFrame.
    """
    for col in ["img_path", "img_qualidade", "img_motivo", "img_substituir"]:
        df[col] = "" if col != "img_substituir" else False

    records = df.to_dict("records")
    results: list[dict] = [{}] * len(records)

    print(f"   [IMG]  Validando imagens para {len(records)} materiais...")
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(_validate_image_single, rec): i for i, rec in enumerate(records)}
        for fut in tqdm(as_completed(future_map), total=len(records), desc="Image Validation"):
            idx = future_map[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                results[idx] = {
                    "img_path": "", "img_qualidade": "NAO_VERIFICADA",
                    "img_motivo": f"Erro: {exc}", "img_substituir": False,
                }

    for i, row_result in enumerate(results):
        for col, val in row_result.items():
            df.iat[i, df.columns.get_loc(col)] = val

    # Propagate substitution flag to pre_analise
    subst_mask = df["img_substituir"].astype(bool)
    if subst_mask.any() and "pre_analise" in df.columns:
        for idx, row in df[subst_mask].iterrows():
            flag = f"[IMG] Substituir imagem — {row.get('img_motivo', '')}"
            df.at[idx, "pre_analise"] = str(df.at[idx, "pre_analise"]).rstrip() + f"\n{flag}"

    # Export report
    not_found_mask = df["img_qualidade"] == "NAO_VERIFICADA"
    report_mask    = subst_mask | not_found_mask
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


# ===========================================================================
# 5.5  REFERENCE-IN-OBS VALIDATION
# ===========================================================================
# Rule:
#   • If Texto_Dados_Basicos_PT is filled → the reference is already documented;
#     no requirement for it to appear in Texto_Observacao_PT.
#   • If Texto_Dados_Basicos_PT is empty → the reference MUST appear in BOTH:
#       – Numero_Peca_Fabricante   (static PN field)
#       – Texto_Observacao_PT      (observation field)
#     Missing from either → flag.
#
# New column:
#   ref_obs_flag  str  – empty if OK, else describes what is missing.
# ===========================================================================

def validate_ref_in_obs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validates whether a material reference is properly documented.

    Logic:
      1. If Texto_Dados_Basicos_PT has content -> OK, reference is in dados basicos.
      2. Else:
         a. PN missing from Numero_Peca_Fabricante -> flag "[REF] PN ausente no campo Numero_Peca_Fabricante"
         b. PN missing from Texto_Observacao_PT    -> flag "[REF] Referencia ausente em Texto_Observacao_PT"
         Both flags may coexist.

    Flags are also appended to ``pre_analise``.
    """
    def _empty(val) -> bool:
        return pd.isna(val) or str(val).strip() == ""

    flags: list[str] = []

    for idx, row in df.iterrows():
        # If dados basicos is filled, reference is documented there — no further check needed
        if not _empty(row.get("Texto_Dados_Basicos_PT")):
            flags.append("")
            continue

        row_flags: list[str] = []

        pn  = str(row.get("Numero_Peca_Fabricante", "") or "").strip()
        obs = str(row.get("Texto_Observacao_PT", "")     or "").strip()

        if _empty(pn):
            row_flags.append("[REF] PN ausente no campo Numero_Peca_Fabricante")

        if not _empty(pn) and not _check_pn_in_obs_static(pn, obs):
            row_flags.append("[REF] PN ausente em Texto_Observacao_PT")

        combined = "\n".join(row_flags)
        flags.append(combined)

        # Propagate to pre_analise
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
    Consolidates ALL validation flags from the 7 stages into structured
    columns designed for quick human analysis and decision-making.

    New columns added:
      ``resumo_validacao``         – Multi-line text organised by category tag, explicitly pointing out actions.
      ``classificacao_validacao``  – ✅ OK | ⚠️ REVISAR
    """
    resumos:  list[str] = []
    classifs: list[str] = []

    for _, row in df.iterrows():
        issues: list[str] = []

        # ── [LT] Lead Time ───────────────────────────────────────────
        if row.get("leadtime_invalido", False):
            obs = str(row.get("leadtime_obs", "")).strip()
            issues.append(f"[LT] Ajustar Prazo_Entrega_Previsto: {obs if obs else 'Inválido'}")

        # ── [GRPM] Grupo Mercadoria ──────────────────────────────────
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

        # ── [TXT] Textos PT/ES ───────────────────────────────────────
        text_analysis = str(row.get("Text_Analysis", "")).strip()
        if text_analysis:
            txt_msg = f"[TXT] Padronizar tradução Texto PT/ES {text_analysis}"
            sug = str(row.get("Texto_Sugerido", "")).strip()
            if sug:
                txt_msg += f" (Sugestão: {sug[:80]}...)"
            issues.append(txt_msg)

        # ── [OBS] Observação / PN ────────────────────────────────────
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

        # ── [REF] Referência de mercado ──────────────────────────────
        ref_issues = str(row.get("ref_validation_issues", "")).strip()
        if ref_issues:
            issues.append(f"[REF] Resolver divergência de mercado: {ref_issues}")

        ref_cov = str(row.get("ref_text_coverage", "")).upper()
        if ref_cov and ref_cov not in ("", "TOTAL", "COMPLETA", "OK"):
            gaps = str(row.get("ref_coverage_gaps", "")).strip()
            issues.append(f"[REF] Adicionar especificação em falta: {ref_cov}: {gaps}")

        # ── [IMG] Imagens ────────────────────────────────────────────
        img_q = str(row.get("img_qualidade", "")).upper()
        if img_q == "SUBSTITUIR":
            motivo = str(row.get("img_motivo", "")).strip()
            issues.append(f"[IMG] Substituir imagem de catálogo: {motivo}")
        elif img_q == "NAO_VERIFICADA":
            issues.append("[IMG] Anexar imagem padronizada (arquivo ausente)")

        # ── [DOC] Referência na documentação ─────────────────────────
        ref_obs = str(row.get("ref_obs_flag", "")).strip()
        if ref_obs:
            clean_ref = ref_obs.replace("[REF] ", "")
            issues.append(f"[DOC] Corrigir cadastro de PN: {clean_ref}")

        # ── Build final values ───────────────────────────────────────
        resumo = "\n".join(issues) if issues else ""

        if len(issues) == 0:
            classif = "✅ OK"
        else:
            classif = "⚠️ REVISAR"

        resumos.append(resumo)
        classifs.append(classif)

    df["resumo_validacao"]        = resumos
    df["classificacao_validacao"] = classifs

    if "score_validacao" in df.columns:
        df.drop(columns=["score_validacao"], inplace=True)
    if "pre_analise" in df.columns:
        df.drop(columns=["pre_analise"], inplace=True)

    # ── Print distribution ───────────────────────────────────────────
    total = len(df)
    ok_count      = sum(1 for c in classifs if "OK" in c)
    revisar_count = sum(1 for c in classifs if "REVISAR" in c)

    print(f"\n{'=' * 60}")
    print(f"[RESUMO VALIDAÇÃO] Total analisado: {total}")
    print(f"   ✅ OK       : {ok_count:>4d} : {100*ok_count/total:.0f}%)" if total else "")
    print(f"   ⚠️ REVISAR  : {revisar_count:>4d} : {100*revisar_count/total:.0f}%)" if total else "")
    print(f"{'=' * 60}\n")

    return df


# ===========================================================================
# MAIN PIPELINE
# ===========================================================================

# ===========================================================================
# PIPELINE — per-stage runners
# ===========================================================================
# Each run_stage_* function is fully self-contained:
#   • Accepts a DataFrame, runs the stage, exports its report, returns df.
#   • Safe to call individually or as part of run_validations().
#   • Guarantees all output columns exist even when the stage fails.
# ===========================================================================

def run_stage_leadtime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stage 1 — Lead Time.
    Validates Prazo_Entrega_Previsto: must be > 0 and a multiple of 30.
    Exports: Validation_LeadTime.xlsx
    Columns added: leadtime_invalido, leadtime_obs
    """
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
    """
    Stage 2 — GRPM.
    Validates Grupo_Mercadoria format, runs ML suggestion, then LLM decision layer.
    Exports: Validation_GRPM.xlsx  (TROCAR / INCERTO rows only)
    Columns added: grpm_formato_invalido, Grupo_Sugerido, Descricao_Grupo_Atual,
                   grpm_decisao_llm, grpm_novo_codigo, grpm_novo_descricao, grpm_justificativa
    """
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
    """
    Stage 3 — Texts PT/ES.
    Computes Qwen embedding similarity; audits low-score rows with AI.
    Exports: Validation_Texts.xlsx
    Columns added: similarity_score, Text_Analysis
    """
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
    """
    Stage 3.5 — OBS Pre-Check.
    Checks PN presence in Texto_Observacao_PT; extracts references for web search.
    Exports: Validation_OBS_PreCheck.xlsx
    Columns added: obs_pn_presente, obs_referencia_extraida, obs_pesquisa_vale,
                   obs_motivo, obs_sugestao_texto
    """
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
    """
    Stage 4 — Market Reference.
    Web-search validation of part numbers and text coverage.
    Exports: Validation_Reference.xlsx
    Columns added: ref_reference_found, ref_supplier, ref_url, ref_search_links,
                   ref_price_estimated, ref_currency, ref_availability,
                   ref_part_number_confirmed, ref_part_number_note,
                   ref_text_coverage, ref_coverage_gaps, ref_validation_issues
    """
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
    """
    Stage 5 — Image Validation.
    LLM vision analysis of material photos; flags outdated images.
    Exports: Validation_Images.xlsx
    Columns added: img_path, img_qualidade, img_motivo, img_substituir
    """
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
    """
    Stage 5.5 — Reference-in-OBS Validation.
    Checks whether the reference is documented per business rules:
      • If Texto_Dados_Basicos_PT is filled -> OK.
      • Otherwise PN must appear in both Numero_Peca_Fabricante and Texto_Observacao_PT.
    Columns added: ref_obs_flag
    """
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


def _print_summary(df: pd.DataFrame) -> None:
    """Prints a consolidated summary of all validation flags present in df."""
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


# ---------------------------------------------------------------------------
# Full pipeline orchestrator
# ---------------------------------------------------------------------------

# Ordered list of all stages — controls execution order in run_validations()
_ALL_STAGES: list[tuple[str, callable]] = [
    ("leadtime",  run_stage_leadtime),
    ("grpm",      run_stage_grpm),
    ("texts",     run_stage_texts),
    ("obs",       run_stage_obs),
    ("reference", run_stage_reference),
    # ("images",    run_stage_images),
    ("ref_obs",   run_stage_ref_obs),
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
                            Valid names: 'leadtime', 'grpm', 'texts', 'obs',
                                         'reference', 'images', 'ref_obs'.
                            If None (default), all stages run in order.
        export_debug_sheet: Whether to save DEBUG_Full.xlsx at the end (default True).

    Returns the enriched DataFrame.

    Examples:
        run_validations(df)                              # all stages
        run_validations(df, stages=["grpm"])             # GRPM only
        run_validations(df, stages=["texts", "obs"])     # texts + OBS
        run_validations(df, export_debug_sheet=False)    # skip debug export
    """
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

    # Consolidate all validation flags into human-readable summary
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