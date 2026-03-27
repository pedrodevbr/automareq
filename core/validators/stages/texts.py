"""
texts.py — Text validation stages: embedding similarity + AI audit.

Stage 3 of the validation pipeline.
"""

from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
from numpy.linalg import norm
from tqdm import tqdm

from config.ai import ai_model_analysis
from config.personnel import country_for_responsavel
from core.validators._base import (
    EMBED_BATCH_SIZE,
    EMBED_THRESHOLD,
    EMBEDDING_MODEL,
    MAX_AUDIT_WORKERS,
    LLMRunner,
    lang_instruction,
    strip_json_fences,
)
from core.validators.rules import validate_text_fields_static

logger = logging.getLogger(__name__)


# ===========================================================================
# Cosine similarity
# ===========================================================================

def _cosine(v1: np.ndarray, v2: np.ndarray) -> float:
    n1, n2 = norm(v1), norm(v2)
    return float(np.dot(v1, v2) / (n1 * n2)) if n1 and n2 else 0.0


# ===========================================================================
# Embedding-based similarity
# ===========================================================================

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


# ===========================================================================
# AI audit for low-similarity rows
# ===========================================================================

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
