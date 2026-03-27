"""
grpm.py — GRPM validation stages: ML suggestion + LLM decision + Planner classification.

Stage 2b-c of the validation pipeline.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional

import pandas as pd
from tqdm import tqdm

from config.ai import ai_model_analysis, ai_model_text
from config.personnel import PLANEJADORES, country_for_responsavel
from core.validators._base import (
    EMBEDDING_MODEL,
    MAX_AUDIT_WORKERS,
    TEXT_COLUMN,
    LLMRunner,
    lang_instruction,
    run_llm_parallel,
    strip_json_fences,
)
from core.validators.rules import ClassificationResponse, MaterialClassification

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
    from utils.export_core import export_by_responsavel

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
