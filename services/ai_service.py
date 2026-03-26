"""
ai_service.py — AI-powered stock analysis module.

Uses LLMRunner for OpenRouter client management. Provides:
  - MaterialAnalysis: Pydantic schema for structured AI output
  - AIModule: batch analysis of materials with pre-checks and parallel execution
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, Field
from tenacity import retry, stop_after_attempt, wait_exponential

from config.sources import ANALYSIS_COLUMNS
from config.prompts import ACTION_SUGGESTION_ADDENDUM, SYSTEM_PROMPT, USER_PROMPT_TEMPLATE
from core.validators._base import LLMRunner

logger = logging.getLogger(__name__)


# --- Pydantic schema for structured AI output ---

class MaterialAnalysis(BaseModel):
    Analise_AI: str = Field(..., description="Resumo da decisão: 'REPOR', 'NAO_REPOR', 'SEM_CONSUMO', 'ANALISE_MANUAL', etc.")
    Quantidade_OP_AI: Optional[float] = Field(None, description="Quantidade sugerida de compra (apenas números)")
    PR_AI: Optional[float] = Field(None, description="Ponto de Reposição sugerido")
    MAX_AI: Optional[float] = Field(None, description="Estoque Máximo sugerido")
    Politica_AI: Optional[str] = Field(None, description="Política de estoque sugerida (ex: ZS, ES, ZD)")
    Comentario: str = Field(..., description="Explicação detalhada e técnica da decisão tomada e recomendações")
    acoes_sugeridas: List[str] = Field(
        default_factory=list,
        description="Lista de 1-5 ações específicas para o analista executar",
    )


class AIModule:
    def __init__(self, model_name=None):
        self.model_name = model_name or "google/gemini-2.0-flash-001"
        logger.info("AIModule initialized with model: %s", self.model_name)

    def format_row(self, row) -> str:
        """Format a row (Series or dict) for the analysis prompt."""
        lines = []
        for k in ANALYSIS_COLUMNS:
            v = row.get(k)
            if pd.notna(v) and v != "":
                lines.append(f"{k}: {v}")

        # Include JIRA context if available
        jira_cols = ["jira_historico_resumo", "jira_acao_sugerida", "jira_status_atual"]
        for k in jira_cols:
            v = row.get(k)
            if pd.notna(v) and v != "":
                lines.append(f"{k}: {v}")

        return "\n".join(lines)

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def analyze_material_raw(self, row) -> dict:
        """Analyze a single material and return a validated dict."""
        material = row.get("Codigo_Material")

        # Pre-check: new item without history
        if pd.isna(row.get("LTD_1")) or row.get("LTD_1") == "":
            return {
                "Analise_AI": "REPOR",
                "Quantidade_OP_AI": None,
                "PR_AI": None,
                "MAX_AI": None,
                "Politica_AI": None,
                "Comentario": "Codigo novo - LTD_1 vazio ou inválido (sem histórico recente).",
            }

        # Pre-check: all consumption zero
        ltd_keys = [c for c in (row.index if hasattr(row, "index") else row.keys()) if str(c).startswith("LTD_")]
        ltd_vals = pd.to_numeric(pd.Series({k: row.get(k) for k in ltd_keys}), errors="coerce")
        if ltd_vals.dropna().eq(0).all():
            return {
                "Analise_AI": "VERIFICAR",
                "Quantidade_OP_AI": None,
                "PR_AI": None,
                "MAX_AI": None,
                "Politica_AI": None,
                "Comentario": "Codigo velho - Todos os consumos zero.",
            }

        prompt = USER_PROMPT_TEMPLATE.format(material_data=self.format_row(row))
        system = SYSTEM_PROMPT + "\n\n" + ACTION_SUGGESTION_ADDENDUM

        try:
            response = LLMRunner.client().chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "material_analysis",
                        "schema": MaterialAnalysis.model_json_schema(),
                    },
                },
                extra_headers={
                    "HTTP-Referer": "https://localhost",
                    "X-Title": "StockAnalyzer",
                },
                temperature=0.1,
            )

            content = response.choices[0].message.content
            parsed_data = json.loads(content)
            validated = MaterialAnalysis(**parsed_data)
            return validated.model_dump()

        except Exception as e:
            logger.error("Erro AI para material %s: %s", material, e)
            raise

    def _safe_analyze_wrapper(self, args):
        """Wrapper for ThreadPool: (index, row) -> (index, result)."""
        index, row = args
        try:
            result = self.analyze_material_raw(row)
            return index, result
        except Exception as e:
            return index, {
                "Analise_AI": "ERRO_API",
                "Comentario": f"Falha técnica: {str(e)}",
            }

    def analyze_batch(self, df: pd.DataFrame, max_workers: int = 3) -> pd.DataFrame:
        """Process a DataFrame in parallel and return results aligned by index."""
        if df.empty:
            return pd.DataFrame()

        from concurrent.futures import ThreadPoolExecutor
        from tqdm import tqdm

        print(f"   Iniciando IA ({self.model_name}) para {len(df)} itens em {max_workers} threads...")

        items_to_process = list(df.iterrows())
        results_map = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = list(
                tqdm(
                    executor.map(self._safe_analyze_wrapper, items_to_process),
                    total=len(df),
                    desc="Processando IA",
                )
            )
            for idx, data in futures:
                results_map[idx] = data

        df_results = pd.DataFrame.from_dict(results_map, orient="index")

        for field in MaterialAnalysis.model_fields.keys():
            if field not in df_results.columns:
                df_results[field] = None

        return df_results

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def simple_chat(self, system_message: str, user_message: str) -> str:
        """Generic text chat. Returns raw string response."""
        try:
            return LLMRunner.chat(
                self.model_name, system_message, user_message,
                temperature=0.2,
                response_format=None,
            )
        except Exception as e:
            logger.error("Simple chat error: %s", e)
            raise
