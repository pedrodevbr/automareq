"""
_base.py — Shared infrastructure for validation stages.

Provides:
  - LLMRunner:        Lazy-init OpenAI client (no crash at import time)
  - run_llm_parallel: Generic concurrent LLM execution over DataFrame rows
  - Shared helpers:   JSON fence stripping, language instructions, PN matching
  - Constants:        Embedding model, thresholds, batch sizes
"""

from __future__ import annotations

import os
import re
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List

import pandas as pd
from tqdm import tqdm

from config.ai import ai_model_text
from config.personnel import country_for_responsavel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

EMBEDDING_MODEL = ai_model_text
TEXT_COLUMN = "Texto_PT"
EMBED_THRESHOLD = 0.99       # cosine similarity above which texts are approved without AI
EMBED_BATCH_SIZE = 50
MAX_AUDIT_WORKERS = 8


# ---------------------------------------------------------------------------
# LLMRunner — lazy-init OpenAI client
# ---------------------------------------------------------------------------

class LLMRunner:
    """
    Lazy singleton for OpenRouter API access.

    The client is created on first use, not at import time, so modules that
    don't call any LLM methods won't crash when OPENROUTER_API_KEY is missing.

    For testing, replace ``LLMRunner._client`` with a mock via monkeypatch.
    """

    _client = None

    @classmethod
    def client(cls):
        if cls._client is None:
            from openai import OpenAI

            key = os.getenv("OPENROUTER_API_KEY")
            if not key:
                raise ValueError("OPENROUTER_API_KEY missing from environment / .env")
            cls._client = OpenAI(base_url="https://openrouter.ai/api/v1", api_key=key)
        return cls._client

    @classmethod
    def chat(cls, model: str, system: str, user: str, **kwargs) -> str:
        """Single-shot text chat completion. Returns raw content string."""
        resp = cls.client().chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            temperature=kwargs.get("temperature", 0.1),
            response_format=kwargs.get("response_format", {"type": "json_object"}),
        )
        return resp.choices[0].message.content or ""

    @classmethod
    def chat_raw(cls, model: str, messages: list, **kwargs) -> str:
        """Chat completion with pre-built messages (for multimodal / vision)."""
        resp = cls.client().chat.completions.create(
            model=model,
            messages=messages,
            temperature=kwargs.get("temperature", 0.1),
            response_format=kwargs.get("response_format"),
        )
        return resp.choices[0].message.content or ""

    @classmethod
    def embed(cls, model: str, texts: List[str]) -> list:
        """Batch embedding. Returns list of embedding vectors."""
        resp = cls.client().embeddings.create(model=model, input=texts)
        return [e.embedding for e in resp.data]


# ---------------------------------------------------------------------------
# Concurrent LLM execution
# ---------------------------------------------------------------------------

def run_llm_parallel(
    df: pd.DataFrame,
    mask: pd.Series,
    row_fn: Callable[[dict], dict],
    output_columns: List[str],
    defaults: Dict[str, Any],
    max_workers: int = MAX_AUDIT_WORKERS,
    desc: str = "LLM",
) -> pd.DataFrame:
    """
    Run *row_fn* in parallel over rows where *mask* is True.

    For each selected row (converted to dict), calls row_fn and writes
    the returned dict values into *output_columns*. On error, writes *defaults*.

    Initialises output_columns with empty strings for all rows first.
    """
    for col in output_columns:
        if col not in df.columns:
            df[col] = defaults.get(col, "")

    to_process = df[mask]
    if to_process.empty:
        return df

    records = to_process.to_dict("records")
    results: list[dict] = [{}] * len(records)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_map = {pool.submit(row_fn, rec): i for i, rec in enumerate(records)}
        for fut in tqdm(as_completed(future_map), total=len(records), desc=desc):
            idx = future_map[fut]
            try:
                results[idx] = fut.result()
            except Exception as exc:
                logger.error("%s error at position %d: %s", desc, idx, exc)
                results[idx] = defaults.copy()

    # Write results back
    proc_indices = to_process.index.tolist()
    for pos, row_result in enumerate(results):
        df_idx = proc_indices[pos]
        for col in output_columns:
            if col in row_result:
                df.at[df_idx, col] = row_result[col]

    return df


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def strip_json_fences(text: str) -> str:
    """Remove ```json ... ``` fences that some models add."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def lang_instruction(country: str) -> str:
    """Returns a language directive to append to user messages."""
    return "Responda em português." if country == "BR" else "Responda en español."


def check_pn_in_obs_static(pn: str, obs: str) -> bool:
    """Case-insensitive PN presence check, ignoring hyphens and spaces."""
    if not pn or not obs:
        return False
    normalize = lambda s: re.sub(r"[\s\-]", "", s).upper()
    return normalize(pn) in normalize(obs)
