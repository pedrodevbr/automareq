"""
search_service.py
=================
Market Reference Validation Service

Responsibilities:
  1. Web-search for a market reference matching the material description.
  2. Validate that Numero_Peca_Fabricante is confirmed in the found reference.
  3. AI-compare the found reference against Texto_PT to detect gaps/incompatibilities.

Cache:
  Results are persisted to a JSON file so identical queries are never sent twice.
  Cache is keyed by a SHA-256 hash of (part_number, texto_pt, texto_breve).
  TTL defaults to 30 days; stale entries are revalidated on next run.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = logging.getLogger(__name__)

try:
    from config.config import country_for_responsavel
except ImportError:
    def country_for_responsavel(resp_key: str) -> str:  # type: ignore[misc]
        return "BR"


def _lang_instruction(country: str) -> str:
    return "Responda em português." if country == "BR" else "Responda en español."

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _get_openrouter_client() -> OpenAI:
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise ValueError("OPENROUTER_API_KEY not found in environment / .env")
    return OpenAI(base_url="https://openrouter.ai/api/v1", api_key=api_key)


def _strip_markdown_json(text: str) -> str:
    """Remove ```json ... ``` fences that some models add."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\n", "", text)
        text = re.sub(r"\n```$", "", text)
    return text.strip()


# ---------------------------------------------------------------------------
# Disk-backed search cache
# ---------------------------------------------------------------------------

_CACHE_VERSION = 1          # bump to invalidate all existing entries
_DEFAULT_TTL_DAYS = 30


class SearchCache:
    """
    Thread-safe, disk-backed JSON cache for reference validation results.

    Key   : SHA-256 of (part_number | texto_pt | texto_breve) — normalised, lowercased.
    Value : ReferenceValidationResult.to_dict() + metadata (cached_at, cache_version).
    TTL   : entries older than ``ttl_days`` are treated as misses and revalidated.

    File layout  : one JSON object, keys are hex hashes.
    Persistence  : flushed to disk after every write (safe for crashes / interruptions).
    """

    def __init__(
        self,
        cache_path: str | Path = "cache/search_cache.json",
        ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        self.path     = Path(cache_path)
        self.ttl      = timedelta(days=ttl_days)
        self._lock    = threading.Lock()
        self._data: dict = {}
        self._load()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get(self, key: str) -> dict | None:
        """Return cached result dict if present and not stale, else None."""
        with self._lock:
            entry = self._data.get(key)
        if entry is None:
            return None
        if entry.get("cache_version") != _CACHE_VERSION:
            return None
        try:
            cached_at = datetime.fromisoformat(entry["cached_at"])
        except (KeyError, ValueError):
            return None
        if datetime.utcnow() - cached_at > self.ttl:
            logger.debug("Cache entry expired for key %s", key[:12])
            return None
        return entry.get("result")

    def set(self, key: str, result: dict) -> None:
        """Persist a result dict under *key*."""
        entry = {
            "cached_at":     datetime.utcnow().isoformat(),
            "cache_version": _CACHE_VERSION,
            "result":        result,
        }
        with self._lock:
            self._data[key] = entry
            self._flush()

    def invalidate(self, key: str) -> None:
        """Remove a single entry."""
        with self._lock:
            self._data.pop(key, None)
            self._flush()

    def clear_expired(self) -> int:
        """Purge stale entries. Returns number removed."""
        cutoff = datetime.utcnow() - self.ttl
        removed = 0
        with self._lock:
            for k in list(self._data):
                try:
                    ts = datetime.fromisoformat(self._data[k].get("cached_at", ""))
                    if ts < cutoff or self._data[k].get("cache_version") != _CACHE_VERSION:
                        del self._data[k]
                        removed += 1
                except ValueError:
                    del self._data[k]
                    removed += 1
            if removed:
                self._flush()
        return removed

    def stats(self) -> dict:
        with self._lock:
            total = len(self._data)
        return {"total_entries": total, "cache_path": str(self.path), "ttl_days": self.ttl.days}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def make_key(part_number: str, texto_pt: str, texto_breve: str) -> str:
        """Stable hash key from the three search-determining fields."""
        raw = "|".join([
            re.sub(r"[\s\-]", "", part_number).lower(),
            texto_pt.strip().lower(),
            texto_breve.strip().lower(),
        ])
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _load(self) -> None:
        if self.path.exists():
            try:
                with open(self.path, encoding="utf-8") as f:
                    self._data = json.load(f)
                logger.info("Cache loaded: %d entries from %s", len(self._data), self.path)
            except Exception as exc:
                logger.warning("Could not load cache (%s), starting fresh.", exc)
                self._data = {}
        else:
            self._data = {}

    def _flush(self) -> None:
        """Write current state to disk (must be called under self._lock)."""
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._data, f, ensure_ascii=False, indent=2)
            tmp.replace(self.path)          # atomic rename
        except Exception as exc:
            logger.error("Cache flush failed: %s", exc)



# ---------------------------------------------------------------------------
# Data contract for reference validation results
# ---------------------------------------------------------------------------

@dataclass
class ReferenceValidationResult:
    # What was found on the market
    reference_found: Optional[str] = None       # Exact name / model found
    supplier: str = ""
    url: str = ""                                   # Primary product URL
    search_links: str = ""                          # All sources consulted, newline-separated
    price_estimated: Optional[float] = None
    currency: str = ""
    availability: str = "Não Verificado"

    # Part-number check
    part_number_confirmed: bool = False          # True  -> PN found in reference
    part_number_note: str = ""                   # Explanation

    # Text coverage: does the reference satisfy every spec in Texto_PT?
    text_coverage: str = "NAO_VERIFICADO"        # COMPLETO | PARCIAL | INCOMPATIVEL | NAO_VERIFICADO
    coverage_gaps: str = ""                      # Missing or incompatible attributes

    # Consolidated issue string (empty = OK)
    validation_issues: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Reference Validator  (new, primary class)
# ---------------------------------------------------------------------------

_REFERENCE_SYSTEM_PROMPT = """\
Você é um especialista em compras industriais e auditor de dados mestre SAP.

Dado os atributos de um material, você deve:
1. Pesquisar na web o produto/peça comercial que melhor corresponde ao item.
2. Confirmar se o número de peça do fabricante (quando informado) está presente na referência encontrada.
3. Avaliar se a referência encontrada satisfaz COMPLETAMENTE todas as especificações técnicas
   descritas no campo Texto_PT. Aponte lacunas ou incompatibilidades.

REGRAS IMPORTANTES:
- O texto PT vem de sistema legado: CAIXA ALTA, sem acentuação ou cedilha. Isso é normal.
- A comparação do part number é CASE-INSENSITIVE e ignora hífens/espaços.
- Valores para text_coverage:
    "COMPLETO"       – referência atende TODOS os requisitos do Texto_PT
    "PARCIAL"        – referência atende a maioria mas tem lacunas menores
    "INCOMPATIVEL"   – produto diferente ou falta requisito crítico
    "NAO_VERIFICADO" – não foi possível localizar referência confiável

Responda no idioma indicado na mensagem. Responda APENAS com um JSON válido — sem preâmbulo, sem markdown.
Schema:
{
  "reference_found":        "<nome/modelo exato encontrado, ou null>",
  "supplier":               "<nome do fornecedor>",
  "url":                    "<link direto para o produto>",
  "search_links":           ["<url1>", "<url2>", "..."],
  "price_estimated":        <valor numérico ou null>,
  "currency":               "<BRL|USD|EUR|''>",
  "availability":           "<Em Estoque|Sob Encomenda|Indisponível|Não Verificado>",
  "part_number_confirmed":  <true|false>,
  "part_number_note":       "<breve explicação sobre a correspondência do PN>",
  "text_coverage":          "<COMPLETO|PARCIAL|INCOMPATIVEL|NAO_VERIFICADO>",
  "coverage_gaps":          "<lista de especificações ausentes/incompatíveis, ou string vazia>"
}
"""

_REFERENCE_USER_TEMPLATE = """\
DADOS DO MATERIAL:
- Código SAP      : {codigo}
- Texto Breve     : {texto_breve}
- Descrição PT    : {texto_pt}
- PN Fabricante   : {part_number}
- Info Adicional  : {texto_desc}

Tarefa:
1. Encontre a referência comercial para este material (prioridade: fornecedores brasileiros, depois internacional).
2. Confirme se o PN "{part_number}" está presente na referência.
3. Avalie se a referência satisfaz COMPLETAMENTE as especificações da descrição PT.

{lang}"""


class ReferenceValidator:
    """
    Validates each material row against a live market reference.

    For every row it:
      * Uses a web-enabled LLM (perplexity/sonar or similar) to search the item.
      * Checks that Numero_Peca_Fabricante is confirmed in the reference.
      * Compares the reference completeness against Texto_PT.
      * Reads from / writes to a SearchCache to avoid redundant API calls.

    Result columns added to DataFrame (prefix ``ref_``):
      ref_reference_found, ref_supplier, ref_url, ref_search_links,
      ref_price_estimated, ref_currency, ref_availability,
      ref_part_number_confirmed, ref_part_number_note,
      ref_text_coverage, ref_coverage_gaps, ref_validation_issues
    """

    RESULT_PREFIX = "ref_"

    DEFAULT_SEARCH_MODEL = "perplexity/sonar"
    FALLBACK_MODEL       = "google/gemini-3.1-flash-lite-preview"

    def __init__(
        self,
        model_name: Optional[str] = None,
        cache: Optional[SearchCache] = None,
        cache_path: str | Path = "cache/search_cache.json",
        cache_ttl_days: int = _DEFAULT_TTL_DAYS,
    ) -> None:
        self.client     = _get_openrouter_client()
        self.model_name = model_name or self.DEFAULT_SEARCH_MODEL
        self.cache      = cache or SearchCache(cache_path=cache_path, ttl_days=cache_ttl_days)
        expired = self.cache.clear_expired()
        logger.info(
            "ReferenceValidator ready | model=%s | cache=%s | entries=%d | expired_purged=%d",
            self.model_name,
            self.cache.path,
            self.cache.stats()["total_entries"],
            expired,
        )

    # ------------------------------------------------------------------
    # Core per-row logic
    # ------------------------------------------------------------------

    def validate_row(self, row: dict) -> ReferenceValidationResult:
        """
        Validates a single material row with cache-aside pattern:
          1. Compute cache key from (part_number, texto_pt, texto_breve).
          2. Return cached result immediately if fresh.
          3. Call the API, store result in cache, return.
        """
        codigo      = str(row.get("Codigo_Material", ""))
        texto_breve = str(row.get("Texto_Breve_Material", ""))
        texto_pt    = str(row.get("Texto_PT", ""))
        pn_field    = str(row.get("Numero_Peca_Fabricante", "") or "").strip()
        texto_desc  = str(row.get("Texto_Dados_Basicos_PT", "") or "")
        responsavel = str(row.get("Responsavel", ""))

        obs_ref     = str(row.get("obs_referencia_extraida", "") or "").strip()
        part_number = obs_ref if obs_ref else pn_field

        # Cache lookup
        cache_key = SearchCache.make_key(part_number, texto_pt, texto_breve)
        cached    = self.cache.get(cache_key)
        if cached is not None:
            logger.debug("Cache HIT for %s (key %s)", codigo, cache_key[:12])
            return ReferenceValidationResult(**cached)

        logger.debug("Cache MISS for %s — calling API", codigo)
        result = self._call_api(
            codigo, texto_breve, texto_pt, part_number,
            texto_desc, responsavel,
        )
        self.cache.set(cache_key, result.to_dict())
        return result

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=12))
    def _call_api(
        self,
        codigo: str,
        texto_breve: str,
        texto_pt: str,
        part_number: str,
        texto_desc: str,
        responsavel: str,
    ) -> ReferenceValidationResult:
        """Makes the actual LLM call. Retried up to 3x on transient errors."""
        country  = country_for_responsavel(responsavel)
        user_msg = _REFERENCE_USER_TEMPLATE.format(
            codigo=codigo,
            texto_breve=texto_breve,
            texto_pt=texto_pt,
            part_number=part_number if part_number else "Não informado",
            texto_desc=texto_desc,
            lang=_lang_instruction(country),
        )

        try:
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": _REFERENCE_SYSTEM_PROMPT},
                    {"role": "user",   "content": user_msg},
                ],
                temperature=0.1,
            )
            raw = response.choices[0].message.content or ""
            clean = _strip_markdown_json(raw)
            data: dict = json.loads(clean)

            # Perplexity/sonar returns native citations on the response object.
            # Collect them as a complement to whatever the model put in search_links.
            native_citations: list[str] = []
            if hasattr(response, "citations") and response.citations:
                native_citations = [str(c) for c in response.citations]

        except json.JSONDecodeError:
            logger.warning("JSON decode error for %s. Raw: %.120s", codigo, raw)
            err = ReferenceValidationResult()
            err.validation_issues = "Erro: resposta da IA não é JSON válido"
            return err

        except Exception as exc:
            logger.error("API error for %s: %s", codigo, exc)
            err = ReferenceValidationResult()
            err.validation_issues = f"Erro API: {exc}"
            return err

        # Merge search_links from JSON + native citations, deduplicated, order preserved
        json_links: list[str] = data.get("search_links") or []
        if isinstance(json_links, str):
            json_links = [u.strip() for u in json_links.split("\n") if u.strip()]
        all_links = list(dict.fromkeys(                  # dedup, preserve order
            u for u in (json_links + native_citations) if u
        ))

        result = ReferenceValidationResult(
            reference_found       = data.get("reference_found"),
            supplier              = data.get("supplier", ""),
            url                   = data.get("url", ""),
            search_links          = "\n".join(all_links),
            price_estimated       = data.get("price_estimated"),
            currency              = data.get("currency", ""),
            availability          = data.get("availability", "Não Verificado"),
            part_number_confirmed = bool(data.get("part_number_confirmed", False)),
            part_number_note      = data.get("part_number_note", ""),
            text_coverage         = data.get("text_coverage", "NAO_VERIFICADO"),
            coverage_gaps         = data.get("coverage_gaps", ""),
        )

        # Build consolidated validation_issues string
        issues: list[str] = []

        if part_number and not result.part_number_confirmed:
            issues.append(
                f"[PN NÃO CONFIRMADO] PN '{part_number}' não encontrado na referência: "
                f"{result.part_number_note}"
            )

        if result.text_coverage == "PARCIAL":
            issues.append(f"[COBERTURA PARCIAL] Gaps: {result.coverage_gaps}")
        elif result.text_coverage == "INCOMPATIVEL":
            issues.append(f"[REFERÊNCIA INCOMPATÍVEL] {result.coverage_gaps}")
        elif result.text_coverage == "NAO_VERIFICADO":
            issues.append("[SEM REFERÊNCIA] Não foi possível localizar referência confiável.")

        result.validation_issues = "\n".join(issues)

        return result

    # ------------------------------------------------------------------
    # Batch runner
    # ------------------------------------------------------------------

    def run_batch(
        self,
        df: pd.DataFrame,
        max_workers: int = 5,
    ) -> pd.DataFrame:
        """
        Runs validate_row for every row in *df* in parallel.
        Cache hits are served immediately without touching the API.

        Returns a new DataFrame with ``ref_*`` columns aligned to *df*'s index.
        Does NOT mutate the original DataFrame.
        """
        if df.empty:
            return pd.DataFrame()

        records = df.to_dict("records")
        results: list[dict] = [{}] * len(records)

        # Pre-count cache hits for the progress message
        hits = sum(
            1 for r in records
            if self.cache.get(SearchCache.make_key(
                str(r.get("obs_referencia_extraida", "") or r.get("Numero_Peca_Fabricante", "") or ""),
                str(r.get("Texto_PT", "")),
                str(r.get("Texto_Breve_Material", "")),
            )) is not None
        )
        misses = len(records) - hits
        print(
            f"[SEARCH] {len(records)} itens | cache hits: {hits} | API calls: {misses}"
        )

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_map = {
                pool.submit(self.validate_row, rec): idx
                for idx, rec in enumerate(records)
            }
            for future in tqdm(future_map, total=len(records), desc="Ref Validation"):
                idx = future_map[future]
                try:
                    results[idx] = future.result().to_dict()
                except Exception as exc:
                    logger.error("Unexpected error on row %d: %s", idx, exc)
                    results[idx] = ReferenceValidationResult(
                        validation_issues=f"Erro inesperado: {exc}"
                    ).to_dict()

        result_df = pd.DataFrame(results, index=df.index)
        result_df.columns = [f"{self.RESULT_PREFIX}{c}" for c in result_df.columns]
        return result_df

    def run_analysis_search(
        self,
        df: pd.DataFrame,
        max_workers: int = 3,
    ) -> pd.DataFrame:
        """
        Adapter for the analysis pipeline.

        Calls run_batch() and maps ref_* columns to the 7 flat column names
        expected by the analysis pipeline (produto_identificado, preco_unitario_estimado, etc.).
        """
        ref_df = self.run_batch(df, max_workers=max_workers)
        if ref_df.empty:
            return pd.DataFrame(index=df.index)

        # Map ref_* columns → analysis column names
        mapping = {
            "ref_reference_found": "produto_identificado",
            "ref_price_estimated": "preco_unitario_estimado",
            "ref_currency": "moeda",
            "ref_url": "url_fonte",
            "ref_availability": "disponibilidade",
            "ref_part_number_note": "analise_confianca",
            "ref_supplier": "fornecedor_principal",
        }

        result = pd.DataFrame(index=df.index)
        for ref_col, analysis_col in mapping.items():
            if ref_col in ref_df.columns:
                result[analysis_col] = ref_df[ref_col]
            else:
                result[analysis_col] = None

        return result