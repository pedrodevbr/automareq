"""
orchestrator.py — Analysis pipeline orchestration.

Contains:
  - run_stage_* wrappers (error handling per stage)
  - _ALL_STAGES registry
  - run_analysis() — main entry point
"""

from __future__ import annotations

import logging
import time
from typing import Optional

import pandas as pd

from core.analyzers._base import init_analysis_columns, step_header

logger = logging.getLogger(__name__)


# ===========================================================================
# Per-stage wrappers
# ===========================================================================

def run_stage_smit(df: pd.DataFrame, *, jira=None, **_kw) -> pd.DataFrame:
    """Stage: SMIT — JIRA read + enrichment."""
    from core.analyzers.stages.smit import run_smit
    try:
        df = run_smit(df, jira)
    except Exception as exc:
        logger.error("Stage SMIT failed: %s", exc)
    return df


def run_stage_frac(df: pd.DataFrame, *, jira=None, sap=None, **_kw) -> pd.DataFrame:
    """Stage: FRAC — JIRA creation + SAP MRP change."""
    from core.analyzers.stages.frac import run_frac
    try:
        df = run_frac(df, jira, sap)
    except Exception as exc:
        logger.error("Stage FRAC failed: %s", exc)
    return df


def run_stage_zstk(df: pd.DataFrame, *, ai_module=None, search_service=None, **_kw) -> pd.DataFrame:
    """Stage: ZSTK — business rules + AI + market search."""
    from core.analyzers.stages.zstk import run_zstk
    try:
        df = run_zstk(df, ai_module, search_service)
    except Exception as exc:
        logger.error("Stage ZSTK failed: %s", exc)
    return df


def run_stage_ad(df: pd.DataFrame, **_kw) -> pd.DataFrame:
    """Stage: AD — auto-decision + quote text."""
    from core.analyzers.stages.ad import run_ad
    try:
        df = run_ad(df)
    except Exception as exc:
        logger.error("Stage AD failed: %s", exc)
    return df


def run_stage_ana(df: pd.DataFrame, **_kw) -> pd.DataFrame:
    """Stage: ANA — manual analysis flag."""
    from core.analyzers.stages.ana import run_ana
    try:
        df = run_ana(df)
    except Exception as exc:
        logger.error("Stage ANA failed: %s", exc)
    return df


# ===========================================================================
# Stage registry
# ===========================================================================

_ALL_STAGES: list[tuple[str, callable]] = [
    ("smit", run_stage_smit),
    ("frac", run_stage_frac),
    ("zstk", run_stage_zstk),
    ("ad", run_stage_ad),
    ("ana", run_stage_ana),
]


# ===========================================================================
# Main entry point
# ===========================================================================

def run_analysis(
    df: pd.DataFrame,
    stages: Optional[list[str]] = None,
    use_jira: bool = True,
    use_search: bool = True,
) -> pd.DataFrame:
    """
    Executes the full (or partial) analysis pipeline.

    Args:
        df:         Input DataFrame (post-validation, post-calculation).
        stages:     Optional list of stage names to run.
                    Valid: 'smit', 'frac', 'zstk', 'ad', 'ana'.
                    If None, all stages run in order.
        use_jira:   Whether to enable JIRA operations.
        use_search: Whether to enable market search.

    Returns the enriched DataFrame.
    """
    bar = "═" * 58
    n = len(df)
    print(f"\n╔{bar}╗")
    print(f"║  PIPELINE DE ANÁLISE — {n} materiais{' ' * max(0, 27 - len(str(n)))}║")
    if stages:
        stage_str = ", ".join(stages)
        print(f"║  Stages: {stage_str:<47}║")
    print(f"║  Jira: {'ativo' if use_jira else 'desativado':<49}║")
    print(f"║  Pesquisa web: {'ativa' if use_search else 'desativada':<42}║")
    print(f"╚{bar}╝")

    t_total = time.time()

    # Initialize columns
    df = init_analysis_columns(df)
    if "pre_analise" not in df.columns:
        df["pre_analise"] = ""
    if "Analise_AI" not in df.columns:
        df["Analise_AI"] = ""

    # Validate requested stages
    active = set(stages or [n for n, _ in _ALL_STAGES])
    valid_names = {n for n, _ in _ALL_STAGES}
    invalid = active - valid_names
    if invalid:
        raise ValueError(
            f"Stage(s) desconhecido(s): {invalid}. "
            f"Válidos: {sorted(valid_names)}"
        )

    # Lazy service creation
    services = _create_services(active, use_jira, use_search)

    # Run stages
    for name, fn in _ALL_STAGES:
        if name in active:
            step_header(_stage_number(name), f"Análise — {name.upper()}")
            df = fn(df, **services)

    # Summary
    repor = int((df.get("Analise_AI", pd.Series()) == "REPOR").sum())
    nao_repor = int((df.get("Analise_AI", pd.Series()) == "NAO_REPOR").sum())
    outros = n - repor - nao_repor

    print(f"\n{'═' * 58}")
    print(f"  ANÁLISE CONCLUÍDA em {time.time() - t_total:.1f}s")
    print(f"  REPOR     : {repor:>4d}")
    print(f"  NAO_REPOR : {nao_repor:>4d}")
    print(f"  OUTROS    : {outros:>4d}  (VERIFICAR / ERRO_API / etc.)")
    print(f"{'═' * 58}\n")

    return df


def _stage_number(name: str) -> int:
    """Return a display number for the stage."""
    numbers = {"smit": 1, "frac": 2, "zstk": 3, "ad": 4, "ana": 5}
    return numbers.get(name, 0)


def _create_services(active: set[str], use_jira: bool, use_search: bool) -> dict:
    """Lazily create only the services needed by active stages."""
    services: dict = {}

    # JIRA + SAP needed by smit and frac
    if use_jira and (active & {"smit", "frac"}):
        try:
            from services.jira_service import JiraModule
            services["jira"] = JiraModule()
        except Exception as exc:
            logger.error("JiraModule init failed: %s", exc)
            services["jira"] = None

        try:
            from services.sap_service import SapManager
            services["sap"] = SapManager()
        except Exception as exc:
            logger.error("SapManager init failed: %s", exc)
            services["sap"] = None
    else:
        services["jira"] = None
        services["sap"] = None

    # AI module needed by zstk
    if "zstk" in active:
        try:
            from config.ai import ai_model_analysis
            from services.ai_service import AIModule
            services["ai_module"] = AIModule(model_name=ai_model_analysis)
        except Exception as exc:
            logger.error("AIModule init failed: %s", exc)
            services["ai_module"] = None

    # Search service needed by zstk
    if use_search and "zstk" in active:
        try:
            from services.search_service import ReferenceValidator
            services["search_service"] = ReferenceValidator()
        except Exception as exc:
            logger.error("ReferenceValidator init failed: %s", exc)
            services["search_service"] = None
    else:
        services["search_service"] = None

    return services
