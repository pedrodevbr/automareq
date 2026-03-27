"""
orchestrator.py — Emission pipeline orchestration.

Contains:
  - run_stage_* wrappers (error handling per stage)
  - _ALL_STAGES registry
  - run_emission() — main entry point
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from core.base_orchestrator import BaseOrchestrator
from utils.formatting import pipeline_banner, pipeline_footer, step_header

logger = logging.getLogger(__name__)


# ===========================================================================
# Per-stage wrappers
# ===========================================================================

def run_stage_dashboard(df: pd.DataFrame, **kw) -> pd.DataFrame:
    """Stage: dashboard — Export HTML + JS dashboard per analyst."""
    from core.emitters.stages.dashboard import export_dashboard_data
    try:
        output_folder = kw.get("output_folder")
        if output_folder:
            export_dashboard_data(df, output_folder=output_folder)
        else:
            export_dashboard_data(df)
        print(f"   Dashboard exported for {df['Responsavel'].nunique() if 'Responsavel' in df.columns else 1} analyst(s).")
    except Exception as exc:
        logger.error("Stage dashboard failed: %s", exc)
    return df


def run_stage_groups(df: pd.DataFrame, **kw) -> pd.DataFrame:
    """Stage: groups — Separate into AD/ZSTK folder structure."""
    from core.emitters.stages.group_separation import separar_por_setor_grupo_taxacao
    try:
        input_file = kw.get("input_file_path")
        separar_por_setor_grupo_taxacao(input_file_path=input_file)
    except Exception as exc:
        logger.error("Stage groups failed: %s", exc)
    return df


def run_stage_templates(df: pd.DataFrame, **kw) -> pd.DataFrame:
    """Stage: templates — Fill and convert procurement templates."""
    logger.info("Templates stage: use solicitar_aprovacao_cpv() for per-item processing.")
    return df


def run_stage_send(df: pd.DataFrame, **kw) -> pd.DataFrame:
    """Stage: send — Zip folders and create Outlook email drafts."""
    from core.emitters.stages.send_drafts import send_all
    try:
        base_path = kw.get("base_path")
        only = kw.get("only")
        if base_path:
            send_all(base_path=Path(base_path), only=only)
        else:
            send_all(only=only)
    except Exception as exc:
        logger.error("Stage send failed: %s", exc)
    return df


# ===========================================================================
# Stage registry
# ===========================================================================

_ALL_STAGES: list[tuple[str, callable]] = [
    ("dashboard", run_stage_dashboard),
    ("groups",    run_stage_groups),
    ("templates", run_stage_templates),
    ("send",      run_stage_send),
]

_STAGE_NUMBERS = {"dashboard": 1, "groups": 2, "templates": 3, "send": 4}


# ===========================================================================
# Main entry point
# ===========================================================================

def run_emission(
    df: pd.DataFrame,
    stages: Optional[list[str]] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Executes the full (or partial) emission pipeline.

    Args:
        df:         Input DataFrame (post-analysis).
        stages:     Optional list of stage names to run.
                    Valid: 'dashboard', 'groups', 'templates', 'send'.
                    If None, all stages run in order.
        **kwargs:   Extra arguments passed to stage wrappers
                    (e.g. output_folder, input_file_path, base_path, only).

    Returns the DataFrame (unchanged — emission stages produce side-effects).
    """
    # Validate stage names
    valid_names = {n for n, _ in _ALL_STAGES}
    active = set(stages) if stages else valid_names
    invalid = active - valid_names
    if invalid:
        raise ValueError(
            f"Stage(s) desconhecido(s): {invalid}. "
            f"Válidos: {sorted(valid_names)}"
        )

    pipeline_banner("PIPELINE DE EMISSÃO", len(df), [
        f"Stages: {', '.join(stages)}" if stages else "",
    ])

    t_total = time.time()

    for name, fn in _ALL_STAGES:
        if name in active:
            step_header(_STAGE_NUMBERS.get(name, 0), f"Emissão — {name.upper()}")
            df = fn(df, **kwargs)

    pipeline_footer("EMISSÃO CONCLUÍDA", time.time() - t_total)
    return df
