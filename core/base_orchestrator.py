"""
base_orchestrator.py — Base class for pipeline sub-orchestrators.

Provides shared infrastructure for the validation, analysis, and emission
orchestrators, eliminating duplicated patterns:
  - Stage registry management
  - Stage name validation
  - Sequential stage execution
  - Pipeline banner/footer display
"""

from __future__ import annotations

import logging
import time
from typing import Callable, Optional

import pandas as pd

from utils.formatting import pipeline_banner, pipeline_footer

logger = logging.getLogger(__name__)

# Type alias for stage functions
StageFunction = Callable[..., pd.DataFrame]


class BaseOrchestrator:
    """
    Base class that encapsulates the common orchestration pattern:
      1. Validate requested stage names
      2. Display a banner
      3. Run stages in registry order
      4. Display a footer with elapsed time

    Subclasses define:
      - ``pipeline_name``: Display name for banners (e.g., "PIPELINE DE VALIDAÇÕES")
      - ``stage_registry``: List of (name, function) tuples in execution order
    """

    pipeline_name: str = "PIPELINE"
    stage_registry: list[tuple[str, StageFunction]] = []

    @classmethod
    def valid_stage_names(cls) -> set[str]:
        """Return the set of valid stage names."""
        return {name for name, _ in cls.stage_registry}

    @classmethod
    def validate_stages(cls, stages: Optional[list[str]]) -> set[str]:
        """
        Resolve and validate the set of stages to run.

        If *stages* is None, all stages in the registry run.
        Raises ValueError for unknown stage names.
        """
        all_names = cls.valid_stage_names()
        active = set(stages) if stages else all_names
        invalid = active - all_names
        if invalid:
            raise ValueError(
                f"Stage(s) desconhecido(s): {invalid}. "
                f"Válidos: {sorted(all_names)}"
            )
        return active

    @classmethod
    def run_stages(
        cls,
        df: pd.DataFrame,
        stages: Optional[list[str]] = None,
        **kwargs,
    ) -> pd.DataFrame:
        """
        Execute stages in registry order, filtering by *stages*.

        Extra keyword arguments are passed through to each stage function.
        Returns the (possibly modified) DataFrame.
        """
        active = cls.validate_stages(stages)

        pipeline_banner(cls.pipeline_name, len(df), [
            f"Stages: {', '.join(s for s in (stages or sorted(active)))}"
        ] if stages else [])

        t_total = time.time()

        for name, fn in cls.stage_registry:
            if name in active:
                df = fn(df, **kwargs)

        pipeline_footer(f"{cls.pipeline_name} CONCLUÍDO", time.time() - t_total)
        return df
