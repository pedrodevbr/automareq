"""
ai_stages.py — Backward-compatible re-exports from individual stage modules.

All implementations have been moved to core/validators/stages/:
  - grpm.py:      suggest_grpm_ml, decide_grpm_llm, validate_planner
  - texts.py:     calculate_text_similarity_batch, run_text_audit
  - obs.py:       run_obs_precheck
  - reference.py: run_reference_validation
  - images.py:    run_image_validation
"""

# GRPM stages
from core.validators.stages.grpm import (
    suggest_grpm_ml,
    decide_grpm_llm,
    validate_planner,
)

# Text stages
from core.validators.stages.texts import (
    calculate_text_similarity_batch,
    run_text_audit,
)

# OBS stage
from core.validators.stages.obs import run_obs_precheck

# Reference stage
from core.validators.stages.reference import run_reference_validation

# Image stage
from core.validators.stages.images import run_image_validation

__all__ = [
    "suggest_grpm_ml",
    "decide_grpm_llm",
    "validate_planner",
    "calculate_text_similarity_batch",
    "run_text_audit",
    "run_obs_precheck",
    "run_reference_validation",
    "run_image_validation",
]
