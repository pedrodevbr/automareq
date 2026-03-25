"""Backward-compatible entry point. Import from core.validators instead."""

from core.validators import run_validations

__all__ = ["run_validations"]
