"""Backward-compatible entry point. Import from core.emitters instead."""

from core.emitters import run_emission

__all__ = ["run_emission"]
