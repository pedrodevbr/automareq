"""Backward-compatible entry point. Import from core.analyzers instead."""

from core.analyzers import run_analysis

__all__ = ["run_analysis"]
