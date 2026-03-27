"""
_base.py — Shared helpers for emission stages.

Re-exports step_header from utils.formatting for backward compatibility.
"""

from __future__ import annotations

from utils.formatting import step_header

__all__ = ["step_header"]
