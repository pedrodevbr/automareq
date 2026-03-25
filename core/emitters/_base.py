"""
_base.py — Shared helpers for emission stages.

Provides:
  - step_header: Console output for stage start
"""

from __future__ import annotations


def step_header(step_num: int, title: str, description: str = "") -> None:
    """Print a formatted stage header."""
    bar = "─" * 56
    print(f"\n┌{bar}┐")
    print(f"│  [{step_num}] {title:<50}│")
    if description:
        print(f"│      {description:<50}│")
    print(f"└{bar}┘")
