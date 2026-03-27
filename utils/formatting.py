"""
formatting.py — Shared formatting and display helpers used across the pipeline.

Consolidates duplicated utilities:
  - step_header:      Console box for stage start (was in analyzers/_base.py + emitters/_base.py)
  - strip_json_fences: Remove ```json fences from LLM output (was in validators/_base.py + search_service.py)
  - lang_instruction:  Language directive for LLM prompts (was in validators/_base.py + search_service.py)
  - configure_encoding: UTF-8 stdout/stderr setup (was in main.py + panel.py + search_service.py)
"""

from __future__ import annotations

import re
import sys


# ---------------------------------------------------------------------------
# Console formatting
# ---------------------------------------------------------------------------

def step_header(step_num: int, title: str, description: str = "") -> None:
    """Print a formatted stage header box."""
    bar = "─" * 56
    print(f"\n┌{bar}┐")
    print(f"│  [{step_num}] {title:<50}│")
    if description:
        print(f"│      {description:<50}│")
    print(f"└{bar}┘")


def pipeline_banner(title: str, n: int, lines: list[str] | None = None) -> None:
    """Print a pipeline start banner with optional extra lines."""
    bar = "═" * 58
    print(f"\n╔{bar}╗")
    print(f"║  {title} — {n} materiais{' ' * max(0, 27 - len(str(n)))}║")
    for line in (lines or []):
        print(f"║  {line:<56}║")
    print(f"╚{bar}╝")


def pipeline_footer(label: str, elapsed: float) -> None:
    """Print a pipeline completion footer."""
    bar = "═" * 58
    print(f"\n{bar}")
    print(f"  {label} em {elapsed:.1f}s")
    print(f"{bar}\n")


# ---------------------------------------------------------------------------
# LLM response helpers
# ---------------------------------------------------------------------------

def strip_json_fences(text: str) -> str:
    """Remove ```json ... ``` fences that some models add."""
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\n?", "", text)
        text = re.sub(r"\n?```$", "", text)
    return text.strip()


def lang_instruction(country: str) -> str:
    """Returns a language directive to append to LLM user messages."""
    return "Responda em português." if country == "BR" else "Responda en español."


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------

def configure_encoding() -> None:
    """Configure stdout/stderr for UTF-8 output on Windows terminals."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
