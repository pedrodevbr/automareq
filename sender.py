"""
sender.py — Backward-compatible entry point.
=========
Zips each Responsavel output folder and creates an Outlook draft.

Usage:
    python sender.py                        # creates drafts for all folders
    python sender.py ACOSTAJ LUCASD         # creates drafts for listed folders
"""

from __future__ import annotations

import sys
from pathlib import Path

from core.emitters.stages.send_drafts import send, send_all  # noqa: F401

__all__ = ["send", "send_all"]


if __name__ == "__main__":
    from config.paths import OUTPUT_FOLDER

    targets = [a.upper() for a in sys.argv[1:]] or None
    send_all(base_path=Path(OUTPUT_FOLDER), only=targets)
