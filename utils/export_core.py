"""
export_core.py — Shared export functions used across the pipeline.

Provides:
  - export_by_responsavel:  Split DataFrame per analyst → Excel files
  - export_debug:           Full DataFrame dump for debugging
  - _sanitize:              Filesystem-safe name helper
  - _format_group_code:     Group code formatting helper
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Union

import pandas as pd

from config.paths import OUTPUT_FOLDER
from utils.columns import (
    EXPORT_COLUMNS,
    _DEBUG_PRIORITY_COLUMNS,
    _select_export_columns,
)
from utils.excel import _ensure_dir, save_excel

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize(name: str) -> str:
    """Replace filesystem-unsafe characters."""
    return str(name).replace("/", "_").replace("\\", "_").strip()


def _format_group_code(grupo) -> str:
    """Return group code as integer string, stripping trailing decimal zeros."""
    try:
        return str(int(float(grupo)))
    except (ValueError, TypeError):
        return str(grupo)


# ---------------------------------------------------------------------------
# Core export
# ---------------------------------------------------------------------------

def export_by_responsavel(
    df: pd.DataFrame,
    base_folder: Union[str, Path] = OUTPUT_FOLDER,
    filename: str = "Analise",
) -> Dict[str, str]:
    """
    Exports *df* to Excel files organised by Responsavel.

    Structure:
      <base_folder>/
        MTSE/<filename>.xlsx          <- full dataset (all rows)
        <RESP>/<filename>_<RESP>.xlsx <- rows for each Responsavel

    Only columns listed in EXPORT_COLUMNS are written, in that order.
    Returns a dict mapping each Responsavel key -> exported file path.
    """
    base_folder = _ensure_dir(base_folder)
    df_export   = _select_export_columns(df, EXPORT_COLUMNS)
    results: Dict[str, str] = {}

    # Full file (MTSE folder = "everyone")
    mtse_path = save_excel(df_export, base_folder / "MTSE" / f"{filename}.xlsx")
    results["MTSE"] = str(mtse_path)
    logger.info("Full export -> %s (%d rows)", mtse_path, len(df_export))

    # Per-Responsavel split
    if "Responsavel" in df.columns:
        for resp in sorted(df["Responsavel"].dropna().unique()):
            safe = _sanitize(resp)
            sub  = df_export[df["Responsavel"] == resp]
            path = save_excel(sub, base_folder / safe / f"{filename}_{safe}.xlsx")
            results[safe] = str(path)
            logger.info("  -> %s: %d rows", path, len(sub))

    return results


# ---------------------------------------------------------------------------
# Debug export — full DataFrame, all columns
# ---------------------------------------------------------------------------

def export_debug(
    df: pd.DataFrame,
    base_folder: Union[str, Path] = OUTPUT_FOLDER,
    filename: str = "DEBUG_Full",
) -> Path:
    """
    Exports the complete DataFrame to a single Excel file for debugging.

    Columns are ordered: priority columns first (those that exist), then all
    remaining columns in their original order — nothing is dropped.
    The file is saved to <base_folder>/MTSE/<filename>.xlsx only (no per-resp split).

    Returns the path of the saved file.
    """
    base_folder = Path(base_folder)

    # Build ordered column list: priorities first, then everything else
    priority = [c for c in _DEBUG_PRIORITY_COLUMNS if c in df.columns]
    rest     = [c for c in df.columns if c not in set(priority)]
    ordered  = priority + rest

    out_path = save_excel(df[ordered], base_folder / "MTSE" / f"{filename}.xlsx")
    logger.info("Debug export -> %s (%d rows, %d cols)", out_path, len(df), len(ordered))
    return out_path
