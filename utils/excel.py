"""
excel.py — Shared Excel writing infrastructure.

Provides:
  - save_excel:         Save DataFrame to .xlsx with table styling
  - _apply_table_style: Auto-fit columns, table format, hyperlinks
  - _ensure_dir:        Create directory tree if needed
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ensure_dir(path: Union[str, Path]) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------------------------------------------------------------------------
# Excel writing
# ---------------------------------------------------------------------------

# Columns whose content is one URL per line — rendered as hyperlinks in Excel
_URL_COLUMNS = {
    "ref_url",
    "ref_search_links",
    "url_fonte",
}


def _apply_table_style(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
) -> None:
    """
    Auto-fit columns, apply table style with frozen header, and write URL
    columns as clickable hyperlinks (one per line -> one row per URL block).
    """
    ws       = writer.sheets[sheet_name]
    workbook = writer.book
    fmt_wrap = workbook.add_format({"text_wrap": True, "valign": "vcenter"})
    fmt_url  = workbook.add_format({
        "text_wrap": True, "valign": "vcenter",
        "font_color": "#0563C1", "underline": True,
    })

    for i, col in enumerate(df.columns):
        col_width = min(
            max(df[col].astype(str).map(len).max(), len(col)) + 2,
            80,          # wider cap so URLs are legible
        )
        ws.set_column(i, i, col_width, fmt_wrap)

    # Overwrite URL cells with write_url (row 0 = header, data starts at row 1)
    for col_name in _URL_COLUMNS:
        if col_name not in df.columns:
            continue
        col_idx = df.columns.get_loc(col_name)
        for row_idx, cell_value in enumerate(df[col_name]):
            excel_row = row_idx + 1          # +1 for header
            cell_str  = str(cell_value or "").strip()
            if not cell_str:
                continue

            urls = [u.strip() for u in cell_str.splitlines() if u.strip()]
            if not urls:
                continue

            if len(urls) == 1:
                # Single URL -> clean hyperlink
                try:
                    ws.write_url(excel_row, col_idx, urls[0], fmt_url, urls[0])
                except Exception:
                    ws.write(excel_row, col_idx, urls[0], fmt_wrap)
            else:
                # Multiple URLs -> write as text block (Excel allows only 1 URL per cell)
                # First URL is the hyperlink; rest appended as plain text below it
                combined = "\n".join(urls)
                try:
                    ws.write_url(excel_row, col_idx, urls[0], fmt_url, combined)
                except Exception:
                    ws.write(excel_row, col_idx, combined, fmt_wrap)

    ws.add_table(
        0, 0, len(df), len(df.columns) - 1,
        {
            "columns": [{"header": c} for c in df.columns],
            "style": "Table Style Medium 9",
        },
    )
    ws.freeze_panes(1, 0)


def save_excel(df: pd.DataFrame, file_path: Path) -> Path:
    """
    Save *df* to *file_path* with table styling.
    If the file is locked (PermissionError), saves with a timestamp suffix.
    """
    _ensure_dir(file_path.parent)

    # Format the suggested text to include line breaks after semicolons for better readability in Excel
    if "Texto_Sugerido" in df.columns:
        df = df.copy()
        mask = df["Texto_Sugerido"].notna()
        df.loc[mask, "Texto_Sugerido"] = df.loc[mask, "Texto_Sugerido"].astype(str).str.replace(";", ";\n", regex=False)

    try:
        with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
            _apply_table_style(writer, "Sheet1", df)
        logger.debug("Saved: %s", file_path)
        return file_path

    except PermissionError:
        ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
        fallback = file_path.parent / f"{file_path.stem}_{ts}{file_path.suffix}"
        logger.warning("File locked: %s -> saving as %s", file_path, fallback)
        with pd.ExcelWriter(fallback, engine="xlsxwriter") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
            _apply_table_style(writer, "Sheet1", df)
        return fallback
