"""
actionable_report.py — Generate analyst-facing multi-tab Excel reports.

Creates a structured workbook with:
  Tab 1: Resumo         — KPIs, totals, summary metrics
  Tab 2: Ações          — What the analyst must do for each material
  Tab 3: Reposição      — REPOR materials ready for requisition
  Tab 4: Pendentes      — Materials needing manual analysis
  Tab 5: Sem Reposição  — Informational, no action needed
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Union

import pandas as pd

from utils.columns import (
    ACAO_ANALISTA_COLUMNS,
    AD_REQUISITION_COLUMNS,
    COLUMN_LABELS,
    NAO_REPOR_COLUMNS,
    ZSTK_SPLIT_COLUMNS,
    _select_export_columns,
    format_acoes_sugeridas,
)
from utils.excel import _ensure_dir

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Priority sort order
# ---------------------------------------------------------------------------

_PRIORITY_ORDER = {"URGENTE": 0, "ALTA": 1, "MÉDIA": 2, "BAIXA": 3}


def _priority_sort_key(series: pd.Series) -> pd.Series:
    """Map Prioridade values to numeric sort keys (lower = higher priority)."""
    return series.map(lambda v: _PRIORITY_ORDER.get(v, 99))


# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------

def _human_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns using COLUMN_LABELS where a mapping exists."""
    rename_map = {c: COLUMN_LABELS.get(c, c) for c in df.columns}
    return df.rename(columns=rename_map)


def _select_and_rename(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Select columns that exist and rename to human-readable labels."""
    selected = _select_export_columns(df, columns)
    return _human_columns(selected)


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _format_acoes_column(df: pd.DataFrame) -> pd.DataFrame:
    """Format the acoes_sugeridas column as numbered list text."""
    df = df.copy()
    col = "acoes_sugeridas"
    if col in df.columns:
        df[col] = df[col].apply(
            lambda v: format_acoes_sugeridas(v) if pd.notna(v) else ""
        )
    # Also check the human-readable label
    label = COLUMN_LABELS.get(col, col)
    if label != col and label in df.columns:
        df[label] = df[label].apply(
            lambda v: format_acoes_sugeridas(v) if pd.notna(v) else ""
        )
    return df


def _format_brl(value: float) -> str:
    """Format a numeric value as Brazilian Real currency string."""
    try:
        v = float(value)
        formatted = f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {formatted}"
    except (ValueError, TypeError):
        return "R$ 0,00"


# ---------------------------------------------------------------------------
# Excel writing helpers
# ---------------------------------------------------------------------------

def _auto_fit_columns(
    worksheet,
    df: pd.DataFrame,
    workbook,
    start_row: int = 0,
) -> None:
    """Set column widths based on content length and apply text wrap."""
    fmt_wrap = workbook.add_format({"text_wrap": True, "valign": "vcenter"})
    for i, col in enumerate(df.columns):
        max_len = max(
            df[col].astype(str).str.len().max() if len(df) > 0 else 0,
            len(str(col)),
        )
        width = min(max_len + 2, 80)
        worksheet.set_column(i, i, width, fmt_wrap)


def _write_data_tab(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
) -> None:
    """Write a DataFrame to a sheet with table style, frozen header, auto-fit."""
    if df.empty:
        # Write an empty sheet with headers only
        df_out = pd.DataFrame(columns=df.columns)
        df_out.to_excel(writer, index=False, sheet_name=sheet_name)
        ws = writer.sheets[sheet_name]
        ws.freeze_panes(1, 0)
        return

    df.to_excel(writer, index=False, sheet_name=sheet_name)
    ws = writer.sheets[sheet_name]
    workbook = writer.book

    _auto_fit_columns(ws, df, workbook)

    ws.add_table(
        0, 0, len(df), len(df.columns) - 1,
        {
            "columns": [{"header": c} for c in df.columns],
            "style": "Table Style Medium 9",
        },
    )
    ws.freeze_panes(1, 0)


# ---------------------------------------------------------------------------
# Tab builders
# ---------------------------------------------------------------------------

def _build_resumo(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
) -> None:
    """Write the Resumo (summary) tab with KPIs and top urgent materials."""
    workbook = writer.book
    ws = workbook.add_worksheet("Resumo")

    # Formats
    fmt_title = workbook.add_format({
        "bold": True, "font_size": 16, "font_color": "#1F4E79",
    })
    fmt_kpi_label = workbook.add_format({
        "bold": True, "font_size": 12, "valign": "vcenter",
    })
    fmt_kpi_value = workbook.add_format({
        "font_size": 12, "valign": "vcenter",
    })
    fmt_header = workbook.add_format({
        "bold": True, "font_size": 11, "bg_color": "#4472C4",
        "font_color": "#FFFFFF", "border": 1,
    })
    fmt_cell = workbook.add_format({
        "font_size": 10, "border": 1, "text_wrap": True, "valign": "vcenter",
    })

    # Column widths
    ws.set_column(0, 0, 35)
    ws.set_column(1, 1, 50)
    ws.set_column(2, 2, 15)
    ws.set_column(3, 3, 40)

    # ── Compute KPI values ────────────────────────────────────────────────
    total = len(df)

    analise_col = df["Analise_AI"] if "Analise_AI" in df.columns else pd.Series(dtype=str)

    repor_mask = analise_col == "REPOR"
    n_repor = int(repor_mask.sum())
    valor_repor = 0.0
    if "Preco_Unitario" in df.columns and "Quantidade_OP_AI" in df.columns:
        repor_df = df.loc[repor_mask]
        preco = pd.to_numeric(repor_df["Preco_Unitario"], errors="coerce").fillna(0)
        qtd = pd.to_numeric(repor_df["Quantidade_OP_AI"], errors="coerce").fillna(0)
        valor_repor = float((preco * qtd).sum())

    pendentes_mask = analise_col.isin(["VERIFICAR", "ERRO_API", "ANALISAR", ""]) | analise_col.isna()
    n_pendentes = int(pendentes_mask.sum())

    nao_repor_mask = analise_col == "NAO_REPOR"
    n_nao_repor = int(nao_repor_mask.sum())

    prioridade_col = df.get("Prioridade", pd.Series(dtype=str))
    n_urgentes = int((prioridade_col == "URGENTE").sum())

    # Total estimated value (all materials)
    valor_total = 0.0
    if "Preco_Unitario" in df.columns and "Quantidade_OP_AI" in df.columns:
        preco_all = pd.to_numeric(df["Preco_Unitario"], errors="coerce").fillna(0)
        qtd_all = pd.to_numeric(df["Quantidade_OP_AI"], errors="coerce").fillna(0)
        valor_total = float((preco_all * qtd_all).sum())

    # ── Write title ───────────────────────────────────────────────────────
    row = 0
    ws.write(row, 0, "Relatório de Análise de Materiais", fmt_title)
    row += 1
    ws.write(row, 0, f"Gerado em: {datetime.now().strftime('%d/%m/%Y %H:%M')}", fmt_kpi_value)
    row += 2

    # ── Write KPIs ────────────────────────────────────────────────────────
    kpis = [
        (f"Total de Materiais: {total}",),
        (f"Para Reposição (REPOR): {n_repor} — {_format_brl(valor_repor)}",),
        (f"Pendentes de Análise: {n_pendentes}",),
        (f"Sem Reposição: {n_nao_repor}",),
        (f"Materiais Urgentes: {n_urgentes}",),
        (f"Valor Total Estimado: {_format_brl(valor_total)}",),
    ]
    for kpi in kpis:
        ws.write(row, 0, kpi[0], fmt_kpi_label)
        row += 1

    row += 2

    # ── Top 10 urgent materials table ─────────────────────────────────────
    ws.write(row, 0, "Top 10 Materiais Urgentes", fmt_title)
    row += 1

    urgent_cols = ["Codigo_Material", "Texto_Breve_Material", "Prioridade", "Analise_AI"]
    headers = [
        COLUMN_LABELS.get("Codigo_Material", "Código"),
        COLUMN_LABELS.get("Texto_Breve_Material", "Descrição"),
        COLUMN_LABELS.get("Prioridade", "Prioridade"),
        COLUMN_LABELS.get("Analise_AI", "Decisão"),
    ]

    # Write headers
    for col_idx, header in enumerate(headers):
        ws.write(row, col_idx, header, fmt_header)
    row += 1

    # Select top 10 urgent materials
    available_cols = [c for c in urgent_cols if c in df.columns]
    if available_cols and "Prioridade" in df.columns:
        urgent_df = df[df["Prioridade"] == "URGENTE"].head(10)
        for _, mat_row in urgent_df.iterrows():
            for col_idx, col_name in enumerate(urgent_cols):
                val = mat_row.get(col_name, "") if col_name in df.columns else ""
                ws.write(row, col_idx, str(val) if pd.notna(val) else "", fmt_cell)
            row += 1

    ws.freeze_panes(0, 0)  # No freeze on summary tab


def _build_acoes(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
) -> None:
    """Write the Ações tab with conditional formatting by priority."""
    sheet_name = "Ações"

    # Select and prepare data
    df_acoes = _select_export_columns(df, ACAO_ANALISTA_COLUMNS).copy()

    # Format acoes_sugeridas before renaming
    if "acoes_sugeridas" in df_acoes.columns:
        df_acoes["acoes_sugeridas"] = df_acoes["acoes_sugeridas"].apply(
            lambda v: format_acoes_sugeridas(v) if pd.notna(v) else ""
        )

    # Sort by priority
    if "Prioridade" in df_acoes.columns:
        df_acoes["_sort_key"] = _priority_sort_key(df_acoes["Prioridade"])
        df_acoes = df_acoes.sort_values("_sort_key").drop(columns=["_sort_key"])
        df_acoes = df_acoes.reset_index(drop=True)

    # Capture priority values before renaming for conditional formatting
    priority_values = (
        df_acoes["Prioridade"].tolist()
        if "Prioridade" in df_acoes.columns
        else []
    )

    # Rename to human-readable
    df_acoes = _human_columns(df_acoes)

    # Write data tab
    _write_data_tab(writer, sheet_name, df_acoes)

    # Apply conditional formatting by priority
    if priority_values and not df_acoes.empty:
        ws = writer.sheets[sheet_name]
        workbook = writer.book

        fmt_urgente = workbook.add_format({"bg_color": "#FFC7CE"})  # light red
        fmt_alta = workbook.add_format({"bg_color": "#FFCC99"})     # light orange
        fmt_media = workbook.add_format({"bg_color": "#FFFF99"})    # light yellow

        n_cols = len(df_acoes.columns) - 1
        for row_idx, prio in enumerate(priority_values):
            excel_row = row_idx + 1  # +1 for header
            fmt = None
            if prio == "URGENTE":
                fmt = fmt_urgente
            elif prio == "ALTA":
                fmt = fmt_alta
            elif prio == "MÉDIA":
                fmt = fmt_media

            if fmt is not None:
                for col_idx in range(n_cols + 1):
                    cell_value = df_acoes.iloc[row_idx, col_idx]
                    cell_str = str(cell_value) if pd.notna(cell_value) else ""
                    ws.write(excel_row, col_idx, cell_str, fmt)


def _build_reposicao(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
) -> None:
    """Write the Reposição tab — REPOR materials ready for requisition."""
    sheet_name = "Reposição"

    repor_mask = df["Analise_AI"] == "REPOR" if "Analise_AI" in df.columns else pd.Series(False, index=df.index)
    df_repor = df[repor_mask].copy()

    # Choose columns based on Setor_Atividade
    frames = []
    if "Setor_Atividade" in df_repor.columns:
        ad_mask = df_repor["Setor_Atividade"].astype(str).str.upper().str.contains("AD")
        if ad_mask.any():
            ad_df = _select_and_rename(df_repor[ad_mask], AD_REQUISITION_COLUMNS)
            frames.append(ad_df)
        if (~ad_mask).any():
            other_df = _select_and_rename(df_repor[~ad_mask], ZSTK_SPLIT_COLUMNS)
            frames.append(other_df)
    else:
        frames.append(_select_and_rename(df_repor, ZSTK_SPLIT_COLUMNS))

    if frames:
        # Use all columns from both frames (union)
        all_cols: list[str] = []
        seen: set[str] = set()
        for f in frames:
            for c in f.columns:
                if c not in seen:
                    all_cols.append(c)
                    seen.add(c)
        # Reindex each frame to the union and concatenate
        aligned = [f.reindex(columns=all_cols) for f in frames]
        df_out = pd.concat(aligned, ignore_index=True)
    else:
        df_out = pd.DataFrame()

    # Sort by Grupo_MRP then Grupo_Mercadoria (using human labels)
    grp_mrp_label = COLUMN_LABELS.get("Grupo_MRP", "Grupo_MRP")
    grp_merc_label = COLUMN_LABELS.get("Grupo_Mercadoria", "Grupo_Mercadoria")
    sort_cols = [c for c in [grp_mrp_label, grp_merc_label] if c in df_out.columns]
    if sort_cols:
        df_out = df_out.sort_values(sort_cols).reset_index(drop=True)

    _write_data_tab(writer, sheet_name, df_out)


def _build_pendentes(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
) -> None:
    """Write the Pendentes tab — materials needing manual analysis."""
    sheet_name = "Pendentes"

    if "Analise_AI" in df.columns:
        pendentes_mask = (
            df["Analise_AI"].isin(["VERIFICAR", "ERRO_API", "ANALISAR", ""])
            | df["Analise_AI"].isna()
        )
    else:
        pendentes_mask = pd.Series(True, index=df.index)

    df_pend = df[pendentes_mask].copy()

    # Format acoes before renaming
    if "acoes_sugeridas" in df_pend.columns:
        df_pend["acoes_sugeridas"] = df_pend["acoes_sugeridas"].apply(
            lambda v: format_acoes_sugeridas(v) if pd.notna(v) else ""
        )

    df_out = _select_and_rename(df_pend, ACAO_ANALISTA_COLUMNS)
    _write_data_tab(writer, sheet_name, df_out)


def _build_sem_reposicao(
    writer: pd.ExcelWriter,
    df: pd.DataFrame,
) -> None:
    """Write the Sem Reposição tab — NAO_REPOR materials."""
    sheet_name = "Sem Reposição"

    nao_repor_mask = (
        df["Analise_AI"] == "NAO_REPOR"
        if "Analise_AI" in df.columns
        else pd.Series(False, index=df.index)
    )
    df_nr = df[nao_repor_mask].copy()
    df_out = _select_and_rename(df_nr, NAO_REPOR_COLUMNS)
    _write_data_tab(writer, sheet_name, df_out)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def generate_analyst_report(
    df: pd.DataFrame,
    output_path: Union[str, Path],
    responsavel: str | None = None,
) -> Path:
    """
    Generate a multi-tab analyst report as an Excel workbook.

    Parameters
    ----------
    df : pd.DataFrame
        Full pipeline output DataFrame.
    output_path : str | Path
        Destination path for the .xlsx file.
    responsavel : str, optional
        If specified, filter df to rows where Responsavel == responsavel.

    Returns
    -------
    Path
        The path of the generated file.
    """
    output_path = Path(output_path)
    _ensure_dir(output_path.parent)

    # Filter by analyst if requested
    if responsavel is not None and "Responsavel" in df.columns:
        df = df[df["Responsavel"] == responsavel].copy()
        logger.info("Filtered to Responsavel=%s: %d rows", responsavel, len(df))

    try:
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            _build_resumo(writer, df)
            _build_acoes(writer, df)
            _build_reposicao(writer, df)
            _build_pendentes(writer, df)
            _build_sem_reposicao(writer, df)

        logger.info("Analyst report saved: %s (%d materials)", output_path, len(df))
        return output_path

    except PermissionError:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fallback = output_path.parent / f"{output_path.stem}_{ts}{output_path.suffix}"
        logger.warning("File locked: %s -> saving as %s", output_path, fallback)
        with pd.ExcelWriter(fallback, engine="xlsxwriter") as writer:
            _build_resumo(writer, df)
            _build_acoes(writer, df)
            _build_reposicao(writer, df)
            _build_pendentes(writer, df)
            _build_sem_reposicao(writer, df)
        return fallback


def generate_all_reports(
    df: pd.DataFrame,
    base_output_dir: Union[str, Path],
) -> Dict[str, Path]:
    """
    Generate one analyst report per Responsavel plus a complete MTSE report.

    Parameters
    ----------
    df : pd.DataFrame
        Full pipeline output DataFrame.
    base_output_dir : str | Path
        Base directory; reports are saved to subdirectories per analyst.

    Returns
    -------
    dict[str, Path]
        Mapping of {responsavel: Path} for each generated report,
        including "MTSE" for the complete report.
    """
    base_output_dir = Path(base_output_dir)
    results: Dict[str, Path] = {}

    # Complete report for MTSE (all analysts)
    mtse_path = base_output_dir / "MTSE" / "Relatorio_Analista_MTSE.xlsx"
    results["MTSE"] = generate_analyst_report(df, mtse_path)
    logger.info("MTSE complete report: %s", mtse_path)

    # Per-analyst reports
    if "Responsavel" in df.columns:
        for resp in sorted(df["Responsavel"].dropna().unique()):
            safe_name = str(resp).replace("/", "_").replace("\\", "_").strip()
            resp_path = (
                base_output_dir / safe_name / f"Relatorio_Analista_{safe_name}.xlsx"
            )
            results[resp] = generate_analyst_report(df, resp_path, responsavel=resp)
            logger.info("  %s report: %s", resp, resp_path)

    return results
