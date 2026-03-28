"""
group_separation.py — Final separation into per-group folder structure.

Organises output into per-analyst folders with 3 categories:
  - grupos/       → Materials approved for requisition (REPOR)
  - pendentes/    → Materials needing manual analysis (VERIFICAR/ERRO/empty)
  - sem_reposicao/→ Materials with no replenishment needed (NAO_REPOR)

Each analyst folder also gets a RESUMO.xlsx summary workbook.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from config.business import AD_VALUE_THRESHOLD
from config.paths import AD_TEMPLATE_DIR, OUTPUT_FOLDER
from utils.columns import (
    AD_REQUISITION_COLUMNS,
    NAO_REPOR_COLUMNS,
    ZSTK_SPLIT_COLUMNS,
    _select_export_columns,
)
from utils.excel import _ensure_dir, save_excel
from utils.export_core import _format_group_code, _sanitize

logger = logging.getLogger(__name__)

# Decision categories
_REPOR_VALUES = {"REPOR"}
_NAO_REPOR_VALUES = {"NAO_REPOR"}
# Everything else is "pendente"


def _copy_ad_templates(target_dir: Path, total_value: float) -> None:
    """
    Copies the appropriate AD procurement templates based on total order value.
      <= AD_VALUE_THRESHOLD -> CPV declaration (simplified process)
      >  AD_VALUE_THRESHOLD -> Inexigibilidade + Justificativa de Preco
    """
    if not AD_TEMPLATE_DIR.exists():
        logger.debug("Template directory not found: %s", AD_TEMPLATE_DIR)
        return

    try:
        if total_value <= AD_VALUE_THRESHOLD:
            src = AD_TEMPLATE_DIR / "Declaracao_CPV_template.docx"
            if src.exists():
                shutil.copy(src, target_dir / "Documentação CPV.docx")
        else:
            for template, output in [
                ("Inexigilibidade_template.docx",       "Inexibilidade.docx"),
                ("Justificativa_de_Preço_template.docx", "Justificativa de Preço.docx"),
            ]:
                src = AD_TEMPLATE_DIR / template
                if src.exists():
                    shutil.copy(src, target_dir / output)
    except Exception as exc:
        logger.error("Error copying AD templates to %s: %s", target_dir, exc)


def _classify_materials(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Split DataFrame into (repor, pendentes, nao_repor)."""
    ai_col = df.get("Analise_AI", pd.Series("", index=df.index)).fillna("").astype(str).str.upper()

    mask_repor = ai_col.isin(_REPOR_VALUES)
    mask_nao = ai_col.isin(_NAO_REPOR_VALUES)
    mask_pendente = ~mask_repor & ~mask_nao

    return df[mask_repor].copy(), df[mask_pendente].copy(), df[mask_nao].copy()


def _generate_summary_sheet(
    df_all: pd.DataFrame,
    df_repor: pd.DataFrame,
    df_pendente: pd.DataFrame,
    df_nao: pd.DataFrame,
    output_path: Path,
) -> Path:
    """Generate a RESUMO.xlsx summary workbook for an analyst."""
    try:
        import xlsxwriter
    except ImportError:
        logger.warning("xlsxwriter not available, skipping summary sheet")
        return output_path

    _ensure_dir(output_path.parent)

    wb = xlsxwriter.Workbook(str(output_path))
    ws = wb.add_worksheet("Resumo")

    # Formats
    fmt_title = wb.add_format({
        "bold": True, "font_size": 16, "font_color": "#1a365d",
        "bottom": 2, "bottom_color": "#2b6cb0",
    })
    fmt_header = wb.add_format({
        "bold": True, "font_size": 12, "font_color": "#2b6cb0",
        "bottom": 1, "bottom_color": "#bee3f8",
    })
    fmt_kpi_label = wb.add_format({"bold": True, "font_size": 11, "indent": 1})
    fmt_kpi_value = wb.add_format({"bold": True, "font_size": 11, "font_color": "#2b6cb0"})
    fmt_money = wb.add_format({"bold": True, "font_size": 11, "font_color": "#2b6cb0", "num_format": "R$ #,##0.00"})
    fmt_urgent = wb.add_format({"bg_color": "#fed7d7", "font_color": "#9b2c2c"})
    fmt_alta = wb.add_format({"bg_color": "#feebc8", "font_color": "#7b341e"})
    fmt_media = wb.add_format({"bg_color": "#fefcbf", "font_color": "#744210"})
    fmt_table_header = wb.add_format({
        "bold": True, "bg_color": "#2b6cb0", "font_color": "white",
        "border": 1, "text_wrap": True,
    })
    fmt_cell = wb.add_format({"border": 1, "text_wrap": True, "valign": "vcenter"})

    ws.set_column(0, 0, 30)
    ws.set_column(1, 1, 20)
    ws.set_column(2, 5, 18)

    row = 0

    # Title
    ws.write(row, 0, "RESUMO DE ANÁLISE DE MATERIAIS", fmt_title)
    row += 2

    # KPIs
    ws.write(row, 0, "VISÃO GERAL", fmt_header)
    row += 1

    total = len(df_all)
    n_repor = len(df_repor)
    n_pendente = len(df_pendente)
    n_nao = len(df_nao)

    valor_repor = 0.0
    if "Valor_Total_Ordem" in df_repor.columns and not df_repor.empty:
        valor_repor = pd.to_numeric(df_repor["Valor_Total_Ordem"], errors="coerce").fillna(0).sum()

    n_urgente = 0
    if "Prioridade" in df_all.columns:
        n_urgente = int((df_all["Prioridade"] == "URGENTE").sum())

    kpis = [
        ("Total de Materiais", str(total)),
        ("Para Reposição (REPOR)", str(n_repor)),
        ("Pendentes de Análise", str(n_pendente)),
        ("Sem Reposição", str(n_nao)),
        ("Materiais Urgentes", str(n_urgente)),
    ]

    for label, value in kpis:
        ws.write(row, 0, label, fmt_kpi_label)
        ws.write(row, 1, value, fmt_kpi_value)
        row += 1

    ws.write(row, 0, "Valor Total Estimado (REPOR)", fmt_kpi_label)
    ws.write(row, 1, valor_repor, fmt_money)
    row += 2

    # Top urgent materials
    ws.write(row, 0, "MATERIAIS PRIORITÁRIOS", fmt_header)
    row += 1

    headers = ["Código", "Descrição", "Prioridade", "Decisão IA", "Estoque", "Preço Unit."]
    for col_idx, h in enumerate(headers):
        ws.write(row, col_idx, h, fmt_table_header)
    row += 1

    # Sort by priority
    priority_order = {"URGENTE": 0, "ALTA": 1, "MÉDIA": 2, "BAIXA": 3}
    df_sorted = df_all.copy()
    if "Prioridade" in df_sorted.columns:
        df_sorted["_prio_sort"] = df_sorted["Prioridade"].map(priority_order).fillna(4)
        df_sorted = df_sorted.sort_values("_prio_sort")

    for _, mat in df_sorted.head(15).iterrows():
        prio = str(mat.get("Prioridade", ""))
        fmt = fmt_cell
        if prio == "URGENTE":
            fmt = wb.add_format({"border": 1, "bg_color": "#fed7d7", "text_wrap": True, "valign": "vcenter"})
        elif prio == "ALTA":
            fmt = wb.add_format({"border": 1, "bg_color": "#feebc8", "text_wrap": True, "valign": "vcenter"})

        ws.write(row, 0, str(mat.get("Codigo_Material", "")), fmt)
        ws.write(row, 1, str(mat.get("Texto_Breve_Material", ""))[:50], fmt)
        ws.write(row, 2, prio, fmt)
        ws.write(row, 3, str(mat.get("Analise_AI", "")), fmt)
        ws.write(row, 4, str(mat.get("Estoque_Total", "")), fmt)
        ws.write(row, 5, str(mat.get("Preco_Unitario", "")), fmt)
        row += 1

    # Distribution by Grupo MRP
    if "Grupo_MRP" in df_all.columns:
        row += 2
        ws.write(row, 0, "DISTRIBUIÇÃO POR GRUPO MRP", fmt_header)
        row += 1
        for grp, count in df_all["Grupo_MRP"].value_counts().items():
            ws.write(row, 0, str(grp), fmt_kpi_label)
            ws.write(row, 1, str(count), fmt_kpi_value)
            row += 1

    wb.close()
    return output_path


def _export_repor_groups(df_repor: pd.DataFrame, base_resp_dir: Path) -> None:
    """Export REPOR materials split by group into grupos/ folder."""
    if df_repor.empty:
        return

    # AD items
    df_ad = df_repor[df_repor["Grupo_MRP"].astype(str).str.upper() == "AD"]
    if not df_ad.empty:
        for (grupo, taxacao), sub in df_ad.groupby(["Grupo_Mercadoria", "Valor_Tributado"]):
            g_str = _format_group_code(grupo)
            out = _ensure_dir(base_resp_dir / "grupos" / f"AD_{g_str}")
            save_excel(
                _select_export_columns(sub, AD_REQUISITION_COLUMNS),
                out / f"AD_{g_str}_{_sanitize(str(taxacao))}_Requisicao.xlsx",
            )
            _copy_ad_templates(out, float(sub["Valor_Total_Ordem"].sum()))

    # Non-AD (ZSTK and others)
    df_other = df_repor[df_repor["Grupo_MRP"].astype(str).str.upper() != "AD"].copy()
    if not df_other.empty:
        out_zstk = _ensure_dir(base_resp_dir / "grupos" / "ZSTK")
        df_other["_grmp4d"] = df_other["Grupo_Mercadoria"].apply(
            lambda x: _format_group_code(x)[:4]
        )
        for (g4d, taxacao), sub in df_other.groupby(["_grmp4d", "Valor_Tributado"]):
            sub_export = _select_export_columns(
                sub.drop(columns=["_grmp4d"]), ZSTK_SPLIT_COLUMNS
            )
            save_excel(
                sub_export,
                out_zstk / f"ZSTK_{g4d}_{_sanitize(str(taxacao))}_Requisicao.xlsx",
            )


def _export_pendentes(df_pendente: pd.DataFrame, base_resp_dir: Path, responsavel: str) -> None:
    """Export pendente materials to pendentes/ folder."""
    if df_pendente.empty:
        return
    from utils.columns import ACAO_ANALISTA_COLUMNS
    out = _ensure_dir(base_resp_dir / "pendentes")
    save_excel(
        _select_export_columns(df_pendente, ACAO_ANALISTA_COLUMNS),
        out / f"Pendentes_{_sanitize(responsavel)}.xlsx",
    )


def _export_nao_repor(df_nao: pd.DataFrame, base_resp_dir: Path, responsavel: str) -> None:
    """Export NAO_REPOR materials to sem_reposicao/ folder."""
    if df_nao.empty:
        return
    out = _ensure_dir(base_resp_dir / "sem_reposicao")
    save_excel(
        _select_export_columns(df_nao, NAO_REPOR_COLUMNS),
        out / f"Sem_Reposicao_{_sanitize(responsavel)}.xlsx",
    )


def separar_por_setor_grupo_taxacao(
    df: Optional[pd.DataFrame] = None,
    input_file_path: Optional[Union[str, Path]] = None,
    output_dir: Optional[Union[str, Path]] = None,
) -> Path:
    """
    Organises output into per-analyst folders with 3 categories.

    Structure per analyst:
      <Responsavel>/
        RESUMO.xlsx                           ← Summary with KPIs
        grupos/                               ← REPOR materials
          AD_<grupo>/
            AD_<grupo>_<taxacao>_Requisicao.xlsx
            Documentação CPV.docx (or Inexibilidade.docx)
          ZSTK/
            ZSTK_<grupo4d>_<taxacao>_Requisicao.xlsx
        pendentes/                            ← VERIFICAR/ERRO materials
          Pendentes_<resp>.xlsx
        sem_reposicao/                        ← NAO_REPOR materials
          Sem_Reposicao_<resp>.xlsx

    Returns the base output directory path.
    """
    base_output_dir = Path(output_dir) if output_dir else Path(OUTPUT_FOLDER)

    # Resolve data source
    if df is None:
        if not input_file_path:
            input_file_path = base_output_dir / "MTSE" / "Relatorio_Completo_MTSE.xlsx"
            if not Path(input_file_path).exists():
                input_file_path = base_output_dir / "MTSE" / "Analise.xlsx"

        file_path = Path(input_file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Analysis file not found: {file_path}")

        df = pd.read_excel(file_path)
        logger.info("Loaded %d rows from %s", len(df), file_path)

    # Normalise columns
    if "Analise_Gestor" in df.columns:
        df["Analise_Gestor"] = df["Analise_Gestor"].fillna("").astype(str).str.upper()
    df["Grupo_MRP"] = df["Grupo_MRP"].fillna("").astype(str).str.upper()
    if "Analise_AI" not in df.columns:
        df["Analise_AI"] = ""

    # Classify
    df_repor, df_pendente, df_nao = _classify_materials(df)

    # Process per analyst
    for responsavel in sorted(df["Responsavel"].dropna().unique()):
        safe = _sanitize(responsavel)
        base_resp_dir = base_output_dir / safe

        # Clean previous output
        for subdir in ["grupos", "pendentes", "sem_reposicao"]:
            shutil.rmtree(base_resp_dir / subdir, ignore_errors=True)

        # Filter per analyst
        resp_repor = df_repor[df_repor["Responsavel"] == responsavel]
        resp_pendente = df_pendente[df_pendente["Responsavel"] == responsavel]
        resp_nao = df_nao[df_nao["Responsavel"] == responsavel]
        resp_all = df[df["Responsavel"] == responsavel]

        # Export each category
        _export_repor_groups(resp_repor, base_resp_dir)
        _export_pendentes(resp_pendente, base_resp_dir, responsavel)
        _export_nao_repor(resp_nao, base_resp_dir, responsavel)

        # Generate summary
        _generate_summary_sheet(
            resp_all, resp_repor, resp_pendente, resp_nao,
            base_resp_dir / "RESUMO.xlsx",
        )

    # Also generate full summary for MTSE
    mtse_dir = _ensure_dir(base_output_dir / "MTSE")
    _generate_summary_sheet(df, df_repor, df_pendente, df_nao, mtse_dir / "RESUMO.xlsx")

    logger.info(
        "Separation complete. REPOR: %d | PENDENTES: %d | NAO_REPOR: %d",
        len(df_repor), len(df_pendente), len(df_nao),
    )
    print(f"   Separação: {len(df_repor)} REPOR | {len(df_pendente)} PENDENTES | {len(df_nao)} NAO_REPOR")
    return base_output_dir
