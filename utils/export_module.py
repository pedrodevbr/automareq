"""
export_module.py
================
Export & File Organisation Service

Responsibilities:
  1. Save DataFrames to Excel with auto-fit columns and table styling.
  2. Export the full analysis broken down by Responsavel (checkpoints + final).
  3. Organise the final output into per-group folders (AD, ZSTK) with AD templates.

Public API:
  export_by_responsavel(df, base_folder, filename)  -> Dict[str, str]
  separar_por_setor_grupo_taxacao(input_file_path)  -> Path
"""

from __future__ import annotations

import logging
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Union

import pandas as pd

from config.config import OUTPUT_FOLDER

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logger = logging.getLogger(__name__)
if not logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[_handler],
    )

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

AD_VALUE_THRESHOLD = 7_000          # BRL – determines which AD template to copy
TEMPLATE_DIR       = Path("templates/AD/")

# Column order for all exports.
# Only columns that actually exist in the DataFrame will be included.
# NOTE: `resumo_validacao` consolidates ALL validation flags (LT, GRPM, TXT,
#       OBS, REF, IMG, DOC) — individual flag columns like `pre_analise`,
#       `ref_obs_flag`, and `leadtime_obs` are kept only in the DEBUG sheet.
EXPORT_COLUMNS: list[str] = [
    # ── Identification ────────────────────────────────────────────────────
    "Codigo_Material", "Texto_Breve_Material",
    "Setor_Atividade", "Numero_Peca_Fabricante",
    "Classificacao_ABC", "Criticidade",
    # ── Validation Summary (first for human visibility) ───────────────────
    "classificacao_validacao", "score_validacao", "resumo_validacao",
    # ── GRPM (actionable columns only) ────────────────────────────────────
    "Grupo_Mercadoria",
    "grpm_decisao_llm", "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa",
    # ── Planning ──────────────────────────────────────────────────────────
    "Analise_Gestor", "Planejador_MRP", "Planejador_Sugerido", "Grupo_MRP",
    "Politica_Atual", "PR_Atual", "MAX_Atual",
    "Politica_Sugerida", "Quantidade_OP_Calculada",
    "PR_Calculado", "MAX_Calculado", "Estoque_Seguranca", "Nivel_Servico",
    # ── Financial ─────────────────────────────────────────────────────────
    "Estoque_Total", "Saldo_Virtual", "Preco_Unitario",
    "Valor Estoque", "Valor_Atualizado", "Valor_Tributado",
    # ── Market Reference (ReferenceValidator) ─────────────────────────────
    "ref_reference_found", "ref_supplier", "ref_url", "ref_search_links",
    "ref_price_estimated", "ref_currency", "ref_availability",
    "ref_part_number_confirmed", "ref_part_number_note",
    "ref_text_coverage", "ref_coverage_gaps",
    # ── OBS / PN (actionable columns) ─────────────────────────────────────
    "obs_pn_presente", "obs_referencia_extraida", "obs_sugestao_texto",
    # ── AI Analysis ───────────────────────────────────────────────────────
    "Analise_AI", "Quantidade_OP_AI", "PR_AI", "MAX_AI",
    "Politica_AI", "Comentario",
    # ── Demand ────────────────────────────────────────────────────────────
    "Consumo_Medio_Mensal", "Demanda_Mensal", "Demanda_Programada",
    "Demanda_Anual", "Perfil_Demanda", "TMD", "CV",
    "Classificacao", "Outliers", "Data_Ultimo_Consumo", "Quantidade_201_12m",
    # ── Purchase Orders ───────────────────────────────────────────────────
    "Data_Ultimo_Pedido", "Anos_Ultima_Compra", "Responsavel",
    "Quantidade_Ordem", "Valor_Total_Ordem",
    # ── Lead Time ─────────────────────────────────────────────────────────
    "Prazo_Entrega_Previsto", "Dias_Em_OP",
    # ── Logistics ────────────────────────────────────────────────────────
    "Volume", "Unidade de volume", "Volume_OP", "Adicional_Lote_Obrigatorio",
    # ── Image ─────────────────────────────────────────────────────────────
    "img_path", "img_qualidade", "img_motivo", "img_substituir",
    # ── Post-analysis ────────────────────────────────────────────────────
    "pos_analise",
    "RTP1", "RTP2", "RTP3", "RTP6", "Quantidade_LMR",
    # ── Texts ─────────────────────────────────────────────────────────────
    "AD_texto", "SMIT_texto", "FRAC_texto",
    "similarity_score", "Text_Analysis", "Texto_Sugerido",
    "Texto_PT", "Texto_ES",
    "Texto_Observacao_PT", "Texto_Observacao_ES",
    "Texto_Qualidade_Material_PT", "Texto_Qualidade_Material_ES",
    "Texto_Dados_Basicos_PT", "Texto_Dados_Basicos_ES",
    "Texto REF LMR",
    # ── LTD History ───────────────────────────────────────────────────────
    *[f"LTD_{i}" for i in range(1, 13)],
]

# Columns included in the final per-group ZSTK split files
ZSTK_SPLIT_COLUMNS: list[str] = [
    # ── ID + Validation ───────────────────────────────────────────────────
    "Codigo_Material", "Texto_Breve_Material", "Grupo_Mercadoria",
    "classificacao_validacao", "score_validacao", "resumo_validacao",
    # ── GRPM ──────────────────────────────────────────────────────────────
    "grpm_decisao_llm", "grpm_novo_codigo", "grpm_justificativa",
    # ── Material data ─────────────────────────────────────────────────────
    "Numero_Peca_Fabricante",
    "Texto_PT", "Texto_ES",
    # ── Pricing & Reference ───────────────────────────────────────────────
    "Preco_Unitario", "ref_price_estimated", "ref_currency",
    "ref_url", "ref_search_links", "ref_availability",
    "ref_part_number_confirmed", "ref_text_coverage", "ref_coverage_gaps",
    # ── OBS / PN ──────────────────────────────────────────────────────────
    "obs_pn_presente", "obs_referencia_extraida", "obs_sugestao_texto",
    # ── Order ─────────────────────────────────────────────────────────────
    "Quantidade_OP_Calculada", "Quantidade_OP_AI",
    # ── Texts ─────────────────────────────────────────────────────────────
    "Texto_Observacao_PT", "Texto_Observacao_ES",
    # ── Image ─────────────────────────────────────────────────────────────
    "img_path", "img_qualidade", "img_substituir",
    # ── Post-analysis ────────────────────────────────────────────────────
    "pos_analise",
]


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _ensure_dir(path: Union[str, Path]) -> Path:
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sanitize(name: str) -> str:
    """Replace filesystem-unsafe characters."""
    return str(name).replace("/", "_").replace("\\", "_").strip()


def _format_group_code(grupo) -> str:
    """Return group code as integer string, stripping trailing decimal zeros."""
    try:
        return str(int(float(grupo)))
    except (ValueError, TypeError):
        return str(grupo)


def _select_export_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Return df with only the columns that exist, in the requested order."""
    return df[[c for c in columns if c in df.columns]]


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
        MTSE/<filename>.xlsx          ← full dataset (all rows)
        <RESP>/<filename>_<RESP>.xlsx ← rows for each Responsavel

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
            mask = df_export["Responsavel"] == resp if "Responsavel" in df_export.columns else \
                   df["Responsavel"] == resp
            sub  = df_export[df["Responsavel"] == resp]
            path = save_excel(sub, base_folder / safe / f"{filename}_{safe}.xlsx")
            results[safe] = str(path)
            logger.info("  -> %s: %d rows", path, len(sub))

    return results


# ---------------------------------------------------------------------------
# Debug export — full DataFrame, all columns
# ---------------------------------------------------------------------------

# Columns shown first in the debug sheet (remaining columns appended in original order)
# Debug sheet keeps ALL raw flags for deep inspection — the consolidated
# summary columns come first for quick triage, then raw detail columns.
_DEBUG_PRIORITY_COLUMNS: list[str] = [
    # ── Quick triage ──────────────────────────────────────────────────
    "Codigo_Material", "Texto_Breve_Material", "Responsavel",
    "classificacao_validacao", "score_validacao", "resumo_validacao",
    # ── Raw flags (detail — only in debug) ────────────────────────────
    "pre_analise", "ref_obs_flag",
    "leadtime_invalido", "leadtime_obs", "Prazo_Entrega_Previsto",
    "grpm_formato_invalido", "grpm_decisao_llm", "grpm_novo_codigo",
    "grpm_novo_descricao", "grpm_justificativa",
    "Grupo_Mercadoria", "Descricao_Grupo_Atual", "Grupo_Sugerido",
    "similarity_score", "Text_Analysis", "Texto_Sugerido",
    "obs_pn_presente", "obs_referencia_extraida", "obs_pesquisa_vale",
    "obs_motivo", "obs_sugestao_texto",
    "ref_reference_found", "ref_supplier", "ref_url", "ref_search_links",
    "ref_part_number_confirmed", "ref_part_number_note",
    "ref_text_coverage", "ref_coverage_gaps", "ref_validation_issues",
    "img_path", "img_qualidade", "img_motivo", "img_substituir",
    # ── Material texts ────────────────────────────────────────────────
    "Numero_Peca_Fabricante",
    "Texto_PT", "Texto_ES",
    "Texto_Observacao_PT", "Texto_Observacao_ES",
    "Texto_Dados_Basicos_PT", "Texto_Dados_Basicos_ES",
]


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




def _copy_ad_templates(target_dir: Path, total_value: float) -> None:
    """
    Copies the appropriate AD procurement templates based on total order value.
      ≤ AD_VALUE_THRESHOLD -> CPV declaration (simplified process)
      >  AD_VALUE_THRESHOLD -> Inexigibilidade + Justificativa de Preço
    """
    if not TEMPLATE_DIR.exists():
        logger.debug("Template directory not found: %s", TEMPLATE_DIR)
        return

    try:
        if total_value <= AD_VALUE_THRESHOLD:
            src = TEMPLATE_DIR / "Declaracao_CPV_template.docx"
            if src.exists():
                shutil.copy(src, target_dir / "Documentação CPV.docx")
        else:
            for template, output in [
                ("Inexigilibidade_template.docx",       "Inexibilidade.docx"),
                ("Justificativa_de_Preço_template.docx", "Justificativa de Preço.docx"),
            ]:
                src = TEMPLATE_DIR / template
                if src.exists():
                    shutil.copy(src, target_dir / output)
    except Exception as exc:
        logger.error("Error copying AD templates to %s: %s", target_dir, exc)


# ---------------------------------------------------------------------------
# Final separation: per-group folder structure
# ---------------------------------------------------------------------------

def separar_por_setor_grupo_taxacao(
    input_file_path: Optional[Union[str, Path]] = None,
) -> Path:
    """
    Reads the final analysis file and organises output into per-group folders.

    Folder structure created under <base_output_dir>/<Responsavel>/grupos/:
      AD_<grupo>/
        <grupo>_<taxacao>.xlsx    ← AD items, with procurement templates
      ZSTK/
        <grupo4d>_<taxacao>.xlsx  ← non-AD items grouped by 4-digit GRPM prefix

    Items with Analise_AI != 'REPOR' are written to:
      <base_output_dir>/<Responsavel>/materiais_nao_repor.xlsx

    Returns the base output directory path.
    """
    # ── Resolve input file ────────────────────────────────────────────────
    if not input_file_path:
        input_file_path = Path(OUTPUT_FOLDER) / "MTSE" / "Analise.xlsx"

    file_path = Path(input_file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Analysis file not found: {file_path}")

    base_output_dir = file_path.parent.parent
    df = pd.read_excel(file_path)
    logger.info("Loaded %d rows from %s", len(df), file_path)

    # ── Clean up previous group folders ───────────────────────────────────
    for resp in df["Responsavel"].dropna().unique():
        shutil.rmtree(base_output_dir / _sanitize(resp) / "grupos", ignore_errors=True)

    # ── Normalise columns ─────────────────────────────────────────────────
    df["Analise_Gestor"] = df["Analise_Gestor"].fillna("").astype(str).str.upper()
    df["Grupo_MRP"]      = df["Grupo_MRP"].fillna("").astype(str).str.upper()

    df_repor = df[df["Analise_AI"] == "REPOR"]
    df_nao   = df[df["Analise_AI"] != "REPOR"]

    # ── REPOR items ───────────────────────────────────────────────────────
    for responsavel, df_resp in df_repor.groupby("Responsavel"):
        base_resp_dir = base_output_dir / _sanitize(responsavel) / "grupos"

        # AD items
        df_ad = df_resp[df_resp["Grupo_MRP"] == "AD"]
        if not df_ad.empty:
            for (grupo, taxacao), sub in df_ad.groupby(["Grupo_Mercadoria", "Valor_Tributado"]):
                g_str  = _format_group_code(grupo)
                out    = _ensure_dir(base_resp_dir / f"AD_{g_str}")
                save_excel(
                    _select_export_columns(sub, EXPORT_COLUMNS),
                    out / f"{g_str}_{_sanitize(str(taxacao))}.xlsx",
                )
                _copy_ad_templates(out, float(sub["Valor_Total_Ordem"].sum()))

        # Non-AD (ZSTK and others)
        df_other = df_resp[df_resp["Grupo_MRP"] != "AD"].copy()
        if not df_other.empty:
            out_zstk = _ensure_dir(base_resp_dir / "ZSTK")
            df_other["_grmp4d"] = df_other["Grupo_Mercadoria"].apply(
                lambda x: _format_group_code(x)[:4]
            )
            for (g4d, taxacao), sub in df_other.groupby(["_grmp4d", "Valor_Tributado"]):
                sub_export = _select_export_columns(sub.drop(columns=["_grmp4d"]), ZSTK_SPLIT_COLUMNS)
                save_excel(
                    sub_export,
                    out_zstk / f"{g4d}_{_sanitize(str(taxacao))}.xlsx",
                )

    # ── NAO REPOR items ───────────────────────────────────────────────────
    for responsavel, df_resp in df_nao.groupby("Responsavel"):
        out = _ensure_dir(base_output_dir / _sanitize(responsavel))
        save_excel(
            _select_export_columns(df_resp, EXPORT_COLUMNS),
            out / "materiais_nao_repor.xlsx",
        )

    logger.info(
        "Separation complete. REPOR: %d | NÃO REPOR: %d",
        len(df_repor), len(df_nao),
    )
    return base_output_dir


# ---------------------------------------------------------------------------
# Dashboard data export
# ---------------------------------------------------------------------------

# Columns serialised into dashboard_data.js
_DASHBOARD_COLS: list[str] = [
    "Codigo_Material", "Texto_Breve_Material", "Responsavel",
    "Grupo_Mercadoria", "Prazo_Entrega_Previsto", "Numero_Peca_Fabricante",
    "Texto_PT", "Texto_ES", "Texto_Observacao_PT", "Texto_Observacao_ES",
    # lead time
    "leadtime_invalido", "leadtime_obs",
    # grpm
    "grpm_formato_invalido", "grpm_decisao_llm",
    "grpm_novo_codigo", "grpm_novo_descricao", "grpm_justificativa",
    # similarity
    "similarity_score",
    # obs pre-check
    "obs_pn_presente", "obs_referencia_extraida", "obs_pesquisa_vale",
    "obs_motivo", "obs_sugestao_texto",
    # reference
    "ref_reference_found", "ref_supplier", "ref_url", "ref_search_links",
    "ref_price_estimated", "ref_currency", "ref_availability",
    "ref_part_number_confirmed", "ref_part_number_note",
    "ref_text_coverage", "ref_coverage_gaps", "ref_validation_issues",
    # image
    "img_path", "img_qualidade", "img_motivo", "img_substituir",
    # validation summary
    "classificacao_validacao", "resumo_validacao",
]


def export_dashboard_data(
    df: pd.DataFrame,
    output_folder: Union[str, Path] = OUTPUT_FOLDER,
) -> Path:
    """
    Serialises *df* to a JavaScript sidecar file ``dashboard_data.js`` and
    copies ``dashboard.html`` next to it for standalone viewing.

    Also creates per-Responsavel dashboards so each analyst gets their own
    self-contained folder with dashboard.html + dashboard_data.js.

    Only the columns listed in ``_DASHBOARD_COLS`` are included; missing
    columns are silently skipped.  Boolean / NaN / Path values are normalised
    so the resulting JSON is always valid.

    Returns the path of the main output folder.
    """
    import json
    import math

    out_folder = Path(output_folder)
    _ensure_dir(out_folder)

    # Source dashboard HTML template
    dashboard_html = Path(__file__).parent.parent / "templates" / "dashboard.html"

    def _write_dashboard(folder: Path, data_df: pd.DataFrame, label: str = "") -> Path:
        """Write dashboard_data.js + copy dashboard.html to a folder."""
        _ensure_dir(folder)
        dest = folder / "dashboard_data.js"

        cols = [c for c in _DASHBOARD_COLS if c in data_df.columns]
        sub  = data_df[cols].copy()

        # Normalise types so json.dumps never chokes
        for col in sub.columns:
            if sub[col].dtype == bool or str(sub[col].dtype) == "boolean":
                sub[col] = sub[col].fillna(False).astype(bool)
            elif sub[col].dtype == object:
                sub[col] = sub[col].fillna("").astype(str)
            else:
                sub[col] = sub[col].where(sub[col].notna(), None)

        records = sub.to_dict(orient="records")

        # Replace Python float nan/inf with None
        def sanitise(obj):
            if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
                return None
            if isinstance(obj, dict):
                return {k: sanitise(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [sanitise(v) for v in obj]
            return obj

        records = sanitise(records)

        payload = json.dumps(records, ensure_ascii=False, indent=None, separators=(",", ":"))
        dest.write_text(
            f"// Auto-generated by export_module.py — {__import__('datetime').datetime.now().isoformat()}\n"
            f"window.PIPELINE_DATA={payload};\n",
            encoding="utf-8",
        )

        # Copy dashboard HTML if available
        html_dest = folder / "dashboard.html"
        if dashboard_html.exists():
            shutil.copy(dashboard_html, html_dest)

        logger.info("Dashboard %s -> %s (%d rows)", label, folder, len(records))
        return dest

    # ── Full dataset (MTSE folder) ────────────────────────────────────
    mtse_folder = out_folder / "MTSE"
    _write_dashboard(mtse_folder, df, label="FULL")

    # ── Per-Responsavel dashboards ────────────────────────────────────
    if "Responsavel" in df.columns:
        for resp in sorted(df["Responsavel"].dropna().unique()):
            resp_folder = out_folder / _sanitize(resp)
            resp_df = df[df["Responsavel"] == resp]
            _write_dashboard(resp_folder, resp_df, label=resp)

    return out_folder