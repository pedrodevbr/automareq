"""
group_separation.py — Final separation into per-group folder structure.

Organises output into per-analyst folders with AD and ZSTK subfolders,
copying appropriate procurement templates.
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Optional, Union

import pandas as pd

from config.business import AD_VALUE_THRESHOLD
from config.paths import AD_TEMPLATE_DIR, OUTPUT_FOLDER
from utils.columns import EXPORT_COLUMNS, ZSTK_SPLIT_COLUMNS, _select_export_columns
from utils.excel import _ensure_dir, save_excel
from utils.export_core import _format_group_code, _sanitize

logger = logging.getLogger(__name__)


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


def separar_por_setor_grupo_taxacao(
    input_file_path: Optional[Union[str, Path]] = None,
) -> Path:
    """
    Reads the final analysis file and organises output into per-group folders.

    Folder structure created under <base_output_dir>/<Responsavel>/grupos/:
      AD_<grupo>/
        <grupo>_<taxacao>.xlsx    <- AD items, with procurement templates
      ZSTK/
        <grupo4d>_<taxacao>.xlsx  <- non-AD items grouped by 4-digit GRPM prefix

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
        "Separation complete. REPOR: %d | NAO REPOR: %d",
        len(df_repor), len(df_nao),
    )
    return base_output_dir
