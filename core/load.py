"""
core/load.py
============
ETL: Load SAP Excel exports, merge, validate schema, and export.

Changes from v1:
  - Column mapping driven by typed config/sources.py (no more CSV template).
  - Saves intermediate Parquet after merge for fast re-loads.
  - Schema validation after merge catches bad data before the pipeline runs.
"""

import logging
import os
from pathlib import Path

import pandas as pd

from config.paths import INPUT_FOLDER, OUTPUT_FOLDER
from config.sources import (
    SOURCE_OP,
    SOURCE_0127,
    SOURCE_0130,
    get_all_included_columns,
    get_merged_type_map,
)
from utils.export_module import export_by_responsavel

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _clean_and_convert(s):
    """Limpa e converte strings BR-formatted para numérico."""
    s = str(s).strip().replace(".", "").replace(",", ".")
    multiplier = -1 if "-" in s else 1
    s = s.replace("-", "")
    return multiplier * pd.to_numeric(s, errors="coerce")


def _apply_types(df: pd.DataFrame, type_map: dict) -> pd.DataFrame:
    """Cast columns to their declared types."""
    for col_name, target_type in type_map.items():
        if col_name not in df.columns:
            continue
        df[col_name] = df[col_name].fillna("")
        if target_type == "str":
            df[col_name] = df[col_name].astype(str)
        elif target_type == "int":
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype(pd.Int64Dtype())
        elif target_type == "float":
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        elif target_type == "datetime":
            df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
        elif target_type == "bool":
            df[col_name] = df[col_name].apply(
                lambda x: str(x).strip().upper() == "X"
            )
    return df


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

class SchemaError(Exception):
    """Raised when the merged DataFrame fails schema checks."""


def validate_schema(df: pd.DataFrame) -> None:
    """
    Post-merge validation. Raises SchemaError on critical problems.
    Logs warnings for non-critical issues.
    """
    errors: list[str] = []

    # 1. Required columns must exist
    required = [
        "Codigo_Material", "Texto_Breve_Material", "Responsavel",
        "Planejador_MRP", "Grupo_MRP", "Prazo_Entrega_Previsto",
        "Estoque_Total", "Preco_Unitario",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")

    # 2. No completely empty key column
    if "Codigo_Material" in df.columns:
        null_count = df["Codigo_Material"].isna().sum()
        if null_count > 0:
            errors.append(f"Codigo_Material has {null_count} null values")

    # 3. No duplicate materials
    if "Codigo_Material" in df.columns:
        dupes = df["Codigo_Material"].duplicated().sum()
        if dupes > 0:
            logger.warning("Schema: %d duplicate Codigo_Material rows (will keep first)", dupes)

    # 4. Numeric columns should have some non-null values
    for col in ["Estoque_Total", "Preco_Unitario"]:
        if col in df.columns and df[col].notna().sum() == 0:
            logger.warning("Schema: column '%s' is entirely null", col)

    # 5. Responsavel should not be all empty
    if "Responsavel" in df.columns:
        if df["Responsavel"].dropna().astype(str).str.strip().eq("").all():
            errors.append("Responsavel column is entirely empty")

    if errors:
        raise SchemaError("Schema validation failed:\n  - " + "\n  - ".join(errors))

    logger.info("Schema validation passed (%d rows, %d columns)", len(df), len(df.columns))


# ---------------------------------------------------------------------------
# Source loaders
# ---------------------------------------------------------------------------

def _load_op(path: Path) -> pd.DataFrame:
    """Load the OP (planned orders) source."""
    df = pd.read_excel(path, parse_dates=True, dtype={"Material": str})
    df.rename(columns=SOURCE_OP.rename_map, inplace=True)
    return df


def _load_0127(path: Path) -> pd.DataFrame:
    """Load the 0127 (material texts) source — groups by material."""
    df = pd.read_excel(path).astype(str)

    # Drop excluded columns
    df = df.drop(columns=SOURCE_0127.drop_columns, errors="ignore")

    # Rename
    rename = SOURCE_0127.rename_map
    df = df.replace("nan", pd.NA).groupby("Material").agg(
        lambda x: "\n".join(x.dropna().astype(str))
    ).reset_index()
    df.rename(columns={"Material": "Codigo_Material"}, inplace=True)
    df.rename(columns=rename, inplace=True)
    return df


def _load_0130(path: Path) -> pd.DataFrame:
    """Load the 0130 (LTD consumption history) source."""
    df = pd.read_excel(path, dtype=str)

    # Rename Material first (needed for merge key)
    df.rename(columns={"Material": "Codigo_Material"}, inplace=True)

    # Drop excluded columns
    df = df.drop(columns=SOURCE_0130.drop_columns, errors="ignore")

    # Convert numeric columns
    for col in df.columns:
        if col != "Codigo_Material":
            df[col] = df[col].apply(_clean_and_convert)

    df.rename(columns=SOURCE_0130.rename_map, inplace=True)
    df["Codigo_Material"] = df["Codigo_Material"].astype(str)
    return df


# ---------------------------------------------------------------------------
# Merge & resolve conflicts
# ---------------------------------------------------------------------------

def _resolve_suffix_conflicts(df: pd.DataFrame) -> pd.DataFrame:
    """When merge creates _t0127 / _t0130 suffixed duplicates, prefer the
    more specific source and fill gaps from the base."""
    for suffix in ("_t0127", "_t0130"):
        for col in [c for c in df.columns if c.endswith(suffix)]:
            base_col = col.replace(suffix, "")
            if base_col in df.columns:
                df[base_col] = df[col].combine_first(df[base_col])
            df.drop(columns=[col], inplace=True)
    return df


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

PARQUET_FILENAME = "etl_merged.parquet"


def process_excel_data(
    file_op: str | Path | None = None,
    file_0127: str | Path | None = None,
    file_0130: str | Path | None = None,
    *,
    use_parquet_cache: bool = False,
) -> pd.DataFrame:
    """
    Load, merge, validate and return the unified DataFrame.

    Parameters
    ----------
    file_op, file_0127, file_0130 :
        Override paths to the source Excel files. Defaults to INPUT_FOLDER/<filename>.
    use_parquet_cache :
        If True and a Parquet file already exists, skip Excel loading and return
        the cached result. Useful during development / re-runs.
    """
    # Resolve paths
    file_op = Path(file_op or os.path.join(INPUT_FOLDER, SOURCE_OP.filename))
    file_0127 = Path(file_0127 or os.path.join(INPUT_FOLDER, SOURCE_0127.filename))
    file_0130 = Path(file_0130 or os.path.join(INPUT_FOLDER, SOURCE_0130.filename))
    parquet_path = Path(OUTPUT_FOLDER) / PARQUET_FILENAME

    # --- Parquet cache shortcut ---
    if use_parquet_cache and parquet_path.exists():
        logger.info("Loading cached Parquet: %s", parquet_path)
        return pd.read_parquet(parquet_path)

    # --- Load sources ---
    op = _load_op(file_op)
    t0127 = _load_0127(file_0127)
    t0130 = _load_0130(file_0130)

    # --- Merge ---
    df = op.merge(t0127, on="Codigo_Material", how="left", suffixes=("", "_t0127"))
    df = df.merge(t0130, on="Codigo_Material", how="left", suffixes=("", "_t0130"))
    df = _resolve_suffix_conflicts(df)

    # --- Apply types ---
    type_map = get_merged_type_map()
    df = _apply_types(df, type_map)

    # --- Select & order included columns ---
    included = get_all_included_columns()
    final_cols = [c for c in included if c in df.columns]
    df = df[final_cols]

    # --- Initialize pipeline columns ---
    df["Nivel_Servico"] = 0.92
    df["Dias_Em_OP"] = 0
    df["Text_Analysis"] = ""
    df["Analise_Gestor"] = ""
    df["pre_analise"] = ""

    # --- Validate schema ---
    validate_schema(df)

    # --- Save Parquet intermediate ---
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    logger.info("Saved Parquet intermediate: %s", parquet_path)

    # --- Export Excel by responsavel ---
    export_by_responsavel(df, filename="Step1-ETL")

    return df
