"""Tests for core/load.py — ETL, validation, Parquet cache."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from core.load import (
    SchemaError,
    _apply_types,
    _clean_and_convert,
    _load_0127,
    _load_0130,
    _load_op,
    _resolve_suffix_conflicts,
    process_excel_data,
    validate_schema,
)


# ---------------------------------------------------------------------------
# _clean_and_convert
# ---------------------------------------------------------------------------

class TestCleanAndConvert:
    def test_br_format_positive(self):
        assert _clean_and_convert("1.234,56") == 1234.56

    def test_br_format_negative(self):
        result = _clean_and_convert("1.234,56-")
        assert result == pytest.approx(-1234.56)

    def test_plain_number(self):
        assert _clean_and_convert("42") == 42.0

    def test_garbage_returns_nan(self):
        import math
        assert math.isnan(_clean_and_convert("abc"))

    def test_empty_string(self):
        import math
        assert math.isnan(_clean_and_convert(""))


# ---------------------------------------------------------------------------
# _apply_types
# ---------------------------------------------------------------------------

class TestApplyTypes:
    def test_str_type(self):
        df = pd.DataFrame({"col": [123, 456]})
        result = _apply_types(df, {"col": "str"})
        assert result["col"].dtype == object
        assert result["col"].iloc[0] == "123"

    def test_int_type(self):
        df = pd.DataFrame({"col": ["10", "20"]})
        result = _apply_types(df, {"col": "int"})
        assert result["col"].dtype == pd.Int64Dtype()

    def test_float_type(self):
        df = pd.DataFrame({"col": ["1.5", "2.5"]})
        result = _apply_types(df, {"col": "float"})
        assert result["col"].dtype == float

    def test_datetime_type(self):
        df = pd.DataFrame({"col": ["2025-01-15", "2025-02-20"]})
        result = _apply_types(df, {"col": "datetime"})
        assert pd.api.types.is_datetime64_any_dtype(result["col"])

    def test_bool_type(self):
        df = pd.DataFrame({"col": ["X", "", "x"]})
        result = _apply_types(df, {"col": "bool"})
        assert result["col"].tolist() == [True, False, True]

    def test_missing_column_ignored(self):
        df = pd.DataFrame({"col": [1]})
        result = _apply_types(df, {"nonexistent": "str"})
        assert list(result.columns) == ["col"]


# ---------------------------------------------------------------------------
# _resolve_suffix_conflicts
# ---------------------------------------------------------------------------

class TestResolveSuffixConflicts:
    def test_t0130_preferred(self):
        df = pd.DataFrame({
            "LTD_1": [1.0, 2.0],
            "LTD_1_t0130": [10.0, 20.0],
        })
        result = _resolve_suffix_conflicts(df)
        assert "LTD_1_t0130" not in result.columns
        assert result["LTD_1"].tolist() == [10.0, 20.0]

    def test_t0127_fills_gaps(self):
        df = pd.DataFrame({
            "Texto_PT": [None, "base"],
            "Texto_PT_t0127": ["from_0127", None],
        })
        result = _resolve_suffix_conflicts(df)
        assert "Texto_PT_t0127" not in result.columns
        assert result["Texto_PT"].iloc[0] == "from_0127"
        assert result["Texto_PT"].iloc[1] == "base"

    def test_no_suffixes_unchanged(self):
        df = pd.DataFrame({"A": [1], "B": [2]})
        result = _resolve_suffix_conflicts(df)
        assert list(result.columns) == ["A", "B"]


# ---------------------------------------------------------------------------
# validate_schema
# ---------------------------------------------------------------------------

class TestValidateSchema:
    def test_valid_df_passes(self, merged_df):
        # Should not raise
        validate_schema(merged_df)

    def test_missing_required_column_raises(self, merged_df):
        df = merged_df.drop(columns=["Codigo_Material"])
        with pytest.raises(SchemaError, match="Missing required columns"):
            validate_schema(df)

    def test_null_material_code_raises(self, merged_df):
        merged_df.loc[0, "Codigo_Material"] = None
        with pytest.raises(SchemaError, match="null values"):
            validate_schema(merged_df)

    def test_empty_responsavel_raises(self, merged_df):
        merged_df["Responsavel"] = ""
        with pytest.raises(SchemaError, match="Responsavel.*empty"):
            validate_schema(merged_df)

    def test_duplicate_materials_warns(self, merged_df, caplog):
        import logging
        # Create a duplicate
        dup = pd.concat([merged_df, merged_df.iloc[[0]]], ignore_index=True)
        with caplog.at_level(logging.WARNING):
            validate_schema(dup)
        assert "duplicate" in caplog.text.lower()


# ---------------------------------------------------------------------------
# Source loaders (with sample DataFrames written to temp Excel)
# ---------------------------------------------------------------------------

class TestLoadOP:
    def test_renames_columns(self, sample_op_df, tmp_path):
        path = tmp_path / "OP.XLSX"
        sample_op_df.to_excel(path, index=False)
        result = _load_op(path)
        assert "Codigo_Material" in result.columns
        assert "Material" not in result.columns
        assert "Texto_Breve_Material" in result.columns


class TestLoad0127:
    def test_groups_by_material_and_renames(self, sample_0127_df, tmp_path):
        path = tmp_path / "0127.XLSX"
        sample_0127_df.to_excel(path, index=False)
        result = _load_0127(path)
        assert "Codigo_Material" in result.columns
        # Excluded columns should be gone
        assert "Status" not in result.columns
        assert "Linha" not in result.columns

    def test_drops_excluded_columns(self, sample_0127_df, tmp_path):
        path = tmp_path / "0127.XLSX"
        sample_0127_df.to_excel(path, index=False)
        result = _load_0127(path)
        for col in ["Texto CLA - pt", "Texto CLA - es", "Texto LMR"]:
            assert col not in result.columns


class TestLoad0130:
    def test_converts_to_numeric(self, sample_0130_df, tmp_path):
        path = tmp_path / "0130.XLSX"
        sample_0130_df.to_excel(path, index=False)
        result = _load_0130(path)
        assert "Codigo_Material" in result.columns
        # LTD columns should be numeric
        assert pd.api.types.is_numeric_dtype(result["LTD_1"])


# ---------------------------------------------------------------------------
# Full ETL pipeline (process_excel_data)
# ---------------------------------------------------------------------------

class TestProcessExcelData:
    def test_full_pipeline(self, sample_op_df, sample_0127_df, sample_0130_df, tmp_path):
        """End-to-end: write temp Excel files, run ETL, check output."""
        op_path = tmp_path / "OP.XLSX"
        t0127_path = tmp_path / "0127.XLSX"
        t0130_path = tmp_path / "0130.XLSX"

        sample_op_df.to_excel(op_path, index=False)
        sample_0127_df.to_excel(t0127_path, index=False)
        sample_0130_df.to_excel(t0130_path, index=False)

        # Patch OUTPUT_FOLDER so Parquet and exports go to tmp_path
        with patch("core.load.OUTPUT_FOLDER", tmp_path), \
             patch("core.load.export_by_responsavel"):
            df = process_excel_data(
                file_op=op_path,
                file_0127=t0127_path,
                file_0130=t0130_path,
            )

        assert len(df) == 2
        assert "Codigo_Material" in df.columns
        assert "Nivel_Servico" in df.columns
        # Parquet should have been saved
        assert (tmp_path / "etl_merged.parquet").exists()

    def test_parquet_cache(self, merged_df, tmp_path):
        """When Parquet exists and cache=True, skip Excel loading."""
        parquet_path = tmp_path / "etl_merged.parquet"
        merged_df.to_parquet(parquet_path, index=False)

        with patch("core.load.OUTPUT_FOLDER", tmp_path):
            result = process_excel_data(use_parquet_cache=True)

        assert len(result) == len(merged_df)
        pd.testing.assert_frame_equal(result, merged_df)
