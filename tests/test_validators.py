"""Tests for core/validators/ — rules, base helpers, and LLMRunner."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from core.validators._base import (
    LLMRunner,
    check_pn_in_obs_static,
    lang_instruction,
    run_llm_parallel,
    strip_json_fences,
)
from core.validators.rules import (
    ClassificationResponse,
    MaterialClassification,
    consolidate_validation_summary,
    validate_grpm_format,
    validate_lead_time,
    validate_ref_in_obs,
    validate_text_fields_static,
)


# ===========================================================================
# _base.py — shared helpers
# ===========================================================================

class TestStripJsonFences:
    def test_plain_json(self):
        assert strip_json_fences('{"a": 1}') == '{"a": 1}'

    def test_fenced_json(self):
        assert strip_json_fences('```json\n{"a": 1}\n```') == '{"a": 1}'

    def test_fenced_no_lang(self):
        assert strip_json_fences('```\n{"a": 1}\n```') == '{"a": 1}'

    def test_whitespace(self):
        assert strip_json_fences('  {"a": 1}  ') == '{"a": 1}'


class TestLangInstruction:
    def test_br(self):
        assert "português" in lang_instruction("BR")

    def test_py(self):
        assert "español" in lang_instruction("PY")


class TestCheckPnInObsStatic:
    def test_present(self):
        assert check_pn_in_obs_static("LC1D25", "Ref: LC1D25 Schneider")

    def test_case_insensitive(self):
        assert check_pn_in_obs_static("lc1d25", "REF: LC1D25 SCHNEIDER")

    def test_ignores_hyphens(self):
        assert check_pn_in_obs_static("C60N-2P", "C60N2P something")

    def test_absent(self):
        assert not check_pn_in_obs_static("LC1D25", "Some other text")

    def test_empty_pn(self):
        assert not check_pn_in_obs_static("", "Some text")

    def test_empty_obs(self):
        assert not check_pn_in_obs_static("LC1D25", "")


# ===========================================================================
# _base.py — LLMRunner
# ===========================================================================

class TestLLMRunner:
    def test_lazy_init_no_crash(self):
        """LLMRunner should not crash on import, only when .client() is called."""
        # Reset state
        original = LLMRunner._client
        LLMRunner._client = None
        try:
            # Import works fine
            from core.validators._base import LLMRunner as LR
            assert LR._client is None
        finally:
            LLMRunner._client = original

    def test_chat_delegates_to_client(self):
        """When _client is set to a mock, chat() uses it."""
        mock_client = MagicMock()
        mock_client.chat.completions.create.return_value.choices = [
            MagicMock(message=MagicMock(content='{"result": "ok"}'))
        ]
        original = LLMRunner._client
        LLMRunner._client = mock_client
        try:
            result = LLMRunner.chat("test-model", "system", "user")
            assert result == '{"result": "ok"}'
            mock_client.chat.completions.create.assert_called_once()
        finally:
            LLMRunner._client = original


# ===========================================================================
# _base.py — run_llm_parallel
# ===========================================================================

class TestRunLlmParallel:
    def test_basic_parallel(self):
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": ["a", "b", "c"],
        })
        mask = pd.Series([True, False, True], index=df.index)

        def row_fn(row):
            return {"result": row["value"].upper()}

        df = run_llm_parallel(
            df, mask, row_fn,
            output_columns=["result"],
            defaults={"result": "ERROR"},
            max_workers=2, desc="test",
        )
        assert df.loc[0, "result"] == "A"
        assert df.loc[1, "result"] == "ERROR"  # not in mask, gets default from init
        assert df.loc[2, "result"] == "C"

    def test_error_uses_defaults(self):
        df = pd.DataFrame({"id": [1]})
        mask = pd.Series([True], index=df.index)

        def row_fn(row):
            raise RuntimeError("boom")

        df = run_llm_parallel(
            df, mask, row_fn,
            output_columns=["out"],
            defaults={"out": "FALLBACK"},
            max_workers=1, desc="test",
        )
        assert df.loc[0, "out"] == "FALLBACK"

    def test_empty_mask(self):
        df = pd.DataFrame({"id": [1, 2]})
        mask = pd.Series([False, False], index=df.index)

        df = run_llm_parallel(
            df, mask, lambda r: {"out": "x"},
            output_columns=["out"],
            defaults={"out": ""},
            max_workers=1, desc="test",
        )
        assert "out" in df.columns


# ===========================================================================
# rules.py — validate_lead_time
# ===========================================================================

class TestValidateLeadTime:
    def test_valid_values(self):
        df = pd.DataFrame({
            "Prazo_Entrega_Previsto": [30, 60, 90],
            "pre_analise": ["", "", ""],
        })
        result, invalid = validate_lead_time(df)
        assert invalid.empty
        assert not result["leadtime_invalido"].any()

    def test_zero_flagged(self):
        df = pd.DataFrame({
            "Prazo_Entrega_Previsto": [0, 30],
            "pre_analise": ["", ""],
        })
        result, invalid = validate_lead_time(df)
        assert len(invalid) == 1
        assert result.loc[0, "leadtime_invalido"] == True

    def test_not_multiple_of_30(self):
        df = pd.DataFrame({
            "Prazo_Entrega_Previsto": [45, 60],
            "pre_analise": ["", ""],
        })
        result, invalid = validate_lead_time(df)
        assert len(invalid) == 1
        assert "múltiplo" in result.loc[0, "leadtime_obs"]

    def test_high_value_warns(self):
        df = pd.DataFrame({
            "Prazo_Entrega_Previsto": [750],
            "pre_analise": [""],
        })
        result, _ = validate_lead_time(df)
        assert "720" in result.loc[0, "leadtime_obs"]

    def test_missing_column(self):
        df = pd.DataFrame({"other": [1]})
        result, invalid = validate_lead_time(df)
        assert "leadtime_invalido" in result.columns
        assert invalid.empty


# ===========================================================================
# rules.py — validate_grpm_format
# ===========================================================================

class TestValidateGrpmFormat:
    def test_valid_codes(self):
        df = pd.DataFrame({
            "Grupo_Mercadoria": ["1234", "56789012"],
            "pre_analise": ["", ""],
        })
        result, invalid = validate_grpm_format(df)
        assert invalid.empty

    def test_99_is_invalid(self):
        df = pd.DataFrame({
            "Grupo_Mercadoria": ["99"],
            "pre_analise": [""],
        })
        _, invalid = validate_grpm_format(df)
        assert len(invalid) == 1

    def test_short_code_invalid(self):
        df = pd.DataFrame({
            "Grupo_Mercadoria": ["12"],
            "pre_analise": [""],
        })
        _, invalid = validate_grpm_format(df)
        assert len(invalid) == 1

    def test_non_numeric_invalid(self):
        df = pd.DataFrame({
            "Grupo_Mercadoria": ["ABC"],
            "pre_analise": [""],
        })
        _, invalid = validate_grpm_format(df)
        assert len(invalid) == 1


# ===========================================================================
# rules.py — validate_text_fields_static
# ===========================================================================

class TestValidateTextFieldsStatic:
    def test_all_empty_ok(self):
        row = {
            "Texto_Observacao_PT": "", "Texto_Observacao_ES": "",
            "Texto_Dados_Basicos_PT": "", "Texto_Dados_Basicos_ES": "",
        }
        assert validate_text_fields_static(row) == ""

    def test_obs_mismatch(self):
        row = {
            "Texto_Observacao_PT": "filled", "Texto_Observacao_ES": "",
            "Texto_Dados_Basicos_PT": "", "Texto_Dados_Basicos_ES": "",
        }
        assert "Observacao" in validate_text_fields_static(row)

    def test_obs_equals_dados(self):
        row = {
            "Texto_Observacao_PT": "same text", "Texto_Observacao_ES": "otro",
            "Texto_Dados_Basicos_PT": "same text", "Texto_Dados_Basicos_ES": "otro2",
        }
        result = validate_text_fields_static(row)
        assert "igual" in result.lower()


# ===========================================================================
# rules.py — validate_ref_in_obs
# ===========================================================================

class TestValidateRefInObs:
    def test_dados_basicos_filled_ok(self):
        df = pd.DataFrame({
            "Texto_Dados_Basicos_PT": ["Has content"],
            "Numero_Peca_Fabricante": ["LC1D25"],
            "Texto_Observacao_PT": [""],
            "pre_analise": [""],
        })
        result = validate_ref_in_obs(df)
        assert result.loc[0, "ref_obs_flag"] == ""

    def test_pn_missing(self):
        df = pd.DataFrame({
            "Texto_Dados_Basicos_PT": [""],
            "Numero_Peca_Fabricante": [""],
            "Texto_Observacao_PT": ["some text"],
            "pre_analise": [""],
        })
        result = validate_ref_in_obs(df)
        assert "PN ausente" in result.loc[0, "ref_obs_flag"]

    def test_pn_not_in_obs(self):
        df = pd.DataFrame({
            "Texto_Dados_Basicos_PT": [""],
            "Numero_Peca_Fabricante": ["LC1D25"],
            "Texto_Observacao_PT": ["no reference here"],
            "pre_analise": [""],
        })
        result = validate_ref_in_obs(df)
        assert "PN ausente em Texto_Observacao" in result.loc[0, "ref_obs_flag"]


# ===========================================================================
# rules.py — consolidate_validation_summary
# ===========================================================================

class TestConsolidateValidationSummary:
    def test_clean_df_is_ok(self):
        df = pd.DataFrame({
            "Codigo_Material": ["001"],
            "pre_analise": [""],
        })
        result = consolidate_validation_summary(df)
        assert "OK" in result.loc[0, "classificacao_validacao"]
        assert result.loc[0, "resumo_validacao"] == ""

    def test_leadtime_issue_flagged(self):
        df = pd.DataFrame({
            "Codigo_Material": ["001"],
            "leadtime_invalido": [True],
            "leadtime_obs": ["Lead time zerado"],
            "pre_analise": [""],
        })
        result = consolidate_validation_summary(df)
        assert "REVISAR" in result.loc[0, "classificacao_validacao"]
        assert "[LT]" in result.loc[0, "resumo_validacao"]

    def test_drops_pre_analise(self):
        df = pd.DataFrame({
            "Codigo_Material": ["001"],
            "pre_analise": ["something"],
        })
        result = consolidate_validation_summary(df)
        assert "pre_analise" not in result.columns


# ===========================================================================
# rules.py — Pydantic schemas
# ===========================================================================

class TestPydanticSchemas:
    def test_material_classification(self):
        mc = MaterialClassification(id=1, code="S21")
        assert mc.id == 1
        assert mc.code == "S21"

    def test_classification_response(self):
        cr = ClassificationResponse(items=[
            MaterialClassification(id=1, code="S21"),
            MaterialClassification(id=2, code="U09"),
        ])
        assert len(cr.items) == 2


# ===========================================================================
# Backward compatibility
# ===========================================================================

class TestBackwardCompat:
    def test_import_from_validate(self):
        from core.validate import run_validations
        assert callable(run_validations)

    def test_import_from_validators(self):
        from core.validators import run_validations
        assert callable(run_validations)
