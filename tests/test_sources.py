"""Tests for config/sources.py — typed column definitions."""

from config.sources import (
    ANALYSIS_COLUMNS,
    ALL_SOURCES,
    Column,
    SourceDef,
    SOURCE_OP,
    SOURCE_0127,
    SOURCE_0130,
    get_all_included_columns,
    get_merged_type_map,
)


# ---------------------------------------------------------------------------
# Column dataclass
# ---------------------------------------------------------------------------

class TestColumn:
    def test_defaults(self):
        c = Column("Original", "Standard", "str")
        assert c.included is True

    def test_excluded(self):
        c = Column("X", "Y", "int", included=False)
        assert c.included is False

    def test_frozen(self):
        c = Column("A", "B", "float")
        try:
            c.original = "Z"  # type: ignore[misc]
            assert False, "Should be frozen"
        except AttributeError:
            pass


# ---------------------------------------------------------------------------
# SourceDef properties
# ---------------------------------------------------------------------------

class TestSourceDef:
    def test_rename_map_returns_dict(self):
        assert isinstance(SOURCE_OP.rename_map, dict)
        assert SOURCE_OP.rename_map["Material"] == "Codigo_Material"

    def test_drop_columns_only_excluded(self):
        drops = SOURCE_0127.drop_columns
        assert "Status" in drops
        assert "Linha" in drops
        # Included columns must NOT appear
        assert "Descrição - pt" not in drops

    def test_included_columns_preserves_order(self):
        cols = SOURCE_OP.included_columns
        assert cols[0] == "Data_Abertura"
        assert cols[1] == "Codigo_Material"

    def test_type_map_no_duplicates(self):
        tm = SOURCE_OP.type_map
        # Every value must be a valid type string
        valid = {"str", "int", "float", "datetime", "bool"}
        for v in tm.values():
            assert v in valid

    def test_0130_has_extended_ltds(self):
        cols = SOURCE_0130.included_columns
        assert "LTD_6" in cols
        assert "LTD_17" in cols

    def test_0130_drops_duplicate_keys(self):
        drops = SOURCE_0130.drop_columns
        assert "Material" in drops or "Txt.brv.material" in drops


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

class TestHelpers:
    def test_get_all_included_columns_no_dupes(self):
        cols = get_all_included_columns()
        assert len(cols) == len(set(cols)), "Duplicate column names found"

    def test_get_all_included_columns_has_basics(self):
        cols = get_all_included_columns()
        assert "Codigo_Material" in cols
        assert "Texto_PT" in cols
        assert "LTD_17" in cols

    def test_get_merged_type_map_covers_all_included(self):
        cols = get_all_included_columns()
        tm = get_merged_type_map()
        for c in cols:
            assert c in tm, f"Missing type for included column: {c}"

    def test_all_sources_list(self):
        assert len(ALL_SOURCES) == 3
        names = [s.name for s in ALL_SOURCES]
        assert names == ["OP", "0127", "0130"]


# ---------------------------------------------------------------------------
# ANALYSIS_COLUMNS
# ---------------------------------------------------------------------------

class TestAnalysisColumns:
    def test_has_core_fields(self):
        assert "Codigo_Material" in ANALYSIS_COLUMNS
        assert "Preco_Unitario" in ANALYSIS_COLUMNS
        assert "LTD_1" in ANALYSIS_COLUMNS

    def test_has_ltds(self):
        ltds = [c for c in ANALYSIS_COLUMNS if c.startswith("LTD_")]
        assert len(ltds) == 11  # LTD_1 through LTD_11
