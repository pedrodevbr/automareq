"""Backward-compatible shim — re-exports from utils.excel, utils.columns, utils.export_core, and core.emitters."""

from utils.excel import (                           # noqa: F401
    _apply_table_style, _ensure_dir, _URL_COLUMNS,
    save_excel,
)
from utils.columns import (                          # noqa: F401
    EXPORT_COLUMNS, ZSTK_SPLIT_COLUMNS,
    _DASHBOARD_COLS, _DEBUG_PRIORITY_COLUMNS,
    _select_export_columns,
)
from utils.export_core import (                      # noqa: F401
    _format_group_code, _sanitize,
    export_by_responsavel, export_debug,
)
from config.business import AD_VALUE_THRESHOLD       # noqa: F401
from config.paths import AD_TEMPLATE_DIR as TEMPLATE_DIR  # noqa: F401

# Emission-stage re-exports (lazy to avoid circular imports at module load)
def export_dashboard_data(*args, **kwargs):
    from core.emitters.stages.dashboard import export_dashboard_data as _fn
    return _fn(*args, **kwargs)

def separar_por_setor_grupo_taxacao(*args, **kwargs):
    from core.emitters.stages.group_separation import separar_por_setor_grupo_taxacao as _fn
    return _fn(*args, **kwargs)
