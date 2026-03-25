"""Backward-compatible shim — import from core.emitters.stages.templates instead."""

from core.emitters.stages.templates import (  # noqa: F401
    converter_docx_para_pdf,
    solicitar_aprovacao_cpv as solitar_aprovacao_CPV,
    substituir_texto,
)

__all__ = ["substituir_texto", "converter_docx_para_pdf", "solitar_aprovacao_CPV"]
