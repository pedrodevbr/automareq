"""
images.py — Image validation stage using LLM vision.

Stage 5 of the validation pipeline.
"""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from config.personnel import country_for_responsavel
from core.validators._base import (
    LLMRunner,
    lang_instruction,
    run_llm_parallel,
    strip_json_fences,
)

logger = logging.getLogger(__name__)


# ===========================================================================
# Constants
# ===========================================================================

IMAGE_BASE_PATH = Path(r"P:\Mfotos\Padronizadas")
IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".bmp", ".tif", ".tiff"]
IMAGE_VISION_MODEL = "google/gemini-3.1-flash-lite-preview"


# ===========================================================================
# Prompts
# ===========================================================================

_IMG_SYSTEM = """\
Você é um especialista em qualidade de imagens de catálogo industrial.

Analise a foto do material fornecida e responda APENAS com um JSON válido:
{
  "img_qualidade":  "<BOA|ACEITAVEL|SUBSTITUIR>",
  "img_motivo":     "<justificativa objetiva em 1-2 frases>"
}

CRITÉRIOS:
- BOA:        Imagem nítida, boa iluminação, fundo limpo, produto claramente identificável.
- ACEITAVEL:  Qualidade razoável com pequenos defeitos (leve desfoque, fundo levemente sujo).
- SUBSTITUIR: Imagem muito antiga, extremamente desfocada, baixa resolução, produto mal visível,
              foto amarelada/desbotada, ou que claramente não representa o material adequadamente.

Sem preâmbulo, sem markdown. Responda no idioma indicado na mensagem.
"""

_IMG_USER = "Material: {codigo} — {texto_breve}. {lang}"

_IMG_EMPTY = {
    "img_path": "",
    "img_qualidade": "NAO_VERIFICADA",
    "img_motivo": "Imagem não encontrada",
    "img_substituir": False,
}


# ===========================================================================
# Helpers
# ===========================================================================

def _resolve_image_path(codigo: str) -> Optional[Path]:
    for ext in IMAGE_EXTENSIONS:
        candidate = IMAGE_BASE_PATH / f"{codigo}(A){ext}"
        if candidate.exists():
            return candidate
    return None


def _encode_image_base64(path: Path) -> Tuple[str, str]:
    ext_to_mime = {
        ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
        ".png": "image/png", ".bmp": "image/bmp",
        ".tif": "image/tiff", ".tiff": "image/tiff",
    }
    mime = ext_to_mime.get(path.suffix.lower(), "image/jpeg")
    data = base64.b64encode(path.read_bytes()).decode("utf-8")
    return data, mime


# ===========================================================================
# Single-row processor
# ===========================================================================

def _validate_image_single(row: dict) -> dict:
    """Runs LLM vision analysis on one row's image."""
    codigo = str(row.get("Codigo_Material", ""))
    texto_breve = str(row.get("Texto_Breve_Material", ""))
    responsavel = str(row.get("Responsavel", ""))
    country = country_for_responsavel(responsavel)
    img_path = _resolve_image_path(codigo)

    if img_path is None:
        return {
            "img_path": str(IMAGE_BASE_PATH / f"{codigo}(A).jpg"),
            "img_qualidade": "NAO_VERIFICADA",
            "img_motivo": "Arquivo de imagem não encontrado",
            "img_substituir": False,
        }

    user = _IMG_USER.format(codigo=codigo, texto_breve=texto_breve, lang=lang_instruction(country))

    try:
        img_data, mime_type = _encode_image_base64(img_path)

        messages = [
            {"role": "system", "content": _IMG_SYSTEM},
            {
                "role": "user",
                "content": [
                    {
                        "type": "image_url",
                        "image_url": {"url": f"data:{mime_type};base64,{img_data}"},
                    },
                    {"type": "text", "text": user},
                ],
            },
        ]

        raw = LLMRunner.chat_raw(IMAGE_VISION_MODEL, messages)
        data = json.loads(strip_json_fences(raw))
        qualidade = str(data.get("img_qualidade", "NAO_VERIFICADA")).upper()
        if qualidade not in ("BOA", "ACEITAVEL", "SUBSTITUIR"):
            qualidade = "NAO_VERIFICADA"

        return {
            "img_path": str(img_path),
            "img_qualidade": qualidade,
            "img_motivo": str(data.get("img_motivo", "")),
            "img_substituir": qualidade == "SUBSTITUIR",
        }

    except json.JSONDecodeError:
        logger.warning("Image LLM JSON error for %s", codigo)
        return {
            "img_path": str(img_path),
            "img_qualidade": "NAO_VERIFICADA",
            "img_motivo": "Erro: resposta JSON invalida do modelo",
            "img_substituir": False,
        }
    except Exception as exc:
        logger.error("Image validation error for %s: %s", codigo, exc)
        return {
            "img_path": str(img_path),
            "img_qualidade": "NAO_VERIFICADA",
            "img_motivo": f"Erro API: {exc}",
            "img_substituir": False,
        }


# ===========================================================================
# Batch runner
# ===========================================================================

def run_image_validation(
    df: pd.DataFrame,
    max_workers: int = 4,
) -> pd.DataFrame:
    """
    Stage 5 — Image Validation. LLM vision analysis of material photos.
    Adds: img_path, img_qualidade, img_motivo, img_substituir.
    """
    from utils.export_core import export_by_responsavel

    for col in ["img_path", "img_qualidade", "img_motivo", "img_substituir"]:
        df[col] = "" if col != "img_substituir" else False

    all_mask = pd.Series(True, index=df.index)
    img_output_cols = ["img_path", "img_qualidade", "img_motivo", "img_substituir"]

    print(f"   [IMG]  Validando imagens para {len(df)} materiais...")
    df = run_llm_parallel(
        df, all_mask, _validate_image_single,
        img_output_cols, _IMG_EMPTY,
        max_workers=max_workers, desc="Image Validation",
    )

    # Propagate substitution flag
    subst_mask = df["img_substituir"].astype(bool)
    if subst_mask.any() and "pre_analise" in df.columns:
        for idx, row in df[subst_mask].iterrows():
            flag = f"[IMG] Substituir imagem — {row.get('img_motivo', '')}"
            df.at[idx, "pre_analise"] = str(df.at[idx, "pre_analise"]).rstrip() + f"\n{flag}"

    # Export report
    not_found_mask = df["img_qualidade"] == "NAO_VERIFICADA"
    report_mask = subst_mask | not_found_mask
    if report_mask.any():
        img_cols = [c for c in [
            "Codigo_Material", "Texto_Breve_Material",
            "img_path", "img_qualidade", "img_motivo", "img_substituir",
            "Texto_PT", "Texto_ES", "Responsavel",
        ] if c in df.columns]
        export_by_responsavel(df.loc[report_mask, img_cols], filename="Validation_Images")
        print(f"   [!]  {subst_mask.sum()} imagens para substituir, "
              f"{not_found_mask.sum()} nao encontradas -> relatorio exportado.")

    return df
