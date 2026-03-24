"""
sender.py
=========
Zips each Responsavel output folder and creates an Outlook draft.

Usage:
    python sender.py                        # creates drafts for all folders
    python sender.py ACOSTAJ LUCASD         # creates drafts for listed folders
"""

from __future__ import annotations

import datetime
import logging
import os
import sys
import zipfile
from pathlib import Path
from typing import Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

from config.config import OUTPUT_FOLDER, RESPONSAVEIS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# MTSE is the "everyone" folder — redirect its email to a specific recipient
MTSE_RECIPIENT = "borchard"

EMAIL_DOMAIN = "itaipu.gov"

EMAIL_SUBJECT = "Relatorio de Analise de Materiais - {date}"

EMAIL_BODY_PT = """\
Prezados,

Segue em anexo o relatorio de analise de materiais, contendo:
  - Validacao dos textos (PT / ES)
  - Validacao dos grupos de mercadoria
  - Referencias de mercado e conferencia de part numbers

Estou disponivel para qualquer esclarecimento.

Atenciosamente,
Pedro Henrique
"""

EMAIL_BODY_ES = """\
Estimados,

Adjunto encontraran el reporte de analisis de materiales, que incluye:
  - Validacion de textos (PT / ES)
  - Validacion de grupos de mercancia
  - Referencias de mercado y verificacion de part numbers

Quedo a disposicion para cualquier consulta.

Atentamente,
Pedro Henrique
"""

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

def _zip_folder(source: Path, dest: Path) -> int:
    """
    Zips the contents of *source* into *dest*.
    Returns the number of files added.
    Raises FileNotFoundError if source does not exist.
    """
    if not source.is_dir():
        raise FileNotFoundError(f"Source folder not found: {source}")

    file_count = 0
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as zf:
        for file_path in source.rglob("*"):
            if file_path.is_file():
                arcname = file_path.relative_to(source)
                zf.write(file_path, arcname)
                logger.debug("  + %s", arcname)
                file_count += 1

    if file_count == 0:
        logger.warning("No files found in '%s' — zip will be empty.", source)

    return file_count


def _build_email_body(country: str) -> str:
    return EMAIL_BODY_PT if country.upper() == "BR" else EMAIL_BODY_ES


def _resolve_recipient(folder: str, country: str) -> str:
    """Returns the full email address for a given folder / country."""
    if folder.upper() == "MTSE":
        local = MTSE_RECIPIENT
    else:
        local = folder.lower()
    return f"{local}@{EMAIL_DOMAIN}.{country.lower()}"


def send(
    folder: str,
    country: str,
    base_path: Path,
    cc: str = "pedrohvb@itaipu.gov.br",
    delete_zip_after: bool = True,
) -> bool:
    """
    Zips *folder* inside *base_path* and creates a draft in Outlook.

    Args:
        folder:           Subfolder name (e.g. 'ACOSTAJ').
        country:          'BR' or 'PY' — controls email domain and body language.
        base_path:        Parent directory that contains *folder*.
        cc:               CC address (default: pedrohvb).
        delete_zip_after: Remove the zip file after creating draft (default: True).

    Returns True on success, False on error.
    """
    source  = base_path / folder
    zip_path = base_path / f"{folder}.zip"

    logger.info("Processing '%s'...", folder)

    # ── 1. Zip ────────────────────────────────────────────────────
    try:
        count = _zip_folder(source, zip_path)
        logger.info("  Zipped %d file(s) -> %s", count, zip_path.name)
    except FileNotFoundError as exc:
        logger.error("  Skipping '%s': %s", folder, exc)
        return False
    except Exception as exc:
        logger.error("  Failed to zip '%s': %s", folder, exc)
        return False

    # ── 2. Create Draft in Outlook ────────────────────────────────
    try:
        import win32com.client  # imported here so non-Windows environments can still import the module

        outlook = win32com.client.Dispatch("Outlook.Application")
        mail    = outlook.CreateItem(0)

        mail.To      = _resolve_recipient(folder, country)
        mail.CC      = cc
        mail.Subject = EMAIL_SUBJECT.format(
            date=datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        )
        mail.Body    = _build_email_body(country)
        mail.Attachments.Add(str(zip_path.resolve()))

        logger.info("  Saving draft for %s ...", mail.To)
        mail.Save()
        logger.info("  Draft saved successfully.")

    except Exception as exc:
        logger.error("  Failed to create draft for '%s': %s", folder, exc)
        return False

    finally:
        # ── 3. Cleanup ────────────────────────────────────────────
        if zip_path.exists() and delete_zip_after:
            try:
                zip_path.unlink()
                logger.debug("  Removed temporary zip: %s", zip_path.name)
            except Exception as exc:
                logger.warning("  Could not remove zip '%s': %s", zip_path.name, exc)

    return True


def send_all(
    base_path: Path,
    only: Optional[list[str]] = None,
    cc: str = "pedrohvb@itaipu.gov.br",
    delete_zip_after: bool = True,
) -> dict[str, bool]:
    """
    Iterates over all Responsavel subfolders in *base_path* and calls send().

    Args:
        base_path:        Root output directory.
        only:             If provided, only these folder names are processed.
        cc:               CC address passed to every send() call.
        delete_zip_after: Passed through to send().

    Returns a dict mapping folder name -> success (bool).
    """
    if not base_path.is_dir():
        logger.error("Output directory not found: %s", base_path)
        return {}

    results: dict[str, bool] = {}
    filter_set = {f.upper() for f in only} if only else None

    for entry in sorted(base_path.iterdir()):
        if not entry.is_dir():
            continue

        folder_upper = entry.name.upper()

        if filter_set and folder_upper not in filter_set:
            continue

        user_data = RESPONSAVEIS.get(folder_upper)
        if not user_data:
            logger.warning("No RESPONSAVEIS entry for '%s' — skipping.", entry.name)
            continue

        country = user_data[0]
        results[entry.name] = send(
            folder=entry.name,
            country=country,
            base_path=base_path,
            cc=cc,
            delete_zip_after=delete_zip_after,
        )

    ok    = sum(v for v in results.values())
    total = len(results)
    logger.info("Done: %d/%d drafts created successfully.", ok, total)
    return results


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Optional: pass folder names as CLI args to processes only those
    # e.g.  python sender.py ACOSTAJ LUCASD
    targets = [a.upper() for a in sys.argv[1:]] or None
    send_all(base_path=Path(OUTPUT_FOLDER), only=targets)