"""
send_drafts.py — Zip analyst folders and create Outlook email drafts.

Provides:
  - enviar_email:  Send email via Outlook Desktop automation
  - send:          Zip a single folder + create Outlook draft
  - send_all:      Batch processing for all responsaveis
"""

from __future__ import annotations

import datetime
import logging
import sys
import zipfile
from pathlib import Path
from typing import Optional

from config.paths import OUTPUT_FOLDER
from config.personnel import (
    EMAIL_BODY_ES,
    EMAIL_BODY_PT,
    EMAIL_DOMAIN,
    EMAIL_SUBJECT,
    MTSE_RECIPIENT,
    RESPONSAVEIS,
)

from utils.formatting import configure_encoding

configure_encoding()

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def enviar_email(
    destinatario: str,
    assunto: str,
    corpo: str,
    anexo_path: str | Path | None = None,
    cc: str = "pedrohvb@itaipu.gov.br",
) -> None:
    """Send an email via Outlook Desktop automation."""
    try:
        import win32com.client  # lazy — Windows only

        outlook = win32com.client.Dispatch("Outlook.Application")
        mail = outlook.CreateItem(0)
        mail.To = destinatario
        mail.CC = cc
        mail.Subject = assunto
        mail.Body = corpo

        if anexo_path and Path(anexo_path).exists():
            mail.Attachments.Add(str(Path(anexo_path).resolve()))

        mail.Send()
        logger.info("Email sent to: %s", destinatario)
    except Exception as exc:
        logger.error("Failed to send email: %s", exc)


# ---------------------------------------------------------------------------
# Zip + Draft
# ---------------------------------------------------------------------------

def _zip_folder(source: Path, dest: Path) -> int:
    """
    Zips the contents of *source* into *dest*.
    Returns the number of files added.
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
    source   = base_path / folder
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
        import win32com.client  # lazy — Windows only

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
    base_path: Path | None = None,
    only: Optional[list[str]] = None,
    cc: str = "pedrohvb@itaipu.gov.br",
    delete_zip_after: bool = True,
) -> dict[str, bool]:
    """
    Iterates over all Responsavel subfolders in *base_path* and calls send().

    Returns a dict mapping folder name -> success (bool).
    """
    if base_path is None:
        base_path = Path(OUTPUT_FOLDER)

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
