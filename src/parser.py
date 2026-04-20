from __future__ import annotations

from pathlib import Path


def extract_text_from_pdf(path: str | Path) -> str:
    """Return a best-effort text payload by decoding PDF bytes as Latin-1.

    This keeps dependencies light for the current pipeline, but it is not a
    full PDF parser and may miss content in complex PDF layouts.
    """
    data = Path(path).read_bytes()
    return data.decode("latin-1", errors="ignore")
