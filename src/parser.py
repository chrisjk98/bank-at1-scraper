from __future__ import annotations

from pathlib import Path


def extract_text_from_pdf(path: str | Path) -> str:
    data = Path(path).read_bytes()
    return data.decode("latin-1", errors="ignore")
