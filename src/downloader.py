from __future__ import annotations

from pathlib import Path

import requests


def download_pdf(url: str, destination: str | Path, *, timeout: int = 60) -> Path:
    output_path = Path(destination)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()

    output_path.write_bytes(response.content)
    return output_path
