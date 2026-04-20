from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

from .models import BankRecord

LOGGER = logging.getLogger(__name__)


@dataclass
class DownloadResult:
    url: str
    path: Path | None
    downloaded: bool
    skipped: bool = False
    reason: str = ""


@dataclass
class PDFDownloader:
    base_dir: Path = Path("data/pdfs")
    timeout_seconds: int = 60
    retries: int = 3
    chunk_size: int = 64 * 1024
    session: requests.Session = field(default_factory=requests.Session)

    def download(self, bank: BankRecord, url: str) -> DownloadResult:
        target_dir = self.base_dir / f"{bank.gvkey}_{self._safe_name(bank.company_name)}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "annual_report_2022.pdf"

        if target_file.exists() and target_file.stat().st_size > 1_000_000:
            return DownloadResult(url=url, path=target_file, downloaded=False, skipped=True, reason="already downloaded")

        headers = {"User-Agent": "Mozilla/5.0 (AT1-Scraper/1.0)"}
        for attempt in range(1, self.retries + 1):
            try:
                with self.session.get(url, stream=True, timeout=self.timeout_seconds, headers=headers) as response:
                    response.raise_for_status()
                    with target_file.open("wb") as file_obj:
                        for chunk in response.iter_content(chunk_size=self.chunk_size):
                            if chunk:
                                file_obj.write(chunk)
                if self._is_corrupted(target_file):
                    target_file.unlink(missing_ok=True)
                    raise ValueError("Downloaded file appears corrupted")
                return DownloadResult(url=url, path=target_file, downloaded=True)
            except (requests.RequestException, ValueError) as exc:
                LOGGER.warning("Download failed for %s attempt %s/%s: %s", url, attempt, self.retries, exc)
                sleep_seconds = 2 ** (attempt - 1)
                time.sleep(sleep_seconds)

        return DownloadResult(url=url, path=None, downloaded=False, reason="failed after retries")

    @staticmethod
    def _safe_name(name: str) -> str:
        return "".join(ch if ch.isalnum() else "_" for ch in name).strip("_")[:80]

    @staticmethod
    def _is_corrupted(file_path: Path) -> bool:
        if not file_path.exists() or file_path.stat().st_size < 50_000:
            return True
        with file_path.open("rb") as file_obj:
            prefix = file_obj.read(5)
            if prefix != b"%PDF-":
                return True
            file_obj.seek(max(file_path.stat().st_size - 1024, 0))
            tail = file_obj.read()
        digest = hashlib.sha1(tail).hexdigest()
        return "255f" not in digest and b"%%EOF" not in tail
