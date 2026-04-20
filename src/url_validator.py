from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import requests

LOGGER = logging.getLogger(__name__)
YEAR_PATTERN = re.compile(r"(?<!\d)2022(?!\d)")


@dataclass
class URLValidationResult:
    url: str
    valid: bool
    status_code: int | None = None
    is_pdf: bool = False
    is_2022_report: bool = False
    file_size_bytes: int | None = None
    manual_review: bool = False
    reason: str = ""


@dataclass
class URLValidator:
    timeout_seconds: int = 20
    min_expected_size_bytes: int = 1_000_000
    verify_ssl: bool = False
    session: requests.Session = field(default_factory=requests.Session)

    def validate(self, url: str) -> URLValidationResult:
        try:
            response = self.session.head(
                url,
                timeout=self.timeout_seconds,
                allow_redirects=True,
                verify=self.verify_ssl,
                headers={"User-Agent": "Mozilla/5.0 (AT1-Scraper/1.0)"},
            )
            if not (200 <= response.status_code < 400):
                return URLValidationResult(
                    url=url,
                    valid=False,
                    status_code=response.status_code,
                    manual_review=True,
                    reason="URL not accessible",
                )

            content_type = (response.headers.get("content-type") or "").lower()
            content_length = response.headers.get("content-length")
            size_bytes = int(content_length) if content_length and content_length.isdigit() else None
            is_pdf = "pdf" in content_type or url.lower().endswith(".pdf")
            is_2022 = bool(YEAR_PATTERN.search(url))

            valid = is_pdf and is_2022 and (size_bytes is None or size_bytes >= self.min_expected_size_bytes)
            reason = ""
            manual_review = False
            if not is_pdf:
                reason = "Not a PDF URL"
                manual_review = True
            elif not is_2022:
                reason = "URL does not appear to target 2022 report"
                manual_review = True
            elif size_bytes is not None and size_bytes < self.min_expected_size_bytes:
                reason = "PDF unexpectedly small"
                manual_review = True

            return URLValidationResult(
                url=url,
                valid=valid,
                status_code=response.status_code,
                is_pdf=is_pdf,
                is_2022_report=is_2022,
                file_size_bytes=size_bytes,
                manual_review=manual_review,
                reason=reason,
            )
        except requests.RequestException as exc:
            LOGGER.warning("URL validation failed for %s: %s", url, exc)
            return URLValidationResult(url=url, valid=False, manual_review=True, reason=str(exc))
