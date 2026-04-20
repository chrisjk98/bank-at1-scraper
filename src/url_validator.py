from __future__ import annotations

from dataclasses import dataclass

import requests

PDF_EOF_SEARCH_WINDOW = 2048
CONTENT_SEARCH_BYTES = 4096


@dataclass(frozen=True)
class URLValidationResult:
    is_valid: bool
    reason: str
    final_url: str | None = None


def _looks_like_pdf(content: bytes, content_type: str, url: str) -> bool:
    if "pdf" in content_type.lower():
        return True
    if url.lower().endswith(".pdf"):
        return True
    return content.startswith(b"%PDF")


def validate_annual_report_url(url: str, *, timeout: int = 30, session: requests.Session | None = None) -> URLValidationResult:
    client = session or requests.Session()

    try:
        response = client.get(url, timeout=timeout, allow_redirects=True)
    except requests.RequestException as exc:
        return URLValidationResult(is_valid=False, reason=f"request_failed:{exc}", final_url=None)

    if response.status_code >= 400:
        return URLValidationResult(
            is_valid=False,
            reason=f"http_{response.status_code}",
            final_url=response.url,
        )

    content = response.content or b""
    if not content:
        return URLValidationResult(is_valid=False, reason="empty_body", final_url=response.url)

    content_type = response.headers.get("Content-Type", "")
    if not _looks_like_pdf(content, content_type, response.url):
        return URLValidationResult(is_valid=False, reason="not_pdf", final_url=response.url)

    if not content.startswith(b"%PDF") or b"%%EOF" not in content[-PDF_EOF_SEARCH_WINDOW:]:
        return URLValidationResult(is_valid=False, reason="corrupted_pdf", final_url=response.url)

    marker_text = (response.url + " " + content[:CONTENT_SEARCH_BYTES].decode("latin-1", errors="ignore")).lower()
    if "2022" not in marker_text:
        return URLValidationResult(is_valid=False, reason="not_2022_report", final_url=response.url)

    return URLValidationResult(is_valid=True, reason="ok", final_url=response.url)
