from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup

from .excel_loader import BankInput

LOGGER = logging.getLogger(__name__)
PDF_LINK_LIMIT = 20


@dataclass(frozen=True)
class URLResolutionResult:
    gvkey: str
    company_name: str
    resolved_url: Optional[str]
    source: Optional[str]
    success: bool


class URLResolver:
    def __init__(
        self,
        *,
        config: Dict[str, object],
        cache_path: str | Path,
        session: requests.Session | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config = config
        self.cache_path = Path(cache_path)
        self.session = session or requests.Session()
        self.logger = logger or LOGGER
        self.rate_limit_seconds = float(config.get("rate_limit_seconds", 0.0) or 0.0)
        self.timeout_seconds = int(config.get("resolution_timeout_seconds", 30) or 30)
        self._cache = self._load_cache()

    def _load_cache(self) -> Dict[str, str]:
        if not self.cache_path.exists():
            return {}
        try:
            return json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            self.logger.warning("Cache file is unreadable, starting with empty cache.")
            return {}

    def _save_cache(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(json.dumps(self._cache, indent=2, sort_keys=True), encoding="utf-8")

    def _throttle(self) -> None:
        if self.rate_limit_seconds > 0:
            time.sleep(self.rate_limit_seconds)

    def _cache_key(self, bank: BankInput) -> str:
        return f"{bank.gvkey}:{bank.company_name.casefold()}"

    def resolve(self, bank: BankInput) -> URLResolutionResult:
        if bank.manual_url:
            self.logger.info("Using manual override URL for %s", bank.company_name)
            return URLResolutionResult(bank.gvkey, bank.company_name, bank.manual_url, "manual_override", True)

        cache_key = self._cache_key(bank)
        cached_url = self._cache.get(cache_key)
        if cached_url:
            self.logger.info("URL cache hit for %s", bank.company_name)
            return URLResolutionResult(bank.gvkey, bank.company_name, cached_url, "cache", True)

        methods = (
            ("ecb_data", self._lookup_ecb_data),
            ("bank_website", self._lookup_bank_website),
            ("regulatory_filings", self._lookup_regulatory_filings),
            ("annual_report_registry", self._lookup_annual_report_registry),
            ("google_search", self._lookup_google_search),
        )

        for source, method in methods:
            self._throttle()
            candidate = method(bank)
            if candidate:
                self._cache[cache_key] = candidate
                self._save_cache()
                self.logger.info("Resolved %s via %s", bank.company_name, source)
                return URLResolutionResult(bank.gvkey, bank.company_name, candidate, source, True)

        self.logger.warning("Failed to resolve annual report URL for %s", bank.company_name)
        return URLResolutionResult(bank.gvkey, bank.company_name, None, None, False)

    def _extract_pdf_links(self, html: str, *, limit: int = PDF_LINK_LIMIT) -> Iterable[str]:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception:
            soup = BeautifulSoup(html, "html.parser")
        count = 0
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if ".pdf" in href.lower() and "2022" in href.lower():
                yield href
                count += 1
                if count >= limit:
                    return

    def _request_text(self, url: str) -> Optional[str]:
        try:
            response = self.session.get(url, timeout=self.timeout_seconds, allow_redirects=True)
        except requests.RequestException:
            return None

        if response.status_code >= 400:
            return None

        return response.text

    def _first_pdf_link(self, url: str) -> Optional[str]:
        html = self._request_text(url)
        if not html:
            return None
        for href in self._extract_pdf_links(html):
            candidate = href if href.startswith("http") else requests.compat.urljoin(url, href)
            if candidate.startswith("http"):
                return candidate
        return None

    def _lookup_ecb_data(self, bank: BankInput) -> Optional[str]:
        template = str(self.config.get("ecb_lookup_url_template", "")).strip()
        if not template:
            return None
        lookup_url = template.format(gvkey=quote_plus(bank.gvkey), company_name=quote_plus(bank.company_name))
        return self._first_pdf_link(lookup_url)

    def _lookup_bank_website(self, bank: BankInput) -> Optional[str]:
        template = str(self.config.get("bank_website_search_template", "")).strip()
        if not template:
            return None
        lookup_url = template.format(company_name=quote_plus(bank.company_name))
        return self._first_pdf_link(lookup_url)

    def _lookup_regulatory_filings(self, bank: BankInput) -> Optional[str]:
        endpoints = self.config.get("regulatory_filings_endpoints", [])
        if not isinstance(endpoints, list):
            return None

        for endpoint in endpoints:
            base = str(endpoint or "").strip()
            if not base:
                continue
            lookup_url = base.format(gvkey=quote_plus(bank.gvkey), company_name=quote_plus(bank.company_name))
            found = self._first_pdf_link(lookup_url)
            if found:
                return found
        return None

    def _lookup_annual_report_registry(self, bank: BankInput) -> Optional[str]:
        template = str(self.config.get("annual_report_registry_template", "")).strip()
        if not template:
            return None
        lookup_url = template.format(company_name=quote_plus(bank.company_name), gvkey=quote_plus(bank.gvkey))
        return self._first_pdf_link(lookup_url)

    def _lookup_google_search(self, bank: BankInput) -> Optional[str]:
        template = str(self.config.get("google_search_template", "")).strip()
        if not template:
            return None

        query = f'"{bank.company_name}" annual report 2022 filetype:pdf'
        lookup_url = template.format(query=quote_plus(query))
        return self._first_pdf_link(lookup_url)
