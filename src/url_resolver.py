from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

import requests

from .models import BankRecord, URLResolutionResult

LOGGER = logging.getLogger(__name__)

MAJOR_BANK_MAPPINGS: dict[str, str] = {
    "BARCLAYS PLC": "https://home.barclays/content/dam/home-barclays/documents/investor-relations/reports-and-events/annualreports/2022/Barclays-PLC-Annual-Report-2022.pdf",
    "NATWEST GROUP PLC": "https://investors.natwestgroup.com/~/media/Files/R/RBS-IR/annual-report/annual-report-2022.pdf",
    "NATL WESTMINSTER BANK": "https://investors.natwestgroup.com/~/media/Files/R/RBS-IR/annual-report/annual-report-2022.pdf",
    "BANCO SANTANDER SA": "https://www.santander.com/content/dam/santander-com/en/documentos/informe-anual/2022/annual-report-2022-en.pdf",
    "BANCO BILBAO VIZCAYA ARGENTA": "https://shareholdersandinvestors.bbva.com/wp-content/uploads/2023/02/BBVA_Annual_Report_2022.pdf",
    "CREDIT LYONNAIS SA": "https://www.credit-agricole.com/en/finance/financial-and-non-financial-information/annual-reports",
    "ABN-AMRO HOLDINGS NV": "https://www.abnamro.com/en/investor-relations/financial-publications",
    "ABN AMRO BANK NV": "https://www.abnamro.com/en/investor-relations/financial-publications",
    "AIB GROUP PLC": "https://aib.ie/content/dam/frontdoor/investorrelations/docs/resultscentre/annualreport/2022/aib-group-plc-annual-financial-report-2022.pdf",
    "HSBC HLDGS PLC": "https://www.hsbc.com/-/files/hsbc/investors/hsbc-results/2022/annual/pdfs/hsbc-holdings-plc/230221-annual-report-and-accounts-2022.pdf",
    "BNP PARIBAS": "https://invest.bnpparibas/en/document/universal-registration-document-and-annual-financial-report-2022",
    "UNICREDIT SPA": "https://www.unicreditgroup.eu/en/investors/financial-reports.html",
    "ING GROEP NV": "https://www.ing.com/Investor-relations/Financial-performance/Annual-reports.htm",
    "COMMERZBANK AG": "https://investor-relations.commerzbank.com/publications/financial-reports/",
    "DEUTSCHE BANK AG": "https://investor-relations.db.com/reports-and-events/annual-reports/",
    "DANSKE BANK AS": "https://danskebank.com/investor-relations/reports",
    "DNB ASA": "https://www.dnb.no/en/about-us/investor-relations/reports-and-presentations",
    "SVENSKA HANDELSBANKEN": "https://www.handelsbanken.com/en/investor-relations/reports-and-presentations",
    "SKANDINAVISKA ENSKILDA BANK": "https://sebgroup.com/investor-relations/reports-and-presentations",
    "KBC GROUP NV": "https://www.kbc.com/en/investor-relations/results-reports-and-presentations.html",
    "INTESA SANPAOLO SPA": "https://group.intesasanpaolo.com/en/investor-relations/financial-results",
    "LLOYDS BANKING GROUP PLC": "https://www.lloydsbankinggroup.com/investors/financial-performance/results-releases.html",
    "CREDIT AGRICOLE SA": "https://www.credit-agricole.com/en/finance/financial-and-non-financial-information/annual-reports",
    "SWEDBANK AB": "https://www.swedbank.com/investor-relations/reports-and-presentations/annual-reports.html",
    "OTP BANK PLC": "https://www.otpbank.hu/portal/en/IR_Annual_reports",
    "POWSZECHNA KASA OSZCZEDNOSCI": "https://www.pkobp.pl/investor-relations/financial-results/",
    "BANKINTER SA": "https://www.bankinter.com/webcorporativa/en/shareholders-investors/economic-financial-information",
    "ERSTE GROUP BK AG": "https://www.erstegroup.com/en/investors/reports",
    "NORDEA BANK ABP": "https://www.nordea.com/en/investors/reports-and-presentations/annual-reports",
    "CAIXABANK SA": "https://www.caixabank.com/investors-shareholders/financial-information/annual-reports_en.html",
    "BANCO BPM SPA": "https://gruppo.bancobpm.it/en/investor-relations/financial-reports/",
    "SOCIETE GENERALE GROUP": "https://investors.societegenerale.com/en/financial-and-non-financial-information/annual-reports",
    "STANDARD CHARTERED PLC": "https://www.sc.com/en/investors/annual-report/",
    "SANTANDER UK PLC": "https://www.santander.co.uk/about-santander/investor-relations/financial-information",
    "VIRGIN MONEY UK PLC": "https://www.virginmoneyukplc.com/investor-relations/results-and-reporting/",
    "COMMERZBANK": "https://investor-relations.commerzbank.com/publications/financial-reports/",
    "BANCO DE SABADELL SA": "https://www.grupbancsabadell.com/en/investor-relations/financial-information/annual-reports/",
    "UNICAJA BANCO": "https://www.unicajabanco.com/en/shareholders-investors/financial-information",
    "RAIFFEISEN BANK INTERNATI AG": "https://www.rbinternational.com/en/investors/reports.html",
    "BANQUE CANTONALE VAUDOISE": "https://www.bcv.ch/en/Group/Investor-relations",
    "BAYERISCHE HYPO- & VEREINSBK": "https://www.hypovereinsbank.de/hvb/ueber-uns/investor-relations",
    "LANDESBANK BADEN-WURTTEMBERG": "https://www.lbbw.de/group/investor-relations/financial-reports/",
    "LANDESBANK HESSEN": "https://www.helaba.com/corporate/en/investor-relations/publications/",
    "DZ BANK AG": "https://www.dzbank.com/content/dzbank_com/en/home/dz_bank/investor_relations/publications.html",
    "BPER BANCA SPA": "https://istituzionale.bper.it/en/investor-relations/financial-results",
    "ALIOR BANK SPOLKA AKCYJNA": "https://inwestor.aliorbank.pl/en/reports/periodic-reports",
    "BANK OF IRELAND GROUP PLC": "https://investorrelations.bankofireland.com/results-centre",
    "KOMERCNI BANKA AS": "https://www.kb.cz/en/about-the-bank/investor-relations/results-and-reports",
    "TATRA BANKA AS": "https://www.tatrabanka.sk/en/about-bank/investor-relations/",
    "NOVA LJUBLJANSKA BANKA": "https://www.nlbgroup.com/investor-relations/results-reports-and-presentations",
    "BANCO COMERCIAL PORTUGUES SA": "https://ind.millenniumbcp.pt/en/Institucional/investidor/Pages/Resultados.aspx",
}


@dataclass
class URLResolver:
    cache_path: Path = Path("data/output/url_cache.json")
    request_delay_seconds: float = 0.5
    timeout_seconds: int = 15
    retries: int = 3
    session: requests.Session = field(default_factory=requests.Session)

    def __post_init__(self) -> None:
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache: dict[str, str] = {}
        if self.cache_path.exists():
            try:
                self.cache = json.loads(self.cache_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                LOGGER.warning("Failed to parse URL cache file: %s", self.cache_path)

    def resolve(self, bank: BankRecord) -> URLResolutionResult:
        result = URLResolutionResult(gvkey=bank.gvkey, company_name=bank.company_name)

        cached = self.cache.get(bank.gvkey)
        if cached:
            result.annual_report_url = cached
            result.source = "cache"
            result.attempts.append("cache hit")
            return result

        strategies: list[tuple[str, Callable[[BankRecord], str | None]]] = [
            ("hardcoded", self._hardcoded_mapping),
            ("ecb", self._ecb_lookup),
            ("refinitiv", self._refinitiv_lookup),
            ("regulatory", self._country_regulatory_lookup),
            ("investor_relations", self._investor_relations_lookup),
            ("google", self._google_fallback),
        ]

        for strategy_name, strategy in strategies:
            for attempt in range(1, self.retries + 1):
                try:
                    url = strategy(bank)
                    result.attempts.append(f"{strategy_name} attempt {attempt}")
                    if url:
                        result.annual_report_url = url
                        result.source = strategy_name
                        self.cache[bank.gvkey] = url
                        self._persist_cache()
                        return result
                except requests.RequestException as exc:
                    result.attempts.append(f"{strategy_name} error: {exc}")
                    LOGGER.warning("Resolver attempt failed for %s via %s: %s", bank.company_name, strategy_name, exc)
                time.sleep(self.request_delay_seconds)

        result.error = "No URL resolved"
        return result

    def _persist_cache(self) -> None:
        self.cache_path.write_text(json.dumps(self.cache, indent=2, ensure_ascii=False), encoding="utf-8")

    def _hardcoded_mapping(self, bank: BankRecord) -> str | None:
        normalized = self._normalize_name(bank.company_name)
        if normalized in MAJOR_BANK_MAPPINGS:
            return MAJOR_BANK_MAPPINGS[normalized]
        return None

    def _ecb_lookup(self, bank: BankRecord) -> str | None:
        # ECB API integration can be plugged in when credentials/source endpoint is available.
        return None

    def _refinitiv_lookup(self, bank: BankRecord) -> str | None:
        # Refinitiv API integration can be plugged in when credentials are available.
        return None

    def _investor_relations_lookup(self, bank: BankRecord) -> str | None:
        return None

    def _country_regulatory_lookup(self, bank: BankRecord) -> str | None:
        return None

    def _google_fallback(self, bank: BankRecord) -> str | None:
        return None

    @staticmethod
    def _normalize_name(name: str) -> str:
        return re.sub(r"\s+", " ", name.upper().strip())
