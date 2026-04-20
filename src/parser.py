from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from .models import BankRecord, ParseResult

LOGGER = logging.getLogger(__name__)

AT1_KEYWORDS = [
    "Additional Tier 1 Capital",
    "AT1 Capital",
    "Core Tier 1",
    "Pillar 3 Disclosure",
    "Capital Instruments",
]


@dataclass
class PDFParser:
    max_pages: int | None = None
    keywords: list[str] = field(default_factory=lambda: list(AT1_KEYWORDS))

    def parse(self, bank: BankRecord, pdf_path: str | Path) -> ParseResult:
        path = Path(pdf_path)
        text = ""
        tables: list[list[list[str]]] = []

        try:
            import pdfplumber

            with pdfplumber.open(path) as pdf:
                pages = pdf.pages[: self.max_pages] if self.max_pages else pdf.pages
                for page in pages:
                    page_text = page.extract_text() or ""
                    text += f"\n{page_text}"
                    page_tables = page.extract_tables() or []
                    if page_tables:
                        tables.extend(page_tables)
        except Exception as exc:
            LOGGER.warning("pdfplumber parse failed for %s: %s", path, exc)
            text = self._fallback_parse_with_pypdf(path)

        ocr_used = False
        if not text.strip():
            ocr_used = True
            text = self._ocr_fallback_placeholder(path)

        matched_sections = [keyword for keyword in self.keywords if keyword.lower() in text.lower()]
        return ParseResult(
            gvkey=bank.gvkey,
            company_name=bank.company_name,
            text=text,
            matched_sections=matched_sections,
            tables=tables,
            ocr_used=ocr_used,
        )

    @staticmethod
    def _fallback_parse_with_pypdf(path: Path) -> str:
        try:
            from pypdf import PdfReader

            reader = PdfReader(str(path))
            return "\n".join((page.extract_text() or "") for page in reader.pages)
        except Exception as exc:
            LOGGER.warning("pypdf parse failed for %s: %s", path, exc)
            return ""

    @staticmethod
    def _ocr_fallback_placeholder(path: Path) -> str:
        # OCR can be integrated here (e.g., pytesseract + pdf2image) if environment provides dependencies.
        return f"OCR required for {path.name}"
