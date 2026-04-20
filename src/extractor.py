from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class AT1Extraction:
    at1_capital_amount: str
    at1_ratio: str
    at1_instruments: str
    extraction_status: str


def extract_at1_data(text: str) -> AT1Extraction:
    amount_match = re.search(r"AT1[^\n]{0,80}?(\d[\d,\.]+)", text, flags=re.IGNORECASE)
    ratio_match = re.search(r"AT1[^\n]{0,80}?(\d{1,2}(?:\.\d+)?\s*%)", text, flags=re.IGNORECASE)

    amount = amount_match.group(1) if amount_match else ""
    ratio = ratio_match.group(1) if ratio_match else ""

    if amount and ratio:
        status = "success"
    elif amount or ratio:
        status = "partial"
    else:
        status = "failed"

    return AT1Extraction(
        at1_capital_amount=amount,
        at1_ratio=ratio,
        at1_instruments="AT1 instruments referenced" if status != "failed" else "",
        extraction_status=status,
    )
