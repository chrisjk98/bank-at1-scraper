from __future__ import annotations

from dataclasses import dataclass

from .models import AT1Metrics


@dataclass
class ValidationOutcome:
    valid: bool
    requires_manual_review: bool
    errors: list[str]


@dataclass
class DataValidator:
    def validate(self, metrics: AT1Metrics) -> ValidationOutcome:
        errors: list[str] = []

        if not metrics.gvkey:
            errors.append("Missing GVKey")
        if not metrics.company_name:
            errors.append("Missing company name")

        if metrics.at1_ratio_pct is not None and not (0 <= metrics.at1_ratio_pct <= 100):
            errors.append("AT1 ratio out of valid range")
        if metrics.cet1_capital is not None and not (0 <= metrics.cet1_capital <= 100):
            errors.append("CET1 ratio out of valid range")
        if metrics.at1_capital_eur_mn is not None and metrics.at1_capital_eur_mn <= 0:
            errors.append("AT1 capital amount must be greater than zero")

        if metrics.extraction_status == "failed":
            errors.append("Extraction failed")

        requires_manual_review = metrics.extraction_status in {"manual_review", "failed"} or bool(errors)
        return ValidationOutcome(valid=not errors, requires_manual_review=requires_manual_review, errors=errors)
