import unittest

from src.extractor import AT1Extractor
from src.models import ParseResult
from src.validator import DataValidator


class TestExtractorValidator(unittest.TestCase):
    def test_extract_and_validate_success(self):
        parse_result = ParseResult(
            gvkey="15509",
            company_name="HSBC HLDGS PLC",
            text="Additional Tier 1 Capital 1234 EUR million. AT1 Capital ratio 12.4%. CET1 ratio 14.9%. perpetual bond requirement",
            matched_sections=["Additional Tier 1 Capital"],
        )
        metrics = AT1Extractor().extract(parse_result, annual_report_url="https://example.com/2022.pdf")
        outcome = DataValidator().validate(metrics)

        self.assertEqual(metrics.extraction_status, "success")
        self.assertEqual(metrics.at1_capital_eur_mn, 1234.0)
        self.assertEqual(metrics.at1_ratio_pct, 12.4)
        self.assertEqual(metrics.cet1_capital, 14.9)
        self.assertTrue(outcome.valid)


if __name__ == "__main__":
    unittest.main()
