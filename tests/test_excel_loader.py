import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.excel_loader import ExcelLoader


class TestExcelLoader(unittest.TestCase):
    @patch.object(ExcelLoader, "_read_rows")
    def test_load_banks_skips_missing_and_honors_limit(self, mock_read_rows):
        mock_read_rows.return_value = [
            {"gvkey": "12673", "company_name": "BARCLAYS PLC"},
            {"gvkey": "", "company_name": "BAD"},
            {"gvkey": "13294", "company_name": "NATL WESTMINSTER BANK"},
        ]
        loader = ExcelLoader()

        with tempfile.TemporaryDirectory() as tmpdir:
            excel_path = Path(tmpdir) / "banks.xlsx"
            excel_path.write_text("placeholder", encoding="utf-8")
            banks = loader.load_banks(excel_path, limit=1)

        self.assertEqual(len(banks), 1)
        self.assertEqual(banks[0].gvkey, "12673")
        self.assertIn("12673", loader.cache)


if __name__ == "__main__":
    unittest.main()
