import tempfile
import unittest
from pathlib import Path

from src.models import BankRecord
from src.url_resolver import URLResolver


class TestURLResolver(unittest.TestCase):
    def test_resolve_uses_hardcoded_mapping_and_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            resolver = URLResolver(cache_path=Path(tmpdir) / "cache.json", request_delay_seconds=0)
            bank = BankRecord(gvkey="12673", company_name="BARCLAYS PLC")
            result = resolver.resolve(bank)
            self.assertEqual(result.source, "hardcoded")
            self.assertTrue(result.annual_report_url)

            result_2 = resolver.resolve(bank)
            self.assertEqual(result_2.source, "cache")


if __name__ == "__main__":
    unittest.main()
