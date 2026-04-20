from pathlib import Path

from src.excel_loader import BankInput
from src.url_resolver import URLResolver


class FakeResolver(URLResolver):
    def _lookup_ecb_data(self, bank):
        return None

    def _lookup_bank_website(self, bank):
        return "https://example.com/ar-2022.pdf"


def test_resolver_caches_successful_resolution(tmp_path: Path) -> None:
    cache_path = tmp_path / "cache.json"
    resolver = FakeResolver(config={}, cache_path=cache_path)
    bank = BankInput(gvkey="123", company_name="Demo Bank")

    first = resolver.resolve(bank)
    second = resolver.resolve(bank)

    assert first.success is True
    assert first.source == "bank_website"
    assert second.source == "cache"
    assert cache_path.exists()
