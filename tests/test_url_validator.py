from types import SimpleNamespace

from src.url_validator import validate_annual_report_url


class StubSession:
    def __init__(self, response):
        self.response = response

    def get(self, *_args, **_kwargs):
        return self.response


def _response(url: str, content: bytes, content_type: str = "application/pdf", status: int = 200):
    return SimpleNamespace(url=url, content=content, headers={"Content-Type": content_type}, status_code=status)


def test_validate_annual_report_url_accepts_pdf_with_2022_marker() -> None:
    content = b"%PDF-1.4\nAnnual report 2022\n%%EOF"
    result = validate_annual_report_url(
        "https://example.com/report-2022.pdf",
        session=StubSession(_response("https://example.com/report-2022.pdf", content)),
    )

    assert result.is_valid is True
    assert result.reason == "ok"


def test_validate_annual_report_url_rejects_non_pdf() -> None:
    result = validate_annual_report_url(
        "https://example.com/report",
        session=StubSession(_response("https://example.com/report", b"<html></html>", "text/html")),
    )

    assert result.is_valid is False
    assert result.reason == "not_pdf"
