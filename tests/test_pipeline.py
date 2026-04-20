from pathlib import Path

import openpyxl

from src import pipeline


def _write_excel(path: Path) -> None:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["GVKey", "Company Name", "Manual URL"])
    sheet.append(["101", "Retry Bank", "https://example.com/report-2022.pdf"])
    workbook.save(path)


def test_pipeline_retries_failed_rows_when_requested(tmp_path: Path, monkeypatch) -> None:
    excel_path = tmp_path / "banks.xlsx"
    _write_excel(excel_path)

    output_path = tmp_path / "out.csv"
    state_path = tmp_path / "state.json"
    cache_path = tmp_path / "cache.json"
    pdf_dir = tmp_path / "pdfs"

    state_path.write_text(
        '{"101:retry bank": {"GVKey": "101", "Company Name": "Retry Bank", "Annual Report URL": "", "AT1 Capital Amount": "", "AT1 Ratio": "", "AT1 Instruments": "", "Extraction Status": "failed"}}',
        encoding="utf-8",
    )

    class GoodValidation:
        is_valid = True
        reason = "ok"
        final_url = "https://example.com/report-2022.pdf"

    monkeypatch.setattr(pipeline, "validate_annual_report_url", lambda *_args, **_kwargs: GoodValidation())

    pdf_bytes = b"%PDF-1.4\nAT1 capital 12345 and AT1 ratio 12%\n%%EOF"

    def _fake_download(_url, destination, timeout=60):
        path = Path(destination)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(pdf_bytes)
        return path

    monkeypatch.setattr(pipeline, "download_pdf", _fake_download)

    rows = pipeline.run_pipeline(
        excel_path=str(excel_path),
        output_path=output_path,
        state_path=state_path,
        cache_path=cache_path,
        pdf_directory=pdf_dir,
        retry_failed=True,
    )

    assert len(rows) == 1
    assert rows[0]["Extraction Status"] in {"success", "partial"}
    assert "report-2022.pdf" in rows[0]["Annual Report URL"]
