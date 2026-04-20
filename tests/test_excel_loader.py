from pathlib import Path

import openpyxl

from src.excel_loader import load_banks_from_excel


def _write_workbook(path: Path) -> None:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["GVKey", "Company Name", "Manual URL"])
    sheet.append(["1001", "Bank A", ""])
    sheet.append(["", "Missing Gvkey", ""])
    sheet.append(["1002", "", ""])
    sheet.append(["1001", "Bank A", "https://example.com/report.pdf"])
    workbook.save(path)


def test_load_banks_from_excel_skips_invalid_and_deduplicates(tmp_path: Path) -> None:
    file_path = tmp_path / "banks.xlsx"
    _write_workbook(file_path)

    banks = load_banks_from_excel(file_path)

    assert len(banks) == 1
    assert banks[0].gvkey == "1001"
    assert banks[0].company_name == "Bank A"
    assert banks[0].manual_url == "https://example.com/report.pdf"
