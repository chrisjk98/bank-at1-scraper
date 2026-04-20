from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import openpyxl

GVKEY_HEADER_VARIANTS = {"gvkey", "gv_key"}
COMPANY_HEADER_VARIANTS = {"companyname", "bankname", "company"}


@dataclass(frozen=True)
class BankInput:
    gvkey: str
    company_name: str
    manual_url: Optional[str] = None


class ExcelLoaderError(ValueError):
    """Raised when an Excel input file cannot be parsed."""


def _normalize_header(value: object) -> str:
    return str(value or "").strip().lower().replace(" ", "")


def _is_header_row(row: Tuple[object, ...]) -> bool:
    if len(row) < 2:
        return False
    return _normalize_header(row[0]) in GVKEY_HEADER_VARIANTS and _normalize_header(row[1]) in COMPANY_HEADER_VARIANTS


def _validate_extension(file_path: Path) -> None:
    if file_path.suffix.lower() not in {".xlsx", ".xls"}:
        raise ExcelLoaderError("Input must be an .xlsx or .xls file")


def _load_xlsx(file_path: Path) -> List[Tuple[object, ...]]:
    workbook = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
    try:
        sheet = workbook.active
        return list(sheet.iter_rows(min_row=1, values_only=True))
    finally:
        workbook.close()


def _load_xls(file_path: Path) -> List[Tuple[object, ...]]:
    try:
        import xlrd  # type: ignore
    except ImportError as exc:
        raise ExcelLoaderError(
            "Reading .xls files requires optional dependency 'xlrd'. Install it to continue."
        ) from exc

    workbook = xlrd.open_workbook(file_path=str(file_path))
    sheet = workbook.sheet_by_index(0)
    rows: List[Tuple[object, ...]] = []
    for index in range(sheet.nrows):
        rows.append(tuple(sheet.row_values(index)))
    return rows


def load_banks_from_excel(path: str | Path) -> List[BankInput]:
    file_path = Path(path)
    if not file_path.exists():
        raise ExcelLoaderError(f"Excel file not found: {file_path}")

    _validate_extension(file_path)
    rows = _load_xlsx(file_path) if file_path.suffix.lower() == ".xlsx" else _load_xls(file_path)

    if not rows:
        raise ExcelLoaderError("Excel file is empty")

    start_row = 1 if _is_header_row(rows[0]) else 0

    unique: Dict[Tuple[str, str], BankInput] = {}
    for row in rows[start_row:]:
        if not row:
            continue

        gvkey_raw = row[0] if len(row) > 0 else None
        company_raw = row[1] if len(row) > 1 else None
        manual_url_raw = row[2] if len(row) > 2 else None

        gvkey = str(gvkey_raw or "").strip()
        company_name = str(company_raw or "").strip()
        manual_url = str(manual_url_raw or "").strip() or None

        if not gvkey or not company_name:
            continue

        dedupe_key = (gvkey, company_name.casefold())
        existing = unique.get(dedupe_key)
        if existing is None:
            unique[dedupe_key] = BankInput(gvkey=gvkey, company_name=company_name, manual_url=manual_url)
        elif existing.manual_url is None and manual_url is not None:
            unique[dedupe_key] = BankInput(gvkey=gvkey, company_name=company_name, manual_url=manual_url)

    return list(unique.values())
