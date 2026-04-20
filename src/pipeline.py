from __future__ import annotations

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import Dict, List

import yaml

from .downloader import download_pdf
from .excel_loader import BankInput, load_banks_from_excel
from .extractor import extract_at1_data
from .parser import extract_text_from_pdf
from .url_resolver import URLResolver
from .url_validator import validate_annual_report_url

LOGGER = logging.getLogger(__name__)


DEFAULT_STATE_PATH = Path("data/output/pipeline_state.json")
DEFAULT_OUTPUT_PATH = Path("data/output/extracted_data.csv")
DEFAULT_CACHE_PATH = Path("data/output/url_cache.json")
DEFAULT_PDF_DIRECTORY = Path("data/pdfs")


def _load_config(path: str | Path) -> Dict[str, object]:
    config_path = Path(path)
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _load_state(path: Path) -> Dict[str, Dict[str, str]]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def _save_state(path: Path, state: Dict[str, Dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")


def _state_key(bank: BankInput) -> str:
    return f"{bank.gvkey}:{bank.company_name.casefold()}"


def run_pipeline(
    *,
    excel_path: str,
    config_path: str = "config.yaml",
    output_path: str | Path = DEFAULT_OUTPUT_PATH,
    state_path: str | Path = DEFAULT_STATE_PATH,
    cache_path: str | Path = DEFAULT_CACHE_PATH,
    pdf_directory: str | Path = DEFAULT_PDF_DIRECTORY,
    retry_failed: bool = False,
) -> List[Dict[str, str]]:
    config = _load_config(config_path)
    banks = load_banks_from_excel(excel_path)
    if not banks:
        return []

    state_path = Path(state_path)
    output_path = Path(output_path)
    pdf_directory = Path(pdf_directory)
    state = _load_state(state_path)

    resolver = URLResolver(config=config, cache_path=cache_path)

    rows_by_key: Dict[str, Dict[str, str]] = {}
    pending_downloads: List[tuple[BankInput, str]] = []
    resolved_count = 0

    for bank in banks:
        key = _state_key(bank)
        previous = state.get(key)
        if previous and (previous.get("extraction_status") == "success" or not retry_failed):
            rows_by_key[key] = previous
            continue

        resolution = resolver.resolve(bank)
        annual_report_url = resolution.resolved_url or ""
        if resolution.success:
            resolved_count += 1

        if not resolution.success:
            row = {
                "GVKey": bank.gvkey,
                "Company Name": bank.company_name,
                "Annual Report URL": "",
                "AT1 Capital Amount": "",
                "AT1 Ratio": "",
                "AT1 Instruments": "",
                "Extraction Status": "failed",
            }
            rows_by_key[key] = row
            state[key] = row
            _save_state(state_path, state)
            continue

        validation = validate_annual_report_url(
            annual_report_url,
            timeout=int(config.get("validation_timeout_seconds", 30) or 30),
        )
        if not validation.is_valid:
            row = {
                "GVKey": bank.gvkey,
                "Company Name": bank.company_name,
                "Annual Report URL": annual_report_url,
                "AT1 Capital Amount": "",
                "AT1 Ratio": "",
                "AT1 Instruments": "",
                "Extraction Status": "failed",
            }
            rows_by_key[key] = row
            state[key] = row
            _save_state(state_path, state)
            continue

        pending_downloads.append((bank, validation.final_url or annual_report_url))

    for bank, resolved_url in pending_downloads:
        key = _state_key(bank)
        pdf_path = pdf_directory / f"{bank.gvkey}.pdf"
        downloaded_pdf = download_pdf(resolved_url, pdf_path)
        text = extract_text_from_pdf(downloaded_pdf)
        extraction = extract_at1_data(text)

        row = {
            "GVKey": bank.gvkey,
            "Company Name": bank.company_name,
            "Annual Report URL": resolved_url,
            "AT1 Capital Amount": extraction.at1_capital_amount,
            "AT1 Ratio": extraction.at1_ratio,
            "AT1 Instruments": extraction.at1_instruments,
            "Extraction Status": extraction.extraction_status,
        }

        rows_by_key[key] = row
        state[key] = row
        _save_state(state_path, state)

    rows = [rows_by_key[_state_key(bank)] for bank in banks if _state_key(bank) in rows_by_key]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "GVKey",
                "Company Name",
                "Annual Report URL",
                "AT1 Capital Amount",
                "AT1 Ratio",
                "AT1 Instruments",
                "Extraction Status",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    success_rate = resolved_count / len(banks)
    LOGGER.info("URL resolution success rate: %.2f%% (%d/%d)", success_rate * 100, resolved_count, len(banks))

    return rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="AT1 scraper pipeline")
    parser.add_argument("--excel", required=True, help="Path to Excel bank list (.xlsx/.xls)")
    parser.add_argument("--config", default="config.yaml", help="Configuration file path")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_PATH), help="Output CSV path")
    parser.add_argument("--state", default=str(DEFAULT_STATE_PATH), help="Resume state JSON path")
    parser.add_argument("--cache", default=str(DEFAULT_CACHE_PATH), help="URL resolver cache JSON path")
    parser.add_argument("--pdf-dir", default=str(DEFAULT_PDF_DIRECTORY), help="Directory for downloaded PDF files")
    parser.add_argument(
        "--retry-failed",
        action="store_true",
        help="Retry only failed banks from state file (otherwise already-seen banks are skipped)",
    )
    return parser


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    args = build_parser().parse_args()
    run_pipeline(
        excel_path=args.excel,
        config_path=args.config,
        output_path=args.output,
        state_path=args.state,
        cache_path=args.cache,
        pdf_directory=args.pdf_dir,
        retry_failed=args.retry_failed,
    )


if __name__ == "__main__":
    main()
