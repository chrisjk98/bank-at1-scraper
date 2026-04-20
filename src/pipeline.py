from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .downloader import PDFDownloader
from .excel_loader import ExcelLoader
from .extractor import AT1Extractor
from .models import AT1Metrics
from .parser import PDFParser
from .reporter import Reporter
from .url_resolver import URLResolver
from .url_validator import URLValidator
from .validator import DataValidator

LOGGER = logging.getLogger(__name__)


@dataclass
class Pipeline:
    excel_loader: ExcelLoader
    url_resolver: URLResolver
    url_validator: URLValidator
    downloader: PDFDownloader
    parser: PDFParser
    extractor: AT1Extractor
    validator: DataValidator
    reporter: Reporter
    state_path: Path = Path("data/output/pipeline_state.json")

    def run(self, excel_path: str, limit: int | None = None, resume: bool = False) -> list[AT1Metrics]:
        banks = self.excel_loader.load_banks(excel_path, limit=limit)
        completed = self._load_state() if resume else {}
        results: list[AT1Metrics] = []

        progress_iter = banks
        try:
            from tqdm import tqdm

            progress_iter = tqdm(banks, desc="Processing banks", unit="bank")
        except Exception:
            pass

        for idx, bank in enumerate(progress_iter, start=1):
            if bank.gvkey in completed:
                metrics = AT1Metrics(**completed[bank.gvkey])
                results.append(metrics)
                continue

            LOGGER.info("[%s/%s] Processing %s (%s)", idx, len(banks), bank.company_name, bank.gvkey)
            resolution = self.url_resolver.resolve(bank)
            if not resolution.annual_report_url:
                metrics = AT1Metrics(
                    gvkey=bank.gvkey,
                    company_name=bank.company_name,
                    extraction_status="manual_review",
                    confidence_score=0.0,
                    notes=resolution.error or "URL resolution failed",
                )
                results.append(metrics)
                self._save_state(results)
                continue

            validation = self.url_validator.validate(resolution.annual_report_url)
            if not validation.valid:
                metrics = AT1Metrics(
                    gvkey=bank.gvkey,
                    company_name=bank.company_name,
                    annual_report_url=resolution.annual_report_url,
                    extraction_status="manual_review",
                    confidence_score=0.0,
                    notes=validation.reason,
                )
                results.append(metrics)
                self._save_state(results)
                continue

            download = self.downloader.download(bank, resolution.annual_report_url)
            if not download.path:
                metrics = AT1Metrics(
                    gvkey=bank.gvkey,
                    company_name=bank.company_name,
                    annual_report_url=resolution.annual_report_url,
                    extraction_status="failed",
                    confidence_score=0.0,
                    notes=download.reason,
                )
                results.append(metrics)
                self._save_state(results)
                continue

            parsed = self.parser.parse(bank, download.path)
            metrics = self.extractor.extract(parsed, annual_report_url=resolution.annual_report_url)
            validation_outcome = self.validator.validate(metrics)
            if validation_outcome.errors:
                prior_notes = metrics.notes.strip()
                metrics.notes = "; ".join(filter(None, [prior_notes, *validation_outcome.errors]))
                metrics.extraction_status = "manual_review"
            results.append(metrics)
            self._save_state(results)

        self.reporter.write_csv(results)
        self.reporter.write_html_report(results)
        return results

    def _save_state(self, metrics_list: list[AT1Metrics]) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {item.gvkey: item.__dict__ for item in metrics_list}
        self.state_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    def _load_state(self) -> dict[str, dict]:
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}


def build_pipeline() -> Pipeline:
    config = load_config(Path("config.yaml"))
    resolver_delay = float(config.get("rate_limiting", {}).get("request_delay_seconds", 0.5))
    resolver_retries = int(config.get("rate_limiting", {}).get("retries", 3))
    request_timeout = int(config.get("timeouts", {}).get("request_seconds", 20))
    download_timeout = int(config.get("timeouts", {}).get("download_seconds", 60))
    output = config.get("output", {})
    return Pipeline(
        excel_loader=ExcelLoader(),
        url_resolver=URLResolver(
            cache_path=Path(output.get("cache_path", "data/output/url_cache.json")),
            request_delay_seconds=resolver_delay,
            retries=resolver_retries,
            timeout_seconds=request_timeout,
        ),
        url_validator=URLValidator(timeout_seconds=request_timeout),
        downloader=PDFDownloader(timeout_seconds=download_timeout),
        parser=PDFParser(),
        extractor=AT1Extractor(),
        validator=DataValidator(),
        reporter=Reporter(output_dir=Path(output.get("csv_path", "data/output/extracted_data.csv")).parent),
        state_path=Path(output.get("state_path", "data/output/pipeline_state.json")),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="EU bank AT1 extraction pipeline")
    parser.add_argument("--excel", required=True, help="Path to input .xlsx/.xls file with GVKey and Company Name")
    parser.add_argument("--mode", choices=["fast", "standard"], default="standard", help="Pipeline mode")
    parser.add_argument("--resume", action="store_true", help="Resume from prior run state")
    parser.add_argument("--banks", type=int, default=None, help="Limit number of banks for test mode")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    pipeline = build_pipeline()
    if args.mode == "fast":
        pipeline.url_resolver.request_delay_seconds = 0.1
        pipeline.downloader.timeout_seconds = 40
    metrics = pipeline.run(args.excel, limit=args.banks, resume=args.resume)

    LOGGER.info("Finished processing %s banks", len(metrics))


def load_config(config_path: Path) -> dict[str, Any]:
    if not config_path.exists():
        return {}
    try:
        import yaml
    except ImportError:
        return {}
    with config_path.open("r", encoding="utf-8") as config_file:
        parsed = yaml.safe_load(config_file) or {}
    return parsed if isinstance(parsed, dict) else {}


if __name__ == "__main__":
    main()
