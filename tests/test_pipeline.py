import tempfile
import unittest
from pathlib import Path

from src.models import AT1Metrics, BankRecord
from src.pipeline import Pipeline


class StubExcelLoader:
    def load_banks(self, excel_path, limit=None):
        rows = [BankRecord(gvkey="1", company_name="BANK A")]
        return rows[:limit] if limit else rows


class StubURLResolver:
    def resolve(self, bank):
        class Result:
            annual_report_url = "https://example.com/report-2022.pdf"
            error = None

        return Result()


class StubURLValidator:
    def validate(self, url):
        class Result:
            valid = True
            reason = ""

        return Result()


class StubDownloader:
    def download(self, bank, url):
        class Result:
            path = Path(__file__)
            reason = ""

        return Result()


class StubParser:
    def parse(self, bank, path):
        from src.models import ParseResult

        return ParseResult(
            gvkey=bank.gvkey,
            company_name=bank.company_name,
            text="Additional Tier 1 Capital 1000 million. AT1 Capital ratio 10% CET1 12% perpetual bond requirement",
            matched_sections=["Additional Tier 1 Capital"],
        )


class StubExtractor:
    def extract(self, parsed, annual_report_url=None):
        return AT1Metrics(
            gvkey=parsed.gvkey,
            company_name=parsed.company_name,
            annual_report_url=annual_report_url,
            at1_capital_eur_mn=1000,
            at1_ratio_pct=10,
            cet1_capital=12,
            at1_instruments=["bond"],
            extraction_status="success",
            confidence_score=0.9,
        )


class StubDataValidator:
    def validate(self, metrics):
        class Outcome:
            errors = []

        return Outcome()


class StubReporter:
    def __init__(self, output_dir):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_csv(self, metrics_list, file_name="extracted_data.csv"):
        path = self.output_dir / file_name
        path.write_text("ok", encoding="utf-8")
        return path

    def write_html_report(self, metrics_list, file_name="extraction_report.html"):
        path = self.output_dir / file_name
        path.write_text("ok", encoding="utf-8")
        return path


class TestPipeline(unittest.TestCase):
    def test_run_generates_results_and_state(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            pipeline = Pipeline(
                excel_loader=StubExcelLoader(),
                url_resolver=StubURLResolver(),
                url_validator=StubURLValidator(),
                downloader=StubDownloader(),
                parser=StubParser(),
                extractor=StubExtractor(),
                validator=StubDataValidator(),
                reporter=StubReporter(tmpdir),
                state_path=Path(tmpdir) / "state.json",
            )
            results = pipeline.run(excel_path="unused.xlsx")
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0].extraction_status, "success")
            self.assertTrue((Path(tmpdir) / "state.json").exists())
            self.assertTrue((Path(tmpdir) / "extracted_data.csv").exists())
            self.assertTrue((Path(tmpdir) / "extraction_report.html").exists())


if __name__ == "__main__":
    unittest.main()
