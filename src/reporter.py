from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path

from .models import AT1Metrics


@dataclass
class Reporter:
    output_dir: Path = Path("data/output")

    def __post_init__(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def write_csv(self, metrics_list: list[AT1Metrics], file_name: str = "extracted_data.csv") -> Path:
        output_path = self.output_dir / file_name
        if not metrics_list:
            output_path.write_text("", encoding="utf-8")
            return output_path

        with output_path.open("w", newline="", encoding="utf-8") as csv_file:
            fieldnames = list(metrics_list[0].to_row().keys())
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for metrics in metrics_list:
                writer.writerow(metrics.to_row())
        return output_path

    def write_html_report(self, metrics_list: list[AT1Metrics], file_name: str = "extraction_report.html") -> Path:
        output_path = self.output_dir / file_name
        total = len(metrics_list)
        success = sum(1 for item in metrics_list if item.extraction_status == "success")
        partial = sum(1 for item in metrics_list if item.extraction_status == "partial")
        failed = sum(1 for item in metrics_list if item.extraction_status in {"failed", "manual_review"})

        failed_rows = "\n".join(
            f"<tr><td>{m.gvkey}</td><td>{m.company_name}</td><td>{m.extraction_status}</td><td>{m.notes}</td></tr>"
            for m in metrics_list
            if m.extraction_status in {"failed", "manual_review"}
        ) or "<tr><td colspan='4'>None</td></tr>"

        html = f"""<!doctype html>
<html>
<head><meta charset='utf-8'><title>AT1 Extraction Report</title></head>
<body>
  <h1>AT1 Extraction Summary</h1>
  <p>Total banks: {total}</p>
  <p>Success: {success} | Partial: {partial} | Failed/Manual review: {failed}</p>
  <h2>Failed / Manual Review</h2>
  <table border='1' cellspacing='0' cellpadding='4'>
    <thead><tr><th>GVKey</th><th>Company</th><th>Status</th><th>Notes</th></tr></thead>
    <tbody>{failed_rows}</tbody>
  </table>
</body>
</html>"""
        output_path.write_text(html, encoding="utf-8")
        return output_path
