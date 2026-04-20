# bank-at1-scraper

Python pipeline to process EU bank lists (GVKey + Company Name), resolve annual report URLs, and extract Additional Tier 1 (AT1) metrics.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Input Excel format

Your Excel file (`.xlsx` or `.xls`) must contain:

- Column A: `GVKey`
- Column B: `Company Name` (or `conm`)

Header names are optional; if absent, first two columns are used.

## Run pipeline

```bash
python -m src.pipeline --excel data/banks.xlsx --mode fast
python -m src.pipeline --excel data/banks.xlsx --resume
python -m src.pipeline --excel data/banks.xlsx --banks 10
```

## Output

Generated in `data/output/`:

- `extracted_data.csv`
- `extraction_report.html`
- `url_cache.json`
- `pipeline_state.json`

CSV columns:

- GVKey
- Company Name
- Annual Report URL
- AT1 Capital (EUR millions)
- AT1 Ratio (%)
- AT1 Instruments
- CET1 Capital
- Regulatory Capital Requirements
- Extraction Status
- Confidence Score
- Notes/Errors

## Troubleshooting

- If `.xls` files fail to load, install `xlrd`.
- Some banks may require manual review where annual report links are unavailable or non-PDF.
- OCR fallback is placeholder-based by default; integrate OCR toolchain in `src/parser.py` for image-only reports.

## AT1 definition

Additional Tier 1 (AT1) capital is a Basel III regulatory capital category, typically comprising perpetual subordinated instruments designed to absorb losses.
