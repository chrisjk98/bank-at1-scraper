# bank-at1-scraper

Scrapes 2022 annual reports for EU banks from an Excel input (`GVKey`, `Company Name`), validates report URLs, downloads PDFs, and extracts AT1 fields into CSV output.

## Input format
- Column A: `GVKey`
- Column B: `Company Name`
- Optional Column C: manual URL override for annual report PDF

Supports `.xlsx` and `.xls`.

## Run
```bash
python -m src.pipeline --excel /absolute/path/to/banks.xlsx
```

## Output CSV columns
- GVKey
- Company Name
- Annual Report URL
- AT1 Capital Amount
- AT1 Ratio
- AT1 Instruments
- Extraction Status
