S&P 500 Financial Metrics Extractor
=================================

This project provides two Python programs for pulling and processing S&P 500 company financial metrics from the U.S. SEC's EDGAR CompanyFacts API.

- fetch_sp500_fy2023.py
  Main driver script. Reads a list of S&P 500 companies, queries the SEC CompanyFacts API, extracts FY2023 values (Revenue, Net Income, Liabilities, Shareholders’ Equity, Cash Flow from Operations), and saves the results to CSV and Excel.

- xbrl_extract.py
  Helper library. Provides functions for resolving tickers to CIKs, fetching/parsing CompanyFacts JSON, selecting the best reported values, computing totals/ratios, and handling caching.

Features
--------
- Pulls the current S&P 500 company list from GitHub (default) or any CSV file/URL you specify.
- Extracts key metrics: Revenue, Net Income, Total Liabilities, Total Shareholders’ Equity, Cash Flow from Operations, Liabilities-to-Equity ratio
- Handles CIK lookup, unit conversion to millions (USD), and non-USD reporters (excluded into a separate file).
- Outputs: Metrics CSV, Excluded companies CSV, Excel workbook (metrics + provenance + excluded)
- Logging with both console and run.log.

Requirements
------------
- Python 3.9+ recommended
- Packages: pandas, tqdm, requests, tenacity, openpyxl

Install with:
    pip install pandas tqdm requests tenacity openpyxl

Usage
-----

1. Basic run (default GitHub S&P 500 list)
    python fetch_sp500_fy2023.py

   Saves outputs in current directory:
   - sp500_fy2023_metrics.csv
   - excluded_non_usd.csv
   - sp500_fy2023_metrics.xlsx

2. Custom input list (local or URL)
    python fetch_sp500_fy2023.py --input "C:\path\to\sp500_companies.csv"

3. Custom output location and filenames
    python fetch_sp500_fy2023.py --output-dir "C:\Users\Kyle\Desktop\Outputs" --output-metrics "my_metrics.csv" --output-excluded "my_excluded.csv" --output-xlsx "my_metrics.xlsx"

4. Other useful flags
   --limit 20              process only the first 20 companies
   --golden-only           process only the built-in golden set
   --resume-from 0000320193  skip ahead until reaching that CIK
   --no-cache              ignore cached API responses
   --sleep 2.0             wait 2 seconds between API calls (default 1.0)
   --pause-every 100 --pause-seconds 15  pause for 15s every 100 companies

Output Structure
----------------
- Metrics CSV / Excel sheet: one row per company with numeric results and tags used.
- Excluded CSV / Excel sheet: companies excluded because reporting currency was not USD.
- Provenance Excel sheet: detailed record of the raw fact tags, accession numbers, and filing dates used.

Notes & Best Practices
----------------------
- SEC APIs are rate-limited. Keep a reasonable --sleep value (1+ second).
- Use --no-cache to force refetching of SEC data.
- Non-USD filers are excluded automatically, with reason codes in the excluded file.
- The golden set flag is handy for quick checks against ~10 large companies.

Example Run
-----------
    python fetch_sp500_fy2023.py --output-dir "results" --limit 10

Output:
    results/
      ├── run.log
      ├── sp500_fy2023_metrics.csv
      ├── excluded_non_usd.csv
      └── sp500_fy2023_metrics.xlsx

License
-------
MIT License. Use at your own risk. This tool queries public SEC endpoints and is not affiliated with the SEC.
