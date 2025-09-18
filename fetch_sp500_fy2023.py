# fetch_sp500_fy2023.py (uses shareholders' equity + liabilities_to_shareholders_equity_ratio)
import os, sys, time, argparse, logging
from typing import Dict, Optional
import pandas as pd
from tqdm import tqdm

from xbrl_extract import (
    GOLDEN_SET, zero_pad_cik, ensure_dir, load_ticker_cik_map, get_companyfacts,
    pick_fact_value, compute_total_liabilities, compute_le_ratio, get_reporting_currency,
    TAGS_NET_INCOME, TAGS_REVENUE, TAGS_EQUITY, TAGS_CFO,
)

# Configurations
INPUT_CSV = r"C:\Users\kylew\OneDrive\Documents\Education\ISQS 6339 - Business Intelligence\BI Project\sp500_companies.csv"
FY = 2023
UA = "Kyle McKee kylemcke@ttu.edu"
DEFAULT_SLEEP = 1.0
DEFAULT_PAUSE_EVERY = 100
DEFAULT_PAUSE_SECONDS = 10.0
CACHE_DIR = ".cache"

def parse_args():
    ap = argparse.ArgumentParser(description="Fetch FY2023 metrics from SEC CompanyFacts for S&P 500 list.")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--sleep", type=float, default=DEFAULT_SLEEP)
    ap.add_argument("--pause-every", type=int, default=DEFAULT_PAUSE_EVERY)
    ap.add_argument("--pause-seconds", type=float, default=DEFAULT_PAUSE_SECONDS)
    ap.add_argument("--golden-only", action="store_true")
    ap.add_argument("--resume-from", type=str, default=None)
    ap.add_argument("--no-cache", action="store_true")
    ap.add_argument("--output-dir", type=str, default=".")
    return ap.parse_args()

def setup_logging(output_dir: str):
    ensure_dir(output_dir)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(os.path.join(output_dir, "run.log"), mode="w", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )

def load_input_df(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"CIK":"string","Symbol":"string","Security":"string"})
    for col in ("CIK","Symbol","Security"):
        if col not in df.columns: df[col] = pd.NA
    df["Symbol_norm"] = df["Symbol"].astype("string").str.upper().str.strip()
    df["CIK_padded"] = df["CIK"].fillna("").apply(zero_pad_cik)
    return df

def resolve_cik_for_row(row, ticker_to_cik: Dict[str,str]) -> Optional[str]:
    cik10 = str(row.get("CIK_padded") or "")
    if cik10: return cik10
    sym = str(row.get("Symbol_norm") or "")
    return ticker_to_cik.get(sym)

def main():
    args = parse_args()
    setup_logging(args.output_dir); ensure_dir(args.output_dir)

    logging.info("Loading input CSV...")
    df_in = load_input_df(INPUT_CSV)

    if args.golden_only:
        df_in = df_in[df_in["Symbol_norm"].isin(GOLDEN_SET)].reset_index(drop=True)
    if args.limit is not None:
        df_in = df_in.head(args.limit).copy()

    logging.info("Loading ticker→CIK mapping...")
    ticker_map = load_ticker_cik_map(CACHE_DIR, UA, sleep_sec=args.sleep) if not args.no_cache else {}

    metrics_rows, provenance_rows, excluded_rows = [], [], []
    processed, started = 0, (args.resume_from is None)
    skipped_nonusd, total_rows = 0, len(df_in)

    for idx, row in tqdm(df_in.iterrows(), total=total_rows, desc="Companies"):
        symbol = (row.get("Symbol") or "").strip()
        security = (row.get("Security") or "").strip()

        cik10 = resolve_cik_for_row(row, ticker_map)
        if not cik10:
            logging.warning(f"Skip {symbol} - no CIK resolvable.")
            continue

        if not started:
            started = (cik10 >= args.resume_from)
            if not started: continue

        if processed > 0 and (processed % max(1, args.pause_every) == 0):
            logging.info(f"Pausing {args.pause_seconds:.1f}s to be friendly...")
            time.sleep(args.pause_seconds)

        facts_data = get_companyfacts(cik10, CACHE_DIR, UA, use_cache=(not args.no_cache), sleep_sec=args.sleep)
        if not facts_data or "facts" not in facts_data:
            logging.warning(f"No CompanyFacts for CIK {cik10} ({symbol}).")
            processed += 1; continue

        facts = facts_data["facts"]

        # Currency gate: only exclude if we positively know it's not USD
        currency = (get_reporting_currency(facts, fy=FY) or "").upper().strip()
        if currency and currency != "USD":
            excluded_rows.append({"Symbol":symbol,"Security":security,"CIK":cik10,
                                  "currency_or_unit":currency,"reason":"Entity reporting currency not USD"})
            skipped_nonusd += 1; processed += 1; continue

        # Extract metrics (shareholders' equity preference applied in TAGS_EQUITY)
        rev = pick_fact_value(facts, TAGS_REVENUE, fy=FY)
        ni  = pick_fact_value(facts, TAGS_NET_INCOME, fy=FY)
        eq  = pick_fact_value(facts, TAGS_EQUITY, fy=FY)
        cfo = pick_fact_value(facts, TAGS_CFO, fy=FY)
        liab= compute_total_liabilities(facts, fy=FY)

        # Add clarity notes for equity variant
        if eq:
            if eq["tag"].endswith("StockholdersEquityAttributableToParent"):
                eq["notes"] = (eq.get("notes") or "") + (" Equity attributable to parent." if eq.get("notes") else "Equity attributable to parent.")
            elif eq["tag"].endswith("StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"):
                eq["notes"] = (eq.get("notes") or "") + (" Includes noncontrolling interests (NCI)." if eq.get("notes") else "Includes noncontrolling interests (NCI).")

        primary = ni or rev or liab or eq or cfo
        filing_accn = primary.get("accn") if primary else None
        filing_date = primary.get("filed") if primary else None
        source_form = primary.get("form") if primary else None

        revenue_m = rev["value_musd"] if rev else None
        net_income_m = ni["value_musd"] if ni else None
        equity_m = eq["value_musd"] if eq else None
        cfo_m = cfo["value_musd"] if cfo else None
        liabilities_m = liab["value_musd"] if liab else None

        # Liabilities / Shareholders' Equity ratio
        le_ratio = compute_le_ratio(liabilities_m, equity_m)

        partial = any(v is None for v in [revenue_m, net_income_m, equity_m, cfo_m, liabilities_m])

        metrics_rows.append({
            "Symbol": symbol,
            "Security": security,
            "CIK": cik10,
            "filing_accession": filing_accn,
            "filing_date": filing_date,
            "fiscal_year": FY,
            "source_form": source_form,
            "revenue_musd": revenue_m,
            "net_income_musd": net_income_m,
            "total_liabilities_musd": liabilities_m,
            "total_shareholders_equity_musd": equity_m,
            "cfo_musd": cfo_m,
            "liabilities_to_shareholders_equity_ratio": le_ratio,
            "partial_data": partial,
            "tag_used_revenue": rev["tag"] if rev else None,
            "tag_used_net_income": ni["tag"] if ni else None,
            "tag_used_total_liabilities": liab["tag"] if liab else None,
            "tag_used_total_shareholders_equity": eq["tag"] if eq else None,
            "tag_used_cfo": cfo["tag"] if cfo else None,
        })

        # Provenance rows (one per metric)
        def prov(metric: str, info: Optional[dict], note: Optional[str]=None):
            if info is None:
                provenance_rows.append({"Symbol":symbol,"CIK":cik10,"metric":metric,
                                        "tag":None,"uom":None,"value":None,"value_musd":None,
                                        "accn":None,"filed":None,"form":None,"fy":None,"fp":None,"frame":None,
                                        "computed":None,"notes":note or "missing"})
            else:
                provenance_rows.append({"Symbol":symbol,"CIK":cik10,"metric":metric,
                                        "tag":info.get("tag"),"uom":info.get("uom"),
                                        "value":info.get("value"),"value_musd":info.get("value_musd"),
                                        "accn":info.get("accn"),"filed":info.get("filed"),
                                        "form":info.get("form"),"fy":info.get("fy"),"fp":info.get("fp"),
                                        "frame":info.get("frame"),"computed":info.get("computed"),
                                        "notes":info.get("notes")})
        prov("revenue", rev); prov("net_income", ni); prov("total_shareholders_equity", eq); prov("cfo", cfo); prov("total_liabilities", liab)

        processed += 1

    df_metrics = pd.DataFrame(metrics_rows)
    df_prov = pd.DataFrame(provenance_rows)
    df_excl = pd.DataFrame(excluded_rows)

    metrics_csv = os.path.join(args.output_dir, "sp500_fy2023_metrics.csv")
    excl_csv = os.path.join(args.output_dir, "excluded_non_usd.csv")
    df_metrics.to_csv(metrics_csv, index=False)
    df_excl.to_csv(excl_csv, index=False)

    xlsx_path = os.path.join(args.output_dir, "sp500_fy2023_metrics.xlsx")
    with pd.ExcelWriter(xlsx_path, engine="openpyxl") as w:
        df_metrics.to_excel(w, index=False, sheet_name="metrics")
        if not df_prov.empty:
            prov_cols = ["Symbol","CIK","metric","tag","uom","value","value_musd",
                         "accn","filed","form","fy","fp","frame","computed","notes"]
            df_prov[prov_cols].to_excel(w, index=False, sheet_name="provenance")
        df_excl.to_excel(w, index=False, sheet_name="excluded_non_usd")

    print(f"\nProcessed: {processed} / {len(df_in)} rows | Excluded non-USD: {skipped_nonusd}")
    if not df_metrics.empty:
        print("\n=== Golden set preview (FY2023) ===")
        preview_cols = ["Symbol","revenue_musd","net_income_musd","total_liabilities_musd",
                        "total_shareholders_equity_musd","cfo_musd","liabilities_to_shareholders_equity_ratio","partial_data"]
        preview = df_metrics[df_metrics["Symbol"].str.upper().isin(GOLDEN_SET)][preview_cols]
        print(preview.to_string(index=False))
        print(f"\nWrote: {metrics_csv}\n       {xlsx_path}\n       {excl_csv}")
    else:
        print("No metrics to show. Check run.log for issues.")

if __name__ == "__main__":
    main()
