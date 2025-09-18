# xbrl_extract.py (namespace-aware, strict FY=2023 by end-date window, shareholders' equity & liabilities)
import os, json, time, math, logging
from datetime import datetime, date
from typing import Dict, List, Optional, Tuple

import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

GOLDEN_SET = {"AAPL", "MSFT", "AMZN", "JNJ", "JPM", "XOM", "PG", "BRK.B", "NVDA", "PEP"}

SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANYFACTS_URL_TMPL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

NS_USGAAP = "us-gaap"
NS_DEI = "dei"

# tag sets
TAGS_NET_INCOME: List[Tuple[str, str]] = [
    (NS_USGAAP, "NetIncomeLoss"),
    (NS_USGAAP, "ProfitLoss"),
    (NS_USGAAP, "NetIncomeLossAvailableToCommonStockholdersBasic"),
]

TAGS_REVENUE: List[Tuple[str, str]] = [
    (NS_USGAAP, "Revenues"),
    (NS_USGAAP, "SalesRevenueNet"),
    (NS_USGAAP, "RevenueFromContractWithCustomerExcludingAssessedTax"),
    (NS_USGAAP, "SalesRevenueGoodsNet"),
    (NS_USGAAP, "SalesRevenueServicesNet"),
]

# Shareholders' equity preference
TAGS_EQUITY: List[Tuple[str, str]] = [
    (NS_USGAAP, "StockholdersEquity"),
    (NS_USGAAP, "StockholdersEquityAttributableToParent"),
    (NS_USGAAP, "MembersEquity"),
    (NS_USGAAP, "PartnersCapital"),
    (NS_USGAAP, "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest"),
]

# Liabilities (preferred + fallbacks)
TAGS_LIABILITIES: List[Tuple[str, str]] = [
    (NS_USGAAP, "Liabilities"),
    (NS_USGAAP, "LiabilitiesAndStockholdersEquity"),  # broader; flag in notes
    (NS_USGAAP, "LiabilitiesCurrent"),                # partial; flag in notes
]

TAGS_CFO: List[Tuple[str, str]] = [
    (NS_USGAAP, "NetCashProvidedByUsedInOperatingActivities"),
    (NS_USGAAP, "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations"),
    (NS_USGAAP, "NetCashProvidedByUsedInOperatingActivitiesIndirectMethod"),
]

PREFER_FORMS = {"10-K", "10-K/A"}

# Helpers
class SecHttpError(Exception): pass

def zero_pad_cik(cik: str) -> str:
    s = "".join(ch for ch in str(cik) if ch.isdigit())
    return s.zfill(10) if s else ""

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def round_millions(x: Optional[float], ndigits: int = 2) -> Optional[float]:
    try:
        xx = float(x)
    except Exception:
        return None
    if math.isnan(xx) or math.isinf(xx): return None
    return round(xx / 1_000_000.0, ndigits)

def default_headers(user_agent: str) -> Dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate",
        "Host": "data.sec.gov",
        "Accept": "application/json",
        "Connection": "keep-alive",
    }

@retry(reraise=True, retry=retry_if_exception_type(SecHttpError),
       stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=20))
def _get_json(url: str, headers: Dict[str, str]) -> Dict:
    r = requests.get(url, headers=headers, timeout=30)
    if not r.ok:
        raise SecHttpError(f"HTTP {r.status_code} fetching {url}")
    return r.json()

def load_cached_json(path: str) -> Optional[Dict]:
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
    return None

def save_cached_json(path: str, obj: Dict) -> None:
    ensure_dir(os.path.dirname(path))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f)

def load_ticker_cik_map(cache_dir: str, user_agent: str, sleep_sec: float = 1.0) -> Dict[str, str]:
    cache_path = os.path.join(cache_dir, "mappings", "company_tickers.json")
    data = load_cached_json(cache_path)
    if data is None:
        headers = default_headers(user_agent); headers["Host"] = "www.sec.gov"
        resp = requests.get(SEC_TICKER_MAP_URL, headers=headers, timeout=30)
        if not resp.ok:
            raise SecHttpError(f"Failed to fetch ticker map: HTTP {resp.status_code}")
        data = resp.json()
        save_cached_json(cache_path, data)
        time.sleep(sleep_sec)
    out = {}
    for _, row in data.items():
        t = str(row.get("ticker","")).upper().strip()
        cik = zero_pad_cik(row.get("cik_str",""))
        if t and cik: out[t] = cik
    return out

def get_companyfacts(cik10: str, cache_dir: str, user_agent: str,
                     use_cache: bool=True, sleep_sec: float=1.0) -> Optional[Dict]:
    cache_path = os.path.join(cache_dir, "companyfacts", f"{cik10}.json")
    if use_cache:
        cached = load_cached_json(cache_path)
        if cached is not None: return cached
    headers = default_headers(user_agent)
    url = COMPANYFACTS_URL_TMPL.format(cik=cik10)
    try:
        data = _get_json(url, headers=headers)
    except Exception as e:
        logging.warning(f"CompanyFacts fetch failed for CIK {cik10}: {e}")
        return None
    save_cached_json(cache_path, data)
    time.sleep(sleep_sec)
    return data

# Fast access
def _get_fact_obj(facts: Dict, ns: str, local: str) -> Optional[Dict]:
    if not facts: return None
    nsbucket = facts.get(ns)
    if not isinstance(nsbucket, dict): return None
    return nsbucket.get(local)

def _iter_fy_items(fact_obj: Dict, uom: str="USD") -> List[Dict]:
    if not fact_obj or "units" not in fact_obj: return []
    items = fact_obj["units"].get(uom) or []
    return items if isinstance(items, list) else []

def _end_date(item: Dict) -> Optional[date]:
    try:
        if item.get("end"):
            return datetime.strptime(item["end"], "%Y-%m-%d").date()
    except Exception:
        pass
    return None

def _sort_candidates(items: List[Dict]) -> List[Dict]:
    """
    Prefer 10-K/10-K(A) first, and within the same prefer-group pick the latest FILED.
    This implementation keeps 'prefer' ascending (0 is better) and FILED descending.
    """
    def key(it):
        form = str(it.get("form","")).upper()
        prefer = 0 if form in PREFER_FORMS else 1
        filed = it.get("filed", "1900-01-01")
        try:
            ts = datetime.strptime(filed, "%Y-%m-%d").timestamp()
        except Exception:
            ts = 0.0
        return (prefer, -ts)  # earlier tuple element wins; for filed we invert to get DESC
    return sorted(items, key=key)

def _filter_annual(items: List[Dict], fy: int, allow_framed: bool) -> List[Dict]:
    """
    Strict calendar-year filter for FY:
      - fp == 'FY'
      - end date within [Jan 1, FY] .. [Dec 31, FY]
      - require no frame unless allow_framed=True
    """
    start = date(fy, 1, 1)
    end   = date(fy, 12, 31)

    out = []
    for it in items:
        if str(it.get("fp","")).upper() != "FY":
            continue
        if not allow_framed and it.get("frame") is not None:
            continue
        ed = _end_date(it)
        if ed is None:
            # If no end date exists, fall back to exact FY match as a last resort
            if it.get("fy") == fy:
                out.append(it)
            continue
        if not (start <= ed <= end):
            continue
        out.append(it)
    return _sort_candidates(out)

def pick_fact_value(facts: Dict, tags: List[Tuple[str,str]], fy: int = 2023) -> Optional[Dict]:
    """
    Try tags in order; prefer dimensionless FY facts in the exact calendar year window,
    then fall back to framed FY facts. Among candidates, prefer 10-K/10-K(A), latest filed.
    """
    if not facts: return None
    for ns, local in tags:
        fobj = _get_fact_obj(facts, ns, local)
        if not fobj: continue
        items = _iter_fy_items(fobj, uom="USD")
        if not items: continue

        # Pass 1: no frame
        cands = _filter_annual(items, fy=fy, allow_framed=False)
        # Pass 2: allow frame
        if not cands:
            cands = _filter_annual(items, fy=fy, allow_framed=True)
        if not cands: continue

        best = cands[0]
        try:
            val_num = float(best.get("val"))
        except Exception:
            continue

        return {
            "value": val_num,
            "value_musd": round_millions(val_num, 2),
            "tag": f"{ns}:{local}",
            "uom": "USD",
            "accn": best.get("accn"),
            "filed": best.get("filed"),
            "form": best.get("form"),
            "fy": best.get("fy"),
            "fp": best.get("fp"),
            "frame": best.get("frame"),
            "start": best.get("start"),
            "end": best.get("end"),
            "computed": False,
            "notes": ""
        }
    return None

# Function: return liabilities and calculate ratio
def compute_total_liabilities(facts: Dict, fy: int = 2023) -> Optional[Dict]:
    """
    Try to fetch total liabilities. Fallbacks:
      - Liabilities (preferred)
      - LiabilitiesAndStockholdersEquity (fallback; note it's broader)
      - LiabilitiesCurrent (partial; flagged)
    """
    liab = pick_fact_value(facts, TAGS_LIABILITIES, fy=fy)
    if liab:
        if liab["tag"].endswith("LiabilitiesAndStockholdersEquity"):
            liab["notes"] = "LiabilitiesAndStockholdersEquity used (broader than liabilities only)"
            liab["computed"] = True
        elif liab["tag"].endswith("LiabilitiesCurrent"):
            liab["notes"] = "Only current liabilities available (partial)"
            liab["computed"] = True
        return liab
    return None

def compute_le_ratio(total_liab_musd: Optional[float], equity_musd: Optional[float]) -> Optional[float]:
    """Liabilities/Equity; None if equity is 0 or missing."""
    if equity_musd is None or equity_musd == 0: return None
    try: return (total_liab_musd or 0.0) / equity_musd
    except Exception: return None

# Function - get reporting currency
def get_reporting_currency(facts: Dict, fy: int=2023) -> Optional[str]:
    for local in ["EntityReportingCurrencyISOCode","EntityCommonCurrencyISOCode","EntityCommonCurrency"]:
        fobj = _get_fact_obj(facts, NS_DEI, local)
        if not fobj: continue
        for unit, items in (fobj.get("units") or {}).items():
            if not isinstance(items, list): continue
            # prefer latest filed in general; no FY restriction here because some DEI tags omit fy
            items_sorted = sorted(items, key=lambda it: it.get("filed","1900-01-01"), reverse=True)
            val = str(items_sorted[0].get("val","")).upper()
            if val: return val
    return None
