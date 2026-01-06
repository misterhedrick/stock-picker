"""
Tradier Options Scanner Lambda (single file, env-driven) + API Gateway proxy response

Key features:
- Scans tickers from env var TICKERS
- Expiration selection:
    - Never same-day expiry (no 0DTE)
    - If EXPIRATION override is future: scan only that expiry
    - Else: scan expirations within EXP_WINDOW_DAYS (default 14)
      OR (if fewer than MIN_EXPIRATIONS_TO_SCAN) scan the next MIN_EXPIRATIONS_TO_SCAN (default 5) future expirations
- Options chain with greeks=true (always)
- Enforces greeks presence (per-expiration; doesn’t kill the whole ticker if one expiry is weird)
- Filters by:
    - MAX_COST_PER_CONTRACT (actual USD paid per contract)
    - MIN_QUALITY_SCORE (0..100)
- Returns ONLY the best (highest-quality) contract per ticker (across scanned expirations)
- Sanitizes output so NaN/Infinity are converted to null (valid JSON)

Alerting (SNS Topic -> email/SMS/etc via subscriptions):
- If results are non-empty, publishes an SNS message to ALERT_SNS_TOPIC_ARN
- Toggle with ALERT_ENABLED=true/false (default true)

Handler: lambda_function.lambda_handler

Required env vars:
- TRADIER_TOKEN
- TICKERS (comma-separated)

Optional alert env vars:
- ALERT_ENABLED (default true)
- ALERT_SNS_TOPIC_ARN (topic ARN)
- ALERT_MAX_ITEMS (default 5)
- ALERT_SUBJECT (default "Options Scan Alert")
"""

import os
import json
import math
import asyncio
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Union, Optional

import boto3
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential_jitter, retry_if_exception_type
from aws_lambda_powertools import Logger, Metrics
from aws_lambda_powertools.metrics import MetricUnit

logger = Logger(service="tradier-options-scan")
metrics = Metrics(namespace="TradierScanner", service="tradier-options-scan")

NY_TZ = ZoneInfo("America/New_York")


# -----------------------
# Small helpers first (used by env parsing)
# -----------------------
def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "t", "yes", "y", "on")


# -----------------------
# Env config
# -----------------------
TRADIER_BASE_URL = os.getenv("TRADIER_BASE_URL", "https://api.tradier.com").rstrip("/")
TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "").strip()
TICKERS_ENV = os.getenv("TICKERS", "")

MIN_QUALITY_SCORE = float(os.getenv("MIN_QUALITY_SCORE", "70"))

MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "6"))
REQUEST_TIMEOUT_SECS = float(os.getenv("REQUEST_TIMEOUT_SECS", "10"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "4"))

EXPIRATION_OVERRIDE = os.getenv("EXPIRATION", "").strip()

# Expiration scan knobs (optional)
EXP_WINDOW_DAYS = int(os.getenv("EXP_WINDOW_DAYS", "14"))
MIN_EXPIRATIONS_TO_SCAN = int(os.getenv("MIN_EXPIRATIONS_TO_SCAN", "5"))

MAX_COST_PER_CONTRACT = float(os.getenv("MAX_COST_PER_CONTRACT", "999999"))
CONTRACT_MULTIPLIER = float(os.getenv("CONTRACT_MULTIPLIER", "100"))
FEES_PER_CONTRACT = float(os.getenv("FEES_PER_CONTRACT", "0"))

TARGET_ABS_DELTA = float(os.getenv("TARGET_ABS_DELTA", "0.25"))
DELTA_TOLERANCE = float(os.getenv("DELTA_TOLERANCE", "0.10"))
MAX_SPREAD_PCT_GOOD = float(os.getenv("MAX_SPREAD_PCT_GOOD", "0.05"))
MAX_SPREAD_PCT_BAD = float(os.getenv("MAX_SPREAD_PCT_BAD", "0.20"))

# DTE scoring knobs (optional)
MIN_DTE_DAYS = int(os.getenv("MIN_DTE_DAYS", "7"))
TARGET_DTE_DAYS = int(os.getenv("TARGET_DTE_DAYS", "14"))
DTE_LONG_DECAY_DAYS = float(os.getenv("DTE_LONG_DECAY_DAYS", "21"))

# Order ticket knobs
ORDER_SIDE = os.getenv("ORDER_SIDE", "buy_to_open").strip()
ORDER_QUANTITY = int(os.getenv("ORDER_QUANTITY", "1"))
ORDER_DURATION = os.getenv("ORDER_DURATION", "day").strip()
ORDER_TYPE = os.getenv("ORDER_TYPE", "limit").strip()

LIMIT_PRICE_MODE = os.getenv("LIMIT_PRICE_MODE", "ask").strip().lower()  # ask | mid
LIMIT_PRICE_OFFSET = float(os.getenv("LIMIT_PRICE_OFFSET", "0.00"))      # dollars per share (+/-)

# Alerting knobs
ALERT_ENABLED = env_bool("ALERT_ENABLED", True)
ALERT_SNS_TOPIC_ARN = os.getenv("ALERT_SNS_TOPIC_ARN", "").strip()
ALERT_MAX_ITEMS = int(os.getenv("ALERT_MAX_ITEMS", "5"))
ALERT_SUBJECT = os.getenv("ALERT_SUBJECT", "Options Scan Alert").strip()

_sns = boto3.client("sns")


# -----------------------
# Exceptions
# -----------------------
class TradierError(RuntimeError):
    pass

class TradierRateLimited(TradierError):
    pass

class TradierServerError(TradierError):
    pass

_RETRY_TYPES = (httpx.TimeoutException, httpx.NetworkError, TradierRateLimited, TradierServerError)


# -----------------------
# Sanitization (NaN/Inf -> None)
# -----------------------
Jsonable = Union[Dict[str, Any], List[Any], str, int, float, bool, None]

def sanitize(obj: Any) -> Jsonable:
    """
    Recursively convert:
      - float('nan'), float('inf'), float('-inf') -> None
    so API Gateway receives valid JSON.
    """
    if isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj

    if isinstance(obj, dict):
        return {str(k): sanitize(v) for k, v in obj.items()}

    if isinstance(obj, (list, tuple)):
        return [sanitize(v) for v in obj]

    if obj is None or isinstance(obj, (str, int, bool)):
        return obj

    return str(obj)


# -----------------------
# Helpers
# -----------------------
def parse_tickers(csv: str) -> List[str]:
    return [t.strip().upper() for t in csv.split(",") if t.strip()]

def auth_headers(token: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def safe_float(x: Any) -> float:
    try:
        if x is None:
            return float("nan")
        return float(x)
    except Exception:
        return float("nan")

def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x

def today_ny() -> date:
    return datetime.now(NY_TZ).date()

def now_ny_str() -> str:
    return datetime.now(NY_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")

def parse_yyyy_mm_dd(s: str) -> Optional[date]:
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def _raise_for(resp: httpx.Response, symbol: str) -> None:
    if resp.status_code == 429:
        raise TradierRateLimited(f"429 for {symbol}: {resp.text}")
    if 500 <= resp.status_code <= 599:
        raise TradierServerError(f"{resp.status_code} for {symbol}: {resp.text}")
    if resp.status_code >= 400:
        raise TradierError(f"{resp.status_code} for {symbol}: {resp.text}")
    resp.raise_for_status()

def select_expirations(dates: List[str], override: str) -> List[str]:
    """
    Expiration selection rules (NY date):
      - Never use same-day or past expirations (0DTE excluded).
      - If EXPIRATION override is provided and is future, use ONLY that.
      - Else scan expirations within EXP_WINDOW_DAYS; if fewer than MIN_EXPIRATIONS_TO_SCAN,
        scan the next MIN_EXPIRATIONS_TO_SCAN future expirations.
    """
    today = today_ny()

    if override:
        od = parse_yyyy_mm_dd(override)
        if not od:
            raise ValueError(f"Bad EXPIRATION override (expected YYYY-MM-DD): {override!r}")
        if od > today:
            return [override]
        logger.info({"msg": "Ignoring same-day/past EXPIRATION override", "override": override, "today_ny": str(today)})

    future: List[str] = []
    for d in dates:
        dd = parse_yyyy_mm_dd(d)
        if dd and dd > today:
            future.append(d)

    if not future:
        return []

    window_end = today + timedelta(days=EXP_WINDOW_DAYS)
    within_window: List[str] = []
    for d in future:
        dd = parse_yyyy_mm_dd(d)
        if dd and dd <= window_end:
            within_window.append(d)

    if len(within_window) >= MIN_EXPIRATIONS_TO_SCAN:
        return within_window

    return future[:MIN_EXPIRATIONS_TO_SCAN]

def contract_dte_days(c: Dict[str, Any]) -> int:
    ed = c.get("expiration_date") or c.get("expiration") or ""
    dd = parse_yyyy_mm_dd(str(ed))
    if not dd:
        return -1
    return (dd - today_ny()).days

def dte_score_from_days(dte: int) -> float:
    """
    Score in [0,1]:
      - below MIN_DTE_DAYS: linear ramp (harsh penalty)
      - between MIN_DTE_DAYS and TARGET_DTE_DAYS: ramps up to 1
      - beyond TARGET_DTE_DAYS: mild exponential decay
    """
    if dte <= 0:
        return 0.0

    min_dte = max(1, int(MIN_DTE_DAYS))
    target = max(min_dte + 1, int(TARGET_DTE_DAYS))
    decay = max(1e-6, float(DTE_LONG_DECAY_DAYS))

    if dte < min_dte:
        return clamp(dte / float(min_dte), 0.0, 1.0)

    if dte <= target:
        span = float(target - min_dte)
        return clamp(0.5 + 0.5 * ((dte - min_dte) / max(span, 1.0)), 0.0, 1.0)

    return clamp(math.exp(-(dte - target) / decay), 0.0, 1.0)


# -----------------------
# Alerting (SNS)
# -----------------------
def alert_if_results(payload: Dict[str, Any]) -> None:
    """
    Publish an SNS message if there are results and alerting is enabled.
    Subscribe your email/SMS/etc to ALERT_SNS_TOPIC_ARN and you’re notified.
    """
    if not ALERT_ENABLED:
        return
    if not ALERT_SNS_TOPIC_ARN:
        return

    results = payload.get("results") or []
    if not results:
        return

    top = results[:max(1, ALERT_MAX_ITEMS)]
    lines: List[str] = []
    lines.append(f"Scan time (NY): {now_ny_str()}")
    lines.append(
        f"Scanned: {payload.get('scanned')}, Qualified: {payload.get('qualified_tickers')}, Failed: {payload.get('failed_tickers')}"
    )
    lines.append("")
    lines.append("Top matches:")

    for r in top:
        t = r.get("ticker", "?")
        opt_type = (r.get("type") or "?")
        opt_type = (opt_type[0].upper() if isinstance(opt_type, str) and opt_type else "?")
        exp = r.get("expiration", "?")
        strike = r.get("strike", "?")
        q = r.get("quality")
        mid = r.get("mid")
        dte = r.get("dte_days")

        q_str = f"{q:.0f}" if isinstance(q, (int, float)) and q == q else "?"
        mid_str = f"{mid:.2f}" if isinstance(mid, (int, float)) and mid == mid else "?"
        dte_str = str(dte) if isinstance(dte, int) else "?"

        lines.append(f"- {t} {opt_type} {exp} {strike} | Q={q_str} | mid={mid_str} | DTE={dte_str}")

    if len(results) > len(top):
        lines.append("")
        lines.append(f"(Showing top {len(top)} of {len(results)} matches.)")

    msg = "\n".join(lines)
    if len(msg) > 10000:
        msg = msg[:9990] + "\n...truncated..."

    _sns.publish(
        TopicArn=ALERT_SNS_TOPIC_ARN,
        Subject=(ALERT_SUBJECT[:100] if ALERT_SUBJECT else "Options Scan Alert"),
        Message=msg,
    )


# -----------------------
# Tradier API calls (with retries)
# -----------------------
@retry(
    retry=retry_if_exception_type(_RETRY_TYPES),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential_jitter(initial=0.5, max=8),
    reraise=True,
)
async def fetch_expirations(client: httpx.AsyncClient, symbol: str) -> List[str]:
    url = f"{TRADIER_BASE_URL}/v1/markets/options/expirations"
    resp = await client.get(url, params={"symbol": symbol})
    _raise_for(resp, symbol)

    data = resp.json()
    dates = ((data.get("expirations") or {}).get("date")) or []
    if isinstance(dates, str):
        dates = [dates]
    if not isinstance(dates, list):
        return []
    return [d for d in dates if isinstance(d, str) and d.strip()]

@retry(
    retry=retry_if_exception_type(_RETRY_TYPES),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential_jitter(initial=0.5, max=8),
    reraise=True,
)
async def fetch_chain(client: httpx.AsyncClient, symbol: str, expiration: str) -> Dict[str, Any]:
    url = f"{TRADIER_BASE_URL}/v1/markets/options/chains"
    params = {"symbol": symbol, "expiration": expiration, "greeks": "true"}
    resp = await client.get(url, params=params)
    _raise_for(resp, symbol)
    return resp.json()


# -----------------------
# Normalization + Greeks enforcement
# -----------------------
def normalize_contracts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    options = payload.get("options") or {}
    opt = options.get("option") or []
    if isinstance(opt, dict):
        return [opt]
    if isinstance(opt, list):
        return [x for x in opt if isinstance(x, dict)]
    return []

def greeks_present_on_contract(c: Dict[str, Any]) -> bool:
    common_keys = ("delta", "gamma", "theta", "vega", "iv", "implied_volatility")
    g = c.get("greeks")
    if isinstance(g, dict):
        return any(k in g and g.get(k) is not None for k in common_keys)
    return any(k in c and c.get(k) is not None for k in common_keys)

def assert_greeks_present(symbol: str, chain_payload: Dict[str, Any]) -> None:
    contracts = normalize_contracts(chain_payload)
    if not contracts:
        raise TradierError(f"{symbol}: no option contracts returned; cannot validate greeks")
    if sum(1 for c in contracts if greeks_present_on_contract(c)) == 0:
        raise TradierError(f"{symbol}: expected greeks=true response, but no contracts contained greeks")

def get_greeks(c: Dict[str, Any]) -> Dict[str, Any]:
    g = c.get("greeks")
    return g if isinstance(g, dict) else {}

def get_delta(c: Dict[str, Any]) -> float:
    g = get_greeks(c)
    d = safe_float(g.get("delta"))
    if d == d:
        return d
    return safe_float(c.get("delta"))

def get_iv(c: Dict[str, Any]) -> float:
    g = get_greeks(c)
    v = safe_float(g.get("iv"))
    if v == v:
        return v
    return safe_float(g.get("implied_volatility"))


# -----------------------
# Cost calculations
# -----------------------
def premium_per_share_to_use(c: Dict[str, Any]) -> float:
    ask = safe_float(c.get("ask"))
    bid = safe_float(c.get("bid"))
    if ask == ask and ask > 0:
        return ask
    if (bid == bid and ask == ask) and bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return float("nan")

def total_cost_per_contract_usd(c: Dict[str, Any]) -> float:
    prem = premium_per_share_to_use(c)
    if not (prem == prem):
        return float("inf")
    return (prem * CONTRACT_MULTIPLIER) + FEES_PER_CONTRACT


# -----------------------
# Quality score (0-100)
# -----------------------
def spread_pct(c: Dict[str, Any]) -> float:
    bid = safe_float(c.get("bid"))
    ask = safe_float(c.get("ask"))
    if not (bid == bid and ask == ask) or bid <= 0 or ask <= 0 or ask <= bid:
        return float("inf")
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return float("inf")
    return (ask - bid) / mid

def quality_score(c: Dict[str, Any]) -> float:
    vol = safe_float(c.get("volume"))
    oi = safe_float(c.get("open_interest"))
    if not (vol == vol):
        vol = 0.0
    if not (oi == oi):
        oi = 0.0
    liq_raw = math.log10(vol + 1.0) + math.log10(oi + 1.0)
    liq = clamp(liq_raw / 8.0, 0.0, 1.0)

    sp = spread_pct(c)
    if sp == float("inf"):
        spread_score = 0.0
    else:
        if sp <= MAX_SPREAD_PCT_GOOD:
            spread_score = 1.0
        elif sp >= MAX_SPREAD_PCT_BAD:
            spread_score = 0.0
        else:
            spread_score = 1.0 - ((sp - MAX_SPREAD_PCT_GOOD) / (MAX_SPREAD_PCT_BAD - MAX_SPREAD_PCT_GOOD))
            spread_score = clamp(spread_score, 0.0, 1.0)

    d = get_delta(c)
    if not (d == d):
        delta_score = 0.0
    else:
        dist = abs(abs(d) - TARGET_ABS_DELTA)
        delta_score = 1.0 - (dist / max(DELTA_TOLERANCE, 1e-6))
        delta_score = clamp(delta_score, 0.0, 1.0)

    cost = total_cost_per_contract_usd(c)
    if cost == float("inf"):
        cost_score = 0.0
    else:
        denom = max(MAX_COST_PER_CONTRACT, 1.0)
        cost_score = 1.0 - clamp(cost / denom, 0.0, 1.0)

    dte = contract_dte_days(c)
    dte_score = dte_score_from_days(dte)

    q = 100.0 * (
        0.32 * liq +
        0.32 * spread_score +
        0.18 * delta_score +
        0.08 * cost_score +
        0.10 * dte_score
    )
    return clamp(q, 0.0, 100.0)


# -----------------------
# Pricing for order tickets
# -----------------------
def choose_limit_price(bid: float, ask: float) -> float:
    bid_ok = (bid == bid) and bid > 0
    ask_ok = (ask == ask) and ask > 0
    mid = (bid + ask) / 2.0 if (bid_ok and ask_ok) else float("nan")

    if LIMIT_PRICE_MODE == "mid" and (mid == mid) and mid > 0:
        return max(0.01, mid + LIMIT_PRICE_OFFSET)

    if ask_ok:
        return max(0.01, ask + LIMIT_PRICE_OFFSET)

    if (mid == mid) and mid > 0:
        return max(0.01, mid + LIMIT_PRICE_OFFSET)

    return float("nan")


# -----------------------
# Best contract per ticker
# -----------------------
def best_contract_for_ticker(underlying: str, chain_payload: Dict[str, Any]) -> Dict[str, Any]:
    contracts = normalize_contracts(chain_payload)
    affordable = [c for c in contracts if total_cost_per_contract_usd(c) <= MAX_COST_PER_CONTRACT]

    best_q = -1.0
    best_c = None

    for c in affordable:
        q = quality_score(c)
        if q >= MIN_QUALITY_SCORE and q > best_q:
            best_q = q
            best_c = c

    if best_c is None:
        return {"ticker": underlying, "skipped": True, "reason": "no contract met quality/cost filters"}

    bid = safe_float(best_c.get("bid"))
    ask = safe_float(best_c.get("ask"))
    mid = (bid + ask) / 2.0 if (bid == bid and ask == ask and bid > 0 and ask > 0) else float("nan")
    sp = spread_pct(best_c)

    option_symbol = best_c.get("symbol")

    limit_price = choose_limit_price(bid, ask)
    est_total_cost = (
        (limit_price * CONTRACT_MULTIPLIER) + FEES_PER_CONTRACT
        if (limit_price == limit_price)
        else float("nan")
    )

    dte = contract_dte_days(best_c)
    dte_s = dte_score_from_days(dte)

    return {
        "ticker": underlying,
        "underlying_symbol": underlying,
        "option_symbol": option_symbol,

        "type": best_c.get("option_type"),
        "expiration": best_c.get("expiration_date"),
        "strike": best_c.get("strike"),

        "bid": best_c.get("bid"),
        "ask": best_c.get("ask"),
        "mid": mid,
        "spread_pct": sp,
        "volume": best_c.get("volume"),
        "open_interest": best_c.get("open_interest"),

        "delta": get_delta(best_c),
        "iv": get_iv(best_c),

        "dte_days": dte,
        "dte_score": dte_s,
        "min_dte_days": MIN_DTE_DAYS,
        "target_dte_days": TARGET_DTE_DAYS,
        "dte_long_decay_days": DTE_LONG_DECAY_DAYS,

        "quality": best_q,
        "min_quality_score": MIN_QUALITY_SCORE,
        "max_cost_per_contract": MAX_COST_PER_CONTRACT,

        "limit_price": limit_price,
        "estimated_total_cost": est_total_cost,
        "fees_per_contract": FEES_PER_CONTRACT,
        "contract_multiplier": CONTRACT_MULTIPLIER,

        "order_ticket": {
            "class": "option",
            "symbol": underlying,
            "option_symbol": option_symbol,
            "side": ORDER_SIDE,
            "quantity": ORDER_QUANTITY,
            "type": ORDER_TYPE,
            "price": limit_price,
            "duration": ORDER_DURATION,
        },
    }


# -----------------------
# Orchestration
# -----------------------
async def run_scan() -> Dict[str, Any]:
    tickers = parse_tickers(TICKERS_ENV)
    if not tickers:
        raise RuntimeError("No tickers provided. Set TICKERS env var.")
    if not TRADIER_TOKEN:
        raise RuntimeError("Missing TRADIER_TOKEN env var.")

    timeout = httpx.Timeout(REQUEST_TIMEOUT_SECS)
    limits = httpx.Limits(max_connections=MAX_CONCURRENCY, max_keepalive_connections=MAX_CONCURRENCY)
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async with httpx.AsyncClient(headers=auth_headers(TRADIER_TOKEN), timeout=timeout, limits=limits) as client:

        async def one(symbol: str) -> Dict[str, Any]:
            async with sem:
                dates = await fetch_expirations(client, symbol)
                if not dates:
                    return {"ticker": symbol, "error": "no expirations returned"}

                exps = select_expirations(dates, EXPIRATION_OVERRIDE)
                if not exps:
                    return {"ticker": symbol, "error": "no non-0DTE expirations available"}

                best: Dict[str, Any] = {}
                best_q = float("-inf")
                exp_errors: List[Dict[str, str]] = []

                for exp in exps:
                    try:
                        chain = await fetch_chain(client, symbol, exp)
                        assert_greeks_present(symbol, chain)

                        candidate = best_contract_for_ticker(symbol, chain)
                        if not candidate.get("option_symbol"):
                            continue

                        qv = float(candidate.get("quality", float("-inf")))
                        if qv > best_q:
                            best_q = qv
                            best = candidate

                    except Exception as e:
                        exp_errors.append({"expiration": exp, "error": str(e), "error_type": type(e).__name__})
                        continue

                if best.get("option_symbol"):
                    best["expirations_scanned"] = exps
                    best["expiration_window_days"] = EXP_WINDOW_DAYS
                    best["min_expirations_to_scan"] = MIN_EXPIRATIONS_TO_SCAN
                    if exp_errors:
                        best["expiration_scan_errors"] = exp_errors
                    return best

                return {
                    "ticker": symbol,
                    "skipped": True,
                    "reason": "no contract met quality/cost filters across expirations",
                    "expirations_scanned": exps,
                    "expiration_window_days": EXP_WINDOW_DAYS,
                    "min_expirations_to_scan": MIN_EXPIRATIONS_TO_SCAN,
                    "expiration_scan_errors": exp_errors,
                }

        tasks = [asyncio.create_task(one(t)) for t in tickers]
        gathered = await asyncio.gather(*tasks, return_exceptions=True)

        results: List[Dict[str, Any]] = []
        errors: List[Dict[str, Any]] = []

        for t, item in zip(tickers, gathered):
            if isinstance(item, Exception):
                errors.append({"ticker": t, "error": str(item), "error_type": type(item).__name__})
                continue

            if "error" in item:
                errors.append({"ticker": t, "error": item.get("error"), "error_type": item.get("error_type", "TradierError")})
                continue

            if item.get("option_symbol"):
                results.append(item)

        return {"results": results, "errors": errors, "scanned": len(tickers)}


# -----------------------
# Lambda entrypoint (API Gateway proxy response)
# -----------------------
@logger.inject_lambda_context(log_event=True)
@metrics.log_metrics(capture_cold_start_metric=True)
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        scan = asyncio.run(run_scan())

        scanned = int(scan.get("scanned", 0))
        results = scan.get("results", [])
        errors = scan.get("errors", [])

        qualified = len(results)
        failed = len(errors)

        metrics.add_metric("TickersScanned", MetricUnit.Count, scanned)
        metrics.add_metric("TickersQualified", MetricUnit.Count, qualified)
        metrics.add_metric("TickersFailed", MetricUnit.Count, failed)

        payload = {
            "scanned": scanned,
            "qualified_tickers": qualified,
            "failed_tickers": failed,
            "min_quality_score": MIN_QUALITY_SCORE,
            "max_cost_per_contract": MAX_COST_PER_CONTRACT,
            "expiration_window_days": EXP_WINDOW_DAYS,
            "min_expirations_to_scan": MIN_EXPIRATIONS_TO_SCAN,
            "min_dte_days": MIN_DTE_DAYS,
            "target_dte_days": TARGET_DTE_DAYS,
            "dte_long_decay_days": DTE_LONG_DECAY_DAYS,
            "alerts_enabled": ALERT_ENABLED,
            "results": results,
            "errors": errors,
        }

        payload = sanitize(payload)

        # Alert only if there are results (and env var is configured + enabled)
        try:
            alert_if_results(payload)
        except Exception:
            logger.exception("Alert failed (continuing)")

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(payload, allow_nan=False),
        }

    except Exception as e:
        logger.exception("Unhandled error")
        err_payload = sanitize({"error": str(e), "error_type": type(e).__name__})

        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(err_payload, allow_nan=False),
        }


# Optional alias
handler = lambda_handler
