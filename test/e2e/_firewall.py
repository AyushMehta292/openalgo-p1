"""Outbound broker network firewall and deterministic market-data responder for E2E tests.

Installed inside isolated E2E server and ws_proxy subprocesses BEFORE importing
application or broker modules so ZERO real external broker HTTP or WebSocket
traffic can ever escape the test sandbox.

Safety invariants:
1. Loopback hosts (127.0.0.1, localhost, ::1) always pass through untouched.
2. Every intercepted outbound external broker call is appended as JSONL to
   ``outbound_log_path`` with ``{ts, broker_host, method, url, is_order_endpoint, blocked}``.
3. Any request targeting a real-broker order/GTT/position-write path is logged
   with ``is_order_endpoint=True, blocked=True`` and immediately raises
   ``RuntimeError("REAL_BROKER_ORDER_BLOCKED_BY_E2E_FIREWALL")``.
4. Market-data, historical-candle, and read-only funds/positions endpoints are
   answered deterministically from ``mock_quotes_path`` (re-read live on every
   request so tests can mutate quotes dynamically).
"""

from __future__ import annotations

import http.client
import json
import os
import posixpath
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1", ""})

EXTERNAL_BROKER_HOSTS = frozenset(
    {
        "api.india.delta.exchange",
        "api-t1.fyers.in",
        "public.fyers.in",
        "api.groww.in",
        "growwapi-assets.groww.in",
        "public-socket.india.delta.exchange",
        "socket.india.delta.exchange",
        "socket.fyers.in",
        "rtsocket-api.fyers.in",
        "socket-api.groww.in",
    }
)

# Exact path prefixes that unconditionally represent real-broker order/GTT APIs.
_ALWAYS_BLOCKED_ORDER_PREFIXES = (
    "/v2/orders/batch",
    "/v2/orders",
    "/api/v3/orders",
    "/api/v3/multi-order",
    "/api/v3/gtt",
    "/v1/order/create",
    "/v1/order/modify",
    "/v1/order/cancel",
    "/v1/gtt",
)

# Regex patterns for parameterized real-broker order/leverage write endpoints.
_ALWAYS_BLOCKED_ORDER_PATTERNS = (
    re.compile(r"^/v2/products/[^/]+/orders/leverage(?:/.*)?$"),
)

# Position paths that are blocked for write methods (GET is read-only for funds/M2M checks).
_WRITE_BLOCKED_POSITION_PREFIXES = (
    "/v2/positions",
    "/api/v3/positions",
    "/v1/positions",
)

_WRITE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})

DEFAULT_QUOTE_SEEDS: dict[str, dict[str, Any]] = {
    "deltaexchange|CRYPTO|BTCUSDFUT": {
        "broker": "deltaexchange",
        "exchange": "CRYPTO",
        "symbol": "BTCUSDFUT",
        "brsymbol": "BTCUSD",
        "product_id": 27,
        "ltp": 65000.0,
        "bid": 64995.0,
        "ask": 65005.0,
        "open": 64500.0,
        "high": 66000.0,
        "low": 64000.0,
        "prev_close": 64800.0,
        "volume": 1200,
        "oi": 5000,
    },
    "deltaexchange|CRYPTO|BTCINR": {
        "broker": "deltaexchange",
        "exchange": "CRYPTO",
        "symbol": "BTCINR",
        "brsymbol": "BTC_INR",
        "product_id": 99,
        "ltp": 5500000.0,
        "bid": 5499500.0,
        "ask": 5500500.0,
        "open": 5480000.0,
        "high": 5550000.0,
        "low": 5450000.0,
        "prev_close": 5490000.0,
        "volume": 250,
        "oi": 0,
    },
    "fyers|NSE|SBIN": {
        "broker": "fyers",
        "exchange": "NSE",
        "symbol": "SBIN",
        "brsymbol": "NSE:SBIN-EQ",
        "ltp": 800.0,
        "bid": 799.8,
        "ask": 800.2,
        "open": 795.0,
        "high": 815.0,
        "low": 785.0,
        "prev_close": 798.0,
        "volume": 50000,
        "oi": 0,
    },
    "fyers|NFO|NIFTY27MAR25FUT": {
        "broker": "fyers",
        "exchange": "NFO",
        "symbol": "NIFTY27MAR25FUT",
        "brsymbol": "NSE:NIFTY27MAR25FUT",
        "ltp": 24000.0,
        "bid": 23998.0,
        "ask": 24002.0,
        "open": 23950.0,
        "high": 24200.0,
        "low": 23800.0,
        "prev_close": 23980.0,
        "volume": 25000,
        "oi": 120000,
    },
    "fyers|MCX|CRUDEOIL25MARFUT": {
        "broker": "fyers",
        "exchange": "MCX",
        "symbol": "CRUDEOIL25MARFUT",
        "brsymbol": "MCX:CRUDEOIL25MARFUT",
        "ltp": 6200.0,
        "bid": 6199.0,
        "ask": 6201.0,
        "open": 6180.0,
        "high": 6280.0,
        "low": 6120.0,
        "prev_close": 6190.0,
        "volume": 10000,
        "oi": 15000,
    },
    "fyers|NSE_INDEX|NIFTY": {
        "broker": "fyers",
        "exchange": "NSE_INDEX",
        "symbol": "NIFTY",
        "brsymbol": "NSE:NIFTY50-INDEX",
        "ltp": 24000.0,
        "bid": 23999.0,
        "ask": 24001.0,
        "open": 23950.0,
        "high": 24150.0,
        "low": 23850.0,
        "prev_close": 23980.0,
        "volume": 0,
        "oi": 0,
    },
    "groww|NSE|SBIN": {
        "broker": "groww",
        "exchange": "NSE",
        "symbol": "SBIN",
        "brsymbol": "SBIN",
        "ltp": 800.0,
        "bid": 799.8,
        "ask": 800.2,
        "open": 795.0,
        "high": 815.0,
        "low": 785.0,
        "prev_close": 798.0,
        "volume": 50000,
        "oi": 0,
    },
    "groww|NFO|NIFTY27MAR25FUT": {
        "broker": "groww",
        "exchange": "NFO",
        "symbol": "NIFTY27MAR25FUT",
        "brsymbol": "NIFTY25MARFUT",
        "ltp": 24000.0,
        "bid": 23998.0,
        "ask": 24002.0,
        "open": 23950.0,
        "high": 24200.0,
        "low": 23800.0,
        "prev_close": 23980.0,
        "volume": 25000,
        "oi": 120000,
    },
}


def normalize_host(host: str | None) -> str:
    """Normalize a hostname/netloc (lowercase, IPv6 brackets, strip :port & trailing FQDN dot)."""
    if not host:
        return ""
    raw = str(host).strip().lower()
    if not raw:
        return ""
    if raw.startswith("["):
        end = raw.find("]")
        if end != -1:
            raw = raw[1:end]
    elif raw.count(":") == 1:
        raw = raw.split(":", 1)[0]
    return raw.rstrip(".")


def _is_loopback_host(host: str | None) -> bool:
    return normalize_host(host) in LOOPBACK_HOSTS


def normalize_url_path(raw_path: str | None) -> str:
    """Normalize URL path: iterative unquote (up to 3 passes), collapse //, normpath, lowercase."""
    path = str(raw_path or "/")
    for _ in range(3):
        decoded = unquote(path)
        if decoded == path:
            break
        path = decoded
    collapsed = re.sub(r"/+", "/", path)
    norm = posixpath.normpath(collapsed)
    if not norm.startswith("/"):
        norm = "/" + norm
    return norm.lower().rstrip("/") or "/"


def is_external_broker_host(host: str | None) -> bool:
    """Return True if host is a non-loopback external broker host."""
    if not host:
        return False
    clean = normalize_host(host)
    if clean in LOOPBACK_HOSTS:
        return False
    if clean in EXTERNAL_BROKER_HOSTS:
        return True
    # Also catch any broker subdomain or custom DELTA_BASE_URL / DELTA_WS_URL host.
    if clean.endswith(("delta.exchange", "fyers.in", "groww.in")):
        return True
    for env_name in ("DELTA_BASE_URL", "DELTA_WS_URL"):
        env_val = os.getenv(env_name, "").strip()
        if env_val:
            env_parsed = urlparse(env_val)
            env_host = normalize_host(env_parsed.hostname or env_parsed.netloc)
            if env_host and env_host not in LOOPBACK_HOSTS and clean == env_host:
                return True
    return False


def is_real_broker_order_request(method: str, url: str) -> bool:
    """Return True if (method, url) targets a real-broker order/GTT/position-write path."""
    parsed = urlparse(str(url))
    host = normalize_host(parsed.hostname or parsed.netloc)
    if host in LOOPBACK_HOSTS:
        return False
    path = normalize_url_path(parsed.path)
    upper_method = (method or "GET").upper()
    for prefix in _ALWAYS_BLOCKED_ORDER_PREFIXES:
        if path == prefix or path.startswith(prefix + "/"):
            return True
    if path.endswith("/orders/leverage") or any(pat.match(path) for pat in _ALWAYS_BLOCKED_ORDER_PATTERNS):
        return True
    if upper_method in _WRITE_METHODS:
        for prefix in _WRITE_BLOCKED_POSITION_PREFIXES:
            if path == prefix or path.startswith(prefix + "/"):
                return True
    return False


def assert_zero_real_broker_order_calls(outbound_log_path: Path) -> None:
    """Fail if the firewall JSONL recorded ANY real-broker order/GTT/position-write call."""
    path = Path(outbound_log_path)
    if not path.exists():
        return
    violations: list[dict[str, Any]] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if record.get("is_order_endpoint") or is_real_broker_order_request(
            str(record.get("method", "GET")), str(record.get("url", ""))
        ):
            violations.append(record)
    if violations:
        raise AssertionError(
            f"Detected {len(violations)} real-broker order call(s) in {path}: "
            f"{json.dumps(violations[:5])}"
        )


def _normalize_quote(entry: dict[str, Any]) -> dict[str, Any]:
    """Ensure OHLC bounds always enclose ltp so sandbox stale-quote guards pass."""
    ltp = float(entry.get("ltp", 100.0))
    tick = max(0.05, round(ltp * 0.0005, 4))
    bid = float(entry["bid"]) if entry.get("bid") is not None else round(ltp - tick, 4)
    ask = float(entry["ask"]) if entry.get("ask") is not None else round(ltp + tick, 4)
    prev_close = float(entry["prev_close"]) if entry.get("prev_close") is not None else ltp
    open_ = float(entry["open"]) if entry.get("open") is not None else prev_close
    raw_high = float(entry["high"]) if entry.get("high") is not None else max(ltp, open_, prev_close) * 1.02
    raw_low = float(entry["low"]) if entry.get("low") is not None else min(ltp, open_, prev_close) * 0.98
    high = max(raw_high, ltp, open_, ask)
    low = min(raw_low, ltp, open_, bid)
    return {
        "broker": entry.get("broker", ""),
        "exchange": entry.get("exchange", ""),
        "symbol": entry.get("symbol", ""),
        "brsymbol": entry.get("brsymbol", entry.get("symbol", "")),
        "product_id": int(entry.get("product_id", 27)),
        "ltp": ltp,
        "bid": bid,
        "ask": ask,
        "open": open_,
        "high": high,
        "low": low,
        "prev_close": prev_close,
        "volume": int(entry.get("volume", 1000)),
        "oi": int(entry.get("oi", 0)),
    }


def _build_synthetic_candles(
    base_price: float,
    start_ts: int,
    end_ts: int,
    step_seconds: int = 86400,
    max_bars: int = 30,
) -> list[list[Any]]:
    """Build deterministic ascending OHLCV candles inside [start_ts, end_ts]."""
    now_ts = int(time.time())
    effective_end = min(end_ts, now_ts - 60) if end_ts > 0 else now_ts - 60
    if start_ts <= 0 or start_ts > effective_end:
        start_ts = effective_end - step_seconds * 10
    step = max(60, step_seconds)
    span_bars = max(1, min(max_bars, int((effective_end - start_ts) // step) + 1))
    candles: list[list[Any]] = []
    for idx in range(span_bars):
        ts = start_ts + idx * step
        if ts > effective_end:
            break
        o = round(base_price * (0.98 + 0.002 * idx), 2)
        c = round(o * 1.003, 2)
        h = round(max(o, c) * 1.004, 2)
        lo = round(min(o, c) * 0.996, 2)
        vol = 1000 + idx * 50
        candles.append([int(ts), o, h, lo, c, vol])
    if not candles:
        ts = max(0, effective_end - step)
        candles.append([int(ts), base_price, round(base_price * 1.01, 2), round(base_price * 0.99, 2), base_price, 1000])
    return candles


def _parse_date_to_epoch(value: str | None, end_of_day: bool = False) -> int:
    if not value:
        return int(time.time()) - (0 if end_of_day else 86400 * 10)
    raw = str(value).strip()
    if raw.isdigit():
        ts = int(raw)
        return ts // 1000 if ts > 4102444800 else ts
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            if fmt == "%Y-%m-%d" and end_of_day:
                return int(dt.timestamp()) + 86399
            return int(dt.timestamp())
        except ValueError:
            continue
    return int(time.time()) - (0 if end_of_day else 86400 * 10)


class BrokerNetworkFirewall:
    """Process-wide outbound broker interceptor with hard order kill-switch."""

    def __init__(self, mock_quotes_path: Path, outbound_log_path: Path) -> None:
        self.mock_quotes_path = Path(mock_quotes_path)
        self.outbound_log_path = Path(outbound_log_path)
        self._log_lock = threading.Lock()
        self._installed = False
        self._orig_httpx_handle = None
        self._orig_httpx_async_handle = None
        self._orig_broker_transport_handle = None
        self._orig_requests_send = None
        self._orig_urllib3_urlopen = None
        self._orig_http_client_request = None
        self._orig_ws_app = None
        self._orig_ws_create_connection = None

    def _record(
        self,
        *,
        broker_host: str,
        method: str,
        url: str,
        is_order_endpoint: bool,
        blocked: bool,
    ) -> None:
        entry = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "broker_host": broker_host,
            "method": (method or "GET").upper(),
            "url": str(url),
            "is_order_endpoint": bool(is_order_endpoint),
            "blocked": bool(blocked),
        }
        self.outbound_log_path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(entry, separators=(",", ":")) + "\n"
        with self._log_lock:
            with self.outbound_log_path.open("a", encoding="utf-8") as fh:
                fh.write(line)
                fh.flush()

    def _load_quotes(self) -> dict[str, dict[str, Any]]:
        merged = {k: dict(v) for k, v in DEFAULT_QUOTE_SEEDS.items()}
        if self.mock_quotes_path.exists():
            try:
                raw = json.loads(self.mock_quotes_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for key, val in raw.items():
                        if isinstance(val, dict):
                            base = merged.get(key, {})
                            base.update(val)
                            merged[key] = base
            except (OSError, json.JSONDecodeError):
                pass
        return {k: _normalize_quote(v) for k, v in merged.items()}

    def _find_quote(self, broker: str, symbol_hint: str | None) -> dict[str, Any]:
        quotes = self._load_quotes()
        broker_lower = broker.lower()
        hint = unquote(str(symbol_hint or "")).strip()
        hint_upper = hint.upper()
        # Strip Groww EXCHANGE_ prefix or Fyers NSE:... / -EQ suffix for matching.
        stripped = hint_upper
        if ":" in stripped:
            stripped = stripped.split(":", 1)[1]
        if stripped.endswith("-EQ"):
            stripped = stripped[:-3]
        if "_" in stripped and stripped.split("_", 1)[0] in ("NSE", "BSE", "NFO", "BFO"):
            stripped = stripped.split("_", 1)[1]

        candidates = [q for q in quotes.values() if q.get("broker", "").lower() == broker_lower]
        for q in candidates:
            if hint_upper in (
                q.get("symbol", "").upper(),
                q.get("brsymbol", "").upper(),
                str(q.get("product_id", "")),
            ):
                return q
        for q in candidates:
            q_br = q.get("brsymbol", "").upper()
            q_sym = q.get("symbol", "").upper()
            if stripped and (stripped == q_sym or stripped == q_br or q_br.endswith(f":{stripped}-EQ")):
                return q
        if candidates:
            return candidates[0]
        return _normalize_quote({"broker": broker_lower, "symbol": hint or "UNKNOWN", "ltp": 100.0})

    def _mock_json_payload(self, method: str, url: str) -> tuple[int, dict[str, Any]]:
        parsed = urlparse(url)
        host = normalize_host(parsed.hostname or parsed.netloc)
        path = normalize_url_path(parsed.path)
        qs = parse_qs(parsed.query)

        # --- Delta Exchange ---
        delta_env_parsed = urlparse(os.getenv("DELTA_BASE_URL", ""))
        delta_env_host = normalize_host(delta_env_parsed.hostname or delta_env_parsed.netloc)
        if "delta.exchange" in host or (delta_env_host and host == delta_env_host):
            if path.startswith("/v2/tickers/"):
                sym = unquote(path.split("/v2/tickers/", 1)[1]).strip("/")
                q = self._find_quote("deltaexchange", sym)
                return 200, {
                    "success": True,
                    "result": {
                        "symbol": q["brsymbol"],
                        "product_id": q["product_id"],
                        "mark_price": str(q["ltp"]),
                        "open": q["open"],
                        "high": q["high"],
                        "low": q["low"],
                        "close": q["prev_close"],
                        "volume": q["volume"],
                        "oi": str(q["oi"]),
                        "quotes": {
                            "best_bid": str(q["bid"]),
                            "best_ask": str(q["ask"]),
                        },
                    },
                }
            if path == "/v2/tickers":
                quotes = [q for q in self._load_quotes().values() if q["broker"] == "deltaexchange"]
                return 200, {
                    "success": True,
                    "result": [
                        {
                            "symbol": q["brsymbol"],
                            "product_id": q["product_id"],
                            "mark_price": str(q["ltp"]),
                            "open": q["open"],
                            "high": q["high"],
                            "low": q["low"],
                            "close": q["prev_close"],
                            "volume": q["volume"],
                            "oi": str(q["oi"]),
                            "quotes": {"best_bid": str(q["bid"]), "best_ask": str(q["ask"])},
                        }
                        for q in quotes
                    ],
                }
            if path.startswith("/v2/l2orderbook/"):
                pid_or_sym = unquote(path.split("/v2/l2orderbook/", 1)[1]).strip("/")
                q = self._find_quote("deltaexchange", pid_or_sym)
                buy = [
                    {"price": f"{q['bid'] - i * 0.5:.2f}", "size": 100 * (i + 1), "depth": i + 1}
                    for i in range(5)
                ]
                sell = [
                    {"price": f"{q['ask'] + i * 0.5:.2f}", "size": 100 * (i + 1), "depth": i + 1}
                    for i in range(5)
                ]
                return 200, {"success": True, "result": {"buy": buy, "sell": sell}}
            if path == "/v2/history/candles":
                sym = (qs.get("symbol") or ["BTCUSD"])[0]
                res = (qs.get("resolution") or ["1d"])[0]
                q = self._find_quote("deltaexchange", sym)
                step = 300 if res == "5m" else 60 if res == "1m" else 3600 if res == "1h" else 86400
                start_ts = _parse_date_to_epoch((qs.get("start") or [None])[0], end_of_day=False)
                end_ts = _parse_date_to_epoch((qs.get("end") or [None])[0], end_of_day=True)
                return 200, {
                    "success": True,
                    "result": _build_synthetic_candles(q["ltp"], start_ts, end_ts, step_seconds=step),
                }
            if path == "/v2/products":
                return 200, {
                    "success": True,
                    "result": [
                        {
                            "id": 27,
                            "symbol": "BTCUSD",
                            "description": "Bitcoin Perpetual",
                            "contract_type": "perpetual_futures",
                            "tick_size": "0.5",
                            "contract_value": "0.001",
                            "state": "live",
                        },
                        {
                            "id": 99,
                            "symbol": "BTC_INR",
                            "description": "Bitcoin Spot INR",
                            "contract_type": "spot",
                            "tick_size": "0.01",
                            "contract_value": "1",
                            "state": "live",
                        },
                    ],
                }
            if path == "/v2/wallet/balances":
                return 200, {
                    "success": True,
                    "result": [
                        {
                            "asset_symbol": "INR",
                            "balance_inr": "1000000.00",
                            "available_balance": "1000000.00",
                            "blocked_margin": "0.00",
                            "cross_locked_collateral": "0.00",
                        }
                    ],
                }
            if path.startswith("/v2/positions"):
                return 200, {"success": True, "result": []}

        # --- Fyers ---
        if "fyers.in" in host:
            if path == "/data/depth":
                raw_sym = unquote((qs.get("symbol") or ["NSE:SBIN-EQ"])[0])
                q = self._find_quote("fyers", raw_sym)
                bids = [
                    {"price": round(q["bid"] - i * 0.05, 2), "volume": 100 * (i + 1), "ord": i + 1}
                    for i in range(5)
                ]
                asks = [
                    {"price": round(q["ask"] + i * 0.05, 2), "volume": 100 * (i + 1), "ord": i + 1}
                    for i in range(5)
                ]
                return 200, {
                    "s": "ok",
                    "d": {
                        raw_sym: {
                            "ltp": q["ltp"],
                            "o": q["open"],
                            "h": q["high"],
                            "l": q["low"],
                            "c": q["prev_close"],
                            "v": q["volume"],
                            "oi": q["oi"],
                            "ltq": 10,
                            "totalbuyqty": 1500,
                            "totalsellqty": 1500,
                            "bids": bids,
                            "ask": asks,
                        }
                    },
                }
            if path == "/data/quotes":
                raw_syms = unquote((qs.get("symbols") or ["NSE:SBIN-EQ"])[0])
                items = []
                for sym in [s.strip() for s in raw_syms.split(",") if s.strip()]:
                    q = self._find_quote("fyers", sym)
                    items.append(
                        {
                            "s": "ok",
                            "n": sym,
                            "v": {
                                "lp": q["ltp"],
                                "open_price": q["open"],
                                "high_price": q["high"],
                                "low_price": q["low"],
                                "prev_close_price": q["prev_close"],
                                "volume": q["volume"],
                                "bid": q["bid"],
                                "ask": q["ask"],
                            },
                        }
                    )
                return 200, {"s": "ok", "d": items}
            if path == "/data/history":
                raw_sym = unquote((qs.get("symbol") or ["NSE:SBIN-EQ"])[0])
                res = (qs.get("resolution") or ["1D"])[0]
                oi_flag = (qs.get("oi_flag") or ["0"])[0]
                q = self._find_quote("fyers", raw_sym)
                step = 60 if res == "1" else 300 if res == "5" else 3600 if res == "60" else 86400
                start_ts = _parse_date_to_epoch((qs.get("range_from") or [None])[0], end_of_day=False)
                end_ts = _parse_date_to_epoch((qs.get("range_to") or [None])[0], end_of_day=True)
                candles = _build_synthetic_candles(q["ltp"], start_ts, end_ts, step_seconds=step)
                if oi_flag == "1":
                    candles = [c + [q["oi"]] for c in candles]
                return 200, {"s": "ok", "candles": candles}
            if path == "/data/options-chain-v3":
                raw_sym = unquote((qs.get("symbol") or ["NSE:NIFTY50-INDEX"])[0])
                q = self._find_quote("fyers", raw_sym)
                atm = round(q["ltp"] / 100.0) * 100.0
                chain = [
                    {"option_type": "", "strike_price": -1, "ltp": q["ltp"], "ltpch": 0.0}
                ]
                for offset in (-100, 0, 100):
                    strike = atm + offset
                    for opt_type in ("CE", "PE"):
                        chain.append(
                            {
                                "option_type": opt_type,
                                "strike_price": strike,
                                "ltp": 120.0,
                                "ltpch": 2.0,
                                "bid": 119.5,
                                "ask": 120.5,
                                "volume": 5000,
                                "oi": 25000,
                            }
                        )
                return 200, {"s": "ok", "code": 200, "data": {"optionsChain": chain}}
            if path == "/api/v3/funds":
                return 200, {
                    "s": "ok",
                    "code": 200,
                    "fund_limit": [
                        {"id": 1, "title": "Clear Balance", "equityAmount": 1000000.0, "commodityAmount": 0.0},
                        {"id": 2, "title": "Utilized Amount", "equityAmount": 0.0, "commodityAmount": 0.0},
                        {"id": 3, "title": "Collaterals", "equityAmount": 0.0, "commodityAmount": 0.0},
                        {"id": 4, "title": "Realized Profit and Loss", "equityAmount": 0.0, "commodityAmount": 0.0},
                    ],
                }
            if path == "/api/v3/positions":
                return 200, {"s": "ok", "code": 200, "netPositions": []}
            if path == "/data/symbol-token":
                return 200, {"s": "ok", "validSymbol": {}}

        # --- Groww ---
        if "groww.in" in host:
            if path == "/v1/live-data/quote":
                sym = unquote((qs.get("trading_symbol") or ["SBIN"])[0])
                q = self._find_quote("groww", sym)
                buy = [{"price": round(q["bid"] - i * 0.05, 2), "quantity": 100 * (i + 1)} for i in range(5)]
                sell = [{"price": round(q["ask"] + i * 0.05, 2), "quantity": 100 * (i + 1)} for i in range(5)]
                return 200, {
                    "status": "SUCCESS",
                    "payload": {
                        "last_price": q["ltp"],
                        "ohlc": {
                            "open": q["open"],
                            "high": q["high"],
                            "low": q["low"],
                            "close": q["prev_close"],
                        },
                        "volume": q["volume"],
                        "open_interest": q["oi"],
                        "bid_price": q["bid"],
                        "offer_price": q["ask"],
                        "bid_quantity": 100,
                        "offer_quantity": 100,
                        "total_buy_quantity": 1500,
                        "total_sell_quantity": 1500,
                        "day_change": round(q["ltp"] - q["prev_close"], 2),
                        "day_change_perc": 0.0,
                        "depth": {"buy": buy, "sell": sell},
                    },
                }
            if path == "/v1/live-data/ltp":
                raw_syms = unquote((qs.get("exchange_symbols") or ["NSE_SBIN"])[0])
                payload = {}
                for es in [s.strip() for s in raw_syms.split(",") if s.strip()]:
                    q = self._find_quote("groww", es)
                    payload[es] = q["ltp"]
                return 200, {"status": "SUCCESS", "payload": payload}
            if path == "/v1/live-data/ohlc":
                raw_syms = unquote((qs.get("exchange_symbols") or ["NSE_SBIN"])[0])
                payload = {}
                for es in [s.strip() for s in raw_syms.split(",") if s.strip()]:
                    q = self._find_quote("groww", es)
                    payload[es] = {
                        "open": q["open"],
                        "high": q["high"],
                        "low": q["low"],
                        "close": q["prev_close"],
                    }
                return 200, {"status": "SUCCESS", "payload": payload}
            if path == "/v1/historical/candle/range":
                sym = unquote((qs.get("trading_symbol") or ["SBIN"])[0])
                q = self._find_quote("groww", sym)
                start_ts = _parse_date_to_epoch((qs.get("start_time") or [None])[0], end_of_day=False)
                end_ts = _parse_date_to_epoch((qs.get("end_time") or [None])[0], end_of_day=True)
                candles = _build_synthetic_candles(q["ltp"], start_ts, end_ts, step_seconds=86400)
                return 200, {"status": "SUCCESS", "payload": {"candles": candles}}
            if path == "/v1/historical/candles":
                sym = unquote((qs.get("groww_symbol") or ["NSE-SBIN"])[0])
                q = self._find_quote("groww", sym.split("-", 1)[-1])
                start_ts = _parse_date_to_epoch((qs.get("start_time") or [None])[0], end_of_day=False)
                end_ts = _parse_date_to_epoch((qs.get("end_time") or [None])[0], end_of_day=True)
                raw_candles = _build_synthetic_candles(q["ltp"], start_ts, end_ts, step_seconds=300, max_bars=12)
                # Groww intraday candles carry ISO timestamp strings in IST regular session (09:15+).
                base_day = datetime.fromtimestamp(raw_candles[0][0], tz=timezone.utc).strftime("%Y-%m-%d")
                formatted = []
                for idx, c in enumerate(raw_candles):
                    minute_total = 9 * 60 + 15 + idx * 5
                    hh, mm = divmod(minute_total, 60)
                    formatted.append([f"{base_day} {hh:02d}:{mm:02d}:00", c[1], c[2], c[3], c[4], c[5]])
                return 200, {"status": "SUCCESS", "payload": {"candles": formatted}}
            if path.startswith("/v1/option-chain/exchange/"):
                return 200, {"status": "SUCCESS", "payload": {"strikes": {}}}
            if path == "/v1/margins/detail/user":
                return 200, {
                    "status": "SUCCESS",
                    "payload": {
                        "clear_cash": 1000000.0,
                        "collateral_available": 0.0,
                        "net_margin_used": 0.0,
                    },
                }
            if path == "/v1/positions/user":
                return 200, {"status": "SUCCESS", "payload": {"positions": []}}
            if path.startswith("/v1/api/apex/v1/socket/token/create"):
                return 200, {"token": "mock-groww-ws-token", "subscriptionId": "mock-sub-1"}

        # Any other external broker path is blocked with deterministic 599 JSON.
        return 599, {"status": "error", "message": f"Blocked unmocked external broker request: {method} {url}"}

    def _intercept_http(self, method: str, url: str) -> tuple[int, dict[str, Any]] | None:
        parsed = urlparse(str(url))
        host = normalize_host(parsed.hostname or parsed.netloc)
        if not is_external_broker_host(host):
            return None

        order_hit = is_real_broker_order_request(method, str(url))
        if order_hit:
            self._record(
                broker_host=host,
                method=method,
                url=str(url),
                is_order_endpoint=True,
                blocked=True,
            )
            raise RuntimeError("REAL_BROKER_ORDER_BLOCKED_BY_E2E_FIREWALL")

        status_code, payload = self._mock_json_payload(method, str(url))
        self._record(
            broker_host=host,
            method=method,
            url=str(url),
            is_order_endpoint=False,
            blocked=(status_code == 599),
        )
        return status_code, payload

    def install(self) -> "BrokerNetworkFirewall":
        """Install HTTP (httpx + requests + urllib3 + http.client) and WebSocket interception hooks."""
        if self._installed:
            return self
        firewall = self

        # 1. Intercept httpx.HTTPTransport.handle_request (also covers _BrokerTransport).
        import httpx

        orig_httpx_handle = httpx.HTTPTransport.handle_request
        self._orig_httpx_handle = orig_httpx_handle

        def _guarded_httpx_handle(transport_self, request: httpx.Request) -> httpx.Response:
            result = firewall._intercept_http(request.method, str(request.url))
            if result is not None:
                status_code, payload = result
                return httpx.Response(
                    status_code=status_code,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    request=request,
                )
            return orig_httpx_handle(transport_self, request)

        httpx.HTTPTransport.handle_request = _guarded_httpx_handle

        # Also intercept httpx.AsyncHTTPTransport.handle_async_request.
        orig_httpx_async_handle = httpx.AsyncHTTPTransport.handle_async_request
        self._orig_httpx_async_handle = orig_httpx_async_handle

        async def _guarded_httpx_async_handle(transport_self, request: httpx.Request) -> httpx.Response:
            result = firewall._intercept_http(request.method, str(request.url))
            if result is not None:
                status_code, payload = result
                return httpx.Response(
                    status_code=status_code,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                    request=request,
                )
            return await orig_httpx_async_handle(transport_self, request)

        httpx.AsyncHTTPTransport.handle_async_request = _guarded_httpx_async_handle

        # Also patch utils.httpx_client._BrokerTransport.handle_request directly if loaded or loadable.
        try:
            import utils.httpx_client as _hc

            orig_broker_handle = _hc._BrokerTransport.handle_request
            self._orig_broker_transport_handle = orig_broker_handle

            def _guarded_broker_handle(transport_self, request: httpx.Request) -> httpx.Response:
                result = firewall._intercept_http(request.method, str(request.url))
                if result is not None:
                    status_code, payload = result
                    return httpx.Response(
                        status_code=status_code,
                        json=payload,
                        headers={"Content-Type": "application/json"},
                        request=request,
                    )
                return orig_broker_handle(transport_self, request)

            _hc._BrokerTransport.handle_request = _guarded_broker_handle
        except Exception:
            pass

        # 2. Intercept requests.Session.send (covers requests.get/post/request,
        #    Session.request, Session.send(PreparedRequest), and Session.resolve_redirects).
        try:
            import requests

            orig_send = requests.Session.send
            self._orig_requests_send = orig_send

            def _guarded_requests_send(session_self, request, *args, **kwargs):
                method = str(getattr(request, "method", None) or "GET")
                url = str(getattr(request, "url", None) or "")
                result = firewall._intercept_http(method, url)
                if result is not None:
                    status_code, payload = result
                    resp = requests.Response()
                    resp.status_code = status_code
                    resp.reason = "OK" if 200 <= status_code < 300 else "Blocked"
                    resp.url = url
                    resp.request = request
                    resp.encoding = "utf-8"
                    resp.headers["Content-Type"] = "application/json"
                    resp._content = json.dumps(payload).encode("utf-8")
                    resp._content_consumed = True
                    return resp
                return orig_send(session_self, request, *args, **kwargs)

            requests.Session.send = _guarded_requests_send
        except ImportError:
            pass

        # 3. Intercept urllib3.connectionpool.HTTPConnectionPool.urlopen (covers
        #    HTTPConnectionPool, HTTPSConnectionPool, and urllib3.PoolManager.request).
        try:
            import urllib3.connectionpool as _urllib3_cp

            orig_urllib3_urlopen = _urllib3_cp.HTTPConnectionPool.urlopen
            self._orig_urllib3_urlopen = orig_urllib3_urlopen

            def _guarded_urllib3_urlopen(pool_self, method, url, *args, **kwargs):
                url_str = str(url or "/")
                if url_str.lower().startswith(("http://", "https://")):
                    full_url = url_str
                else:
                    scheme = str(getattr(pool_self, "scheme", "http") or "http").lower()
                    raw_host = str(getattr(pool_self, "host", "") or "").strip()
                    if ":" in raw_host and not raw_host.startswith("["):
                        host_part = f"[{raw_host}]"
                    else:
                        host_part = raw_host
                    path_part = url_str if url_str.startswith("/") else "/" + url_str
                    port = getattr(pool_self, "port", None)
                    default_port = 443 if scheme == "https" else 80
                    if port and port != default_port and ":" not in host_part.split("]")[-1]:
                        full_url = f"{scheme}://{host_part}:{port}{path_part}"
                    else:
                        full_url = f"{scheme}://{host_part}{path_part}"
                parsed_u3 = urlparse(full_url)
                host = normalize_host(parsed_u3.hostname or parsed_u3.netloc)
                if is_external_broker_host(host):
                    upper_method = str(method or "GET").upper()
                    if is_real_broker_order_request(upper_method, full_url):
                        firewall._record(
                            broker_host=host,
                            method=upper_method,
                            url=full_url,
                            is_order_endpoint=True,
                            blocked=True,
                        )
                        raise RuntimeError(
                            f"REAL_BROKER_ORDER_BLOCKED_BY_E2E_FIREWALL: {upper_method} {full_url}"
                        )
                    firewall._record(
                        broker_host=host,
                        method=upper_method,
                        url=full_url,
                        is_order_endpoint=False,
                        blocked=True,
                    )
                    raise RuntimeError(
                        f"EXTERNAL_BROKER_HTTP_BLOCKED_BY_E2E_FIREWALL: {upper_method} {full_url}"
                    )
                return orig_urllib3_urlopen(pool_self, method, url, *args, **kwargs)

            _urllib3_cp.HTTPConnectionPool.urlopen = _guarded_urllib3_urlopen
        except ImportError:
            pass

        # 4. Intercept http.client.HTTPConnection.request (covers HTTPSConnection & urllib.request.urlopen).
        orig_http_req = http.client.HTTPConnection.request
        self._orig_http_client_request = orig_http_req

        def _guarded_http_client_request(conn_self, method, url, *args, **kwargs):
            scheme = "https" if isinstance(conn_self, http.client.HTTPSConnection) else "http"
            url_str = str(url or "/")
            if url_str.lower().startswith(("http://", "https://")):
                full_url = url_str
            else:
                path_part = url_str if url_str.startswith("/") else "/" + url_str
                raw_host = str(getattr(conn_self, "host", "") or "").strip()
                if ":" in raw_host and not raw_host.startswith("["):
                    host_part = f"[{raw_host}]"
                else:
                    host_part = raw_host
                port = getattr(conn_self, "port", None)
                default_port = 443 if scheme == "https" else 80
                if port and port != default_port and ":" not in host_part.split("]")[-1]:
                    full_url = f"{scheme}://{host_part}:{port}{path_part}"
                else:
                    full_url = f"{scheme}://{host_part}{path_part}"
            parsed_hc = urlparse(full_url)
            host = normalize_host(parsed_hc.hostname or parsed_hc.netloc)
            if is_external_broker_host(host):
                if is_real_broker_order_request(str(method), full_url):
                    firewall._record(
                        broker_host=host,
                        method=str(method),
                        url=full_url,
                        is_order_endpoint=True,
                        blocked=True,
                    )
                    raise RuntimeError("REAL_BROKER_ORDER_BLOCKED_BY_E2E_FIREWALL")
                firewall._record(
                    broker_host=host,
                    method=str(method),
                    url=full_url,
                    is_order_endpoint=False,
                    blocked=True,
                )
                raise RuntimeError(f"EXTERNAL_BROKER_HTTP_BLOCKED_BY_E2E_FIREWALL: {method} {full_url}")
            return orig_http_req(conn_self, method, url, *args, **kwargs)

        http.client.HTTPConnection.request = _guarded_http_client_request

        # 5. Intercept websocket.WebSocketApp and websocket.create_connection for external WS hosts.
        try:
            import websocket as ws_mod

            orig_ws_app = ws_mod.WebSocketApp
            self._orig_ws_app = orig_ws_app

            class _GuardedWebSocketApp:
                def __new__(cls, url, *args, **kwargs):
                    parsed_ws = urlparse(str(url))
                    host = normalize_host(parsed_ws.hostname or parsed_ws.netloc)
                    scheme = (parsed_ws.scheme or "").lower()
                    is_external_ws = scheme in ("ws", "wss") and not _is_loopback_host(host)
                    if not (is_external_broker_host(host) or is_external_ws):
                        return orig_ws_app(url, *args, **kwargs)
                    firewall._record(
                        broker_host=host,
                        method="WS",
                        url=str(url),
                        is_order_endpoint=False,
                        blocked=True,
                    )
                    return _BlockedExternalWebSocketApp(str(url), *args, **kwargs)

            ws_mod.WebSocketApp = _GuardedWebSocketApp

            orig_ws_create_conn = getattr(ws_mod, "create_connection", None)
            self._orig_ws_create_connection = orig_ws_create_conn
            if orig_ws_create_conn is not None:

                def _guarded_ws_create_connection(url, *args, **kwargs):
                    parsed_ws = urlparse(str(url))
                    host = normalize_host(parsed_ws.hostname or parsed_ws.netloc)
                    scheme = (parsed_ws.scheme or "").lower()
                    is_external_ws = scheme in ("ws", "wss") and not _is_loopback_host(host)
                    if is_external_broker_host(host) or is_external_ws:
                        firewall._record(
                            broker_host=host,
                            method="WS",
                            url=str(url),
                            is_order_endpoint=False,
                            blocked=True,
                        )
                        raise RuntimeError("EXTERNAL_BROKER_WS_BLOCKED_BY_E2E_FIREWALL")
                    return orig_ws_create_conn(url, *args, **kwargs)

                ws_mod.create_connection = _guarded_ws_create_connection
        except ImportError:
            pass

        self._installed = True
        return self

    def uninstall(self) -> None:
        """Restore all monkeypatched HTTP and WebSocket transports."""
        if not self._installed:
            return
        try:
            import httpx

            if self._orig_httpx_handle is not None:
                httpx.HTTPTransport.handle_request = self._orig_httpx_handle
                self._orig_httpx_handle = None
            if self._orig_httpx_async_handle is not None:
                httpx.AsyncHTTPTransport.handle_async_request = self._orig_httpx_async_handle
                self._orig_httpx_async_handle = None
        except Exception:
            pass
        try:
            import utils.httpx_client as _hc

            if self._orig_broker_transport_handle is not None:
                _hc._BrokerTransport.handle_request = self._orig_broker_transport_handle
                self._orig_broker_transport_handle = None
        except Exception:
            pass
        try:
            import requests

            if self._orig_requests_send is not None:
                requests.Session.send = self._orig_requests_send
                self._orig_requests_send = None
        except Exception:
            pass
        try:
            import urllib3.connectionpool as _urllib3_cp

            if self._orig_urllib3_urlopen is not None:
                _urllib3_cp.HTTPConnectionPool.urlopen = self._orig_urllib3_urlopen
                self._orig_urllib3_urlopen = None
        except Exception:
            pass
        if self._orig_http_client_request is not None:
            http.client.HTTPConnection.request = self._orig_http_client_request
            self._orig_http_client_request = None
        try:
            import websocket as ws_mod

            if self._orig_ws_app is not None:
                ws_mod.WebSocketApp = self._orig_ws_app
                self._orig_ws_app = None
            if self._orig_ws_create_connection is not None:
                ws_mod.create_connection = self._orig_ws_create_connection
                self._orig_ws_create_connection = None
        except Exception:
            pass
        self._installed = False


class _BlockedExternalWebSocketApp:
    """Harmless stub replacing external broker WebSocketApp connections in E2E."""

    def __init__(self, url: str, *args, **kwargs) -> None:
        self.url = url
        self.on_open = kwargs.get("on_open")
        self.on_message = kwargs.get("on_message")
        self.on_error = kwargs.get("on_error")
        self.on_close = kwargs.get("on_close")
        self.keep_running = False
        self._stop_event = threading.Event()
        self.sock = None

    def run_forever(self, *args, **kwargs) -> None:
        self.keep_running = True
        if callable(self.on_open):
            try:
                self.on_open(self)
            except Exception:
                pass
        self._stop_event.wait(timeout=3600)
        self.keep_running = False
        if callable(self.on_close):
            try:
                self.on_close(self, 1000, "firewall-closed")
            except Exception:
                pass

    def send(self, data: Any, *args, **kwargs) -> None:
        return None

    def close(self, *args, **kwargs) -> None:
        self.keep_running = False
        self._stop_event.set()
