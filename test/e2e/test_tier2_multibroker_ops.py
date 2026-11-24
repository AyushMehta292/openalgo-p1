"""Tier 2 E2E Boundary, Corner, Negative & Concurrency Tests: Multi-Broker Ops,
Streaming, Sandbox, Live Strategies, Stability & Production Monitoring
(E2E-F6 through E2E-F11).

Strict opaque-box discipline:
- Interacts ONLY through public HTTP REST/session routes, WebSocket wire frames,
  CLI subprocesses, loopback mock HTTP servers, and observable filesystem/SQLite
  artifacts.
- Uses ONLY public fixtures and constants from ``test/e2e/conftest.py``.
- Never skips, xfails, imports private app helpers, or weakens contract assertions.
"""

from __future__ import annotations

import ast
import concurrent.futures
import json
import os
import socket
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import httpx
import pytest
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect as ws_connect

from .conftest import (
    BROKERS,
    DEFAULT_SYMBOLS,
    REPO_ROOT,
    OpenAlgoE2EInstance,
    WsProxyE2EInstance,
)

# ---------------------------------------------------------------------------
# Compiled OpenScript strategy program (canonical SHA-256 hash matches engine)
# ---------------------------------------------------------------------------
OPENSCRIPT_FIRST_BAR_SOURCE = (
    'version 1\n'
    'strategy("Opening", overlay = true, qty = 1)\n'
    'if session.isFirstBar\n'
    '    buy()\n'
)
OPENSCRIPT_FIRST_BAR_PROGRAM = (
    '{"callSites":[],"cells":[],"channels":[],"code":[["CALL_LIB",0,0,-1],["JUMP_F'
    'ALSE",10],["CONST",0],["CONST",0],["CONST",0],["CONST",3],["CONST",0],["CONS'
    'T",3],["CALL_LIB",1,6,-1],["POP"],["HALT"]],"compiler":{"name":"openscript"'
    ',"version":"0.5.0"},"consts":[["z",null],["b",false],["b",true],["s",""]'
    '],"debug":{"fnPos":[],"names":{"cells":[],"channels":[],"series":[],"slots"'
    ':[]},"pos":[[0,3,4],[1,3,1],[2,4,5]],"retain":false},"frame":{"slots":0},"funct'
    'ions":[],"inputs":[],"lib":{"functions":[{"arity":0,"effect":"none","name"'
    ':"session.isFirstBar","state":false},{"arity":6,"effect":"order","name":"bu'
    'y","state":false}],"manifest":1},"limits":{"history":null,"loops":2000000},"'
    'loops":[],"meta":{"format":"price","group":"","kind":"strategy","onUncon'
    'firmed":false,"overlay":true,"precision":4,"range":null,"scale":"right","sho'
    'rt":"Opening","strategy":{"capital":100000,"closeOnSessionEnd":false,"commissi'
    'on":0,"commissionType":"perTrade","currency":"","fillOn":"nextOpen","produ'
    'ct":"intraday","pyramiding":1,"qty":1,"qtyType":"units","slippage":0},"tit'
    'le":"Opening"},"openscript":{"format":"1.1","language":1},"outputs":{"aler'
    'ts":[],"background":null,"barColor":null,"fills":[],"levels":[],"markers":[],'
    '"plots":[],"tables":[]},"requests":[],"requires":["core.1","orders"],"serie'
    's":[],"source":{"file":"firstbar.oscript","hash":"sha256:f2498b08a681f288050b2'
    'f2ac0c4ed6ccadce06f73e472f0c936a441eaf15f5e","lines":5},"states":[]}'
)

REQUIRED_MONITOR_TOP_KEYS = (
    "timestamp_utc",
    "check_number",
    "overall_status",
    "https",
    "services",
    "resources",
    "brokers",
    "strategies",
    "errors_last_3h",
    "alerts",
)


# ---------------------------------------------------------------------------
# Pure Opaque-Box Helper Utilities
# ---------------------------------------------------------------------------


def _alloc_unused_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _place_paper_order(
    server: OpenAlgoE2EInstance,
    broker: str,
    *,
    symbol: str | None = None,
    exchange: str | None = None,
    product: str | None = None,
    action: str = "BUY",
    quantity: int | float | str = 1,
    pricetype: str = "MARKET",
    price: float = 0.0,
    trigger_price: float = 0.0,
    strategy: str = "e2e_t2_strat",
) -> httpx.Response:
    spec = DEFAULT_SYMBOLS[broker]
    payload = {
        "apikey": server.api_key,
        "strategy": strategy,
        "exchange": exchange if exchange is not None else spec["exchange"],
        "symbol": symbol if symbol is not None else spec["symbol"],
        "action": action,
        "quantity": quantity,
        "pricetype": pricetype,
        "product": product if product is not None else spec["product"],
        "price": price,
        "trigger_price": trigger_price,
    }
    return server.api_post("/api/v1/placeorder", payload, broker=broker)


def _order_status(server: OpenAlgoE2EInstance, broker: str, orderid: str) -> str:
    resp = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker=broker)
    assert resp.status_code == 200, f"orderbook failed for {broker}: {resp.text}"
    orders = resp.json()["data"]["orders"]
    row = next((o for o in orders if str(o.get("orderid")) == str(orderid)), None)
    assert row is not None, f"Order {orderid} not found in {broker} orderbook: {orders}"
    return str(row["order_status"]).lower()


def _wait_for_order_status(
    server: OpenAlgoE2EInstance,
    broker: str,
    orderid: str,
    expected: str,
    timeout: float = 15.0,
) -> str:
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        last = _order_status(server, broker, orderid)
        if last == expected.lower():
            return last
        time.sleep(0.35)
    return last


def _sqlite_count(db_path: Path, table: str) -> int:
    if not db_path.exists():
        return 0
    with sqlite3.connect(str(db_path)) as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        if cur.fetchone() is None:
            return 0
        return int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _ws_recv_json(ws: Any, timeout: float = 5.0) -> dict[str, Any]:
    raw = ws.recv(timeout=timeout)
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    return json.loads(raw)


def _ws_recv_matching(
    ws: Any,
    predicate: Callable[[dict[str, Any]], bool],
    timeout: float = 6.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    seen: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        remaining = max(0.1, deadline - time.monotonic())
        try:
            msg = _ws_recv_json(ws, timeout=min(1.0, remaining))
        except TimeoutError:
            continue
        seen.append(msg)
        if predicate(msg):
            return msg
    raise AssertionError(f"Did not receive matching WS frame within {timeout}s; seen={seen}")


def _ws_authenticate(ws: Any, api_key: str, broker: str | None = None) -> dict[str, Any]:
    frame: dict[str, Any] = {"action": "authenticate", "api_key": api_key}
    if broker is not None:
        frame["broker"] = broker
    ws.send(json.dumps(frame))
    return _ws_recv_matching(
        ws,
        lambda m: m.get("type") == "auth" or m.get("status") == "error",
        timeout=6.0,
    )


def _live_strategies_dir() -> Path:
    return REPO_ROOT / "strategies" / "live"


def _discover_live_strategy_entry(broker: str | None = None) -> Path:
    live_dir = _live_strategies_dir()
    assert live_dir.is_dir(), (
        f"Required tracked live-bar strategy directory missing: {live_dir} "
        "(PROJECT.md Feature F15 / Interface Contract #5)."
    )
    py_files = sorted(p for p in live_dir.rglob("*.py") if p.name != "__init__.py")
    assert py_files, f"No tracked Python strategy files found under {live_dir}"

    if broker:
        tokens = {
            "deltaexchange": ("delta",),
            "fyers": ("fyers",),
            "groww": ("groww",),
        }.get(broker.lower(), (broker.lower(),))
        for candidate in py_files:
            stem = candidate.stem.lower()
            if any(tok in stem for tok in tokens):
                return candidate

    for preferred in ("runner", "cli", "main", "ema", "strategy", "engine"):
        for candidate in py_files:
            if preferred in candidate.stem.lower():
                return candidate
    return py_files[0]


def _probe_cli_help(script_path: Path, env: dict[str, str] | None = None) -> str:
    proc = subprocess.run(
        [sys.executable, str(script_path), "--help"],
        cwd=str(REPO_ROOT),
        env=env or os.environ.copy(),
        capture_output=True,
        text=True,
        timeout=15.0,
    )
    return (proc.stdout or "") + "\n" + (proc.stderr or "")


def _build_live_strategy_cmd(
    script_path: Path,
    *,
    broker: str,
    symbol: str,
    exchange: str,
    state_file: Path,
    cycles: int = 2,
    poll_seconds: float = 0.1,
    help_text: str | None = None,
) -> list[str]:
    help_out = help_text if help_text is not None else _probe_cli_help(script_path)
    cmd = [sys.executable, str(script_path)]

    def _add_opt(names: tuple[str, ...], value: str) -> None:
        for flag in names:
            if flag in help_out:
                cmd.extend([flag, value])
                return

    _add_opt(("--broker",), broker)
    _add_opt(("--symbol",), symbol)
    _add_opt(("--exchange",), exchange)
    _add_opt(("--state-file", "--state-path", "--state"), str(state_file))
    _add_opt(("--poll-seconds", "--interval-seconds", "--poll"), str(poll_seconds))
    if "--cycles" in help_out:
        cmd.extend(["--cycles", str(cycles)])
    elif "--max-cycles" in help_out:
        cmd.extend(["--max-cycles", str(cycles)])
    elif "--iterations" in help_out:
        cmd.extend(["--iterations", str(cycles)])
    elif "--once" in help_out:
        cmd.append("--once")
    return cmd


def _run_monitor_once(
    *,
    base_url: str,
    root_dir: Path,
    output_jsonl: Path,
    api_key: str,
    ws_port: int | None = None,
    extra_env: dict[str, str] | None = None,
) -> tuple[subprocess.CompletedProcess[str], list[dict[str, Any]]]:
    script = REPO_ROOT / "scripts" / "monitor_production.py"
    assert script.is_file(), (
        f"Required production monitoring script missing: {script} "
        "(PROJECT.md Feature F20 / Interface Contract #6)."
    )
    help_out = _probe_cli_help(script)

    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(REPO_ROOT),
            "OPENALGO_BASE_URL": base_url,
            "OPENALGO_HOST": base_url,
            "OPENALGO_ROOT": str(root_dir),
            "OPENALGO_APP_DIR": str(root_dir),
            "OPENALGO_API_KEY": api_key,
            "LOG_DIR": str(root_dir / "log"),
            "MONITOR_OUTPUT_PATH": str(output_jsonl),
            "MONITOR_JSONL_PATH": str(output_jsonl),
            "MONITOR_SKIP_SYSTEMD": "true",
            "MONITOR_LOCAL_MODE": "true",
        }
    )
    if ws_port is not None:
        env["WEBSOCKET_PORT"] = str(ws_port)
        env["WS_PORT"] = str(ws_port)
        env["WEBSOCKET_URL"] = f"ws://127.0.0.1:{ws_port}"
    if extra_env:
        env.update(extra_env)

    cmd = [sys.executable, str(script)]

    def _add_opt(names: tuple[str, ...], value: str) -> None:
        for flag in names:
            if flag in help_out:
                cmd.extend([flag, value])
                return

    _add_opt(("--base-url", "--url", "--host"), base_url)
    _add_opt(("--output", "--output-file", "--jsonl", "--log-file"), str(output_jsonl))
    _add_opt(("--app-dir", "--root", "--root-dir", "--data-dir"), str(root_dir))
    _add_opt(("--api-key",), api_key)
    if ws_port is not None:
        _add_opt(("--ws-port",), str(ws_port))
        _add_opt(("--ws-url",), f"ws://127.0.0.1:{ws_port}")
    for flag in ("--once", "--no-github", "--local"):
        if flag in help_out:
            cmd.append(flag)

    proc = subprocess.run(
        cmd,
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=30.0,
    )
    records: list[dict[str, Any]] = []
    if output_jsonl.exists():
        for line in output_jsonl.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return proc, records


def _start_loopback_http_server(
    handler_cls: type[BaseHTTPRequestHandler],
) -> tuple[ThreadingHTTPServer, threading.Thread, int]:
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    port = int(httpd.server_address[1])
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread, port


def _append_padding_jsonl(path: Path, size_mb: float, sentinel: str = "") -> None:
    target_bytes = int(size_mb * 1024 * 1024)
    sample_line = (
        json.dumps(
            {
                "ts": "2026-11-24 00:00:00",
                "level": "ERROR",
                "logger": "e2e.padding",
                "message": "pad " + ("p" * 240),
            }
        )
        + "\n"
    ).encode("utf-8")
    chunk = sample_line * 256
    written = 0
    with path.open("ab") as fh:
        while written < target_bytes:
            remaining = target_bytes - written
            if remaining >= len(chunk):
                fh.write(chunk)
                written += len(chunk)
            else:
                lines_needed = max(1, remaining // len(sample_line))
                buf = sample_line * lines_needed
                fh.write(buf)
                written += len(buf)
                break
        if sentinel:
            fh.write(
                (
                    json.dumps(
                        {
                            "ts": "2026-11-24 12:00:00",
                            "level": "ERROR",
                            "logger": "e2e.sentinel",
                            "message": sentinel,
                        }
                    )
                    + "\n"
                ).encode("utf-8")
            )
        fh.flush()


# ===========================================================================
# E2E-F6: Multi-Broker Isolation & Routing — Boundary/Negative (6 Tier 2 tests)
# ===========================================================================


@pytest.mark.tier2
def test_t2_f6_broker_precedence_conflicts_path_query_header_shared_fallback(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Strict precedence holds under conflicts: /<broker>/... > ?broker= > X-OpenAlgo-Broker > shared fallback."""
    # 1. URL path /<broker>/auth overrides conflicting ?broker= and X-OpenAlgo-Broker
    with cloud_server.session_client("fyers") as cli:
        r_valid_path = cli.get(
            "/deltaexchange/auth",
            params={"broker": "groww"},
            headers={"X-OpenAlgo-Broker": "fyers", "Accept": "text/html"},
        )
        assert r_valid_path.status_code == 200, r_valid_path.text[:300]

        # Unknown URL path broker is rejected (404) even when ?broker= and header name valid brokers
        r_bad_path = cli.get(
            "/zerodha/auth",
            params={"broker": "fyers"},
            headers={"X-OpenAlgo-Broker": "deltaexchange", "Accept": "text/html"},
        )
        assert r_bad_path.status_code == 404, f"Unknown path broker must 404, got {r_bad_path.status_code}"

    # 2. Query ?broker= overrides conflicting X-OpenAlgo-Broker across all conflicting pairs
    for query_broker, header_broker in [
        ("groww", "fyers"),
        ("deltaexchange", "groww"),
        ("fyers", "deltaexchange"),
    ]:
        resp = cloud_server.api_post(
            "/api/v1/ping",
            {"apikey": cloud_server.api_key},
            broker=header_broker,
            query={"broker": query_broker},
        )
        assert resp.status_code == 200, resp.text
        assert str(resp.json().get("data", {}).get("broker", "")).lower() == query_broker

    # 3. X-OpenAlgo-Broker wins when no path/query broker is provided
    for broker in BROKERS:
        resp = cloud_server.api_post("/api/v1/ping", {"apikey": cloud_server.api_key}, broker=broker)
        assert resp.status_code == 200, resp.text
        assert str(resp.json().get("data", {}).get("broker", "")).lower() == broker

    # 4. Shared API key fallback (no path, no query, no header) deterministically resolves to first broker
    resp_fb = cloud_server.api_post("/api/v1/ping", {"apikey": cloud_server.api_key})
    assert resp_fb.status_code == 200, resp_fb.text
    assert str(resp_fb.json().get("data", {}).get("broker", "")).lower() == BROKERS[0]


@pytest.mark.tier2
def test_t2_f6_unknown_and_malformed_broker_headers_and_paths_rejected(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Unknown/malformed broker names are rejected cleanly without 500s; valid casing/whitespace normalizes."""
    # Uppercase + surrounding whitespace normalizes cleanly
    norm_resp = cloud_server.api_post(
        "/api/v1/ping",
        {"apikey": cloud_server.api_key},
        headers={"X-OpenAlgo-Broker": "  DELTAEXCHANGE  "},
    )
    assert norm_resp.status_code == 200, norm_resp.text
    assert str(norm_resp.json().get("data", {}).get("broker", "")).lower() == "deltaexchange"

    with cloud_server.session_client("fyers") as cli:
        # Unknown broker credentials PUT returns 404 JSON error
        for bad_broker in ("zerodha", "upstox", "invalid_broker_xyz"):
            r_cred = cli.put(
                f"/api/broker/credentials/{bad_broker}",
                json={"broker_api_key": "dummy"},
                headers={"X-OpenAlgo-Broker": "fyers"},
            )
            assert r_cred.status_code == 404, (
                f"Expected 404 for unknown broker credentials {bad_broker}, got {r_cred.status_code}: {r_cred.text}"
            )
            r_auth = cli.get(f"/{bad_broker}/auth")
            assert r_auth.status_code == 404

    # Malformed / injection-like X-OpenAlgo-Broker & ?broker= never cause 500 or leak unknown broker
    malformed_values = ("zerodha", "../etc/passwd", "fyers;DROP TABLE", "", "   ")
    for bad in malformed_values:
        r_api = cloud_server.api_post(
            "/api/v1/ping",
            {"apikey": "definitely-not-a-valid-key"},
            query={"broker": bad},
            headers={"X-OpenAlgo-Broker": bad},
        )
        assert r_api.status_code in (400, 401, 403), (
            f"Malformed broker '{bad}' with bad apikey returned unexpected {r_api.status_code}: {r_api.text}"
        )
        assert r_api.status_code != 500

    ctx_errors = [e for e in cloud_server.read_error_lines() if "BrokerContextError" in json.dumps(e)]
    assert ctx_errors == [], f"Malformed broker probes triggered BrokerContextError: {ctx_errors[:3]}"


@pytest.mark.tier2
def test_t2_f6_groww_rejects_unsupported_exchanges_mcx_and_crypto(server_factory) -> None:
    """Groww supports NSE/BSE/NFO/BFO only and rejects MCX and CRYPTO orders/quotes."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    orders_before = _sqlite_count(server.broker_db_path("groww", "sandbox.db"), "sandbox_orders")

    for unsupported_exch, sym in [("MCX", "CRUDEOIL25MARFUT"), ("CRYPTO", "BTCUSDFUT")]:
        ord_resp = _place_paper_order(
            server,
            "groww",
            symbol=sym,
            exchange=unsupported_exch,
            product="NRML",
            action="BUY",
            quantity=1,
            pricetype="MARKET",
            strategy="t2_groww_bad_exch",
        )
        body = ord_resp.json()
        assert ord_resp.status_code in (400, 404, 422), (
            f"Groww must reject unsupported exchange {unsupported_exch} order with 4xx, got "
            f"HTTP {ord_resp.status_code} {body}"
        )
        assert body.get("status") == "error", (
            f"Groww unexpectedly accepted unsupported exchange {unsupported_exch}: {body}"
        )

        q_resp = server.api_post(
            "/api/v1/quotes",
            {"apikey": server.api_key, "symbol": sym, "exchange": unsupported_exch},
            broker="groww",
        )
        q_body = q_resp.json()
        assert q_resp.status_code in (400, 404, 422), (
            f"Groww quotes must reject unsupported exchange {unsupported_exch} with 4xx, got "
            f"HTTP {q_resp.status_code} {q_body}"
        )
        assert q_body.get("status") == "error", (
            f"Groww quotes unexpectedly accepted unsupported exchange {unsupported_exch}: {q_body}"
        )

    orders_after = _sqlite_count(server.broker_db_path("groww", "sandbox.db"), "sandbox_orders")
    assert orders_after == orders_before, "Rejected unsupported-exchange orders must not persist in sandbox_orders"


@pytest.mark.tier2
def test_t2_f6_delta_fractional_qty_allowed_for_spot_rejected_for_derivatives(server_factory) -> None:
    """Delta Exchange allows fractional quantity for SPOT (BTCINR) and rejects it for derivatives (PERPFUT)."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCINR", ltp=5500000.0)
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0)

    # 1. Delta SPOT fractional quantity -> allowed
    spot_resp = _place_paper_order(
        server,
        "deltaexchange",
        symbol="BTCINR",
        exchange="CRYPTO",
        product="CNC",
        action="BUY",
        quantity=0.5,
        pricetype="MARKET",
        strategy="t2_delta_spot_frac",
    )
    assert spot_resp.status_code == 200, f"Delta SPOT fractional qty failed: {spot_resp.text}"
    assert spot_resp.json().get("status") == "success", spot_resp.json()

    # 2. Delta derivative (PERPFUT) fractional quantity -> rejected
    fut_resp = _place_paper_order(
        server,
        "deltaexchange",
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        product="NRML",
        action="BUY",
        quantity=0.5,
        pricetype="MARKET",
        strategy="t2_delta_fut_frac",
    )
    fut_body = fut_resp.json()
    assert 400 <= fut_resp.status_code < 500 and fut_body.get("status") == "error", (
        f"Delta derivative must reject fractional qty 0.5 with 4xx error, got HTTP {fut_resp.status_code} {fut_body}"
    )

    # 3. Fyers & Groww equity fractional quantities -> rejected
    for broker in ("fyers", "groww"):
        eq_resp = _place_paper_order(
            server,
            broker,
            symbol="SBIN",
            exchange="NSE",
            product="CNC",
            action="BUY",
            quantity=0.5,
            pricetype="MARKET",
            strategy=f"t2_{broker}_frac",
        )
        eq_body = eq_resp.json()
        assert 400 <= eq_resp.status_code < 500 and eq_body.get("status") == "error", (
            f"{broker} must reject fractional qty 0.5 with 4xx error, got HTTP {eq_resp.status_code} {eq_body}"
        )


@pytest.mark.tier2
def test_t2_f6_python_concurrent_same_name_strategy_isolation_no_collision(
    server_factory, tmp_path: Path
) -> None:
    """Same-name /python strategies running on deltaexchange and fyers never collide in RUNNING/STOPPING maps."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    delta_hb = tmp_path / "delta_hb.txt"
    fyers_hb = tmp_path / "fyers_hb.txt"

    def _make_script(hb_path: Path) -> bytes:
        return (
            "import os, time\n"
            f"out = {str(hb_path)!r}\n"
            "broker = os.environ.get('OPENALGO_BROKER', '')\n"
            "for i in range(60):\n"
            "    with open(out, 'a', encoding='utf-8') as f:\n"
            "        f.write(f'{broker}:{i}\\n')\n"
            "    time.sleep(0.25)\n"
        ).encode("utf-8")

    with server.session_client("deltaexchange") as delta_cli, server.session_client("fyers") as fyers_cli:
        # Upload same filename & same strategy_name to BOTH brokers concurrently
        def _upload(cli, exch: str, script_bytes: bytes) -> str:
            up = cli.post(
                "/python/new",
                data={
                    "strategy_name": "SharedNameStrat",
                    "exchange": exch,
                    "schedule_start": "00:00",
                    "schedule_days": json.dumps(["mon", "tue", "wed", "thu", "fri", "sat", "sun"]),
                },
                files={"strategy_file": ("shared_strat.py", script_bytes, "text/x-python")},
                headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
            )
            assert up.status_code in (200, 201), f"Upload failed: {up.status_code} {up.text[:300]}"
            body = up.json()
            assert body.get("status") == "success", body
            return str(body["data"]["strategy_id"])

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f_delta = pool.submit(_upload, delta_cli, "CRYPTO", _make_script(delta_hb))
            f_fyers = pool.submit(_upload, fyers_cli, "NSE", _make_script(fyers_hb))
            delta_sid = f_delta.result()
            fyers_sid = f_fyers.result()

        # Start BOTH strategies
        s_delta = delta_cli.post(
            f"/python/start/{delta_sid}",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        s_fyers = fyers_cli.post(
            f"/python/start/{fyers_sid}",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        assert s_delta.status_code == 200, f"Delta start failed: {s_delta.text}"
        assert s_fyers.status_code == 200, f"Fyers start failed: {s_fyers.text}"

        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and not (delta_hb.exists() and fyers_hb.exists()):
            time.sleep(0.15)
        assert delta_hb.exists() and fyers_hb.exists(), "Both broker strategies must start concurrently"
        assert "deltaexchange:" in delta_hb.read_text(encoding="utf-8")
        assert "fyers:" in fyers_hb.read_text(encoding="utf-8")

        # Stop and delete Fyers strategy -> Delta strategy MUST keep running unaffected!
        stop_fyers = fyers_cli.post(
            f"/python/stop/{fyers_sid}",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        assert stop_fyers.status_code == 200, stop_fyers.text
        del_fyers = fyers_cli.post(
            f"/python/delete/{fyers_sid}",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        assert del_fyers.status_code == 200, del_fyers.text

        lines_before = len(delta_hb.read_text(encoding="utf-8").splitlines())
        time.sleep(1.0)
        lines_after = len(delta_hb.read_text(encoding="utf-8").splitlines())
        assert lines_after > lines_before, (
            "Stopping/deleting Fyers same-name strategy killed or stalled Delta's sibling strategy!"
        )

        # Delta still reports its strategy running
        d_info = delta_cli.get(f"/python/api/strategy/{delta_sid}")
        assert d_info.status_code == 200, d_info.text
        assert "running" in json.dumps(d_info.json()).lower()

        delta_cli.post(
            f"/python/stop/{delta_sid}",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )


@pytest.mark.tier2
def test_t2_f6_invalid_or_missing_apikey_rejected_across_all_brokers(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Missing, empty, whitespace, and wrong API keys are rejected across all 3 brokers without side effects."""
    bad_keys: list[Any] = [None, "", "   ", "wrong-api-key-0000000000"]
    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        before = _sqlite_count(cloud_server.broker_db_path(broker, "sandbox.db"), "sandbox_orders")
        for bad_key in bad_keys:
            ping_payload = {} if bad_key is None else {"apikey": bad_key}
            r_ping = cloud_server.api_post("/api/v1/ping", ping_payload, broker=broker)
            assert r_ping.status_code in (400, 401, 403), (
                f"{broker} ping with bad apikey={bad_key!r} returned {r_ping.status_code}: {r_ping.text}"
            )
            assert r_ping.json().get("status") == "error"

            ord_payload = {
                "strategy": "t2_bad_key",
                "exchange": spec["exchange"],
                "symbol": spec["symbol"],
                "action": "BUY",
                "quantity": 1,
                "pricetype": "MARKET",
                "product": spec["product"],
            }
            if bad_key is not None:
                ord_payload["apikey"] = bad_key
            r_ord = cloud_server.api_post("/api/v1/placeorder", ord_payload, broker=broker)
            assert r_ord.status_code in (400, 401, 403), (
                f"{broker} placeorder with bad apikey={bad_key!r} returned {r_ord.status_code}: {r_ord.text}"
            )
            assert r_ord.json().get("status") == "error"

        after = _sqlite_count(cloud_server.broker_db_path(broker, "sandbox.db"), "sandbox_orders")
        assert after == before, f"Unauthenticated placeorder mutated {broker} sandbox_orders"


# ===========================================================================
# E2E-F7: Multi-Broker WebSocket Proxy (:8765) — Boundary/Negative (6 Tier 2 tests)
# ===========================================================================


@pytest.mark.tier2
def test_t2_f7_ws_unauthenticated_commands_return_not_authenticated(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Sending subscribe, unsubscribe, subscribe_orders, or broker_info before auth returns NOT_AUTHENTICATED."""
    proxy = ws_proxy_factory(server=cloud_server)
    unauth_frames = [
        {
            "action": "subscribe",
            "symbols": [{"symbol": "BTCUSDFUT", "exchange": "CRYPTO"}],
            "mode": 1,
            "request_id": "unauth-sub",
        },
        {
            "action": "unsubscribe",
            "symbols": [{"symbol": "BTCUSDFUT", "exchange": "CRYPTO"}],
            "mode": 1,
            "request_id": "unauth-unsub",
        },
        {"action": "subscribe_orders", "request_id": "unauth-ord"},
        {"action": "get_broker_info", "request_id": "unauth-info"},
    ]
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        for frame in unauth_frames:
            ws.send(json.dumps(frame))
            resp = _ws_recv_matching(ws, lambda m: m.get("status") == "error", timeout=5.0)
            assert resp.get("status") == "error", resp
            assert resp.get("code") == "NOT_AUTHENTICATED", (
                f"Expected NOT_AUTHENTICATED for {frame['action']}, got {resp}"
            )


@pytest.mark.tier2
def test_t2_f7_ws_malformed_json_returns_invalid_json_without_crashing_proxy(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Malformed JSON frames return INVALID_JSON error code and never crash the WS proxy server."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        for bad_raw in ("{truncated json", "not-json-at-all", ""):
            ws.send(bad_raw)
            resp = _ws_recv_matching(ws, lambda m: m.get("status") == "error", timeout=5.0)
            assert resp.get("status") == "error", resp
            assert resp.get("code") == "INVALID_JSON", (
                f"Expected INVALID_JSON for payload {bad_raw!r}, got {resp}"
            )

        # Same connection can still authenticate normally afterwards
        ack = _ws_authenticate(ws, cloud_server.api_key, broker="fyers")
        assert ack.get("status") == "success", ack


@pytest.mark.tier2
def test_t2_f7_ws_invalid_mode_rejects_bool_zero_oob_and_unknown_string(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Subscribe rejects bool (True/False), 0, negative, out-of-range (4, 99), and unknown strings with INVALID_MODE."""
    proxy = ws_proxy_factory(server=cloud_server)
    invalid_modes: list[Any] = [True, False, 0, -1, 4, 99, "TICK_STREAM", ""]
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        ack = _ws_authenticate(ws, cloud_server.api_key, broker="deltaexchange")
        assert ack.get("status") == "success", ack

        for idx, bad_mode in enumerate(invalid_modes):
            req_id = f"bad-mode-{idx}"
            ws.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "symbols": [{"symbol": "BTCUSDFUT", "exchange": "CRYPTO"}],
                        "mode": bad_mode,
                        "request_id": req_id,
                    }
                )
            )
            resp = _ws_recv_matching(
                ws,
                lambda m: m.get("status") == "error" or m.get("type") == "subscribe",
                timeout=5.0,
            )
            assert resp.get("status") == "error", f"mode={bad_mode!r} must fail, got {resp}"
            assert resp.get("code") == "INVALID_MODE", (
                f"Expected INVALID_MODE for mode={bad_mode!r}, got {resp}"
            )


@pytest.mark.tier2
def test_t2_f7_ws_invalid_action_and_empty_subscribe_params_rejected(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Unknown action returns INVALID_ACTION and empty/missing subscribe symbols returns INVALID_PARAMETERS."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        ack = _ws_authenticate(ws, cloud_server.api_key, broker="groww")
        assert ack.get("status") == "success", ack

        ws.send(json.dumps({"action": "nonexistent_command_xyz", "request_id": "bad-act-1"}))
        act_err = _ws_recv_matching(ws, lambda m: m.get("status") == "error", timeout=5.0)
        assert act_err.get("code") == "INVALID_ACTION", act_err

        ws.send(json.dumps({"action": "subscribe", "symbols": [], "mode": 1, "request_id": "empty-sym"}))
        param_err = _ws_recv_matching(ws, lambda m: m.get("status") == "error", timeout=5.0)
        assert param_err.get("code") == "INVALID_PARAMETERS", param_err


@pytest.mark.tier2
def test_t2_f7_ws_bad_apikey_unknown_broker_and_auth_timeout_4401(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Invalid API key returns AUTHENTICATION_ERROR, unknown broker is rejected, and idle unauth socket closes 4401."""
    proxy = ws_proxy_factory(
        server=cloud_server,
        extra_env={"WS_AUTH_GRACE_SECONDS": "2"},
    )
    # 1. Wrong API key -> AUTHENTICATION_ERROR
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        err = _ws_authenticate(ws, "totally-wrong-api-key-1234", broker="fyers")
        assert err.get("status") == "error", err
        assert err.get("code") == "AUTHENTICATION_ERROR", err

    # 2. Empty API key -> AUTHENTICATION_ERROR
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        err_empty = _ws_authenticate(ws, "", broker="deltaexchange")
        assert err_empty.get("status") == "error", err_empty
        assert err_empty.get("code") == "AUTHENTICATION_ERROR", err_empty

    # 3. Unknown broker name -> error (never succeeds with bogus broker)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        err_broker = _ws_authenticate(ws, cloud_server.api_key, broker="zerodha")
        assert err_broker.get("status") == "error", f"Unknown broker must be rejected: {err_broker}"
        assert err_broker.get("code") in (
            "AUTHENTICATION_ERROR",
            "INVALID_PARAMETERS",
            "BROKER_ERROR",
            "BROKER_INIT_ERROR",
        ), err_broker

    # 4. Unauthenticated idle socket closed by server after WS_AUTH_GRACE_SECONDS with code 4401
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        closed_code: int | None = None
        try:
            ws.recv(timeout=5.0)
        except ConnectionClosed as exc:
            closed_code = exc.rcvd.code if exc.rcvd is not None else None
        assert closed_code == 4401, f"Expected WS auth timeout close code 4401, got {closed_code}"


@pytest.mark.tier2
def test_t2_f7_ws_rapid_multibroker_connect_disconnect_preserves_sibling_adapters(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Rapidly connecting and closing Groww sockets never evicts active Delta and Fyers adapters/subscriptions."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws_delta, ws_connect(
        proxy.ws_url, open_timeout=5.0
    ) as ws_fyers:
        assert _ws_authenticate(ws_delta, cloud_server.api_key, broker="deltaexchange").get("status") == "success"
        assert _ws_authenticate(ws_fyers, cloud_server.api_key, broker="fyers").get("status") == "success"

        ws_delta.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "symbols": [{"symbol": "BTCUSDFUT", "exchange": "CRYPTO"}],
                    "mode": "LTP",
                }
            )
        )
        assert _ws_recv_matching(ws_delta, lambda m: m.get("type") == "subscribe").get("status") == "success"

        ws_fyers.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "symbols": [{"symbol": "SBIN", "exchange": "NSE"}],
                    "mode": "LTP",
                }
            )
        )
        assert _ws_recv_matching(ws_fyers, lambda m: m.get("type") == "subscribe").get("status") == "success"

        # Rapid connect / auth / subscribe / disconnect churn on third broker (groww)
        for i in range(5):
            with ws_connect(proxy.ws_url, open_timeout=5.0) as ws_churn:
                ack = _ws_authenticate(ws_churn, cloud_server.api_key, broker="groww")
                assert ack.get("status") == "success", f"Groww churn iter {i} failed: {ack}"
                ws_churn.send(
                    json.dumps(
                        {
                            "action": "subscribe",
                            "symbols": [{"symbol": "SBIN", "exchange": "NSE"}],
                            "mode": "LTP",
                        }
                    )
                )
                _ws_recv_matching(ws_churn, lambda m: m.get("type") == "subscribe")

        # Both persistent sibling sessions must remain alive and receive ticks with their own broker tag
        for broker, sock, exch, sym, ltp in [
            ("deltaexchange", ws_delta, "CRYPTO", "BTCUSDFUT", 66420.5),
            ("fyers", ws_fyers, "NSE", "SBIN", 815.75),
        ]:
            sock.send(json.dumps({"action": "ping", "_pingId": f"post-churn-{broker}"}))
            pong = _ws_recv_matching(sock, lambda m: m.get("type") == "pong")
            assert pong.get("_pingId") == f"post-churn-{broker}"

            for _ in range(3):
                proxy.publish_tick(exch, sym, "LTP", {"symbol": sym, "exchange": exch, "ltp": ltp})
                time.sleep(0.08)

            tick = _ws_recv_matching(
                sock,
                lambda m, s=sym: m.get("type") == "market_data" and m.get("symbol") == s,
                timeout=6.0,
            )
            assert str(tick.get("broker", "")).lower() == broker
            assert float(tick.get("data", {}).get("ltp", 0.0)) == pytest.approx(ltp, rel=1e-4)


# ===========================================================================
# E2E-F8: Multi-Broker Order Updates & Sandbox — Boundary/Negative (6 Tier 2 tests)
# ===========================================================================


@pytest.mark.tier2
def test_t2_f8_limit_and_sl_exact_boundary_fill_vs_one_tick_away_non_fill(server_factory) -> None:
    """LIMIT and SL-M orders do NOT fill 1 tick away from price/trigger and DO fill at exact boundary."""
    server = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"SANDBOX_ENGINE_TYPE": "polling", "SANDBOX_POLL_INTERVAL": "1"},
    )
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0, low=780.0, high=820.0)
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0, low=64000.0, high=67000.0)
    server.set_mock_quote("groww", "NSE", "SBIN", ltp=800.0, low=780.0, high=820.0)

    # 1. BUY LIMIT @ 790.00 (Fyers, tick=0.05)
    buy_lim_id = _place_paper_order(
        server, "fyers", action="BUY", quantity=2, pricetype="LIMIT", price=790.0
    ).json()["orderid"]
    # 2. SELL LIMIT @ 66000.0 (Delta, tick=0.5)
    sell_lim_id = _place_paper_order(
        server, "deltaexchange", action="SELL", quantity=1, pricetype="LIMIT", price=66000.0
    ).json()["orderid"]
    # 3. BUY SL-M trigger @ 810.00 (Groww, tick=0.05)
    buy_slm_id = _place_paper_order(
        server, "groww", action="BUY", quantity=2, pricetype="SL-M", trigger_price=810.0, price=0.0
    ).json()["orderid"]

    # Move all 3 quotes to 1 tick away from fill/trigger boundary -> NONE may fill
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=790.05, bid=790.0, ask=790.1, low=780.0, high=820.0)
    server.set_mock_quote(
        "deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65999.5, bid=65999.0, ask=66000.0, low=64000.0, high=67000.0
    )
    server.set_mock_quote("groww", "NSE", "SBIN", ltp=809.95, bid=809.9, ask=810.0, low=780.0, high=820.0)
    time.sleep(2.5)

    assert _order_status(server, "fyers", buy_lim_id) == "open", "BUY LIMIT filled 1 tick above limit!"
    assert _order_status(server, "deltaexchange", sell_lim_id) == "open", "SELL LIMIT filled 1 tick below limit!"
    assert _order_status(server, "groww", buy_slm_id) == "trigger pending", "BUY SL-M triggered 1 tick below trigger!"

    # Move all 3 quotes to EXACT boundary -> ALL 3 must fill
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=790.00, bid=789.95, ask=790.05, low=780.0, high=820.0)
    server.set_mock_quote(
        "deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=66000.0, bid=65999.5, ask=66000.5, low=64000.0, high=67000.0
    )
    server.set_mock_quote("groww", "NSE", "SBIN", ltp=810.00, bid=809.95, ask=810.05, low=780.0, high=820.0)

    assert _wait_for_order_status(server, "fyers", buy_lim_id, "complete") == "complete"
    assert _wait_for_order_status(server, "deltaexchange", sell_lim_id, "complete") == "complete"
    assert _wait_for_order_status(server, "groww", buy_slm_id, "complete") == "complete"


@pytest.mark.tier2
def test_t2_f8_stale_quote_guard_defers_fill_when_ltp_outside_low_high(server_factory) -> None:
    """Sandbox stale-quote guard defers LIMIT fills when LTP is outside [low, high], then fills once valid."""
    quote_state: dict[str, Any] = {
        "ltp": 65000.0,
        "bid": 64995.0,
        "ask": 65005.0,
        "open": 64800.0,
        "low": 64000.0,
        "high": 66000.0,
        "close": 64800.0,
        "order_violations": [],
    }
    state_lock = threading.Lock()

    class _DeltaStaleQuoteHandler(BaseHTTPRequestHandler):
        def log_message(self, _fmt: str, *_args: Any) -> None:
            return

        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            with state_lock:
                q = dict(quote_state)
            ticker = {
                "symbol": "BTCUSD",
                "product_id": 27,
                "mark_price": str(q["ltp"]),
                "open": q["open"],
                "high": q["high"],
                "low": q["low"],
                "close": q["close"],
                "volume": 1200,
                "oi": "5000",
                "quotes": {"best_bid": str(q["bid"]), "best_ask": str(q["ask"])},
            }
            if parsed.path.startswith("/v2/tickers/"):
                payload = {"success": True, "result": ticker}
            elif parsed.path == "/v2/tickers":
                payload = {"success": True, "result": [ticker]}
            else:
                payload = {"success": True, "result": []}
            body = json.dumps(payload).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self) -> None:  # noqa: N802
            with state_lock:
                quote_state["order_violations"].append(("POST", self.path))
            body = json.dumps({"success": False, "error": "REAL_ORDER_BLOCKED"}).encode("utf-8")
            self.send_response(403)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    httpd, _thread, port = _start_loopback_http_server(_DeltaStaleQuoteHandler)
    try:
        server = server_factory(
            profile="cloud",
            paper_trading_only=True,
            extra_env={
                "DELTA_BASE_URL": f"http://127.0.0.1:{port}",
                "SANDBOX_ENGINE_TYPE": "polling",
                "SANDBOX_POLL_INTERVAL": "1",
            },
        )
        # Place BUY LIMIT @ 63500.0 while normal LTP=65000 is above limit
        ord_resp = _place_paper_order(
            server,
            "deltaexchange",
            action="BUY",
            quantity=1,
            pricetype="LIMIT",
            price=63500.0,
            strategy="t2_stale_guard",
        )
        assert ord_resp.status_code == 200, ord_resp.text
        orderid = ord_resp.json()["orderid"]

        # Inject STALE quote: ltp=62000 <= 63500, BUT ltp=62000 < low=64000 (outside [low, high])
        with state_lock:
            quote_state.update({"ltp": 62000.0, "bid": 61995.0, "ask": 62005.0, "low": 64000.0, "high": 66000.0})
        time.sleep(3.0)
        assert _order_status(server, "deltaexchange", orderid) == "open", (
            "Sandbox filled LIMIT order on stale quote where LTP was outside [low, high]!"
        )

        # Now supply valid quote where LTP=63400 is inside [low=63000, high=66000] -> MUST fill
        with state_lock:
            quote_state.update({"ltp": 63400.0, "bid": 63395.0, "ask": 63405.0, "low": 63000.0, "high": 66000.0})
        assert _wait_for_order_status(server, "deltaexchange", orderid, "complete", timeout=15.0) == "complete"
        assert quote_state["order_violations"] == []
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.tier2
def test_t2_f8_gtt_oco_invalid_sl_ge_target_and_mis_product_rejected(server_factory) -> None:
    """GTT rejects OCO with triggerprice_sl >= triggerprice_tg, MIS product (400), and missing triggers."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    gtt_before = _sqlite_count(server.broker_db_path("fyers", "sandbox.db"), "sandbox_gtt")

    invalid_payloads = [
        # 1. OCO boundary: triggerprice_sl == triggerprice_tg
        {
            "apikey": server.api_key,
            "strategy": "t2_gtt_eq",
            "trigger_type": "OCO",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "SELL",
            "product": "CNC",
            "quantity": 1,
            "pricetype": "LIMIT",
            "price": 800.0,
            "triggerprice_sl": 800.0,
            "stoploss": 799.0,
            "triggerprice_tg": 800.0,
            "target": 801.0,
        },
        # 2. OCO inverted: triggerprice_sl > triggerprice_tg
        {
            "apikey": server.api_key,
            "strategy": "t2_gtt_inv",
            "trigger_type": "OCO",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "SELL",
            "product": "CNC",
            "quantity": 1,
            "pricetype": "LIMIT",
            "price": 820.0,
            "triggerprice_sl": 830.0,
            "stoploss": 829.0,
            "triggerprice_tg": 790.0,
            "target": 791.0,
        },
        # 3. MIS product rejected with 400
        {
            "apikey": server.api_key,
            "strategy": "t2_gtt_mis",
            "trigger_type": "SINGLE",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "BUY",
            "product": "MIS",
            "quantity": 1,
            "pricetype": "LIMIT",
            "price": 780.0,
            "triggerprice_tg": 785.0,
        },
        # 4. SINGLE with neither triggerprice_sl nor triggerprice_tg > 0
        {
            "apikey": server.api_key,
            "strategy": "t2_gtt_notrig",
            "trigger_type": "SINGLE",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "BUY",
            "product": "CNC",
            "quantity": 1,
            "pricetype": "LIMIT",
            "price": 780.0,
            "triggerprice_sl": 0.0,
            "triggerprice_tg": 0.0,
        },
    ]

    for idx, payload in enumerate(invalid_payloads):
        resp = server.api_post("/api/v1/placegttorder", payload, broker="fyers")
        assert resp.status_code == 400, (
            f"Invalid GTT payload #{idx} expected HTTP 400, got {resp.status_code}: {resp.text}"
        )
        assert resp.json().get("status") == "error", resp.json()

    gtt_after = _sqlite_count(server.broker_db_path("fyers", "sandbox.db"), "sandbox_gtt")
    assert gtt_after == gtt_before, "Rejected GTT requests must not persist rows in sandbox_gtt"


@pytest.mark.tier2
def test_t2_f8_zero_negative_and_invalid_quantity_or_price_rejected(server_factory) -> None:
    """Zero, negative, and non-numeric quantities and invalid LIMIT/SL prices are rejected across brokers."""
    server = server_factory(profile="cloud", paper_trading_only=True)

    for broker in BROKERS:
        before = _sqlite_count(server.broker_db_path(broker, "sandbox.db"), "sandbox_orders")

        for bad_qty in (0, -1, -50, "invalid_qty"):
            resp = _place_paper_order(server, broker, action="BUY", quantity=bad_qty, pricetype="MARKET")
            assert resp.status_code == 400, (
                f"{broker} accepted invalid qty={bad_qty!r}: HTTP {resp.status_code} {resp.text}"
            )
            assert resp.json().get("status") == "error"

        # LIMIT order with zero / negative price
        for bad_price in (0.0, -10.0):
            resp_lim = _place_paper_order(
                server, broker, action="BUY", quantity=1, pricetype="LIMIT", price=bad_price
            )
            assert 400 <= resp_lim.status_code < 500 and resp_lim.json().get("status") == "error", (
                f"{broker} accepted LIMIT with price={bad_price}: HTTP {resp_lim.status_code} {resp_lim.text}"
            )

        # SL order with zero / negative trigger_price
        for bad_trig in (0.0, -5.0):
            resp_sl = _place_paper_order(
                server,
                broker,
                action="BUY",
                quantity=1,
                pricetype="SL",
                price=DEFAULT_SYMBOLS[broker]["ltp"],
                trigger_price=bad_trig,
            )
            assert 400 <= resp_sl.status_code < 500 and resp_sl.json().get("status") == "error", (
                f"{broker} accepted SL with trigger_price={bad_trig}: HTTP {resp_sl.status_code} {resp_sl.text}"
            )

        after = _sqlite_count(server.broker_db_path(broker, "sandbox.db"), "sandbox_orders")
        assert after == before, f"Invalid orders leaked rows into {broker} sandbox_orders"


@pytest.mark.tier2
def test_t2_f8_modify_or_cancel_nonexistent_completed_or_cross_broker_order_rejected(
    server_factory,
) -> None:
    """Cancelling/modifying nonexistent, already-completed, or sibling-broker order IDs is rejected cleanly."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0)

    # 1. Cancel nonexistent order ID
    r_missing = server.api_post(
        "/api/v1/cancelorder",
        {"apikey": server.api_key, "strategy": "t2_missing", "orderid": "DOES-NOT-EXIST-99999"},
        broker="fyers",
    )
    assert 400 <= r_missing.status_code < 500 and r_missing.json().get("status") == "error", (
        f"Expected 4xx error for missing order cancel, got HTTP {r_missing.status_code} {r_missing.text}"
    )

    # 2. Place resting LIMIT order on fyers -> cross-broker cancel via groww & deltaexchange MUST fail
    lim_id = _place_paper_order(
        server, "fyers", action="BUY", quantity=2, pricetype="LIMIT", price=750.0, strategy="t2_xbroker"
    ).json()["orderid"]

    for wrong_broker in ("groww", "deltaexchange"):
        r_xcancel = server.api_post(
            "/api/v1/cancelorder",
            {"apikey": server.api_key, "strategy": "t2_xbroker", "orderid": lim_id},
            broker=wrong_broker,
        )
        assert r_xcancel.json().get("status") == "error", (
            f"Cross-broker cancel from {wrong_broker} unexpectedly succeeded on Fyers order {lim_id}: {r_xcancel.text}"
        )

    # Fyers order must remain untouched ("open")
    assert _order_status(server, "fyers", lim_id) == "open"

    # 3. Cancel already-completed MARKET order -> MUST fail
    mkt_id = _place_paper_order(
        server, "fyers", action="BUY", quantity=1, pricetype="MARKET", strategy="t2_done_cancel"
    ).json()["orderid"]
    assert _order_status(server, "fyers", mkt_id) == "complete"

    r_done = server.api_post(
        "/api/v1/cancelorder",
        {"apikey": server.api_key, "strategy": "t2_done_cancel", "orderid": mkt_id},
        broker="fyers",
    )
    assert r_done.json().get("status") == "error", (
        f"Cancelling completed order unexpectedly succeeded: {r_done.text}"
    )


@pytest.mark.tier2
def test_t2_f8_concurrent_multibroker_modify_cancel_and_squareoff_no_leakage(server_factory) -> None:
    """Concurrent modify, cancel, fill, and square-off reload across all 3 brokers stay isolated without clobber."""
    server = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"SANDBOX_ENGINE_TYPE": "polling", "SANDBOX_POLL_INTERVAL": "1"},
    )
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0, low=63000.0, high=67000.0)
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0, low=770.0, high=820.0)
    server.set_mock_quote("groww", "NSE", "SBIN", ltp=800.0, low=770.0, high=820.0)

    fyers_lim = _place_paper_order(
        server, "fyers", action="BUY", quantity=4, pricetype="LIMIT", price=760.0, strategy="t2_conc"
    ).json()["orderid"]
    groww_lim = _place_paper_order(
        server, "groww", action="BUY", quantity=3, pricetype="LIMIT", price=765.0, strategy="t2_conc"
    ).json()["orderid"]
    delta_lim = _place_paper_order(
        server, "deltaexchange", action="BUY", quantity=1, pricetype="LIMIT", price=64200.0, strategy="t2_conc"
    ).json()["orderid"]

    # Trigger concurrent operations across all 3 brokers simultaneously
    def _mod_fyers() -> int:
        r = server.api_post(
            "/api/v1/modifyorder",
            {
                "apikey": server.api_key,
                "strategy": "t2_conc",
                "exchange": "NSE",
                "symbol": "SBIN",
                "orderid": fyers_lim,
                "action": "BUY",
                "product": "CNC",
                "pricetype": "LIMIT",
                "price": 775.0,
                "quantity": 6,
                "disclosed_quantity": 0,
                "trigger_price": 0,
            },
            broker="fyers",
        )
        assert r.json().get("status") == "success", r.text
        return r.status_code

    def _cancel_groww() -> int:
        r = server.api_post(
            "/api/v1/cancelorder",
            {"apikey": server.api_key, "strategy": "t2_conc", "orderid": groww_lim},
            broker="groww",
        )
        assert r.json().get("status") == "success", r.text
        return r.status_code

    def _fill_delta() -> int:
        server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=64000.0, low=63000.0, high=67000.0)
        r = _place_paper_order(
            server, "deltaexchange", action="BUY", quantity=1, pricetype="MARKET", strategy="t2_conc"
        )
        assert r.json().get("status") == "success", r.text
        return r.status_code

    def _reload_sq(broker: str) -> int:
        with server.session_client(broker) as cli:
            r1 = cli.post("/sandbox/reload-squareoff")
            r2 = cli.get("/sandbox/squareoff-status")
            assert r1.status_code == 200 and r2.status_code == 200
            return r2.status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        futures = [
            pool.submit(_mod_fyers),
            pool.submit(_cancel_groww),
            pool.submit(_fill_delta),
            pool.submit(_reload_sq, "fyers"),
            pool.submit(_reload_sq, "groww"),
            pool.submit(_reload_sq, "deltaexchange"),
        ]
        codes = [f.result() for f in futures]

    assert all(c == 200 for c in codes), codes
    assert _order_status(server, "fyers", fyers_lim) == "open"
    assert _order_status(server, "groww", groww_lim) == "cancelled"
    assert _wait_for_order_status(server, "deltaexchange", delta_lim, "complete", timeout=15.0) == "complete"

    # Strict per-broker counts: fyers=1 order, groww=1 order, delta=2 orders
    assert _sqlite_count(server.broker_db_path("fyers", "sandbox.db"), "sandbox_orders") == 1
    assert _sqlite_count(server.broker_db_path("groww", "sandbox.db"), "sandbox_orders") == 1
    assert _sqlite_count(server.broker_db_path("deltaexchange", "sandbox.db"), "sandbox_orders") == 2

    ctx_errors = [e for e in server.read_error_lines() if "BrokerContextError" in json.dumps(e)]
    assert ctx_errors == [], f"Concurrent sandbox ops triggered BrokerContextError: {ctx_errors[:3]}"
    server.assert_zero_real_broker_order_calls()


# ===========================================================================
# E2E-F9: Live-Bar Strategies & OpenScript Cloud — Boundary/Negative (6 Tier 2 tests)
# ===========================================================================


@pytest.mark.tier2
def test_t2_f9_live_strategy_zero_pandas_numpy_across_all_modules(tmp_path: Path) -> None:
    """Every module under strategies/live/ has zero static or dynamic pandas/numpy dependencies."""
    live_dir = _live_strategies_dir()
    assert live_dir.is_dir(), f"Missing tracked strategies/live/ directory: {live_dir}"

    py_files = sorted(live_dir.rglob("*.py"))
    assert py_files, f"No Python modules found in {live_dir}"

    forbidden = {"pandas", "numpy"}
    for py_file in py_files:
        src = py_file.read_text(encoding="utf-8")
        tree = ast.parse(src, filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert alias.name.split(".")[0] not in forbidden, (
                        f"{py_file} imports forbidden module {alias.name}"
                    )
            elif isinstance(node, ast.ImportFrom) and node.module:
                assert node.module.split(".")[0] not in forbidden, (
                    f"{py_file} imports from forbidden module {node.module}"
                )
            elif isinstance(node, ast.Call):
                # Catch dynamic __import__("pandas") / importlib.import_module("numpy")
                if node.args and isinstance(node.args[0], ast.Constant) and isinstance(node.args[0].value, str):
                    arg_top = node.args[0].value.split(".")[0]
                    if arg_top in forbidden:
                        func_name = ast.unparse(node.func)
                        if "import" in func_name:
                            raise AssertionError(f"{py_file} dynamically imports forbidden module {arg_top}")

    # Dynamically import every module with poisoned pandas/numpy on sys.path
    poison_dir = tmp_path / "poison"
    poison_dir.mkdir()
    for mod in forbidden:
        (poison_dir / f"{mod}.py").write_text(
            f"raise ImportError('{mod} blocked by E2E zero-bloat policy')\n",
            encoding="utf-8",
        )

    module_names = [
        ".".join(p.relative_to(REPO_ROOT).with_suffix("").parts)
        for p in py_files
        if p.name != "__init__.py"
    ]
    probe_code = (
        "import importlib, sys\n"
        f"for name in {module_names!r}:\n"
        "    importlib.import_module(name)\n"
        "assert 'pandas' not in sys.modules and 'numpy' not in sys.modules\n"
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{poison_dir}{os.pathsep}{REPO_ROOT}"
    proc = subprocess.run(
        [sys.executable, "-c", probe_code],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=20.0,
    )
    assert proc.returncode == 0, (
        f"Importing strategies/live/ modules failed without pandas/numpy:\n{proc.stdout}\n{proc.stderr}"
    )


@pytest.mark.tier2
def test_t2_f9_live_strategy_state_json_strictly_bounded_under_10kb_after_over_100_bars(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """After >100 bars of state accumulation, .state.json remains strictly < 10 KB via rolling bar cap."""
    entry = _discover_live_strategy_entry("deltaexchange")
    state_file = tmp_path / "bounded_120_bars.state.json"

    # First run 1 cycle to observe exact native state schema written by strategies/live/
    cloud_server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0)
    cmd_init = _build_live_strategy_cmd(
        entry,
        broker="deltaexchange",
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        state_file=state_file,
        cycles=2,
        poll_seconds=0.05,
    )
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(REPO_ROOT),
            "OPENALGO_HOST": cloud_server.base_url,
            "OPENALGO_API_KEY": cloud_server.api_key,
            "OPENALGO_BROKER": "deltaexchange",
            "OPENALGO_PROFILE": "cloud",
        }
    )
    p1 = subprocess.run(cmd_init, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    assert p1.returncode == 0, f"Initial live strategy run failed:\n{p1.stdout}\n{p1.stderr}"

    candidates = [state_file] if state_file.is_file() else sorted(tmp_path.rglob("*.state.json"))
    assert candidates, f"Expected .state.json under {tmp_path}"
    persisted = candidates[0]
    state_obj = json.loads(persisted.read_text(encoding="utf-8"))

    # Inject 140 synthetic bars into the rolling bars list to test hard rolling-cap compaction
    bars_key = next(
        (k for k, v in state_obj.items() if isinstance(v, list) and ("bar" in k.lower() or "candle" in k.lower())),
        None,
    )
    assert bars_key is not None, f"Expected rolling bars list in state JSON, got keys={list(state_obj.keys())}"
    sample_bar = (
        state_obj[bars_key][-1]
        if state_obj[bars_key]
        else {"ts": 1700000000, "open": 65000.0, "high": 65100.0, "low": 64900.0, "close": 65050.0, "volume": 100}
    )
    inflated_bars = []
    for i in range(140):
        if isinstance(sample_bar, dict):
            b = dict(sample_bar)
            if "ts" in b:
                b["ts"] = 1700000000 + i * 60
            elif "timestamp" in b:
                b["timestamp"] = 1700000000 + i * 60
            if "close" in b:
                b["close"] = round(65000.0 + i * 2.5, 2)
            inflated_bars.append(b)
        else:
            inflated_bars.append(sample_bar)
    state_obj[bars_key] = inflated_bars
    persisted.write_text(json.dumps(state_obj), encoding="utf-8")

    # Run another live cycle -> rolling cap MUST compact bars and keep file strictly < 10 KB
    cloud_server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65450.0)
    cmd_next = _build_live_strategy_cmd(
        entry,
        broker="deltaexchange",
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        state_file=persisted,
        cycles=3,
        poll_seconds=0.05,
    )
    p2 = subprocess.run(cmd_next, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    assert p2.returncode == 0, f"Live strategy compaction run failed:\n{p2.stdout}\n{p2.stderr}"

    size_bytes = persisted.stat().st_size
    assert 0 < size_bytes < 10 * 1024, (
        f".state.json exceeded 10 KB cap after >100 bars: {size_bytes} bytes"
    )
    compacted = json.loads(persisted.read_text(encoding="utf-8"))
    assert len(compacted[bars_key]) < 100, (
        f"Rolling bars list was not capped after 140 bars: len={len(compacted[bars_key])}"
    )


@pytest.mark.tier2
def test_t2_f9_live_strategy_atomic_restart_recovery_and_corrupt_state_resilience(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """Live strategy recovers cleanly across restarts via atomic .state.json and handles corrupt state gracefully."""
    entry = _discover_live_strategy_entry("fyers")
    state_file = tmp_path / "restart_recovery.state.json"
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(REPO_ROOT),
            "OPENALGO_HOST": cloud_server.base_url,
            "OPENALGO_API_KEY": cloud_server.api_key,
            "OPENALGO_BROKER": "fyers",
            "OPENALGO_PROFILE": "cloud",
        }
    )

    # Run 1: create atomic state
    cloud_server.set_mock_quote("fyers", "NSE", "SBIN", ltp=801.0)
    cmd = _build_live_strategy_cmd(
        entry,
        broker="fyers",
        symbol="SBIN",
        exchange="NSE",
        state_file=state_file,
        cycles=2,
        poll_seconds=0.08,
    )
    r1 = subprocess.run(cmd, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    assert r1.returncode == 0, f"Run 1 failed:\n{r1.stdout}\n{r1.stderr}"

    candidates = [state_file] if state_file.is_file() else sorted(tmp_path.rglob("*.state.json"))
    assert candidates, f"Expected .state.json after Run 1 in {tmp_path}"
    persisted = candidates[0]
    assert not list(persisted.parent.glob("*.tmp")), "Leftover temp file violates atomic os.replace contract"
    state1 = json.loads(persisted.read_text(encoding="utf-8"))

    # Run 2: restart pointing at existing .state.json -> preserves valid JSON & atomic write
    cloud_server.set_mock_quote("fyers", "NSE", "SBIN", ltp=804.5)
    r2 = subprocess.run(cmd, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    assert r2.returncode == 0, f"Run 2 restart failed:\n{r2.stdout}\n{r2.stderr}"
    assert not list(persisted.parent.glob("*.tmp"))
    state2 = json.loads(persisted.read_text(encoding="utf-8"))
    assert isinstance(state2, dict) and state2.keys() >= state1.keys()
    assert persisted.stat().st_size < 10 * 1024

    # Run 3 (corner case): truncated/corrupt .state.json on disk -> strategy recovers cleanly without traceback crash
    persisted.write_text("{corrupt truncated json", encoding="utf-8")
    r3 = subprocess.run(cmd, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    assert r3.returncode == 0, (
        f"Strategy crashed on corrupt .state.json instead of graceful reset:\n{r3.stdout}\n{r3.stderr}"
    )
    recovered = json.loads(persisted.read_text(encoding="utf-8"))
    assert isinstance(recovered, dict)
    assert persisted.stat().st_size < 10 * 1024


@pytest.mark.tier2
def test_t2_f9_live_strategy_zero_history_requests_verified_via_recording_proxy(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """Wire-level HTTP proxy confirms strategies/live/ makes ZERO /api/v1/history calls and sends X-OpenAlgo-Broker."""
    entry = _discover_live_strategy_entry("deltaexchange")
    state_file = tmp_path / "proxy_wire.state.json"
    recorded: list[dict[str, Any]] = []
    rec_lock = threading.Lock()
    upstream = cloud_server.base_url

    class _RecordingProxyHandler(BaseHTTPRequestHandler):
        def log_message(self, _fmt: str, *_args: Any) -> None:
            return

        def _forward(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length > 0 else b""
            hdrs = {
                k: v
                for k, v in self.headers.items()
                if k.lower() not in ("host", "content-length", "connection")
            }
            with rec_lock:
                recorded.append(
                    {
                        "method": method,
                        "path": urlparse(self.path).path,
                        "headers": {k.lower(): v for k, v in hdrs.items()},
                    }
                )
            resp = httpx.request(
                method,
                f"{upstream}{self.path}",
                content=body,
                headers=hdrs,
                timeout=15.0,
            )
            self.send_response(resp.status_code)
            for k, v in resp.headers.items():
                if k.lower() not in ("transfer-encoding", "connection", "content-encoding", "content-length"):
                    self.send_header(k, v)
            self.send_header("Content-Length", str(len(resp.content)))
            self.end_headers()
            self.wfile.write(resp.content)

        def do_GET(self) -> None:  # noqa: N802
            self._forward("GET")

        def do_POST(self) -> None:  # noqa: N802
            self._forward("POST")

    httpd, _thread, proxy_port = _start_loopback_http_server(_RecordingProxyHandler)
    try:
        cloud_server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65300.0)
        cmd = _build_live_strategy_cmd(
            entry,
            broker="deltaexchange",
            symbol="BTCUSDFUT",
            exchange="CRYPTO",
            state_file=state_file,
            cycles=3,
            poll_seconds=0.1,
        )
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(REPO_ROOT),
                "OPENALGO_HOST": f"http://127.0.0.1:{proxy_port}",
                "OPENALGO_API_KEY": cloud_server.api_key,
                "OPENALGO_BROKER": "deltaexchange",
                "OPENALGO_PROFILE": "cloud",
            }
        )
        proc = subprocess.run(cmd, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
        assert proc.returncode == 0, f"Strategy via proxy failed:\n{proc.stdout}\n{proc.stderr}"

        assert len(recorded) >= 1, "Live strategy made zero HTTP requests to OpenAlgo"
        history_hits = [r for r in recorded if "/history" in r["path"] or "/ticker" in r["path"]]
        assert history_hits == [], f"Live strategy called forbidden history endpoint in cloud mode: {history_hits}"

        api_hits = [r for r in recorded if r["path"].startswith("/api/v1/")]
        assert api_hits, f"Expected /api/v1/* calls from live strategy, got: {recorded}"
        for hit in api_hits:
            assert hit["headers"].get("x-openalgo-broker", "").lower() == "deltaexchange", (
                f"Live strategy omitted explicit X-OpenAlgo-Broker header on {hit}"
            )
        assert cloud_server.duckdb_files() == []
    finally:
        httpd.shutdown()
        httpd.server_close()


@pytest.mark.tier2
def test_t2_f9_openscript_runner_rejects_extra_config_keys_and_nonempty_start_body(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """OpenScript runner API strictly rejects extra config keys (400), non-empty /start body (400), and short search."""
    with cloud_server.session_client("fyers") as cli:
        # 1. Extra config key (e.g. live escalation attempt) -> 400
        bad_cfg = cli.post(
            "/openscript/runner/config/firstbar.oscript",
            json={
                "symbol": "SBIN",
                "exchange": "NSE",
                "interval": "5m",
                "product": "CNC",
                "mode": "live",
            },
        )
        assert bad_cfg.status_code == 400, (
            f"OpenScript config with extra key 'mode' must return 400, got {bad_cfg.status_code}: {bad_cfg.text}"
        )

        # 2. Non-empty body on /openscript/runner/start/<filename> -> 400
        bad_start = cli.post(
            "/openscript/runner/start/firstbar.oscript",
            json={"force_live": True},
        )
        assert bad_start.status_code == 400, (
            f"OpenScript /start with non-empty JSON body must return 400, got {bad_start.status_code}: {bad_start.text}"
        )

        # 3. Empty body on missing script -> 404
        missing_start = cli.post("/openscript/runner/start/does_not_exist_e2e.oscript")
        assert missing_start.status_code == 404, (
            f"Missing OpenScript start must return 404, got {missing_start.status_code}: {missing_start.text}"
        )

        # 4. Instrument search boundary len(q) < 2 -> empty list
        short_q = cli.get("/openscript/runner/instruments", params={"q": "S", "exchange": "NSE"})
        assert short_q.status_code == 200, short_q.text
        short_body = short_q.json()
        assert short_body.get("data") == [], f"Expected empty instruments list for len(q)<2: {short_body}"


@pytest.mark.tier2
def test_t2_f9_openscript_runner_cloud_403_and_503_graceful_live_warmup(tmp_path: Path) -> None:
    """OpenScript runner degrades gracefully to live-bar warmup on BOTH HTTP 403 and 503 history refusals."""
    scripts_dir = tmp_path / "strategies" / "openscript"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (scripts_dir / "firstbar.oscript").write_text(OPENSCRIPT_FIRST_BAR_SOURCE, encoding="utf-8")
    (scripts_dir / "firstbar.oscript.program.json").write_text(
        OPENSCRIPT_FIRST_BAR_PROGRAM, encoding="utf-8"
    )
    runner_script = REPO_ROOT / "openscript_host" / "openscript_runner.py"

    for refusal_status in (403, 503):
        hits: dict[str, int] = {"history": 0, "quotes": 0}
        hits_lock = threading.Lock()
        status_to_send = refusal_status

        class _CloudRefusalHandler(BaseHTTPRequestHandler):
            def log_message(self, _fmt: str, *_args: Any) -> None:
                return

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                if length > 0:
                    self.rfile.read(length)
                path = urlparse(self.path).path
                if path == "/api/v1/history":
                    with hits_lock:
                        hits["history"] += 1
                    payload = {
                        "status": "error",
                        "code": "HISTORICAL_DATA_DISABLED",
                        "message": "Historical data and backtesting are disabled in cloud deployment (OPENALGO_PROFILE=cloud). Available locally.",
                    }
                    self._send_json(status_to_send, payload)
                elif path == "/api/v1/quotes":
                    with hits_lock:
                        hits["quotes"] += 1
                    self._send_json(
                        200,
                        {
                            "status": "success",
                            "data": {
                                "ltp": 65100.0 + hits["quotes"],
                                "open": 65000.0,
                                "high": 65500.0,
                                "low": 64900.0,
                                "prev_close": 65000.0,
                                "bid": 65095.0,
                                "ask": 65105.0,
                                "volume": 1000 + hits["quotes"] * 10,
                            },
                        },
                    )
                elif path == "/api/v1/intervals":
                    self._send_json(
                        200,
                        {"status": "success", "data": {"minutes": ["1m", "5m"], "hours": ["1h"], "days": ["D"]}},
                    )
                elif path == "/api/v1/openposition":
                    self._send_json(200, {"status": "success", "quantity": 0, "mode": "analyze"})
                elif path == "/api/v1/orderbook":
                    self._send_json(200, {"status": "success", "data": {"orders": []}, "mode": "analyze"})
                elif path in ("/api/v1/tradebook", "/api/v1/positionbook"):
                    self._send_json(200, {"status": "success", "data": [], "mode": "analyze"})
                elif path == "/api/v1/placeorder":
                    self._send_json(200, {"status": "success", "orderid": "MOCK-OS-1", "mode": "analyze"})
                else:
                    self._send_json(200, {"status": "success", "data": {}})

            def _send_json(self, code: int, obj: dict[str, Any]) -> None:
                raw = json.dumps(obj).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

        httpd, _thread, port = _start_loopback_http_server(_CloudRefusalHandler)
        try:
            env = os.environ.copy()
            env.update(
                {
                    "PYTHONPATH": str(REPO_ROOT),
                    "OPENALGO_HOST": f"http://127.0.0.1:{port}",
                    "OPENALGO_API_KEY": "e2e-mock-key",
                    "OPENALGO_BROKER": "deltaexchange",
                    "OPENALGO_PROFILE": "cloud",
                }
            )
            proc = subprocess.run(
                [
                    sys.executable,
                    str(runner_script),
                    "--script",
                    "firstbar.oscript",
                    "--symbol",
                    "BTCUSDFUT",
                    "--exchange",
                    "CRYPTO",
                    "--interval",
                    "1m",
                    "--product",
                    "NRML",
                    "--poll-seconds",
                    "0.2",
                    "--cycles",
                    "3",
                ],
                cwd=str(tmp_path),
                env=env,
                capture_output=True,
                text=True,
                timeout=25.0,
            )
            combined = proc.stdout + "\n" + proc.stderr
            assert proc.returncode == 0, (
                f"OpenScript failed on HTTP {refusal_status} history refusal (exit={proc.returncode}):\n{combined}"
            )
            assert "History answered with no bars. Waiting." not in combined, (
                f"OpenScript stalled on HTTP {refusal_status} history refusal:\n{combined}"
            )
            assert hits["quotes"] >= 1, (
                f"OpenScript did not enter live-bar quote polling on HTTP {refusal_status}; hits={hits}\n{combined}"
            )
            assert hits["history"] <= 1, (
                f"OpenScript hammered blocked /api/v1/history repeatedly ({hits['history']} times) on {refusal_status}"
            )
        finally:
            httpd.shutdown()
            httpd.server_close()


# ===========================================================================
# E2E-F10: Runtime Stability & Storage Hygiene — Boundary/Negative (6 Tier 2 tests)
# ===========================================================================


@pytest.mark.tier2
def test_t2_f10_high_concurrency_3_broker_mixed_rw_burst_zero_500_zero_ctx_errors(
    server_factory,
) -> None:
    """High-concurrency 3-broker mixed read/write burst yields 0 HTTP 500s, 0 BrokerContextErrors, exact DB isolation."""
    server = server_factory(profile="cloud", paper_trading_only=True)

    order_counts_expected = {b: 0 for b in BROKERS}
    tasks: list[tuple[int, str, str]] = []
    for i in range(72):
        broker = BROKERS[i % len(BROKERS)]
        op = ("placeorder", "orderbook", "quotes", "funds", "positionbook", "intervals")[i % 6]
        if op == "placeorder":
            order_counts_expected[broker] += 1
        tasks.append((i, broker, op))

    def _run_one(item: tuple[int, str, str]) -> tuple[int, str, str, int]:
        idx, broker, op = item
        spec = DEFAULT_SYMBOLS[broker]
        if op == "placeorder":
            r = _place_paper_order(server, broker, action="BUY", quantity=1, strategy=f"t2_burst_{idx}")
        elif op == "quotes":
            r = server.api_post(
                "/api/v1/quotes",
                {"apikey": server.api_key, "symbol": spec["symbol"], "exchange": spec["exchange"]},
                broker=broker,
            )
        else:
            r = server.api_post(f"/api/v1/{op}", {"apikey": server.api_key}, broker=broker)
        return idx, broker, op, r.status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=18) as pool:
        results = list(pool.map(_run_one, tasks))

    non_200 = [r for r in results if r[3] != 200]
    assert non_200 == [], f"Non-200 responses under high concurrency: {non_200[:10]}"

    # Wait for background latency/apilog/analyzer/event_bus executors to flush
    time.sleep(2.0)
    err_lines = server.read_error_lines()
    ctx_errors = [e for e in err_lines if "BrokerContextError" in json.dumps(e)]
    assert ctx_errors == [], f"High-concurrency burst produced BrokerContextError storm: {ctx_errors[:5]}"

    for broker, expected in order_counts_expected.items():
        actual = _sqlite_count(server.broker_db_path(broker, "sandbox.db"), "sandbox_orders")
        assert actual == expected, (
            f"Per-broker sandbox_orders mismatch for {broker}: expected {expected}, got {actual}"
        )


@pytest.mark.tier2
@pytest.mark.timeout(180)
def test_t2_f10_oversized_preseeded_errors_jsonl_startup_non_oom_tail_bounded(
    server_factory, tmp_path: Path
) -> None:
    """25 MB pre-seeded errors.jsonl boots cleanly via tail-bounded read, preserving tail sentinel without OOM."""
    shared_root = tmp_path / "oversized_err_root"
    log_dir = shared_root / "log"
    log_dir.mkdir(parents=True, exist_ok=True)
    err_path = log_dir / "errors.jsonl"
    sentinel_msg = "E2E-TAIL-SENTINEL-KEEP-ME-25MB"
    _append_padding_jsonl(err_path, size_mb=25.0, sentinel=sentinel_msg)
    assert err_path.stat().st_size >= 24 * 1024 * 1024

    t0 = time.monotonic()
    server = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    boot_elapsed = time.monotonic() - t0
    assert boot_elapsed < 40.0, f"Oversized errors.jsonl startup took too long ({boot_elapsed:.1f}s)"

    ping = server.api_post("/api/v1/ping", {"apikey": server.api_key}, broker="fyers")
    assert ping.status_code == 200

    assert err_path.stat().st_size <= 11 * 1024 * 1024, (
        f"Active errors.jsonl was not tail-bounded on startup: {err_path.stat().st_size} bytes"
    )
    tail_text = err_path.read_text(encoding="utf-8", errors="replace")
    assert sentinel_msg in tail_text, "Tail-bounded startup truncation lost newest tail sentinel record!"

    rotated = sorted(log_dir.glob("errors.jsonl*"))
    assert len(rotated) <= 4, f"Expected <= 4 errors.jsonl* files, found {rotated}"
    assert sum(p.stat().st_size for p in rotated) <= 42 * 1024 * 1024


@pytest.mark.tier2
def test_t2_f10_errors_jsonl_rotating_file_handler_enforces_10mb_x_3_cap(server_factory) -> None:
    """Runtime ERROR writes trigger RotatingFileHandler rollover at 10 MB and keep at most 3 backups."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    err_path = server.errors_jsonl_path()

    with server.session_client("fyers") as cli:
        # Emit initial error so handler stream is opened
        r0 = cli.post(
            "/admin/api/errors/client",
            json={"level": "ERROR", "message": "e2e-rot-init"},
        )
        assert r0.status_code == 200, r0.text

        # Trigger 4 rollovers past 10 MB threshold
        for idx in range(1, 5):
            _append_padding_jsonl(err_path, size_mb=10.1)
            r = cli.post(
                "/admin/api/errors/client",
                json={"level": "ERROR", "message": f"e2e-rot-trigger-{idx}"},
            )
            assert r.status_code == 200, r.text
            time.sleep(0.15)

    assert (server.log_dir / "errors.jsonl.1").is_file(), (
        "RotatingFileHandler did not rotate errors.jsonl into errors.jsonl.1 after crossing 10 MB!"
    )
    assert not (server.log_dir / "errors.jsonl.4").exists(), (
        "RotatingFileHandler exceeded backupCount=3 (found errors.jsonl.4)!"
    )
    rotated = sorted(server.log_dir.glob("errors.jsonl*"))
    assert len(rotated) <= 4, f"Expected active + <=3 backups, found {rotated}"
    assert err_path.stat().st_size < 1 * 1024 * 1024, (
        f"Active errors.jsonl was not reset after rollover: {err_path.stat().st_size} bytes"
    )
    assert sum(p.stat().st_size for p in rotated) <= 42 * 1024 * 1024

    # Active errors.jsonl contains valid structured JSON for the latest error
    entries = server.read_error_lines()
    assert any("e2e-rot-trigger-4" in str(e.get("message", "")) for e in entries), entries[:5]
    for entry in entries:
        for field in ("ts", "level", "logger", "message"):
            assert field in entry, f"Malformed JSON error record after rotation: {entry}"


@pytest.mark.tier2
@pytest.mark.timeout(180)
def test_t2_f10_health_db_boundary_retention_47h_kept_vs_49h_pruned(
    server_factory, tmp_path: Path
) -> None:
    """Exact 2-day (48h) retention boundary: 47h-old rows are kept while 49h-old & 72h-old rows are pruned."""
    shared_root = tmp_path / "health_48h_boundary"
    srv1 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    srv1.stop()

    now = datetime.now(timezone.utc)
    ts_47h = (now - timedelta(hours=47)).strftime("%Y-%m-%d %H:%M:%S")
    ts_49h = (now - timedelta(hours=49)).strftime("%Y-%m-%d %H:%M:%S")
    ts_72h = (now - timedelta(hours=72)).strftime("%Y-%m-%d %H:%M:%S")

    for broker in BROKERS:
        hdb = shared_root / "db" / "brokers" / broker / "health.db"
        assert hdb.is_file(), f"Missing health.db for {broker}: {hdb}"
        with sqlite3.connect(str(hdb)) as conn:
            conn.execute(
                "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 470, 'pass')",
                (ts_47h,),
            )
            conn.execute(
                "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 490, 'pass')",
                (ts_49h,),
            )
            conn.execute(
                "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 720, 'pass')",
                (ts_72h,),
            )
            conn.commit()

    srv2 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    time.sleep(1.5)
    srv2.stop()

    for broker in BROKERS:
        hdb = shared_root / "db" / "brokers" / broker / "health.db"
        with sqlite3.connect(str(hdb)) as conn:
            kept = conn.execute("SELECT COUNT(*) FROM health_metrics WHERE fd_count = 470").fetchone()[0]
            pruned_49 = conn.execute("SELECT COUNT(*) FROM health_metrics WHERE fd_count = 490").fetchone()[0]
            pruned_72 = conn.execute("SELECT COUNT(*) FROM health_metrics WHERE fd_count = 720").fetchone()[0]
        assert kept == 1, f"{broker} wrongly pruned 47h-old row (inside 2-day retention)"
        assert pruned_49 == 0, f"{broker} failed to prune 49h-old row (outside 2-day retention)"
        assert pruned_72 == 0, f"{broker} failed to prune 72h-old row (outside 2-day retention)"


@pytest.mark.tier2
@pytest.mark.timeout(180)
def test_t2_f10_health_db_incremental_vacuum_compaction_all_three_brokers(
    server_factory, tmp_path: Path
) -> None:
    """All 3 broker health.db files use auto_vacuum=INCREMENTAL and reclaim disk space after aged-row pruning."""
    shared_root = tmp_path / "health_vac_all3"
    srv1 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    srv1.stop()

    old_ts = (datetime.now(timezone.utc) - timedelta(days=4)).strftime("%Y-%m-%d %H:%M:%S")
    heavy_td = json.dumps([{"id": i, "name": "worker-" + ("x" * 32), "alive": True} for i in range(40)])
    sizes_before: dict[str, int] = {}

    for broker in BROKERS:
        hdb = shared_root / "db" / "brokers" / broker / "health.db"
        with sqlite3.connect(str(hdb)) as conn:
            auto_vac = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0])
            assert auto_vac == 2, f"{broker} health.db PRAGMA auto_vacuum={auto_vac}, expected 2 (INCREMENTAL)"
            conn.executemany(
                "INSERT INTO health_metrics (timestamp, fd_count, thread_details, overall_status) "
                "VALUES (?, 88, ?, 'pass')",
                [(old_ts, heavy_td) for _ in range(350)],
            )
            conn.commit()
        sizes_before[broker] = hdb.stat().st_size

    srv2 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    time.sleep(1.5)
    srv2.stop()

    for broker in BROKERS:
        hdb = shared_root / "db" / "brokers" / broker / "health.db"
        size_after = hdb.stat().st_size
        with sqlite3.connect(str(hdb)) as conn:
            freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        assert size_after < sizes_before[broker], (
            f"{broker} health.db did not compact: before={sizes_before[broker]}, after={size_after}, freelist={freelist}"
        )


@pytest.mark.tier2
def test_t2_f10_health_db_thread_details_bounded_under_concurrency(server_factory) -> None:
    """Under concurrent load creating many threads, health_metrics.thread_details remains strictly bounded."""
    server = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"HEALTH_SAMPLE_INTERVAL": "1"},
    )

    def _ping_worker(i: int) -> int:
        broker = BROKERS[i % len(BROKERS)]
        return server.api_post("/api/v1/ping", {"apikey": server.api_key}, broker=broker).status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
        codes = list(pool.map(_ping_worker, range(40)))
    assert all(c == 200 for c in codes)

    with server.session_client("fyers") as cli:
        assert cli.get("/health/check").status_code == 200

    deadline = time.monotonic() + 8.0
    hdb = server.broker_db_path("fyers", "health.db")
    rows: list[Any] = []
    while time.monotonic() < deadline:
        if hdb.is_file():
            with sqlite3.connect(str(hdb)) as conn:
                rows = conn.execute(
                    "SELECT thread_details FROM health_metrics WHERE thread_details IS NOT NULL"
                ).fetchall()
            if rows:
                break
        time.sleep(0.3)

    assert rows, "Expected recorded health_metrics rows under concurrency"
    for (td_raw,) in rows:
        td_str = td_raw if isinstance(td_raw, str) else json.dumps(td_raw)
        assert len(td_str.encode("utf-8")) < 4096, (
            f"thread_details payload exceeded 4 KB bound under concurrency: {len(td_str)} bytes"
        )
        parsed = json.loads(td_str)
        if isinstance(parsed, list):
            assert len(parsed) <= 25, f"thread_details list not trimmed/bounded: {len(parsed)} entries"


# ===========================================================================
# E2E-F11: Production 3-Hour Monitoring Contract — Boundary/Negative (6 Tier 2 tests)
# ===========================================================================


@pytest.mark.tier2
@pytest.mark.timeout(180)
def test_t2_f11_monitor_single_expired_fyers_or_groww_token_yields_warn_not_critical(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """Revoking ONLY Fyers OR ONLY Groww daily OAuth token yields WARN (never CRITICAL) per Q7."""
    for target_broker in ("fyers", "groww"):
        server = server_factory(profile="cloud", paper_trading_only=True)
        proxy = ws_proxy_factory(server=server)

        with sqlite3.connect(str(server.broker_db_path(target_broker, "openalgo.db"))) as conn:
            conn.execute("UPDATE auth SET is_revoked = 1")
            conn.commit()

        out_jsonl = tmp_path / f"monitor-warn-{target_broker}.jsonl"
        _, records = _run_monitor_once(
            base_url=server.base_url,
            root_dir=server.root_dir,
            output_jsonl=out_jsonl,
            api_key=server.api_key,
            ws_port=proxy.ws_port,
        )
        assert len(records) == 1
        rec = records[0]
        assert rec["overall_status"] == "WARN", (
            f"Single expired {target_broker} token must yield WARN (not CRITICAL): {rec}"
        )
        severities = [str(a.get("severity", "")).upper() for a in rec["alerts"]]
        assert "WARN" in severities, rec["alerts"]
        assert "CRITICAL" not in severities, rec["alerts"]


@pytest.mark.tier2
@pytest.mark.timeout(180)
def test_t2_f11_monitor_paper_lock_breach_on_delta_or_groww_yields_critical(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """A paper-lock breach (analyze_mode=0) on any single broker (deltaexchange) immediately yields CRITICAL."""
    server = server_factory(profile="cloud", paper_trading_only=False)
    proxy = ws_proxy_factory(server=server)

    with sqlite3.connect(str(server.broker_db_path("deltaexchange", "openalgo.db"))) as conn:
        conn.execute("UPDATE settings SET analyze_mode = 0")
        conn.commit()

    out_jsonl = tmp_path / "monitor-delta-paper-crit.jsonl"
    _, records = _run_monitor_once(
        base_url=server.base_url,
        root_dir=server.root_dir,
        output_jsonl=out_jsonl,
        api_key=server.api_key,
        ws_port=proxy.ws_port,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["overall_status"] == "CRITICAL", f"Delta paper-lock breach must be CRITICAL: {rec}"
    assert rec["brokers"]["deltaexchange"].get("analyze_mode") is False
    assert any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec["alerts"])


@pytest.mark.tier2
@pytest.mark.timeout(180)
def test_t2_f11_monitor_history_block_breach_and_broker_context_error_storm_critical(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """Monitor escalates to CRITICAL both on history-block breach and on BrokerContextError entries in errors.jsonl."""
    # Sub-case A: History-block breach (full profile where /api/v1/history is open)
    srv_full = server_factory(profile="full", paper_trading_only=True)
    proxy_full = ws_proxy_factory(server=srv_full)
    _, recs_hist = _run_monitor_once(
        base_url=srv_full.base_url,
        root_dir=srv_full.root_dir,
        output_jsonl=tmp_path / "monitor-hist.jsonl",
        api_key=srv_full.api_key,
        ws_port=proxy_full.ws_port,
    )
    assert len(recs_hist) == 1
    assert recs_hist[0]["overall_status"] == "CRITICAL", recs_hist[0]
    assert recs_hist[0]["https"].get("history_blocked") is False

    # Sub-case B: Cloud server with recent BrokerContextError entries in errors.jsonl
    srv_ctx = server_factory(profile="cloud", paper_trading_only=True)
    proxy_ctx = ws_proxy_factory(server=srv_ctx)
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with srv_ctx.errors_jsonl_path().open("a", encoding="utf-8") as fh:
        for i in range(4):
            fh.write(
                json.dumps(
                    {
                        "ts": now_str,
                        "level": "ERROR",
                        "logger": "utils.latency_monitor",
                        "message": f"BrokerContextError: Enter a broker context (#{i})",
                    }
                )
                + "\n"
            )

    _, recs_ctx = _run_monitor_once(
        base_url=srv_ctx.base_url,
        root_dir=srv_ctx.root_dir,
        output_jsonl=tmp_path / "monitor-ctx.jsonl",
        api_key=srv_ctx.api_key,
        ws_port=proxy_ctx.ws_port,
    )
    assert len(recs_ctx) == 1
    rec_ctx = recs_ctx[0]
    assert rec_ctx["overall_status"] == "CRITICAL", (
        f"Recent BrokerContextError entries must escalate monitor to CRITICAL: {rec_ctx}"
    )
    assert any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec_ctx["alerts"])


@pytest.mark.tier2
def test_t2_f11_monitor_ws_port_down_yields_critical(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """When HTTP is healthy but WebSocket proxy port is closed/down, monitor reports CRITICAL."""
    dead_ws_port = _alloc_unused_port()
    out_jsonl = tmp_path / "monitor-ws-down.jsonl"

    _, records = _run_monitor_once(
        base_url=cloud_server.base_url,
        root_dir=cloud_server.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_server.api_key,
        ws_port=dead_ws_port,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["overall_status"] == "CRITICAL", f"Closed WS port must yield CRITICAL: {rec}"
    ws_svc = rec["services"].get("ws_port_8765")
    assert ws_svc in (False, "down", "inactive", "error", "closed") or not bool(ws_svc), rec["services"]
    assert any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec["alerts"])


@pytest.mark.tier2
def test_t2_f11_monitor_unreachable_http_service_down_and_resource_bounds_critical(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """Unreachable HTTP target never crashes monitor; emits valid 10-field CRITICAL JSONL with numeric resources."""
    dead_http_port = _alloc_unused_port()
    dead_ws_port = _alloc_unused_port()
    out_jsonl = tmp_path / "monitor-http-down.jsonl"

    proc, records = _run_monitor_once(
        base_url=f"http://127.0.0.1:{dead_http_port}",
        root_dir=cloud_server.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_server.api_key,
        ws_port=dead_ws_port,
    )
    assert proc.returncode in (0, 1, 2), (
        f"Monitor crashed with unhandled exception on unreachable target:\n{proc.stdout}\n{proc.stderr}"
    )
    assert len(records) == 1, f"Monitor must still append 1 JSONL record when target is down: {proc.stderr}"
    rec = records[0]
    for key in REQUIRED_MONITOR_TOP_KEYS:
        assert key in rec, f"Missing Contract #6 key '{key}' on unreachable target: {rec}"
    assert rec["overall_status"] == "CRITICAL", rec
    assert any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec["alerts"])

    # Verify resource metrics remain valid bounded numbers even when service is down
    res = rec["resources"]
    assert 0.0 <= float(res["disk_used_pct"]) <= 100.0
    assert float(res["disk_avail_gb"]) >= 0.0
    assert float(res["ram_avail_mb"]) >= 0.0
    assert float(res["swap_used_mb"]) >= 0.0


@pytest.mark.tier2
def test_t2_f11_monitor_sequential_runs_increment_check_number_and_survive_corrupt_tail(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """Sequential runs append valid 10-field JSONL with strictly incrementing check_number and survive corrupt lines."""
    proxy = ws_proxy_factory(server=cloud_server)
    out_jsonl = tmp_path / "monitor-seq.jsonl"

    for expected_n in (1, 2, 3):
        _, records = _run_monitor_once(
            base_url=cloud_server.base_url,
            root_dir=cloud_server.root_dir,
            output_jsonl=out_jsonl,
            api_key=cloud_server.api_key,
            ws_port=proxy.ws_port,
        )
        assert len(records) == expected_n, f"Expected {expected_n} records after run #{expected_n}"

    check_nums = [r["check_number"] for r in records]
    assert check_nums == [1, 2, 3], f"Expected strictly incrementing check_number [1,2,3], got {check_nums}"
    timestamps = [datetime.fromisoformat(str(r["timestamp_utc"]).replace("Z", "+00:00")) for r in records]
    assert timestamps[0] <= timestamps[1] <= timestamps[2]

    # Corner case: corrupt trailing JSONL line must not crash next run; next valid record appends cleanly
    with out_jsonl.open("a", encoding="utf-8") as fh:
        fh.write("\n{corrupt_trailing_line\n")

    proc4, recs4 = _run_monitor_once(
        base_url=cloud_server.base_url,
        root_dir=cloud_server.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_server.api_key,
        ws_port=proxy.ws_port,
    )
    assert proc4.returncode in (0, 1, 2), f"Monitor crashed on corrupt JSONL tail:\n{proc4.stdout}\n{proc4.stderr}"
    assert len(recs4) == 4, f"Expected 4 valid JSONL records after recovery run, got {len(recs4)}"
    assert recs4[-1]["check_number"] >= 4, f"check_number failed to advance after corrupt tail: {recs4[-1]}"
    for key in REQUIRED_MONITOR_TOP_KEYS:
        assert key in recs4[-1]
