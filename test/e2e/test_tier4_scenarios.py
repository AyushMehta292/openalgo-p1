"""Tier 4 E2E Real-World Multi-Step Application Scenarios (T4-S1 through T4-S7).

Implements all 7 end-to-end workloads specified in ``TEST_INFRA.md`` Section 4:
1. T4-S1 — Full Simulated Multi-Broker Cloud Paper Trading Day (Delta + Fyers + Groww)
2. T4-S2 — Live-Bar 5-EMA / 20-EMA Paper Strategy Lifecycle Under Cloud Lockout
3. T4-S3 — Full Local Mode (OPENALGO_PROFILE=full) Historical Download & Strategy Backtest
4. T4-S4 — Concurrent 3-Broker WebSocket Proxy Streaming + Order Update & Sandbox Fills
5. T4-S5 — Security & Safety Adversarial Assault Under Cloud Deployment
6. T4-S6 — High-Concurrency Multi-Broker Load + Storage Hygiene & Crash-Restart Recovery
7. T4-S7 — Production 3-Hour Monitoring Probe End-to-End (OK -> WARN -> CRITICAL Escalation)

Strict opaque-box discipline:
- Zero private-helper imports bypassing public boundaries.
- Zero skipped or expected-failure shortcuts; every scenario asserts its full contract.
- Zero vacuous/tautological assertions.
- Explicit assert_zero_real_broker_order_calls() and zero *.duckdb files in all cloud scenarios.
"""

from __future__ import annotations

import base64
import concurrent.futures
import hashlib
import json
import os
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

import httpx
import pytest
from websockets.sync.client import connect as ws_connect

from .conftest import (
    BROKERS,
    DEFAULT_SYMBOLS,
    REPO_ROOT,
    OpenAlgoE2EInstance,
    SessionClient,
    WsProxyE2EInstance,
)

pytestmark = pytest.mark.tier4

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
# Pure Opaque-Box Scenario Helpers
# ---------------------------------------------------------------------------


def _recent_date_window(days_back: int = 10) -> tuple[str, str]:
    end_d = date.today() - timedelta(days=1)
    start_d = end_d - timedelta(days=days_back)
    return start_d.isoformat(), end_d.isoformat()


def _assert_paper_locked_response(resp: httpx.Response, context: str = "") -> dict[str, Any]:
    assert resp.status_code == 403, (
        f"Expected 403 PAPER_TRADING_ONLY{f' ({context})' if context else ''}, "
        f"got {resp.status_code}: {resp.text[:400]}"
    )
    body = resp.json()
    assert body.get("status") == "error", f"Unexpected body ({context}): {body}"
    assert body.get("code") == "PAPER_TRADING_ONLY", (
        f"Expected code=PAPER_TRADING_ONLY ({context}), got: {body}"
    )
    assert isinstance(body.get("message"), str) and body["message"].strip(), f"Missing message: {body}"
    return body


def _assert_historical_disabled_response(
    resp: httpx.Response,
    *,
    allowed_statuses: tuple[int, ...] = (403, 503),
    context: str = "",
) -> dict[str, Any]:
    assert resp.status_code in allowed_statuses, (
        f"Expected status in {allowed_statuses} for HISTORICAL_DATA_DISABLED"
        f"{f' ({context})' if context else ''}, got {resp.status_code}: {resp.text[:400]}"
    )
    body = resp.json()
    assert body.get("status") == "error", f"Unexpected body ({context}): {body}"
    assert body.get("code") == "HISTORICAL_DATA_DISABLED", (
        f"Expected code=HISTORICAL_DATA_DISABLED ({context}), got: {body}"
    )
    assert isinstance(body.get("message"), str) and body["message"].strip(), f"Missing message: {body}"
    return body


def _place_paper_order(
    server: OpenAlgoE2EInstance,
    broker: str,
    *,
    symbol: str | None = None,
    exchange: str | None = None,
    product: str | None = None,
    action: str = "BUY",
    quantity: int = 1,
    pricetype: str = "MARKET",
    price: float = 0.0,
    trigger_price: float = 0.0,
    strategy: str = "e2e_t4_strat",
) -> dict[str, Any]:
    spec = DEFAULT_SYMBOLS[broker]
    resp = server.api_post(
        "/api/v1/placeorder",
        {
            "apikey": server.api_key,
            "strategy": strategy,
            "exchange": exchange or spec["exchange"],
            "symbol": symbol or spec["symbol"],
            "action": action,
            "quantity": quantity,
            "pricetype": pricetype,
            "product": product or spec["product"],
            "price": price,
            "trigger_price": trigger_price,
        },
        broker=broker,
    )
    assert resp.status_code == 200, f"[{broker}] placeorder failed: {resp.status_code} {resp.text[:400]}"
    body = resp.json()
    assert body.get("status") == "success", f"[{broker}] unexpected placeorder body: {body}"
    assert body.get("mode") == "analyze", f"[{broker}] expected mode='analyze': {body}"
    assert body.get("orderid"), f"[{broker}] missing orderid: {body}"
    return body


def _order_status(server: OpenAlgoE2EInstance, broker: str, orderid: str) -> str:
    ob = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker=broker)
    assert ob.status_code == 200, f"[{broker}] orderbook failed: {ob.text[:300]}"
    orders = ob.json()["data"]["orders"]
    row = next((o for o in orders if str(o.get("orderid")) == str(orderid)), None)
    assert row is not None, f"[{broker}] orderid {orderid} not found in orderbook: {orders}"
    return str(row.get("order_status", "")).lower()


def _wait_orders_complete(
    server: OpenAlgoE2EInstance,
    targets: list[tuple[str, str]],
    timeout: float = 20.0,
) -> dict[tuple[str, str], str]:
    deadline = time.monotonic() + timeout
    statuses: dict[tuple[str, str], str] = {}
    while time.monotonic() < deadline:
        for broker, oid in targets:
            statuses[(broker, oid)] = _order_status(server, broker, oid)
        if all(st == "complete" for st in statuses.values()):
            return statuses
        time.sleep(0.4)
    return statuses


def _create_signal_strategy(client: SessionClient, name: str = "E2E T4 Probe") -> int:
    resp = client.post(
        "/strategy/api/strategies",
        json={
            "name": name,
            "strategy_kind": "signal",
            "direction": "both",
            "underlying": "SBIN",
            "underlying_exchange": "NSE",
            "strategy_type": "positional",
            "product": "NRML",
            "pricetype": "MARKET",
            "legs": [
                {
                    "id": 1,
                    "symbol": "SBIN",
                    "exchange": "NSE",
                    "side": "both",
                    "segment": "cash",
                    "qty_mode": "units",
                    "qty": 1,
                }
            ],
        },
    )
    assert resp.status_code == 201, f"Failed to create strategy: {resp.status_code} {resp.text[:400]}"
    sid = resp.json().get("data", {}).get("id")
    assert isinstance(sid, int), f"Expected integer strategy id: {resp.json()}"
    return sid


def _mint_mcp_bearer_token(
    instance: OpenAlgoE2EInstance,
    *,
    scope: str = "read:market read:account write:orders",
    broker: str = "fyers",
) -> str:
    redirect_uri = "http://127.0.0.1:9876/callback"
    reg_resp = instance.api_post(
        "/oauth/register",
        payload={
            "client_name": "E2E T4 MCP Probe",
            "redirect_uris": [redirect_uri],
            "scope": scope,
            "token_endpoint_auth_method": "client_secret_post",
        },
    )
    assert reg_resp.status_code in (200, 201), f"MCP DCR failed: {reg_resp.status_code} {reg_resp.text[:300]}"
    reg = reg_resp.json()
    client_id = reg["client_id"]
    client_secret = reg.get("client_secret", "")

    verifier = secrets.token_urlsafe(48)
    challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("ascii")).digest())
        .rstrip(b"=")
        .decode("ascii")
    )
    state = secrets.token_urlsafe(12)
    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    with instance.session_client(broker=broker) as session:
        assert session.get("/oauth/authorize", params=auth_params).status_code == 200
        approve_resp = session.post("/oauth/authorize", params=auth_params, data={"decision": "approve"})
        assert approve_resp.status_code in (302, 303), approve_resp.text[:300]
    location = approve_resp.headers.get("location", "")
    code = parse_qs(urlparse(location).query)["code"][0]

    token_resp = httpx.post(
        f"{instance.base_url}/oauth/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "code_verifier": verifier,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Accept": "application/json"},
        timeout=15.0,
    )
    assert token_resp.status_code == 200, token_resp.text[:300]
    token = token_resp.json().get("access_token")
    assert isinstance(token, str) and token
    return token


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


def _discover_live_strategy_entry(broker: str | None = None) -> Path:
    live_dir = REPO_ROOT / "strategies" / "live"
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
            if any(tok in candidate.stem.lower() for tok in tokens):
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
    cycles: int = 4,
    poll_seconds: float = 0.2,
    backtest: bool = False,
) -> list[str]:
    help_out = _probe_cli_help(script_path)
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
    if backtest:
        assert "--backtest" in help_out, (
            f"Live strategy CLI {script_path} must expose --backtest flag per Interface Contract #5."
        )
        cmd.append("--backtest")
    else:
        for flag in ("--cycles", "--max-cycles", "--iterations"):
            if flag in help_out:
                cmd.extend([flag, str(cycles)])
                break
        else:
            if "--once" in help_out:
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
        for line in output_jsonl.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return proc, records


# ===========================================================================
# T4-S1: Full Simulated Multi-Broker Cloud Paper Trading Day
# ===========================================================================


@pytest.mark.timeout(180)
def test_t4_s1_full_simulated_multibroker_cloud_paper_trading_day(server_factory) -> None:
    """T4-S1: Full simulated trading day across Delta (CRYPTO), Fyers (NSE), and Groww (NSE) under cloud+paper lock."""
    srv = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"SANDBOX_ENGINE_TYPE": "polling", "SANDBOX_POLL_INTERVAL": "1"},
    )

    # Step 1: Verify /auth/app-config and initial paper lock across all 3 brokers
    cfg_resp = srv.api_get("/auth/app-config")
    assert cfg_resp.status_code == 200, cfg_resp.text[:300]
    cfg = cfg_resp.json()
    assert cfg.get("profile") == "cloud"
    assert cfg.get("historical_data_enabled") is False
    assert cfg.get("paper_trading_only") is True
    assert set(BROKERS).issubset({str(b).lower() for b in cfg.get("brokers", [])})

    for broker in BROKERS:
        an = srv.api_post("/api/v1/analyzer", {"apikey": srv.api_key}, broker=broker).json()
        assert an["data"]["analyze_mode"] is True

    # Step 2: Set opening quotes with wide intraday high/low bounds so stale-quote guard stays happy
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0, low=62000.0, high=69000.0)
    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0, low=750.0, high=860.0)
    srv.set_mock_quote("groww", "NSE", "SBIN", ltp=800.0, low=750.0, high=860.0)

    # Step 3: Place concurrent MARKET, LIMIT, and SL paper orders across all 3 brokers
    def _place_broker_day_orders(broker: str) -> dict[str, str]:
        if broker == "deltaexchange":
            mkt = _place_paper_order(srv, broker, action="BUY", quantity=2, pricetype="MARKET", strategy="t4_s1_mkt")
            lim = _place_paper_order(
                srv, broker, action="BUY", quantity=1, pricetype="LIMIT", price=64000.0, strategy="t4_s1_lim"
            )
            sl = _place_paper_order(
                srv,
                broker,
                action="BUY",
                quantity=1,
                pricetype="SL",
                trigger_price=66500.0,
                price=66600.0,
                strategy="t4_s1_sl",
            )
        elif broker == "fyers":
            mkt = _place_paper_order(srv, broker, action="BUY", quantity=5, pricetype="MARKET", strategy="t4_s1_mkt")
            lim = _place_paper_order(
                srv, broker, action="BUY", quantity=3, pricetype="LIMIT", price=790.0, strategy="t4_s1_lim"
            )
            sl = _place_paper_order(
                srv,
                broker,
                action="BUY",
                quantity=2,
                pricetype="SL",
                trigger_price=820.0,
                price=822.0,
                strategy="t4_s1_sl",
            )
        else:
            mkt = _place_paper_order(srv, broker, action="BUY", quantity=4, pricetype="MARKET", strategy="t4_s1_mkt")
            lim = _place_paper_order(
                srv, broker, action="BUY", quantity=2, pricetype="LIMIT", price=788.0, strategy="t4_s1_lim"
            )
            sl = _place_paper_order(
                srv,
                broker,
                action="BUY",
                quantity=2,
                pricetype="SL",
                trigger_price=818.0,
                price=820.0,
                strategy="t4_s1_sl",
            )
        return {"mkt": str(mkt["orderid"]), "lim": str(lim["orderid"]), "sl": str(sl["orderid"])}

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        day_orders = dict(zip(BROKERS, pool.map(_place_broker_day_orders, BROKERS)))

    for broker in BROKERS:
        assert _order_status(srv, broker, day_orders[broker]["mkt"]) == "complete"
        assert _order_status(srv, broker, day_orders[broker]["lim"]) == "open"
        assert _order_status(srv, broker, day_orders[broker]["sl"]) == "trigger pending"

    # Step 4: Dip quotes to fill all 3 LIMIT orders concurrently
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=63800.0, low=62000.0, high=69000.0)
    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=788.0, low=750.0, high=860.0)
    srv.set_mock_quote("groww", "NSE", "SBIN", ltp=785.0, low=750.0, high=860.0)

    lim_statuses = _wait_orders_complete(srv, [(b, day_orders[b]["lim"]) for b in BROKERS], timeout=20.0)
    assert all(st == "complete" for st in lim_statuses.values()), f"LIMIT orders failed to fill: {lim_statuses}"

    # Step 5: Rally quotes to trigger & fill all 3 SL orders concurrently, generating positive unrealized PnL
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=66550.0, low=62000.0, high=69000.0)
    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=821.0, low=750.0, high=860.0)
    srv.set_mock_quote("groww", "NSE", "SBIN", ltp=819.0, low=750.0, high=860.0)

    sl_statuses = _wait_orders_complete(srv, [(b, day_orders[b]["sl"]) for b in BROKERS], timeout=20.0)
    assert all(st == "complete" for st in sl_statuses.values()), f"SL orders failed to fill: {sl_statuses}"

    # Step 6: Book partial realized profit on each broker via reverse MARKET SELL
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=67500.0, low=62000.0, high=69000.0)
    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=840.0, low=750.0, high=860.0)
    srv.set_mock_quote("groww", "NSE", "SBIN", ltp=838.0, low=750.0, high=860.0)

    exit_qtys = {"deltaexchange": 2, "fyers": 5, "groww": 4}
    expected_remaining = {"deltaexchange": 2, "fyers": 5, "groww": 4}
    for broker in BROKERS:
        exit_ord = _place_paper_order(
            srv, broker, action="SELL", quantity=exit_qtys[broker], pricetype="MARKET", strategy="t4_s1_exit"
        )
        assert _order_status(srv, broker, str(exit_ord["orderid"])) == "complete"

    # Step 7: Verify per-broker DB isolation, PnL accounting, zero DuckDB, zero BrokerContextError, bounded disk
    sandbox_inodes: set[int] = set()
    for broker in BROKERS:
        sb_db = srv.broker_db_path(broker, "sandbox.db")
        assert sb_db.is_file(), f"Missing sandbox.db for {broker}"
        sandbox_inodes.add(sb_db.stat().st_ino)

        ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker=broker).json()["data"]["orders"]
        assert len(ob) == 4, f"[{broker}] expected 4 orders (mkt+lim+sl+exit), got {len(ob)}"
        own_oids = {str(o["orderid"]) for o in ob}
        for other_b in BROKERS:
            if other_b != broker:
                assert day_orders[other_b]["mkt"] not in own_oids

        tb = srv.api_post("/api/v1/tradebook", {"apikey": srv.api_key}, broker=broker).json()["data"]
        assert len(tb) == 4, f"[{broker}] expected 4 trades, got {len(tb)}"

        pb = srv.api_post("/api/v1/positionbook", {"apikey": srv.api_key}, broker=broker).json()["data"]
        assert any(
            p["symbol"] == DEFAULT_SYMBOLS[broker]["symbol"]
            and int(float(p["quantity"])) == expected_remaining[broker]
            for p in pb
        ), f"[{broker}] unexpected remaining position: {pb}"

        funds = srv.api_post("/api/v1/funds", {"apikey": srv.api_key}, broker=broker).json()["data"]
        assert float(funds["availablecash"]) > 0.0
        assert float(funds.get("total_realized_pnl", funds.get("m2mrealized", 0.0))) != 0.0 or float(
            funds.get("m2munrealized", 0.0)
        ) != 0.0, f"[{broker}] expected non-zero PnL after profitable price rally: {funds}"

    assert len(sandbox_inodes) == 3, "Each broker must have its own distinct sandbox.db file"
    assert srv.duckdb_files() == [], f"Cloud trading day must create ZERO DuckDB files: {srv.duckdb_files()}"

    time.sleep(1.0)
    ctx_errs = [e for e in srv.read_error_lines() if "BrokerContextError" in json.dumps(e)]
    assert ctx_errs == [], f"Found BrokerContextError during simulated day: {ctx_errs[:3]}"

    disk_bytes = sum(p.stat().st_size for p in srv.root_dir.rglob("*") if p.is_file())
    assert disk_bytes < 50 * 1024 * 1024, f"Unexpectedly large cloud disk footprint: {disk_bytes} bytes"
    srv.assert_zero_real_broker_order_calls()


# ===========================================================================
# T4-S2: Live-Bar 5-EMA / 20-EMA Paper Strategy Lifecycle Under Cloud Lockout
# ===========================================================================


@pytest.mark.timeout(180)
def test_t4_s2_live_bar_ema_strategy_lifecycle_under_cloud_lockout(
    server_factory, tmp_path: Path
) -> None:
    """T4-S2: Tracked strategies/live/ engine aggregates live quotes under cloud lockout, persists <10KB state, recovers on restart."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    start_d, end_d = _recent_date_window(5)

    # Step 1: Confirm /api/v1/history is hard-blocked in cloud
    hist_resp = srv.api_post(
        "/api/v1/history",
        {
            "apikey": srv.api_key,
            "symbol": "BTCUSDFUT",
            "exchange": "CRYPTO",
            "interval": "1m",
            "start_date": start_d,
            "end_date": end_d,
        },
        broker="deltaexchange",
    )
    _assert_historical_disabled_response(hist_resp, allowed_statuses=(403,), context="T4-S2 cloud history lockout")

    entry = _discover_live_strategy_entry("deltaexchange")
    state_file = tmp_path / "t4_s2_delta_ema.state.json"

    # Step 2: Feed deterministic rising-then-reversing quote sequence while live strategy runs
    quote_sequence = [65000.0, 65150.0, 65320.0, 65500.0, 65420.0, 65180.0, 64950.0, 64700.0]
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=quote_sequence[0], low=64000.0, high=66500.0)

    stop_feeder = threading.Event()

    def _quote_feeder() -> None:
        for px in quote_sequence[1:]:
            if stop_feeder.wait(timeout=0.2):
                break
            srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=px, low=64000.0, high=66500.0)

    feeder = threading.Thread(target=_quote_feeder, daemon=True)
    feeder.start()

    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(REPO_ROOT),
            "OPENALGO_HOST": srv.base_url,
            "OPENALGO_API_KEY": srv.api_key,
            "OPENALGO_BROKER": "deltaexchange",
            "OPENALGO_PROFILE": "cloud",
        }
    )

    cmd_run1 = _build_live_strategy_cmd(
        entry,
        broker="deltaexchange",
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        state_file=state_file,
        cycles=6,
        poll_seconds=0.15,
    )
    try:
        proc1 = subprocess.run(cmd_run1, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=35.0)
    finally:
        stop_feeder.set()
        feeder.join(timeout=3.0)

    out1 = proc1.stdout + "\n" + proc1.stderr
    assert proc1.returncode == 0, f"T4-S2 live strategy run 1 failed (rc={proc1.returncode}):\n{out1}"
    assert "HISTORICAL_DATA_DISABLED" not in out1.upper(), f"Strategy attempted /api/v1/history in cloud:\n{out1}"

    # Step 3: Inspect atomic .state.json (< 10 KB, rolling bar cap, valid JSON, no dangling .tmp)
    candidates = [state_file] if state_file.is_file() else sorted(tmp_path.rglob("*.state.json"))
    assert candidates, f"Expected atomic .state.json under {tmp_path}"
    persisted = candidates[0]
    size_run1 = persisted.stat().st_size
    assert 0 < size_run1 < 10 * 1024, f".state.json exceeded 10 KB bound: {size_run1} bytes"
    assert not list(persisted.parent.glob("*.tmp")), "Dangling .tmp file found after atomic write"

    state_run1 = json.loads(persisted.read_text(encoding="utf-8"))
    assert isinstance(state_run1, dict)
    keys_lower = {k.lower() for k in state_run1.keys()}
    assert any("bar" in k or "candle" in k for k in keys_lower), f"Missing rolling bars key: {state_run1.keys()}"
    for k, v in state_run1.items():
        if isinstance(v, list) and ("bar" in k.lower() or "candle" in k.lower()):
            assert len(v) <= 100, f"Rolling bars unbounded ({len(v)} entries) in {k}"

    # Step 4: Restart strategy pointing to the SAME .state.json and verify state recovery
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=64600.0, low=64000.0, high=66500.0)
    cmd_run2 = _build_live_strategy_cmd(
        entry,
        broker="deltaexchange",
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        state_file=persisted,
        cycles=3,
        poll_seconds=0.15,
    )
    proc2 = subprocess.run(cmd_run2, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    out2 = proc2.stdout + "\n" + proc2.stderr
    assert proc2.returncode == 0, f"T4-S2 restart recovery run 2 failed (rc={proc2.returncode}):\n{out2}"
    assert "HISTORICAL_DATA_DISABLED" not in out2.upper(), out2

    size_run2 = persisted.stat().st_size
    assert 0 < size_run2 < 10 * 1024, f"Post-restart .state.json exceeded 10 KB bound: {size_run2} bytes"
    state_run2 = json.loads(persisted.read_text(encoding="utf-8"))
    assert isinstance(state_run2, dict) and len(state_run2) > 0

    # Step 5: Verify zero /api/v1/history requests recorded in Delta logs.db and zero DuckDB files
    logs_db = srv.broker_db_path("deltaexchange", "logs.db")
    if logs_db.is_file():
        with sqlite3.connect(str(logs_db)) as conn:
            tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
            for tbl in tables:
                cols = [c[1] for c in conn.execute(f"PRAGMA table_info({tbl})").fetchall()]
                if "endpoint" in cols:
                    hist_hits = int(
                        conn.execute(f"SELECT COUNT(*) FROM {tbl} WHERE endpoint LIKE '%/history%'").fetchone()[0]
                    )
                    # Only our own Step 1 pre-check call (at most 1) may exist — zero from strategy runs
                    assert hist_hits <= 1, f"Strategy made unexpected /history calls: count={hist_hits}"

    assert srv.duckdb_files() == [], f"Zero DuckDB files allowed in cloud: {srv.duckdb_files()}"
    srv.assert_zero_real_broker_order_calls()


# ===========================================================================
# T4-S3: Full Local Mode Historical Download & Strategy Backtest Workflow
# ===========================================================================


def test_t4_s3_full_local_mode_historical_download_and_backtest(
    full_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """T4-S3: Full local profile enables /history, /ticker, Historify, portfolio/SIP backtests, and --backtest CLI while keeping PAPER_TRADING_ONLY=true."""
    start_d, end_d = _recent_date_window(25)

    # Step 1: Verify /auth/app-config enables history while keeping paper lock enabled
    cfg = full_server.api_get("/auth/app-config")
    assert cfg.status_code == 200, cfg.text[:300]
    cfg_body = cfg.json()
    assert cfg_body.get("profile") == "full"
    assert cfg_body.get("historical_data_enabled") is True
    assert cfg_body.get("paper_trading_only") is True

    # Step 2: Fetch historical candles via POST /api/v1/history and GET /api/v1/ticker/<sym>
    hist_resp = full_server.api_post(
        "/api/v1/history",
        {
            "apikey": full_server.api_key,
            "symbol": "SBIN",
            "exchange": "NSE",
            "interval": "D",
            "start_date": start_d,
            "end_date": end_d,
        },
        broker="fyers",
    )
    assert hist_resp.status_code == 200, f"/api/v1/history failed in full mode: {hist_resp.text[:300]}"
    hist_body = hist_resp.json()
    assert hist_body.get("status") == "success" and len(hist_body.get("data", [])) > 0, hist_body

    ticker_resp = full_server.api_get(
        "/api/v1/ticker/NSE:SBIN",
        broker="fyers",
        query={"apikey": full_server.api_key, "interval": "D", "from": start_d, "to": end_d, "format": "json"},
    )
    assert ticker_resp.status_code == 200, f"/api/v1/ticker failed in full mode: {ticker_resp.text[:300]}"
    assert "HISTORICAL_DATA_DISABLED" not in ticker_resp.text

    # Step 3: Exercise Historify, Portfolio, SIP, and Sector Rotation endpoints in full mode
    with full_server.session_client("fyers") as cli:
        wl_resp = cli.get("/historify/api/watchlist")
        assert wl_resp.status_code == 200, f"Historify watchlist failed: {wl_resp.status_code} {wl_resp.text[:300]}"
        stats_resp = cli.get("/historify/api/stats")
        assert stats_resp.status_code == 200, f"Historify stats failed: {stats_resp.status_code} {stats_resp.text[:300]}"

    bench_resp = full_server.api_get(
        "/api/v1/portfolio/benchmarks", broker="fyers", query={"apikey": full_server.api_key}
    )
    assert bench_resp.status_code == 200, f"Portfolio benchmarks failed: {bench_resp.text[:300]}"

    sip_freq = full_server.api_get("/api/v1/sip/frequencies", broker="fyers")
    assert sip_freq.status_code == 200, f"SIP frequencies failed: {sip_freq.text[:300]}"

    sec_opt = full_server.api_get("/api/v1/sectorrotation/options", broker="fyers")
    assert sec_opt.status_code == 200, f"Sector rotation options failed: {sec_opt.text[:300]}"

    pb_resp = full_server.api_post(
        "/api/v1/portfolio/backtest",
        {
            "apikey": full_server.api_key,
            "holdings": [{"symbol": "SBIN", "exchange": "NSE", "weight": 100}],
            "start_date": start_d,
            "end_date": end_d,
            "source": "api",
        },
        broker="fyers",
    )
    assert pb_resp.status_code == 200, f"Portfolio backtest failed in full mode: {pb_resp.status_code} {pb_resp.text[:400]}"
    assert "HISTORICAL_DATA_DISABLED" not in pb_resp.text

    sip_resp = full_server.api_post(
        "/api/v1/sip/backtest",
        {
            "apikey": full_server.api_key,
            "symbol": "SBIN",
            "exchange": "NSE",
            "start_date": start_d,
            "end_date": end_d,
            "amount": 10000,
            "frequency": "weekly",
            "source": "api",
        },
        broker="fyers",
    )
    assert sip_resp.status_code == 200, f"SIP backtest failed in full mode: {sip_resp.status_code} {sip_resp.text[:400]}"
    assert "HISTORICAL_DATA_DISABLED" not in sip_resp.text

    # Step 4: Execute strategies/live/ --backtest CLI end-to-end
    entry = _discover_live_strategy_entry("fyers")
    bt_cmd = _build_live_strategy_cmd(
        entry,
        broker="fyers",
        symbol="SBIN",
        exchange="NSE",
        state_file=tmp_path / "t4_s3_bt.state.json",
        backtest=True,
    )
    bt_env = os.environ.copy()
    bt_env.update(
        {
            "PYTHONPATH": str(REPO_ROOT),
            "OPENALGO_HOST": full_server.base_url,
            "OPENALGO_API_KEY": full_server.api_key,
            "OPENALGO_BROKER": "fyers",
            "OPENALGO_PROFILE": "full",
        }
    )
    bt_proc = subprocess.run(bt_cmd, cwd=str(tmp_path), env=bt_env, capture_output=True, text=True, timeout=30.0)
    bt_out = bt_proc.stdout + "\n" + bt_proc.stderr
    assert bt_proc.returncode == 0, f"T4-S3 --backtest CLI failed (rc={bt_proc.returncode}):\n{bt_out}"
    assert any(tok in bt_out.lower() for tok in ("backtest", "trades", "pnl", "return", "bars")), bt_out

    # Step 5: PAPER_TRADING_ONLY=true remains strictly enforced in local full mode
    _assert_paper_locked_response(
        full_server.api_post(
            "/api/v1/analyzer/toggle", {"apikey": full_server.api_key, "mode": False}, broker="fyers"
        ),
        context="T4-S3 full mode paper lock",
    )
    full_server.assert_zero_real_broker_order_calls()


# ===========================================================================
# T4-S4: Concurrent 3-Broker WS Proxy Streaming + Order Update & Sandbox Fills
# ===========================================================================


@pytest.mark.timeout(180)
def test_t4_s4_concurrent_three_broker_ws_proxy_and_sandbox_fills(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """T4-S4: Concurrent Delta/Fyers/Groww WS clients authenticate, subscribe LTP/Quote/Depth + orders, and receive ticks & sandbox fills without eviction."""
    srv = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"SANDBOX_ENGINE_TYPE": "polling", "SANDBOX_POLL_INTERVAL": "1"},
    )
    proxy = ws_proxy_factory(server=srv)

    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0, low=63000.0, high=67000.0)
    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0, low=770.0, high=830.0)
    srv.set_mock_quote("groww", "NFO", "NIFTY27MAR25FUT", ltp=24000.0, low=23500.0, high=24500.0)

    specs = {
        "deltaexchange": ("CRYPTO", "BTCUSDFUT", "NRML", "LTP", 64000.0, 63850.0, 1),
        "fyers": ("NSE", "SBIN", "CNC", "Quote", 790.0, 788.0, 2),
        "groww": ("NFO", "NIFTY27MAR25FUT", "NRML", "Depth", 23850.0, 23800.0, 75),
    }

    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws_d, ws_connect(
        proxy.ws_url, open_timeout=5.0
    ) as ws_f, ws_connect(proxy.ws_url, open_timeout=5.0) as ws_g:
        sockets = {"deltaexchange": ws_d, "fyers": ws_f, "groww": ws_g}
        user_ids: dict[str, str] = {}

        # Step 1 & 2: Authenticate all 3 concurrently and subscribe to market data + order updates
        for broker, sock in sockets.items():
            ack = _ws_authenticate(sock, srv.api_key, broker=broker)
            assert ack.get("status") == "success", f"WS auth failed for {broker}: {ack}"
            assert str(ack.get("broker", "")).lower() == broker, ack
            user_ids[broker] = str(ack.get("user_id") or srv.username)

            exch, sym, _, mode, _, _, _ = specs[broker]
            sock.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "symbols": [{"symbol": sym, "exchange": exch}],
                        "mode": mode,
                        "request_id": f"t4s4-sub-{broker}",
                    }
                )
            )
            assert _ws_recv_matching(sock, lambda m: m.get("type") == "subscribe").get("status") == "success"

            sock.send(json.dumps({"action": "subscribe_orders", "request_id": f"t4s4-ord-{broker}"}))
            assert _ws_recv_matching(sock, lambda m: m.get("type") == "subscribe_orders").get("status") == "success"

        # Step 3: Verify ZERO eviction across all 3 sockets via ping/pong
        for broker, sock in sockets.items():
            sock.send(json.dumps({"action": "ping", "_pingId": f"t4s4-ping-{broker}"}))
            pong = _ws_recv_matching(sock, lambda m: m.get("type") == "pong")
            assert pong.get("_pingId") == f"t4s4-ping-{broker}"

        # Step 4: Place resting paper LIMIT orders on all 3 brokers
        limit_oids: dict[str, str] = {}
        for broker, (exch, sym, prod, _, lim_px, _, qty) in specs.items():
            placed = _place_paper_order(
                srv,
                broker,
                symbol=sym,
                exchange=exch,
                product=prod,
                action="BUY",
                quantity=qty,
                pricetype="LIMIT",
                price=lim_px,
                strategy=f"t4_s4_{broker}",
            )
            limit_oids[broker] = str(placed["orderid"])
            assert _order_status(srv, broker, limit_oids[broker]) == "open"

        # Step 5: Move quotes across limit prices AND publish ZMQ market ticks for all 3 brokers
        for broker, (exch, sym, _, mode, _, fill_px, _) in specs.items():
            srv.set_mock_quote(broker, exch, sym, ltp=fill_px)
            for _ in range(2):
                proxy.publish_tick(exch, sym, mode.upper(), {"symbol": sym, "exchange": exch, "ltp": fill_px})
                time.sleep(0.08)

        # Verify every WS client receives its broker-tagged market tick
        for broker, sock in sockets.items():
            _, sym, _, _, _, fill_px, _ = specs[broker]
            tick = _ws_recv_matching(
                sock,
                lambda m, s=sym: m.get("type") == "market_data" and m.get("symbol") == s,
                timeout=6.0,
            )
            assert str(tick.get("broker", "")).lower() == broker
            assert float(tick.get("data", {}).get("ltp", 0.0)) == pytest.approx(fill_px, rel=1e-4)

        # Verify all 3 resting LIMIT orders fill concurrently in Sandbox without singleton clobber
        fill_statuses = _wait_orders_complete(srv, [(b, limit_oids[b]) for b in BROKERS], timeout=20.0)
        assert all(st == "complete" for st in fill_statuses.values()), f"Sandbox fills incomplete: {fill_statuses}"

        # Publish order_update notifications and verify clean fan-out on all 3 WS clients
        for broker, (exch, sym, _, _, _, _, qty) in specs.items():
            for _ in range(2):
                proxy.publish_order_update(
                    "ANALYZE",
                    user_ids[broker],
                    {
                        "type": "order_update",
                        "user_id": user_ids[broker],
                        "mode": "analyze",
                        "broker": broker,
                        "orderid": limit_oids[broker],
                        "symbol": sym,
                        "exchange": exch,
                        "action": "BUY",
                        "quantity": qty,
                        "order_status": "complete",
                    },
                )
                time.sleep(0.08)

        for broker, sock in sockets.items():
            oid = limit_oids[broker]
            evt = _ws_recv_matching(
                sock,
                lambda m, o=oid: m.get("type") == "order_update" and str(m.get("orderid")) == o,
                timeout=6.0,
            )
            assert str(evt.get("broker", "")).lower() == broker
            assert evt.get("mode") == "analyze"
            assert evt.get("order_status") == "complete"

    assert srv.duckdb_files() == []
    srv.assert_zero_real_broker_order_calls()


# ===========================================================================
# T4-S5: Security & Safety Adversarial Assault Under Cloud Deployment
# ===========================================================================


def test_t4_s5_security_and_safety_adversarial_assault_under_cloud(server_factory) -> None:
    """T4-S5: Exhaustive live-mode escalation + historical/backtest bypass assault under cloud+paper lock."""
    srv = server_factory(profile="cloud", paper_trading_only=True, mcp_http_enabled=True)
    start_d, end_d = _recent_date_window(7)

    # --- Assault Wave 1: Live-mode escalation across EVERY surface & all 3 brokers ---
    for broker in BROKERS:
        _assert_paper_locked_response(
            srv.api_post("/api/v1/analyzer/toggle", {"apikey": srv.api_key, "mode": False}, broker=broker),
            context=f"Wave1 RESTX analyzer/toggle {broker}",
        )
        with srv.session_client(broker) as cli:
            _assert_paper_locked_response(cli.post("/auth/analyzer-toggle"), context=f"Wave1 SPA toggle {broker}")
            _assert_paper_locked_response(cli.post("/settings/analyze-mode/0"), context=f"Wave1 settings/0 {broker}")

    with srv.session_client("fyers") as f_cli:
        sid = _create_signal_strategy(f_cli, name="T4-S5 Adversarial Strategy")
        _assert_paper_locked_response(
            f_cli.post(f"/strategy/api/strategies/{sid}/live", json={"enabled": True}),
            context="Wave1 Strategy Module /live",
        )
        _assert_paper_locked_response(
            f_cli.post(f"/strategy/api/strategies/{sid}/start", json={"mode": "live"}),
            context="Wave1 Strategy Module /start mode=live",
        )

    _assert_paper_locked_response(
        srv.api_post(
            "/api/v1/strategy/start",
            {"apikey": srv.api_key, "strategy_id": sid, "mode": "live"},
            broker="fyers",
        ),
        context="Wave1 RESTX /api/v1/strategy/start mode=live",
    )

    mcp_token = _mint_mcp_bearer_token(srv, scope="read:market read:account write:orders", broker="fyers")
    mcp_toggle = srv.api_post(
        "/mcp",
        payload={
            "jsonrpc": "2.0",
            "id": 101,
            "method": "tools/call",
            "params": {"name": "analyzer_toggle", "arguments": {"mode": False}},
        },
        broker="fyers",
        headers={"Authorization": f"Bearer {mcp_token}"},
    )
    if mcp_toggle.status_code == 403:
        _assert_paper_locked_response(mcp_toggle, context="Wave1 MCP analyzer_toggle")
    else:
        assert mcp_toggle.status_code == 200
        tog_rpc = mcp_toggle.json()
        assert bool(tog_rpc.get("error")) or bool(tog_rpc.get("result", {}).get("isError")), (
            f"Wave1 MCP analyzer_toggle must return JSON-RPC error / isError: {tog_rpc}"
        )
        assert "PAPER_TRADING_ONLY" in mcp_toggle.text, mcp_toggle.text[:400]

    # --- Assault Wave 2: Historical & backtesting bypass across RESTX, Blueprints, SPA & MCP ---
    _assert_historical_disabled_response(
        srv.api_post(
            "/api/v1/history",
            {
                "apikey": srv.api_key,
                "symbol": "SBIN",
                "exchange": "NSE",
                "interval": "D",
                "start_date": start_d,
                "end_date": end_d,
            },
            broker="fyers",
        ),
        allowed_statuses=(403,),
        context="Wave2 /api/v1/history",
    )
    _assert_historical_disabled_response(
        srv.api_get(
            "/api/v1/ticker/NSE:SBIN",
            broker="fyers",
            query={"apikey": srv.api_key, "interval": "D", "from": start_d, "to": end_d, "format": "json"},
        ),
        allowed_statuses=(403,),
        context="Wave2 /api/v1/ticker",
    )
    _assert_historical_disabled_response(
        srv.api_post("/api/v1/sectorrotation", {"apikey": srv.api_key}, broker="fyers"),
        allowed_statuses=(403, 503),
        context="Wave2 /api/v1/sectorrotation",
    )
    _assert_historical_disabled_response(
        srv.api_post(
            "/api/v1/portfolio/backtest",
            {
                "apikey": srv.api_key,
                "holdings": [{"symbol": "SBIN", "exchange": "NSE", "weight": 100}],
                "start_date": start_d,
                "end_date": end_d,
            },
            broker="fyers",
        ),
        allowed_statuses=(403, 503),
        context="Wave2 /api/v1/portfolio/backtest",
    )
    _assert_historical_disabled_response(
        srv.api_post(
            "/api/v1/sip/backtest",
            {
                "apikey": srv.api_key,
                "symbol": "SBIN",
                "exchange": "NSE",
                "start_date": start_d,
                "end_date": end_d,
                "amount": 5000,
            },
            broker="fyers",
        ),
        allowed_statuses=(403, 503),
        context="Wave2 /api/v1/sip/backtest",
    )

    with srv.session_client("fyers") as cli:
        for get_route in (
            "/scalping/api/history?symbol=SBIN&exchange=NSE&interval=5m",
            "/chart/test/api/history?symbol=SBIN&exchange=NSE&interval=D",
            "/historify",
            "/historify/api/stats",
            "/test_chart",
            "/pnl-tracker",
            "/ivchart",
            "/straddle",
            "/oiprofile",
            "/portfolio-backtester",
            "/sip-backtester",
        ):
            r = cli.get(get_route)
            _assert_historical_disabled_response(r, allowed_statuses=(403, 503), context=f"Wave2 GET {get_route}")

        r_pnl = cli.post("/pnltracker/api/pnl", json={"date": end_d})
        _assert_historical_disabled_response(r_pnl, allowed_statuses=(403, 503), context="Wave2 POST /pnltracker/api/pnl")

    for rpc_id, tool_name, args in (
        (
            201,
            "get_historical_data",
            {"symbol": "SBIN", "exchange": "NSE", "interval": "D", "start_date": start_d, "end_date": end_d},
        ),
        (
            202,
            "calculate_indicator",
            {"symbol": "SBIN", "exchange": "NSE", "interval": "D", "indicator": "ema", "period": 5},
        ),
    ):
        mcp_h = srv.api_post(
            "/mcp",
            payload={
                "jsonrpc": "2.0",
                "id": rpc_id,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": args},
            },
            broker="fyers",
            headers={"Authorization": f"Bearer {mcp_token}"},
        )
        if mcp_h.status_code in (403, 503):
            _assert_historical_disabled_response(mcp_h, context=f"Wave2 MCP {tool_name}")
        else:
            assert mcp_h.status_code == 200
            h_rpc = mcp_h.json()
            assert bool(h_rpc.get("error")) or bool(h_rpc.get("result", {}).get("isError")), (
                f"Wave2 MCP {tool_name} must return JSON-RPC error / isError: {h_rpc}"
            )
            assert "HISTORICAL_DATA_DISABLED" in mcp_h.text, mcp_h.text[:400]

    # --- Post-Assault Integrity Verification ---
    for broker in BROKERS:
        an = srv.api_post("/api/v1/analyzer", {"apikey": srv.api_key}, broker=broker).json()
        assert an["data"]["analyze_mode"] is True, f"[{broker}] analyze_mode mutated after assault: {an}"
        with srv.session_client(broker) as cli:
            assert cli.get("/auth/analyzer-mode").json()["data"]["analyze_mode"] is True
            assert cli.get("/settings/analyze-mode").json()["analyze_mode"] is True

    assert srv.duckdb_files() == [], f"DuckDB files created during assault: {srv.duckdb_files()}"
    srv.assert_zero_real_broker_order_calls()


# ===========================================================================
# T4-S6: High-Concurrency Multi-Broker Load + Storage Hygiene & Crash-Restart Recovery
# ===========================================================================


@pytest.mark.timeout(180)
def test_t4_s6_high_concurrency_multibroker_load_hygiene_and_restart_recovery(
    server_factory, tmp_path: Path
) -> None:
    """T4-S6: Concurrent 3-broker burst + >10MB errors.jsonl rotation + aged health.db pruning + restart position persistence."""
    shared_root = tmp_path / "t4_s6_recovery_root"

    # Step 1: Boot server #1 with pre-seeded 12 MB errors.jsonl -> verify fast non-OOM boot & rotation cap
    t0 = time.monotonic()
    srv1 = server_factory(
        profile="cloud",
        paper_trading_only=True,
        root_dir=shared_root,
        preseed_errors_mb=12.0,
        extra_env={"HEALTH_SAMPLE_INTERVAL": "1"},
    )
    boot_elapsed = time.monotonic() - t0
    assert boot_elapsed < 35.0, f"Pre-seeded errors.jsonl boot took too long ({boot_elapsed:.1f}s)"

    err_path = srv1.errors_jsonl_path()
    assert err_path.exists()
    assert err_path.stat().st_size <= 11 * 1024 * 1024, (
        f"Active errors.jsonl exceeded 10 MB rotation bound: {err_path.stat().st_size} bytes"
    )
    rotated_files = sorted(srv1.log_dir.glob("errors.jsonl*"))
    assert len(rotated_files) <= 4, f"Expected <= 4 errors.jsonl* files (active + 3 backups), got {rotated_files}"
    assert sum(p.stat().st_size for p in rotated_files) <= 42 * 1024 * 1024

    # Step 2: Establish deterministic positions across all 3 brokers + fire 48 concurrent multi-broker requests
    pre_qty = {"deltaexchange": 3, "fyers": 5, "groww": 4}
    for broker, qty in pre_qty.items():
        _place_paper_order(srv1, broker, action="BUY", quantity=qty, pricetype="MARKET", strategy=f"t4_s6_base_{broker}")

    def _load_worker(idx: int) -> int:
        broker = BROKERS[idx % len(BROKERS)]
        spec = DEFAULT_SYMBOLS[broker]
        op = idx % 5
        if op == 0:
            r = srv1.api_post(
                "/api/v1/quotes",
                {"apikey": srv1.api_key, "symbol": spec["symbol"], "exchange": spec["exchange"]},
                broker=broker,
            )
        elif op == 1:
            r = srv1.api_post("/api/v1/intervals", {"apikey": srv1.api_key}, broker=broker)
        elif op == 2:
            r = srv1.api_post("/api/v1/orderbook", {"apikey": srv1.api_key}, broker=broker)
        elif op == 3:
            r = srv1.api_post("/api/v1/positionbook", {"apikey": srv1.api_key}, broker=broker)
        else:
            r = srv1.api_post("/api/v1/funds", {"apikey": srv1.api_key}, broker=broker)
        return r.status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        load_statuses = list(pool.map(_load_worker, range(48)))
    assert all(code == 200 for code in load_statuses), f"Non-200 status during concurrent load: {load_statuses}"

    time.sleep(1.5)
    ctx_errs_1 = [e for e in srv1.read_error_lines() if "BrokerContextError" in json.dumps(e)]
    assert ctx_errs_1 == [], f"BrokerContextError occurred during load: {ctx_errs_1[:3]}"

    # Step 3: Stop server #1 (simulating crash/restart) and seed aged (>2d) bloated health.db rows per broker
    srv1.stop()

    old_ts = (datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d %H:%M:%S")
    fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    bloated_td = json.dumps([{"id": i, "name": "worker-" + ("x" * 35), "daemon": True, "alive": True} for i in range(40)])

    health_sizes_before: dict[str, int] = {}
    for broker in BROKERS:
        hdb = srv1.broker_db_path(broker, "health.db")
        assert hdb.is_file(), f"Missing health.db for {broker}"
        with sqlite3.connect(str(hdb)) as conn:
            conn.executemany(
                "INSERT INTO health_metrics (timestamp, fd_count, thread_details, overall_status) "
                "VALUES (?, 654, ?, 'pass')",
                [(old_ts, bloated_td) for _ in range(300)],
            )
            conn.execute(
                "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 321, 'pass')",
                (fresh_ts,),
            )
            conn.commit()
        health_sizes_before[broker] = hdb.stat().st_size

    # Step 4: Restart server #2 on the SAME root_dir -> verify hygiene + position survival
    srv2 = server_factory(
        profile="cloud",
        paper_trading_only=True,
        root_dir=shared_root,
        extra_env={"HEALTH_SAMPLE_INTERVAL": "1"},
    )
    with srv2.session_client("fyers") as cli:
        assert cli.get("/health/check").status_code == 200
    time.sleep(2.0)

    for broker, expected_qty in pre_qty.items():
        # Verify sandbox position survived restart intact
        pb = srv2.api_post("/api/v1/positionbook", {"apikey": srv2.api_key}, broker=broker).json()["data"]
        assert any(
            p["symbol"] == DEFAULT_SYMBOLS[broker]["symbol"] and int(float(p["quantity"])) == expected_qty
            for p in pb
        ), f"[{broker}] position lost across restart: {pb}"

        # Verify health.db retention pruning, incremental vacuum compaction, and bounded live thread_details
        hdb = srv2.broker_db_path(broker, "health.db")
        with sqlite3.connect(str(hdb)) as conn:
            auto_vac = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0])
            old_left = int(conn.execute("SELECT COUNT(*) FROM health_metrics WHERE fd_count = 654").fetchone()[0])
            fresh_left = int(conn.execute("SELECT COUNT(*) FROM health_metrics WHERE fd_count = 321").fetchone()[0])
            live_td = conn.execute(
                "SELECT thread_details FROM health_metrics WHERE fd_count != 654 AND thread_details IS NOT NULL LIMIT 10"
            ).fetchall()
        assert auto_vac == 2, f"[{broker}] expected PRAGMA auto_vacuum=2, got {auto_vac}"
        assert old_left == 0, f"[{broker}] aged (>2d) health_metrics rows not pruned: {old_left}"
        assert fresh_left == 1, f"[{broker}] fresh (<2d) health_metrics row wrongly pruned"
        assert hdb.stat().st_size < health_sizes_before[broker], (
            f"[{broker}] health.db did not compact after pruning: "
            f"before={health_sizes_before[broker]}, after={hdb.stat().st_size}"
        )
        for (td_raw,) in live_td:
            td_str = td_raw if isinstance(td_raw, str) else json.dumps(td_raw)
            assert len(td_str.encode("utf-8")) < 4096, f"[{broker}] live thread_details exceeded 4 KB"

    ctx_errs_2 = [e for e in srv2.read_error_lines() if "BrokerContextError" in json.dumps(e)]
    assert ctx_errs_2 == [], f"BrokerContextError occurred after restart: {ctx_errs_2[:3]}"
    assert srv2.duckdb_files() == []
    srv2.assert_zero_real_broker_order_calls()


# ===========================================================================
# T4-S7: Production 3-Hour Monitoring Probe End-to-End (OK -> WARN -> CRITICAL)
# ===========================================================================


@pytest.mark.timeout(180)
def test_t4_s7_production_3h_monitoring_probe_ok_warn_critical_escalation(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """T4-S7: Sequential monitor_production.py probes escalate cleanly OK -> WARN (expired Fyers/Groww) -> CRITICAL (breach)."""
    cloud_srv = server_factory(profile="cloud", paper_trading_only=True)
    cloud_ws = ws_proxy_factory(server=cloud_srv)
    out_jsonl = tmp_path / "monitor-3h.jsonl"

    # --- Stage 1: Healthy cloud+paper target -> overall_status == "OK" ---
    proc_ok, recs_ok = _run_monitor_once(
        base_url=cloud_srv.base_url,
        root_dir=cloud_srv.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_srv.api_key,
        ws_port=cloud_ws.ws_port,
    )
    assert proc_ok.returncode in (0, 1, 2), f"Stage 1 monitor crash: {proc_ok.stdout}\n{proc_ok.stderr}"
    assert len(recs_ok) == 1
    rec1 = recs_ok[0]
    assert rec1["overall_status"] == "OK", f"Stage 1 expected OK, got: {rec1}"
    assert int(rec1["check_number"]) == 1
    assert rec1["https"].get("history_blocked") is True
    assert not any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec1["alerts"])

    # --- Stage 2: Expire Fyers & Groww daily OAuth tokens (keep Delta active) -> overall_status == "WARN" ---
    for broker in ("fyers", "groww"):
        with sqlite3.connect(str(cloud_srv.broker_db_path(broker, "openalgo.db"))) as conn:
            conn.execute("UPDATE auth SET is_revoked = 1")
            conn.commit()

    proc_warn, recs_warn = _run_monitor_once(
        base_url=cloud_srv.base_url,
        root_dir=cloud_srv.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_srv.api_key,
        ws_port=cloud_ws.ws_port,
    )
    assert proc_warn.returncode in (0, 1, 2), f"Stage 2 monitor crash: {proc_warn.stdout}\n{proc_warn.stderr}"
    assert len(recs_warn) == 2
    rec2 = recs_warn[1]
    assert rec2["overall_status"] == "WARN", f"Stage 2 expected WARN (per Q7), got: {rec2}"
    assert int(rec2["check_number"]) == 2
    warn_sevs = [str(a.get("severity", "")).upper() for a in rec2["alerts"]]
    assert "WARN" in warn_sevs and "CRITICAL" not in warn_sevs, rec2["alerts"]

    # --- Stage 3: History-block breach (full server where /api/v1/history is open) -> overall_status == "CRITICAL" ---
    breach_srv = server_factory(profile="full", paper_trading_only=True)
    breach_ws = ws_proxy_factory(server=breach_srv)

    proc_crit, recs_crit = _run_monitor_once(
        base_url=breach_srv.base_url,
        root_dir=breach_srv.root_dir,
        output_jsonl=out_jsonl,
        api_key=breach_srv.api_key,
        ws_port=breach_ws.ws_port,
    )
    assert proc_crit.returncode in (0, 1, 2), f"Stage 3 monitor crash: {proc_crit.stdout}\n{proc_crit.stderr}"
    assert len(recs_crit) == 3
    rec3 = recs_crit[2]
    assert rec3["overall_status"] == "CRITICAL", f"Stage 3 expected CRITICAL on history breach, got: {rec3}"
    assert int(rec3["check_number"]) == 3
    assert rec3["https"].get("history_blocked") is False
    assert any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec3["alerts"]), rec3["alerts"]

    # --- Validate all 3 sequential JSONL records against Interface Contract #6 ---
    assert [int(r["check_number"]) for r in recs_crit] == [1, 2, 3]
    for idx, rec in enumerate(recs_crit, start=1):
        for key in REQUIRED_MONITOR_TOP_KEYS:
            assert key in rec, f"Record #{idx} missing Contract #6 key '{key}': {rec}"
        datetime.fromisoformat(str(rec["timestamp_utc"]).replace("Z", "+00:00"))
        for broker in BROKERS:
            assert broker in rec["brokers"], f"Record #{idx} missing broker {broker}"

    assert cloud_srv.duckdb_files() == []
    cloud_srv.assert_zero_real_broker_order_calls()
    breach_srv.assert_zero_real_broker_order_calls()
