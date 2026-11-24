"""Tier 3 E2E Cross-Feature Pairwise Combination Tests.

Strict opaque-box tests exercising pairwise cross-feature interactions across:
- Runtime profiles (cloud, full, full + DISABLE_HISTORICAL_DATA=true)
- Paper-trading lock (PAPER_TRADING_ONLY=true vs false fail-safe in cloud)
- All 3 target brokers (deltaexchange, fyers, groww)
- HTTP routing & header/query precedence, /python strategy runner, WebSocket proxy :8765
- Per-broker Sandbox execution / MIS square-off, live-bar strategies, OpenScript runner
- Runtime hygiene (errors.jsonl, health.db), MCP HTTP tools, and production monitoring

Uses ONLY public fixtures & helpers from ``test/e2e/conftest.py``, public HTTP/WS
interfaces, CLI subprocesses, and observable filesystem/SQLite artifacts.
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

pytestmark = pytest.mark.tier3

# ---------------------------------------------------------------------------
# Compiled OpenScript program (canonical SHA-256 hash matches openscript engine)
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
# Pure Opaque-Box Protocol & Contract Assertion Helpers
# ---------------------------------------------------------------------------


def _recent_date_window(days_back: int = 10) -> tuple[str, str]:
    end_d = date.today() - timedelta(days=1)
    start_d = end_d - timedelta(days=days_back)
    return start_d.isoformat(), end_d.isoformat()


def _assert_paper_locked_response(resp: httpx.Response, context: str = "") -> dict[str, Any]:
    """Assert HTTP 403 PAPER_TRADING_ONLY JSON error contract (PROJECT.md § Interface Contracts)."""
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
    """Assert HTTP 403/503 HISTORICAL_DATA_DISABLED JSON error contract."""
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
    strategy: str = "e2e_t3_strat",
) -> dict[str, Any]:
    spec = DEFAULT_SYMBOLS[broker]
    payload = {
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
    }
    resp = server.api_post("/api/v1/placeorder", payload, broker=broker)
    assert resp.status_code == 200, (
        f"placeorder failed for {broker}: HTTP {resp.status_code} {resp.text[:400]}"
    )
    body = resp.json()
    assert body.get("status") == "success", f"Unexpected placeorder body for {broker}: {body}"
    assert body.get("mode") == "analyze", f"Expected mode='analyze' for {broker}: {body}"
    assert body.get("orderid"), f"Missing orderid for {broker}: {body}"
    return body


def _create_signal_strategy(client: SessionClient, name: str = "E2E T3 Probe") -> int:
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
    """Complete public RFC 7591 + OAuth 2.1 PKCE dance over HTTP to obtain an MCP Bearer JWT."""
    redirect_uri = "http://127.0.0.1:9876/callback"
    reg_resp = instance.api_post(
        "/oauth/register",
        payload={
            "client_name": "E2E T3 MCP Probe",
            "redirect_uris": [redirect_uri],
            "scope": scope,
            "token_endpoint_auth_method": "client_secret_post",
        },
    )
    assert reg_resp.status_code in (200, 201), (
        f"MCP OAuth DCR failed: {reg_resp.status_code} {reg_resp.text[:300]}"
    )
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
        consent_get = session.get("/oauth/authorize", params=auth_params)
        assert consent_get.status_code == 200, (
            f"MCP OAuth consent GET failed: {consent_get.status_code} {consent_get.text[:300]}"
        )
        approve_resp = session.post(
            "/oauth/authorize",
            params=auth_params,
            data={"decision": "approve"},
        )
        assert approve_resp.status_code in (302, 303), (
            f"MCP OAuth approve POST failed: {approve_resp.status_code} {approve_resp.text[:300]}"
        )
    location = approve_resp.headers.get("location", "")
    qs = parse_qs(urlparse(location).query)
    assert "code" in qs, f"No authorization code in redirect location: {location}"
    code = qs["code"][0]

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
    assert token_resp.status_code == 200, (
        f"MCP OAuth token exchange failed: {token_resp.status_code} {token_resp.text[:300]}"
    )
    token = token_resp.json().get("access_token")
    assert isinstance(token, str) and token, f"Missing access_token: {token_resp.json()}"
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
    cycles: int = 3,
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
# Pairwise 01–06: Profile × Paper-Lock × Live/History/SPA Surfaces
# ===========================================================================


def test_t3_01_cloud_profile_paper_trading_only_false_failsafe_locks_paper_and_blocks_history(
    server_factory,
) -> None:
    """Pair: OPENALGO_PROFILE=cloud × PAPER_TRADING_ONLY=false still enforces fail-safe paper lock & history lockout."""
    srv = server_factory(profile="cloud", paper_trading_only=False)
    start_d, end_d = _recent_date_window(7)

    cfg_resp = srv.api_get("/auth/app-config")
    assert cfg_resp.status_code == 200, cfg_resp.text[:300]
    cfg = cfg_resp.json()
    assert cfg.get("profile") == "cloud"
    assert cfg.get("paper_trading_only") is True, f"Cloud fail-safe must keep paper_trading_only=True: {cfg}"
    assert cfg.get("historical_data_enabled") is False, cfg

    for broker in BROKERS:
        toggle_resp = srv.api_post(
            "/api/v1/analyzer/toggle",
            {"apikey": srv.api_key, "mode": False},
            broker=broker,
        )
        _assert_paper_locked_response(toggle_resp, context=f"cloud failsafe RESTX toggle {broker}")

        with srv.session_client(broker) as cli:
            _assert_paper_locked_response(
                cli.post("/auth/analyzer-toggle"), context=f"cloud failsafe SPA toggle {broker}"
            )
            _assert_paper_locked_response(
                cli.post("/settings/analyze-mode/0"), context=f"cloud failsafe settings/0 {broker}"
            )

        hist_resp = srv.api_post(
            "/api/v1/history",
            {
                "apikey": srv.api_key,
                "symbol": DEFAULT_SYMBOLS[broker]["symbol"],
                "exchange": DEFAULT_SYMBOLS[broker]["exchange"],
                "interval": "D",
                "start_date": start_d,
                "end_date": end_d,
            },
            broker=broker,
        )
        _assert_historical_disabled_response(hist_resp, allowed_statuses=(403,), context=f"cloud history {broker}")

    assert srv.duckdb_files() == [], f"Cloud fail-safe must create zero DuckDB files: {srv.duckdb_files()}"
    srv.assert_zero_real_broker_order_calls()


def test_t3_02_full_profile_paper_trading_only_true_allows_history_while_blocking_live(
    full_server: OpenAlgoE2EInstance,
) -> None:
    """Pair: OPENALGO_PROFILE=full × PAPER_TRADING_ONLY=true allows history/DuckDB while hard-blocking all live toggles."""
    start_d, end_d = _recent_date_window(7)

    cfg = full_server.api_get("/auth/app-config")
    assert cfg.status_code == 200, cfg.text[:300]
    cfg_body = cfg.json()
    assert cfg_body.get("profile") == "full"
    assert cfg_body.get("historical_data_enabled") is True
    assert cfg_body.get("paper_trading_only") is True

    # Historical APIs and Historify remain fully functional locally
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
    assert hist_resp.status_code == 200, f"Full profile /api/v1/history failed: {hist_resp.text[:300]}"
    assert hist_resp.json().get("status") == "success"
    assert hist_resp.json().get("data"), f"Expected candles in full profile: {hist_resp.json()}"

    ticker_resp = full_server.api_get(
        "/api/v1/ticker/NSE:SBIN",
        broker="fyers",
        query={"apikey": full_server.api_key, "interval": "D", "from": start_d, "to": end_d, "format": "json"},
    )
    assert ticker_resp.status_code == 200, f"Full profile /api/v1/ticker failed: {ticker_resp.text[:300]}"

    with full_server.session_client("fyers") as cli:
        hist_stats = cli.get("/historify/api/stats")
        assert hist_stats.status_code == 200, f"Full profile Historify stats failed: {hist_stats.text[:300]}"

        # Simultaneously, PAPER_TRADING_ONLY=true blocks EVERY live toggle surface
        _assert_paper_locked_response(cli.post("/auth/analyzer-toggle"), context="full SPA toggle")
        _assert_paper_locked_response(cli.post("/settings/analyze-mode/0"), context="full settings/0")
        sid = _create_signal_strategy(cli, name="T3 Full Paper Lock")
        _assert_paper_locked_response(
            cli.post(f"/strategy/api/strategies/{sid}/live", json={"enabled": True}),
            context="full strategy /live",
        )

    restx_toggle = full_server.api_post(
        "/api/v1/analyzer/toggle",
        {"apikey": full_server.api_key, "mode": False},
        broker="fyers",
    )
    _assert_paper_locked_response(restx_toggle, context="full RESTX toggle")

    analyzer = full_server.api_post("/api/v1/analyzer", {"apikey": full_server.api_key}, broker="fyers")
    assert analyzer.status_code == 200 and analyzer.json()["data"]["analyze_mode"] is True
    full_server.assert_zero_real_broker_order_calls()


def test_t3_03_full_profile_with_disable_historical_data_true_blocks_history_without_cloud(
    server_factory,
) -> None:
    """Pair: OPENALGO_PROFILE=full × DISABLE_HISTORICAL_DATA=true blocks history while keeping live quotes/orders open."""
    srv = server_factory(profile="full", paper_trading_only=True, disable_historical_data=True)
    start_d, end_d = _recent_date_window(7)

    cfg = srv.api_get("/auth/app-config")
    assert cfg.status_code == 200, cfg.text[:300]
    cfg_body = cfg.json()
    assert cfg_body.get("profile") == "full"
    assert cfg_body.get("historical_data_enabled") is False
    assert cfg_body.get("paper_trading_only") is True

    # Historical surfaces MUST be blocked
    hist_resp = srv.api_post(
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
    )
    _assert_historical_disabled_response(hist_resp, allowed_statuses=(403,), context="full+DHD /api/v1/history")

    ticker_resp = srv.api_get(
        "/api/v1/ticker/NSE:SBIN",
        broker="fyers",
        query={"apikey": srv.api_key, "interval": "D", "from": start_d, "to": end_d, "format": "json"},
    )
    _assert_historical_disabled_response(ticker_resp, allowed_statuses=(403,), context="full+DHD /api/v1/ticker")

    with srv.session_client("fyers") as cli:
        scalp_hist = cli.get("/scalping/api/history", params={"symbol": "SBIN", "exchange": "NSE", "interval": "5m"})
        _assert_historical_disabled_response(scalp_hist, allowed_statuses=(403, 503), context="full+DHD scalping history")

    # Live data & paper trading MUST stay completely operational
    q_resp = srv.api_post(
        "/api/v1/quotes",
        {"apikey": srv.api_key, "symbol": "SBIN", "exchange": "NSE"},
        broker="fyers",
    )
    assert q_resp.status_code == 200 and q_resp.json().get("status") == "success"

    int_resp = srv.api_post("/api/v1/intervals", {"apikey": srv.api_key}, broker="fyers")
    assert int_resp.status_code == 200 and int_resp.json().get("status") == "success"

    _place_paper_order(srv, "fyers", action="BUY", quantity=1, pricetype="MARKET", strategy="t3_03_dhd")
    assert srv.duckdb_files() == []
    srv.assert_zero_real_broker_order_calls()


def test_t3_04_cloud_profile_live_quotes_intervals_and_concurrent_three_broker_paper_orders(
    server_factory,
) -> None:
    """Pair: OPENALGO_PROFILE=cloud × live /quotes & /intervals × concurrent 3-broker paper orders."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65120.0)
    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=804.5)
    srv.set_mock_quote("groww", "NSE", "SBIN", ltp=803.25)

    def _broker_workflow(broker: str) -> dict[str, Any]:
        spec = DEFAULT_SYMBOLS[broker]
        q_r = srv.api_post(
            "/api/v1/quotes",
            {"apikey": srv.api_key, "symbol": spec["symbol"], "exchange": spec["exchange"]},
            broker=broker,
        )
        i_r = srv.api_post("/api/v1/intervals", {"apikey": srv.api_key}, broker=broker)
        o_r = srv.api_post(
            "/api/v1/placeorder",
            {
                "apikey": srv.api_key,
                "strategy": f"t3_04_{broker}",
                "exchange": spec["exchange"],
                "symbol": spec["symbol"],
                "action": "BUY",
                "quantity": 2,
                "pricetype": "MARKET",
                "product": spec["product"],
            },
            broker=broker,
        )
        return {
            "broker": broker,
            "q_status": q_r.status_code,
            "q_ltp": float(q_r.json().get("data", {}).get("ltp", 0.0)) if q_r.status_code == 200 else 0.0,
            "i_status": i_r.status_code,
            "o_status": o_r.status_code,
            "orderid": o_r.json().get("orderid") if o_r.status_code == 200 else None,
            "mode": o_r.json().get("mode") if o_r.status_code == 200 else None,
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(_broker_workflow, BROKERS))

    by_broker = {r["broker"]: r for r in results}
    expected_ltps = {"deltaexchange": 65120.0, "fyers": 804.5, "groww": 803.25}
    for broker in BROKERS:
        res = by_broker[broker]
        assert res["q_status"] == 200, res
        assert res["q_ltp"] == pytest.approx(expected_ltps[broker], rel=1e-4)
        assert res["i_status"] == 200, res
        assert res["o_status"] == 200 and res["mode"] == "analyze" and res["orderid"], res

        ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker=broker).json()
        own_ids = {str(o["orderid"]) for o in ob["data"]["orders"]}
        assert str(res["orderid"]) in own_ids
        # Other brokers' orderids must NOT leak into this broker's orderbook
        for other_b in BROKERS:
            if other_b != broker:
                assert str(by_broker[other_b]["orderid"]) not in own_ids

    assert srv.duckdb_files() == []
    srv.assert_zero_real_broker_order_calls()


def test_t3_05_cloud_profile_scalping_live_endpoints_open_while_scalping_history_blocked(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Pair: OPENALGO_PROFILE=cloud × /scalping live endpoints open while /scalping/api/history returns 403."""
    with cloud_server.session_client("fyers") as cli:
        page = cli.get("/scalping", headers={"Accept": "text/html"})
        assert page.status_code == 200, f"GET /scalping must stay open in cloud: {page.status_code}"

        for live_ep in (
            "/scalping/api/underlyings",
            "/scalping/api/all_underlyings",
            "/scalping/api/tracked",
            "/scalping/api/sl",
        ):
            r = cli.get(live_ep)
            assert r.status_code == 200, f"Live scalping endpoint {live_ep} failed in cloud: {r.status_code} {r.text[:250]}"

        hist_r = cli.get(
            "/scalping/api/history",
            params={"symbol": "SBIN", "exchange": "NSE", "interval": "5m"},
        )
        _assert_historical_disabled_response(
            hist_r, allowed_statuses=(403,), context="cloud GET /scalping/api/history"
        )

    assert cloud_server.duckdb_files() == []
    cloud_server.assert_zero_real_broker_order_calls()


def test_t3_06_cloud_app_config_consistency_with_spa_route_guards(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Pair: OPENALGO_PROFILE=cloud × /auth/app-config flags match SPA route guard outcomes (200 vs 503)."""
    cfg_resp = cloud_server.api_get("/auth/app-config")
    assert cfg_resp.status_code == 200, cfg_resp.text[:300]
    cfg = cfg_resp.json()
    assert cfg.get("status") == "success"
    assert cfg.get("profile") == "cloud"
    assert cfg.get("historical_data_enabled") is False
    assert cfg.get("paper_trading_only") is True
    assert set(BROKERS).issubset({str(b).lower() for b in cfg.get("brokers", [])})

    # Live SPA routes must return 200 with HTML
    for live_route in ("/trading", "/scalping", "/dashboard", "/sandbox", "/positions", "/orderbook"):
        r = cloud_server.api_get(live_route, headers={"Accept": "text/html"})
        assert r.status_code == 200, f"Live SPA route {live_route} failed in cloud: {r.status_code}"
        assert "<html" in r.text.lower() or "<!doctype html" in r.text.lower()

    # Historical/backtesting SPA routes must return 503 HISTORICAL_DATA_DISABLED
    for blocked_route in (
        "/historify",
        "/portfolio-backtester",
        "/sip-backtester",
        "/ivchart",
        "/straddle",
        "/oiprofile",
        "/sector-rotation",
        "/pnl-tracker",
    ):
        r = cloud_server.api_get(blocked_route, headers={"Accept": "application/json"})
        _assert_historical_disabled_response(
            r, allowed_statuses=(503,), context=f"cloud SPA route {blocked_route}"
        )

    assert cloud_server.duckdb_files() == []


# ===========================================================================
# Pairwise 07–11: PAPER_TRADING_ONLY × 3 Brokers × Routing × /python Runner
# ===========================================================================


def test_t3_07_paper_lock_deltaexchange_crypto_order_and_live_toggle_rejection(server_factory) -> None:
    """Pair: PAPER_TRADING_ONLY × Delta Exchange (CRYPTO) paper orders + live toggle rejection + zero real calls."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65300.0)
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCINR", ltp=5510000.0)

    fut_ord = _place_paper_order(
        srv, "deltaexchange", symbol="BTCUSDFUT", exchange="CRYPTO", product="NRML", action="BUY", quantity=2
    )
    spot_ord = _place_paper_order(
        srv, "deltaexchange", symbol="BTCINR", exchange="CRYPTO", product="CNC", action="BUY", quantity=1
    )

    ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker="deltaexchange").json()
    oids = {str(o["orderid"]) for o in ob["data"]["orders"]}
    assert str(fut_ord["orderid"]) in oids and str(spot_ord["orderid"]) in oids

    # Attempt live escalation on Delta across all 3 surfaces
    _assert_paper_locked_response(
        srv.api_post("/api/v1/analyzer/toggle", {"apikey": srv.api_key, "mode": False}, broker="deltaexchange"),
        context="delta RESTX toggle",
    )
    with srv.session_client("deltaexchange") as cli:
        _assert_paper_locked_response(cli.post("/auth/analyzer-toggle"), context="delta SPA toggle")
        _assert_paper_locked_response(cli.post("/settings/analyze-mode/0"), context="delta settings/0")

    an = srv.api_post("/api/v1/analyzer", {"apikey": srv.api_key}, broker="deltaexchange").json()
    assert an["data"]["analyze_mode"] is True
    srv.assert_zero_real_broker_order_calls()


def test_t3_08_paper_lock_fyers_nse_nfo_order_and_gtt_with_live_toggle_rejection(server_factory) -> None:
    """Pair: PAPER_TRADING_ONLY × Fyers (NSE/NFO) paper order + GTT + live toggle rejection + zero real calls."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=805.0)
    srv.set_mock_quote("fyers", "NFO", "NIFTY27MAR25FUT", ltp=24100.0)

    nse_ord = _place_paper_order(
        srv, "fyers", symbol="SBIN", exchange="NSE", product="CNC", action="BUY", quantity=3
    )
    nfo_ord = _place_paper_order(
        srv,
        "fyers",
        symbol="NIFTY27MAR25FUT",
        exchange="NFO",
        product="NRML",
        action="BUY",
        quantity=75,
        pricetype="LIMIT",
        price=23900.0,
    )
    gtt_resp = srv.api_post(
        "/api/v1/placegttorder",
        {
            "apikey": srv.api_key,
            "strategy": "t3_08_fyers_gtt",
            "trigger_type": "SINGLE",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "BUY",
            "product": "CNC",
            "quantity": 2,
            "pricetype": "LIMIT",
            "price": 780.0,
            "triggerprice_tg": 785.0,
        },
        broker="fyers",
    )
    assert gtt_resp.status_code == 200 and gtt_resp.json().get("mode") == "analyze", gtt_resp.text
    assert str(gtt_resp.json().get("trigger_id", "")).startswith("GTT-")

    ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker="fyers").json()
    oids = {str(o["orderid"]) for o in ob["data"]["orders"]}
    assert str(nse_ord["orderid"]) in oids and str(nfo_ord["orderid"]) in oids

    _assert_paper_locked_response(
        srv.api_post("/api/v1/analyzer/toggle", {"apikey": srv.api_key, "mode": False}, broker="fyers"),
        context="fyers RESTX toggle",
    )
    with srv.session_client("fyers") as cli:
        _assert_paper_locked_response(cli.post("/auth/analyzer-toggle"), context="fyers SPA toggle")
        _assert_paper_locked_response(cli.post("/settings/analyze-mode/0"), context="fyers settings/0")
        sid = _create_signal_strategy(cli, name="T3 Fyers Live Probe")
        _assert_paper_locked_response(
            cli.post(f"/strategy/api/strategies/{sid}/live", json={"enabled": True}),
            context="fyers strategy /live",
        )

    srv.assert_zero_real_broker_order_calls()


def test_t3_09_paper_lock_groww_nse_nfo_order_and_live_toggle_rejection(server_factory) -> None:
    """Pair: PAPER_TRADING_ONLY × Groww (NSE/NFO) paper orders + live toggle rejection + zero real calls."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    srv.set_mock_quote("groww", "NSE", "SBIN", ltp=802.0)
    srv.set_mock_quote("groww", "NFO", "NIFTY27MAR25FUT", ltp=24120.0)

    nse_ord = _place_paper_order(
        srv, "groww", symbol="SBIN", exchange="NSE", product="CNC", action="BUY", quantity=4
    )
    nfo_ord = _place_paper_order(
        srv, "groww", symbol="NIFTY27MAR25FUT", exchange="NFO", product="NRML", action="BUY", quantity=75
    )

    pb = srv.api_post("/api/v1/positionbook", {"apikey": srv.api_key}, broker="groww").json()["data"]
    syms = {p["symbol"] for p in pb}
    assert {"SBIN", "NIFTY27MAR25FUT"}.issubset(syms), pb

    # Sibling brokers remain completely untouched
    for other in ("fyers", "deltaexchange"):
        other_pb = srv.api_post("/api/v1/positionbook", {"apikey": srv.api_key}, broker=other).json()["data"]
        assert other_pb == [], f"Groww orders leaked into {other}: {other_pb}"

    _assert_paper_locked_response(
        srv.api_post("/api/v1/analyzer/toggle", {"apikey": srv.api_key, "mode": False}, broker="groww"),
        context="groww RESTX toggle",
    )
    with srv.session_client("groww") as cli:
        _assert_paper_locked_response(cli.post("/auth/analyzer-toggle"), context="groww SPA toggle")
        _assert_paper_locked_response(cli.post("/settings/analyze-mode/0"), context="groww settings/0")

    assert nse_ord["orderid"] and nfo_ord["orderid"]
    srv.assert_zero_real_broker_order_calls()


def test_t3_10_multibroker_header_query_precedence_and_sandbox_isolation(server_factory) -> None:
    """Pair: /api/v1/* routing precedence (?broker= > X-OpenAlgo-Broker > shared key) × per-broker sandbox books."""
    srv = server_factory(profile="cloud", paper_trading_only=True)

    # 1. ?broker=groww MUST override X-OpenAlgo-Broker: fyers
    r_groww = srv.api_post(
        "/api/v1/placeorder",
        {
            "apikey": srv.api_key,
            "strategy": "t3_10_query_wins",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "BUY",
            "quantity": 7,
            "pricetype": "MARKET",
            "product": "CNC",
        },
        broker="fyers",
        query={"broker": "groww"},
    )
    assert r_groww.status_code == 200 and r_groww.json().get("status") == "success", r_groww.text
    groww_oid = str(r_groww.json()["orderid"])

    # 2. X-OpenAlgo-Broker: deltaexchange (no ?broker=) routes to deltaexchange
    r_delta = srv.api_post(
        "/api/v1/placeorder",
        {
            "apikey": srv.api_key,
            "strategy": "t3_10_header_wins",
            "exchange": "CRYPTO",
            "symbol": "BTCUSDFUT",
            "action": "BUY",
            "quantity": 3,
            "pricetype": "MARKET",
            "product": "NRML",
        },
        broker="deltaexchange",
    )
    assert r_delta.status_code == 200 and r_delta.json().get("status") == "success", r_delta.text
    delta_oid = str(r_delta.json()["orderid"])

    # 3. Shared API key with NO explicit header/query defaults to fyers
    r_fyers = srv.api_post(
        "/api/v1/placeorder",
        {
            "apikey": srv.api_key,
            "strategy": "t3_10_default_fyers",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "BUY",
            "quantity": 5,
            "pricetype": "MARKET",
            "product": "CNC",
        },
    )
    assert r_fyers.status_code == 200 and r_fyers.json().get("status") == "success", r_fyers.text
    fyers_oid = str(r_fyers.json()["orderid"])

    expected = {
        "groww": (groww_oid, 7),
        "deltaexchange": (delta_oid, 3),
        "fyers": (fyers_oid, 5),
    }
    for broker, (expected_oid, expected_qty) in expected.items():
        ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker=broker).json()["data"]["orders"]
        oids = {str(o["orderid"]) for o in ob}
        assert oids == {expected_oid}, f"Broker {broker} orderbook cross-contaminated: {oids}"

        pb = srv.api_post("/api/v1/positionbook", {"apikey": srv.api_key}, broker=broker).json()["data"]
        assert len(pb) == 1 and int(float(pb[0]["quantity"])) == expected_qty, f"{broker} position mismatch: {pb}"

    srv.assert_zero_real_broker_order_calls()


def test_t3_11_multibroker_python_strategy_upload_start_header_propagation_isolated_orders(
    server_factory, tmp_path: Path
) -> None:
    """Pair: Multi-broker /python strategy upload/start × X-OpenAlgo-Broker propagation × isolated sandbox orders."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    delta_done = tmp_path / "delta_strat_done.json"
    groww_done = tmp_path / "groww_strat_done.json"

    def _make_script(done_path: Path, symbol: str, exchange: str, product: str, qty: int) -> bytes:
        return (
            "import json, os, time, urllib.request\n"
            f"done = {str(done_path)!r}\n"
            "broker = os.environ.get('OPENALGO_BROKER', '')\n"
            "host = os.environ.get('OPENALGO_HOST', '')\n"
            "apikey = os.environ.get('OPENALGO_API_KEY', '')\n"
            "payload = {\n"
            "    'apikey': apikey, 'strategy': f'py_{broker}',\n"
            f"    'exchange': {exchange!r}, 'symbol': {symbol!r},\n"
            f"    'action': 'BUY', 'quantity': {qty}, 'pricetype': 'MARKET', 'product': {product!r},\n"
            "}\n"
            "req = urllib.request.Request(\n"
            "    f'{host}/api/v1/placeorder',\n"
            "    data=json.dumps(payload).encode('utf-8'),\n"
            "    headers={'Content-Type': 'application/json', 'X-OpenAlgo-Broker': broker},\n"
            "    method='POST',\n"
            ")\n"
            "with urllib.request.urlopen(req, timeout=10) as r:\n"
            "    resp = json.loads(r.read().decode('utf-8'))\n"
            "with open(done, 'w', encoding='utf-8') as fh:\n"
            "    json.dump({'broker': broker, 'resp': resp}, fh)\n"
            "time.sleep(2.5)\n"
        ).encode("utf-8")

    delta_src = _make_script(delta_done, "BTCUSDFUT", "CRYPTO", "NRML", 2)
    groww_src = _make_script(groww_done, "SBIN", "NSE", "CNC", 4)

    with srv.session_client("deltaexchange") as d_cli, srv.session_client("groww") as g_cli:
        up_d = d_cli.post(
            "/python/new",
            data={
                "strategy_name": "SharedStratName",
                "exchange": "CRYPTO",
                "schedule_start": "00:00",
                "schedule_days": json.dumps(["mon", "tue", "wed", "thu", "fri", "sat", "sun"]),
            },
            files={"strategy_file": ("shared_strat.py", delta_src, "text/x-python")},
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        up_g = g_cli.post(
            "/python/new",
            data={
                "strategy_name": "SharedStratName",
                "exchange": "NSE",
                "schedule_start": "00:00",
                "schedule_stop": "23:59",
                "schedule_days": json.dumps(["mon", "tue", "wed", "thu", "fri", "sat", "sun"]),
            },
            files={"strategy_file": ("shared_strat.py", groww_src, "text/x-python")},
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        assert up_d.status_code in (200, 201), up_d.text[:300]
        assert up_g.status_code in (200, 201), up_g.text[:300]
        d_sid = up_d.json()["data"]["strategy_id"]
        g_sid = up_g.json()["data"]["strategy_id"]

        assert d_cli.post(
            f"/python/start/{d_sid}", headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
        ).status_code == 200
        assert g_cli.post(
            f"/python/start/{g_sid}", headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"}
        ).status_code == 200

        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and not (delta_done.exists() and groww_done.exists()):
            time.sleep(0.15)
        assert delta_done.exists() and groww_done.exists(), "Both broker strategies must execute concurrently"

        d_res = json.loads(delta_done.read_text(encoding="utf-8"))
        g_res = json.loads(groww_done.read_text(encoding="utf-8"))
        assert d_res["broker"] == "deltaexchange" and d_res["resp"].get("mode") == "analyze"
        assert g_res["broker"] == "groww" and g_res["resp"].get("mode") == "analyze"

        d_cli.post(f"/python/stop/{d_sid}", headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})
        g_cli.post(f"/python/stop/{g_sid}", headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"})

    # Verify strict per-broker sandbox orderbook isolation
    d_ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker="deltaexchange").json()["data"]["orders"]
    g_ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker="groww").json()["data"]["orders"]
    f_ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker="fyers").json()["data"]["orders"]

    assert any(str(o["orderid"]) == str(d_res["resp"]["orderid"]) for o in d_ob)
    assert not any(str(o["orderid"]) == str(g_res["resp"]["orderid"]) for o in d_ob)
    assert any(str(o["orderid"]) == str(g_res["resp"]["orderid"]) for o in g_ob)
    assert not any(str(o["orderid"]) == str(d_res["resp"]["orderid"]) for o in g_ob)
    assert f_ob == []
    srv.assert_zero_real_broker_order_calls()


# ===========================================================================
# Pairwise 12–14: WS Proxy :8765 × Sandbox Fills × MIS Square-Off
# ===========================================================================


def test_t3_12_multibroker_ws_proxy_concurrent_auth_subscribe_and_orders_no_eviction(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Pair: WS proxy :8765 concurrent 3-broker auth × subscribe + subscribe_orders without sibling eviction."""
    proxy = ws_proxy_factory(server=cloud_server)
    subs = {
        "deltaexchange": ("CRYPTO", "BTCUSDFUT", "LTP", 65450.0, "T3-ORD-DELTA"),
        "fyers": ("NSE", "SBIN", "Quote", 811.25, "T3-ORD-FYERS"),
        "groww": ("NFO", "NIFTY27MAR25FUT", "Depth", 24210.0, "T3-ORD-GROWW"),
    }

    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws_d, ws_connect(
        proxy.ws_url, open_timeout=5.0
    ) as ws_f, ws_connect(proxy.ws_url, open_timeout=5.0) as ws_g:
        sockets = {"deltaexchange": ws_d, "fyers": ws_f, "groww": ws_g}
        user_ids: dict[str, str] = {}

        for broker, sock in sockets.items():
            ack = _ws_authenticate(sock, cloud_server.api_key, broker=broker)
            assert ack.get("status") == "success" and str(ack.get("broker", "")).lower() == broker, ack
            user_ids[broker] = str(ack.get("user_id") or cloud_server.username)

            exch, sym, mode, _, _ = subs[broker]
            sock.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "symbols": [{"symbol": sym, "exchange": exch}],
                        "mode": mode,
                        "request_id": f"t3-sub-{broker}",
                    }
                )
            )
            sub_ack = _ws_recv_matching(sock, lambda m: m.get("type") == "subscribe")
            assert sub_ack.get("status") == "success", sub_ack

            sock.send(json.dumps({"action": "subscribe_orders", "request_id": f"t3-ordsub-{broker}"}))
            ord_ack = _ws_recv_matching(sock, lambda m: m.get("type") == "subscribe_orders")
            assert ord_ack.get("status") == "success", ord_ack

        # Publish both market ticks and order updates for all 3 brokers simultaneously
        for broker, (exch, sym, mode, ltp, oid) in subs.items():
            for _ in range(2):
                proxy.publish_tick(exch, sym, mode.upper(), {"symbol": sym, "exchange": exch, "ltp": ltp})
                proxy.publish_order_update(
                    "ANALYZE",
                    user_ids[broker],
                    {
                        "type": "order_update",
                        "user_id": user_ids[broker],
                        "mode": "analyze",
                        "broker": broker,
                        "orderid": oid,
                        "symbol": sym,
                        "exchange": exch,
                        "action": "BUY",
                        "quantity": 1,
                        "order_status": "complete",
                    },
                )
                time.sleep(0.08)

        for broker, sock in sockets.items():
            _, sym, _, ltp, oid = subs[broker]
            got_tick = False
            got_order = False
            deadline = time.monotonic() + 6.0
            while time.monotonic() < deadline and not (got_tick and got_order):
                try:
                    msg = _ws_recv_json(sock, timeout=1.0)
                except TimeoutError:
                    continue
                if msg.get("type") == "market_data" and msg.get("symbol") == sym:
                    assert str(msg.get("broker", "")).lower() == broker
                    assert float(msg.get("data", {}).get("ltp", 0.0)) == pytest.approx(ltp, rel=1e-4)
                    got_tick = True
                elif msg.get("type") == "order_update" and msg.get("orderid") == oid:
                    assert str(msg.get("broker", "")).lower() == broker
                    assert msg.get("mode") == "analyze"
                    got_order = True
            assert got_tick, f"Broker {broker} missed market tick (evicted?)"
            assert got_order, f"Broker {broker} missed order_update (evicted?)"


@pytest.mark.timeout(180)
def test_t3_13_ws_proxy_market_ticks_with_sandbox_limit_sl_fills_and_order_update_fanout(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Pair: WS proxy market ticks × Sandbox LIMIT/SL fills × order_update fan-out across Delta & Fyers."""
    srv = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"SANDBOX_ENGINE_TYPE": "polling", "SANDBOX_POLL_INTERVAL": "1"},
    )
    proxy = ws_proxy_factory(server=srv)

    srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0, low=770.0, high=830.0)
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0, low=63000.0, high=68000.0)

    lim_oid = _place_paper_order(
        srv, "fyers", action="BUY", quantity=2, pricetype="LIMIT", price=790.0, strategy="t3_13_lim"
    )["orderid"]
    sl_oid = _place_paper_order(
        srv,
        "deltaexchange",
        action="BUY",
        quantity=1,
        pricetype="SL",
        trigger_price=66500.0,
        price=66600.0,
        strategy="t3_13_sl",
    )["orderid"]

    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws_f, ws_connect(proxy.ws_url, open_timeout=5.0) as ws_d:
        ack_f = _ws_authenticate(ws_f, srv.api_key, broker="fyers")
        ack_d = _ws_authenticate(ws_d, srv.api_key, broker="deltaexchange")
        assert ack_f.get("status") == "success" and ack_d.get("status") == "success"

        ws_f.send(json.dumps({"action": "subscribe_orders"}))
        ws_d.send(json.dumps({"action": "subscribe_orders"}))
        assert _ws_recv_matching(ws_f, lambda m: m.get("type") == "subscribe_orders").get("status") == "success"
        assert _ws_recv_matching(ws_d, lambda m: m.get("type") == "subscribe_orders").get("status") == "success"

        # Cross both LIMIT (fyers <= 790) and SL (delta >= 66500 and <= 66600)
        srv.set_mock_quote("fyers", "NSE", "SBIN", ltp=788.0, low=770.0, high=830.0)
        srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=66550.0, low=63000.0, high=68000.0)

        deadline = time.monotonic() + 20.0
        f_status, d_status = "", ""
        while time.monotonic() < deadline:
            f_ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker="fyers").json()["data"]["orders"]
            d_ob = srv.api_post("/api/v1/orderbook", {"apikey": srv.api_key}, broker="deltaexchange").json()["data"]["orders"]
            f_status = str(next(o for o in f_ob if str(o["orderid"]) == str(lim_oid))["order_status"]).lower()
            d_status = str(next(o for o in d_ob if str(o["orderid"]) == str(sl_oid))["order_status"]).lower()
            if f_status == "complete" and d_status == "complete":
                break
            time.sleep(0.4)

        assert f_status == "complete", f"Fyers LIMIT order did not fill: status={f_status}"
        assert d_status == "complete", f"Delta SL order did not fill: status={d_status}"

        # Publish matching analyze order_update events through proxy and verify broker-tagged fan-out
        uid_f = str(ack_f.get("user_id") or srv.username)
        uid_d = str(ack_d.get("user_id") or srv.username)
        for _ in range(2):
            proxy.publish_order_update(
                "ANALYZE",
                uid_f,
                {
                    "type": "order_update",
                    "user_id": uid_f,
                    "mode": "analyze",
                    "broker": "fyers",
                    "orderid": str(lim_oid),
                    "symbol": "SBIN",
                    "exchange": "NSE",
                    "order_status": "complete",
                },
            )
            proxy.publish_order_update(
                "ANALYZE",
                uid_d,
                {
                    "type": "order_update",
                    "user_id": uid_d,
                    "mode": "analyze",
                    "broker": "deltaexchange",
                    "orderid": str(sl_oid),
                    "symbol": "BTCUSDFUT",
                    "exchange": "CRYPTO",
                    "order_status": "complete",
                },
            )
            time.sleep(0.1)

        f_evt = _ws_recv_matching(ws_f, lambda m: m.get("type") == "order_update" and str(m.get("orderid")) == str(lim_oid))
        d_evt = _ws_recv_matching(ws_d, lambda m: m.get("type") == "order_update" and str(m.get("orderid")) == str(sl_oid))
        assert str(f_evt.get("broker", "")).lower() == "fyers" and f_evt.get("mode") == "analyze"
        assert str(d_evt.get("broker", "")).lower() == "deltaexchange" and d_evt.get("mode") == "analyze"

    srv.assert_zero_real_broker_order_calls()


def test_t3_14_multibroker_sandbox_mis_squareoff_nse_vs_crypto_exempt(server_factory) -> None:
    """Pair: Multi-broker Sandbox MIS square-off (fyers/groww NSE isolated) × deltaexchange CRYPTO exempt."""
    srv = server_factory(profile="cloud", paper_trading_only=True)

    # Open 24/7-safe CNC positions on Fyers & Groww (NSE) and NRML perpetual position on Delta (CRYPTO)
    _place_paper_order(srv, "fyers", symbol="SBIN", exchange="NSE", product="CNC", action="BUY", quantity=3)
    _place_paper_order(srv, "groww", symbol="SBIN", exchange="NSE", product="CNC", action="BUY", quantity=4)
    _place_paper_order(srv, "deltaexchange", symbol="BTCUSDFUT", exchange="CRYPTO", product="NRML", action="BUY", quantity=2)

    with srv.session_client("fyers") as f_cli, srv.session_client("groww") as g_cli, srv.session_client(
        "deltaexchange"
    ) as d_cli:
        # Mutate Fyers NSE MIS square-off time and reload independently of Groww
        upd_f = f_cli.post("/sandbox/update", json={"config_key": "nse_bse_square_off_time", "config_value": "15:12"})
        assert upd_f.status_code == 200, upd_f.text
        assert f_cli.post("/sandbox/reload-squareoff").status_code == 200

        upd_g = g_cli.post("/sandbox/update", json={"config_key": "nse_bse_square_off_time", "config_value": "15:18"})
        assert upd_g.status_code == 200, upd_g.text
        assert g_cli.post("/sandbox/reload-squareoff").status_code == 200

        f_cfg_txt = json.dumps(f_cli.get("/sandbox/api/configs").json())
        g_cfg_txt = json.dumps(g_cli.get("/sandbox/api/configs").json())
        assert "15:12" in f_cfg_txt and "15:18" not in f_cfg_txt
        assert "15:18" in g_cfg_txt and "15:12" not in g_cfg_txt

        # Verify all 3 brokers report independent squareoff-status and Delta CRYPTO has no crypto square-off schedule
        for name, cli in (("fyers", f_cli), ("groww", g_cli), ("deltaexchange", d_cli)):
            sq = cli.get("/sandbox/squareoff-status")
            assert sq.status_code == 200, f"squareoff-status failed for {name}: {sq.text}"
        d_cfg_txt = json.dumps(d_cli.get("/sandbox/api/configs").json()).lower()
        assert "crypto_square_off" not in d_cfg_txt, f"CRYPTO must be exempt from MIS square-off: {d_cfg_txt}"

    # Verify Delta CRYPTO position remains open & untouched
    d_pos = srv.api_post("/api/v1/positionbook", {"apikey": srv.api_key}, broker="deltaexchange").json()["data"]
    assert any(p["symbol"] == "BTCUSDFUT" and int(float(p["quantity"])) == 2 for p in d_pos), d_pos
    srv.assert_zero_real_broker_order_calls()


# ===========================================================================
# Pairwise 15–17: Cloud/Full × strategies/live/ × OpenScript Runner
# ===========================================================================


def test_t3_15_cloud_history_lockout_with_live_bar_strategy_bounded_state_zero_history_calls(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """Pair: Cloud history lockout × strategies/live/ live-bar execution + bounded .state.json + zero /history calls."""
    start_d, end_d = _recent_date_window(5)
    hist_check = cloud_server.api_post(
        "/api/v1/history",
        {
            "apikey": cloud_server.api_key,
            "symbol": "BTCUSDFUT",
            "exchange": "CRYPTO",
            "interval": "1m",
            "start_date": start_d,
            "end_date": end_d,
        },
        broker="deltaexchange",
    )
    _assert_historical_disabled_response(hist_check, allowed_statuses=(403,), context="pre-check cloud history")

    entry = _discover_live_strategy_entry("deltaexchange")
    state_file = tmp_path / "t3_15_delta.state.json"
    cloud_server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65200.0)

    cmd = _build_live_strategy_cmd(
        entry,
        broker="deltaexchange",
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        state_file=state_file,
        cycles=3,
        poll_seconds=0.15,
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
    proc = subprocess.run(cmd, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    combined = proc.stdout + "\n" + proc.stderr
    assert proc.returncode == 0, f"Live strategy failed under cloud lockout:\n{combined}"
    assert "HISTORICAL_DATA_DISABLED" not in combined.upper(), f"Strategy called /history in cloud:\n{combined}"

    candidates = [state_file] if state_file.is_file() else sorted(tmp_path.rglob("*.state.json"))
    assert candidates, f"Expected atomic .state.json in {tmp_path}"
    persisted = candidates[0]
    assert 0 < persisted.stat().st_size < 10 * 1024, f".state.json exceeded 10 KB: {persisted.stat().st_size}"
    assert not list(persisted.parent.glob("*.tmp")), "Dangling temp file after atomic write"
    assert cloud_server.duckdb_files() == []
    cloud_server.assert_zero_real_broker_order_calls()


def test_t3_16_full_local_mode_live_strategy_backtest_cli_with_paper_lock_enforced(
    full_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """Pair: OPENALGO_PROFILE=full × strategies/live/ --backtest CLI × PAPER_TRADING_ONLY=true."""
    entry = _discover_live_strategy_entry("fyers")
    state_file = tmp_path / "t3_16_backtest.state.json"

    cmd = _build_live_strategy_cmd(
        entry,
        broker="fyers",
        symbol="SBIN",
        exchange="NSE",
        state_file=state_file,
        backtest=True,
    )
    env = os.environ.copy()
    env.update(
        {
            "PYTHONPATH": str(REPO_ROOT),
            "OPENALGO_HOST": full_server.base_url,
            "OPENALGO_API_KEY": full_server.api_key,
            "OPENALGO_BROKER": "fyers",
            "OPENALGO_PROFILE": "full",
        }
    )
    proc = subprocess.run(cmd, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
    combined = proc.stdout + "\n" + proc.stderr
    assert proc.returncode == 0, f"--backtest failed in full profile:\n{combined}"
    assert any(tok in combined.lower() for tok in ("backtest", "trades", "pnl", "return", "bars")), combined

    # Confirm PAPER_TRADING_ONLY=true remains strictly enforced alongside local backtesting
    toggle = full_server.api_post(
        "/api/v1/analyzer/toggle", {"apikey": full_server.api_key, "mode": False}, broker="fyers"
    )
    _assert_paper_locked_response(toggle, context="full+backtest paper lock")
    full_server.assert_zero_real_broker_order_calls()


def test_t3_17_cloud_history_lockout_with_openscript_graceful_warmup_and_intervals_open(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """Pair: Cloud history lockout × openscript_runner.py graceful live-bar warmup × /api/v1/intervals open."""
    # Verify /api/v1/intervals and /openscript/runner/intervals remain open in cloud
    int_resp = cloud_server.api_post("/api/v1/intervals", {"apikey": cloud_server.api_key}, broker="deltaexchange")
    assert int_resp.status_code == 200 and int_resp.json().get("status") == "success", int_resp.text

    with cloud_server.session_client("deltaexchange") as cli:
        os_int = cli.get("/openscript/runner/intervals")
        assert os_int.status_code == 200, f"/openscript/runner/intervals blocked in cloud: {os_int.text[:300]}"

    scripts_dir = tmp_path / "strategies" / "openscript"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (scripts_dir / "firstbar.oscript").write_text(OPENSCRIPT_FIRST_BAR_SOURCE, encoding="utf-8")
    (scripts_dir / "firstbar.oscript.program.json").write_text(OPENSCRIPT_FIRST_BAR_PROGRAM, encoding="utf-8")

    runner_script = REPO_ROOT / "openscript_host" / "openscript_runner.py"
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
            "0.3",
            "--cycles",
            "2",
        ],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
        timeout=30.0,
    )
    combined = proc.stdout + "\n" + proc.stderr
    assert proc.returncode == 0, f"OpenScript runner crashed under cloud lockout:\n{combined}"
    assert "History answered with no bars. Waiting." not in combined, combined
    assert any(
        tok in combined.lower() for tok in ("live", "warmup", "warm-up", "history disabled", "historical data")
    ), combined
    assert cloud_server.duckdb_files() == []
    cloud_server.assert_zero_real_broker_order_calls()


# ===========================================================================
# Pairwise 18–22: Concurrency/Hygiene, MCP, Monitoring & Full Market-Data Matrix
# ===========================================================================


@pytest.mark.timeout(180)
def test_t3_18_concurrent_three_broker_burst_zero_broker_context_errors_and_health_db_hygiene(
    server_factory, tmp_path: Path
) -> None:
    """Pair: Concurrent 3-broker burst × zero BrokerContextError in errors.jsonl × health.db 2d retention & bounds."""
    shared_root = tmp_path / "t3_18_hygiene_root"
    srv1 = server_factory(
        profile="cloud",
        paper_trading_only=True,
        root_dir=shared_root,
        extra_env={"HEALTH_SAMPLE_INTERVAL": "1"},
    )
    srv1.stop()

    old_ts = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
    fresh_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
    for broker in BROKERS:
        hdb = srv1.broker_db_path(broker, "health.db")
        assert hdb.is_file(), f"Missing health.db for {broker}"
        with sqlite3.connect(str(hdb)) as conn:
            conn.execute(
                "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 777, 'pass')",
                (old_ts,),
            )
            conn.execute(
                "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 888, 'pass')",
                (fresh_ts,),
            )
            conn.commit()

    srv2 = server_factory(
        profile="cloud",
        paper_trading_only=True,
        root_dir=shared_root,
        extra_env={"HEALTH_SAMPLE_INTERVAL": "1"},
    )

    def _burst_call(i: int) -> int:
        broker = BROKERS[i % len(BROKERS)]
        spec = DEFAULT_SYMBOLS[broker]
        mod = i % 4
        if mod == 0:
            r = srv2.api_post(
                "/api/v1/quotes",
                {"apikey": srv2.api_key, "symbol": spec["symbol"], "exchange": spec["exchange"]},
                broker=broker,
            )
        elif mod == 1:
            r = srv2.api_post("/api/v1/intervals", {"apikey": srv2.api_key}, broker=broker)
        elif mod == 2:
            r = srv2.api_post("/api/v1/funds", {"apikey": srv2.api_key}, broker=broker)
        else:
            r = srv2.api_post(
                "/api/v1/placeorder",
                {
                    "apikey": srv2.api_key,
                    "strategy": f"t3_18_{broker}",
                    "exchange": spec["exchange"],
                    "symbol": spec["symbol"],
                    "action": "BUY",
                    "quantity": 1,
                    "pricetype": "MARKET",
                    "product": spec["product"],
                },
                broker=broker,
            )
        return r.status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        statuses = list(pool.map(_burst_call, range(36)))

    assert all(code == 200 for code in statuses), f"Non-200 status during 3-broker burst: {statuses}"

    with srv2.session_client("fyers") as cli:
        assert cli.get("/health/check").status_code == 200

    time.sleep(2.0)

    # Zero BrokerContextError in errors.jsonl
    ctx_errs = [e for e in srv2.read_error_lines() if "BrokerContextError" in json.dumps(e)]
    assert ctx_errs == [], f"Found BrokerContextError under concurrent 3-broker burst: {ctx_errs[:3]}"

    # Verify 2-day retention pruning, incremental auto_vacuum, and bounded thread_details per broker
    for broker in BROKERS:
        hdb = srv2.broker_db_path(broker, "health.db")
        with sqlite3.connect(str(hdb)) as conn:
            auto_vac = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0])
            old_left = int(conn.execute("SELECT COUNT(*) FROM health_metrics WHERE fd_count = 777").fetchone()[0])
            fresh_left = int(conn.execute("SELECT COUNT(*) FROM health_metrics WHERE fd_count = 888").fetchone()[0])
            td_rows = conn.execute(
                "SELECT thread_details FROM health_metrics WHERE thread_details IS NOT NULL LIMIT 10"
            ).fetchall()
        assert auto_vac == 2, f"[{broker}] health.db auto_vacuum must be 2 (INCREMENTAL), got {auto_vac}"
        assert old_left == 0, f"[{broker}] aged (>2d) health_metrics row was not pruned"
        assert fresh_left == 1, f"[{broker}] fresh (<2d) health_metrics row was wrongly pruned"
        for (td_raw,) in td_rows:
            td_str = td_raw if isinstance(td_raw, str) else json.dumps(td_raw)
            assert len(td_str.encode("utf-8")) < 4096, f"[{broker}] thread_details exceeded 4 KB"

    assert srv2.duckdb_files() == []
    srv2.assert_zero_real_broker_order_calls()


def test_t3_19_cloud_mcp_http_history_tool_block_and_analyzer_toggle_paper_lock(server_factory) -> None:
    """Pair: Cloud profile × MCP HTTP (POST /mcp) history/indicator tool block × analyzer_toggle paper lock."""
    srv = server_factory(profile="cloud", paper_trading_only=True, mcp_http_enabled=True)
    token = _mint_mcp_bearer_token(srv, scope="read:market read:account write:orders", broker="fyers")
    start_d, end_d = _recent_date_window(7)

    # 1. Blocked historical & indicator MCP tools
    for rpc_id, tool_name, args in (
        (
            1,
            "get_historical_data",
            {"symbol": "SBIN", "exchange": "NSE", "interval": "D", "start_date": start_d, "end_date": end_d},
        ),
        (
            2,
            "calculate_indicator",
            {"symbol": "SBIN", "exchange": "NSE", "interval": "D", "indicator": "ema", "period": 5},
        ),
    ):
        resp = srv.api_post(
            "/mcp",
            payload={
                "jsonrpc": "2.0",
                "id": rpc_id,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": args},
            },
            broker="fyers",
            headers={"Authorization": f"Bearer {token}"},
        )
        if resp.status_code in (403, 503):
            _assert_historical_disabled_response(resp, context=f"MCP {tool_name}")
        else:
            assert resp.status_code == 200, f"Unexpected HTTP {resp.status_code} for {tool_name}: {resp.text[:300]}"
            rpc = resp.json()
            assert bool(rpc.get("error")) or bool(rpc.get("result", {}).get("isError")), (
                f"MCP {tool_name} must return JSON-RPC error / isError in cloud: {rpc}"
            )
            assert "HISTORICAL_DATA_DISABLED" in resp.text, (
                f"MCP {tool_name} must report HISTORICAL_DATA_DISABLED in cloud: {resp.text[:400]}"
            )

    # 2. Blocked analyzer_toggle(mode=False) via MCP
    tog_resp = srv.api_post(
        "/mcp",
        payload={
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "analyzer_toggle", "arguments": {"mode": False}},
        },
        broker="fyers",
        headers={"Authorization": f"Bearer {token}"},
    )
    if tog_resp.status_code == 403:
        _assert_paper_locked_response(tog_resp, context="MCP analyzer_toggle")
    else:
        assert tog_resp.status_code == 200, f"Unexpected HTTP {tog_resp.status_code}: {tog_resp.text[:300]}"
        tog_rpc = tog_resp.json()
        assert bool(tog_rpc.get("error")) or bool(tog_rpc.get("result", {}).get("isError")), (
            f"MCP analyzer_toggle(mode=false) must return JSON-RPC error / isError: {tog_rpc}"
        )
        assert "PAPER_TRADING_ONLY" in tog_resp.text, (
            f"MCP analyzer_toggle(mode=false) must report PAPER_TRADING_ONLY: {tog_resp.text[:400]}"
        )

    an = srv.api_post("/api/v1/analyzer", {"apikey": srv.api_key}, broker="fyers").json()
    assert an["data"]["analyze_mode"] is True
    assert srv.duckdb_files() == []
    srv.assert_zero_real_broker_order_calls()


@pytest.mark.timeout(180)
def test_t3_20_monitor_production_cloud_expired_fyers_groww_warn_vs_active_delta_paper_ok(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """Pair: scripts/monitor_production.py × cloud profile × active Delta paper OK -> expired Fyers/Groww WARN."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    proxy = ws_proxy_factory(server=srv)
    out_jsonl = tmp_path / "t3_20_monitor.jsonl"

    # Place an active Delta paper position so Delta has live sandbox state
    _place_paper_order(srv, "deltaexchange", symbol="BTCUSDFUT", exchange="CRYPTO", product="NRML", action="BUY", quantity=1)

    # Probe 1: all 3 brokers healthy -> overall_status == "OK"
    proc1, recs1 = _run_monitor_once(
        base_url=srv.base_url,
        root_dir=srv.root_dir,
        output_jsonl=out_jsonl,
        api_key=srv.api_key,
        ws_port=proxy.ws_port,
    )
    assert proc1.returncode in (0, 1, 2), f"Monitor crashed: {proc1.stdout}\n{proc1.stderr}"
    assert len(recs1) == 1
    r1 = recs1[0]
    for key in REQUIRED_MONITOR_TOP_KEYS:
        assert key in r1, f"Missing monitor key {key}: {r1}"
    assert r1["overall_status"] == "OK", f"Expected OK on healthy cloud+Delta target: {r1}"
    assert r1["https"].get("history_blocked") is True
    assert int(r1["brokers"]["deltaexchange"].get("sandbox_positions", 0)) >= 1

    # Expire Fyers & Groww OAuth tokens while keeping Delta active and paper-locked
    for broker in ("fyers", "groww"):
        with sqlite3.connect(str(srv.broker_db_path(broker, "openalgo.db"))) as conn:
            conn.execute("UPDATE auth SET is_revoked = 1")
            conn.commit()

    # Probe 2: expired Fyers & Groww -> overall_status == "WARN" (non-CRITICAL per Q7)
    proc2, recs2 = _run_monitor_once(
        base_url=srv.base_url,
        root_dir=srv.root_dir,
        output_jsonl=out_jsonl,
        api_key=srv.api_key,
        ws_port=proxy.ws_port,
    )
    assert proc2.returncode in (0, 1, 2), f"Monitor crashed on run 2: {proc2.stdout}\n{proc2.stderr}"
    assert len(recs2) == 2
    r2 = recs2[1]
    assert int(r2["check_number"]) == int(r1["check_number"]) + 1
    assert r2["overall_status"] == "WARN", f"Expected WARN for expired Fyers/Groww with active Delta: {r2}"
    severities = [str(a.get("severity", "")).upper() for a in r2["alerts"]]
    assert "WARN" in severities and "CRITICAL" not in severities, r2["alerts"]
    assert r2["brokers"]["deltaexchange"].get("analyze_mode") is True
    assert srv.duckdb_files() == []
    srv.assert_zero_real_broker_order_calls()


def test_t3_21_full_profile_mcp_history_allowed_while_mcp_analyzer_toggle_paper_locked(
    server_factory,
) -> None:
    """Pair: OPENALGO_PROFILE=full × PAPER_TRADING_ONLY=true × MCP history allowed while analyzer_toggle blocked."""
    srv = server_factory(profile="full", paper_trading_only=True, mcp_http_enabled=True)
    token = _mint_mcp_bearer_token(srv, scope="read:market read:account write:orders", broker="fyers")
    start_d, end_d = _recent_date_window(7)

    # 1. MCP get_historical_data MUST succeed in full profile (not HISTORICAL_DATA_DISABLED)
    hist_resp = srv.api_post(
        "/mcp",
        payload={
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {
                "name": "get_historical_data",
                "arguments": {
                    "symbol": "SBIN",
                    "exchange": "NSE",
                    "interval": "D",
                    "start_date": start_d,
                    "end_date": end_d,
                },
            },
        },
        broker="fyers",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert hist_resp.status_code == 200, f"Full profile MCP get_historical_data failed: {hist_resp.text[:400]}"
    assert "HISTORICAL_DATA_DISABLED" not in hist_resp.text, hist_resp.text[:400]
    rpc = hist_resp.json()
    assert not rpc.get("error"), f"Unexpected JSON-RPC error in full profile: {rpc}"
    assert not rpc.get("result", {}).get("isError"), f"Unexpected MCP tool error in full profile: {rpc}"

    # 2. MCP analyzer_toggle(mode=False) MUST STILL be blocked by PAPER_TRADING_ONLY=true
    tog_resp = srv.api_post(
        "/mcp",
        payload={
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {"name": "analyzer_toggle", "arguments": {"mode": False}},
        },
        broker="fyers",
        headers={"Authorization": f"Bearer {token}"},
    )
    if tog_resp.status_code == 403:
        _assert_paper_locked_response(tog_resp, context="full MCP analyzer_toggle")
    else:
        assert tog_resp.status_code == 200
        tog_rpc = tog_resp.json()
        assert bool(tog_rpc.get("error")) or bool(tog_rpc.get("result", {}).get("isError")), (
            f"MCP analyzer_toggle(mode=false) must return JSON-RPC error / isError: {tog_rpc}"
        )
        assert "PAPER_TRADING_ONLY" in tog_resp.text, tog_resp.text[:400]

    an = srv.api_post("/api/v1/analyzer", {"apikey": srv.api_key}, broker="fyers").json()
    assert an["data"]["analyze_mode"] is True
    srv.assert_zero_real_broker_order_calls()


def test_t3_22_cloud_live_multiquotes_depth_intervals_all_brokers_with_concurrent_history_rejection(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Pair: Cloud profile × live multiquotes/depth/intervals across all 3 brokers × concurrent history block."""
    start_d, end_d = _recent_date_window(5)

    # Live multiquotes across all 3 brokers
    mq_cases = [
        ("deltaexchange", [{"symbol": "BTCUSDFUT", "exchange": "CRYPTO"}, {"symbol": "BTCINR", "exchange": "CRYPTO"}]),
        ("fyers", [{"symbol": "SBIN", "exchange": "NSE"}, {"symbol": "CRUDEOIL25MARFUT", "exchange": "MCX"}]),
        ("groww", [{"symbol": "SBIN", "exchange": "NSE"}, {"symbol": "NIFTY27MAR25FUT", "exchange": "NFO"}]),
    ]
    for broker, syms in mq_cases:
        mq = cloud_server.api_post(
            "/api/v1/multiquotes",
            {"apikey": cloud_server.api_key, "symbols": syms},
            broker=broker,
        )
        assert mq.status_code == 200, f"[{broker}] multiquotes failed in cloud: {mq.text[:300]}"
        assert mq.json().get("status") == "success"

        dep = cloud_server.api_post(
            "/api/v1/depth",
            {"apikey": cloud_server.api_key, "symbol": syms[0]["symbol"], "exchange": syms[0]["exchange"]},
            broker=broker,
        )
        assert dep.status_code == 200, f"[{broker}] depth failed in cloud: {dep.text[:300]}"
        assert dep.json().get("status") == "success"

        iv = cloud_server.api_post("/api/v1/intervals", {"apikey": cloud_server.api_key}, broker=broker)
        assert iv.status_code == 200 and iv.json().get("status") == "success", iv.text[:300]

        # Interleaved history request MUST still be blocked with 403 HISTORICAL_DATA_DISABLED
        hist = cloud_server.api_post(
            "/api/v1/history",
            {
                "apikey": cloud_server.api_key,
                "symbol": syms[0]["symbol"],
                "exchange": syms[0]["exchange"],
                "interval": "D",
                "start_date": start_d,
                "end_date": end_d,
            },
            broker=broker,
        )
        _assert_historical_disabled_response(hist, allowed_statuses=(403,), context=f"cloud history {broker}")

    assert cloud_server.duckdb_files() == []
    cloud_server.assert_zero_real_broker_order_calls()
