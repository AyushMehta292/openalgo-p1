"""Tier 1 E2E Happy-Path Tests: Multi-Broker Ops, Streaming, Sandbox, Live Strategies,
Stability & Production Monitoring (E2E-F6 through E2E-F11).

Strict opaque-box discipline:
- Interacts ONLY through public HTTP REST/session routes, WebSocket wire frames,
  CLI subprocesses, and observable filesystem/SQLite artifacts.
- Uses ONLY public fixtures and constants from ``test/e2e/conftest.py``.
- Never skips, xfails, mocks app logic, or weakens contract assertions.
"""

from __future__ import annotations

import ast
import concurrent.futures
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import pytest
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
    strategy: str = "e2e_t1_strat",
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
    return body


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
    poll_seconds: float = 0.2,
    backtest: bool = False,
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
    if backtest:
        assert "--backtest" in help_out, (
            f"Live strategy CLI {script_path} must expose --backtest flag per Interface Contract #5."
        )
        cmd.append("--backtest")
    else:
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
        for line in output_jsonl.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return proc, records


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


# ===========================================================================
# E2E-F6: Multi-Broker Isolation & Concurrent Routing (6 Tier 1 tests)
# ===========================================================================


@pytest.mark.tier1
def test_t1_f6_per_broker_sqlite_db_isolation_and_shared_db(server_factory) -> None:
    """Shared DB and per-broker SQLite DBs are isolated on disk; orders write only to target broker."""
    server = server_factory(profile="cloud", paper_trading_only=True)

    shared_db = server.db_dir / "shared" / "openalgo.db"
    assert shared_db.is_file(), f"Shared admin DB missing at {shared_db}"

    inodes: dict[str, set[int]] = {}
    for broker in BROKERS:
        db_file = server.broker_db_path(broker, "openalgo.db")
        assert db_file.is_file(), f"Broker openalgo.db missing for {broker}: {db_file}"
        inodes.setdefault("openalgo.db", set()).add(db_file.stat().st_ino)
    assert len(inodes["openalgo.db"]) == len(BROKERS), "Broker openalgo.db files must be distinct inodes"

    fyers_before = _sqlite_count(server.broker_db_path("fyers", "sandbox.db"), "sandbox_orders")
    groww_before = _sqlite_count(server.broker_db_path("groww", "sandbox.db"), "sandbox_orders")
    delta_before = _sqlite_count(server.broker_db_path("deltaexchange", "sandbox.db"), "sandbox_orders")

    _place_paper_order(server, "deltaexchange", action="BUY", quantity=1, strategy="t1_f6_iso")

    assert _sqlite_count(server.broker_db_path("deltaexchange", "sandbox.db"), "sandbox_orders") == (
        delta_before + 1
    )
    assert _sqlite_count(server.broker_db_path("fyers", "sandbox.db"), "sandbox_orders") == fyers_before
    assert _sqlite_count(server.broker_db_path("groww", "sandbox.db"), "sandbox_orders") == groww_before
    server.assert_zero_real_broker_order_calls()


@pytest.mark.tier1
def test_t1_f6_routing_precedence_header_query_and_shared_apikey(cloud_server: OpenAlgoE2EInstance) -> None:
    """/api/v1/* honors ?broker= > X-OpenAlgo-Broker > shared API key default cleanly."""
    # 1. Explicit X-OpenAlgo-Broker header routes to each broker
    for broker in BROKERS:
        resp = cloud_server.api_post("/api/v1/ping", {"apikey": cloud_server.api_key}, broker=broker)
        assert resp.status_code == 200, f"ping failed for {broker}: {resp.text}"
        body = resp.json()
        assert body.get("status") == "success"
        assert str(body.get("data", {}).get("broker", "")).lower() == broker

    # 2. Query param ?broker= overrides conflicting X-OpenAlgo-Broker header
    resp_q = cloud_server.api_post(
        "/api/v1/ping",
        {"apikey": cloud_server.api_key},
        broker="groww",
        query={"broker": "deltaexchange"},
    )
    assert resp_q.status_code == 200
    assert str(resp_q.json().get("data", {}).get("broker", "")).lower() == "deltaexchange"

    # 3. Backwards-compatible shared API key without explicit broker resolves without error
    resp_default = cloud_server.api_post("/api/v1/ping", {"apikey": cloud_server.api_key})
    assert resp_default.status_code == 200
    assert resp_default.json().get("status") == "success"
    assert str(resp_default.json().get("data", {}).get("broker", "")).lower() in BROKERS


@pytest.mark.tier1
def test_t1_f6_tab_session_isolation_and_url_prefix_precedence(cloud_server: OpenAlgoE2EInstance) -> None:
    """Concurrent browser sessions bind independent brokers via X-OpenAlgo-Broker and URL prefix."""
    with cloud_server.session_client("deltaexchange") as delta_cli, cloud_server.session_client(
        "fyers"
    ) as fyers_cli, cloud_server.session_client("groww") as groww_cli:
        r_delta = delta_cli.get("/python/api/exchanges")
        r_fyers = fyers_cli.get("/python/api/exchanges")
        r_groww = groww_cli.get("/python/api/exchanges")

        assert r_delta.status_code == 200, r_delta.text
        assert r_fyers.status_code == 200, r_fyers.text
        assert r_groww.status_code == 200, r_groww.text

        delta_text = json.dumps(r_delta.json()).upper()
        fyers_text = json.dumps(r_fyers.json()).upper()
        groww_text = json.dumps(r_groww.json()).upper()

        assert "CRYPTO" in delta_text, f"Delta exchanges should include CRYPTO: {delta_text}"
        assert "NSE" in fyers_text, f"Fyers exchanges should include NSE: {fyers_text}"
        assert "NSE" in groww_text, f"Groww exchanges should include NSE: {groww_text}"

        # URL prefix /<broker>/... takes precedence over conflicting ?broker= and header
        url_pref = fyers_cli.get(
            "/deltaexchange/auth",
            params={"broker": "groww"},
            headers={"X-OpenAlgo-Broker": "fyers", "Accept": "text/html"},
        )
        assert url_pref.status_code == 200, (
            f"URL-prefixed broker route /deltaexchange/auth returned unexpected status {url_pref.status_code}"
        )
        assert "X-OpenAlgo-Broker" in url_pref.text, (
            "URL-prefixed SPA route missing injected X-OpenAlgo-Broker tab hook"
        )


@pytest.mark.tier1
def test_t1_f6_delta_crypto_quotes_multiquotes_and_base_url(server_factory) -> None:
    """Delta Exchange supports CRYPTO quotes, multiquotes, and respects DELTA_BASE_URL."""
    custom_host = "custom-delta-test.delta.exchange"
    server = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"DELTA_BASE_URL": f"https://{custom_host}"},
    )
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=66123.5)
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCINR", ltp=5512345.0)

    q_resp = server.api_post(
        "/api/v1/quotes",
        {"apikey": server.api_key, "symbol": "BTCUSDFUT", "exchange": "CRYPTO"},
        broker="deltaexchange",
    )
    assert q_resp.status_code == 200, q_resp.text
    q_body = q_resp.json()
    assert q_body.get("status") == "success"
    assert float(q_body["data"]["ltp"]) == pytest.approx(66123.5, rel=1e-4)

    mq_resp = server.api_post(
        "/api/v1/multiquotes",
        {
            "apikey": server.api_key,
            "symbols": [
                {"symbol": "BTCUSDFUT", "exchange": "CRYPTO"},
                {"symbol": "BTCINR", "exchange": "CRYPTO"},
            ],
        },
        broker="deltaexchange",
    )
    assert mq_resp.status_code == 200, mq_resp.text
    mq_body = mq_resp.json()
    assert mq_body.get("status") == "success"
    results = mq_body.get("results") or mq_body.get("data") or []
    assert len(results) == 2, f"Expected 2 multiquotes entries for Delta, got: {mq_body}"

    # Verify custom DELTA_BASE_URL host was targeted (and intercepted cleanly by firewall)
    hosts_seen = {c.get("broker_host") for c in server.outbound_calls()}
    assert custom_host in hosts_seen, (
        f"Delta REST client did not honor DELTA_BASE_URL={custom_host}; observed hosts={hosts_seen}"
    )


@pytest.mark.tier1
def test_t1_f6_fyers_and_groww_exchange_quotes_contract(cloud_server: OpenAlgoE2EInstance) -> None:
    """Fyers (NSE/NFO/MCX) and Groww (NSE/NFO) resolve native exchange/symbol live quotes."""
    cases = [
        ("fyers", "NSE", "SBIN", 805.25),
        ("fyers", "NFO", "NIFTY27MAR25FUT", 24150.0),
        ("fyers", "MCX", "CRUDEOIL25MARFUT", 6240.0),
        ("groww", "NSE", "SBIN", 802.50),
        ("groww", "NFO", "NIFTY27MAR25FUT", 24180.0),
    ]
    for broker, exchange, symbol, ltp in cases:
        cloud_server.set_mock_quote(broker, exchange, symbol, ltp=ltp)
        resp = cloud_server.api_post(
            "/api/v1/quotes",
            {"apikey": cloud_server.api_key, "symbol": symbol, "exchange": exchange},
            broker=broker,
        )
        assert resp.status_code == 200, f"{broker}/{exchange}/{symbol} quote failed: {resp.text}"
        body = resp.json()
        assert body.get("status") == "success", body
        assert float(body["data"]["ltp"]) == pytest.approx(ltp, rel=1e-4)


@pytest.mark.tier1
def test_t1_f6_python_strategy_per_broker_isolation_and_broker_env(server_factory, tmp_path: Path) -> None:
    """Uploaded /python strategies are isolated per broker and receive OPENALGO_BROKER + API key."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    probe_file = tmp_path / "delta_probe_out.json"

    script_src = (
        "import json, os, time, urllib.request\n"
        f"out = {str(probe_file)!r}\n"
        "broker = os.environ.get('OPENALGO_BROKER', '')\n"
        "host = os.environ.get('OPENALGO_HOST', '')\n"
        "apikey = os.environ.get('OPENALGO_API_KEY', '')\n"
        "req = urllib.request.Request(\n"
        "    f'{host}/api/v1/ping',\n"
        "    data=json.dumps({'apikey': apikey}).encode('utf-8'),\n"
        "    headers={'Content-Type': 'application/json', 'X-OpenAlgo-Broker': broker},\n"
        "    method='POST',\n"
        ")\n"
        "with urllib.request.urlopen(req, timeout=10) as r:\n"
        "    ping_body = json.loads(r.read().decode('utf-8'))\n"
        "with open(out, 'w', encoding='utf-8') as f:\n"
        "    json.dump({'broker': broker, 'ping': ping_body}, f)\n"
        "time.sleep(2.0)\n"
    ).encode("utf-8")

    with server.session_client("deltaexchange") as delta_cli, server.session_client("fyers") as fyers_cli:
        up = delta_cli.post(
            "/python/new",
            data={
                "strategy_name": "DeltaProbe",
                "exchange": "CRYPTO",
                "schedule_start": "00:00",
                "schedule_days": json.dumps(["mon", "tue", "wed", "thu", "fri", "sat", "sun"]),
            },
            files={"strategy_file": ("delta_probe.py", script_src, "text/x-python")},
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        assert up.status_code in (200, 201), f"Upload failed: {up.status_code} {up.text[:300]}"
        up_body = up.json()
        assert up_body.get("status") == "success", up_body
        strategy_id = up_body["data"]["strategy_id"]

        # Delta lists it; Fyers does NOT see Delta's strategy
        delta_list = delta_cli.get("/python/api/strategies")
        fyers_list = fyers_cli.get("/python/api/strategies")
        assert delta_list.status_code == 200 and fyers_list.status_code == 200
        assert strategy_id in json.dumps(delta_list.json())
        assert strategy_id not in json.dumps(fyers_list.json())

        # Start and wait for subprocess to write broker probe output
        start_resp = delta_cli.post(
            f"/python/start/{strategy_id}",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )
        assert start_resp.status_code == 200, f"Start failed: {start_resp.text}"

        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline and not probe_file.exists():
            time.sleep(0.15)
        assert probe_file.exists(), "Python strategy subprocess did not execute or write probe file"

        probe = json.loads(probe_file.read_text(encoding="utf-8"))
        assert probe["broker"] == "deltaexchange"
        assert probe["ping"].get("status") == "success"
        assert str(probe["ping"].get("data", {}).get("broker", "")).lower() == "deltaexchange"

        delta_cli.post(
            f"/python/stop/{strategy_id}",
            headers={"Accept": "application/json", "X-Requested-With": "XMLHttpRequest"},
        )


# ===========================================================================
# E2E-F7: Multi-Broker WebSocket Proxy & Streaming (:8765) (6 Tier 1 tests)
# ===========================================================================


@pytest.mark.tier1
def test_t1_f7_ws_authenticate_explicit_broker_all_three(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """WebSocket proxy authenticates each of deltaexchange, fyers, and groww with explicit broker."""
    proxy = ws_proxy_factory(server=cloud_server)
    for broker in BROKERS:
        with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
            ack = _ws_authenticate(ws, cloud_server.api_key, broker=broker)
            assert ack.get("type") == "auth", ack
            assert ack.get("status") == "success", f"WS auth failed for {broker}: {ack}"
            assert str(ack.get("broker", "")).lower() == broker, (
                f"WS auth response returned wrong broker for {broker}: {ack}"
            )
            features = ack.get("supported_features") or {}
            assert features.get("ltp") is True
            assert features.get("quote") is True
            assert features.get("depth") is True


@pytest.mark.tier1
def test_t1_f7_ws_ping_pong_and_broker_info(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Authenticated WS client receives pong with echoed _pingId and supported broker metadata."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        ack = _ws_authenticate(ws, cloud_server.api_key, broker="fyers")
        assert ack.get("status") == "success", ack

        ws.send(json.dumps({"action": "ping", "timestamp": 1700000001234, "_pingId": "t1-ping-42"}))
        pong = _ws_recv_matching(ws, lambda m: m.get("type") == "pong")
        assert pong.get("status") == "success"
        assert pong.get("_pingId") == "t1-ping-42"
        assert "server_timestamp" in pong

        ws.send(json.dumps({"action": "get_supported_brokers"}))
        brokers_msg = _ws_recv_matching(
            ws, lambda m: m.get("type") in ("supported_brokers", "broker_info") or "brokers" in m
        )
        assert brokers_msg.get("status") == "success"
        serialized = json.dumps(brokers_msg).lower()
        for b in BROKERS:
            assert b in serialized, f"Expected broker {b} in supported brokers frame: {brokers_msg}"


@pytest.mark.tier1
def test_t1_f7_ws_subscribe_and_unsubscribe_numeric_and_string_modes(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Subscribe and unsubscribe work with numeric (1|2|3) and string (LTP|Quote|Depth) modes."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        ack = _ws_authenticate(ws, cloud_server.api_key, broker="deltaexchange")
        assert ack.get("status") == "success", ack

        for mode_in, expected_mode in [(1, "LTP"), ("Quote", "Quote"), (3, "Depth")]:
            req_id = f"sub-{mode_in}"
            ws.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "symbols": [{"symbol": "BTCUSDFUT", "exchange": "CRYPTO"}],
                        "mode": mode_in,
                        "request_id": req_id,
                    }
                )
            )
            sub_ack = _ws_recv_matching(ws, lambda m: m.get("type") == "subscribe")
            assert sub_ack.get("status") == "success", sub_ack
            assert sub_ack.get("request_id") == req_id
            assert str(sub_ack.get("broker", "")).lower() == "deltaexchange"
            subs = sub_ack.get("subscriptions") or []
            assert len(subs) == 1
            assert str(subs[0].get("mode", "")).lower() == expected_mode.lower()

        ws.send(
            json.dumps(
                {
                    "action": "unsubscribe",
                    "symbols": [{"symbol": "BTCUSDFUT", "exchange": "CRYPTO"}],
                    "mode": "LTP",
                    "request_id": "unsub-1",
                }
            )
        )
        unsub_ack = _ws_recv_matching(ws, lambda m: m.get("type") == "unsubscribe")
        assert unsub_ack.get("status") == "success", unsub_ack
        assert unsub_ack.get("request_id") == "unsub-1"


@pytest.mark.tier1
def test_t1_f7_ws_market_tick_fanout_tagged_with_broker(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Published ZMQ market ticks fan out to subscribed WS client tagged with broker."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        ack = _ws_authenticate(ws, cloud_server.api_key, broker="fyers")
        assert ack.get("status") == "success", ack

        ws.send(
            json.dumps(
                {
                    "action": "subscribe",
                    "symbols": [{"symbol": "SBIN", "exchange": "NSE"}],
                    "mode": "Quote",
                }
            )
        )
        sub_ack = _ws_recv_matching(ws, lambda m: m.get("type") == "subscribe")
        assert sub_ack.get("status") == "success", sub_ack

        # Publish tick on ZMQ; wait briefly for ZMQ subscriber join then emit
        for _ in range(3):
            proxy.publish_tick(
                "NSE",
                "SBIN",
                "QUOTE",
                {"symbol": "SBIN", "exchange": "NSE", "ltp": 812.45, "volume": 9001},
            )
            time.sleep(0.1)

        tick = _ws_recv_matching(
            ws,
            lambda m: m.get("type") == "market_data" and m.get("symbol") == "SBIN",
            timeout=6.0,
        )
        assert tick.get("exchange") == "NSE"
        assert str(tick.get("broker", "")).lower() == "fyers"
        assert float(tick.get("data", {}).get("ltp", 0.0)) == pytest.approx(812.45, rel=1e-4)


@pytest.mark.tier1
def test_t1_f7_ws_subscribe_orders_and_order_update_fanout(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """subscribe_orders receives broker-tagged order_update frames via ZMQ order topic."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws:
        ack = _ws_authenticate(ws, cloud_server.api_key, broker="deltaexchange")
        assert ack.get("status") == "success", ack
        user_id = str(ack.get("user_id") or cloud_server.username)

        ws.send(json.dumps({"action": "subscribe_orders", "request_id": "ord-sub-99"}))
        sub_ack = _ws_recv_matching(ws, lambda m: m.get("type") == "subscribe_orders")
        assert sub_ack.get("status") == "success", sub_ack
        assert sub_ack.get("request_id") == "ord-sub-99"

        order_payload = {
            "type": "order_update",
            "user_id": user_id,
            "mode": "analyze",
            "broker": "deltaexchange",
            "orderid": "E2E-ORD-1001",
            "symbol": "BTCUSDFUT",
            "exchange": "CRYPTO",
            "action": "BUY",
            "quantity": 1,
            "price": 65000.0,
            "order_status": "complete",
            "filled_quantity": 1,
            "pending_quantity": 0,
            "average_price": 65000.0,
        }
        for _ in range(3):
            proxy.publish_order_update("ANALYZE", user_id, order_payload)
            time.sleep(0.1)

        event = _ws_recv_matching(
            ws,
            lambda m: m.get("type") == "order_update" and m.get("orderid") == "E2E-ORD-1001",
            timeout=6.0,
        )
        assert str(event.get("broker", "")).lower() == "deltaexchange"
        assert event.get("order_status") == "complete"
        assert event.get("mode") == "analyze"


@pytest.mark.tier1
def test_t1_f7_ws_concurrent_three_brokers_no_sibling_eviction(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
) -> None:
    """Simultaneous (user_id, broker) WS sessions across all 3 brokers never evict siblings."""
    proxy = ws_proxy_factory(server=cloud_server)
    with ws_connect(proxy.ws_url, open_timeout=5.0) as ws_delta, ws_connect(
        proxy.ws_url, open_timeout=5.0
    ) as ws_fyers, ws_connect(proxy.ws_url, open_timeout=5.0) as ws_groww:
        sockets = {
            "deltaexchange": ws_delta,
            "fyers": ws_fyers,
            "groww": ws_groww,
        }
        subs = {
            "deltaexchange": ("CRYPTO", "BTCUSDFUT", 65432.0),
            "fyers": ("NSE", "SBIN", 808.0),
            "groww": ("NFO", "NIFTY27MAR25FUT", 24250.0),
        }

        for broker, sock in sockets.items():
            ack = _ws_authenticate(sock, cloud_server.api_key, broker=broker)
            assert ack.get("status") == "success", f"Auth failed for {broker}: {ack}"
            assert str(ack.get("broker", "")).lower() == broker

            exch, sym, _ = subs[broker]
            sock.send(
                json.dumps(
                    {
                        "action": "subscribe",
                        "symbols": [{"symbol": sym, "exchange": exch}],
                        "mode": "LTP",
                    }
                )
            )
            sub_ack = _ws_recv_matching(sock, lambda m: m.get("type") == "subscribe")
            assert sub_ack.get("status") == "success", sub_ack

        # After all 3 are authenticated & subscribed, verify EVERY sibling is alive & receives ticks
        for broker, sock in sockets.items():
            sock.send(json.dumps({"action": "ping", "_pingId": f"alive-{broker}"}))
            pong = _ws_recv_matching(sock, lambda m: m.get("type") == "pong")
            assert pong.get("_pingId") == f"alive-{broker}"

        for broker, (exch, sym, ltp) in subs.items():
            for _ in range(2):
                proxy.publish_tick(exch, sym, "LTP", {"symbol": sym, "exchange": exch, "ltp": ltp})
                time.sleep(0.08)

        for broker, sock in sockets.items():
            exch, sym, ltp = subs[broker]
            tick = _ws_recv_matching(
                sock,
                lambda m, s=sym: m.get("type") == "market_data" and m.get("symbol") == s,
                timeout=6.0,
            )
            assert str(tick.get("broker", "")).lower() == broker
            assert float(tick.get("data", {}).get("ltp", 0.0)) == pytest.approx(ltp, rel=1e-4)


# ===========================================================================
# E2E-F8: Multi-Broker Order Updates & Sandbox Execution / Square-Off (6 Tier 1 tests)
# ===========================================================================


@pytest.mark.tier1
def test_t1_f8_paper_market_order_books_and_position_all_brokers(server_factory) -> None:
    """Paper MARKET orders fill immediately and populate orderbook/tradebook/positionbook per broker."""
    server = server_factory(profile="cloud", paper_trading_only=True)

    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        placed = _place_paper_order(server, broker, action="BUY", quantity=2, pricetype="MARKET")
        assert placed.get("mode") == "analyze"
        orderid = placed["orderid"]
        assert orderid

        ob = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker=broker)
        assert ob.status_code == 200
        orders = ob.json()["data"]["orders"]
        matching = [o for o in orders if str(o.get("orderid")) == str(orderid)]
        assert matching and matching[0]["order_status"] == "complete", matching

        tb = server.api_post("/api/v1/tradebook", {"apikey": server.api_key}, broker=broker)
        assert tb.status_code == 200
        trades = tb.json()["data"]
        assert any(str(t.get("orderid")) == str(orderid) for t in trades), trades

        pb = server.api_post("/api/v1/positionbook", {"apikey": server.api_key}, broker=broker)
        assert pb.status_code == 200
        positions = pb.json()["data"]
        assert any(
            p.get("symbol") == spec["symbol"] and int(float(p.get("quantity", 0))) == 2
            for p in positions
        ), positions

        op = server.api_post(
            "/api/v1/openposition",
            {
                "apikey": server.api_key,
                "strategy": "e2e_t1_strat",
                "symbol": spec["symbol"],
                "exchange": spec["exchange"],
                "product": spec["product"],
            },
            broker=broker,
        )
        assert op.status_code == 200
        assert int(float(op.json().get("quantity", 0))) == 2

    server.assert_zero_real_broker_order_calls()


@pytest.mark.tier1
def test_t1_f8_paper_limit_modify_and_cancel_lifecycle(server_factory) -> None:
    """Resting paper LIMIT order can be modified and then cancelled cleanly."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0)

    placed = _place_paper_order(
        server, "fyers", action="BUY", quantity=5, pricetype="LIMIT", price=750.0, strategy="t1_lim"
    )
    orderid = placed["orderid"]

    # Confirm resting "open"
    ob1 = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker="fyers").json()
    row1 = next(o for o in ob1["data"]["orders"] if str(o["orderid"]) == str(orderid))
    assert row1["order_status"] == "open"
    assert float(row1["price"]) == pytest.approx(750.0)

    # Modify price & quantity
    mod = server.api_post(
        "/api/v1/modifyorder",
        {
            "apikey": server.api_key,
            "strategy": "t1_lim",
            "exchange": "NSE",
            "symbol": "SBIN",
            "orderid": orderid,
            "action": "BUY",
            "product": "CNC",
            "pricetype": "LIMIT",
            "price": 760.0,
            "quantity": 8,
            "disclosed_quantity": 0,
            "trigger_price": 0,
        },
        broker="fyers",
    )
    assert mod.status_code == 200, mod.text
    assert mod.json().get("status") == "success"

    ob2 = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker="fyers").json()
    row2 = next(o for o in ob2["data"]["orders"] if str(o["orderid"]) == str(orderid))
    assert float(row2["price"]) == pytest.approx(760.0)
    assert int(float(row2["quantity"])) == 8

    # Cancel order
    can = server.api_post(
        "/api/v1/cancelorder",
        {"apikey": server.api_key, "strategy": "t1_lim", "orderid": orderid},
        broker="fyers",
    )
    assert can.status_code == 200, can.text
    assert can.json().get("status") == "success"

    ob3 = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker="fyers").json()
    row3 = next(o for o in ob3["data"]["orders"] if str(o["orderid"]) == str(orderid))
    assert row3["order_status"] == "cancelled"


@pytest.mark.tier1
def test_t1_f8_paper_sl_and_slm_orders_and_funds_accounting(server_factory) -> None:
    """SL and SL-M orders rest in trigger pending and /api/v1/funds returns sandbox balances."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0)
    server.set_mock_quote("groww", "NFO", "NIFTY27MAR25FUT", ltp=24000.0)

    sl_order = _place_paper_order(
        server,
        "deltaexchange",
        action="BUY",
        quantity=1,
        pricetype="SL",
        trigger_price=67000.0,
        price=67100.0,
        strategy="t1_sl",
    )
    slm_order = _place_paper_order(
        server,
        "groww",
        symbol="NIFTY27MAR25FUT",
        exchange="NFO",
        product="NRML",
        action="SELL",
        quantity=75,
        pricetype="SL-M",
        trigger_price=23500.0,
        price=0.0,
        strategy="t1_slm",
    )

    delta_ob = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker="deltaexchange").json()
    delta_row = next(o for o in delta_ob["data"]["orders"] if str(o["orderid"]) == str(sl_order["orderid"]))
    assert delta_row["order_status"] == "trigger pending"

    groww_ob = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker="groww").json()
    groww_row = next(o for o in groww_ob["data"]["orders"] if str(o["orderid"]) == str(slm_order["orderid"]))
    assert groww_row["order_status"] == "trigger pending"

    for broker in ("deltaexchange", "groww"):
        funds = server.api_post("/api/v1/funds", {"apikey": server.api_key}, broker=broker)
        assert funds.status_code == 200
        f_body = funds.json()
        assert f_body.get("status") == "success"
        data = f_body["data"]
        assert float(data["availablecash"]) > 0.0
        for field in ("availablecash", "m2mrealized", "m2munrealized", "totalpnl"):
            assert field in data, f"Missing funds field {field} for {broker}: {data}"


@pytest.mark.tier1
def test_t1_f8_paper_gtt_single_and_oco_lifecycle(server_factory) -> None:
    """SINGLE and OCO paper GTT orders place, list in gttorderbook, modify, and cancel cleanly."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0)
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0)

    single_resp = server.api_post(
        "/api/v1/placegttorder",
        {
            "apikey": server.api_key,
            "strategy": "t1_gtt",
            "trigger_type": "SINGLE",
            "exchange": "NSE",
            "symbol": "SBIN",
            "action": "BUY",
            "product": "CNC",
            "quantity": 2,
            "pricetype": "LIMIT",
            "price": 770.0,
            "triggerprice_tg": 775.0,
        },
        broker="fyers",
    )
    assert single_resp.status_code == 200, single_resp.text
    single_body = single_resp.json()
    assert single_body.get("status") == "success"
    assert single_body.get("mode") == "analyze"
    single_tid = str(single_body["trigger_id"])
    assert single_tid.startswith("GTT-")

    oco_resp = server.api_post(
        "/api/v1/placegttorder",
        {
            "apikey": server.api_key,
            "strategy": "t1_gtt_oco",
            "trigger_type": "OCO",
            "exchange": "CRYPTO",
            "symbol": "BTCUSDFUT",
            "action": "SELL",
            "product": "NRML",
            "quantity": 1,
            "pricetype": "LIMIT",
            "price": 68000.0,
            "triggerprice_sl": 62000.0,
            "stoploss": 61900.0,
            "triggerprice_tg": 68000.0,
            "target": 68100.0,
        },
        broker="deltaexchange",
    )
    assert oco_resp.status_code == 200, oco_resp.text
    oco_tid = str(oco_resp.json()["trigger_id"])
    assert oco_tid.startswith("GTT-")

    fyers_gtt_book = server.api_post("/api/v1/gttorderbook", {"apikey": server.api_key}, broker="fyers")
    assert fyers_gtt_book.status_code == 200
    assert single_tid in json.dumps(fyers_gtt_book.json())
    assert oco_tid not in json.dumps(fyers_gtt_book.json())

    cancel_gtt = server.api_post(
        "/api/v1/cancelgttorder",
        {"apikey": server.api_key, "strategy": "t1_gtt", "trigger_id": single_tid},
        broker="fyers",
    )
    assert cancel_gtt.status_code == 200, cancel_gtt.text
    assert cancel_gtt.json().get("status") == "success"


@pytest.mark.tier1
def test_t1_f8_concurrent_per_broker_limit_fills_without_singleton_clobber(server_factory) -> None:
    """Resting LIMIT orders across Delta, Fyers, and Groww ALL execute concurrently on price cross."""
    server = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"SANDBOX_ENGINE_TYPE": "polling", "SANDBOX_POLL_INTERVAL": "1"},
    )
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65000.0, low=63000.0, high=67000.0)
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=800.0, low=770.0, high=820.0)
    server.set_mock_quote("groww", "NSE", "SBIN", ltp=800.0, low=770.0, high=820.0)

    order_ids = {
        "deltaexchange": _place_paper_order(
            server, "deltaexchange", action="BUY", quantity=1, pricetype="LIMIT", price=64000.0
        )["orderid"],
        "fyers": _place_paper_order(
            server, "fyers", action="BUY", quantity=2, pricetype="LIMIT", price=790.0
        )["orderid"],
        "groww": _place_paper_order(
            server, "groww", action="BUY", quantity=3, pricetype="LIMIT", price=785.0
        )["orderid"],
    }

    # Cross limit prices simultaneously on all 3 brokers
    server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=63800.0, low=63000.0, high=67000.0)
    server.set_mock_quote("fyers", "NSE", "SBIN", ltp=788.0, low=770.0, high=820.0)
    server.set_mock_quote("groww", "NSE", "SBIN", ltp=782.0, low=770.0, high=820.0)

    deadline = time.monotonic() + 20.0
    statuses: dict[str, str] = {}
    while time.monotonic() < deadline:
        for broker, oid in order_ids.items():
            ob = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker=broker).json()
            row = next(o for o in ob["data"]["orders"] if str(o["orderid"]) == str(oid))
            statuses[broker] = str(row["order_status"]).lower()
        if all(st == "complete" for st in statuses.values()):
            break
        time.sleep(0.4)

    assert statuses == {
        "deltaexchange": "complete",
        "fyers": "complete",
        "groww": "complete",
    }, f"Not all 3 brokers filled concurrently (singleton clobber?): {statuses}"


@pytest.mark.tier1
def test_t1_f8_per_broker_sandbox_squareoff_and_order_update_isolation(server_factory) -> None:
    """Sandbox square-off schedulers and configs operate independently per broker."""
    server = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"ORDER_UPDATES_ENABLED": "TRUE"},
    )
    for broker in BROKERS:
        with server.session_client(broker) as cli:
            cfg = cli.get("/sandbox/api/configs")
            assert cfg.status_code == 200, f"sandbox configs failed for {broker}: {cfg.text}"

            sq = cli.get("/sandbox/squareoff-status")
            assert sq.status_code == 200, f"squareoff-status failed for {broker}: {sq.text}"
            sq_body = sq.json()
            assert sq_body.get("status") != "error", f"squareoff-status returned error for {broker}: {sq_body}"
            assert sq_body.get("status") == "success" or sq_body.get("running") is True, (
                f"Unexpected squareoff-status body for {broker}: {sq_body}"
            )

            rel = cli.post("/sandbox/reload-squareoff")
            assert rel.status_code == 200, f"reload-squareoff failed for {broker}: {rel.text}"

    # Verify per-broker config mutation on fyers does not alter groww's sandbox config
    with server.session_client("fyers") as fyers_cli, server.session_client("groww") as groww_cli:
        upd = fyers_cli.post(
            "/sandbox/update",
            json={"config_key": "nse_bse_square_off_time", "config_value": "15:10"},
        )
        assert upd.status_code == 200, upd.text
        fyers_cfg = json.dumps(fyers_cli.get("/sandbox/api/configs").json())
        groww_cfg = json.dumps(groww_cli.get("/sandbox/api/configs").json())
        assert "15:10" in fyers_cfg
        assert "15:10" not in groww_cfg


# ===========================================================================
# E2E-F9: Zero-History Live-Bar Strategies & OpenScript Cloud Degradation (6 Tier 1 tests)
# ===========================================================================


@pytest.mark.tier1
def test_t1_f9_live_strategy_library_exists_and_no_pandas_numpy(tmp_path: Path) -> None:
    """strategies/live/ exists, has ZERO pandas/numpy imports statically and at runtime."""
    live_dir = _live_strategies_dir()
    assert live_dir.is_dir(), f"Missing tracked strategies/live/ directory: {live_dir}"

    py_files = sorted(live_dir.rglob("*.py"))
    assert py_files, f"No Python files found under {live_dir}"

    forbidden = {"pandas", "numpy"}
    for py_file in py_files:
        tree = ast.parse(py_file.read_text(encoding="utf-8"), filename=str(py_file))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    top = alias.name.split(".")[0]
                    assert top not in forbidden, f"{py_file} imports forbidden dependency {top}"
            elif isinstance(node, ast.ImportFrom) and node.module:
                top = node.module.split(".")[0]
                assert top not in forbidden, f"{py_file} imports from forbidden dependency {top}"

    # Runtime verification with shadowed pandas/numpy modules that raise ImportError
    blocker = tmp_path / "no_bloat_pkg"
    blocker.mkdir()
    for mod_name in ("pandas", "numpy"):
        (blocker / f"{mod_name}.py").write_text(
            f"raise ImportError('{mod_name} is forbidden in strategies/live/')\n",
            encoding="utf-8",
        )

    entry = _discover_live_strategy_entry()
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{blocker}{os.pathsep}{REPO_ROOT}"
    proc = subprocess.run(
        [sys.executable, str(entry), "--help"],
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=15.0,
    )
    assert proc.returncode == 0, (
        f"Live strategy {entry} failed without pandas/numpy:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )


@pytest.mark.tier1
def test_t1_f9_live_strategy_aggregates_bars_with_zero_history_calls(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """strategies/live/ aggregates live OHLCV bars via /api/v1/quotes with ZERO /api/v1/history calls."""
    entry = _discover_live_strategy_entry("deltaexchange")
    state_file = tmp_path / "delta_live.state.json"

    cloud_server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65100.0)
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
    assert proc.returncode == 0, (
        f"Live strategy exited {proc.returncode}:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
    assert cloud_server.duckdb_files() == [], "Cloud live strategy must never create DuckDB files"

    # Confirm zero /api/v1/history calls recorded in Delta traffic/API logs
    combined_out = (proc.stdout + "\n" + proc.stderr).upper()
    assert "HISTORICAL_DATA_DISABLED" not in combined_out, (
        f"Live strategy must NOT call /api/v1/history in cloud mode:\n{combined_out}"
    )
    cloud_server.assert_zero_real_broker_order_calls()


@pytest.mark.tier1
def test_t1_f9_live_strategy_atomic_state_json_under_10kb(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """Live-bar strategy persists atomic .state.json strictly < 10 KB with bars & EMA state."""
    entry = _discover_live_strategy_entry("deltaexchange")
    state_file = tmp_path / "atomic_check.state.json"
    cloud_server.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=65250.0)

    cmd = _build_live_strategy_cmd(
        entry,
        broker="deltaexchange",
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        state_file=state_file,
        cycles=2,
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
    assert proc.returncode == 0, f"Strategy failed: {proc.stdout}\n{proc.stderr}"

    # Locate persisted .state.json (either explicit state_file or generated in tmp_path)
    candidates = [state_file] if state_file.is_file() else sorted(tmp_path.rglob("*.state.json"))
    assert candidates, f"Expected atomic .state.json to be written under {tmp_path}"
    persisted = candidates[0]

    size_bytes = persisted.stat().st_size
    assert 0 < size_bytes < 10 * 1024, f".state.json must be < 10 KB, got {size_bytes} bytes"
    assert not list(persisted.parent.glob("*.tmp")), "Dangling .tmp file left behind (non-atomic write)"

    data = json.loads(persisted.read_text(encoding="utf-8"))
    assert isinstance(data, dict)
    keys_lower = {k.lower() for k in data.keys()}
    assert any("bar" in k or "candle" in k for k in keys_lower), (
        f".state.json missing rolling bars key: {list(data.keys())}"
    )


@pytest.mark.tier1
def test_t1_f9_live_strategy_runners_cover_all_three_brokers(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """strategies/live/ runs cleanly for deltaexchange, fyers, and groww with isolated state files."""
    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        entry = _discover_live_strategy_entry(broker)
        state_file = tmp_path / f"{broker}.state.json"

        cmd = _build_live_strategy_cmd(
            entry,
            broker=broker,
            symbol=spec["symbol"],
            exchange=spec["exchange"],
            state_file=state_file,
            cycles=2,
            poll_seconds=0.15,
        )
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(REPO_ROOT),
                "OPENALGO_HOST": cloud_server.base_url,
                "OPENALGO_API_KEY": cloud_server.api_key,
                "OPENALGO_BROKER": broker,
                "OPENALGO_PROFILE": "cloud",
            }
        )
        proc = subprocess.run(cmd, cwd=str(tmp_path), env=env, capture_output=True, text=True, timeout=30.0)
        assert proc.returncode == 0, (
            f"Live strategy failed for {broker} ({entry}):\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
        )
    cloud_server.assert_zero_real_broker_order_calls()


@pytest.mark.tier1
def test_t1_f9_live_strategy_backtest_cli_in_full_profile(
    full_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """In OPENALGO_PROFILE=full, strategies/live/ supports --backtest CLI using local history."""
    entry = _discover_live_strategy_entry("fyers")
    state_file = tmp_path / "backtest.state.json"

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
    assert proc.returncode == 0, (
        f"--backtest failed in full profile:\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
    output = (proc.stdout + "\n" + proc.stderr).lower()
    assert any(tok in output for tok in ("backtest", "trades", "pnl", "return", "bars")), (
        f"--backtest produced no recognizable summary output:\n{output}"
    )
    full_server.assert_zero_real_broker_order_calls()


@pytest.mark.tier1
def test_t1_f9_openscript_runner_cloud_graceful_live_bar_warmup(
    cloud_server: OpenAlgoE2EInstance, tmp_path: Path
) -> None:
    """OpenScript runner transitions gracefully to live-bar warmup when cloud blocks /api/v1/history."""
    scripts_dir = tmp_path / "strategies" / "openscript"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (scripts_dir / "firstbar.oscript").write_text(OPENSCRIPT_FIRST_BAR_SOURCE, encoding="utf-8")
    (scripts_dir / "firstbar.oscript.program.json").write_text(
        OPENSCRIPT_FIRST_BAR_PROGRAM, encoding="utf-8"
    )

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
    assert proc.returncode == 0, (
        f"OpenScript runner crashed or refused under cloud no-history (exit {proc.returncode}):\n{combined}"
    )
    assert "History answered with no bars. Waiting." not in combined, (
        f"OpenScript stalled on blocked history instead of degrading to live-bar warmup:\n{combined}"
    )
    assert any(
        phrase in combined.lower()
        for phrase in ("live", "warmup", "warm-up", "history disabled", "historical data")
    ), f"Expected explicit live-bar warmup log notice in OpenScript output:\n{combined}"


# ===========================================================================
# E2E-F10: Runtime Stability & Bounded Storage Hygiene (6 Tier 1 tests)
# ===========================================================================


@pytest.mark.tier1
def test_t1_f10_concurrent_multibroker_burst_zero_broker_context_errors(server_factory) -> None:
    """Concurrent multi-broker API burst produces ZERO HTTP 500s and ZERO BrokerContextErrors."""
    server = server_factory(profile="cloud", paper_trading_only=True)

    def _hit(idx: int) -> int:
        broker = BROKERS[idx % len(BROKERS)]
        spec = DEFAULT_SYMBOLS[broker]
        choice = idx % 4
        if choice == 0:
            r = server.api_post("/api/v1/ping", {"apikey": server.api_key}, broker=broker)
        elif choice == 1:
            r = server.api_post(
                "/api/v1/quotes",
                {"apikey": server.api_key, "symbol": spec["symbol"], "exchange": spec["exchange"]},
                broker=broker,
            )
        elif choice == 2:
            r = server.api_post("/api/v1/orderbook", {"apikey": server.api_key}, broker=broker)
        else:
            r = server.api_post("/api/v1/funds", {"apikey": server.api_key}, broker=broker)
        return r.status_code

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        statuses = list(pool.map(_hit, range(36)))

    assert all(code == 200 for code in statuses), f"Non-200 status during concurrent burst: {statuses}"

    # Allow async background executors (latency_monitor, apilog_db, analyzer_db, event_bus) to flush
    time.sleep(1.5)
    err_lines = server.read_error_lines()
    ctx_errors = [e for e in err_lines if "BrokerContextError" in json.dumps(e)]
    assert ctx_errors == [], f"Found BrokerContextError in errors.jsonl: {ctx_errors[:3]}"


@pytest.mark.tier1
def test_t1_f10_async_logs_and_latency_persisted_per_broker(server_factory) -> None:
    """Interleaved multi-broker orders write async API/latency logs to each broker's own DBs."""
    server = server_factory(profile="cloud", paper_trading_only=True)

    for broker in BROKERS:
        _place_paper_order(server, broker, action="BUY", quantity=1, strategy=f"t1_f10_{broker}")

    time.sleep(1.5)

    for broker in BROKERS:
        logs_db = server.broker_db_path(broker, "logs.db")
        latency_db = server.broker_db_path(broker, "latency.db")
        assert logs_db.is_file(), f"Missing broker logs.db for {broker}"
        assert latency_db.is_file(), f"Missing broker latency.db for {broker}"

    ctx_errors = [e for e in server.read_error_lines() if "BrokerContextError" in json.dumps(e)]
    assert ctx_errors == [], f"Background executor raised BrokerContextError: {ctx_errors[:3]}"


@pytest.mark.tier1
@pytest.mark.timeout(180)
def test_t1_f10_errors_jsonl_tail_bounded_startup_and_rotation(server_factory) -> None:
    """Pre-seeded >10 MB errors.jsonl boots cleanly via tail-bounded read and enforces 10 MB x 3 cap."""
    server = server_factory(profile="cloud", paper_trading_only=True, preseed_errors_mb=12.0)

    resp = server.api_post("/api/v1/ping", {"apikey": server.api_key}, broker="fyers")
    assert resp.status_code == 200

    err_path = server.errors_jsonl_path()
    assert err_path.exists()
    assert err_path.stat().st_size <= 11 * 1024 * 1024, (
        f"Active errors.jsonl was not bounded/rotated on startup: {err_path.stat().st_size} bytes"
    )
    rotated = sorted(server.log_dir.glob("errors.jsonl*"))
    assert len(rotated) <= 4, f"Expected at most active + 3 backups (4 files), found {rotated}"
    total_bytes = sum(p.stat().st_size for p in rotated)
    assert total_bytes <= 42 * 1024 * 1024, f"Total errors.jsonl* size exceeded cap: {total_bytes}"


@pytest.mark.tier1
@pytest.mark.timeout(180)
def test_t1_f10_health_db_two_day_retention_pruning(server_factory, tmp_path: Path) -> None:
    """health.db retention pruning removes health_metrics older than 2 days while keeping recent rows."""
    shared_root = tmp_path / "health_prune_root"
    srv1 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    health_db = srv1.broker_db_path("fyers", "health.db")
    srv1.stop()

    assert health_db.is_file(), f"Expected health.db at {health_db}"
    now = datetime.now(timezone.utc)
    old_5d = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
    old_9d = (now - timedelta(days=9)).strftime("%Y-%m-%d %H:%M:%S")
    fresh_1h = (now - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(str(health_db)) as conn:
        conn.execute(
            "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 10, 'pass')",
            (old_5d,),
        )
        conn.execute(
            "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 11, 'pass')",
            (old_9d,),
        )
        conn.execute(
            "INSERT INTO health_metrics (timestamp, fd_count, overall_status) VALUES (?, 12, 'pass')",
            (fresh_1h,),
        )
        conn.commit()

    # Reboot on same root_dir so startup retention pruning executes
    srv2 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    time.sleep(1.5)
    with sqlite3.connect(str(health_db)) as conn:
        old_remaining = conn.execute(
            "SELECT COUNT(*) FROM health_metrics WHERE fd_count IN (10, 11)"
        ).fetchone()[0]
        fresh_remaining = conn.execute(
            "SELECT COUNT(*) FROM health_metrics WHERE fd_count = 12"
        ).fetchone()[0]

    assert old_remaining == 0, f"Aged (>2d) health_metrics rows were not pruned: {old_remaining}"
    assert fresh_remaining == 1, f"Fresh (<2d) health_metrics row was wrongly pruned"
    srv2.stop()


@pytest.mark.tier1
@pytest.mark.timeout(180)
def test_t1_f10_health_db_incremental_vacuum_mode_and_compaction(server_factory, tmp_path: Path) -> None:
    """health.db configures SQLite incremental auto_vacuum and reclaims space after pruning."""
    shared_root = tmp_path / "health_vac_root"
    srv1 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    health_db = srv1.broker_db_path("fyers", "health.db")
    srv1.stop()

    with sqlite3.connect(str(health_db)) as conn:
        auto_vac = int(conn.execute("PRAGMA auto_vacuum").fetchone()[0])
    assert auto_vac == 2, f"Expected PRAGMA auto_vacuum=2 (INCREMENTAL) on health.db, got {auto_vac}"

    # Seed large aged rows to grow the DB file, then restart to trigger prune + incremental_vacuum
    old_ts = (datetime.now(timezone.utc) - timedelta(days=6)).strftime("%Y-%m-%d %H:%M:%S")
    big_blob = json.dumps([{"id": i, "name": "t" * 40, "daemon": True, "alive": True} for i in range(40)])
    with sqlite3.connect(str(health_db)) as conn:
        conn.executemany(
            "INSERT INTO health_metrics (timestamp, fd_count, thread_details, overall_status) "
            "VALUES (?, 99, ?, 'pass')",
            [(old_ts, big_blob) for _ in range(400)],
        )
        conn.commit()
    size_before = health_db.stat().st_size

    srv2 = server_factory(profile="cloud", paper_trading_only=True, root_dir=shared_root)
    time.sleep(1.5)
    with sqlite3.connect(str(health_db)) as conn:
        freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
    size_after = health_db.stat().st_size
    srv2.stop()

    assert size_after < size_before, (
        f"health.db did not shrink after pruning aged rows + incremental_vacuum: "
        f"before={size_before}, after={size_after}, freelist={freelist}"
    )


@pytest.mark.tier1
def test_t1_f10_health_db_bounded_thread_details_payload(server_factory) -> None:
    """Stored health_metrics.thread_details JSON payload is strictly bounded per row."""
    server = server_factory(
        profile="cloud",
        paper_trading_only=True,
        extra_env={"HEALTH_SAMPLE_INTERVAL": "1"},
    )
    with server.session_client("fyers") as cli:
        chk = cli.get("/health/check")
        assert chk.status_code == 200, chk.text

    health_db = server.broker_db_path("fyers", "health.db")
    deadline = time.monotonic() + 8.0
    rows: list[Any] = []
    while time.monotonic() < deadline:
        if health_db.is_file():
            with sqlite3.connect(str(health_db)) as conn:
                rows = conn.execute(
                    "SELECT thread_details FROM health_metrics WHERE thread_details IS NOT NULL LIMIT 10"
                ).fetchall()
            if rows:
                break
        time.sleep(0.3)

    assert rows, "No health_metrics rows with thread_details were recorded"
    for (td_raw,) in rows:
        td_str = td_raw if isinstance(td_raw, str) else json.dumps(td_raw)
        assert len(td_str.encode("utf-8")) < 4096, (
            f"health_metrics.thread_details payload exceeds 4 KB bound: {len(td_str)} bytes"
        )


# ===========================================================================
# E2E-F11: Production 3-Hour Monitoring Contract (6 Tier 1 tests)
# ===========================================================================


@pytest.mark.tier1
def test_t1_f11_monitor_emits_valid_10_field_jsonl(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """scripts/monitor_production.py appends valid 10-field JSONL matching Interface Contract #6."""
    proxy = ws_proxy_factory(server=cloud_server)
    out_jsonl = tmp_path / "monitor-3h.jsonl"

    proc, records = _run_monitor_once(
        base_url=cloud_server.base_url,
        root_dir=cloud_server.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_server.api_key,
        ws_port=proxy.ws_port,
    )
    assert proc.returncode in (0, 1, 2), f"Unexpected monitor crash: {proc.stdout}\n{proc.stderr}"
    assert len(records) == 1, f"Expected exactly 1 JSONL record, got {len(records)}"

    rec = records[0]
    for key in REQUIRED_MONITOR_TOP_KEYS:
        assert key in rec, f"Monitor JSONL record missing required Contract #6 key '{key}': {rec}"
    assert rec["overall_status"] in ("OK", "WARN", "CRITICAL")
    assert isinstance(rec["check_number"], int) and rec["check_number"] >= 1
    # Validate ISO-8601 UTC timestamp parses cleanly
    datetime.fromisoformat(str(rec["timestamp_utc"]).replace("Z", "+00:00"))


@pytest.mark.tier1
def test_t1_f11_monitor_healthy_cloud_target_reports_ok(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """Healthy cloud+paper server + WS proxy classifies overall_status as OK with zero CRITICAL alerts."""
    proxy = ws_proxy_factory(server=cloud_server)
    out_jsonl = tmp_path / "monitor-ok.jsonl"

    _, records = _run_monitor_once(
        base_url=cloud_server.base_url,
        root_dir=cloud_server.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_server.api_key,
        ws_port=proxy.ws_port,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["overall_status"] == "OK", f"Expected OK for healthy cloud target, got: {rec}"
    assert rec["https"].get("history_blocked") is True
    assert not any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec["alerts"])


@pytest.mark.tier1
@pytest.mark.timeout(180)
def test_t1_f11_monitor_expired_fyers_groww_tokens_classified_as_warn(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """Per Q7, expired Fyers & Groww daily OAuth tokens produce overall_status=WARN (not CRITICAL)."""
    server = server_factory(profile="cloud", paper_trading_only=True)
    proxy = ws_proxy_factory(server=server)

    # Expire/revoke Fyers and Groww tokens in their isolated broker DBs while keeping Delta active
    for broker in ("fyers", "groww"):
        with sqlite3.connect(str(server.broker_db_path(broker, "openalgo.db"))) as conn:
            conn.execute("UPDATE auth SET is_revoked = 1")
            conn.commit()

    out_jsonl = tmp_path / "monitor-warn.jsonl"
    _, records = _run_monitor_once(
        base_url=server.base_url,
        root_dir=server.root_dir,
        output_jsonl=out_jsonl,
        api_key=server.api_key,
        ws_port=proxy.ws_port,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["overall_status"] == "WARN", f"Expired Fyers/Groww tokens must yield WARN: {rec}"
    severities = [str(a.get("severity", "")).upper() for a in rec["alerts"]]
    assert "WARN" in severities, f"Expected WARN alert for expired Fyers/Groww tokens: {rec['alerts']}"
    assert "CRITICAL" not in severities, f"Expired Fyers/Groww must not trigger CRITICAL: {rec['alerts']}"


@pytest.mark.tier1
def test_t1_f11_monitor_history_block_breach_classified_as_critical(
    full_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """When /api/v1/history is NOT blocked (history-block breach), monitor classifies CRITICAL."""
    proxy = ws_proxy_factory(server=full_server)
    out_jsonl = tmp_path / "monitor-hist-crit.jsonl"

    _, records = _run_monitor_once(
        base_url=full_server.base_url,
        root_dir=full_server.root_dir,
        output_jsonl=out_jsonl,
        api_key=full_server.api_key,
        ws_port=proxy.ws_port,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["overall_status"] == "CRITICAL", f"History-block breach must be CRITICAL: {rec}"
    assert rec["https"].get("history_blocked") is False
    assert any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec["alerts"])


@pytest.mark.tier1
@pytest.mark.timeout(180)
def test_t1_f11_monitor_paper_lock_breach_classified_as_critical(
    server_factory,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """A broker with analyze_mode=False (paper-lock breach) causes monitor to report CRITICAL."""
    server = server_factory(profile="cloud", paper_trading_only=False)
    proxy = ws_proxy_factory(server=server)

    # Flip fyers settings analyze_mode=0 directly in isolated DB to simulate paper-lock breach
    with sqlite3.connect(str(server.broker_db_path("fyers", "openalgo.db"))) as conn:
        conn.execute("UPDATE settings SET analyze_mode = 0")
        conn.commit()

    out_jsonl = tmp_path / "monitor-paper-crit.jsonl"
    _, records = _run_monitor_once(
        base_url=server.base_url,
        root_dir=server.root_dir,
        output_jsonl=out_jsonl,
        api_key=server.api_key,
        ws_port=proxy.ws_port,
    )
    assert len(records) == 1
    rec = records[0]
    assert rec["overall_status"] == "CRITICAL", f"Paper-lock breach must be CRITICAL: {rec}"
    assert any(str(a.get("severity", "")).upper() == "CRITICAL" for a in rec["alerts"])


@pytest.mark.tier1
def test_t1_f11_monitor_subschemas_brokers_resources_errors_populated(
    cloud_server: OpenAlgoE2EInstance,
    ws_proxy_factory: Callable[..., WsProxyE2EInstance],
    tmp_path: Path,
) -> None:
    """Monitor JSONL sub-objects (https, services, resources, brokers, errors_last_3h) match Contract #6."""
    proxy = ws_proxy_factory(server=cloud_server)
    out_jsonl = tmp_path / "monitor-subschema.jsonl"

    _, records = _run_monitor_once(
        base_url=cloud_server.base_url,
        root_dir=cloud_server.root_dir,
        output_jsonl=out_jsonl,
        api_key=cloud_server.api_key,
        ws_port=proxy.ws_port,
    )
    assert len(records) == 1
    rec = records[0]

    for https_key in ("login", "health", "historify", "history_blocked", "ws"):
        assert https_key in rec["https"], f"Missing https.{https_key}: {rec['https']}"

    for svc_key in ("openalgo", "caddy", "ws_port_8765"):
        assert svc_key in rec["services"], f"Missing services.{svc_key}: {rec['services']}"

    for res_key in ("ram_avail_mb", "swap_used_mb", "disk_used_pct", "disk_avail_gb"):
        assert res_key in rec["resources"], f"Missing resources.{res_key}: {rec['resources']}"
        assert isinstance(rec["resources"][res_key], (int, float))

    for broker in BROKERS:
        assert broker in rec["brokers"], f"Missing brokers.{broker}: {rec['brokers']}"
        b_info = rec["brokers"][broker]
        for b_key in ("analyze_mode", "auth_status", "sandbox_positions", "sandbox_orders", "missed_exits"):
            assert b_key in b_info, f"Missing brokers.{broker}.{b_key}: {b_info}"

    err_keys = {k.lower() for k in rec["errors_last_3h"].keys()}
    assert any("brokercontexterror" in k or "broker_context" in k for k in err_keys), rec["errors_last_3h"]
    assert any("health" in k for k in err_keys), rec["errors_last_3h"]
    assert any("errors" in k or "total" in k for k in err_keys), rec["errors_last_3h"]
