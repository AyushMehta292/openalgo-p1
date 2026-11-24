"""Tier 2 E2E Boundary, Negative & Corner-Case Tests — Safety & Profiles (E2E-F1..E2E-F5).

Applies Category-Partition, Boundary-Value Analysis (BVA), forged-payload probes,
auth/CSRF boundary checks, env-var parsing edge cases, and concurrency stress strictly
through public HTTP, MCP JSON-RPC, process-env, and filesystem boundaries.

Coverage (30 tests, 6 per feature):
- E2E-F1: Paper-Trading Hard Lock boundary, fail-safe, bypass & concurrency tests
- E2E-F2: Cloud Zero-Historical-Storage & Backtesting Lockout boundary, env-override & bypass tests
- E2E-F3: Cloud Live-Trading Data Unhampered validation, boundary & mutation tests
- E2E-F4: Local Mode Full Parity boundary, validation & auth/CSRF tests
- E2E-F5: Frontend Runtime Config secret-leak, HTTP method, header/broker & unknown-profile tests
"""

from __future__ import annotations

import base64
import hashlib
import secrets
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from typing import Any
from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from .conftest import (
    BROKERS,
    DEFAULT_SYMBOLS,
    OpenAlgoE2EInstance,
    SessionClient,
)

pytestmark = pytest.mark.tier2


# ---------------------------------------------------------------------------
# Opaque-box assertion & protocol helpers
# ---------------------------------------------------------------------------


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
    assert isinstance(body.get("message"), str) and body["message"].strip()
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
    assert isinstance(body.get("message"), str) and body["message"].strip()
    return body


def _recent_date_window(days_back: int = 10) -> tuple[str, str]:
    end_d = date.today() - timedelta(days=1)
    start_d = end_d - timedelta(days=days_back)
    return start_d.isoformat(), end_d.isoformat()


def _create_signal_strategy(client: SessionClient, name: str = "E2E Tier2 Probe") -> int:
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
    assert isinstance(sid, int)
    return sid


def _mint_mcp_bearer_token(
    instance: OpenAlgoE2EInstance,
    *,
    scope: str = "read:market read:account write:orders",
    broker: str = "fyers",
) -> str:
    redirect_uri = "http://127.0.0.1:9877/callback"
    reg_resp = instance.api_post(
        "/oauth/register",
        payload={
            "client_name": "E2E Tier2 MCP Probe",
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
    session = instance.session_client(broker=broker)
    consent_get = session.get("/oauth/authorize", params=auth_params)
    assert consent_get.status_code == 200
    approve_resp = session.post(
        "/oauth/authorize",
        params=auth_params,
        data={"decision": "approve"},
    )
    assert approve_resp.status_code in (302, 303)
    qs = parse_qs(urlparse(approve_resp.headers.get("location", "")).query)
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
    assert token_resp.status_code == 200
    return str(token_resp.json()["access_token"])


# ===========================================================================
# E2E-F1 Boundary / Negative / Concurrency Tests — 6 Tests
# ===========================================================================


@pytest.mark.timeout(180)
def test_f1_cloud_failsafe_overrides_paper_trading_only_false(server_factory) -> None:
    """In OPENALGO_PROFILE=cloud, PAPER_TRADING_ONLY=false/'0' MUST still fail-safe to paper-only."""
    for raw_val in ("false", "0"):
        srv = server_factory(
            profile="cloud",
            paper_trading_only=False,
            extra_env={"PAPER_TRADING_ONLY": raw_val},
        )
        cfg = srv.api_get("/auth/app-config")
        assert cfg.status_code == 200
        assert cfg.json().get("paper_trading_only") is True, (
            f"Cloud profile must fail-safe paper_trading_only=True even when PAPER_TRADING_ONLY={raw_val!r}"
        )

        toggle = srv.api_post(
            "/api/v1/analyzer/toggle",
            {"apikey": srv.api_key, "mode": False},
            broker="fyers",
        )
        _assert_paper_locked_response(toggle, context=f"cloud fail-safe PAPER_TRADING_ONLY={raw_val}")

        status = srv.api_post("/api/v1/analyzer", {"apikey": srv.api_key}, broker="fyers")
        assert status.status_code == 200
        assert status.json()["data"]["analyze_mode"] is True


@pytest.mark.timeout(180)
def test_f1_truthy_case_insensitive_paper_lock_env_variants_in_full(server_factory) -> None:
    """Case-insensitive truthy values ('1', 'yes', 'On') enforce PAPER_TRADING_ONLY in full profile."""
    for raw_val in ("1", "yes", "On"):
        srv = server_factory(
            profile="full",
            extra_env={"PAPER_TRADING_ONLY": raw_val},
        )
        cfg = srv.api_get("/auth/app-config")
        assert cfg.status_code == 200
        assert cfg.json().get("paper_trading_only") is True, (
            f"PAPER_TRADING_ONLY={raw_val!r} must evaluate to True"
        )
        toggle = srv.api_post(
            "/api/v1/analyzer/toggle",
            {"apikey": srv.api_key, "mode": False},
            broker="deltaexchange",
        )
        _assert_paper_locked_response(toggle, context=f"full PAPER_TRADING_ONLY={raw_val!r}")


def test_f1_falsy_paper_lock_env_in_full_allows_toggle_without_real_orders(server_factory) -> None:
    """In non-cloud (full) profile, explicit PAPER_TRADING_ONLY='0' disables the hard paper lock."""
    srv = server_factory(
        profile="full",
        paper_trading_only=False,
        extra_env={"PAPER_TRADING_ONLY": "0"},
    )
    cfg = srv.api_get("/auth/app-config")
    assert cfg.status_code == 200
    assert cfg.json().get("paper_trading_only") is False

    toggle_live = srv.api_post(
        "/api/v1/analyzer/toggle",
        {"apikey": srv.api_key, "mode": False},
        broker="fyers",
    )
    assert toggle_live.status_code == 200, (
        f"Expected toggle allowed in full mode with PAPER_TRADING_ONLY=0, got "
        f"{toggle_live.status_code}: {toggle_live.text[:300]}"
    )
    assert toggle_live.json().get("data", {}).get("analyze_mode") is False

    # Immediately restore analyze_mode=True; no order endpoints are ever called.
    restore = srv.api_post(
        "/api/v1/analyzer/toggle",
        {"apikey": srv.api_key, "mode": True},
        broker="fyers",
    )
    assert restore.status_code == 200
    assert restore.json().get("data", {}).get("analyze_mode") is True
    srv.assert_zero_real_broker_order_calls()


def test_f1_forged_payloads_and_mcp_analyzer_toggle_blocked(server_factory) -> None:
    """Forged bypass fields (force_live, bypass flags), coerced values, and MCP analyzer_toggle are all blocked."""
    srv = server_factory(profile="cloud", paper_trading_only=True, mcp_http_enabled=True)

    # 1. Forged extra fields on RESTX analyzer toggle
    forged_restx = srv.api_post(
        "/api/v1/analyzer/toggle",
        {
            "apikey": srv.api_key,
            "mode": False,
            "force_live": True,
            "override": True,
            "bypass_paper_lock": True,
        },
        broker="fyers",
    )
    assert forged_restx.status_code in (400, 403), f"Forged RESTX toggle must fail: {forged_restx.text[:300]}"
    if forged_restx.status_code == 403:
        assert forged_restx.json().get("code") == "PAPER_TRADING_ONLY"

    # 2. Integer/string falsy coercion attempts on RESTX analyzer toggle
    for weird_mode in (0, "false", "0", "live", None):
        r = srv.api_post(
            "/api/v1/analyzer/toggle",
            {"apikey": srv.api_key, "mode": weird_mode},
            broker="fyers",
        )
        assert r.status_code in (400, 403), (
            f"Coerced mode={weird_mode!r} must be rejected, got {r.status_code}: {r.text[:250]}"
        )

    # 3. Strategy module forged force_live payload
    session = srv.session_client(broker="fyers")
    sid = _create_signal_strategy(session, name="E2E F1 Forged Probe")
    strat_live = session.post(
        f"/strategy/api/strategies/{sid}/live",
        json={"enabled": True, "force_live": True},
    )
    assert strat_live.status_code in (400, 403)
    if strat_live.status_code == 403:
        assert strat_live.json().get("code") == "PAPER_TRADING_ONLY"

    # 4. MCP HTTP tools/call analyzer_toggle(mode=False)
    token = _mint_mcp_bearer_token(srv, scope="read:market read:account write:orders", broker="fyers")
    mcp_resp = srv.api_post(
        "/mcp",
        payload={
            "jsonrpc": "2.0",
            "id": 99,
            "method": "tools/call",
            "params": {"name": "analyzer_toggle", "arguments": {"mode": False}},
        },
        broker="fyers",
        headers={"Authorization": f"Bearer {token}"},
    )
    if mcp_resp.status_code == 403:
        _assert_paper_locked_response(mcp_resp, context="MCP analyzer_toggle")
    else:
        assert mcp_resp.status_code == 200, f"Unexpected MCP status: {mcp_resp.status_code} {mcp_resp.text[:300]}"
        rpc = mcp_resp.json()
        assert (
            "PAPER_TRADING_ONLY" in mcp_resp.text
            or bool(rpc.get("error"))
            or bool(rpc.get("result", {}).get("isError"))
        ), f"MCP analyzer_toggle(mode=False) must be blocked: {rpc}"
        assert "PAPER_TRADING_ONLY" in mcp_resp.text, f"Expected PAPER_TRADING_ONLY in MCP reply: {rpc}"

    # Verify mode remained locked in analyze across all brokers
    for broker in BROKERS:
        check = srv.api_post("/api/v1/analyzer", {"apikey": srv.api_key}, broker=broker)
        assert check.status_code == 200
        assert check.json()["data"]["analyze_mode"] is True


def test_f1_concurrent_live_toggle_storm_all_rejected(cloud_server: OpenAlgoE2EInstance) -> None:
    """12 concurrent live-toggle requests across all 3 brokers all return 403 PAPER_TRADING_ONLY with zero 500s."""
    sessions = {b: cloud_server.session_client(broker=b) for b in BROKERS}

    def _attack(idx: int) -> tuple[str, int, str]:
        broker = BROKERS[idx % len(BROKERS)]
        kind = idx % 3
        if kind == 0:
            resp = cloud_server.api_post(
                "/api/v1/analyzer/toggle",
                {"apikey": cloud_server.api_key, "mode": False},
                broker=broker,
            )
            return f"restx:{broker}", resp.status_code, resp.text
        if kind == 1:
            resp = sessions[broker].post("/auth/analyzer-toggle")
            return f"spa:{broker}", resp.status_code, resp.text
        resp = sessions[broker].post("/settings/analyze-mode/0")
        return f"settings:{broker}", resp.status_code, resp.text

    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(_attack, range(12)))

    for label, status_code, body_text in results:
        assert status_code == 403, f"[{label}] expected 403, got {status_code}: {body_text[:250]}"
        assert "PAPER_TRADING_ONLY" in body_text, f"[{label}] missing PAPER_TRADING_ONLY: {body_text[:250]}"

    for broker in BROKERS:
        check = cloud_server.api_post("/api/v1/analyzer", {"apikey": cloud_server.api_key}, broker=broker)
        assert check.status_code == 200
        assert check.json()["data"]["analyze_mode"] is True


def test_f1_invalid_api_key_and_missing_csrf_on_toggle_endpoints(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Invalid/missing API key and missing session/CSRF token are cleanly rejected on live-toggle endpoints."""
    for bad_key in ("", "invalid-e2e-apikey-0000"):
        resp = cloud_server.api_post(
            "/api/v1/analyzer/toggle",
            {"apikey": bad_key, "mode": False},
            broker="fyers",
        )
        assert resp.status_code in (400, 401, 403), (
            f"Bad apikey={bad_key!r} must be rejected: {resp.status_code} {resp.text[:250]}"
        )

    # Unauthenticated session routes
    unauth_spa = cloud_server.api_post("/auth/analyzer-toggle", broker="fyers")
    assert unauth_spa.status_code in (400, 401, 403)

    unauth_settings = cloud_server.api_post("/settings/analyze-mode/0", broker="fyers")
    assert unauth_settings.status_code in (400, 401, 403)

    # Authenticated session WITHOUT CSRF header must be rejected
    session = cloud_server.session_client(broker="fyers")
    no_csrf_resp = session.post(
        "/settings/analyze-mode/0",
        include_csrf=False,
        headers={"X-OpenAlgo-Broker": "fyers", "Accept": "application/json"},
    )
    assert no_csrf_resp.status_code in (400, 403), (
        f"Missing CSRF header must fail with 400/403, got {no_csrf_resp.status_code}: {no_csrf_resp.text[:250]}"
    )


# ===========================================================================
# E2E-F2 Boundary / Negative / Concurrency Tests — 6 Tests
# ===========================================================================


@pytest.mark.timeout(180)
def test_f2_disable_historical_data_flag_blocks_history_in_full_profile(server_factory) -> None:
    """DISABLE_HISTORICAL_DATA truthy variants ('on', '1', 'yes') disable history even in OPENALGO_PROFILE=full."""
    start_d, end_d = _recent_date_window(7)
    for raw_flag in ("on", "1", "yes"):
        srv = server_factory(
            profile="full",
            paper_trading_only=True,
            disable_historical_data=raw_flag,
        )
        cfg = srv.api_get("/auth/app-config")
        assert cfg.status_code == 200
        cfg_json = cfg.json()
        assert cfg_json.get("profile") == "full"
        assert cfg_json.get("historical_data_enabled") is False, (
            f"DISABLE_HISTORICAL_DATA={raw_flag!r} must disable historical_data_enabled in full profile"
        )

        hist = srv.api_post(
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
        _assert_historical_disabled_response(
            hist, allowed_statuses=(403,), context=f"full + DISABLE_HISTORICAL_DATA={raw_flag}"
        )

        ticker = srv.api_get(
            "/api/v1/ticker/NSE:SBIN",
            broker="fyers",
            query={"apikey": srv.api_key, "interval": "D", "from": start_d, "to": end_d},
        )
        _assert_historical_disabled_response(
            ticker, allowed_statuses=(403,), context=f"ticker + DISABLE_HISTORICAL_DATA={raw_flag}"
        )
        assert srv.duckdb_files() == []


def test_f2_cloud_ignores_disable_historical_data_false(server_factory) -> None:
    """OPENALGO_PROFILE=cloud keeps historical data disabled even if DISABLE_HISTORICAL_DATA=false."""
    srv = server_factory(
        profile="cloud",
        paper_trading_only=True,
        disable_historical_data="false",
    )
    cfg = srv.api_get("/auth/app-config")
    assert cfg.status_code == 200
    assert cfg.json().get("profile") == "cloud"
    assert cfg.json().get("historical_data_enabled") is False

    start_d, end_d = _recent_date_window(5)
    hist = srv.api_post(
        "/api/v1/history",
        {
            "apikey": srv.api_key,
            "symbol": "BTCUSDFUT",
            "exchange": "CRYPTO",
            "interval": "D",
            "start_date": start_d,
            "end_date": end_d,
        },
        broker="deltaexchange",
    )
    _assert_historical_disabled_response(hist, allowed_statuses=(403,), context="cloud + DHD=false")
    assert srv.duckdb_files() == []


def test_f2_cloud_case_insensitive_profile_env_value(server_factory) -> None:
    """Uppercase OPENALGO_PROFILE='CLOUD' is normalized to 'cloud' and enforces full historical & paper lockout."""
    srv = server_factory(
        profile="cloud",
        extra_env={"OPENALGO_PROFILE": "CLOUD"},
    )
    cfg = srv.api_get("/auth/app-config")
    assert cfg.status_code == 200
    body = cfg.json()
    assert body.get("profile") == "cloud"
    assert body.get("historical_data_enabled") is False
    assert body.get("paper_trading_only") is True

    start_d, end_d = _recent_date_window(5)
    hist = srv.api_post(
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
    _assert_historical_disabled_response(hist, allowed_statuses=(403,), context="OPENALGO_PROFILE=CLOUD")
    assert srv.duckdb_files() == []


def test_f2_cloud_direct_bypass_routes_and_unauthenticated_test_chart_blocked(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Direct bypass vectors (ticker txt/json, source=db/api, unauthenticated /test_chart, pnltracker/legacy) are blocked."""
    start_d, end_d = _recent_date_window(5)

    for source_val in ("db", "api"):
        r = cloud_server.api_post(
            "/api/v1/history",
            {
                "apikey": cloud_server.api_key,
                "symbol": "SBIN",
                "exchange": "NSE",
                "interval": "D",
                "start_date": start_d,
                "end_date": end_d,
                "source": source_val,
            },
            broker="fyers",
        )
        _assert_historical_disabled_response(r, allowed_statuses=(403,), context=f"history source={source_val}")

    for sym_path, fmt in (("CRYPTO:BTCUSDFUT", "txt"), ("NSE:SBIN", "json")):
        broker = "deltaexchange" if sym_path.startswith("CRYPTO:") else "fyers"
        r = cloud_server.api_get(
            f"/api/v1/ticker/{sym_path}",
            broker=broker,
            query={
                "apikey": cloud_server.api_key,
                "interval": "D",
                "from": start_d,
                "to": end_d,
                "format": fmt,
            },
        )
        _assert_historical_disabled_response(r, allowed_statuses=(403,), context=f"ticker {sym_path} {fmt}")

    # /test_chart had no @check_session_validity in baseline — must still be blocked in cloud!
    unauth_test_chart = cloud_server.api_get("/test_chart", broker="fyers")
    _assert_historical_disabled_response(unauth_test_chart, context="unauthenticated GET /test_chart")

    session = cloud_server.session_client(broker="fyers")
    pnl_legacy = session.get("/pnltracker/legacy")
    _assert_historical_disabled_response(pnl_legacy, context="GET /pnltracker/legacy")

    assert cloud_server.duckdb_files() == []


def test_f2_cloud_all_mcp_indicator_tools_blocked_and_zero_duckdb(server_factory) -> None:
    """All history-backed MCP technical-analysis tools return HISTORICAL_DATA_DISABLED in cloud."""
    srv = server_factory(profile="cloud", paper_trading_only=True, mcp_http_enabled=True)
    token = _mint_mcp_bearer_token(srv, scope="read:market read:account", broker="fyers")

    indicator_tools = [
        ("get_trend_snapshot", {"symbol": "SBIN", "exchange": "NSE", "interval": "D"}),
        ("get_momentum_snapshot", {"symbol": "SBIN", "exchange": "NSE", "interval": "D"}),
        ("get_volatility_snapshot", {"symbol": "SBIN", "exchange": "NSE", "interval": "D"}),
        ("detect_signals", {"symbol": "SBIN", "exchange": "NSE", "interval": "D"}),
        ("multi_timeframe_analysis", {"symbol": "SBIN", "exchange": "NSE"}),
        ("correlation_beta", {"symbol": "SBIN", "exchange": "NSE", "benchmark": "NIFTY"}),
    ]
    for idx, (tool_name, args) in enumerate(indicator_tools, start=1):
        resp = srv.api_post(
            "/mcp",
            payload={
                "jsonrpc": "2.0",
                "id": idx,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": args},
            },
            broker="fyers",
            headers={"Authorization": f"Bearer {token}"},
        )
        if resp.status_code in (403, 503):
            _assert_historical_disabled_response(resp, context=f"MCP {tool_name}")
        else:
            assert resp.status_code == 200, f"Unexpected HTTP {resp.status_code} for MCP {tool_name}"
            rpc = resp.json()
            has_err = bool(rpc.get("error")) or bool(rpc.get("result", {}).get("isError"))
            assert has_err and "HISTORICAL_DATA_DISABLED" in resp.text, (
                f"MCP tool {tool_name} must refuse with HISTORICAL_DATA_DISABLED in cloud, got: {rpc}"
            )

    assert srv.duckdb_files() == []


def test_f2_cloud_concurrent_history_assault_zero_duckdb_and_no_500s(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """15 concurrent history/backtest probes across all 3 brokers yield zero 200s, zero 500s, and zero DuckDB files."""
    start_d, end_d = _recent_date_window(5)
    sessions = {b: cloud_server.session_client(broker=b) for b in BROKERS}

    def _probe(idx: int) -> tuple[str, int, str]:
        broker = BROKERS[idx % len(BROKERS)]
        spec = DEFAULT_SYMBOLS[broker]
        kind = idx % 5
        if kind == 0:
            r = cloud_server.api_post(
                "/api/v1/history",
                {
                    "apikey": cloud_server.api_key,
                    "symbol": spec["symbol"],
                    "exchange": spec["exchange"],
                    "interval": "D",
                    "start_date": start_d,
                    "end_date": end_d,
                },
                broker=broker,
            )
            return f"history:{broker}", r.status_code, r.text
        if kind == 1:
            r = cloud_server.api_get(
                f"/api/v1/ticker/{spec['exchange']}:{spec['symbol']}",
                broker=broker,
                query={"apikey": cloud_server.api_key, "interval": "D", "from": start_d, "to": end_d},
            )
            return f"ticker:{broker}", r.status_code, r.text
        if kind == 2:
            r = cloud_server.api_post(
                "/api/v1/sectorrotation",
                {"apikey": cloud_server.api_key, "timeframe": "D"},
                broker=broker,
            )
            return f"sector:{broker}", r.status_code, r.text
        if kind == 3:
            r = sessions[broker].get(f"/scalping/api/history?symbol={spec['symbol']}&exchange={spec['exchange']}&interval=5m")
            return f"scalping_hist:{broker}", r.status_code, r.text
        r = sessions[broker].post(
            "/historify/api/download",
            json={"symbol": spec["symbol"], "exchange": spec["exchange"], "interval": "D"},
        )
        return f"historify_dl:{broker}", r.status_code, r.text

    with ThreadPoolExecutor(max_workers=15) as pool:
        results = list(pool.map(_probe, range(15)))

    for label, status_code, body_text in results:
        assert status_code in (403, 503), f"[{label}] expected 403/503, got {status_code}: {body_text[:250]}"
        assert "HISTORICAL_DATA_DISABLED" in body_text, f"[{label}] missing code: {body_text[:250]}"

    assert cloud_server.duckdb_files() == []


# ===========================================================================
# E2E-F3 Boundary / Negative / Mutation Tests — 6 Tests
# ===========================================================================


def test_f3_cloud_quotes_invalid_api_key_and_missing_fields_return_4xx_not_500(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """In cloud profile, /api/v1/quotes validates API key and required fields cleanly (4xx, not HISTORICAL_DATA_DISABLED)."""
    bad_key = cloud_server.api_post(
        "/api/v1/quotes",
        {"apikey": "wrong-key-xyz", "symbol": "SBIN", "exchange": "NSE"},
        broker="fyers",
    )
    assert bad_key.status_code in (401, 403)
    assert "HISTORICAL_DATA_DISABLED" not in bad_key.text

    missing_symbol = cloud_server.api_post(
        "/api/v1/quotes",
        {"apikey": cloud_server.api_key, "exchange": "NSE"},
        broker="fyers",
    )
    assert missing_symbol.status_code == 400
    assert "HISTORICAL_DATA_DISABLED" not in missing_symbol.text

    missing_exchange = cloud_server.api_post(
        "/api/v1/quotes",
        {"apikey": cloud_server.api_key, "symbol": "SBIN"},
        broker="fyers",
    )
    assert missing_exchange.status_code == 400
    assert "HISTORICAL_DATA_DISABLED" not in missing_exchange.text


def test_f3_cloud_multiquotes_empty_and_multi_symbol_boundary(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """BVA on /api/v1/multiquotes in cloud: 0 symbols -> 400; 1 and 2 symbols -> 200 with zero DuckDB files."""
    empty_resp = cloud_server.api_post(
        "/api/v1/multiquotes",
        {"apikey": cloud_server.api_key, "symbols": []},
        broker="fyers",
    )
    assert empty_resp.status_code == 400, (
        f"Empty multiquotes symbols list must return 400, got {empty_resp.status_code}: {empty_resp.text[:250]}"
    )

    multi_resp = cloud_server.api_post(
        "/api/v1/multiquotes",
        {
            "apikey": cloud_server.api_key,
            "symbols": [
                {"symbol": "SBIN", "exchange": "NSE"},
                {"symbol": "NIFTY27MAR25FUT", "exchange": "NFO"},
            ],
        },
        broker="fyers",
    )
    assert multi_resp.status_code == 200, (
        f"Multi-symbol multiquotes failed in cloud: {multi_resp.status_code} {multi_resp.text[:300]}"
    )
    assert multi_resp.json().get("status") == "success"
    assert cloud_server.duckdb_files() == []


def test_f3_cloud_intervals_read_only_across_all_brokers_no_disk_history(server_factory) -> None:
    """POST /api/v1/intervals in cloud returns static metadata with zero external history HTTP calls and zero DuckDB."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    for broker in BROKERS:
        resp = srv.api_post("/api/v1/intervals", {"apikey": srv.api_key}, broker=broker)
        assert resp.status_code == 200, f"[{broker}] /api/v1/intervals failed: {resp.status_code} {resp.text[:250]}"
        assert resp.json().get("status") == "success"

    history_calls = [
        call
        for call in srv.outbound_calls()
        if any(seg in str(call.get("url", "")) for seg in ("/history", "/historical"))
    ]
    assert history_calls == [], f"/api/v1/intervals must never fetch broker history: {history_calls}"
    assert srv.duckdb_files() == []


def test_f3_cloud_placeorder_boundary_invalid_qty_and_action_rejected(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """BVA on /api/v1/placeorder in cloud: qty=0, qty=-5, bad action/pricetype return 400; qty=1 succeeds in paper."""
    base_order = {
        "apikey": cloud_server.api_key,
        "strategy": "e2e_t2_f3_bva",
        "exchange": "NSE",
        "symbol": "SBIN",
        "action": "BUY",
        "quantity": 1,
        "pricetype": "MARKET",
        "product": "CNC",
    }
    invalid_overrides = [
        {"quantity": 0},
        {"quantity": -5},
        {"action": "HOLD"},
        {"pricetype": "INVALID_TYPE"},
    ]
    for override in invalid_overrides:
        payload = {**base_order, **override}
        resp = cloud_server.api_post("/api/v1/placeorder", payload, broker="fyers")
        assert resp.status_code == 400, (
            f"Invalid order override {override} expected 400, got {resp.status_code}: {resp.text[:250]}"
        )

    valid_resp = cloud_server.api_post("/api/v1/placeorder", base_order, broker="fyers")
    assert valid_resp.status_code == 200
    assert valid_resp.json().get("mode") == "analyze"
    cloud_server.assert_zero_real_broker_order_calls()


def test_f3_cloud_dynamic_quote_mutation_reflected_immediately(server_factory) -> None:
    """Dynamic live-quote updates via mock feed propagate immediately to /api/v1/quotes under cloud profile."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    srv.set_mock_quote("deltaexchange", "CRYPTO", "BTCUSDFUT", ltp=67123.5)

    resp = srv.api_post(
        "/api/v1/quotes",
        {"apikey": srv.api_key, "symbol": "BTCUSDFUT", "exchange": "CRYPTO"},
        broker="deltaexchange",
    )
    assert resp.status_code == 200
    assert float(resp.json()["data"]["ltp"]) == pytest.approx(67123.5, rel=1e-4)
    assert srv.duckdb_files() == []


def test_f3_cloud_scalping_live_mutations_require_csrf_while_history_blocked(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """In cloud profile, live scalping mutations enforce CSRF (not HISTORICAL_DATA_DISABLED), while history is blocked."""
    session = cloud_server.session_client(broker="fyers")

    no_csrf = session.post(
        "/scalping/api/cancel_all",
        json={},
        include_csrf=False,
        headers={"X-OpenAlgo-Broker": "fyers", "Accept": "application/json"},
    )
    assert no_csrf.status_code in (400, 403), (
        f"Missing CSRF on /scalping/api/cancel_all expected 400/403, got {no_csrf.status_code}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in no_csrf.text

    with_csrf = session.post("/scalping/api/cancel_all", json={})
    assert with_csrf.status_code != 503 and "HISTORICAL_DATA_DISABLED" not in with_csrf.text, (
        f"Live /scalping/api/cancel_all must not be blocked as historical: {with_csrf.status_code} {with_csrf.text[:250]}"
    )

    hist = session.get("/scalping/api/history?symbol=SBIN&exchange=NSE&interval=5m")
    _assert_historical_disabled_response(hist, context="GET /scalping/api/history")


# ===========================================================================
# E2E-F4 Boundary / Negative / Validation Tests — 6 Tests
# ===========================================================================


def test_f4_full_history_invalid_date_range_and_bad_interval_rejected(
    full_server: OpenAlgoE2EInstance,
) -> None:
    """BVA on /api/v1/history in full profile: malformed date, reversed range, and invalid interval return 400."""
    start_d, end_d = _recent_date_window(7)
    bad_payloads = [
        {
            "apikey": full_server.api_key,
            "symbol": "SBIN",
            "exchange": "NSE",
            "interval": "D",
            "start_date": "2026/13/45",
            "end_date": end_d,
        },
        {
            "apikey": full_server.api_key,
            "symbol": "SBIN",
            "exchange": "NSE",
            "interval": "D",
            "start_date": end_d,
            "end_date": start_d,
        },
        {
            "apikey": full_server.api_key,
            "symbol": "SBIN",
            "exchange": "NSE",
            "interval": "999z",
            "start_date": start_d,
            "end_date": end_d,
        },
    ]
    for payload in bad_payloads:
        resp = full_server.api_post("/api/v1/history", payload, broker="fyers")
        assert resp.status_code == 400, (
            f"Expected 400 for invalid history payload {payload}, got {resp.status_code}: {resp.text[:300]}"
        )
        assert "HISTORICAL_DATA_DISABLED" not in resp.text


def test_f4_full_ticker_invalid_symbol_format_and_missing_apikey(
    full_server: OpenAlgoE2EInstance,
) -> None:
    """Category-partition on GET /api/v1/ticker/<sym> in full mode: auth check, symbol format check, txt output."""
    start_d, end_d = _recent_date_window(7)

    no_key = full_server.api_get(
        "/api/v1/ticker/NSE:SBIN",
        broker="fyers",
        query={"interval": "D", "from": start_d, "to": end_d},
    )
    assert no_key.status_code in (400, 401, 403)
    assert "HISTORICAL_DATA_DISABLED" not in no_key.text

    bad_sym = full_server.api_get(
        "/api/v1/ticker/SBIN",
        broker="fyers",
        query={"apikey": full_server.api_key, "interval": "D", "from": start_d, "to": end_d},
    )
    assert bad_sym.status_code == 400, (
        f"Missing EXCHANGE: prefix in /api/v1/ticker/SBIN expected 400, got {bad_sym.status_code}: {bad_sym.text[:250]}"
    )

    txt_resp = full_server.api_get(
        "/api/v1/ticker/NSE:SBIN",
        broker="fyers",
        query={
            "apikey": full_server.api_key,
            "interval": "D",
            "from": start_d,
            "to": end_d,
            "format": "txt",
        },
    )
    assert txt_resp.status_code == 200, (
        f"Expected 200 for ticker format=txt in full mode, got {txt_resp.status_code}: {txt_resp.text[:250]}"
    )
    assert len(txt_resp.text.strip()) > 0


def test_f4_full_portfolio_backtest_validation_boundary(full_server: OpenAlgoE2EInstance) -> None:
    """BVA on POST /api/v1/portfolio/backtest in full mode: empty holdings returns 400 (mounted, not 403/404)."""
    start_d, end_d = _recent_date_window(30)
    resp = full_server.api_post(
        "/api/v1/portfolio/backtest",
        {
            "apikey": full_server.api_key,
            "holdings": [],
            "start_date": start_d,
            "end_date": end_d,
        },
        broker="fyers",
    )
    assert resp.status_code == 400, (
        f"Empty holdings expected 400 in full mode, got {resp.status_code}: {resp.text[:300]}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in resp.text


def test_f4_full_sip_backtest_validation_boundary(full_server: OpenAlgoE2EInstance) -> None:
    """BVA on POST /api/v1/sip/backtest in full mode: amount<=0 returns 400 (mounted, not 403/404)."""
    start_d, end_d = _recent_date_window(30)
    resp = full_server.api_post(
        "/api/v1/sip/backtest",
        {
            "apikey": full_server.api_key,
            "symbol": "SBIN",
            "exchange": "NSE",
            "start_date": start_d,
            "end_date": end_d,
            "amount": 0,
            "frequency": "monthly",
        },
        broker="fyers",
    )
    assert resp.status_code == 400, (
        f"Zero SIP amount expected 400 in full mode, got {resp.status_code}: {resp.text[:300]}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in resp.text


def test_f4_full_historify_requires_auth_and_csrf_for_mutations(
    full_server: OpenAlgoE2EInstance,
) -> None:
    """In full profile, Historify enforces session auth on reads and CSRF on writes (not 503 disabled)."""
    unauth = full_server.api_get(
        "/historify/api/catalog",
        broker="fyers",
        headers={"Accept": "application/json"},
    )
    assert unauth.status_code == 401, (
        f"Unauthenticated /historify/api/catalog expected 401 in full mode, got {unauth.status_code}: {unauth.text[:250]}"
    )

    session = full_server.session_client(broker="fyers")
    no_csrf = session.post(
        "/historify/api/watchlist",
        json={"symbol": "SBIN", "exchange": "NSE"},
        include_csrf=False,
        headers={"X-OpenAlgo-Broker": "fyers", "Accept": "application/json"},
    )
    assert no_csrf.status_code in (400, 403), (
        f"Missing CSRF on /historify/api/watchlist expected 400/403, got {no_csrf.status_code}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in no_csrf.text


def test_f4_full_paper_trading_still_locked_by_default(full_server: OpenAlgoE2EInstance) -> None:
    """Even in OPENALGO_PROFILE=full, default PAPER_TRADING_ONLY=true blocks live toggle while history works."""
    toggle = full_server.api_post(
        "/api/v1/analyzer/toggle",
        {"apikey": full_server.api_key, "mode": False},
        broker="fyers",
    )
    _assert_paper_locked_response(toggle, context="full mode default paper lock")

    start_d, end_d = _recent_date_window(5)
    hist = full_server.api_post(
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
    assert hist.status_code == 200 and hist.json().get("status") == "success"


# ===========================================================================
# E2E-F5 Boundary / Negative / Security Tests — 6 Tests
# ===========================================================================


def test_f5_app_config_zero_secret_leak_check(cloud_server: OpenAlgoE2EInstance) -> None:
    """Unauthenticated GET /auth/app-config MUST NOT leak any secret keys, tokens, passwords, or extra private keys."""
    resp = cloud_server.api_get("/auth/app-config")
    assert resp.status_code == 200
    raw = resp.text
    body = resp.json()

    expected_keys = {"status", "profile", "historical_data_enabled", "paper_trading_only", "brokers"}
    assert set(body.keys()) == expected_keys, (
        f"/auth/app-config must expose only public contract keys {expected_keys}, got {set(body.keys())}"
    )

    forbidden_substrings = (
        "APP_KEY",
        "API_KEY_PEPPER",
        "FERNET_SALT",
        "api_secret",
        "auth_token",
        "feed_token",
        "password",
        cloud_server.api_key,
        cloud_server.password,
        "e2e-fyers-secret",
        "e2e-deltaexchange-auth-token",
        "e2e-groww-secret",
    )
    for secret_str in forbidden_substrings:
        assert secret_str not in raw, f"Secret/private value leaked in /auth/app-config: {secret_str!r}"


def test_f5_app_config_rejects_mutating_http_methods(cloud_server: OpenAlgoE2EInstance) -> None:
    """Mutating HTTP methods (POST, PUT, DELETE, PATCH) on /auth/app-config return 405 Method Not Allowed."""
    url = f"{cloud_server.base_url}/auth/app-config"
    for method in ("POST", "PUT", "DELETE", "PATCH"):
        resp = httpx.request(
            method,
            url,
            json={"profile": "full", "paper_trading_only": False, "historical_data_enabled": True},
            timeout=10.0,
        )
        assert resp.status_code == 405, (
            f"{method} /auth/app-config must return 405, got {resp.status_code}: {resp.text[:200]}"
        )


def test_f5_cloud_history_spa_routes_blocked_regardless_of_accept_header_or_broker(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Blocked SPA routes return 503 HISTORICAL_DATA_DISABLED for both HTML & JSON Accept headers across all brokers."""
    probe_routes = (
        "/historify",
        "/portfolio-backtester",
        "/sip-backtester",
        "/sector-rotation",
        "/ivchart",
        "/straddle",
        "/pnl-tracker",
    )
    for broker in BROKERS:
        session = cloud_server.session_client(broker=broker)
        for accept in ("text/html", "application/json"):
            for route in probe_routes:
                resp = session.get(route, headers={"Accept": accept})
                _assert_historical_disabled_response(
                    resp,
                    allowed_statuses=(503,),
                    context=f"GET {route} broker={broker} Accept={accept}",
                )


def test_f5_cloud_unknown_spa_route_returns_404_not_503(cloud_server: OpenAlgoE2EInstance) -> None:
    """Unmatched non-API path returns 404 (standard SPA fallback), distinct from 503 HISTORICAL_DATA_DISABLED."""
    resp = cloud_server.api_get("/definitely-nonexistent-e2e-route-xyz", broker="fyers")
    assert resp.status_code == 404, (
        f"Unknown SPA route should return 404, got {resp.status_code}: {resp.text[:200]}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in resp.text


def test_f5_unknown_profile_env_value_handled_safely(server_factory) -> None:
    """An unrecognized OPENALGO_PROFILE value boots safely without 500s and keeps PAPER_TRADING_ONLY=True."""
    srv = server_factory(
        profile="full",
        paper_trading_only=True,
        extra_env={"OPENALGO_PROFILE": "staging_unknown_profile"},
    )
    cfg = srv.api_get("/auth/app-config")
    assert cfg.status_code == 200, f"Unknown profile caused /auth/app-config error: {cfg.status_code} {cfg.text[:250]}"
    body = cfg.json()
    assert body.get("status") == "success"
    assert isinstance(body.get("historical_data_enabled"), bool)
    assert body.get("paper_trading_only") is True

    toggle = srv.api_post(
        "/api/v1/analyzer/toggle",
        {"apikey": srv.api_key, "mode": False},
        broker="fyers",
    )
    _assert_paper_locked_response(toggle, context="unknown profile paper safety")


def test_f5_app_config_consistent_across_brokers_and_sessions(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """GET /auth/app-config returns identical JSON unauthenticated, with ?broker=, and across all broker sessions."""
    baseline = cloud_server.api_get("/auth/app-config")
    assert baseline.status_code == 200
    expected = baseline.json()

    for broker in BROKERS:
        q_resp = cloud_server.api_get("/auth/app-config", query={"broker": broker})
        assert q_resp.status_code == 200
        assert q_resp.json() == expected

        session = cloud_server.session_client(broker=broker)
        s_resp = session.get("/auth/app-config")
        assert s_resp.status_code == 200
        assert s_resp.json() == expected
