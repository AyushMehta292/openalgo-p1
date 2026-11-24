"""Tier 1 E2E Feature Coverage Tests — Safety & Profile Contracts (E2E-F1..E2E-F5).

Strict opaque-box tests exercising public HTTP REST, session-authenticated SPA/API,
MCP JSON-RPC, and filesystem boundaries via the shared harness in ``test/e2e/conftest.py``.

Coverage (30 tests, 6 per feature):
- E2E-F1: Paper-Trading Hard Lock (PAPER_TRADING_ONLY=true)
- E2E-F2: Cloud Zero-Historical-Storage & Backtesting Lockout (OPENALGO_PROFILE=cloud)
- E2E-F3: Cloud Live-Trading Data Unhampered
- E2E-F4: Local Mode Full Parity (OPENALGO_PROFILE=full)
- E2E-F5: Frontend Runtime Config & Profile/Paper UI Contract
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import secrets
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

pytestmark = pytest.mark.tier1


# ---------------------------------------------------------------------------
# Opaque-box assertion & protocol helpers
# ---------------------------------------------------------------------------


def _assert_paper_locked_response(resp: httpx.Response, context: str = "") -> dict[str, Any]:
    """Assert HTTP 403 PAPER_TRADING_ONLY JSON error contract (PROJECT.md § Interface Contracts)."""
    assert resp.status_code == 403, (
        f"Expected 403 PAPER_TRADING_ONLY{f' ({context})' if context else ''}, "
        f"got {resp.status_code}: {resp.text[:400]}"
    )
    body = resp.json()
    assert body.get("status") == "error", f"Unexpected body ({context}): {body}"
    assert body.get("code") == "PAPER_TRADING_ONLY", f"Expected code=PAPER_TRADING_ONLY ({context}), got: {body}"
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


def _recent_date_window(days_back: int = 10) -> tuple[str, str]:
    end_d = date.today() - timedelta(days=1)
    start_d = end_d - timedelta(days=days_back)
    return start_d.isoformat(), end_d.isoformat()


def _create_signal_strategy(client: SessionClient, name: str = "E2E Tier1 Probe") -> int:
    """Create a valid minimal Strategy Module strategy via public HTTP API."""
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
    assert resp.status_code == 201, f"Failed to create test strategy: {resp.status_code} {resp.text[:400]}"
    payload = resp.json()
    sid = payload.get("data", {}).get("id")
    assert isinstance(sid, int), f"Expected integer strategy id, got: {payload}"
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
            "client_name": "E2E MCP Safety Probe",
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
    assert isinstance(token, str) and token, f"Missing access_token in response: {token_resp.json()}"
    return token


# ===========================================================================
# E2E-F1: Paper-Trading Hard Lock (PAPER_TRADING_ONLY=true) — 6 Tests
# ===========================================================================


def test_f1_analyzer_defaults_to_paper_mode_for_all_brokers(cloud_server: OpenAlgoE2EInstance) -> None:
    """Every broker starts locked in paper/analyze mode across RESTX, SPA, and Settings read endpoints."""
    for broker in BROKERS:
        restx_resp = cloud_server.api_post(
            "/api/v1/analyzer",
            {"apikey": cloud_server.api_key},
            broker=broker,
        )
        assert restx_resp.status_code == 200, f"[{broker}] /api/v1/analyzer failed: {restx_resp.text[:300]}"
        data = restx_resp.json().get("data", {})
        assert data.get("analyze_mode") is True, f"[{broker}] expected analyze_mode=True, got {data}"
        assert data.get("mode") == "analyze", f"[{broker}] expected mode='analyze', got {data}"

        session = cloud_server.session_client(broker=broker)
        spa_resp = session.get("/auth/analyzer-mode")
        assert spa_resp.status_code == 200, f"[{broker}] /auth/analyzer-mode failed: {spa_resp.text[:300]}"
        spa_data = spa_resp.json().get("data", {})
        assert spa_data.get("analyze_mode") is True, f"[{broker}] SPA mode not analyze: {spa_data}"
        assert spa_data.get("mode") == "analyze"

        settings_resp = session.get("/settings/analyze-mode")
        assert settings_resp.status_code == 200, f"[{broker}] /settings/analyze-mode failed: {settings_resp.text[:300]}"
        assert settings_resp.json().get("analyze_mode") is True


def test_f1_restx_analyzer_toggle_to_live_blocked_403(cloud_server: OpenAlgoE2EInstance) -> None:
    """POST /api/v1/analyzer/toggle with mode=False returns 403 PAPER_TRADING_ONLY across all brokers."""
    for broker in BROKERS:
        resp = cloud_server.api_post(
            "/api/v1/analyzer/toggle",
            {"apikey": cloud_server.api_key, "mode": False},
            broker=broker,
        )
        _assert_paper_locked_response(resp, context=f"RESTX toggle broker={broker}")

        verify = cloud_server.api_post(
            "/api/v1/analyzer",
            {"apikey": cloud_server.api_key},
            broker=broker,
        )
        assert verify.status_code == 200
        assert verify.json()["data"]["analyze_mode"] is True
        assert verify.json()["data"]["mode"] == "analyze"


def test_f1_restx_analyzer_toggle_keep_paper_succeeds(cloud_server: OpenAlgoE2EInstance) -> None:
    """POST /api/v1/analyzer/toggle with mode=True succeeds and keeps paper/analyze mode active."""
    resp = cloud_server.api_post(
        "/api/v1/analyzer/toggle",
        {"apikey": cloud_server.api_key, "mode": True},
        broker="deltaexchange",
    )
    assert resp.status_code == 200, f"Keeping paper mode should succeed: {resp.status_code} {resp.text[:300]}"
    body = resp.json()
    assert body.get("status") == "success"
    data = body.get("data", {})
    assert data.get("analyze_mode") is True
    assert data.get("mode") == "analyze"


def test_f1_spa_and_settings_live_toggles_blocked_403(cloud_server: OpenAlgoE2EInstance) -> None:
    """SPA /auth/analyzer-toggle and Settings /settings/analyze-mode/0 are blocked with 403 PAPER_TRADING_ONLY."""
    session = cloud_server.session_client(broker="fyers")

    spa_toggle = session.post("/auth/analyzer-toggle")
    _assert_paper_locked_response(spa_toggle, context="POST /auth/analyzer-toggle")

    settings_live = session.post("/settings/analyze-mode/0")
    _assert_paper_locked_response(settings_live, context="POST /settings/analyze-mode/0")

    settings_paper = session.post("/settings/analyze-mode/1")
    assert settings_paper.status_code == 200, (
        f"Setting analyze-mode/1 should stay allowed: {settings_paper.status_code} {settings_paper.text[:300]}"
    )

    check = session.get("/settings/analyze-mode")
    assert check.status_code == 200
    assert check.json().get("analyze_mode") is True


def test_f1_strategy_module_live_enable_and_start_live_blocked_403(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Strategy Module /live enable=true, /start mode=live, and RESTX /api/v1/strategy/start live are blocked 403."""
    session = cloud_server.session_client(broker="fyers")
    sid = _create_signal_strategy(session, name="E2E F1 Live Lock Test")

    enable_live = session.post(f"/strategy/api/strategies/{sid}/live", json={"enabled": True})
    _assert_paper_locked_response(enable_live, context="POST /strategy/api/strategies/<sid>/live")

    disable_live = session.post(f"/strategy/api/strategies/{sid}/live", json={"enabled": False})
    assert disable_live.status_code == 200, (
        f"Disabling live mode should succeed: {disable_live.status_code} {disable_live.text[:300]}"
    )

    start_live_spa = session.post(f"/strategy/api/strategies/{sid}/start", json={"mode": "live"})
    _assert_paper_locked_response(start_live_spa, context="POST /strategy/api/strategies/<sid>/start live")

    start_live_restx = cloud_server.api_post(
        "/api/v1/strategy/start",
        {"apikey": cloud_server.api_key, "strategy_id": sid, "mode": "live"},
        broker="fyers",
    )
    _assert_paper_locked_response(start_live_restx, context="POST /api/v1/strategy/start live")


def test_f1_paper_orders_execute_in_sandbox_with_zero_real_broker_calls(
    cloud_server: OpenAlgoE2EInstance,
) -> None:
    """Paper orders across Delta Exchange, Fyers, and Groww return mode='analyze' and never hit real brokers."""
    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        resp = cloud_server.api_post(
            "/api/v1/placeorder",
            {
                "apikey": cloud_server.api_key,
                "strategy": f"e2e_t1_f1_{broker}",
                "exchange": spec["exchange"],
                "symbol": spec["symbol"],
                "action": "BUY",
                "quantity": 1,
                "pricetype": "MARKET",
                "product": "CNC" if spec["exchange"] == "NSE" else spec["product"],
            },
            broker=broker,
        )
        assert resp.status_code == 200, f"[{broker}] placeorder failed: {resp.status_code} {resp.text[:300]}"
        body = resp.json()
        assert body.get("status") == "success", f"[{broker}] unexpected order body: {body}"
        assert body.get("mode") == "analyze", f"[{broker}] expected mode='analyze', got {body}"
        assert body.get("orderid"), f"[{broker}] missing orderid: {body}"

    cloud_server.assert_zero_real_broker_order_calls()


# ===========================================================================
# E2E-F2: Cloud Zero-Historical-Storage & Backtesting Lockout — 6 Tests
# ===========================================================================


def test_f2_cloud_zero_duckdb_files_created_on_disk(cloud_server: OpenAlgoE2EInstance) -> None:
    """Under OPENALGO_PROFILE=cloud, zero *.duckdb files exist anywhere under isolated root_dir."""
    assert cloud_server.duckdb_files() == [], (
        f"Cloud profile must create ZERO DuckDB files, found: {cloud_server.duckdb_files()}"
    )


def test_f2_cloud_restx_history_and_ticker_blocked_403(cloud_server: OpenAlgoE2EInstance) -> None:
    """POST /api/v1/history and GET /api/v1/ticker/<sym> return 403 HISTORICAL_DATA_DISABLED in cloud."""
    start_d, end_d = _recent_date_window(7)
    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        hist_resp = cloud_server.api_post(
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
        _assert_historical_disabled_response(
            hist_resp, allowed_statuses=(403,), context=f"POST /api/v1/history broker={broker}"
        )

    ticker_resp = cloud_server.api_get(
        "/api/v1/ticker/NSE:SBIN",
        broker="fyers",
        query={
            "apikey": cloud_server.api_key,
            "interval": "D",
            "from": start_d,
            "to": end_d,
            "format": "json",
        },
    )
    _assert_historical_disabled_response(
        ticker_resp, allowed_statuses=(403,), context="GET /api/v1/ticker/NSE:SBIN"
    )
    assert cloud_server.duckdb_files() == []


def test_f2_cloud_restx_portfolio_sip_sectorrotation_blocked(cloud_server: OpenAlgoE2EInstance) -> None:
    """All backtester and sector-rotation RESTX endpoints are blocked in cloud with HISTORICAL_DATA_DISABLED."""
    start_d, end_d = _recent_date_window(30)
    post_cases = [
        (
            "/api/v1/portfolio/backtest",
            {
                "apikey": cloud_server.api_key,
                "holdings": [{"symbol": "SBIN", "exchange": "NSE", "weight": 100}],
                "start_date": start_d,
                "end_date": end_d,
            },
        ),
        (
            "/api/v1/portfolio/tearsheet",
            {
                "apikey": cloud_server.api_key,
                "holdings": [{"symbol": "SBIN", "exchange": "NSE", "weight": 100}],
                "start_date": start_d,
                "end_date": end_d,
            },
        ),
        ("/api/v1/portfolio/holdings", {"apikey": cloud_server.api_key}),
        (
            "/api/v1/sip/backtest",
            {
                "apikey": cloud_server.api_key,
                "symbol": "SBIN",
                "exchange": "NSE",
                "start_date": start_d,
                "end_date": end_d,
                "amount": 5000,
                "frequency": "monthly",
            },
        ),
        (
            "/api/v1/sectorrotation",
            {"apikey": cloud_server.api_key, "timeframe": "D"},
        ),
    ]
    for path, payload in post_cases:
        resp = cloud_server.api_post(path, payload, broker="fyers")
        _assert_historical_disabled_response(resp, context=f"POST {path}")

    get_cases = [
        ("/api/v1/portfolio/benchmarks", {"apikey": cloud_server.api_key}),
        ("/api/v1/sip/frequencies", None),
        ("/api/v1/sectorrotation/options", None),
    ]
    for path, query in get_cases:
        resp = cloud_server.api_get(path, broker="fyers", query=query)
        _assert_historical_disabled_response(resp, context=f"GET {path}")

    assert cloud_server.duckdb_files() == []


def test_f2_cloud_historify_api_routes_blocked(cloud_server: OpenAlgoE2EInstance) -> None:
    """Historify watchlist, catalog, stats, data, and download routes return 403/503 HISTORICAL_DATA_DISABLED."""
    session = cloud_server.session_client(broker="fyers")
    for path in (
        "/historify/api/watchlist",
        "/historify/api/catalog",
        "/historify/api/stats",
        "/historify/api/data?symbol=SBIN&exchange=NSE&interval=D",
    ):
        resp = session.get(path)
        _assert_historical_disabled_response(resp, context=f"GET {path}")

    dl_resp = session.post(
        "/historify/api/download",
        json={"symbol": "SBIN", "exchange": "NSE", "interval": "D"},
    )
    _assert_historical_disabled_response(dl_resp, context="POST /historify/api/download")
    assert cloud_server.duckdb_files() == []


def test_f2_cloud_chart_analytics_history_blueprints_blocked(cloud_server: OpenAlgoE2EInstance) -> None:
    """Chart, scalping-history, straddle, IV chart, OI profile, strategybuilder, and pnltracker are blocked."""
    session = cloud_server.session_client(broker="fyers")
    start_d, end_d = _recent_date_window(5)

    get_routes = [
        f"/chart/test/api/history?symbol=SBIN&exchange=NSE&interval=D&start_date={start_d}&end_date={end_d}",
        f"/scalping/api/history?symbol=SBIN&exchange=NSE&interval=5m&start_date={start_d}&end_date={end_d}",
        "/straddle/api/intervals",
        "/straddlepnl/api/intervals",
        "/ivchart/api/intervals",
        "/oiprofile/api/intervals",
        "/strategybuilder/api/intervals",
    ]
    for route in get_routes:
        resp = session.get(route)
        _assert_historical_disabled_response(resp, context=f"GET {route}")

    post_routes = [
        ("/straddle/api/straddle-data", {"symbol": "NIFTY", "exchange": "NFO", "interval": "5m"}),
        ("/straddlepnl/api/simulate", {"symbol": "NIFTY", "exchange": "NFO"}),
        ("/ivchart/api/iv-data", {"symbol": "NIFTY", "exchange": "NFO"}),
        ("/oiprofile/api/profile-data", {"symbol": "NIFTY", "exchange": "NFO"}),
        ("/strategybuilder/api/strategy-chart", {"symbol": "NIFTY", "exchange": "NFO"}),
        ("/pnltracker/api/pnl", {"date": end_d}),
    ]
    for route, payload in post_routes:
        resp = session.post(route, json=payload)
        _assert_historical_disabled_response(resp, context=f"POST {route}")

    assert cloud_server.duckdb_files() == []


def test_f2_cloud_mcp_history_and_indicator_tools_blocked(server_factory) -> None:
    """MCP HTTP tools get_historical_data and calculate_indicator refuse execution in cloud profile."""
    srv = server_factory(profile="cloud", paper_trading_only=True, mcp_http_enabled=True)
    token = _mint_mcp_bearer_token(srv, scope="read:market read:account write:orders", broker="fyers")
    start_d, end_d = _recent_date_window(7)

    tool_calls = [
        (
            "get_historical_data",
            {
                "symbol": "SBIN",
                "exchange": "NSE",
                "interval": "D",
                "start_date": start_d,
                "end_date": end_d,
            },
        ),
        (
            "calculate_indicator",
            {
                "symbol": "SBIN",
                "exchange": "NSE",
                "interval": "D",
                "indicator": "ema",
                "period": 5,
            },
        ),
    ]
    for idx, (tool_name, args) in enumerate(tool_calls, start=1):
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
        raw_text = resp.text
        if resp.status_code in (403, 503):
            _assert_historical_disabled_response(resp, context=f"MCP {tool_name}")
        else:
            assert resp.status_code == 200, f"Unexpected HTTP {resp.status_code} for MCP {tool_name}: {raw_text[:400]}"
            rpc = resp.json()
            has_error = bool(rpc.get("error")) or bool(rpc.get("result", {}).get("isError"))
            assert has_error or "HISTORICAL_DATA_DISABLED" in raw_text, (
                f"MCP tool {tool_name} must fail with HISTORICAL_DATA_DISABLED in cloud, got: {rpc}"
            )
            assert "HISTORICAL_DATA_DISABLED" in raw_text, (
                f"Expected HISTORICAL_DATA_DISABLED error code in MCP response for {tool_name}, got: {rpc}"
            )

    assert srv.duckdb_files() == []


# ===========================================================================
# E2E-F3: Cloud Live-Trading Data Unhampered — 6 Tests
# ===========================================================================


def test_f3_cloud_live_quotes_unhampered_all_brokers(cloud_server: OpenAlgoE2EInstance) -> None:
    """POST /api/v1/quotes remains 100% operational in cloud across Delta Exchange, Fyers, and Groww."""
    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        resp = cloud_server.api_post(
            "/api/v1/quotes",
            {
                "apikey": cloud_server.api_key,
                "symbol": spec["symbol"],
                "exchange": spec["exchange"],
            },
            broker=broker,
        )
        assert resp.status_code == 200, f"[{broker}] /api/v1/quotes failed: {resp.status_code} {resp.text[:300]}"
        body = resp.json()
        assert body.get("status") == "success", f"[{broker}] quotes error: {body}"
        data = body.get("data", {})
        assert float(data.get("ltp", 0)) == pytest.approx(spec["ltp"], rel=1e-3), (
            f"[{broker}] expected ltp≈{spec['ltp']}, got {data}"
        )

    assert cloud_server.duckdb_files() == []


def test_f3_cloud_multiquotes_and_depth_unhampered(cloud_server: OpenAlgoE2EInstance) -> None:
    """POST /api/v1/multiquotes and POST /api/v1/depth succeed in cloud profile with live data."""
    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        mq_resp = cloud_server.api_post(
            "/api/v1/multiquotes",
            {
                "apikey": cloud_server.api_key,
                "symbols": [{"symbol": spec["symbol"], "exchange": spec["exchange"]}],
            },
            broker=broker,
        )
        assert mq_resp.status_code == 200, (
            f"[{broker}] /api/v1/multiquotes failed: {mq_resp.status_code} {mq_resp.text[:300]}"
        )
        assert mq_resp.json().get("status") == "success"

        depth_resp = cloud_server.api_post(
            "/api/v1/depth",
            {
                "apikey": cloud_server.api_key,
                "symbol": spec["symbol"],
                "exchange": spec["exchange"],
            },
            broker=broker,
        )
        assert depth_resp.status_code == 200, (
            f"[{broker}] /api/v1/depth failed: {depth_resp.status_code} {depth_resp.text[:300]}"
        )
        depth_body = depth_resp.json()
        assert depth_body.get("status") == "success"
        depth_data = depth_body.get("data", {})
        assert "bids" in depth_data and "asks" in depth_data, f"[{broker}] missing bids/asks: {depth_data}"

    assert cloud_server.duckdb_files() == []


def test_f3_cloud_optionchain_unhampered(cloud_server: OpenAlgoE2EInstance) -> None:
    """POST /api/v1/optionchain stays open in cloud (never rejected as HISTORICAL_DATA_DISABLED)."""
    resp = cloud_server.api_post(
        "/api/v1/optionchain",
        {
            "apikey": cloud_server.api_key,
            "underlying": "NIFTY",
            "exchange": "NFO",
            "expiry_date": "27MAR25",
            "strike_count": 3,
        },
        broker="fyers",
    )
    assert resp.status_code != 403 and resp.status_code != 503, (
        f"/api/v1/optionchain must NOT be blocked in cloud: {resp.status_code} {resp.text[:300]}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in resp.text
    assert resp.status_code == 200, f"/api/v1/optionchain expected 200, got {resp.status_code}: {resp.text[:300]}"
    assert cloud_server.duckdb_files() == []


def test_f3_cloud_intervals_and_ping_unhampered(cloud_server: OpenAlgoE2EInstance) -> None:
    """Read-only static POST /api/v1/intervals and POST /api/v1/ping remain 200 across all 3 brokers in cloud."""
    for broker in BROKERS:
        ping_resp = cloud_server.api_post(
            "/api/v1/ping",
            {"apikey": cloud_server.api_key},
            broker=broker,
        )
        assert ping_resp.status_code == 200, f"[{broker}] ping failed: {ping_resp.status_code} {ping_resp.text[:200]}"
        assert ping_resp.json().get("status") == "success"

        iv_resp = cloud_server.api_post(
            "/api/v1/intervals",
            {"apikey": cloud_server.api_key},
            broker=broker,
        )
        assert iv_resp.status_code == 200, f"[{broker}] intervals failed: {iv_resp.status_code} {iv_resp.text[:300]}"
        iv_body = iv_resp.json()
        assert iv_body.get("status") == "success"
        assert isinstance(iv_body.get("data"), dict) and len(iv_body["data"]) > 0, (
            f"[{broker}] expected non-empty interval categories, got {iv_body}"
        )

    assert cloud_server.duckdb_files() == []


def test_f3_cloud_paper_order_and_books_lifecycle_unhampered(server_factory) -> None:
    """Full paper order/book/funds/position/close lifecycle works end-to-end under cloud profile."""
    srv = server_factory(profile="cloud", paper_trading_only=True)
    broker = "deltaexchange"
    spec = DEFAULT_SYMBOLS[broker]
    strategy = "e2e_f3_lifecycle"

    place = srv.api_post(
        "/api/v1/placeorder",
        {
            "apikey": srv.api_key,
            "strategy": strategy,
            "exchange": spec["exchange"],
            "symbol": spec["symbol"],
            "action": "BUY",
            "quantity": 1,
            "pricetype": "MARKET",
            "product": spec["product"],
        },
        broker=broker,
    )
    assert place.status_code == 200 and place.json().get("mode") == "analyze"

    for endpoint in ("/api/v1/orderbook", "/api/v1/tradebook", "/api/v1/positionbook", "/api/v1/funds"):
        resp = srv.api_post(endpoint, {"apikey": srv.api_key}, broker=broker)
        assert resp.status_code == 200, f"{endpoint} failed in cloud: {resp.status_code} {resp.text[:300]}"
        assert resp.json().get("status") == "success"
        assert resp.json().get("mode") == "analyze"

    openpos = srv.api_post(
        "/api/v1/openposition",
        {
            "apikey": srv.api_key,
            "strategy": strategy,
            "symbol": spec["symbol"],
            "exchange": spec["exchange"],
            "product": spec["product"],
        },
        broker=broker,
    )
    assert openpos.status_code == 200 and openpos.json().get("status") == "success"

    close_resp = srv.api_post(
        "/api/v1/closeposition",
        {"apikey": srv.api_key, "strategy": strategy},
        broker=broker,
    )
    assert close_resp.status_code == 200 and close_resp.json().get("status") == "success"
    assert srv.duckdb_files() == []
    srv.assert_zero_real_broker_order_calls()


def test_f3_cloud_trading_and_scalping_live_surfaces_unhampered(cloud_server: OpenAlgoE2EInstance) -> None:
    """In cloud profile, /trading, /scalping, and live /scalping/api/* routes stay 200 while /scalping/api/history is blocked."""
    session = cloud_server.session_client(broker="fyers")

    for page in ("/trading", "/scalping"):
        resp = session.get(page, headers={"Accept": "text/html"})
        assert resp.status_code == 200, f"Cloud must keep live page {page} open: {resp.status_code}"

    for live_api in ("/scalping/api/underlyings", "/scalping/api/tracked", "/scalping/api/sl"):
        resp = session.get(live_api)
        assert resp.status_code == 200, (
            f"Live scalping endpoint {live_api} must stay open in cloud: {resp.status_code} {resp.text[:300]}"
        )

    hist_resp = session.get("/scalping/api/history?symbol=SBIN&exchange=NSE&interval=5m")
    _assert_historical_disabled_response(hist_resp, context="GET /scalping/api/history")


# ===========================================================================
# E2E-F4: Local Mode Full Parity (OPENALGO_PROFILE=full) — 6 Tests
# ===========================================================================


def test_f4_full_app_config_reports_full_and_history_enabled(full_server: OpenAlgoE2EInstance) -> None:
    """Under OPENALGO_PROFILE=full, /auth/app-config reports profile='full' and historical_data_enabled=True."""
    resp = full_server.api_get("/auth/app-config")
    assert resp.status_code == 200, f"/auth/app-config failed in full mode: {resp.status_code} {resp.text[:300]}"
    body = resp.json()
    assert body.get("status") == "success"
    assert body.get("profile") == "full"
    assert body.get("historical_data_enabled") is True
    assert body.get("paper_trading_only") is True
    assert set(BROKERS).issubset(set(body.get("brokers", [])))


def test_f4_full_restx_history_returns_candles(full_server: OpenAlgoE2EInstance) -> None:
    """Under OPENALGO_PROFILE=full, POST /api/v1/history returns 200 with OHLCV candles across all 3 brokers."""
    start_d, end_d = _recent_date_window(7)
    for broker in BROKERS:
        spec = DEFAULT_SYMBOLS[broker]
        resp = full_server.api_post(
            "/api/v1/history",
            {
                "apikey": full_server.api_key,
                "symbol": spec["symbol"],
                "exchange": spec["exchange"],
                "interval": "D",
                "start_date": start_d,
                "end_date": end_d,
            },
            broker=broker,
        )
        assert resp.status_code == 200, (
            f"[{broker}] /api/v1/history failed in full mode: {resp.status_code} {resp.text[:400]}"
        )
        body = resp.json()
        assert body.get("status") == "success", f"[{broker}] unexpected history body: {body}"
        candles = body.get("data", [])
        assert isinstance(candles, list) and len(candles) >= 1, (
            f"[{broker}] expected >= 1 OHLCV candle in full mode, got {body}"
        )


def test_f4_full_restx_ticker_endpoint_succeeds(full_server: OpenAlgoE2EInstance) -> None:
    """Under OPENALGO_PROFILE=full, GET /api/v1/ticker/NSE:SBIN returns 200 with candle rows."""
    start_d, end_d = _recent_date_window(7)
    resp = full_server.api_get(
        "/api/v1/ticker/NSE:SBIN",
        broker="fyers",
        query={
            "apikey": full_server.api_key,
            "interval": "D",
            "from": start_d,
            "to": end_d,
            "format": "json",
        },
    )
    assert resp.status_code == 200, f"/api/v1/ticker failed in full mode: {resp.status_code} {resp.text[:400]}"
    body = resp.json()
    assert body.get("status") == "success"
    assert isinstance(body.get("data"), list) and len(body["data"]) >= 1


def test_f4_full_portfolio_sip_sectorrotation_endpoints_available(full_server: OpenAlgoE2EInstance) -> None:
    """Under OPENALGO_PROFILE=full, portfolio benchmarks, SIP frequencies, and sector rotation options return 200."""
    checks = [
        ("/api/v1/portfolio/benchmarks", {"apikey": full_server.api_key}),
        ("/api/v1/sip/frequencies", None),
        ("/api/v1/sectorrotation/options", None),
    ]
    for path, query in checks:
        resp = full_server.api_get(path, broker="fyers", query=query)
        assert resp.status_code == 200, (
            f"Expected 200 for {path} in full mode, got {resp.status_code}: {resp.text[:300]}"
        )
        assert "HISTORICAL_DATA_DISABLED" not in resp.text


def test_f4_full_historify_and_duckdb_operational(full_server: OpenAlgoE2EInstance) -> None:
    """Under OPENALGO_PROFILE=full, Historify watchlist/catalog/stats routes return 200 and DuckDB works."""
    session = full_server.session_client(broker="fyers")
    for path in ("/historify/api/watchlist", "/historify/api/catalog", "/historify/api/stats"):
        resp = session.get(path)
        assert resp.status_code == 200, (
            f"Expected 200 for {path} in full mode, got {resp.status_code}: {resp.text[:300]}"
        )
        assert "HISTORICAL_DATA_DISABLED" not in resp.text

    add_resp = session.post(
        "/historify/api/watchlist",
        json={"symbol": "SBIN", "exchange": "NSE"},
    )
    assert add_resp.status_code in (200, 201), (
        f"Adding Historify watchlist item failed in full mode: {add_resp.status_code} {add_resp.text[:300]}"
    )


def test_f4_full_chart_test_and_scalping_history_operational(full_server: OpenAlgoE2EInstance) -> None:
    """Under OPENALGO_PROFILE=full, chart_test, scalping history, and backtester SPA pages remain operational."""
    session = full_server.session_client(broker="fyers")
    start_d, end_d = _recent_date_window(7)

    chart_resp = session.get(
        f"/chart/test/api/history?symbol=SBIN&exchange=NSE&interval=D&start_date={start_d}&end_date={end_d}"
    )
    assert chart_resp.status_code == 200, (
        f"/chart/test/api/history failed in full mode: {chart_resp.status_code} {chart_resp.text[:300]}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in chart_resp.text

    scalp_hist = session.get(
        f"/scalping/api/history?symbol=SBIN&exchange=NSE&interval=D&start_date={start_d}&end_date={end_d}"
    )
    assert scalp_hist.status_code == 200, (
        f"/scalping/api/history failed in full mode: {scalp_hist.status_code} {scalp_hist.text[:300]}"
    )
    assert "HISTORICAL_DATA_DISABLED" not in scalp_hist.text

    for spa_page in ("/historify", "/portfolio-backtester", "/sip-backtester"):
        page_resp = session.get(spa_page, headers={"Accept": "text/html"})
        assert page_resp.status_code == 200, (
            f"Expected 200 for local SPA page {spa_page} in full mode, got {page_resp.status_code}"
        )


# ===========================================================================
# E2E-F5: Frontend Runtime Config & Profile/Paper UI Contract — 6 Tests
# ===========================================================================


def test_f5_app_config_public_unauthenticated_cloud_schema(cloud_server: OpenAlgoE2EInstance) -> None:
    """Unauthenticated GET /auth/app-config returns exact public runtime config schema in cloud."""
    resp = cloud_server.api_get("/auth/app-config")
    assert resp.status_code == 200, f"GET /auth/app-config failed: {resp.status_code} {resp.text[:300]}"
    body = resp.json()
    assert body.get("status") == "success"
    assert body.get("profile") == "cloud"
    assert body.get("historical_data_enabled") is False
    assert body.get("paper_trading_only") is True
    brokers = body.get("brokers")
    assert isinstance(brokers, list) and set(BROKERS).issubset(set(brokers)), (
        f"Expected brokers containing {BROKERS}, got {brokers}"
    )


def test_f5_cloud_blocks_all_history_backtest_spa_routes_503(cloud_server: OpenAlgoE2EInstance) -> None:
    """All history and backtest SPA routes return 503 HISTORICAL_DATA_DISABLED under OPENALGO_PROFILE=cloud."""
    blocked_spa_routes = (
        "/historify",
        "/historify/charts",
        "/historify/charts/SBIN",
        "/portfolio-backtester",
        "/portfolio-backtester/results",
        "/sip-backtester",
        "/sip-backtester/results",
        "/sector-rotation",
        "/portfolio",
        "/portfolio-analyzer",
        "/pnl-tracker",
        "/ivchart",
        "/chart/test",
        "/straddle",
        "/straddlepnl",
        "/oiprofile",
    )
    session = cloud_server.session_client(broker="fyers")
    for route in blocked_spa_routes:
        resp = session.get(route, headers={"Accept": "application/json"})
        _assert_historical_disabled_response(
            resp, allowed_statuses=(503,), context=f"SPA GET {route}"
        )


def test_f5_cloud_keeps_live_trading_spa_routes_accessible_200(cloud_server: OpenAlgoE2EInstance) -> None:
    """Live trading & paper portfolio SPA routes remain accessible (200 HTML) under OPENALGO_PROFILE=cloud."""
    live_spa_routes = (
        "/dashboard",
        "/trading",
        "/scalping",
        "/sandbox",
        "/positions",
        "/orderbook",
        "/tradebook",
        "/optionchain",
        "/analyzer",
        "/playground",
    )
    session = cloud_server.session_client(broker="fyers")
    for route in live_spa_routes:
        resp = session.get(route, headers={"Accept": "text/html"})
        assert resp.status_code == 200, (
            f"Live SPA route {route} must return 200 in cloud, got {resp.status_code}: {resp.text[:200]}"
        )
        assert "HISTORICAL_DATA_DISABLED" not in resp.text


def test_f5_spa_html_injects_broker_header_interceptor(cloud_server: OpenAlgoE2EInstance) -> None:
    """Served React SPA HTML injects the X-OpenAlgo-Broker fetch/XHR interceptor hook."""
    session = cloud_server.session_client(broker="deltaexchange")
    resp = session.get("/trading", headers={"Accept": "text/html"})
    assert resp.status_code == 200
    html = resp.text
    assert "X-OpenAlgo-Broker" in html, (
        "Served SPA HTML must inject X-OpenAlgo-Broker interceptor script for multi-broker tab isolation"
    )
    assert "<script" in html.lower()


def test_f5_app_config_reflects_paper_lock_and_profile_consistently(
    cloud_server: OpenAlgoE2EInstance,
    full_server: OpenAlgoE2EInstance,
) -> None:
    """GET /auth/app-config flips profile & historical_data_enabled between cloud and full while keeping paper lock."""
    cloud_cfg = cloud_server.api_get("/auth/app-config")
    full_cfg = full_server.api_get("/auth/app-config")
    assert cloud_cfg.status_code == 200 and full_cfg.status_code == 200

    c_json = cloud_cfg.json()
    f_json = full_cfg.json()

    assert c_json["profile"] == "cloud"
    assert c_json["historical_data_enabled"] is False
    assert c_json["paper_trading_only"] is True

    assert f_json["profile"] == "full"
    assert f_json["historical_data_enabled"] is True
    assert f_json["paper_trading_only"] is True


def test_f5_frontend_dist_assets_and_paper_lock_contract(cloud_server: OpenAlgoE2EInstance) -> None:
    """Built frontend assets referenced by SPA HTML are served with 200 alongside locked paper-mode config."""
    session = cloud_server.session_client(broker="fyers")
    html_resp = session.get("/dashboard", headers={"Accept": "text/html"})
    assert html_resp.status_code == 200

    asset_paths = re.findall(r"""(?:src|href)=["'](/assets/[^"']+)["']""", html_resp.text)
    assert asset_paths, "Expected compiled /assets/* bundle references in served SPA HTML"
    for asset_path in asset_paths[:3]:
        asset_resp = cloud_server.api_get(asset_path)
        assert asset_resp.status_code == 200, (
            f"Frontend dist asset {asset_path} returned {asset_resp.status_code}"
        )
        assert len(asset_resp.content) > 0

    cfg = cloud_server.api_get("/auth/app-config").json()
    mode = session.get("/auth/analyzer-mode").json()["data"]
    assert cfg["paper_trading_only"] is True
    assert mode["analyze_mode"] is True
    assert mode["mode"] == "analyze"
