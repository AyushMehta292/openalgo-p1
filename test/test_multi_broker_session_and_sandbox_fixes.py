"""Regression tests for multi-broker session isolation, sandbox contract_value,
non-web master contract socketio.emit guard, and broker login diagnostics.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from utils.broker_context import (
    broker_context,
    broker_from_http_request,
    current_broker,
)


def test_broker_context_removes_scoped_db_sessions_on_enter_and_exit(monkeypatch):
    """Switching across broker_context() on one thread must call db_session.remove()."""
    import database.auth_db as auth_db

    removed_Under: list[str | None] = []
    orig_remove = auth_db.db_session.remove

    def tracking_remove():
        removed_Under.append(current_broker())
        return orig_remove()

    monkeypatch.setattr(auth_db.db_session, "remove", tracking_remove)

    with broker_context("fyers"):
        assert current_broker() == "fyers"
        with broker_context("deltaexchange"):
            assert current_broker() == "deltaexchange"
        assert current_broker() == "fyers"
    assert current_broker() is None
    # Called on enter fyers (None), enter deltaexchange (fyers), exit deltaexchange (deltaexchange), exit fyers (fyers)
    assert len(removed_Under) >= 4


def test_broker_from_http_request_prefers_explicit_target_over_stale_tab_header():
    """Explicit path, query, or login body broker must win over stale X-OpenAlgo-Broker header."""
    assert (
        broker_from_http_request(
            "/auth/broker/login",
            header="fyers",
            query="",
            body_broker="groww",
        )
        == "groww"
    )
    assert (
        broker_from_http_request(
            "/groww/callback",
            header="fyers",
            query="",
            body_broker="",
        )
        == "groww"
    )
    assert (
        broker_from_http_request(
            "/dashboard",
            header="fyers",
            query="deltaexchange",
            body_broker="",
        )
        == "deltaexchange"
    )
    assert (
        broker_from_http_request(
            "/api/v1/placeorder",
            header="deltaexchange",
            query="",
            body_broker="",
        )
        == "deltaexchange"
    )


def test_symbol_info_dbquery_and_cache_preserve_contract_value_and_fractional_lotsize(
    monkeypatch,
):
    """Delta BTCUSDFUT contract_value / fractional lotsize must be preserved before restart."""
    import database.symbol as sym_mod
    import database.token_db_enhanced as tdb

    fake_row = SimpleNamespace(
        symbol="BTCUSDFUT",
        brsymbol="BTCUSD",
        name="BTC",
        exchange="CRYPTO",
        brexchange="DELTAIN",
        token="27",
        expiry="",
        strike=0.0,
        lotsize=0.001,
        instrumenttype="PERPFUT",
        tick_size=0.5,
        contract_value=None,
    )

    monkeypatch.setattr(
        sym_mod,
        "SymToken",
        SimpleNamespace(
            query=SimpleNamespace(
                filter_by=lambda **_kw: SimpleNamespace(first=lambda: fake_row),
                all=lambda: [fake_row],
            )
        ),
    )

    with broker_context("deltaexchange"):
        info_db = tdb.get_symbol_info_dbquery("BTCUSDFUT", "CRYPTO")
        assert info_db is not None
        assert info_db.contract_value == pytest.approx(0.001)

        cache = tdb.BrokerSymbolCache()
        assert cache.load_all_symbols("deltaexchange") is True
        info_cached = cache.get_symbol_info("BTCUSDFUT", "CRYPTO")
        assert info_cached is not None
        assert info_cached.contract_value == pytest.approx(0.001)

    with broker_context("fyers"):
        # Fyers scope must not see deltaexchange's cached snapshot as loaded
        assert cache.cache_loaded is False


def test_sandbox_margin_applies_contract_value_multiplier(monkeypatch):
    """BTCUSDFUT qty 1 @ 76000 with contract_value=0.001 and 10x leverage blocks 7.6, not 7600."""
    import sandbox.fund_manager as fm_mod

    fake_sym = SimpleNamespace(
        symbol="BTCUSDFUT",
        exchange="CRYPTO",
        lotsize=1,
        contract_value=0.001,
        instrumenttype="PERPFUT",
    )
    monkeypatch.setattr(fm_mod, "get_symbol_info", lambda _s, _e: fake_sym)
    monkeypatch.setattr(fm_mod, "is_future", lambda _s, _e: True)
    monkeypatch.setattr(fm_mod, "get_config", lambda key, default=None: "10" if key == "futures_leverage" else default)

    fm = fm_mod.FundManager("ayushmehta")
    margin, msg = fm.calculate_margin_required(
        "BTCUSDFUT", "CRYPTO", "NRML", 1, Decimal("76000"), "BUY"
    )
    assert margin == Decimal("7.600")
    assert "successfully" in msg.lower()


def test_delta_master_contract_safe_emit_guards_non_web_context(monkeypatch):
    """socketio.emit raising outside a request/socket context must not fail master download."""
    import broker.deltaexchange.database.master_contract_db as delta_mc

    def boom(*_a, **_kw):
        raise RuntimeError("Working outside of request context")

    monkeypatch.setattr(delta_mc.socketio, "emit", boom)
    res = delta_mc._safe_emit("master_contract_download", {"status": "success"})
    assert res == {"status": "success"}


def test_groww_login_error_surfaces_errorMessage():
    """Groww 403 error.errorMessage must be included in the user-facing login error."""
    from broker.groww.api.auth_api import _login_error

    body = {
        "header": None,
        "error": {
            "errorCode": "403",
            "errorMessage": "Session approval required before generating token",
        },
        "response": None,
    }
    msg = _login_error(body, 403)
    assert "Session approval required before generating token" in msg


def test_order_adapter_configures_certifi_ca_bundle():
    """websocket_proxy.order_adapter must configure certifi CA bundle for macOS SSL."""
    import websocket_proxy.order_adapter as oa

    assert oa._CA_CERTS is not None
    assert oa.os.environ.get("SSL_CERT_FILE")
