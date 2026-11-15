"""A stored Fyers, Delta Exchange, or Groww token survives 03:00.

The clock does not revoke it. Startup and the first request of the trading
day ask the broker's funds endpoint. Acceptance keeps the context logged in.
Rejection revokes only that broker. A missing key or a network failure leaves
the token in place.

No broker is called. The funds functions are given a fake client, or replaced
outright.
"""

from __future__ import annotations

from datetime import datetime as real_datetime
from pathlib import Path

import httpx
import pytest
import pytz
from flask import Flask, jsonify, request, session

import database.auth_db as auth_db
import utils.broker_keys as broker_keys
import utils.daily_token_check as daily
import utils.session as session_utils
from database.engine_factory import create_db_engine
from services.order_update_service import scan_stored_broker_sessions
from utils.broker_context import BrokerContextError, broker_context
from utils.session import check_session_validity, is_session_valid
from utils.tab_session import TabSessionInterface

IST = pytz.timezone("Asia/Kolkata")
ROOT = Path(__file__).resolve().parents[1]


def ist(*args) -> real_datetime:
    return IST.localize(real_datetime(*args))


@pytest.fixture(autouse=True)
def _reset_daily_checks():
    daily.reset_checks_for_tests()
    yield
    daily.reset_checks_for_tests()


@pytest.fixture
def clock(monkeypatch):
    def set_to(moment: real_datetime) -> None:
        class Frozen(real_datetime):
            @classmethod
            def now(cls, tz=None):
                return moment.astimezone(tz) if tz else moment.replace(tzinfo=None)

        monkeypatch.setattr(session_utils, "datetime", Frozen)

    return set_to


@pytest.fixture
def auth_store(tmp_path, monkeypatch):
    """Auth rows on a file of their own, restored when the test ends."""
    from sqlalchemy.orm import scoped_session, sessionmaker

    engine = create_db_engine(f"sqlite:///{(tmp_path / 'auth.db').as_posix()}")
    store = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    monkeypatch.setattr(auth_db, "engine", engine)
    monkeypatch.setattr(auth_db, "db_session", store)
    previous = auth_db.Base.__dict__["query"]
    auth_db.Base.query = store.query_property()
    auth_db.Base.metadata.create_all(engine)
    try:
        yield store
    finally:
        auth_db.Base.query = previous
        store.remove()
        engine.dispose()


def _store_token(store, broker, token="stored-token", username="am"):
    store.add(
        auth_db.Auth(
            name=username,
            auth=auth_db.encrypt_token(token),
            broker=broker,
            is_revoked=False,
        )
    )
    store.commit()


def _funds(outcome):
    calls = []

    def classify(token):
        calls.append(token)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    module = type("Funds", (), {"classify_auth_token": staticmethod(classify)})()
    return module, calls


def _install_funds(monkeypatch, broker, outcome):
    module, calls = _funds(outcome)
    real = daily.importlib.import_module

    def fake(name):
        if name == f"broker.{broker}.api.funds":
            return module
        return real(name)

    monkeypatch.setattr(daily.importlib, "import_module", fake)
    return calls


# --- the clock -------------------------------------------------------------


def test_an_unset_broker_variable_is_not_present():
    """Membership follows the process environment.

    Reading the variable with no broker selected still yields an empty
    string, so a missing key is not a broker name. Deleting is only attempted
    when the variable is really set.
    """
    import os

    name = "BROKER_API_SECRET_MARKET"
    if name in os.environ:
        del os.environ[name]
    assert name not in os.environ
    assert os.getenv(name, "missing") == ""


def test_the_clock_does_not_revoke_fyers_delta_or_groww():
    assert daily.clock_may_revoke("fyers") is False
    assert daily.clock_may_revoke("deltaexchange") is False
    assert daily.clock_may_revoke("groww") is False
    assert daily.clock_may_revoke(None) is False
    assert daily.clock_may_revoke("zerodha") is True


@pytest.mark.parametrize("broker", ["fyers", "deltaexchange", "groww"])
def test_a_tab_on_these_brokers_stays_valid_after_the_rollover(clock, broker):
    clock(ist(2026, 10, 30, 10, 0))
    app = Flask(__name__)
    app.secret_key = "test"
    app.session_interface = TabSessionInterface()
    with app.test_request_context("/dashboard", headers={"X-OpenAlgo-Broker": broker}):
        session["user"] = "am"
        session["logged_in"] = True
        session["login_time"] = ist(2026, 10, 29, 10, 0).isoformat()
        session["broker"] = broker
        assert is_session_valid() is True


def test_a_session_with_no_broker_still_ends_at_the_rollover(clock):
    """Routes that read is_session_valid directly keep the old clock.

    The expiry hook must not revoke when this returns false and the request
    named no broker. That case is test_a_page_without_a_broker_does_not_revoke.
    """
    clock(ist(2026, 10, 30, 10, 0))
    app = Flask(__name__)
    app.secret_key = "test"
    with app.test_request_context("/dashboard"):
        session["user"] = "am"
        session["logged_in"] = True
        session["login_time"] = ist(2026, 10, 29, 10, 0).isoformat()
        assert is_session_valid() is False


def _tab_app():
    app = Flask(__name__)
    app.secret_key = "test"
    app.session_interface = TabSessionInterface()

    @app.before_request
    def _name_the_tab():
        name = request.headers.get("X-OpenAlgo-Broker")
        if name:
            session["broker"] = name

    @app.route("/page")
    @check_session_validity
    def page():
        return jsonify(ok=True)

    return app


def test_a_fyers_tab_is_not_logged_out_by_the_decorator(clock, monkeypatch):
    clock(ist(2026, 10, 30, 10, 0))
    monkeypatch.delenv("DISABLE_SESSION_EXPIRY", raising=False)
    revoked = []
    monkeypatch.setattr(session_utils, "revoke_user_tokens", lambda *a, **k: revoked.append(1))
    client = _tab_app().test_client()
    with client.session_transaction() as cookie:
        cookie["user"] = "am"
        cookie["logged_in"] = True
        cookie["login_time"] = ist(2026, 10, 29, 10, 0).isoformat()

    response = client.get("/page", headers={"X-OpenAlgo-Broker": "fyers", "Accept": "application/json"})

    assert response.status_code == 200
    assert revoked == []
    with client.session_transaction() as cookie:
        assert cookie.get("user") == "am"


def test_a_page_without_a_broker_does_not_revoke_after_the_rollover(clock, monkeypatch):
    clock(ist(2026, 10, 30, 10, 0))
    monkeypatch.delenv("DISABLE_SESSION_EXPIRY", raising=False)
    revoked = []
    monkeypatch.setattr(session_utils, "revoke_user_tokens", lambda *a, **k: revoked.append(1))
    client = _tab_app().test_client()
    with client.session_transaction() as cookie:
        cookie["user"] = "am"
        cookie["logged_in"] = True
        cookie["login_time"] = ist(2026, 10, 29, 10, 0).isoformat()

    response = client.get("/page", headers={"Accept": "application/json"})

    assert response.status_code == 401
    assert revoked == []
    with client.session_transaction() as cookie:
        assert cookie.get("logged_in") is True


def test_a_named_broker_outside_the_three_is_still_revoked_at_the_rollover(clock, monkeypatch):
    clock(ist(2026, 10, 30, 10, 0))
    monkeypatch.delenv("DISABLE_SESSION_EXPIRY", raising=False)
    revoked = []
    monkeypatch.setattr(session_utils, "revoke_user_tokens", lambda *a, **k: revoked.append(1))
    client = _tab_app().test_client()
    with client.session_transaction() as cookie:
        cookie["user"] = "am"
        cookie["logged_in"] = True
        cookie["login_time"] = ist(2026, 10, 29, 10, 0).isoformat()

    response = client.get(
        "/page", headers={"X-OpenAlgo-Broker": "zerodha", "Accept": "application/json"}
    )

    assert response.status_code == 401
    assert revoked == [1]


# --- funds check -----------------------------------------------------------


@pytest.mark.parametrize("broker", ["fyers", "deltaexchange", "groww"])
def test_an_accepted_token_is_checked_once_and_stays_stored(auth_store, monkeypatch, broker):
    _store_token(auth_store, broker)
    calls = _install_funds(monkeypatch, broker, "accepted")

    with broker_context(broker):
        assert daily.check_broker_token(broker) == "accepted"
        assert daily.check_broker_token(broker) == "accepted"

    assert calls == ["stored-token"]
    row = auth_store.query(auth_db.Auth).one()
    assert row.is_revoked is False
    assert auth_db.decrypt_token(row.auth) == "stored-token"


@pytest.mark.parametrize("broker", ["fyers", "deltaexchange", "groww"])
def test_a_rejected_token_revokes_only_that_broker(auth_store, monkeypatch, broker):
    _store_token(auth_store, broker, username="am")
    _install_funds(monkeypatch, broker, "rejected")
    revoked = []

    def fake_upsert(name, token, broker_name, revoke=False, **kwargs):
        revoked.append((name, token, broker_name, revoke))
        row = auth_store.query(auth_db.Auth).filter_by(name=name).one()
        row.is_revoked = True
        auth_store.commit()
        return 1

    monkeypatch.setattr(auth_db, "upsert_auth", fake_upsert)

    with broker_context(broker):
        assert daily.check_broker_token(broker) == "rejected"
        assert daily.check_broker_token(broker) == "absent"

    assert revoked == [("am", "", broker, True)]


def test_a_network_failure_leaves_the_token_and_backs_off(auth_store, monkeypatch):
    _store_token(auth_store, "fyers")
    calls = _install_funds(monkeypatch, "fyers", httpx.ConnectError("down"))

    with broker_context("fyers"):
        assert daily.check_broker_token("fyers") == "unchecked"
        assert daily.check_broker_token("fyers") == "unchecked"

    assert len(calls) == 1
    row = auth_store.query(auth_db.Auth).one()
    assert row.is_revoked is False


def test_a_missing_token_does_not_call_the_broker(auth_store, monkeypatch):
    calls = _install_funds(monkeypatch, "groww", "accepted")
    with broker_context("groww"):
        assert daily.check_broker_token("groww") == "absent"
    assert calls == []


def test_the_check_refuses_to_run_outside_its_broker(auth_store):
    with pytest.raises(BrokerContextError):
        daily.check_broker_token("fyers")


def test_the_next_trading_day_asks_again(auth_store, monkeypatch):
    _store_token(auth_store, "deltaexchange")
    calls = _install_funds(monkeypatch, "deltaexchange", "accepted")
    days = iter(["2026-11-06", "2026-11-07"])
    monkeypatch.setattr(daily, "_day_key", lambda: next(days))

    with broker_context("deltaexchange"):
        daily.check_broker_token("deltaexchange")
        daily.check_broker_token("deltaexchange")

    assert len(calls) == 2


def test_an_unrevoked_fyers_token_from_before_today_is_still_live(auth_store, monkeypatch):
    monkeypatch.delenv("DISABLE_SESSION_EXPIRY", raising=False)
    broker_keys.forget_logged_in_broker()
    _store_token(auth_store, "fyers")
    assert broker_keys.live_broker() == "fyers"


def test_an_unrevoked_zerodha_token_from_before_today_is_not_live(auth_store, monkeypatch):
    monkeypatch.delenv("DISABLE_SESSION_EXPIRY", raising=False)
    broker_keys.forget_logged_in_broker()
    _store_token(auth_store, "zerodha")
    assert broker_keys.live_broker() is None


def test_a_funds_accepted_fyers_token_starts_the_order_feed_without_a_login_today(
    auth_store, monkeypatch
):
    _store_token(auth_store, "fyers", username="am")
    monkeypatch.setattr(daily, "check_broker_token", lambda broker: "accepted")
    monkeypatch.setattr(session_utils, "has_login_this_trading_session", lambda username: False)
    started = []

    with broker_context("fyers"):
        scan_stored_broker_sessions(start_adapter=lambda name, broker: started.append((name, broker)))

    assert started == [("am", "fyers")]


def test_a_funds_rejected_fyers_token_does_not_start_the_order_feed(auth_store, monkeypatch):
    _store_token(auth_store, "fyers", username="am")

    def reject(broker):
        row = auth_store.query(auth_db.Auth).one()
        row.is_revoked = True
        auth_store.commit()
        return "rejected"

    monkeypatch.setattr(daily, "check_broker_token", reject)
    started = []
    with broker_context("fyers"):
        scan_stored_broker_sessions(start_adapter=lambda name, broker: started.append((name, broker)))
    assert started == []


# --- what each broker's funds call answers ---------------------------------


class _Response:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class _Client:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    def get(self, url, headers=None, timeout=None):
        self.calls.append(url)
        if self.error:
            raise self.error
        return self.response


def _patched_getenv(monkeypatch, module, values):
    """The process getenv is the broker-key overlay. These tests name the key."""

    def getenv(key, default=None):
        if key in values:
            return values[key]
        return default

    monkeypatch.setattr(module.os, "getenv", getenv)


def test_fyers_accepts_a_funds_payload_and_rejects_an_auth_error(monkeypatch):
    import broker.fyers.api.funds as funds

    _patched_getenv(monkeypatch, funds, {"BROKER_API_KEY": "APP"})
    client = _Client(_Response(200, {"code": 200, "fund_limit": []}))
    monkeypatch.setattr(funds, "get_httpx_client", lambda: client)
    assert funds.classify_auth_token("access") == "accepted"
    assert "api/v3/funds" in client.calls[0]

    client = _Client(_Response(401, {"code": -15, "message": "invalid"}))
    monkeypatch.setattr(funds, "get_httpx_client", lambda: client)
    assert funds.classify_auth_token("access") == "rejected"


def test_fyers_does_not_treat_a_missing_key_or_a_transport_error_as_rejection(monkeypatch):
    import broker.fyers.api.funds as funds

    _patched_getenv(monkeypatch, funds, {"BROKER_API_KEY": ""})
    assert funds.classify_auth_token("access") == "unchecked"

    _patched_getenv(monkeypatch, funds, {"BROKER_API_KEY": "APP"})
    client = _Client(error=httpx.ConnectError("down"))
    monkeypatch.setattr(funds, "get_httpx_client", lambda: client)
    assert funds.classify_auth_token("access") == "unchecked"


def test_groww_accepts_success_and_rejects_an_auth_status(monkeypatch):
    import broker.groww.api.funds as funds

    client = _Client()
    monkeypatch.setattr(funds, "get_httpx_client", lambda: client)

    def ok(http, method, url, category, **kwargs):
        return _Response(200, {"status": "SUCCESS", "payload": {}})

    monkeypatch.setattr(funds, "groww_request", ok)
    assert funds.classify_auth_token("groww-token") == "accepted"

    def denied(http, method, url, category, **kwargs):
        return _Response(401, {"status": "FAILURE"})

    monkeypatch.setattr(funds, "groww_request", denied)
    assert funds.classify_auth_token("groww-token") == "rejected"


def test_delta_skips_the_call_without_a_secret_and_rejects_a_bad_signature(monkeypatch):
    import broker.deltaexchange.api.funds as funds

    _patched_getenv(monkeypatch, funds, {"BROKER_API_SECRET": ""})
    assert funds.classify_auth_token("key") == "unchecked"

    _patched_getenv(monkeypatch, funds, {"BROKER_API_SECRET": "secret"})
    client = _Client(_Response(200, {"success": False, "error": {"code": "Unauthorized"}}))
    monkeypatch.setattr(funds, "get_httpx_client", lambda: client)
    monkeypatch.setattr(funds, "consume", lambda *a, **k: None)
    monkeypatch.setattr(funds, "get_auth_headers", lambda **k: {"api-key": "key"})
    assert funds.classify_auth_token("key") == "rejected"
    assert client.calls


def test_delta_transport_failure_is_not_a_rejection(monkeypatch):
    import broker.deltaexchange.api.funds as funds

    _patched_getenv(monkeypatch, funds, {"BROKER_API_SECRET": "secret"})
    monkeypatch.setattr(funds, "consume", lambda *a, **k: None)
    monkeypatch.setattr(funds, "get_auth_headers", lambda **k: {})
    monkeypatch.setattr(funds, "get_httpx_client", lambda: _Client(error=httpx.ConnectError("down")))
    assert funds.classify_auth_token("key") == "unchecked"


def test_refresh_tokens_are_not_redeemed():
    """Fyers documents authorization_code only. Delta and Groww have no refresh token."""
    fyers_auth = (ROOT / "broker" / "fyers" / "api" / "auth_api.py").read_text()
    checker = (ROOT / "utils" / "daily_token_check.py").read_text()
    assert '"grant_type": "authorization_code"' in fyers_auth
    assert '"grant_type": "refresh_token"' not in fyers_auth
    assert "validate-refresh-token" not in checker
    assert "importlib.import_module" in checker
    for broker in ("groww", "deltaexchange"):
        tree = ROOT / "broker" / broker
        for path in tree.rglob("*.py"):
            assert "refresh_token" not in path.read_text()
