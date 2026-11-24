"""Milestone 1 runtime hygiene test suite (F2, F3, F4).

Covers:
- F2: BrokerContextThreadPoolExecutor, _BROKER_SCOPED_SESSIONS, EventBus context
  propagation, sqlite_url_needs_broker + no-broker guards, and end-to-end Flask
  RESTX integration for /api/v1/quotes, /api/v1/openposition, /api/v1/placeorder
  asserting ZERO BrokerContextError logs and persisted latency + analyzer rows.
- F3: Bounded startup tail truncation of errors.jsonl (large >2.5 MB file with
  mid-line seek & invalid UTF-8 bytes), small file untouched, and RotatingFileHandler
  setup with env-configurable maxBytes/backupCount.
- F4: health.db payload compaction (UI schema preserved), 2-day retention defaults &
  env overrides, periodic write-cadence pruning & older-row detail compaction,
  timestamp index, and incremental auto_vacuum / WAL truncate file shrinkage (>50%)
  on both brand-new and legacy auto_vacuum=0 SQLite databases.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import pytest
from flask import Flask, g, request
from sqlalchemy import text
from sqlalchemy.orm import scoped_session, sessionmaker

import database  # noqa: F401  ensures SQLite broker retargeting guard is installed
import database.analyzer_db as analyzer_db
import database.apilog_db as apilog_db
import database.auth_db as auth_db
import database.health_db as health_db
import database.latency_db as latency_db
import database.sandbox_db as sandbox_db
import database.settings_db as settings_db
import database.symbol as symbol_db
import database.token_db_enhanced as token_db_enhanced
import utils.broker_context as bc
import utils.latency_monitor as latency_monitor
import utils.logging as oa_logging
from database.engine_factory import create_db_engine
from utils.broker_context import (
    BROKERS,
    BrokerContextThreadPoolExecutor,
    broker_context,
    current_broker,
    pop_broker,
    push_broker,
    retarget_sqlite_database,
    sqlite_url_needs_broker,
)
from utils.event_bus import EventBus


# ============================================================================
# Shared Helpers
# ============================================================================


class _RecordCollector(logging.Handler):
    """Thread-safe log handler capturing all emitted LogRecords."""

    def __init__(self, level: int = logging.ERROR) -> None:
        super().__init__(level)
        self._lock = threading.Lock()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        with self._lock:
            self.records.append(record)

    def broker_context_errors(self) -> list[logging.LogRecord]:
        with self._lock:
            hits = []
            for rec in self.records:
                msg = rec.getMessage()
                exc_text = ""
                if rec.exc_info and rec.exc_info[1] is not None:
                    exc_text = f"{type(rec.exc_info[1]).__name__}: {rec.exc_info[1]}"
                combined = f"{msg} {exc_text}"
                if (
                    "BrokerContextError" in combined
                    or "Enter a broker context before opening a broker database" in combined
                ):
                    hits.append(rec)
            return hits


def _rebind_module_db(monkeypatch, module, engine, session_attr="db_session", base_attr="Base"):
    """Swap a database module's engine/session/Base.query onto ``engine``."""
    sess = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
    if hasattr(module, "engine"):
        monkeypatch.setattr(module, "engine", engine)
    if hasattr(module, "health_engine"):
        monkeypatch.setattr(module, "health_engine", engine)
    if hasattr(module, "latency_engine"):
        monkeypatch.setattr(module, "latency_engine", engine)
    monkeypatch.setattr(module, session_attr, sess)
    base = getattr(module, base_attr, None)
    orig_query = base.__dict__.get("query") if base is not None else None
    if base is not None:
        base.query = sess.query_property()
    return sess, base, orig_query


# ============================================================================
# F2 — BrokerContextThreadPoolExecutor, Sessions, EventBus, Guards & E2E Flow
# ============================================================================


def test_f2_executor_submit_args_kwargs_including_fn_and_clean_teardown():
    def worker(a, b, *, fn="default", extra=0):
        return (current_broker(), a + b + extra, fn)

    with BrokerContextThreadPoolExecutor(max_workers=1) as pool:
        with broker_context("deltaexchange"):
            fut = pool.submit(worker, 2, 3, fn="custom-fn-arg", extra=5)
        assert fut.result(timeout=5) == ("deltaexchange", 10, "custom-fn-arg")

        # Worker thread must have popped the broker context cleanly on exit
        outside_fut = pool.submit(current_broker)
        assert outside_fut.result(timeout=5) is None


def test_f2_executor_map_propagates_broker_context():
    def capture(item):
        return (item, current_broker())

    with BrokerContextThreadPoolExecutor(max_workers=2) as pool:
        with broker_context("groww"):
            results = list(pool.map(capture, [1, 2, 3, 4], timeout=5))
        assert results == [(i, "groww") for i in [1, 2, 3, 4]]

        # Subsequent outside-context call sees None
        outside = list(pool.map(lambda x: (x, current_broker()), [10, 20], timeout=5))
        assert outside == [(10, None), (20, None)]


def test_f2_executor_concurrent_cross_broker_single_worker_zero_bleed(tmp_path, monkeypatch):
    monkeypatch.setattr(bc, "PROJECT_ROOT", tmp_path)
    logical_db = tmp_path / "db" / "openalgo.db"
    logical_db.parent.mkdir(parents=True, exist_ok=True)

    def write_marker(tag: str) -> tuple[str | None, str]:
        active = current_broker()
        dest = retarget_sqlite_database(str(logical_db))
        conn = sqlite3.connect(dest)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS markers (broker TEXT, tag TEXT)")
            conn.execute("INSERT INTO markers (broker, tag) VALUES (?, ?)", (active, tag))
            conn.commit()
        finally:
            conn.close()
        return active, dest

    with BrokerContextThreadPoolExecutor(max_workers=1) as pool:
        futures = []
        for idx in range(9):
            broker = BROKERS[idx % len(BROKERS)]
            with broker_context(broker):
                futures.append((broker, pool.submit(write_marker, f"{broker}-{idx}")))

        for expected_broker, fut in futures:
            observed_broker, dest_path = fut.result(timeout=5)
            assert observed_broker == expected_broker
            assert Path(dest_path) == (tmp_path / "db" / "brokers" / expected_broker / "openalgo.db")

        # Worker thread context is completely clean after all tasks
        assert pool.submit(current_broker).result(timeout=5) is None

    for broker in BROKERS:
        db_file = tmp_path / "db" / "brokers" / broker / "openalgo.db"
        assert db_file.is_file()
        with sqlite3.connect(db_file) as conn:
            rows = conn.execute("SELECT broker, tag FROM markers").fetchall()
        assert len(rows) == 3
        assert all(row[0] == broker and row[1].startswith(f"{broker}-") for row in rows)


def test_f2_broker_scoped_sessions_registry_contains_contract_entries():
    required = {
        ("database.latency_db", "latency_session"),
        ("database.analyzer_db", "db_session"),
        ("database.apilog_db", "db_session"),
        ("database.traffic_db", "logs_session"),
    }
    assert required.issubset(set(bc._BROKER_SCOPED_SESSIONS))


def test_f2_event_bus_default_and_critical_lanes_propagate_broker_context():
    bus = EventBus(workers=1, critical_workers=1)
    default_seen: list[str | None] = []
    critical_seen: list[str | None] = []
    default_done = threading.Event()
    critical_done = threading.Event()

    def on_default(_event):
        default_seen.append(current_broker())
        default_done.set()

    def on_critical(_event):
        critical_seen.append(current_broker())
        critical_done.set()

    class _DummyEvent:
        topic = "order.placed"

    try:
        bus.subscribe("order.placed", on_default, "test:default", critical=False)
        bus.subscribe("order.placed", on_critical, "test:critical", critical=True)

        with broker_context("fyers"):
            bus.publish(_DummyEvent())

        assert default_done.wait(timeout=5), "default lane callback did not run"
        assert critical_done.wait(timeout=5), "critical lane callback did not run"
        assert default_seen == ["fyers"]
        assert critical_seen == ["fyers"]

        # Verify worker threads in both lanes tore down context cleanly
        assert bus._executor.submit(current_broker).result(timeout=5) is None
        assert bus._critical_executor.submit(current_broker).result(timeout=5) is None
    finally:
        bus._executor.shutdown(wait=True)
        bus._critical_executor.shutdown(wait=True)


def test_f2_sqlite_url_needs_broker_and_async_logger_no_broker_guards(tmp_path, monkeypatch):
    monkeypatch.setattr(bc, "PROJECT_ROOT", tmp_path)
    db_root = tmp_path / "db"
    db_root.mkdir(parents=True, exist_ok=True)

    # Positive cases: broker-scoped SQLite files under PROJECT_ROOT/db/
    assert sqlite_url_needs_broker(f"sqlite:///{(db_root / 'openalgo.db').as_posix()}") is True
    assert sqlite_url_needs_broker(f"sqlite:///{(db_root / 'latency.db').as_posix()}") is True
    assert sqlite_url_needs_broker(f"sqlite:///{(db_root / 'health.db').as_posix()}") is True
    monkeypatch.chdir(tmp_path)
    assert sqlite_url_needs_broker("sqlite:///db/openalgo.db") is True

    # Negative cases: memory, shared, already-scoped, outside tmp, non-sqlite, empty
    assert sqlite_url_needs_broker("sqlite:///:memory:") is False
    assert sqlite_url_needs_broker("sqlite:///file:memdb?mode=memory") is False
    assert sqlite_url_needs_broker(f"sqlite:///{(db_root / 'shared' / 'openalgo.db').as_posix()}") is False
    assert sqlite_url_needs_broker(f"sqlite:///{(db_root / 'brokers' / 'fyers' / 'openalgo.db').as_posix()}") is False
    assert sqlite_url_needs_broker("sqlite:////tmp/isolated-test.db") is False
    assert sqlite_url_needs_broker("postgresql://user:pass@localhost/openalgo") is False
    assert sqlite_url_needs_broker("") is False
    assert sqlite_url_needs_broker(None) is False

    # Guard tests: calling async loggers outside broker_context against project-root DBs
    # must return cleanly with zero BrokerContextError logged or raised.
    openalgo_engine = create_db_engine(f"sqlite:///{(db_root / 'openalgo.db').as_posix()}")
    latency_engine = create_db_engine(f"sqlite:///{(db_root / 'latency.db').as_posix()}")

    apilog_sess, apilog_base, apilog_q = _rebind_module_db(
        monkeypatch, apilog_db, openalgo_engine, "db_session", "Base"
    )
    analyzer_sess, analyzer_base, analyzer_q = _rebind_module_db(
        monkeypatch, analyzer_db, openalgo_engine, "db_session", "Base"
    )
    lat_sess, lat_base, lat_q = _rebind_module_db(
        monkeypatch, latency_db, latency_engine, "latency_session", "LatencyBase"
    )
    monkeypatch.setattr(latency_monitor, "latency_session", lat_sess)

    collector = _RecordCollector(logging.ERROR)
    watched_loggers = [
        logging.getLogger("database.apilog_db"),
        logging.getLogger("database.analyzer_db"),
        logging.getLogger("database.latency_db"),
        logging.getLogger("utils.latency_monitor"),
    ]
    for lgr in watched_loggers:
        lgr.addHandler(collector)

    try:
        assert current_broker() is None
        apilog_db.async_log_order("openposition", {"symbol": "BTCUSDFUT"}, {"status": "error"})
        analyzer_db.async_log_analyzer({"symbol": "BTCUSDFUT"}, {"status": "error"}, "placeorder")
        latency_monitor._log_latency_async(
            order_id="test-unauth",
            user_id="unknown",
            api_key=None,
            symbol="BTCUSDFUT",
            order_type="QUOTES",
            latencies={"rtt_ms": 1.0, "validation_ms": 0.1, "response_ms": 0.1, "overhead_ms": 0.2, "total_ms": 1.2},
            status="FAILED",
            error="Invalid API key",
            broker=None,
        )
        assert collector.broker_context_errors() == []
        assert collector.records == []
    finally:
        for lgr in watched_loggers:
            lgr.removeHandler(collector)
        apilog_base.query = apilog_q
        analyzer_base.query = analyzer_q
        lat_base.query = lat_q
        apilog_sess.remove()
        analyzer_sess.remove()
        lat_sess.remove()
        openalgo_engine.dispose()
        latency_engine.dispose()


def test_f2_flask_restx_deltaexchange_zero_broker_context_errors_and_persisted_rows(
    tmp_path, monkeypatch
):
    """End-to-end Flask test client reproducing the exact production storm scenario:
    POST /api/v1/quotes, POST /api/v1/openposition, POST /api/v1/placeorder
    with X-OpenAlgo-Broker: deltaexchange in paper/analyze mode.
    """
    from limiter import limiter
    from restx_api import api as restx_api_obj
    from restx_api import api_v1_bp
    from subscribers import log_subscriber
    from utils.event_bus import bus as global_bus

    monkeypatch.setattr(bc, "PROJECT_ROOT", tmp_path)
    db_root = tmp_path / "db"
    db_root.mkdir(parents=True, exist_ok=True)

    openalgo_url = f"sqlite:///{(db_root / 'openalgo.db').as_posix()}"
    latency_url = f"sqlite:///{(db_root / 'latency.db').as_posix()}"
    sandbox_url = f"sqlite:///{(db_root / 'sandbox.db').as_posix()}"

    openalgo_engine = create_db_engine(openalgo_url)
    latency_engine = create_db_engine(latency_url)
    sandbox_engine = create_db_engine(sandbox_url)

    rebound = []
    for mod, eng, sess_attr, base_attr in [
        (auth_db, openalgo_engine, "db_session", "Base"),
        (settings_db, openalgo_engine, "db_session", "Base"),
        (analyzer_db, openalgo_engine, "db_session", "Base"),
        (apilog_db, openalgo_engine, "db_session", "Base"),
        (symbol_db, openalgo_engine, "db_session", "Base"),
        (latency_db, latency_engine, "latency_session", "LatencyBase"),
        (sandbox_db, sandbox_engine, "db_session", "Base"),
    ]:
        rebound.append((mod, *_rebind_module_db(monkeypatch, mod, eng, sess_attr, base_attr)))

    # Patch imported session references in latency_monitor, sandbox managers, and URL constants
    import sandbox.fund_manager as sandbox_fund_manager
    import sandbox.holdings_manager as sandbox_holdings_manager
    import sandbox.order_manager as sandbox_order_manager
    import sandbox.position_manager as sandbox_position_manager

    for sb_mod in (
        sandbox_fund_manager,
        sandbox_order_manager,
        sandbox_position_manager,
        sandbox_holdings_manager,
    ):
        if hasattr(sb_mod, "db_session"):
            monkeypatch.setattr(sb_mod, "db_session", sandbox_db.db_session)

    monkeypatch.setattr(latency_monitor, "latency_session", latency_db.latency_session)
    monkeypatch.setattr(latency_monitor, "auth_db_session", auth_db.db_session)
    monkeypatch.setattr(apilog_db, "DATABASE_URL", openalgo_url)
    monkeypatch.setattr(analyzer_db, "DATABASE_URL", openalgo_url)
    monkeypatch.setattr(latency_db, "LATENCY_DATABASE_URL", latency_url)
    monkeypatch.setattr(sandbox_db, "SANDBOX_DATABASE_URL", sandbox_url)

    # Clear caches so reads go strictly to our isolated tmp_path DBs
    auth_db.auth_cache.clear()
    auth_db.verified_api_key_cache.clear()
    auth_db.invalid_api_key_cache.clear()
    auth_db.feed_token_cache.clear()
    settings_db.clear_settings_cache()
    token_db_enhanced.clear_cache()

    # Initialize deltaexchange broker DBs and seed user, API key, symbol, sandbox funds, analyze_mode=True
    test_user = "m1_delta_tester"
    test_api_key = "m1-delta-test-api-key-1234567890"
    with broker_context("deltaexchange"):
        auth_db.init_db()
        settings_db.init_db()
        analyzer_db.init_db()
        apilog_db.init_db()
        symbol_db.init_db()
        latency_db.init_latency_db()
        sandbox_db.init_db()

        settings_db.set_analyze_mode(True)
        auth_db.upsert_auth(
            name=test_user,
            auth_token="delta-test-auth-token",
            broker="deltaexchange",
            feed_token="delta-test-feed-token",
            revoke=False,
        )
        auth_db.upsert_api_key(user_id=test_user, api_key=test_api_key)

        sym = symbol_db.SymToken(
            symbol="BTCUSDFUT",
            brsymbol="BTCUSD",
            name="Bitcoin Perpetual",
            exchange="CRYPTO",
            brexchange="CRYPTO",
            token="27",
            expiry="",
            strike=0.0,
            lotsize=1,
            instrumenttype="FUT",
            tick_size=0.5,
        )
        symbol_db.db_session.add(sym)
        symbol_db.db_session.commit()

        # Seed sandbox funds so LIMIT/MARKET paper order succeeds cleanly
        from sandbox.fund_manager import get_user_funds

        get_user_funds(test_user)

    # Mock ONLY external broker HTTP quote fetch (no network calls)
    class _FakeDeltaBrokerData:
        def __init__(self, auth_token):
            self.auth_token = auth_token

        def get_quotes(self, symbol, exchange):
            return {
                "ltp": 95000.0,
                "open": 94500.0,
                "high": 95500.0,
                "low": 94000.0,
                "prev_close": 94800.0,
                "volume": 1234.0,
                "oi": 500.0,
                "bid": 94999.5,
                "ask": 95000.5,
            }

    class _FakeDeltaModule:
        BrokerData = _FakeDeltaBrokerData

    import services.quotes_service as quotes_service

    monkeypatch.setattr(
        quotes_service,
        "import_broker_module",
        lambda broker_name: _FakeDeltaModule if broker_name == "deltaexchange" else None,
    )
    # Prevent socketio emits during test from failing if socketio not initialized
    import services.openposition_service as openposition_service

    monkeypatch.setattr(openposition_service, "_emit_analyzer_update", lambda payload: None)

    # Ensure latency tracking wraps quotes, openposition, place_order resources (without double-wrapping)
    from restx_api.openposition import OpenPosition
    from restx_api.place_order import PlaceOrder
    from restx_api.quotes import Quotes

    orig_methods = {}
    for cls, api_type in [
        (Quotes, "QUOTES"),
        (OpenPosition, "POSITION"),
        (PlaceOrder, "PLACE"),
    ]:
        orig_methods[cls] = cls.post
        cls.post = latency_monitor.track_latency(api_type)(cls.post)

    # Ensure log_subscriber is subscribed on global bus (avoid duplicates)
    existing_log_subs = [
        s.name for s in global_bus._subscribers.get("order.placed", [])
    ]
    added_sub = False
    if "log:order_placed" not in existing_log_subs:
        global_bus.subscribe("order.placed", log_subscriber.on_order_placed, "log:order_placed")
        added_sub = True

    # Build minimal Flask app with real api_v1_bp + bind_tab_broker/release_tab_broker
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.config["RATELIMIT_ENABLED"] = False
    app.secret_key = "m1-test-secret"
    limiter.init_app(app)

    @app.before_request
    def _bind_broker():
        broker = bc.broker_from_http_request(
            request.path,
            request.headers.get("X-OpenAlgo-Broker", ""),
            request.args.get("broker", ""),
            "",
        )
        if broker:
            g._tab_state = {"broker": broker}
            g._broker_token = push_broker(broker)

    @app.teardown_request
    def _release_broker(_exc=None):
        pop_broker(getattr(g, "_broker_token", None))

    app.register_blueprint(api_v1_bp)

    # Attach log collector across all relevant loggers
    collector = _RecordCollector(logging.ERROR)
    watched_names = [
        "",
        "database.auth_db",
        "database.latency_db",
        "database.analyzer_db",
        "database.apilog_db",
        "database.sandbox_db",
        "utils.latency_monitor",
        "utils.event_bus",
    ]
    watched = [logging.getLogger(name) for name in watched_names]
    for lgr in watched:
        lgr.addHandler(collector)

    headers = {"X-OpenAlgo-Broker": "deltaexchange"}
    try:
        client = app.test_client()

        # 1. POST /api/v1/quotes
        r_quotes = client.post(
            "/api/v1/quotes",
            json={"apikey": test_api_key, "symbol": "BTCUSDFUT", "exchange": "CRYPTO"},
            headers=headers,
        )
        assert r_quotes.status_code == 200, r_quotes.get_data(as_text=True)
        assert r_quotes.get_json()["status"] == "success"

        # 2. POST /api/v1/openposition (analyze mode -> logs to analyzer_logs via apilog_db.executor)
        r_pos = client.post(
            "/api/v1/openposition",
            json={
                "apikey": test_api_key,
                "strategy": "m1_smoke",
                "symbol": "BTCUSDFUT",
                "exchange": "CRYPTO",
                "product": "NRML",
            },
            headers=headers,
        )
        assert r_pos.status_code == 200, r_pos.get_data(as_text=True)
        assert r_pos.get_json()["status"] == "success"

        # 3. POST /api/v1/placeorder (analyze/paper mode -> sandbox + EventBus -> analyzer_logs)
        r_order = client.post(
            "/api/v1/placeorder",
            json={
                "apikey": test_api_key,
                "strategy": "m1_smoke",
                "symbol": "BTCUSDFUT",
                "action": "BUY",
                "exchange": "CRYPTO",
                "pricetype": "LIMIT",
                "product": "NRML",
                "quantity": "1",
                "price": "90000",
            },
            headers=headers,
        )
        assert r_order.status_code == 200, r_order.get_data(as_text=True)
        assert r_order.get_json()["status"] == "success"

        # Flush all background worker pools cleanly
        latency_monitor._latency_log_executor.submit(lambda: None).result(timeout=5)
        apilog_db.executor.submit(lambda: None).result(timeout=5)
        analyzer_db.executor.submit(lambda: None).result(timeout=5)
        global_bus._executor.submit(lambda: None).result(timeout=5)
        # Wait briefly for EventBus pending counter to reach 0
        deadline = time.monotonic() + 5.0
        while global_bus.stats()["pending"] > 0 and time.monotonic() < deadline:
            time.sleep(0.02)

        # Assert ZERO BrokerContextError logs anywhere
        bc_errors = collector.broker_context_errors()
        assert bc_errors == [], [r.getMessage() for r in bc_errors]

        # Assert persisted latency rows in deltaexchange/latency.db
        lat_db_path = tmp_path / "db" / "brokers" / "deltaexchange" / "latency.db"
        assert lat_db_path.is_file(), f"Expected latency DB at {lat_db_path}"
        with sqlite3.connect(lat_db_path) as conn:
            lat_rows = conn.execute(
                "SELECT order_type, broker, status FROM order_latency ORDER BY id"
            ).fetchall()
        order_types = {row[0] for row in lat_rows}
        assert len(lat_rows) >= 3, f"Expected >= 3 latency rows, got {lat_rows}"
        assert {"QUOTES", "POSITION", "PLACE"}.issubset(order_types), lat_rows
        assert all(row[1] == "deltaexchange" for row in lat_rows), lat_rows

        # Assert persisted analyzer_logs rows in deltaexchange/openalgo.db
        oa_db_path = tmp_path / "db" / "brokers" / "deltaexchange" / "openalgo.db"
        assert oa_db_path.is_file(), f"Expected openalgo DB at {oa_db_path}"
        with sqlite3.connect(oa_db_path) as conn:
            an_rows = conn.execute(
                "SELECT api_type FROM analyzer_logs ORDER BY id"
            ).fetchall()
        api_types = {row[0] for row in an_rows}
        assert "openposition" in api_types, an_rows
        assert "placeorder" in api_types, an_rows
    finally:
        for lgr in watched:
            lgr.removeHandler(collector)
        for cls, orig in orig_methods.items():
            cls.post = orig
        if added_sub:
            with global_bus._lock:
                global_bus._subscribers["order.placed"] = [
                    s
                    for s in global_bus._subscribers["order.placed"]
                    if s.name != "log:order_placed"
                ]
        auth_db.auth_cache.clear()
        auth_db.verified_api_key_cache.clear()
        auth_db.invalid_api_key_cache.clear()
        auth_db.feed_token_cache.clear()
        settings_db.clear_settings_cache()
        token_db_enhanced.clear_cache()
        for _mod, sess, base, orig_q in rebound:
            if base is not None:
                base.query = orig_q
            sess.remove()
        openalgo_engine.dispose()
        latency_engine.dispose()
        sandbox_engine.dispose()


# ============================================================================
# F3 — errors.jsonl Startup OOM Trap & Bounded Log Rotation
# ============================================================================


def test_f3_truncate_errors_file_large_synthetic_tail_and_invalid_utf8(tmp_path):
    errors_file = tmp_path / "errors.jsonl"

    # Build >2.5 MB of valid JSON lines (~6,000 lines, ~460 bytes each)
    total_lines = 6000
    payload_pad = "x" * 380
    raw_chunks: list[bytes] = []
    for idx in range(total_lines):
        line = json.dumps({"seq": idx, "level": "ERROR", "msg": f"err-{idx}", "pad": payload_pad})
        raw_chunks.append((line + "\n").encode("utf-8"))

    data = bytearray(b"".join(raw_chunks))
    assert len(data) > 2_500_000

    # Inject invalid UTF-8 bytes inside the tail window boundary (mid-line seek offset)
    max_tail_bytes = 256 * 1024
    seek_pos = len(data) - max_tail_bytes
    # Ensure seek_pos lands right in the middle of a JSON line and inject invalid UTF-8 right there
    data[seek_pos : seek_pos + 3] = b"\xff\xfe\x80"
    errors_file.write_bytes(bytes(data))

    changed = oa_logging._truncate_errors_file(
        errors_file, max_lines=100, max_tail_bytes=max_tail_bytes
    )
    assert changed is True

    retained = errors_file.read_text(encoding="utf-8").splitlines()
    assert len(retained) == 100

    # Every retained line must be valid JSON (partial first line dropped) and match exact last 100
    parsed = [json.loads(line) for line in retained]
    assert [row["seq"] for row in parsed] == list(range(total_lines - 100, total_lines))
    assert errors_file.stat().st_size < max_tail_bytes


def test_f3_truncate_errors_file_small_and_missing_untouched(tmp_path):
    missing = tmp_path / "does_not_exist.jsonl"
    assert oa_logging._truncate_errors_file(missing, max_lines=100, max_tail_bytes=65536) is False

    empty = tmp_path / "empty.jsonl"
    empty.write_text("", encoding="utf-8")
    assert oa_logging._truncate_errors_file(empty, max_lines=100, max_tail_bytes=65536) is False

    small = tmp_path / "small.jsonl"
    original = "\n".join(json.dumps({"seq": i}) for i in range(10)) + "\n"
    small.write_text(original, encoding="utf-8")
    assert oa_logging._truncate_errors_file(small, max_lines=100, max_tail_bytes=65536) is False
    assert small.read_text(encoding="utf-8") == original


def test_f3_setup_logging_rotates_errors_jsonl_with_env_cap(tmp_path, monkeypatch):
    root_logger = logging.getLogger()
    saved_handlers = list(root_logger.handlers)
    saved_level = root_logger.level

    monkeypatch.setenv("LOG_DIR", str(tmp_path))
    monkeypatch.setenv("LOG_TO_FILE", "False")
    monkeypatch.setenv("ERROR_LOG_MAX_BYTES", "2048")
    monkeypatch.setenv("ERROR_LOG_BACKUP_COUNT", "2")
    monkeypatch.setenv("ERROR_LOG_STARTUP_MAX_LINES", "100")
    monkeypatch.setenv("ERROR_LOG_STARTUP_TAIL_BYTES", "65536")

    try:
        oa_logging.setup_logging()

        expected_path = (tmp_path / "errors.jsonl").resolve()
        rotating_handlers = [
            h
            for h in root_logger.handlers
            if isinstance(h, RotatingFileHandler)
            and Path(h.baseFilename).resolve() == expected_path
        ]
        assert len(rotating_handlers) == 1
        handler = rotating_handlers[0]
        assert handler.maxBytes == 2048
        assert handler.backupCount == 2

        test_logger = logging.getLogger("test.f3.rotation")
        for i in range(60):
            test_logger.error("synthetic rotation payload %03d %s", i, "Z" * 160)
        handler.flush()

        assert (tmp_path / "errors.jsonl").is_file()
        assert (tmp_path / "errors.jsonl.1").is_file()
        assert (tmp_path / "errors.jsonl.2").is_file()
        assert not (tmp_path / "errors.jsonl.3").exists()

        # Each file stays bounded near maxBytes (one record overshoot max before rollover)
        for name in ("errors.jsonl", "errors.jsonl.1", "errors.jsonl.2"):
            assert (tmp_path / name).stat().st_size <= 2048 + 1024
    finally:
        for h in list(root_logger.handlers):
            if h not in saved_handlers:
                root_logger.removeHandler(h)
                try:
                    h.close()
                except Exception:
                    pass
        root_logger.handlers = saved_handlers
        root_logger.setLevel(saved_level)


# ============================================================================
# F4 — health.db Unbounded Growth, Compaction, Retention & Incremental Vacuum
# ============================================================================


@pytest.fixture
def isolated_health_db(tmp_path, monkeypatch):
    """Provide an isolated SQLite health.db bound cleanly to database.health_db."""
    db_path = tmp_path / "health.db"
    db_url = f"sqlite:///{db_path.as_posix()}"
    engine = create_db_engine(db_url)
    sess, base, orig_q = _rebind_module_db(
        monkeypatch, health_db, engine, session_attr="health_session", base_attr="HealthBase"
    )
    monkeypatch.setattr(health_db, "HEALTH_DATABASE_URL", db_url)
    with health_db._prune_lock:
        saved_prune_map = dict(health_db._last_prune_by_key)
        health_db._last_prune_by_key.clear()

    try:
        yield db_path, engine, sess
    finally:
        with health_db._prune_lock:
            health_db._last_prune_by_key.clear()
            health_db._last_prune_by_key.update(saved_prune_map)
        base.query = orig_q
        sess.remove()
        engine.dispose()


def _sample_bloated_threads(count: int = 64) -> list[dict]:
    items = []
    for i in range(10):
        items.append(
            {
                "id": 1000 + i,
                "name": f"AppWorker-{i}-" + ("A" * 150),
                "daemon": False,
                "alive": True,
                "extra_junk": "discard-me",
            }
        )
    for i in range(count - 10):
        prefix = "ThreadPoolExecutor" if i % 2 == 0 else "Dummy-"
        items.append(
            {
                "id": 2000 + i,
                "name": f"{prefix}-worker-{i}-" + ("P" * 120),
                "daemon": True,
                "alive": True,
                "extra_junk": "discard-me",
            }
        )
    return items


def _sample_bloated_processes(count: int = 12) -> list[dict]:
    return [
        {
            "pid": 5000 + i,
            "name": f"proc-{i}-" + ("N" * 100),
            "rss_mb": 123.456789 + i,
            "vms_mb": 987.654321 + i,
            "memory_percent": 12.34567 + i,
            "cmdline": ["should", "not", "be", "persisted"],
        }
        for i in range(count)
    ]


def test_f4_compaction_caps_prioritizes_and_preserves_ui_schema(isolated_health_db):
    _db_path, _engine, _sess = isolated_health_db
    health_db.init_health_db()

    threads = _sample_bloated_threads(64)
    processes = _sample_bloated_processes(12)

    # Direct helper checks (including None and max_items=0)
    assert health_db.compact_thread_details(None) is None
    assert health_db.compact_thread_details(threads, max_items=0) == []
    assert health_db.compact_process_details(None) is None
    assert health_db.compact_process_details(processes, max_items=0) == []

    # Persist via log_metrics and inspect stored JSON
    ok = health_db.HealthMetric.log_metrics(
        fd_metrics={"count": 40, "limit": 1024, "usage_percent": 3.9, "available": 984, "status": "pass"},
        memory_metrics={"rss_mb": 120.5, "vms_mb": 400.0, "percent": 15.0, "available_mb": 500.0, "swap_mb": 0.0, "status": "pass"},
        db_metrics={"total": 3, "connections": {"openalgo": 1}, "status": "pass"},
        ws_metrics={"total": 1, "connections": {}, "total_symbols": 2, "status": "pass"},
        thread_metrics={"count": 64, "stuck_count": 0, "threads": threads, "status": "pass"},
        process_metrics=processes,
        auto_prune=False,
    )
    assert ok is True

    current = health_db.HealthMetric.get_current_metrics()
    assert current is not None
    assert current.thread_count == 64  # scalar count remains exact
    assert len(current.thread_details) == 15
    # All 10 non-pool AppWorker threads must appear BEFORE any ThreadPoolExecutor/Dummy- threads
    first_10_names = [t["name"] for t in current.thread_details[:10]]
    assert all(name.startswith("AppWorker-") for name in first_10_names)
    for item in current.thread_details:
        assert set(item.keys()) == {"id", "name", "daemon", "alive"}
        assert len(item["name"]) <= 64
        assert isinstance(item["daemon"], bool)
        assert isinstance(item["alive"], bool)

    assert len(current.process_details) == 5
    for idx, proc in enumerate(current.process_details):
        assert set(proc.keys()) == {"pid", "name", "rss_mb", "vms_mb", "memory_percent"}
        assert len(proc["name"]) <= 64
        assert proc["rss_mb"] == round(123.456789 + idx, 2)
        assert proc["vms_mb"] == round(987.654321 + idx, 2)
        assert proc["memory_percent"] == round(12.34567 + idx, 2)


def test_f4_retention_days_defaults_and_env_overrides(monkeypatch):
    monkeypatch.delenv("HEALTH_METRICS_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("HEALTH_RETENTION_DAYS", raising=False)
    assert health_db.get_health_retention_days() == 2

    monkeypatch.setenv("HEALTH_RETENTION_DAYS", "3")
    assert health_db.get_health_retention_days() == 3

    monkeypatch.setenv("HEALTH_METRICS_RETENTION_DAYS", "5")
    assert health_db.get_health_retention_days() == 5

    monkeypatch.setenv("HEALTH_METRICS_RETENTION_DAYS", "0")
    assert health_db.get_health_retention_days() == 1

    monkeypatch.setenv("HEALTH_METRICS_RETENTION_DAYS", "not-a-number")
    assert health_db.get_health_retention_days() == 2


def test_f4_periodic_pruning_on_write_cadence_and_detail_compaction(
    isolated_health_db, monkeypatch
):
    _db_path, _engine, sess = isolated_health_db
    health_db.init_health_db()

    now = datetime.now(timezone.utc)
    # 3 expired rows (>2 days old)
    for h in (72, 60, 50):
        sess.add(
            health_db.HealthMetric(
                timestamp=now - timedelta(hours=h),
                fd_count=10,
                thread_details=[{"id": 1, "name": "old", "daemon": False, "alive": True}],
                process_details=[{"pid": 1, "name": "old", "rss_mb": 1.0, "vms_mb": 2.0, "memory_percent": 0.1}],
                overall_status="pass",
            )
        )
    # 2 retained rows older than detail retention (10 min)
    for mins in (120, 45):
        sess.add(
            health_db.HealthMetric(
                timestamp=now - timedelta(minutes=mins),
                fd_count=20,
                thread_details=[{"id": 2, "name": "mid", "daemon": False, "alive": True}],
                process_details=[{"pid": 2, "name": "mid", "rss_mb": 1.0, "vms_mb": 2.0, "memory_percent": 0.1}],
                overall_status="pass",
            )
        )
    # 1 alert (must NEVER be purged)
    sess.add(
        health_db.HealthAlert(
            timestamp=now - timedelta(days=30),
            alert_type="memory_warn",
            severity="warn",
            metric_name="memory_rss_mb",
            metric_value=500.0,
            threshold_value=400.0,
            message="keep forever",
        )
    )
    sess.commit()

    monkeypatch.setenv("HEALTH_PRUNE_INTERVAL_SECONDS", "1")
    monkeypatch.setenv("HEALTH_DETAIL_RETENTION_MINUTES", "10")
    monkeypatch.delenv("HEALTH_METRICS_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("HEALTH_RETENTION_DAYS", raising=False)

    # Backdate last prune timestamp so next log_metrics() triggers automatic pruning
    key = health_db._current_prune_key()
    with health_db._prune_lock:
        health_db._last_prune_by_key[key] = time.monotonic() - 10.0

    ok = health_db.HealthMetric.log_metrics(
        fd_metrics={"count": 30, "status": "pass"},
        thread_metrics={
            "count": 5,
            "stuck_count": 0,
            "threads": [{"id": 99, "name": "MainThread", "daemon": False, "alive": True}],
            "status": "pass",
        },
        process_metrics=[{"pid": 99, "name": "gunicorn", "rss_mb": 50.0, "vms_mb": 100.0, "memory_percent": 5.0}],
        auto_prune=True,
    )
    assert ok is True

    remaining = (
        sess.query(health_db.HealthMetric)
        .order_by(health_db.HealthMetric.timestamp.asc(), health_db.HealthMetric.id.asc())
        .all()
    )
    # 3 expired rows purged; 2 retained mid rows + 1 latest row remain
    assert len(remaining) == 3
    # Older retained rows had detail payloads compacted to None
    for mid_row in remaining[:-1]:
        assert mid_row.thread_details is None
        assert mid_row.process_details is None
    # Latest row preserves its full compacted details for /health/api/current
    latest = remaining[-1]
    assert latest.thread_details is not None and len(latest.thread_details) == 1
    assert latest.process_details is not None and len(latest.process_details) == 1
    # HealthAlert untouched
    assert sess.query(health_db.HealthAlert).count() == 1


def test_f4_timestamp_index_exists_on_new_and_preexisting_db(tmp_path, monkeypatch):
    # Pre-create health_metrics table WITHOUT timestamp index to simulate legacy DB
    db_path = tmp_path / "legacy_idx_health.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE health_metrics (id INTEGER PRIMARY KEY, timestamp DATETIME)"
        )
        conn.commit()

    engine = create_db_engine(f"sqlite:///{db_path.as_posix()}")
    sess, base, orig_q = _rebind_module_db(
        monkeypatch, health_db, engine, session_attr="health_session", base_attr="HealthBase"
    )
    monkeypatch.setattr(health_db, "HEALTH_DATABASE_URL", f"sqlite:///{db_path.as_posix()}")
    try:
        health_db.init_health_db()
        with engine.connect() as conn:
            indexes = [row[1] for row in conn.execute(text("PRAGMA index_list('health_metrics')")).fetchall()]
        assert "ix_health_metrics_timestamp" in indexes
    finally:
        base.query = orig_q
        sess.remove()
        engine.dispose()


def _insert_bloated_rows(sess, count_old: int, count_recent: int) -> None:
    now = datetime.now(timezone.utc)
    big_threads = [
        {"id": j, "name": f"worker-{j}-" + ("T" * 40), "daemon": True, "alive": True}
        for j in range(15)
    ]
    big_procs = [
        {"pid": j, "name": f"proc-{j}-" + ("P" * 40), "rss_mb": 100.0, "vms_mb": 200.0, "memory_percent": 10.0}
        for j in range(5)
    ]
    rows = []
    for i in range(count_old):
        rows.append(
            health_db.HealthMetric(
                timestamp=now - timedelta(days=3, minutes=i),
                fd_count=50,
                memory_rss_mb=100.0,
                thread_count=64,
                thread_details=big_threads,
                process_details=big_procs,
                overall_status="pass",
            )
        )
    for i in range(count_recent):
        rows.append(
            health_db.HealthMetric(
                timestamp=now - timedelta(minutes=i),
                fd_count=50,
                memory_rss_mb=100.0,
                thread_count=64,
                thread_details=big_threads,
                process_details=big_procs,
                overall_status="pass",
            )
        )
    sess.bulk_save_objects(rows)
    sess.commit()


def test_f4_incremental_vacuum_shrinks_new_db_by_over_50_percent(isolated_health_db):
    db_path, engine, sess = isolated_health_db
    health_db.init_health_db()

    # Brand-new DB must immediately have PRAGMA auto_vacuum == 2 (INCREMENTAL)
    with engine.connect() as conn:
        assert conn.execute(text("PRAGMA auto_vacuum")).scalar() == 2

    _insert_bloated_rows(sess, count_old=300, count_recent=5)
    sess.remove()
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)")).fetchall()

    size_before = os.path.getsize(db_path)
    assert size_before > 100_000

    deleted = health_db.purge_old_metrics(days=2)
    assert deleted == 300

    with engine.connect() as conn:
        assert conn.execute(text("PRAGMA auto_vacuum")).scalar() == 2
        assert conn.execute(text("PRAGMA freelist_count")).scalar() == 0

    size_after = os.path.getsize(db_path)
    assert size_after < size_before * 0.5, (
        f"Expected >50% shrink on new DB: before={size_before}, after={size_after}"
    )


def test_f4_incremental_vacuum_migrates_and_shrinks_legacy_auto_vacuum_none_db(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "legacy_health.db"
    db_url = f"sqlite:///{db_path.as_posix()}"

    # Create a legacy SQLite DB explicitly with PRAGMA auto_vacuum = NONE (0)
    with sqlite3.connect(db_path) as raw:
        raw.execute("PRAGMA auto_vacuum = NONE")
        raw.execute("VACUUM")
        assert raw.execute("PRAGMA auto_vacuum").fetchone()[0] == 0

    engine = create_db_engine(db_url)
    sess, base, orig_q = _rebind_module_db(
        monkeypatch, health_db, engine, session_attr="health_session", base_attr="HealthBase"
    )
    monkeypatch.setattr(health_db, "HEALTH_DATABASE_URL", db_url)
    with health_db._prune_lock:
        saved_prune_map = dict(health_db._last_prune_by_key)
        health_db._last_prune_by_key.clear()

    try:
        # Create tables directly without enabling auto_vacuum=2, then seed 300 old + 5 recent rows
        health_db.HealthBase.metadata.create_all(bind=engine)
        _insert_bloated_rows(sess, count_old=300, count_recent=5)
        sess.remove()
        with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text("PRAGMA wal_checkpoint(TRUNCATE)")).fetchall()
            assert conn.execute(text("PRAGMA auto_vacuum")).scalar() == 0

        size_before = os.path.getsize(db_path)
        assert size_before > 100_000

        # Non-empty legacy DB: init_health_db keeps VACUUM deferred until purge_old_metrics
        health_db.init_health_db()
        deleted = health_db.purge_old_metrics(days=2)
        assert deleted == 300

        with engine.connect() as conn:
            assert conn.execute(text("PRAGMA auto_vacuum")).scalar() == 2
            assert conn.execute(text("PRAGMA freelist_count")).scalar() == 0
            indexes = [row[1] for row in conn.execute(text("PRAGMA index_list('health_metrics')")).fetchall()]
            assert "ix_health_metrics_timestamp" in indexes

        size_after = os.path.getsize(db_path)
        assert size_after < size_before * 0.5, (
            f"Expected >50% shrink on legacy DB: before={size_before}, after={size_after}"
        )
    finally:
        with health_db._prune_lock:
            health_db._last_prune_by_key.clear()
            health_db._last_prune_by_key.update(saved_prune_map)
        base.query = orig_q
        sess.remove()
        engine.dispose()


def test_f4_compaction_persists_sql_null_and_is_idempotent(
    isolated_health_db, monkeypatch, caplog
):
    """Regression test for D1: JSON(none_as_null=True) + SQL null() compaction.

    Verifies:
    1. ORM inserts with thread_details=None / process_details=None store real SQL NULL
       (SQLite typeof == 'null'), NOT 4-byte JSON literal 'null' ('text').
    2. purge_old_metrics() compacts aged detail rows AND any legacy JSON 'null' text
       rows into real SQL NULL while preserving only the latest row.
    3. An immediate second purge_old_metrics() call with no new rows compacts 0 rows
       and triggers 0 incremental vacuum/checkpoint calls.
    """
    db_path, engine, sess = isolated_health_db
    health_db.init_health_db()

    monkeypatch.setenv("HEALTH_DETAIL_RETENTION_MINUTES", "10")
    now = datetime.now(timezone.utc)

    # 1. Verify ORM inserts with None persist real SQL NULL (not JSON 'null' text)
    none_row = health_db.HealthMetric(
        timestamp=now - timedelta(hours=3),
        fd_count=10,
        thread_details=None,
        process_details=None,
        overall_status="pass",
    )
    sess.add(none_row)
    sess.commit()
    none_row_id = none_row.id

    assert health_db.HealthMetric.log_metrics(
        fd_metrics={"count": 11, "status": "pass"},
        thread_metrics=None,
        process_metrics=None,
        auto_prune=False,
    ) is True
    # Age the log_metrics(None, None) row so it also sits older than detail_cutoff
    logged_none_row = (
        sess.query(health_db.HealthMetric)
        .filter(health_db.HealthMetric.id != none_row_id)
        .one()
    )
    logged_none_row.timestamp = now - timedelta(hours=2, minutes=30)
    sess.commit()
    logged_none_id = logged_none_row.id

    with sqlite3.connect(db_path) as raw:
        for row_id in (none_row_id, logged_none_id):
            types = raw.execute(
                "SELECT typeof(thread_details), typeof(process_details) "
                "FROM health_metrics WHERE id = ?",
                (row_id,),
            ).fetchone()
            assert types == ("null", "null"), f"Expected SQL NULL for ORM None row {row_id}, got {types}"

    # 2. Seed 3 aged rows with bloated JSON payloads, 1 legacy row storing JSON 'null' text,
    #    and 1 latest row that must preserve its details.
    aged_ids = []
    for mins in (120, 90, 60):
        row = health_db.HealthMetric(
            timestamp=now - timedelta(minutes=mins),
            fd_count=20,
            thread_details=[{"id": mins, "name": "aged-worker", "daemon": True, "alive": True}],
            process_details=[{"pid": mins, "name": "aged-proc", "rss_mb": 10.0, "vms_mb": 20.0, "memory_percent": 1.0}],
            overall_status="pass",
        )
        sess.add(row)
        sess.flush()
        aged_ids.append(row.id)

    latest_row = health_db.HealthMetric(
        timestamp=now - timedelta(minutes=1),
        fd_count=30,
        thread_details=[{"id": 999, "name": "MainThread", "daemon": False, "alive": True}],
        process_details=[{"pid": 999, "name": "gunicorn", "rss_mb": 50.0, "vms_mb": 100.0, "memory_percent": 5.0}],
        overall_status="pass",
    )
    sess.add(latest_row)
    sess.commit()
    latest_id = latest_row.id

    # Simulate a legacy row written before none_as_null=True (JSON literal 'null' stored as TEXT)
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO health_metrics (timestamp, fd_count, thread_details, process_details, overall_status) "
                "VALUES (:ts, 15, 'null', 'null', 'pass')"
            ),
            {"ts": now - timedelta(hours=2)},
        )
        legacy_id = conn.execute(
            text("SELECT id FROM health_metrics WHERE typeof(thread_details) = 'text' AND thread_details = 'null'")
        ).scalar()
    assert legacy_id is not None

    # Wrap real run_incremental_vacuum to observe whether subsequent idempotent runs avoid vacuum churn
    vacuum_calls: list[int] = []
    real_inc_vac = health_db.run_incremental_vacuum

    def _counting_inc_vac(*args, **kwargs):
        res = real_inc_vac(*args, **kwargs)
        vacuum_calls.append(res)
        return res

    monkeypatch.setattr(health_db, "run_incremental_vacuum", _counting_inc_vac)

    # First purge: compacts the 3 aged payload rows + 1 legacy 'null'-text row = 4 rows
    # (does NOT touch the 2 already-SQL-NULL rows or the preserved latest row)
    with caplog.at_level(logging.DEBUG, logger="database.health_db"):
        caplog.clear()
        deleted_1 = health_db.purge_old_metrics(days=2)
        run1_logs = [rec.getMessage() for rec in caplog.records]

    assert deleted_1 == 0
    assert any("compacted 4 detail payloads" in msg for msg in run1_logs), run1_logs
    assert len(vacuum_calls) == 1

    # Direct SQLite verification: every compacted/legacy/none row has SQL NULL (typeof == 'null'),
    # and ONLY the 1 preserved latest row has IS NOT NULL.
    with sqlite3.connect(db_path) as raw:
        non_null_ids = [
            r[0]
            for r in raw.execute(
                "SELECT id FROM health_metrics "
                "WHERE thread_details IS NOT NULL OR process_details IS NOT NULL"
            ).fetchall()
        ]
        assert non_null_ids == [latest_id]

        for row_id in [*aged_ids, legacy_id, none_row_id, logged_none_id]:
            types = raw.execute(
                "SELECT typeof(thread_details), typeof(process_details) "
                "FROM health_metrics WHERE id = ?",
                (row_id,),
            ).fetchone()
            assert types == ("null", "null"), f"Row {row_id} expected SQL NULL, got {types}"

        latest_types = raw.execute(
            "SELECT typeof(thread_details), typeof(process_details) "
            "FROM health_metrics WHERE id = ?",
            (latest_id,),
        ).fetchone()
        assert latest_types == ("text", "text")

    # Second consecutive purge with zero new rows: must be a strict no-op (compacted 0, no vacuum)
    with caplog.at_level(logging.DEBUG, logger="database.health_db"):
        caplog.clear()
        deleted_2 = health_db.purge_old_metrics(days=2)
        run2_logs = [rec.getMessage() for rec in caplog.records]

    assert deleted_2 == 0
    assert any("Purged 0 old health metrics" in msg and "compacted 0 detail payloads" in msg for msg in run2_logs), run2_logs
    assert len(vacuum_calls) == 1, f"Expected zero additional vacuum calls on idempotent run, got {len(vacuum_calls)}"

    with sqlite3.connect(db_path) as raw:
        non_null_count = raw.execute(
            "SELECT COUNT(*) FROM health_metrics "
            "WHERE thread_details IS NOT NULL OR process_details IS NOT NULL"
        ).fetchone()[0]
        assert non_null_count == 1

