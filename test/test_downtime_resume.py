"""A missed exit while the process was down is a warning, not an order.

No broker is called and no account is created. Candle coverage is supplied
by the test.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
import pytz

import services.downtime_resume as resume
from utils.broker_context import broker_context

IST = pytz.timezone("Asia/Kolkata")


def at(hour, minute, day=6):
    return IST.localize(datetime(2026, 11, day, hour, minute))


@pytest.fixture
def store(tmp_path, monkeypatch):
    def state_dir():
        from utils.broker_context import current_broker

        path = tmp_path / (current_broker() or "none")
        path.mkdir(parents=True, exist_ok=True)
        return path

    monkeypatch.setattr(resume, "state_dir", state_dir)
    resume.reset_for_tests()
    yield
    resume.reset_for_tests()


def _open_friday_gap():
    resume.write_heartbeat(at(10, 0))
    return resume.open_gap(at(16, 0))


def _bars(start, end, low=99.0):
    rows = []
    stamp = int(start.timestamp())
    stop = int(end.timestamp())
    while stamp <= stop:
        rows.append({"timestamp": stamp, "high": 101.0, "low": low, "close": 100.0})
        stamp += 60
    return rows


def _leg():
    return {
        "leg_id": "1",
        "position": "B",
        "status": "open",
        "entry_avg": 100.0,
        "qty": 1,
        "sl_pts": 10,
        "target_pts": 30,
    }


def test_a_clock_stop_inside_the_gap_is_a_warning_and_a_later_day_is_not(store):
    _open_friday_gap()

    missed = resume.clock_stop_blocked("sandbox:NSE", "15:15", source="sandbox", now=at(16, 0))
    later = resume.clock_stop_blocked(
        "sandbox:NSE", "15:15", source="sandbox", now=at(16, 0, day=9)
    )

    assert missed is True
    assert later is False
    warnings = resume.list_warnings()
    assert len(warnings) == 1
    assert warnings[0]["kind"] == "clock_stop"
    assert "did not guess an exit" in warnings[0]["detail"]
    assert "still at the broker" in warnings[0]["detail"]


def test_confirm_sends_the_held_close_and_clearing_does_not(store):
    _open_friday_gap()
    sent = []
    resume.set_close_sender(lambda payload: sent.append(payload) or {"ok": True})

    resume.arm_first_tick("strategy-run:1")
    assert resume.block_automatic_exit(
        "strategy-run:1",
        True,
        rule="sl",
        price="89",
        symbol="RELIANCE",
        close={"action": "strategy_stop", "run_id": 1},
    )
    confirmed_id = resume.list_warnings()[0]["id"]

    resume.arm_first_tick("strategy-run:2")
    assert resume.block_automatic_exit(
        "strategy-run:2",
        True,
        rule="sl",
        price="80",
        symbol="TCS",
        close={"action": "strategy_stop", "run_id": 2},
    )
    cleared_id = next(row["id"] for row in resume.list_warnings() if row["subject"] == "strategy-run:2")

    assert resume.confirm_warning(confirmed_id)["sent"] is True
    assert sent == [{"action": "strategy_stop", "run_id": 1}]
    assert resume.warnings_for_tab()[0]["id"] == cleared_id

    assert resume.clear_warning(cleared_id) is True
    assert sent == [{"action": "strategy_stop", "run_id": 1}]
    assert resume.warnings_for_tab() == []

    # The cleared breach is still held. The next tick does not send it.
    assert resume.block_automatic_exit(
        "strategy-run:2", True, rule="sl", price="80", symbol="TCS"
    ) is True
    assert sent == [{"action": "strategy_stop", "run_id": 1}]

    # The price comes back. A later new breach may arm a live stop.
    assert resume.block_automatic_exit("strategy-run:2", False, rule="sl", symbol="TCS") is False
    assert resume.block_automatic_exit(
        "strategy-run:2", True, rule="sl", price="70", symbol="TCS"
    ) is False


def test_a_quiet_first_tick_allows_a_later_breach_to_exit(store):
    _open_friday_gap()
    resume.arm_first_tick("strategy-run:8")

    assert resume.block_automatic_exit("strategy-run:8", False, rule="sl") is False
    assert resume.block_automatic_exit("strategy-run:8", True, rule="sl", price="89") is False
    assert resume.list_warnings() == []


def test_historify_covers_a_breached_bar_without_an_order(store):
    start, end = _open_friday_gap()
    original = _leg()
    bars = _bars(start, end)
    bars[30]["low"] = 80.0
    sent = []

    covered = resume.judge_symbol_gap(
        subject="strategy-run:1",
        symbol="RELIANCE",
        exchange="NSE",
        leg=original,
        historify_bars=bars,
        session_alive=False,
    )

    assert covered == "historify"
    assert original["entry_avg"] == 100.0
    assert sent == []
    warning = resume.list_warnings()[0]
    assert warning["kind"] == "price_breach"
    assert warning["rule"] == "sl"
    assert warning["price"] == "80.0"
    assert "was not sent" in warning["detail"]


def test_broker_history_is_used_when_the_session_is_alive_and_historify_does_not_cover(store):
    start, end = _open_friday_gap()
    bars = _bars(start, end, low=99.0)

    covered = resume.judge_symbol_gap(
        subject="strategy-run:2",
        symbol="RELIANCE",
        exchange="NSE",
        leg=_leg(),
        historify_bars=[],
        broker_bars=bars,
        session_alive=True,
    )

    assert covered == "broker"
    assert resume.list_warnings() == []


def test_the_gap_cannot_be_judged_when_neither_source_covers_it(store):
    _open_friday_gap()

    covered = resume.judge_symbol_gap(
        subject="strategy-run:3",
        symbol="RELIANCE",
        exchange="NSE",
        leg=_leg(),
        historify_bars=[],
        broker_bars=None,
        session_alive=False,
    )

    assert covered == "unjudged"
    warning = resume.list_warnings()[0]
    assert warning["kind"] == "gap_unjudged"
    assert "cannot be judged" in warning["detail"]
    assert "session is not alive" in warning["detail"]
    assert "nothing happened" not in warning["detail"]


def _gap_through_now():
    now = datetime.now(IST)
    resume.write_heartbeat(now - timedelta(hours=6))
    resume.open_gap(now)
    due = now - timedelta(hours=1)
    names = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
    return due.strftime("%H:%M"), [names[due.weekday()]]


def test_a_missed_python_stop_does_not_stop_the_child_or_guess_a_flatten(store, monkeypatch):
    import blueprints.python_strategy as ps

    hhmm, days = _gap_through_now()
    ps.STRATEGY_CONFIGS["s1"] = {
        "schedule_stop": hhmm,
        "schedule_days": days,
        "is_running": True,
    }
    stopped = []
    monkeypatch.setattr(ps, "stop_strategy_process", lambda *args, **kwargs: stopped.append(1))

    ps.scheduled_stop_strategy("s1")

    assert stopped == []
    assert "did not guess an exit" in resume.list_warnings()[0]["detail"]


def test_a_missed_openscript_stop_does_not_stop_the_run(store, monkeypatch):
    import blueprints.openscript_runner as runner

    hhmm, days = _gap_through_now()
    monkeypatch.setattr(
        runner,
        "_load_schedules",
        lambda: {"held.oscript": {"stop_time": hhmm, "days": days}},
    )
    stopped = []
    monkeypatch.setattr(runner, "stop_run", lambda *args, **kwargs: stopped.append(1) or (True, ""))

    runner._scheduled_stop("held.oscript")

    assert stopped == []
    assert resume.list_warnings()[0]["source"] == "openscript"


def test_each_broker_keeps_its_own_warning(store):
    with broker_context("fyers"):
        _open_friday_gap()
        resume.note_missed_clock("python:one", "15:15", source="python")
        assert len(resume.list_warnings()) == 1
    with broker_context("groww"):
        assert resume.list_warnings() == []


def test_the_open_tab_route_shows_only_that_brokers_warnings(store):
    from flask import Flask, g, request, session

    from blueprints.missed_exit import missed_exit_bp
    from utils.broker_context import pop_broker, push_broker

    app = Flask(__name__)
    app.secret_key = "test"
    app.config["TESTING"] = True
    app.register_blueprint(missed_exit_bp)

    @app.before_request
    def _enter():
        name = request.headers.get("X-OpenAlgo-Broker")
        if name:
            session["broker"] = name
            g._broker_token = push_broker(name)

    @app.teardown_request
    def _leave(_exc=None):
        pop_broker(getattr(g, "_broker_token", None))

    with broker_context("fyers"):
        _open_friday_gap()
        resume.note_missed_clock("python:held", "15:15", source="python")

    client = app.test_client()
    with client.session_transaction() as cookie:
        cookie["logged_in"] = True
        cookie["user"] = "am"

    fyers = client.get("/auth/missed-exits", headers={"X-OpenAlgo-Broker": "fyers"})
    groww = client.get("/auth/missed-exits", headers={"X-OpenAlgo-Broker": "groww"})

    assert fyers.status_code == 200
    shown = fyers.get_json()["warnings"]
    assert len(shown) == 1
    assert shown[0]["can_confirm"] is True
    assert "did not guess an exit" in shown[0]["detail"]
    assert groww.get_json()["warnings"] == []


def test_the_broker_tab_shell_carries_the_warning_banner():
    from blueprints.react_app import serve_react_app

    html = serve_react_app()
    if isinstance(html, tuple):
        pytest.skip("frontend dist is not built")
    page = html.get_data(as_text=True)
    assert "openalgo-missed-exits" in page
    assert "Confirm close" in page
    assert "/auth/missed-exits" in page


def test_the_broker_tab_lists_a_warning_that_can_send_its_close(store):
    _open_friday_gap()
    resume.arm_first_tick("strategy-run:4")
    resume.block_automatic_exit(
        "strategy-run:4",
        True,
        rule="sl",
        price="89",
        symbol="RELIANCE",
        close={"action": "strategy_stop", "run_id": 4},
    )

    shown = resume.warnings_for_tab()

    assert shown[0]["detail"]
    assert shown[0]["can_confirm"] is True
    assert "close_payload" not in shown[0]
