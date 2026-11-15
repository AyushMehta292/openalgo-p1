"""Restore passes run one at a time and never mark a live strategy failed (hosts-04).

restore_strategy_states runs from the startup thread, from every login's master
contract hook and from requests, with no lock between them. Two passes both saw
a strategy as down: one restarted it, the other's start was refused with
"Strategy already running", and its failure branch then wrote is_running=False
and an error over the strategy the first had just started. The page showed an
error while the strategy traded, and Stop only recorded a manual stop.

initialize_with_app_context marks each broker done only after that broker's
work finishes, and under a lock, so a second caller does not go on against
half-restored state. A broker whose attempt raised is retried; one that
finished is not.

The first test runs two restore passes together against a strategy whose
recorded process is gone. On the old code one of them marks the running
strategy as failed.
"""

import threading
import time

import pytest

from blueprints import python_strategy as ps


class SlowProcess:
    """A spawned strategy that took a moment to launch."""

    next_pid = 2_000_000_000

    def __init__(self, *args, **kwargs):
        time.sleep(0.2)
        SlowProcess.next_pid += 1
        self.pid = SlowProcess.next_pid

    def poll(self):
        return None


@pytest.fixture
def host(monkeypatch, tmp_path):
    saved_configs = dict(ps.STRATEGY_CONFIGS)
    saved_running = dict(ps.RUNNING_STRATEGIES)
    ps.STRATEGY_CONFIGS.clear()
    ps.RUNNING_STRATEGIES.clear()

    script = tmp_path / "trend.py"
    script.write_text("print('hi')\n", encoding="utf-8")
    ps.STRATEGY_CONFIGS["trend"] = {
        "name": "Trend",
        "file_path": str(script),
        "exchange": "NSE",
        "is_running": True,
        # A process id that does not exist: the previous worker's child is gone.
        "pid": 2_147_000_000,
    }

    spawned = []

    def popen(*args, **kwargs):
        process = SlowProcess()
        spawned.append(process)
        return process

    monkeypatch.setattr(ps.subprocess, "Popen", popen)
    monkeypatch.setattr(ps, "check_master_contract_ready", lambda *a, **k: (True, "ready"))
    monkeypatch.setattr(ps, "save_configs", lambda: True)
    monkeypatch.setattr(ps, "broadcast_status_update", lambda *a, **k: None)
    monkeypatch.setattr(ps, "LOGS_DIR", tmp_path)
    yield spawned

    ps.STRATEGY_CONFIGS.clear()
    ps.STRATEGY_CONFIGS.update(saved_configs)
    ps.RUNNING_STRATEGIES.clear()
    ps.RUNNING_STRATEGIES.update(saved_running)


def test_two_restore_passes_start_once_and_leave_it_running(host):
    spawned = host
    barrier = threading.Barrier(2)

    def restore():
        barrier.wait()
        ps.restore_strategy_states()

    threads = [threading.Thread(target=restore) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    config = ps.STRATEGY_CONFIGS["trend"]
    assert len(spawned) == 1
    assert "trend" in ps.RUNNING_STRATEGIES
    assert config["is_running"] is True, config
    assert "is_error" not in config, config
    assert config["pid"] == spawned[0].pid


def test_login_restore_and_the_contracts_route_start_a_waiting_strategy_once(host):
    """Pending starts are single flight too: two callers, one spawn."""
    spawned = host
    config = ps.STRATEGY_CONFIGS["trend"]
    config.update(
        {
            "is_running": False,
            "pid": None,
            "is_error": True,
            "error_message": "Waiting for master contracts to be downloaded",
        }
    )
    barrier = threading.Barrier(2)
    results = []

    def pending():
        barrier.wait()
        results.append(ps.check_and_start_pending_strategies())

    threads = [threading.Thread(target=pending) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(10)

    assert len(spawned) == 1
    assert sorted(started for _, started, _ in results) == [0, 1]


def test_initialisation_runs_once_per_broker_and_is_marked_done_last(monkeypatch):
    """Eight callers restore each broker once, and only after that work."""
    from utils.broker_context import BROKERS, current_broker

    saved_init = set(ps._initialized_brokers)
    ps._initialized_brokers.clear()
    runs = []
    marked_before_work = []

    def slow_restore():
        marked_before_work.append(current_broker() in ps._initialized_brokers)
        runs.append(current_broker())
        time.sleep(0.2)

    # Keep the test off the machine's real strategy_configs.json.
    monkeypatch.setattr(ps, "load_configs", lambda: None)
    monkeypatch.setattr(ps, "restore_strategy_states", slow_restore)
    monkeypatch.setattr(ps, "daily_trading_day_check", lambda: None)
    monkeypatch.setattr(ps, "snapshot_strategy_configs", lambda: [])

    barrier = threading.Barrier(8)
    returned_before_done = []

    def init():
        barrier.wait()
        ps.initialize_with_app_context()
        if set(ps._initialized_brokers) != set(BROKERS):
            returned_before_done.append(1)

    try:
        threads = [threading.Thread(target=init) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)

        assert runs == list(BROKERS)
        assert marked_before_work == [False, False, False], (
            "a broker was marked done before its restore finished"
        )
        assert returned_before_done == [], "a caller went on before initialisation finished"
        assert set(ps._initialized_brokers) == set(BROKERS)
    finally:
        ps._initialized_brokers.clear()
        ps._initialized_brokers.update(saved_init)


def test_a_failed_initialisation_is_retried(monkeypatch):
    """A broker whose init raised is tried again. One that finished is not."""
    from utils.broker_context import BROKERS, current_broker

    saved_init = set(ps._initialized_brokers)
    ps._initialized_brokers.clear()
    attempts = []

    def failing_restore():
        broker = current_broker()
        attempts.append(broker)
        if broker == "fyers" and attempts.count("fyers") == 1:
            raise RuntimeError("no app context yet")

    monkeypatch.setattr(ps, "load_configs", lambda: None)
    monkeypatch.setattr(ps, "restore_strategy_states", failing_restore)
    monkeypatch.setattr(ps, "daily_trading_day_check", lambda: None)
    monkeypatch.setattr(ps, "snapshot_strategy_configs", lambda: [])

    try:
        ps.initialize_with_app_context()
        assert set(ps._initialized_brokers) == {"deltaexchange", "groww"}

        ps.initialize_with_app_context()
        assert set(ps._initialized_brokers) == set(BROKERS)
        assert attempts.count("fyers") == 2
        assert attempts.count("deltaexchange") == 1
        assert attempts.count("groww") == 1
    finally:
        ps._initialized_brokers.clear()
        ps._initialized_brokers.update(saved_init)
