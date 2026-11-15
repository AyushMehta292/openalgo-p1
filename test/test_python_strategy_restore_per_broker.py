"""Each broker's Python strategies come back from that broker's file.

Import used to load one global STRATEGY_CONFIGS with no broker selected, so
db/brokers/<broker>/strategies/strategy_configs.json was never restored, and a
save could write one broker's strategies into another's file. A slower save
for one broker could also be skipped because another broker had already
published a higher shared generation.
"""

from __future__ import annotations

import json
import os
import threading

import pytest

from blueprints import python_strategy as ps
from utils import broker_context as bc


@pytest.fixture
def broker_dirs(tmp_path, monkeypatch):
    """Point strategy files at tmp_path and put each broker's memory back."""
    monkeypatch.setattr(bc, "PROJECT_ROOT", tmp_path)
    assert bc.current_broker() is None

    saved_unscoped = dict(ps.STRATEGY_CONFIGS)
    saved_brokers = {}
    for name in bc.BROKERS:
        with bc.broker_context(name):
            saved_brokers[name] = dict(ps.STRATEGY_CONFIGS)
            ps.STRATEGY_CONFIGS.clear()
    saved_generation = dict(ps._CONFIG_GENERATION)
    saved_persisted = dict(ps._PERSISTED_GENERATION)
    saved_init = set(ps._initialized_brokers)
    ps._initialized_brokers.clear()
    yield tmp_path

    assert bc.current_broker() is None
    ps.STRATEGY_CONFIGS.clear()
    ps.STRATEGY_CONFIGS.update(saved_unscoped)
    for name in bc.BROKERS:
        with bc.broker_context(name):
            ps.STRATEGY_CONFIGS.clear()
            ps.STRATEGY_CONFIGS.update(saved_brokers[name])
    ps._CONFIG_GENERATION.clear()
    ps._CONFIG_GENERATION.update(saved_generation)
    ps._PERSISTED_GENERATION.clear()
    ps._PERSISTED_GENERATION.update(saved_persisted)
    ps._initialized_brokers.clear()
    ps._initialized_brokers.update(saved_init)


def _config_file(root, broker):
    return root / "db" / "brokers" / broker / "strategies" / "strategy_configs.json"


def test_restart_loads_each_brokers_strategy_file(broker_dirs, monkeypatch):
    """Startup reads each broker's file into that broker's map, then restores."""
    fyers_file = _config_file(broker_dirs, "fyers")
    delta_file = _config_file(broker_dirs, "deltaexchange")
    fyers_file.parent.mkdir(parents=True)
    delta_file.parent.mkdir(parents=True)
    fyers_file.write_text(
        json.dumps({"fyers-one": {"name": "Fyers one", "exchange": "NSE", "is_running": False}}),
        encoding="utf-8",
    )
    delta_file.write_text(
        json.dumps({"delta-one": {"name": "Delta one", "exchange": "crypto", "is_running": False}}),
        encoding="utf-8",
    )

    seen = {}

    def spy_restore():
        seen[bc.current_broker()] = set(ps.STRATEGY_CONFIGS)

    monkeypatch.setattr(ps, "restore_strategy_states", spy_restore)
    monkeypatch.setattr(ps, "daily_trading_day_check", lambda: None)

    ps.initialize_with_app_context()

    assert seen["fyers"] == {"fyers-one"}
    assert seen["deltaexchange"] == {"delta-one"}
    assert seen["groww"] == set()
    assert not _config_file(broker_dirs, "groww").exists()

    with bc.broker_context("fyers"):
        assert set(ps.STRATEGY_CONFIGS) == {"fyers-one"}
        assert "delta-one" not in ps.STRATEGY_CONFIGS
    with bc.broker_context("deltaexchange"):
        assert ps.STRATEGY_CONFIGS["delta-one"]["exchange"] == "CRYPTO"
        assert "fyers-one" not in ps.STRATEGY_CONFIGS

    fyers_saved = json.loads(fyers_file.read_text(encoding="utf-8"))
    delta_saved = json.loads(delta_file.read_text(encoding="utf-8"))
    assert set(fyers_saved) == {"fyers-one"}
    assert set(delta_saved) == {"delta-one"}
    assert delta_saved["delta-one"]["exchange"] == "CRYPTO"


def test_saving_one_broker_does_not_copy_its_strategies_into_another(broker_dirs):
    with bc.broker_context("fyers"):
        ps.STRATEGY_CONFIGS["fyers-one"] = {"name": "Fyers one", "exchange": "NSE"}
        assert ps.save_configs() is True
    with bc.broker_context("deltaexchange"):
        ps.STRATEGY_CONFIGS["delta-one"] = {"name": "Delta one", "exchange": "CRYPTO"}
        assert ps.save_configs() is True
        assert set(ps.STRATEGY_CONFIGS) == {"delta-one"}

    fyers_saved = json.loads(_config_file(broker_dirs, "fyers").read_text(encoding="utf-8"))
    delta_saved = json.loads(_config_file(broker_dirs, "deltaexchange").read_text(encoding="utf-8"))
    assert set(fyers_saved) == {"fyers-one"}
    assert set(delta_saved) == {"delta-one"}
    with bc.broker_context("fyers"):
        assert set(ps.STRATEGY_CONFIGS) == {"fyers-one"}


def test_a_slower_save_is_not_dropped_because_another_broker_saved(broker_dirs, monkeypatch):
    """Fyers serialises first and reaches the file after deltaexchange wrote.

    A shared generation would treat the fyers snapshot as stale and skip it,
    so the fyers file would never receive fyers-one.
    """
    for broker in ("fyers", "deltaexchange"):
        ps._CONFIG_GENERATION[broker] = 0
        ps._PERSISTED_GENERATION[broker] = 0

    first_serialised = threading.Event()
    second_written = threading.Event()
    calls = []
    errors = []
    real_replace = os.replace

    class GatedLock:
        def __init__(self):
            self.inner = threading.Lock()

        def __enter__(self):
            if threading.current_thread().name == "fyers":
                first_serialised.set()
                if not second_written.wait(5):
                    raise TimeoutError("deltaexchange save did not write")
            self.inner.acquire()
            return self

        def __exit__(self, *exc):
            self.inner.release()

    def tracking_replace(src, dst):
        calls.append(threading.current_thread().name)
        real_replace(src, dst)
        if threading.current_thread().name == "delta":
            second_written.set()

    monkeypatch.setattr(ps, "_CONFIG_FILE_LOCK", GatedLock())
    monkeypatch.setattr(ps.os, "replace", tracking_replace)

    def fyers():
        try:
            with bc.broker_context("fyers"):
                ps.STRATEGY_CONFIGS["fyers-one"] = {"name": "Fyers one", "exchange": "NSE"}
                assert ps.save_configs() is True
        except Exception as exc:
            errors.append(exc)

    def delta():
        try:
            with bc.broker_context("deltaexchange"):
                assert first_serialised.wait(5)
                ps.STRATEGY_CONFIGS["delta-one"] = {"name": "Delta one", "exchange": "CRYPTO"}
                assert ps.save_configs() is True
        except Exception as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=fyers, name="fyers"),
        threading.Thread(target=delta, name="delta"),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(10)

    assert errors == []
    assert calls == ["delta", "fyers"]
    fyers_saved = json.loads(_config_file(broker_dirs, "fyers").read_text(encoding="utf-8"))
    delta_saved = json.loads(_config_file(broker_dirs, "deltaexchange").read_text(encoding="utf-8"))
    assert set(fyers_saved) == {"fyers-one"}
    assert set(delta_saved) == {"delta-one"}
