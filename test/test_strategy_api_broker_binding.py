"""Python/OpenScript /api/v1 calls resolve a broker without X-OpenAlgo-Broker.

Without a broker context, verify_api_key cannot open the broker database and
every SDK call answers 403 Invalid openalgo apikey. bind_tab_broker must pick
the broker from the platform API key; strategy children must also carry
OPENALGO_BROKER so build_client can send the header.
"""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from flask import Flask, g

from database.auth_db import api_key_auth_error_message
from utils.broker_context import (
    broker_context,
    current_broker,
    pop_broker,
    select_broker_for_api_key,
)


def test_api_key_auth_error_message_distinguishes_revoked_session():
    assert api_key_auth_error_message(None, None) == "Invalid openalgo apikey"
    assert (
        api_key_auth_error_message(None, "fyers")
        == "Broker session expired. Log in to fyers again."
    )
    assert api_key_auth_error_message("tok", "fyers") == ""


def test_select_broker_for_api_key_enters_matching_broker():
    def fake_verify(key):
        return "am" if key == "platform-key" and current_broker() == "groww" else None

    app = Flask(__name__)
    with app.test_request_context("/api/v1/funds", method="POST"):
        with patch("database.auth_db.verify_api_key", side_effect=fake_verify):
            assert select_broker_for_api_key("platform-key") is True
            assert current_broker() == "groww"
            assert g._tab_state["broker"] == "groww"
            pop_broker(g._broker_token)


def test_select_broker_for_api_key_misses_unknown_key():
    app = Flask(__name__)
    with app.test_request_context("/api/v1/funds", method="POST"):
        with patch("database.auth_db.verify_api_key", return_value=None):
            assert select_broker_for_api_key("no-such-key") is False
            assert current_broker() is None


def test_select_broker_for_api_key_ignores_blank():
    assert select_broker_for_api_key("") is False
    assert select_broker_for_api_key("   ") is False


def test_build_client_sets_broker_header_from_env(monkeypatch):
    monkeypatch.setenv("OPENALGO_API_KEY", "test-key-not-secret")
    monkeypatch.setenv("OPENALGO_HOST", "http://127.0.0.1:5000")
    monkeypatch.setenv("OPENALGO_BROKER", "deltaexchange")

    class FakeApi:
        def __init__(self, api_key, host="http://127.0.0.1:5000"):
            self.api_key = api_key
            self.host = host
            self.headers = {"Content-Type": "application/json"}

    import openscript_host.openscript_runner as runner

    fake_openalgo = SimpleNamespace(api=FakeApi)
    with patch.dict("sys.modules", {"openalgo": fake_openalgo}):
        client = runner.build_client(MagicMock())
    assert client.headers.get("X-OpenAlgo-Broker") == "deltaexchange"


def test_build_client_omits_header_without_broker(monkeypatch):
    monkeypatch.setenv("OPENALGO_API_KEY", "test-key-not-secret")
    monkeypatch.setenv("OPENALGO_HOST", "http://127.0.0.1:5000")
    monkeypatch.delenv("OPENALGO_BROKER", raising=False)

    class FakeApi:
        def __init__(self, api_key, host="http://127.0.0.1:5000"):
            self.headers = {"Content-Type": "application/json"}

    import openscript_host.openscript_runner as runner

    with patch.dict("sys.modules", {"openalgo": SimpleNamespace(api=FakeApi)}):
        client = runner.build_client(MagicMock())
    assert "X-OpenAlgo-Broker" not in client.headers


def test_python_strategy_env_includes_openalgo_broker(monkeypatch, tmp_path):
    import blueprints.python_strategy as ps_module

    strat = tmp_path / "noop.py"
    strat.write_text("import sys; sys.exit(0)\n")

    # Ensure a previous run is not left registered.
    ps_module.RUNNING_STRATEGIES.pop("broker-env", None)
    ps_module.STOPPING_STRATEGIES.discard("broker-env")
    ps_module.DELETING_STRATEGIES.discard("broker-env")

    captured_env = {}

    class FakeProc:
        def __init__(self, *a, **kw):
            captured_env.update(kw.get("env") or {})
            self.pid = 4242

        def poll(self):
            return None

    monkeypatch.setattr(ps_module.subprocess, "Popen", FakeProc)
    monkeypatch.setattr(ps_module, "check_master_contract_ready", lambda: (True, "ok"))
    monkeypatch.setattr(ps_module, "broadcast_status_update", lambda *a, **kw: None)
    monkeypatch.setattr(ps_module, "save_configs", lambda: None)
    monkeypatch.setattr(ps_module, "LOGS_DIR", Path(tmp_path))

    # The config belongs to the broker that starts it. A shared map used to
    # make a config written with no broker visible inside fyers.
    with broker_context("fyers"):
        ps_module.STRATEGY_CONFIGS["broker-env"] = {
            "name": "BrokerEnv",
            "file_path": str(strat),
            "user_id": "bob",
            "exchange": "NSE",
        }
        try:
            success, msg = ps_module.start_strategy_process("broker-env")
        finally:
            ps_module.STRATEGY_CONFIGS.pop("broker-env", None)
            ps_module.RUNNING_STRATEGIES.pop("broker-env", None)
    assert success, msg
    assert captured_env.get("OPENALGO_BROKER") == "fyers"


def test_openscript_spawn_env_includes_openalgo_broker(monkeypatch, tmp_path):
    import services.openscript_runner_service as ors

    captured = {}

    class FakeHandle:
        def write(self, *_a, **_k):
            return None

        def flush(self):
            return None

        def close(self):
            return None

        def fileno(self):
            return 1

    class FakeProc:
        def __init__(self, *a, **kw):
            captured.update(kw.get("env") or {})
            self.pid = 99

        def poll(self):
            return None

    monkeypatch.setattr(ors, "runner_program_path", lambda: tmp_path / "runner.py")
    (tmp_path / "runner.py").write_text("# stub\n")
    monkeypatch.setattr(ors, "_api_key_for", lambda _uid: "k")
    monkeypatch.setattr(ors, "log_file_for", lambda *_a, **_k: tmp_path / "run.log")
    monkeypatch.setattr(ors, "_instrument_facts_text", lambda *_a, **_k: "{}")
    monkeypatch.setattr(ors, "_analyzer_mode", lambda: "analyze")
    monkeypatch.setattr(ors, "_inputs_as_text", lambda *_a, **_k: "{}")
    monkeypatch.setattr(ors, "mark_running", lambda *_a, **_k: None)
    monkeypatch.setattr(ors, "record_run_mode", lambda *_a, **_k: None)
    monkeypatch.setattr(ors.subprocess, "Popen", FakeProc)
    monkeypatch.setattr("builtins.open", lambda *_a, **_k: FakeHandle())

    with broker_context("groww"):
        ok, message = ors._spawn_claimed(
            script="demo.oas",
            run_id="run-broker-env",
            symbol="SBIN",
            exchange="NSE",
            interval="1m",
            product="MIS",
            user_id="am",
            history_days=1,
            poll_seconds=1.0,
            inputs={},
        )
    assert ok, message
    assert captured.get("OPENALGO_BROKER") == "groww"
    ors.RUNNING_RUNS.pop("run-broker-env", None)
