"""Database-per-broker slice: separate files, and the broker is not in the cookie."""

import sqlite3
from pathlib import Path

import pytest
from flask import Flask, session

import database  # noqa: F401  installs the SQLite path guard

from utils.broker_context import (
    BROKERS,
    BrokerContextError,
    broker_context,
    broker_from_http_request,
    retarget_sqlite_database,
)
from utils.tab_session import TabSessionInterface

PROJECT = Path(__file__).resolve().parents[1]


def test_sqlite_without_a_broker_is_refused():
    target = PROJECT / "db" / "openalgo.db"
    with pytest.raises(BrokerContextError):
        sqlite3.connect(target)


def test_each_broker_gets_its_own_database(tmp_path, monkeypatch):
    monkeypatch.chdir(PROJECT)
    monkeypatch.setattr(
        "utils.broker_context.PROJECT_ROOT",
        tmp_path,
    )
    # retarget compares against PROJECT_ROOT / "db" / the filename.
    # Point the connection at that layout under tmp_path.
    for name in BROKERS:
        main = tmp_path / "db" / "openalgo.db"
        main.parent.mkdir(parents=True, exist_ok=True)
        with broker_context(name):
            opened = Path(retarget_sqlite_database(str(main)))
            connection = sqlite3.connect(opened)
            connection.execute("create table if not exists marker (broker text)")
            connection.execute("insert into marker values (?)", (name,))
            connection.commit()
            connection.close()
    fyers = tmp_path / "db" / "brokers" / "fyers" / "openalgo.db"
    groww = tmp_path / "db" / "brokers" / "groww" / "openalgo.db"
    assert fyers.is_file() and groww.is_file() and fyers != groww
    with sqlite3.connect(fyers) as connection:
        assert connection.execute("select broker from marker").fetchall() == [("fyers",)]
    with sqlite3.connect(groww) as connection:
        assert connection.execute("select broker from marker").fetchall() == [("groww",)]


def test_the_request_broker_comes_from_the_tab_header_not_a_shared_cookie():
    assert (
        broker_from_http_request("/dashboard", "groww", "", "")
        == "groww"
    )
    assert broker_from_http_request("/fyers/callback", "", "", "") == "fyers"
    assert broker_from_http_request("/dashboard", "", "deltaexchange", "") == "deltaexchange"
    assert broker_from_http_request("/dashboard", "", "", "") is None


def test_broker_is_not_written_into_the_session_cookie():
    app = Flask(__name__)
    app.secret_key = "test-secret"
    app.session_interface = TabSessionInterface()
    with app.test_request_context("/dashboard"):
        session["user"] = "am"
        session["broker"] = "fyers"
        assert session.get("broker") == "fyers"
        assert "broker" not in list(session.keys())
        assert "user" in list(session.keys())
