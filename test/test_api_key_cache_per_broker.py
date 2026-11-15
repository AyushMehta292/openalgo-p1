"""API-key auth caches must be scoped per broker.

The same OpenAlgo platform API key string lives in every broker database.
Caching by key hash alone lets the first broker to fill the cache lock the
others into 403 Invalid openalgo apikey when _read_coherently refuses the
mismatched session.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from flask import Flask

import database.auth_db as auth_db
from utils.broker_context import broker_context, current_broker


API_KEY = "shared-platform-key-not-a-secret"


@pytest.fixture()
def clear_api_key_caches():
    auth_db.auth_cache.clear()
    auth_db.verified_api_key_cache.clear()
    auth_db.invalid_api_key_cache.clear()
    auth_db.broker_cache.clear()
    yield
    auth_db.auth_cache.clear()
    auth_db.verified_api_key_cache.clear()
    auth_db.invalid_api_key_cache.clear()
    auth_db.broker_cache.clear()


def test_broker_scoped_api_key_cache_keys_differ_by_broker():
    digest = auth_db._api_key_digest(API_KEY)
    with broker_context("deltaexchange"):
        delta_key = auth_db._broker_scoped_api_key_cache_key(API_KEY, False)
    with broker_context("groww"):
        groww_key = auth_db._broker_scoped_api_key_cache_key(API_KEY, False)
    assert delta_key != groww_key
    assert digest in delta_key and digest in groww_key
    assert delta_key.startswith("deltaexchange_")
    assert groww_key.startswith("groww_")


def test_same_key_hash_for_broker_a_does_not_resolve_broker_b(
    clear_api_key_caches, monkeypatch
):
    """Cache filled under broker A must not make broker B return A's auth."""

    monkeypatch.setattr(auth_db, "verify_api_key", lambda _key: "am")
    monkeypatch.setattr(auth_db, "_pin_keys", lambda _broker: True)
    monkeypatch.setattr(auth_db, "_unpin_keys", lambda: None)
    monkeypatch.setattr(auth_db, "_browser_session_expects", lambda: (None, None))

    class QueryRoot:
        @staticmethod
        def filter_by(**_kwargs):
            broker = current_broker()
            row = SimpleNamespace(
                name="am",
                broker=broker,
                is_revoked=False,
                auth=auth_db.encrypt_token(f"{broker}-token"),
                feed_token=None,
            )
            return SimpleNamespace(first=lambda: row)

    monkeypatch.setattr(auth_db, "Auth", SimpleNamespace(query=QueryRoot))

    app = Flask(__name__)
    with app.test_request_context("/api/v1/funds", method="POST"):
        with broker_context("deltaexchange"):
            token_a, broker_a = auth_db.get_auth_token_broker(API_KEY)
        with broker_context("groww"):
            token_b, broker_b = auth_db.get_auth_token_broker(API_KEY)

    assert (token_a, broker_a) == ("deltaexchange-token", "deltaexchange")
    assert (token_b, broker_b) == ("groww-token", "groww")
    assert token_a != token_b

    with broker_context("deltaexchange"):
        assert auth_db.auth_cache.get(
            auth_db._broker_scoped_api_key_cache_key(API_KEY, False)
        ) == ("deltaexchange-token", "deltaexchange")
    with broker_context("groww"):
        assert auth_db.auth_cache.get(
            auth_db._broker_scoped_api_key_cache_key(API_KEY, False)
        ) == ("groww-token", "groww")
