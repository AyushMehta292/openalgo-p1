"""Keep a stored broker token until that broker rejects it.

The 03:00 IST clock is not a reason to revoke Fyers, Delta Exchange, or Groww.
Startup and the first request of each trading day ask that broker's funds
endpoint whether the stored token is still accepted. Acceptance leaves the
context logged in. Rejection revokes only that broker's token, so that broker
needs a sign-in. Another broker's token is not touched.

A missing key, a rate limit, or a network failure is not a rejection. The
token stays and the check is tried again later.

Fyers returns a refresh_token from the auth-code exchange. The documented
validate-authcode call only accepts grant_type ``authorization_code``. This
module does not redeem a refresh token. Delta Exchange and Groww have no
refresh token in this tree.
"""

from __future__ import annotations

import importlib
import threading
import time

from utils.broker_context import BROKERS, BrokerContextError, current_broker, run_for_each_broker
from utils.logging import get_logger

logger = get_logger(__name__)

_lock = threading.Lock()
# (broker, trading-day key) whose funds check already succeeded.
_accepted: set[tuple[str, str]] = set()
# Don't hammer a broker that did not answer. The next try is after this.
_UNCHECKED_RETRY_SECONDS = 60
_unchecked_until: dict[str, float] = {}


def clock_may_revoke(broker: str | None) -> bool:
    """True only for a named broker outside Fyers, Delta Exchange, and Groww.

    A request that has not named a broker must not revoke a stored token
    because the clock passed. The funds check is what ends one of these tokens.
    """
    return bool(broker) and broker not in BROKERS


def keeps_login_across_rollover(broker: str | None) -> bool:
    """True when this tab's broker stays logged in across 03:00.

    A request that has not named a broker is left to the clock for the
    validity flag, and the expiry hook must not revoke or clear on that
    flag alone. The tab header is what names Fyers, Delta Exchange, or Groww.
    """
    return broker in BROKERS


def _day_key() -> str:
    from utils.session import last_rollover_boundary

    return last_rollover_boundary().date().isoformat()


def _stored_token(broker: str):
    """``(username, plaintext token)`` for this broker's unrevoked row, or None."""
    from database.auth_db import Auth, decrypt_token

    try:
        rows = Auth.query.filter_by(is_revoked=False, broker=broker).all()
    except Exception:
        logger.exception("Could not read the stored %s token", broker)
        return None
    for row in rows:
        if not row.auth or not row.name:
            continue
        try:
            token = decrypt_token(row.auth)
        except Exception:
            logger.exception("Could not decrypt the stored %s token", broker)
            continue
        if token:
            return row.name, token
    return None


def _ask_broker(broker: str, token: str) -> str:
    """``accepted``, ``rejected``, or ``unchecked`` from the broker's funds call."""
    funds = importlib.import_module(f"broker.{broker}.api.funds")
    classify = getattr(funds, "classify_auth_token", None)
    if classify is None:
        data = funds.get_margin_data(token)
        if not isinstance(data, dict) or not data:
            return "rejected"
        status = str(data.get("status", "")).lower()
        if status in {"error", "failed", "failure"}:
            return "rejected"
        return "accepted"
    outcome = classify(token)
    if outcome not in {"accepted", "rejected", "unchecked"}:
        logger.warning("%s funds check returned %r; token left in place", broker, outcome)
        return "unchecked"
    return outcome


def check_broker_token(broker: str) -> str:
    """Funds-check ``broker`` once per trading day.

    Returns ``accepted``, ``rejected``, ``absent``, ``unchecked``, or ``skip``.
    ``rejected`` revokes that broker's stored token. A network failure is
    ``unchecked`` and is tried again on a later call. Call inside that
    broker's context.
    """
    if broker not in BROKERS:
        return "skip"
    if current_broker() != broker:
        raise BrokerContextError(
            f"Funds-check {broker} only inside that broker's context."
        )
    day = _day_key()
    now = time.monotonic()
    with _lock:
        if (broker, day) in _accepted:
            return "accepted"
        if _unchecked_until.get(broker, 0) > now:
            return "unchecked"

    stored = _stored_token(broker)
    if stored is None:
        return "absent"
    username, token = stored

    try:
        outcome = _ask_broker(broker, token)
    except Exception:
        logger.warning(
            "Could not funds-check %s; the stored token was left in place", broker
        )
        outcome = "unchecked"
    if outcome == "unchecked":
        with _lock:
            _unchecked_until[broker] = time.monotonic() + _UNCHECKED_RETRY_SECONDS
        logger.info("%s funds check did not get an answer; token left in place", broker)
        return "unchecked"
    if outcome == "rejected":
        from database.auth_db import upsert_auth

        upsert_auth(username, "", broker, revoke=True)
        logger.info("%s rejected the stored token; that broker needs a sign-in", broker)
        return "rejected"

    with _lock:
        _accepted.add((broker, day))
        _unchecked_until.pop(broker, None)
    logger.info("%s accepted the stored token for %s", broker, day)
    return "accepted"


def check_each_broker() -> None:
    """Funds-check every broker that has a context. Used at startup."""

    def _one() -> None:
        broker = current_broker()
        if broker in BROKERS:
            try:
                check_broker_token(broker)
            except Exception:
                logger.exception("Startup funds check failed for %s", broker)

    run_for_each_broker(_one)


def reset_checks_for_tests() -> None:
    """Drop the in-memory 'already accepted today' set."""
    with _lock:
        _accepted.clear()
        _unchecked_until.clear()
