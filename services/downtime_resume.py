"""Resume after downtime, and warnings for an exit that was missed.

A clean shutdown is not the trader pressing Stop. The next process resumes
each broker's runs. If a clock stop or a price exit would have happened while
this process was down, a warning is stored and the order is not sent.

Paper does not close a square-off that came due during the gap. Python and
OpenScript do not gain a guessed flatten: the warning says the stop did not
run and any position is still at the broker.

Candles for a missed price exit come from Historify when they cover the gap,
otherwise from that broker's history when its session is alive, otherwise the
warning says the gap cannot be judged.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path

import pytz

from utils.broker_context import BROKERS, current_broker, run_for_each_broker
from utils.logging import get_logger

logger = get_logger(__name__)

IST = pytz.timezone("Asia/Kolkata")

CLOCK_DETAIL = (
    "The scheduled stop was due at {due} while OpenAlgo was off, so it did not run. "
    "Any position is still at the broker. OpenAlgo did not send that order and did not guess an exit."
)
PRICE_DETAIL = (
    "A {rule} would have been hit at {price} while OpenAlgo was off. "
    "The close was not sent. Any position is still at the broker. "
    "Confirm sends this close. Clear leaves it unsent. "
    "You can also close the position manually."
)
UNJUDGED_DETAIL = (
    "The gap from {start} to {end} cannot be judged for {symbol}. "
    "Historify does not cover it, and {why}."
)

_lock = threading.Lock()
# broker -> (gap_start, gap_end), both aware IST datetimes.
_gaps: dict[str, tuple[datetime, datetime]] = {}
# (broker, subject) waiting for the first tick after resume.
_awaiting_first: set[tuple[str, str]] = set()
# subjects whose next clock fire must not send an order (one shot).
_suppress_clock: set[tuple[str, str]] = set()
_resumed: set[str] = set()
_heartbeat_started = False
# Tests replace the real close. Production uses _dispatch_held_close.
_close_sender = None


def _broker() -> str:
    broker = current_broker()
    return broker if broker in BROKERS else ""


def state_dir() -> Path:
    """Where this broker's resume files live. Tests may monkeypatch this."""
    broker = _broker()
    if broker:
        from utils.broker_context import PROJECT_ROOT

        path = PROJECT_ROOT / "db" / "brokers" / broker
    else:
        path = Path("db") / "resume"
    path.mkdir(parents=True, exist_ok=True)
    return path


def warning_db_path() -> Path:
    return state_dir() / "missed_exit.db"


def _connect() -> sqlite3.Connection:
    connection = sqlite3.connect(warning_db_path())
    connection.row_factory = sqlite3.Row
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS missed_exit_warning (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subject TEXT NOT NULL,
            kind TEXT NOT NULL,
            due_at TEXT,
            rule TEXT,
            detail TEXT NOT NULL,
            symbol TEXT,
            price TEXT,
            bar_time TEXT,
            source TEXT,
            confirmed_at TEXT,
            cleared_at TEXT,
            created_at TEXT NOT NULL,
            close_payload TEXT,
            resolution TEXT,
            UNIQUE(subject, kind, due_at, rule, symbol)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS held_breach (
            subject TEXT NOT NULL,
            rule TEXT NOT NULL,
            symbol TEXT NOT NULL DEFAULT '',
            recovered_at TEXT,
            PRIMARY KEY (subject, rule, symbol)
        )
        """
    )
    present = {row[1] for row in connection.execute("PRAGMA table_info(missed_exit_warning)")}
    if "close_payload" not in present:
        connection.execute("ALTER TABLE missed_exit_warning ADD COLUMN close_payload TEXT")
    if "resolution" not in present:
        connection.execute("ALTER TABLE missed_exit_warning ADD COLUMN resolution TEXT")
    return connection


def _iso(value: datetime | None) -> str | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = IST.localize(value)
    return value.astimezone(IST).isoformat()


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = IST.localize(parsed)
    return parsed.astimezone(IST)


def read_heartbeat() -> datetime | None:
    path = state_dir() / "resume_heartbeat.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    return _parse(raw.get("at") if isinstance(raw, dict) else None)


def write_heartbeat(when: datetime | None = None) -> None:
    moment = when or datetime.now(IST)
    path = state_dir() / "resume_heartbeat.json"
    path.write_text(json.dumps({"at": _iso(moment)}), encoding="utf-8")


def touch_all_heartbeats() -> None:
    """Record that every broker context was up, including on a clean shutdown."""
    if _broker():
        write_heartbeat()
        return
    run_for_each_broker(write_heartbeat)


def open_gap(now: datetime | None = None) -> tuple[datetime, datetime] | None:
    """The downtime before this process, from the last heartbeat through now.

    Stored for the rest of the process. A second call returns the same gap.
    """
    broker = _broker()
    with _lock:
        existing = _gaps.get(broker)
    if existing is not None:
        return existing
    end = now or datetime.now(IST)
    if end.tzinfo is None:
        end = IST.localize(end)
    else:
        end = end.astimezone(IST)
    start = read_heartbeat()
    if start is None or start >= end:
        return None
    with _lock:
        _gaps[broker] = (start, end)
    return start, end


def current_gap() -> tuple[datetime, datetime] | None:
    with _lock:
        return _gaps.get(_broker())


def time_in_gap(moment: datetime) -> bool:
    gap = current_gap()
    if gap is None:
        return False
    if moment.tzinfo is None:
        moment = IST.localize(moment)
    else:
        moment = moment.astimezone(IST)
    start, end = gap
    return start < moment <= end


def due_in_gap(hhmm: str, days: list[str] | None = None) -> datetime | None:
    """The clock time inside the downtime gap, or None."""
    gap = current_gap()
    if gap is None or not hhmm or ":" not in hhmm:
        return None
    try:
        hour, minute = _hhmm(hhmm)
    except ValueError:
        return None
    start, end = gap
    day = start.astimezone(IST).date()
    last = end.astimezone(IST).date()
    names = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
    allowed = {str(item)[:3].lower() for item in days} if days else None
    found = None
    while day <= last:
        if allowed is not None and names[day.weekday()] not in allowed:
            day += timedelta(days=1)
            continue
        due = IST.localize(datetime(day.year, day.month, day.day, hour, minute))
        if start < due <= end:
            found = due
        day += timedelta(days=1)
    return found


def _hhmm(value: str) -> tuple[int, int]:
    parts = str(value).split(":")
    if len(parts) < 2:
        raise ValueError(value)
    return int(parts[0]), int(parts[1])


def record_warning(
    *,
    subject: str,
    kind: str,
    detail: str,
    due_at: datetime | None = None,
    rule: str | None = None,
    symbol: str | None = None,
    price: str | None = None,
    bar_time: datetime | None = None,
    source: str | None = None,
    close: dict | None = None,
) -> dict:
    """Store one warning. The same subject, kind, time, rule and symbol is kept once.

    A restart does not remove it. Confirming it does not send an order.
    """
    created = _iso(datetime.now(IST))
    with _connect() as connection:
        cursor = connection.execute(
            """
            INSERT OR IGNORE INTO missed_exit_warning (
                subject, kind, due_at, rule, detail, symbol, price, bar_time,
                source, created_at, close_payload
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                subject,
                kind,
                _iso(due_at),
                rule,
                detail,
                symbol,
                price,
                _iso(bar_time),
                source,
                created,
                json.dumps(close) if close else None,
            ),
        )
        connection.commit()
        inserted = cursor.rowcount == 1
        row = connection.execute(
            """
            SELECT * FROM missed_exit_warning
            WHERE subject = ? AND kind = ? AND ifnull(due_at, '') = ifnull(?, '')
              AND ifnull(rule, '') = ifnull(?, '') AND ifnull(symbol, '') = ifnull(?, '')
            """,
            (subject, kind, _iso(due_at), rule, symbol),
        ).fetchone()
    if inserted:
        logger.warning(detail)
        _show_on_strategy(subject, detail)
    return dict(row) if row is not None else {}


def _show_on_strategy(subject: str, detail: str) -> None:
    """Put the warning on the strategy's event list when that strategy can be read."""
    try:
        from services.strategy_module import store

        strategy_id = None
        user_id = ""
        if subject.startswith("strategy-run:"):
            run = store.get_run(int(subject.split(":", 1)[1]))
            if run is None:
                return
            strategy_id = int(run.strategy_id)
        elif subject.startswith("strategy-module:"):
            strategy_id = int(subject.split(":", 1)[1])
        else:
            return
        strategy = store.get_strategy_unscoped(strategy_id)
        if strategy is None:
            return
        user_id = str(getattr(strategy, "user_id", "") or "")
        store.record_event(strategy_id, user_id, "missed_exit", detail, severity="warn")
    except Exception:
        logger.debug("Could not show the missed-exit warning on the strategy", exc_info=True)


def list_warnings(*, include_cleared: bool = False) -> list[dict]:
    query = "SELECT * FROM missed_exit_warning"
    if not include_cleared:
        query += " WHERE cleared_at IS NULL"
    query += " ORDER BY id"
    with _connect() as connection:
        rows = connection.execute(query).fetchall()
    return [dict(row) for row in rows]


def has_open_warning(subject: str, kind: str | None = None) -> bool:
    query = "SELECT 1 FROM missed_exit_warning WHERE subject = ? AND cleared_at IS NULL"
    params: list = [subject]
    if kind is not None:
        query += " AND kind = ?"
        params.append(kind)
    with _connect() as connection:
        return connection.execute(query, params).fetchone() is not None


def set_close_sender(sender) -> None:
    """Tests supply the function that receives the held close. None restores the real one."""
    global _close_sender
    _close_sender = sender


def _row_by_id(warning_id: int):
    with _connect() as connection:
        return connection.execute(
            "SELECT * FROM missed_exit_warning WHERE id = ?", (warning_id,)
        ).fetchone()


def confirm_warning(warning_id: int) -> dict:
    """Send the close this warning is holding. A warning with no close is not confirmed.

    Confirm is not a dismiss. If the send fails, the warning stays open.
    """
    row = _row_by_id(warning_id)
    if row is None or row["cleared_at"] or row["confirmed_at"]:
        return {"ok": False, "sent": False}
    raw = row["close_payload"]
    payload = json.loads(raw) if raw else None
    if not isinstance(payload, dict) or not payload.get("action"):
        return {"ok": False, "sent": False, "error": "no held close"}
    sender = _close_sender or _dispatch_held_close
    try:
        result = sender(payload)
    except Exception:
        logger.exception("Held close for warning %s failed", warning_id)
        return {"ok": False, "sent": False, "error": "the close failed"}
    if isinstance(result, dict) and result.get("ok") is False:
        return {
            "ok": False,
            "sent": False,
            "error": result.get("error") or "the close failed",
        }
    now = _iso(datetime.now(IST))
    with _connect() as connection:
        connection.execute(
            """
            UPDATE missed_exit_warning
            SET confirmed_at = ?, cleared_at = ?, resolution = 'confirmed'
            WHERE id = ? AND confirmed_at IS NULL AND cleared_at IS NULL
            """,
            (now, now, warning_id),
        )
        connection.commit()
    _hold_breach(row["subject"], row["rule"] or "", row["symbol"] or "")
    return {"ok": True, "sent": True, "result": result}


def clear_warning(warning_id: int) -> bool:
    """Put the warning away without sending its close.

    The breach that was held stays held. The next tick does not send it.
    """
    row = _row_by_id(warning_id)
    if row is None or row["cleared_at"]:
        return False
    now = _iso(datetime.now(IST))
    with _connect() as connection:
        cursor = connection.execute(
            """
            UPDATE missed_exit_warning
            SET cleared_at = ?, resolution = 'cleared'
            WHERE id = ? AND cleared_at IS NULL AND confirmed_at IS NULL
            """,
            (now, warning_id),
        )
        connection.commit()
        cleared = cursor.rowcount > 0
    if cleared:
        _hold_breach(row["subject"], row["rule"] or "", row["symbol"] or "")
    return cleared


def _hold_breach(subject: str, rule: str, symbol: str) -> None:
    with _connect() as connection:
        connection.execute(
            """
            INSERT INTO held_breach (subject, rule, symbol, recovered_at)
            VALUES (?, ?, ?, NULL)
            ON CONFLICT(subject, rule, symbol) DO UPDATE SET recovered_at = NULL
            """,
            (subject, rule or "", symbol or ""),
        )
        connection.commit()


def _breach_is_held(subject: str, rule: str, symbol: str) -> bool:
    with _connect() as connection:
        row = connection.execute(
            """
            SELECT 1 FROM held_breach
            WHERE subject = ? AND rule = ? AND symbol = ? AND recovered_at IS NULL
            """,
            (subject, rule or "", symbol or ""),
        ).fetchone()
    return row is not None


def _recover_breach(subject: str, symbol: str) -> None:
    """This symbol's price is back inside the stop. A later breach is a new one."""
    now = _iso(datetime.now(IST))
    with _connect() as connection:
        connection.execute(
            """
            UPDATE held_breach SET recovered_at = ?
            WHERE subject = ? AND symbol = ? AND recovered_at IS NULL
            """,
            (now, subject, symbol or ""),
        )
        connection.commit()


def warnings_for_tab() -> list[dict]:
    """The open warnings this broker's tab shows. The close payload stays on the server."""
    shown = []
    for row in list_warnings():
        shown.append(
            {
                "id": row["id"],
                "kind": row["kind"],
                "detail": row["detail"],
                "symbol": row["symbol"],
                "rule": row["rule"],
                "source": row["source"],
                "price": row["price"],
                "due_at": row["due_at"],
                "can_confirm": bool(row.get("close_payload")),
            }
        )
    return shown


def _close_for_price(subject: str, rule: str, symbol: str | None) -> dict | None:
    if subject.startswith("strategy-run:"):
        return {
            "action": "strategy_legs",
            "run_id": int(subject.split(":", 1)[1]),
            "symbol": symbol,
            "rule": rule,
        }
    if subject.startswith("scalping:"):
        parts = subject.split(":")
        if len(parts) < 4:
            return None
        return {
            "action": "scalping_exit",
            "symbol": parts[1],
            "exchange": parts[2],
            "product": parts[3],
            "reason": rule,
            "ltp": 0,
        }
    return None


def _clock_close(subject: str, source: str) -> dict | None:
    """The action Confirm runs for a clock stop. Python and OpenScript stop the
    child. They do not gain a guessed flatten."""
    name = subject.split(":", 1)[1] if ":" in subject else subject
    if source == "python":
        return {"action": "python_stop", "strategy_id": name}
    if source == "openscript":
        return {"action": "openscript_stop", "filename": name}
    if source == "chartink":
        return {"action": "chartink_squareoff", "strategy_id": int(name)}
    if source == "sandbox":
        return {"action": "sandbox_squareoff", "exchange": name}
    if source == "strategy_module":
        return {"action": "strategy_scheduled_stop", "strategy_id": int(name)}
    if source == "flow":
        return {"action": "flow_run", "workflow_id": int(name)} if str(name).isdigit() else {
            "action": "flow_run",
            "workflow_id": name,
        }
    return None


def _record_clock(subject: str, due: datetime, source: str) -> None:
    record_warning(
        subject=subject,
        kind="clock_stop",
        due_at=due,
        rule="scheduled_stop",
        source=source,
        detail=CLOCK_DETAIL.format(due=due.strftime("%Y-%m-%d %H:%M IST")),
        close=_clock_close(subject, source),
    )
    with _lock:
        _suppress_clock.add((_broker(), subject))


def note_missed_clock(
    subject: str,
    hhmm: str,
    *,
    source: str,
    days: list[str] | None = None,
) -> datetime | None:
    """Record a clock stop that fell inside the downtime. Does not send an order."""
    due = due_in_gap(hhmm, days)
    if due is None:
        return None
    _record_clock(subject, due, source)
    return due


def _acting_occurrence(hhmm: str, now: datetime, days: list[str] | None) -> datetime | None:
    """The stop a caller acting at ``now`` would be carrying out today."""
    if not hhmm or ":" not in hhmm:
        return None
    try:
        hour, minute = _hhmm(hhmm)
    except ValueError:
        return None
    local = now.astimezone(IST)
    due = IST.localize(datetime(local.year, local.month, local.day, hour, minute))
    if local < due:
        return None
    if days is not None:
        names = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
        allowed = {str(item)[:3].lower() for item in days}
        if names[due.weekday()] not in allowed:
            return None
    return due


def clock_stop_blocked(
    subject: str,
    hhmm: str,
    *,
    source: str,
    days: list[str] | None = None,
    now: datetime | None = None,
) -> bool:
    """True when the stop being acted on now fell while the process was down.

    The caller must not send the order. A later day, after the gap, is not
    blocked. Python and OpenScript are not flattened from the script text.
    """
    moment = now or datetime.now(IST)
    if moment.tzinfo is None:
        moment = IST.localize(moment)
    due = _acting_occurrence(hhmm, moment, days)
    if due is None or not time_in_gap(due):
        return False
    _record_clock(subject, due, source)
    return True


def arm_first_tick(subject: str) -> None:
    """The next price for ``subject`` is the first one after resume."""
    with _lock:
        _awaiting_first.add((_broker(), subject))


def block_automatic_exit(
    subject: str,
    breached: bool,
    *,
    rule: str,
    price: str | None = None,
    symbol: str | None = None,
    source: str | None = None,
    close: dict | None = None,
) -> bool:
    """True when the automatic close must not be sent.

    The breach held for confirmation is never auto-exited, including on the
    tick after the warning is cleared. A quiet tick means the price came back,
    so a later breach is a new one and may arm a live stop.
    """
    rule_name = rule or "stop"
    symbol_name = symbol or ""
    broker = _broker()
    key = (broker, subject)
    with _lock:
        awaiting = key in _awaiting_first
        if awaiting:
            _awaiting_first.discard(key)
    if not breached:
        _recover_breach(subject, symbol_name)
        return False
    if _breach_is_held(subject, rule_name, symbol_name):
        return True
    if awaiting:
        record_warning(
            subject=subject,
            kind="price_breach",
            rule=rule_name,
            price=None if price is None else str(price),
            symbol=symbol,
            source=source or "first_tick",
            close=close,
            detail=PRICE_DETAIL.format(
                rule=rule_name,
                price=price if price is not None else "the resumed price",
            ),
        )
        _hold_breach(subject, rule_name, symbol_name)
        return True
    if has_open_warning(subject, "price_breach"):
        return True
    return False


def bars_cover_gap(bars, start: datetime, end: datetime, step_seconds: int = 60) -> bool:
    """True when ``bars`` span the downtime, not merely touch one end of it."""
    stamps = []
    for bar in bars or []:
        raw = bar.get("timestamp") if isinstance(bar, dict) else None
        if raw is None:
            continue
        if isinstance(raw, datetime):
            moment = raw if raw.tzinfo else IST.localize(raw)
            stamps.append(moment.timestamp())
        else:
            stamps.append(float(raw))
    if not stamps:
        return False
    first, last = min(stamps), max(stamps)
    start_ts = start.timestamp()
    end_ts = end.timestamp()
    return first <= start_ts + step_seconds and last >= end_ts - step_seconds


def replay_leg(leg: dict, bars) -> list[dict]:
    """Run the strategy-module leg rules over ``bars`` in memory.

    The caller's leg is not changed. Nothing is sent.
    """
    from services.strategy_module.risk_adapter import evaluate_leg

    sample = deepcopy(leg)
    found = []
    for bar in bars or []:
        stamp = bar.get("timestamp")
        for label in ("low", "high"):
            price = bar.get(label)
            if price is None:
                continue
            trial = deepcopy(sample)
            decision = evaluate_leg(trial, price)
            if getattr(decision, "breached", False):
                found.append(
                    {
                        "rule": getattr(decision, "reason", None) or label,
                        "price": str(price),
                        "bar_time": stamp,
                        "detail": getattr(decision, "detail", "") or "",
                    }
                )
                return found
        close = bar.get("close")
        if close is not None:
            evaluate_leg(sample, close)
    return found


def replay_run(state: dict, strategy: dict, bars) -> list[dict]:
    """Run the run-level rules over bar closes, on a copy of ``state``."""
    from services.strategy_module.risk_adapter import evaluate_run

    sample = deepcopy(state)
    found = []
    for bar in bars or []:
        close = bar.get("close")
        if close is None:
            continue
        for leg in sample.get("legs", {}).values():
            if leg.get("status") == "open":
                leg["ltp"] = float(close)
        decision = evaluate_run(sample, strategy)
        if getattr(decision, "breached", False):
            found.append(
                {
                    "rule": getattr(decision, "reason", None) or "run",
                    "price": str(close),
                    "bar_time": bar.get("timestamp"),
                    "detail": getattr(decision, "detail", "") or "",
                }
            )
            return found
    return found


def load_historify_bars(symbol: str, exchange: str, start: datetime, end: datetime):
    """1-minute Historify bars for the gap, or None when they cannot be read."""
    try:
        from database.historify_db import get_ohlcv

        frame = get_ohlcv(
            symbol,
            exchange,
            "1m",
            int(start.timestamp()),
            int(end.timestamp()),
        )
    except Exception:
        logger.debug("Historify could not be read for %s", symbol, exc_info=True)
        return None
    if frame is None or getattr(frame, "empty", True):
        return []
    rows = []
    for record in frame.to_dict("records"):
        rows.append(
            {
                "timestamp": record.get("timestamp"),
                "high": record.get("high"),
                "low": record.get("low"),
                "close": record.get("close"),
            }
        )
    return rows


def session_is_alive() -> bool:
    """True when this broker still has an unrevoked stored token."""
    try:
        from database.auth_db import Auth

        for row in Auth.query.filter_by(is_revoked=False).all():
            if row.auth:
                return True
    except Exception:
        logger.debug("Could not tell whether the broker session is alive", exc_info=True)
    return False


def load_broker_bars(symbol: str, exchange: str, start: datetime, end: datetime):
    """That broker's history, only when its session is alive.

    A failure, including the broker being unreachable, is not a cover.
    """
    if not session_is_alive():
        return None
    try:
        from database.auth_db import Auth, decrypt_token
        from services.history_service import get_history

        token = None
        broker = _broker()
        for row in Auth.query.filter_by(is_revoked=False).all():
            if row.auth:
                token = decrypt_token(row.auth)
                broker = broker or row.broker
                break
        if not token or not broker:
            return None
        ok, payload, _status = get_history(
            symbol,
            exchange,
            "1m",
            start.astimezone(IST).date().isoformat(),
            end.astimezone(IST).date().isoformat(),
            auth_token=token,
            broker=broker,
        )
    except Exception:
        logger.warning("Broker history for %s could not be read", symbol, exc_info=True)
        return None
    if not ok or not isinstance(payload, dict):
        return None
    data = payload.get("data") or payload.get("results") or []
    if not isinstance(data, list):
        return None
    return data


def replay_scalping(state: dict, bars) -> list[dict]:
    """Run the scalping stop and target over ``bars``, on a copy."""
    from services.scalping_risk_monitor_service import evaluate_trail

    sample = deepcopy(state)
    found = []
    for bar in bars or []:
        for label in ("low", "high"):
            price = bar.get(label)
            if price is None:
                continue
            decision = evaluate_trail(deepcopy(sample), float(price))
            if decision.get("breached"):
                found.append(
                    {
                        "rule": decision.get("reason") or label,
                        "price": str(price),
                        "bar_time": bar.get("timestamp"),
                        "detail": "",
                    }
                )
                return found
        close = bar.get("close")
        if close is not None:
            updated = evaluate_trail(sample, float(close))
            for key in ("highest_price", "lowest_price", "current_sl"):
                if key in updated:
                    sample[key] = updated[key]
    return found


def judge_symbol_gap(
    *,
    subject: str,
    symbol: str,
    exchange: str,
    leg: dict | None = None,
    run_state: dict | None = None,
    strategy: dict | None = None,
    scalping_state: dict | None = None,
    historify_bars=None,
    broker_bars=None,
    session_alive: bool | None = None,
) -> str:
    """Decide the gap for one symbol. Returns ``historify``, ``broker``, or ``unjudged``.

    Writes a warning for a breach or for a gap that cannot be judged. Does not
    send an order.
    """
    gap = current_gap()
    if gap is None:
        return "unjudged"
    start, end = gap
    if historify_bars is None:
        historify_bars = load_historify_bars(symbol, exchange, start, end)
    covered_by = None
    bars = None
    if bars_cover_gap(historify_bars, start, end):
        covered_by = "historify"
        bars = historify_bars
    else:
        alive = session_is_alive() if session_alive is None else session_alive
        if broker_bars is None and alive:
            broker_bars = load_broker_bars(symbol, exchange, start, end)
        if alive and bars_cover_gap(broker_bars, start, end):
            covered_by = "broker"
            bars = broker_bars
        else:
            why = (
                "this broker's session is not alive"
                if not alive
                else "that broker's history does not cover it either"
            )
            record_warning(
                subject=subject,
                kind="gap_unjudged",
                symbol=symbol,
                source=covered_by,
                due_at=end,
                detail=UNJUDGED_DETAIL.format(
                    start=start.strftime("%Y-%m-%d %H:%M IST"),
                    end=end.strftime("%Y-%m-%d %H:%M IST"),
                    symbol=symbol,
                    why=why,
                ),
            )
            return "unjudged"

    breaches = []
    if scalping_state is not None:
        breaches.extend(replay_scalping(scalping_state, bars))
    elif leg is not None:
        breaches.extend(replay_leg(leg, bars))
    if run_state is not None and strategy is not None:
        breaches.extend(replay_run(run_state, strategy, bars))
    for breach in breaches:
        stamp = breach.get("bar_time")
        bar_time = stamp if isinstance(stamp, datetime) else None
        if bar_time is None and isinstance(stamp, (int, float)):
            bar_time = datetime.fromtimestamp(stamp, IST)
        rule = str(breach.get("rule") or "stop")
        record_warning(
            subject=subject,
            kind="price_breach",
            rule=rule,
            symbol=symbol,
            price=str(breach.get("price")),
            bar_time=bar_time,
            due_at=bar_time,
            source=covered_by,
            close=_close_for_price(subject, rule, symbol),
            detail=PRICE_DETAIL.format(rule=rule, price=breach.get("price")),
        )
        _hold_breach(subject, rule, symbol or "")
    return covered_by


def _python_clock_stops() -> None:
    from blueprints.python_strategy import CONFIG_FILE

    try:
        raw = json.loads(Path(CONFIG_FILE).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return
    if not isinstance(raw, dict):
        return
    for strategy_id, config in raw.items():
        if not isinstance(config, dict) or not config.get("schedule_stop"):
            continue
        if config.get("manually_stopped"):
            continue
        note_missed_clock(
            f"python:{strategy_id}",
            str(config.get("schedule_stop")),
            source="python",
            days=config.get("schedule_days"),
        )


def _openscript_clock_stops() -> None:
    try:
        from blueprints.openscript_runner import _load_schedules
    except Exception:
        logger.debug("OpenScript schedules could not be read", exc_info=True)
        return
    for filename, entry in (_load_schedules() or {}).items():
        if not isinstance(entry, dict) or not entry.get("stop_time"):
            continue
        note_missed_clock(
            f"openscript:{filename}",
            str(entry.get("stop_time")),
            source="openscript",
            days=entry.get("days"),
        )


def _strategy_module_clock_and_prices() -> None:
    try:
        from services.strategy_module import state as sm_state
        from services.strategy_module import store
    except Exception:
        logger.debug("Strategy module state could not be read", exc_info=True)
        return
    try:
        run_ids = list(sm_state.active_run_ids())
    except Exception:
        logger.debug("Strategy module runs could not be listed", exc_info=True)
        return
    for run_id in run_ids:
        arm_first_tick(f"strategy-run:{run_id}")
        run_row = None
        try:
            run_row = store.get_run(run_id)
        except Exception:
            run_row = None
        strategy = None
        if run_row is not None:
            try:
                strategy_row = store.get_strategy_unscoped(int(run_row.strategy_id))
                strategy = store.strategy_to_dict(strategy_row) if strategy_row is not None else None
                hhmm = None
                if strategy:
                    schedule = strategy.get("scheduler") or {}
                    hhmm = schedule.get("auto_stop_time") or strategy.get("exit_time")
                if hhmm:
                    note_missed_clock(
                        f"strategy-module:{run_row.strategy_id}",
                        str(hhmm),
                        source="strategy_module",
                        days=(strategy.get("scheduler") or {}).get("days"),
                    )
            except Exception:
                logger.debug("Strategy %s schedule could not be read", run_id, exc_info=True)
        run_state = sm_state.get_run_state(run_id)
        if not run_state:
            continue
        for leg in sm_state.open_legs(run_state):
            symbol = str(leg.get("symbol") or "")
            exchange = str(leg.get("exchange") or "")
            if not symbol:
                continue
            judge_symbol_gap(
                subject=f"strategy-run:{run_id}",
                symbol=symbol,
                exchange=exchange,
                leg=leg,
                run_state=run_state,
                strategy=strategy or {},
            )


def _scalping_prices() -> None:
    try:
        from database.scalping_db import get_active_sl_states
    except Exception:
        logger.debug("Scalping stops could not be read", exc_info=True)
        return
    try:
        rows = get_active_sl_states()
    except Exception:
        logger.debug("Scalping stops could not be listed", exc_info=True)
        return
    for row in rows or []:
        symbol = str(row.get("symbol") or "")
        exchange = str(row.get("exchange") or "")
        product = str(row.get("product") or "")
        subject = f"scalping:{symbol}:{exchange}:{product}"
        arm_first_tick(subject)
        if symbol:
            judge_symbol_gap(
                subject=subject,
                symbol=symbol,
                exchange=exchange,
                scalping_state=dict(row),
            )


def _chartink_clock_stops() -> None:
    try:
        from database.chartink_db import ChartinkStrategy
    except Exception:
        return
    try:
        rows = ChartinkStrategy.query.all()
    except Exception:
        logger.debug("Chartink strategies could not be read", exc_info=True)
        return
    for strategy in rows:
        if not getattr(strategy, "is_intraday", False) or not getattr(strategy, "squareoff_time", None):
            continue
        if not getattr(strategy, "is_active", False):
            continue
        note_missed_clock(
            f"chartink:{strategy.id}",
            str(strategy.squareoff_time),
            source="chartink",
        )


def _sandbox_squareoff() -> None:
    """Warn for a paper square-off that came due during the gap. Do not close."""
    try:
        from sandbox.squareoff_manager import SquareOffManager
    except Exception:
        return
    manager = SquareOffManager()
    for exchange, square_off_time in (manager.square_off_times or {}).items():
        if square_off_time is None:
            continue
        note_missed_clock(
            f"sandbox:{exchange}",
            square_off_time.strftime("%H:%M"),
            source="sandbox",
        )


def _flow_clock_stops() -> None:
    try:
        from services.flow_scheduler_service import get_flow_scheduler

        scheduler = get_flow_scheduler()
        jobs = scheduler.get_jobs()
    except Exception:
        logger.debug("Flow jobs could not be listed", exc_info=True)
        return
    gap = current_gap()
    if gap is None:
        return
    start, end = gap
    for job in jobs or []:
        trigger = getattr(job, "trigger", None)
        if trigger is None or not hasattr(trigger, "get_next_fire_time"):
            continue
        try:
            fired = trigger.get_next_fire_time(None, start)
        except Exception:
            continue
        if fired is None:
            continue
        if fired.tzinfo is None:
            fired = IST.localize(fired)
        if not (start < fired <= end):
            continue
        args = getattr(job, "args", None) or ()
        identity = args[0] if args else getattr(job, "id", "")
        subject = f"flow:{identity}"
        record_warning(
            subject=subject,
            kind="clock_stop",
            due_at=fired,
            rule="scheduled_run",
            source="flow",
            detail=CLOCK_DETAIL.format(due=fired.astimezone(IST).strftime("%Y-%m-%d %H:%M IST")),
        )
        with _lock:
            _suppress_clock.add((_broker(), subject))


def consume_clock_suppression(subject: str) -> bool:
    """True once, when this subject must not fire the missed clock stop."""
    key = (_broker(), subject)
    with _lock:
        if key in _suppress_clock:
            _suppress_clock.discard(key)
            return True
    return False


def resume_after_downtime() -> dict:
    """Judge this broker's gap, warn, and do not send the missed orders.

    Safe to call once per process. Later calls leave the recorded gap alone.
    """
    broker = _broker()
    with _lock:
        if broker in _resumed:
            return {"broker": broker, "skipped": True}
        _resumed.add(broker)
    gap = open_gap()
    if gap is not None:
        for step in (
            _python_clock_stops,
            _openscript_clock_stops,
            _strategy_module_clock_and_prices,
            _scalping_prices,
            _chartink_clock_stops,
            _sandbox_squareoff,
            _flow_clock_stops,
        ):
            try:
                step()
            except Exception:
                logger.exception("Resume check failed: %s", step.__name__)
    try:
        write_heartbeat()
    except Exception:
        logger.exception("Could not write the resume heartbeat")
    warnings = []
    try:
        warnings = list_warnings()
    except Exception:
        logger.exception("Could not read resume warnings")
    return {"broker": broker, "gap": gap, "warnings": len(warnings)}


def resume_for_each_broker() -> None:
    """Resume and judge the gap in every broker context."""
    run_for_each_broker(resume_after_downtime)
    _start_heartbeat()


def _start_heartbeat() -> None:
    global _heartbeat_started
    with _lock:
        if _heartbeat_started:
            return
        _heartbeat_started = True

    def _loop() -> None:
        while True:
            threading.Event().wait(60)
            try:
                touch_all_heartbeats()
            except Exception:
                logger.debug("Resume heartbeat failed", exc_info=True)

    threading.Thread(target=_loop, name="resume-heartbeat", daemon=True).start()


def _dispatch_held_close(payload: dict):
    """Run the close that was held. Callers pass ``from_confirm`` so the gap
    guard does not swallow the order AM just confirmed."""
    action = payload.get("action")
    if action == "strategy_stop":
        from services.strategy_module.engine import stop_run

        return stop_run(int(payload["run_id"]), str(payload.get("user_id") or ""), reason="confirmed")
    if action == "strategy_legs":
        from services.strategy_module import state as sm_state
        from services.strategy_module import store
        from services.strategy_module.engine import _api_key_for, _exit_legs

        run_id = int(payload["run_id"])
        run_row = store.get_run(run_id)
        if run_row is None:
            return {"ok": False, "error": "run is gone"}
        strategy_row = store.get_strategy_unscoped(int(run_row.strategy_id))
        strategy = store.strategy_to_dict(strategy_row) if strategy_row is not None else {}
        user_id = str(getattr(strategy_row, "user_id", "") or payload.get("user_id") or "")
        mode = str(run_row.mode)
        api_key = _api_key_for(user_id)
        legs = payload.get("legs") or []
        if not legs and payload.get("symbol"):
            run_state = sm_state.get_run_state(run_id) or {}
            legs = [
                {"leg_id": leg.get("leg_id"), "kind": payload.get("rule") or "sl"}
                for leg in (run_state.get("legs") or {}).values()
                if leg.get("symbol") == payload.get("symbol") and leg.get("status") == "open"
            ]
        sent = []
        for leg in legs:
            sent.extend(
                _exit_legs(
                    run_id,
                    strategy,
                    [leg.get("leg_id")],
                    str(leg.get("kind") or "sl"),
                    mode,
                    api_key,
                    user_id,
                )
            )
        return {"ok": True, "sent": sent}
    if action == "strategy_scheduled_stop":
        from services.strategy_module.scheduler import run_scheduled_stop

        run_scheduled_stop(int(payload["strategy_id"]), from_confirm=True)
        return {"ok": True}
    if action == "scalping_exit":
        from services.scalping_risk_monitor_service import get_scalping_risk_monitor

        get_scalping_risk_monitor().confirm_exit(
            payload.get("symbol"),
            payload.get("exchange"),
            payload.get("product"),
            payload.get("reason") or "sl",
            float(payload.get("ltp") or 0),
        )
        return {"ok": True}
    if action == "python_stop":
        from blueprints.python_strategy import scheduled_stop_strategy

        scheduled_stop_strategy(str(payload["strategy_id"]), from_confirm=True)
        return {"ok": True}
    if action == "openscript_stop":
        from blueprints.openscript_runner import _scheduled_stop

        _scheduled_stop(str(payload["filename"]), from_confirm=True)
        return {"ok": True}
    if action == "chartink_squareoff":
        from blueprints.chartink import squareoff_positions

        squareoff_positions(int(payload["strategy_id"]), from_confirm=True)
        return {"ok": True}
    if action == "sandbox_squareoff":
        from sandbox.squareoff_manager import SquareOffManager

        SquareOffManager().close_missed_squareoff(str(payload["exchange"]))
        return {"ok": True}
    if action == "flow_run":
        from services.flow_scheduler_service import execute_workflow_scheduled

        execute_workflow_scheduled(int(payload["workflow_id"]), from_confirm=True)
        return {"ok": True}
    raise ValueError(f"No held close for {action!r}")


def reset_for_tests() -> None:
    """Drop the in-memory resume state. Does not delete warning files."""
    global _heartbeat_started, _close_sender
    with _lock:
        _gaps.clear()
        _awaiting_first.clear()
        _suppress_clock.clear()
        _resumed.clear()
        _heartbeat_started = True
        _close_sender = None
