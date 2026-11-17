"""One broker's databases and keys for the duration of a call.

Fyers, Delta Exchange, and Groww each have a directory under ``db/brokers/``.
A request, webhook, scheduler job, or tick enters exactly one of those
directories before it reads broker keys from the environment or opens a
broker SQLite file. The OpenAlgo administrator, the market calendar, and
freeze quantities stay in ``db/shared/`` and do not need a broker.

Whether a stored token survives 03:00 is decided by utils.daily_token_check.
Missed-exit warnings are not handled here.
"""

from __future__ import annotations

import contextvars
import os
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

# Plugin ids, in the order AM set.
BROKERS: tuple[str, ...] = ("fyers", "deltaexchange", "groww")

#: SQLite files that move with the broker. ``openalgo.db`` is the main one.
BROKER_DB_FILES = frozenset(
    {"openalgo.db", "logs.db", "latency.db", "health.db", "sandbox.db"}
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]

_current: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "openalgo_broker", default=None
)

_BROKER_SCOPED_SESSIONS: tuple[tuple[str, str], ...] = (
    ("database.auth_db", "db_session"),
    ("database.broker_keys_db", "db_session"),
    ("database.settings_db", "db_session"),
    ("database.sandbox_db", "db_session"),
    ("database.symbol", "db_session"),
)


def _release_broker_db_sessions() -> None:
    """Release per-broker scoped SQLAlchemy sessions on broker context enter/exit."""
    import sys

    for module_name, attr in _BROKER_SCOPED_SESSIONS:
        mod = sys.modules.get(module_name)
        if mod is None:
            continue
        sess = getattr(mod, attr, None)
        if sess is not None and hasattr(sess, "remove"):
            try:
                sess.remove()
            except Exception:
                pass


class BrokerContextError(RuntimeError):
    """A broker database or broker key was used with no broker selected."""


def current_broker() -> str | None:
    """The broker whose directory this context is using, or None."""
    return _current.get()


def require_broker() -> str:
    """The current broker, or an error if the caller has not entered one."""
    broker = _current.get()
    if broker not in BROKERS:
        raise BrokerContextError(
            "Enter a broker context before opening a broker database or reading broker keys."
        )
    return broker


def push_broker(broker: str):
    """Select ``broker`` until ``pop_broker`` on the returned token."""
    if broker not in BROKERS:
        raise BrokerContextError(f"Unknown broker {broker!r}.")
    (PROJECT_ROOT / "db" / "brokers" / broker).mkdir(parents=True, exist_ok=True)
    _release_broker_db_sessions()
    return _current.set(broker)


def pop_broker(token) -> None:
    """Leave the broker selected by ``push_broker``."""
    if token is not None:
        _release_broker_db_sessions()
        _current.reset(token)


@contextmanager
def broker_context(broker: str) -> Iterator[str]:
    """Use ``broker``'s directory until the block ends.

    ``broker`` is one of ``fyers``, ``deltaexchange``, ``groww``.
    """
    token = push_broker(broker)
    try:
        yield broker
    finally:
        pop_broker(token)


def broker_from_http_request(path: str, header: str, query: str, body_broker: str) -> str | None:
    """Which broker this HTTP request is for.

    Explicit targets on the request (callback or credentials path, ``?broker=``
    query parameter, or ``POST /auth/broker/login`` body) take precedence over
    the tab's previous ``X-OpenAlgo-Broker`` header so switching or logging in
    to a different broker in the same tab enters the target broker immediately.
    """
    parts = [part for part in (path or "").split("/") if part]
    path_broker = None
    if parts and parts[0] in BROKERS:
        path_broker = parts[0]
    elif len(parts) >= 4 and parts[0] == "api" and parts[1] == "broker" and parts[2] == "credentials":
        if parts[3] in BROKERS:
            path_broker = parts[3]
    for candidate in (path_broker, query, body_broker, header):
        name = (candidate or "").strip().lower()
        if name in BROKERS:
            return name
    return None


def select_broker(predicate: Callable[[], bool]) -> bool:
    """Enter the first broker for which ``predicate`` is true. Leave none if all miss.

    Used by webhooks, which have no tab. The matching broker stays selected
    for the rest of the request.
    """
    from flask import g

    for name in BROKERS:
        token = push_broker(name)
        try:
            matched = bool(predicate())
        except Exception:
            pop_broker(token)
            raise
        if matched:
            previous = getattr(g, "_broker_token", None)
            if previous is not None:
                pop_broker(previous)
            g._broker_token = token
            tab = getattr(g, "_tab_state", None) or {}
            tab["broker"] = name
            g._tab_state = tab
            return True
        pop_broker(token)
    return False


def select_broker_for_api_key(apikey: str) -> bool:
    """Enter the broker whose platform ``api_keys`` row verifies ``apikey``.

    ``/api/v1`` callers (Python strategies, OpenScript, the openalgo SDK) often
    send no ``X-OpenAlgo-Broker`` header. The verified key picks the directory,
    the same way a webhook finds its strategy.
    """
    if not (apikey or "").strip():
        return False
    from database.auth_db import verify_api_key

    key = apikey.strip()
    return select_broker(lambda: verify_api_key(key) is not None)


def run_for_each_broker(func: Callable, *args, **kwargs) -> None:
    """Call ``func`` once inside each broker directory.

    A caller that is already inside a broker runs once, for that broker.
    """
    broker = current_broker()
    if broker in BROKERS:
        func(*args, **kwargs)
        return
    for name in BROKERS:
        with broker_context(name):
            func(*args, **kwargs)


def bind_job(broker: str, func: Callable) -> Callable:
    """Return ``func`` so a scheduler thread re-enters ``broker`` before it runs."""
    if getattr(func, "_openalgo_broker_bound", False):
        return func

    def wrapped(*args, **kwargs):
        with broker_context(broker):
            return func(*args, **kwargs)

    wrapped._openalgo_broker_bound = True
    wrapped.__name__ = getattr(func, "__name__", "job")
    wrapped.__doc__ = getattr(func, "__doc__", None)
    return wrapped


def project_main_db() -> Path:
    return (PROJECT_ROOT / "db" / "openalgo.db").resolve()


def env_points_at_project_db() -> bool:
    """True when DATABASE_URL is this install's ``db/openalgo.db``.

    Tests that point DATABASE_URL at a temporary file stay on that file.
    """
    url = os.getenv("DATABASE_URL", "sqlite:///db/openalgo.db") or ""
    if "sqlite" not in url:
        return False
    raw = url.split("sqlite:///", 1)[-1]
    path = Path(raw)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()
    return path == project_main_db()


def shared_database_url() -> str:
    """SQLite URL for the administrator, calendar, and freeze quantities."""
    path = PROJECT_ROOT / "db" / "shared" / "openalgo.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return "sqlite:///" + path.resolve().as_posix()


def database_url_for_module(env_url: str | None) -> str | None:
    """Shared URL for process-wide tables when this install uses ``db/openalgo.db``."""
    if env_url and "sqlite" in env_url and env_points_at_project_db():
        return shared_database_url()
    return env_url


def broker_db_file(name: str) -> Path:
    """``name`` (``openalgo.db``, ``sandbox.db``, ...) inside the current broker directory."""
    broker = require_broker()
    path = PROJECT_ROOT / "db" / "brokers" / broker / name
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def historify_file() -> Path:
    """This broker's Historify DuckDB file."""
    return broker_db_file("historify.duckdb")


def strategy_dir() -> Path:
    """Strategy scripts and JSON for the current broker, or the legacy folder with no broker."""
    broker = current_broker()
    if broker not in BROKERS:
        return Path("strategies")
    path = PROJECT_ROOT / "db" / "brokers" / broker / "strategies"
    path.mkdir(parents=True, exist_ok=True)
    return path


class BrokerPath:
    """A path that follows the current broker, and the legacy folder when none is selected.

    ``kind`` is ``scripts``, ``config``, ``logs``, ``openscript_running``, or
    ``openscript_schedules``.
    """

    def __init__(self, kind: str):
        self.kind = kind

    def _path(self) -> Path:
        if self.kind == "logs":
            return strategy_log_dir()
        root = strategy_dir()
        if self.kind == "scripts":
            return root / "scripts"
        if self.kind == "config":
            return root / "strategy_configs.json"
        if self.kind == "openscript_running":
            return root / "openscript_running.json"
        if self.kind == "openscript_schedules":
            return root / "openscript_runner_schedules.json"
        return root

    def __fspath__(self) -> str:
        return os.fspath(self._path())

    def __str__(self) -> str:
        return str(self._path())

    def __repr__(self) -> str:
        return f"BrokerPath({self.kind})"

    def __truediv__(self, other):
        return self._path() / other

    def __getattr__(self, name):
        return getattr(self._path(), name)


def strategy_log_dir() -> Path:
    broker = current_broker()
    if broker not in BROKERS:
        return Path("log") / "strategies"
    path = PROJECT_ROOT / "db" / "brokers" / broker / "log" / "strategies"
    path.mkdir(parents=True, exist_ok=True)
    return path


def retarget_sqlite_database(database):
    """The SQLite file a connection should open.

    ``db/openalgo.db`` and the satellite files are sent to the current broker.
    ``db/shared/`` is left alone. Any other path, including tests' temporary
    files, is left alone. Connecting to a broker file with no broker selected
    raises ``BrokerContextError``.
    """
    if not isinstance(database, (str, os.PathLike)):
        return database
    text = os.fspath(database)
    if not text or text == ":memory:" or text.startswith("file:"):
        return database
    path = Path(text)
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()

    shared_root = (PROJECT_ROOT / "db" / "shared").resolve()
    try:
        path.relative_to(shared_root)
        return str(path)
    except ValueError:
        pass

    db_root = (PROJECT_ROOT / "db").resolve()
    try:
        rel = path.relative_to(db_root)
    except ValueError:
        return database
    # db/brokers/<id>/... is already specific.
    if len(rel.parts) != 1 or rel.name not in BROKER_DB_FILES:
        return database

    dest = broker_db_file(rel.name)
    return str(dest)


def install_scheduler_scope() -> None:
    """Prefix scheduler job ids with the broker and re-enter that broker when the job runs.

    Memory schedulers (Python host, Chartink, strategy module, sandbox) add
    jobs while a broker context is active. Flow and Historify use their own
    thread, which enters the broker before it touches the job store.
    """
    from apscheduler.schedulers.base import BaseScheduler

    if getattr(BaseScheduler.add_job, "_openalgo_scoped", False):
        return

    orig_add = BaseScheduler.add_job
    orig_get = BaseScheduler.get_job
    orig_remove = BaseScheduler.remove_job
    orig_modify = BaseScheduler.modify_job
    orig_reschedule = BaseScheduler.reschedule_job

    def _scoped_id(job_id):
        broker = current_broker()
        if broker not in BROKERS or job_id is None:
            return job_id
        prefix = broker + ":"
        text = str(job_id)
        return text if text.startswith(prefix) else prefix + text

    def add_job(self, func, *args, **kwargs):
        broker = current_broker()
        if (
            broker in BROKERS
            and callable(func)
            and not getattr(func, "_openalgo_broker_bound", False)
        ):
            func = bind_job(broker, func)
            if kwargs.get("id") is not None:
                kwargs["id"] = _scoped_id(kwargs["id"])
        return orig_add(self, func, *args, **kwargs)

    def get_job(self, job_id, jobstore=None):
        return orig_get(self, _scoped_id(job_id), jobstore)

    def remove_job(self, job_id, jobstore=None):
        return orig_remove(self, _scoped_id(job_id), jobstore)

    def modify_job(self, job_id, jobstore=None, **changes):
        return orig_modify(self, _scoped_id(job_id), jobstore, **changes)

    def reschedule_job(self, job_id, jobstore=None, trigger=None, **trigger_args):
        return orig_reschedule(
            self, _scoped_id(job_id), jobstore, trigger, **trigger_args
        )

    add_job._openalgo_scoped = True
    BaseScheduler.add_job = add_job
    BaseScheduler.get_job = get_job
    BaseScheduler.remove_job = remove_job
    BaseScheduler.modify_job = modify_job
    BaseScheduler.reschedule_job = reschedule_job


class BrokerThreadPoolExecutor:
    """APScheduler thread pool that enters one broker before each job."""

    def __init__(self, broker: str, max_workers: int = 10):
        from apscheduler.executors.pool import ThreadPoolExecutor

        self._broker = broker
        self._inner = ThreadPoolExecutor(max_workers=max_workers)

    def __getattr__(self, name):
        return getattr(self._inner, name)

    def start(self, scheduler, alias):
        return self._inner.start(scheduler, alias)

    def shutdown(self, wait=True):
        return self._inner.shutdown(wait)

    def _do_submit_job(self, job, run_times):
        from apscheduler.executors.base import run_job

        def _run(*args, **kwargs):
            with broker_context(self._broker):
                return run_job(*args, **kwargs)

        def callback(future):
            exc, tb = (
                future.exception_info()
                if hasattr(future, "exception_info")
                else (future.exception(), getattr(future.exception(), "__traceback__", None))
            )
            if exc:
                self._inner._run_job_error(job.id, exc, tb)
            else:
                self._inner._run_job_success(job.id, future.result())

        future = self._inner._pool.submit(
            _run, job, job._jobstore_alias, run_times, self._inner._logger.name
        )
        future.add_done_callback(callback)


def start_in_broker(scheduler, broker: str) -> None:
    """Start a BackgroundScheduler whose loop and jobs stay inside ``broker``.

    Call this from inside ``broker_context(broker)``. The job store is opened
    during start, on this thread. Later reads happen on the scheduler thread,
    which enters ``broker`` before it touches SQLite.
    """
    from threading import Event, Thread

    from apscheduler.schedulers.base import BaseScheduler

    scheduler._executors["default"] = BrokerThreadPoolExecutor(broker)
    if getattr(scheduler, "_event", None) is None or scheduler._event.is_set():
        scheduler._event = Event()
    BaseScheduler.start(scheduler)

    def _loop():
        with broker_context(broker):
            scheduler._main_loop()

    scheduler._thread = Thread(target=_loop, name=f"APScheduler-{broker}", daemon=True)
    scheduler._thread.start()
