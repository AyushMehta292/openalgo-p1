"""Isolated subprocess launcher for OpenAlgo HTTP server and WebSocket proxy.

Run as:
    python -m test.e2e._server_launcher --mode server --root <dir> --port <port> ...
    python -m test.e2e._server_launcher --mode ws_proxy --root <dir> --ws-port <port> --zmq-port <port> ...

Critical isolation sequence (executed BEFORE importing ``app`` or ``database``):
1. Set all 5 ``*_DATABASE_URL`` env vars under ``<root>/db/`` and ``LOG_DIR=<root>/log``.
2. Neutralize ``dotenv.load_dotenv`` and ``utils.env_check.load_and_check_env_variables``
   so repository ``.env`` is never read, created, or modified.
3. Redirect ``utils.broker_context.PROJECT_ROOT = Path(root)`` so
   ``retarget_sqlite_database()`` genuinely isolates ``db/shared/openalgo.db`` and
   ``db/brokers/{fyers,deltaexchange,groww}/*.db`` inside ``tmp_path``.
4. Install ``BrokerNetworkFirewall`` so zero real-broker order calls can escape.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import json
import os
import signal
import sys
import threading
import time
from pathlib import Path

_E2E_DIR = Path(__file__).resolve().parent
REPO_ROOT = _E2E_DIR.parents[1]


def _load_firewall_module():
    spec = importlib.util.spec_from_file_location("_openalgo_e2e_firewall", _E2E_DIR / "_firewall.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


BrokerNetworkFirewall = _load_firewall_module().BrokerNetworkFirewall
BROKERS = ("fyers", "deltaexchange", "groww")

# Seeded SymToken catalog per broker (matching TEST_INFRA.md and Explorer 2 conventions).
SEED_SYMTOKENS: dict[str, list[dict]] = {
    "deltaexchange": [
        {
            "symbol": "BTCUSDFUT",
            "brsymbol": "BTCUSD",
            "name": "Bitcoin Perpetual",
            "exchange": "CRYPTO",
            "brexchange": "DELTAEXCHANGE",
            "token": "27",
            "expiry": "",
            "strike": 0.0,
            "lotsize": 1,
            "instrumenttype": "PERPFUT",
            "tick_size": 0.5,
            "contract_value": 0.001,
        },
        {
            "symbol": "BTCINR",
            "brsymbol": "BTC_INR",
            "name": "Bitcoin Spot INR",
            "exchange": "CRYPTO",
            "brexchange": "DELTAEXCHANGE",
            "token": "99",
            "expiry": "",
            "strike": 0.0,
            "lotsize": 1,
            "instrumenttype": "SPOT",
            "tick_size": 0.01,
            "contract_value": 1.0,
        },
    ],
    "fyers": [
        {
            "symbol": "SBIN",
            "brsymbol": "NSE:SBIN-EQ",
            "name": "State Bank of India",
            "exchange": "NSE",
            "brexchange": "NSE",
            "token": "10100000003045",
            "expiry": "",
            "strike": 0.0,
            "lotsize": 1,
            "instrumenttype": "EQ",
            "tick_size": 0.05,
            "contract_value": 1.0,
        },
        {
            "symbol": "NIFTY27MAR25FUT",
            "brsymbol": "NSE:NIFTY27MAR25FUT",
            "name": "NIFTY MAR FUT",
            "exchange": "NFO",
            "brexchange": "NFO",
            "token": "10112503270001",
            "expiry": "27-MAR-25",
            "strike": 0.0,
            "lotsize": 75,
            "instrumenttype": "FUT",
            "tick_size": 0.05,
            "contract_value": 1.0,
        },
        {
            "symbol": "CRUDEOIL25MARFUT",
            "brsymbol": "MCX:CRUDEOIL25MARFUT",
            "name": "CRUDEOIL MAR FUT",
            "exchange": "MCX",
            "brexchange": "MCX",
            "token": "11202503190001",
            "expiry": "19-MAR-25",
            "strike": 0.0,
            "lotsize": 100,
            "instrumenttype": "FUT",
            "tick_size": 1.0,
            "contract_value": 1.0,
        },
        {
            "symbol": "NIFTY",
            "brsymbol": "NSE:NIFTY50-INDEX",
            "name": "NIFTY 50",
            "exchange": "NSE_INDEX",
            "brexchange": "NSE",
            "token": "101000000026000",
            "expiry": "",
            "strike": 0.0,
            "lotsize": 1,
            "instrumenttype": "INDEX",
            "tick_size": 0.05,
            "contract_value": 1.0,
        },
        *[
            {
                "symbol": f"NIFTY27MAR25{strike}{opt}",
                "brsymbol": f"NSE:NIFTY25MAR{strike}{opt}",
                "name": "NIFTY",
                "exchange": "NFO",
                "brexchange": "NFO",
                "token": f"1011250327{strike}{1 if opt == 'CE' else 2}",
                "expiry": "27-MAR-25",
                "strike": float(strike),
                "lotsize": 75,
                "instrumenttype": opt,
                "tick_size": 0.05,
                "contract_value": 1.0,
            }
            for strike in (23900, 24000, 24100)
            for opt in ("CE", "PE")
        ],
    ],
    "groww": [
        {
            "symbol": "SBIN",
            "brsymbol": "SBIN",
            "name": "State Bank of India",
            "exchange": "NSE",
            "brexchange": "NSE",
            "token": "3045",
            "expiry": "",
            "strike": 0.0,
            "lotsize": 1,
            "instrumenttype": "EQ",
            "tick_size": 0.05,
            "contract_value": 1.0,
        },
        {
            "symbol": "NIFTY27MAR25FUT",
            "brsymbol": "NIFTY25MARFUT",
            "name": "NIFTY MAR FUT",
            "exchange": "NFO",
            "brexchange": "NSE",
            "token": "50001",
            "expiry": "27-MAR-25",
            "strike": 0.0,
            "lotsize": 75,
            "instrumenttype": "FUT",
            "tick_size": 0.05,
            "contract_value": 1.0,
        },
        {
            "symbol": "NIFTY",
            "brsymbol": "NIFTY",
            "name": "NIFTY 50",
            "exchange": "NSE_INDEX",
            "brexchange": "NSE",
            "token": "26000",
            "expiry": "",
            "strike": 0.0,
            "lotsize": 1,
            "instrumenttype": "INDEX",
            "tick_size": 0.05,
            "contract_value": 1.0,
        },
    ],
}


def _configure_isolated_env(
    *,
    root: Path,
    port: int,
    profile: str,
    paper_trading_only: bool | str,
    disable_historical_data: str | None,
    mcp_http_enabled: bool,
    ws_port: int | None = None,
    zmq_port: int | None = None,
    stats_file: Path | None = None,
) -> None:
    """Populate isolated env vars before any OpenAlgo module imports."""
    db_dir = root / "db"
    log_dir = root / "log"
    strategies_dir = root / "strategies"
    db_dir.mkdir(parents=True, exist_ok=True)
    (db_dir / "shared").mkdir(parents=True, exist_ok=True)
    for b in BROKERS:
        (db_dir / "brokers" / b).mkdir(parents=True, exist_ok=True)
        (db_dir / "brokers" / b / "strategies").mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)
    strategies_dir.mkdir(parents=True, exist_ok=True)

    defaults: dict[str, str] = {
        "ENV_CONFIG_VERSION": "1.0.5",
        "APP_KEY": "e2e-openalgo-app-key-" + ("a" * 48),
        "API_KEY_PEPPER": "b" * 64,
        "FERNET_SALT": "c" * 32,
        "DATABASE_URL": "sqlite:///" + (db_dir / "openalgo.db").resolve().as_posix(),
        "SANDBOX_DATABASE_URL": "sqlite:///" + (db_dir / "sandbox.db").resolve().as_posix(),
        "LOGS_DATABASE_URL": "sqlite:///" + (db_dir / "logs.db").resolve().as_posix(),
        "LATENCY_DATABASE_URL": "sqlite:///" + (db_dir / "latency.db").resolve().as_posix(),
        "HEALTH_DATABASE_URL": "sqlite:///" + (db_dir / "health.db").resolve().as_posix(),
        "HISTORIFY_DATABASE_PATH": str((db_dir / "historify.duckdb").resolve()),
        "LOG_DIR": str(log_dir.resolve()),
        "LOG_TO_FILE": "True",
        "LOG_LEVEL": "INFO",
        "LOG_RETENTION": "7",
        "NGROK_ALLOW": "FALSE",
        "HOST_SERVER": f"http://127.0.0.1:{port}",
        "FLASK_HOST_IP": "127.0.0.1",
        "FLASK_PORT": str(port),
        "FLASK_DEBUG": "False",
        "FLASK_ENV": "production",
        "VALID_BROKERS": "fyers,deltaexchange,groww",
        "SESSION_EXPIRY_TIME": "03:00",
        "LOGIN_RATE_LIMIT_MIN": "10000 per minute",
        "LOGIN_RATE_LIMIT_HOUR": "100000 per hour",
        "API_RATE_LIMIT": "10000 per second",
        "ORDER_RATE_LIMIT": "10000 per second",
        "SMART_ORDER_RATE_LIMIT": "10000 per second",
        "WEBHOOK_RATE_LIMIT": "10000 per second",
        "STRATEGY_RATE_LIMIT": "10000 per second",
        "WEBSOCKET_HOST": "127.0.0.1",
        "WEBSOCKET_PORT": str(ws_port if ws_port is not None else 8765),
        "WEBSOCKET_URL": f"ws://127.0.0.1:{ws_port if ws_port is not None else 8765}",
        "ZMQ_HOST": "127.0.0.1",
        "ZMQ_PORT": str(zmq_port if zmq_port is not None else 5555),
        "WEBSOCKET_PROXY_MODE": "external",
        "ORDER_UPDATES_ENABLED": "FALSE",
        "CSRF_ENABLED": "TRUE",
    }

    for k, v in defaults.items():
        os.environ.setdefault(k, v)

    # Guard against short test keys inherited from outer test/conftest.py.
    if len(os.environ.get("APP_KEY", "")) < 32:
        os.environ["APP_KEY"] = defaults["APP_KEY"]
    if len(os.environ.get("API_KEY_PEPPER", "")) < 32:
        os.environ["API_KEY_PEPPER"] = defaults["API_KEY_PEPPER"]
    if len(os.environ.get("FERNET_SALT", "")) < 32:
        os.environ["FERNET_SALT"] = defaults["FERNET_SALT"]

    # Forced isolation overrides (must not be inherited from outer shell).
    os.environ["DATABASE_URL"] = defaults["DATABASE_URL"]
    os.environ["SANDBOX_DATABASE_URL"] = defaults["SANDBOX_DATABASE_URL"]
    os.environ["LOGS_DATABASE_URL"] = defaults["LOGS_DATABASE_URL"]
    os.environ["LATENCY_DATABASE_URL"] = defaults["LATENCY_DATABASE_URL"]
    os.environ["HEALTH_DATABASE_URL"] = defaults["HEALTH_DATABASE_URL"]
    os.environ["HISTORIFY_DATABASE_PATH"] = defaults["HISTORIFY_DATABASE_PATH"]
    os.environ["LOG_DIR"] = defaults["LOG_DIR"]
    os.environ["HOST_SERVER"] = defaults["HOST_SERVER"]
    os.environ["FLASK_HOST_IP"] = "127.0.0.1"
    os.environ["FLASK_PORT"] = str(port)
    os.environ["FLASK_DEBUG"] = "False"
    os.environ["OPENALGO_PROFILE"] = profile
    if isinstance(paper_trading_only, bool):
        os.environ["PAPER_TRADING_ONLY"] = "TRUE" if paper_trading_only else "FALSE"
    else:
        os.environ["PAPER_TRADING_ONLY"] = str(paper_trading_only)

    if disable_historical_data is not None:
        os.environ["DISABLE_HISTORICAL_DATA"] = disable_historical_data
    elif "DISABLE_HISTORICAL_DATA" not in os.environ:
        os.environ.pop("DISABLE_HISTORICAL_DATA", None)

    if mcp_http_enabled:
        os.environ["MCP_HTTP_ENABLED"] = "true"
        os.environ["MCP_PUBLIC_URL"] = f"http://127.0.0.1:{port}"

    if ws_port is not None:
        os.environ["WEBSOCKET_PORT"] = str(ws_port)
        os.environ["WEBSOCKET_URL"] = f"ws://127.0.0.1:{ws_port}"
    if zmq_port is not None:
        os.environ["ZMQ_PORT"] = str(zmq_port)
    if stats_file is not None:
        os.environ["WS_PROXY_STATS_FILE"] = str(stats_file)


def _install_isolation_hooks(root: Path, mock_quotes: Path, outbound_log: Path) -> BrokerNetworkFirewall:
    """Neutralize dotenv/repo-.env, redirect PROJECT_ROOT, and install BrokerNetworkFirewall."""
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))

    # 1. Neutralize dotenv so repo .env is never loaded.
    import dotenv

    dotenv.load_dotenv = lambda *a, **kw: False

    # 2. Redirect utils.broker_context.PROJECT_ROOT BEFORE importing database or app.
    import utils.broker_context as bc

    bc.PROJECT_ROOT = root.resolve()

    # 3. Neutralize env_check so repo .env is never created/rewritten.
    import utils.env_check as ec

    ec.load_and_check_env_variables = lambda: None

    # 4. Install network firewall BEFORE any broker/app code can open a socket.
    firewall = BrokerNetworkFirewall(mock_quotes_path=mock_quotes, outbound_log_path=outbound_log)
    firewall.install()

    # 5. Point broker_keys.env_file_path away from repo .env.
    try:
        import utils.broker_keys as bk

        bk.env_file_path = lambda: root / ".env.nonexistent"
    except Exception:
        pass

    return firewall


def _seed_isolated_state(
    flask_app,
    *,
    port: int,
    username: str,
    password: str,
    api_key: str,
) -> None:
    """Seed admin user, broker keys, mock auth tokens, shared API key, and SymTokens."""
    from database.auth_db import upsert_api_key, upsert_auth
    from database.broker_keys_db import save_broker_keys
    from database.symbol import SymToken, db_session as sym_session
    from database.user_db import add_user, find_user_by_exact_username
    from utils.broker_context import broker_context

    with flask_app.app_context():
        # 1. Shared admin user (db/shared/openalgo.db).
        if find_user_by_exact_username(username) is None:
            add_user(username, f"{username}@e2e.local", password, is_admin=True)

        # 2. Per-broker keys, mock auth tokens, and SymToken catalog.
        for broker in BROKERS:
            with broker_context(broker):
                save_broker_keys(
                    broker,
                    {
                        "api_key": f"e2e-{broker}-apikey",
                        "api_secret": f"e2e-{broker}-secret",
                    },
                    redirect_url=f"http://127.0.0.1:{port}/{broker}/callback",
                )
                # Pass defer=[] so seeding does not emit ZMQ teardown or start live adapters.
                upsert_auth(
                    name=username,
                    auth_token=f"e2e-{broker}-auth-token",
                    broker=broker,
                    feed_token=f"e2e-{broker}-feed-token",
                    user_id=f"e2e-{broker}-uid",
                    revoke=False,
                    defer=[],
                )
                for row_data in SEED_SYMTOKENS.get(broker, []):
                    existing = (
                        sym_session.query(SymToken)
                        .filter_by(symbol=row_data["symbol"], exchange=row_data["exchange"])
                        .first()
                    )
                    if existing is None:
                        sym_session.add(SymToken(**row_data))
                sym_session.commit()
                sym_session.remove()

        # 3. Shared API key synced across all 3 broker DBs.
        with broker_context("fyers"):
            upsert_api_key(username, api_key)


def _write_ready_file(ready_file: Path, payload: dict) -> None:
    ready_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = ready_file.with_suffix(ready_file.suffix + ".tmp")
    tmp.write_text(json.dumps(payload), encoding="utf-8")
    os.replace(tmp, ready_file)


def _run_http_server(args: argparse.Namespace) -> None:
    root = Path(args.root).resolve()
    mock_quotes = Path(args.mock_quotes).resolve()
    outbound_log = Path(args.outbound_log).resolve()
    ready_file = Path(args.ready_file).resolve()

    _configure_isolated_env(
        root=root,
        port=args.port,
        profile=args.profile,
        paper_trading_only=args.paper_trading_only,
        disable_historical_data=args.disable_historical_data,
        mcp_http_enabled=(args.mcp_http_enabled.lower() in ("true", "1", "yes", "on")),
        ws_port=args.ws_port,
        zmq_port=args.zmq_port,
    )
    _install_isolation_hooks(root=root, mock_quotes=mock_quotes, outbound_log=outbound_log)

    import app as openalgo_app_mod

    flask_app = openalgo_app_mod.app
    flask_app.db_ready.wait(timeout=30.0)
    flask_app.db_ready.set()

    _seed_isolated_state(
        flask_app,
        port=args.port,
        username=args.username,
        password=args.password,
        api_key=args.api_key,
    )

    from werkzeug.serving import make_server

    httpd = make_server("127.0.0.1", args.port, flask_app, threaded=True)

    def _shutdown_handler(_signum, _frame):
        threading.Thread(target=httpd.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, _shutdown_handler)
    signal.signal(signal.SIGINT, _shutdown_handler)

    _write_ready_file(
        ready_file,
        {
            "status": "ready",
            "mode": "server",
            "port": args.port,
            "pid": os.getpid(),
            "profile": args.profile,
        },
    )
    httpd.serve_forever()


def _run_ws_proxy(args: argparse.Namespace) -> None:
    """Isolated websocket_proxy.server launcher with PROJECT_ROOT and firewall hooks."""
    root = Path(args.root).resolve()
    mock_quotes = Path(args.mock_quotes).resolve()
    outbound_log = Path(args.outbound_log).resolve()
    ready_file = Path(args.ready_file).resolve()
    stats_file = Path(args.stats_file).resolve()

    _configure_isolated_env(
        root=root,
        port=args.port,
        profile=args.profile,
        paper_trading_only=True,
        disable_historical_data=None,
        mcp_http_enabled=False,
        ws_port=args.ws_port,
        zmq_port=args.zmq_port,
        stats_file=stats_file,
    )
    _install_isolation_hooks(root=root, mock_quotes=mock_quotes, outbound_log=outbound_log)

    # Ensure auth/symbol DB tables exist if ws_proxy is started standalone without a server.
    from database.auth_db import init_db as init_auth_db, upsert_api_key, upsert_auth
    from database.symbol import Base as SymBase, engine as sym_engine
    from utils.broker_context import broker_context

    for broker in BROKERS:
        with broker_context(broker):
            init_auth_db()
            SymBase.metadata.create_all(bind=sym_engine)
            if args.seed_standalone:
                upsert_auth(
                    name=args.username,
                    auth_token=f"e2e-{broker}-auth-token",
                    broker=broker,
                    feed_token=f"e2e-{broker}-feed-token",
                    user_id=f"e2e-{broker}-uid",
                    revoke=False,
                    defer=[],
                )
    if args.seed_standalone:
        for broker in BROKERS:
            with broker_context(broker):
                upsert_api_key(args.username, args.api_key)

    from websocket_proxy.server import WebSocketProxy

    async def _serve() -> None:
        proxy = WebSocketProxy(host="127.0.0.1", port=args.ws_port)

        def _stop_signal(*_a):
            proxy.running = False

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _stop_signal)
            except NotImplementedError:
                pass

        async def _signal_ready_when_bound():
            deadline = time.monotonic() + 15.0
            while time.monotonic() < deadline:
                if getattr(proxy, "server", None) is not None:
                    _write_ready_file(
                        ready_file,
                        {
                            "status": "ready",
                            "mode": "ws_proxy",
                            "ws_port": args.ws_port,
                            "zmq_port": args.zmq_port,
                            "pid": os.getpid(),
                        },
                    )
                    return
                await asyncio.sleep(0.05)

        ready_task = asyncio.create_task(_signal_ready_when_bound())
        try:
            await proxy.start()
        finally:
            ready_task.cancel()
            await proxy.stop()

    asyncio.run(_serve())


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Isolated E2E launcher for OpenAlgo.")
    parser.add_argument("--mode", choices=("server", "ws_proxy"), default="server")
    parser.add_argument("--root", required=True, help="Isolated tmp_path acting as PROJECT_ROOT")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--ws-port", type=int, default=8765)
    parser.add_argument("--zmq-port", type=int, default=5555)
    parser.add_argument("--profile", default="cloud")
    parser.add_argument("--paper-trading-only", default="TRUE")
    parser.add_argument("--disable-historical-data", default=None)
    parser.add_argument("--mcp-http-enabled", default="false")
    parser.add_argument("--username", default="e2e_admin")
    parser.add_argument("--password", default="E2ePass!2026")
    parser.add_argument("--api-key", default="e2e-shared-apikey-0123456789abcdef")
    parser.add_argument("--mock-quotes", required=True)
    parser.add_argument("--outbound-log", required=True)
    parser.add_argument("--ready-file", required=True)
    parser.add_argument("--stats-file", default=None)
    parser.add_argument("--seed-standalone", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    if args.mode == "ws_proxy":
        if not args.stats_file:
            args.stats_file = str(Path(args.root) / "log" / "ws_proxy_stats.json")
        _run_ws_proxy(args)
    else:
        _run_http_server(args)


if __name__ == "__main__":
    main()
