"""Shared pytest fixtures and opaque-box client helpers for test/e2e/.

Exports the exact public harness contract defined in TEST_INFRA.md Section 3.3:
- BROKERS, DEFAULT_SYMBOLS
- OpenAlgoE2EInstance, SessionClient, WsProxyE2EInstance
- Fixtures: server_factory, cloud_server, full_server, ws_proxy_factory
"""

from __future__ import annotations

import importlib.util
import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable

import pytest

try:
    import httpx
except ImportError:  # pragma: no cover - allows collection even without httpx
    httpx = None  # type: ignore[assignment]

_E2E_DIR = Path(__file__).resolve().parent
_LAUNCHER_SCRIPT = _E2E_DIR / "_server_launcher.py"
REPO_ROOT = _E2E_DIR.parents[1]


def _load_firewall_module():
    spec = importlib.util.spec_from_file_location("_openalgo_e2e_firewall", _E2E_DIR / "_firewall.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_FIREWALL_MOD = _load_firewall_module()
DEFAULT_QUOTE_SEEDS = _FIREWALL_MOD.DEFAULT_QUOTE_SEEDS
_assert_zero_real_broker_order_calls = _FIREWALL_MOD.assert_zero_real_broker_order_calls

BROKERS = ("fyers", "deltaexchange", "groww")
DEFAULT_SYMBOLS = {
    "deltaexchange": {
        "symbol": "BTCUSDFUT",
        "exchange": "CRYPTO",
        "product": "NRML",
        "ltp": 65000.0,
    },
    "fyers": {
        "symbol": "SBIN",
        "exchange": "NSE",
        "product": "CNC",
        "ltp": 800.0,
    },
    "groww": {
        "symbol": "SBIN",
        "exchange": "NSE",
        "product": "CNC",
        "ltp": 800.0,
    },
}

DEFAULT_USERNAME = "e2e_admin"
DEFAULT_PASSWORD = "E2ePass!2026"
DEFAULT_API_KEY = "e2e-shared-apikey-0123456789abcdef"


def pytest_configure(config: pytest.Config) -> None:
    """Register the 4 E2E tier markers dynamically without touching pyproject.toml."""
    config.addinivalue_line("markers", "tier1: Tier 1 E2E happy-path feature coverage")
    config.addinivalue_line("markers", "tier2: Tier 2 E2E boundary, negative, and corner cases")
    config.addinivalue_line("markers", "tier3: Tier 3 E2E cross-feature pairwise combinations")
    config.addinivalue_line("markers", "tier4: Tier 4 E2E real-world multi-step scenarios")


def _alloc_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return int(sock.getsockname()[1])


def _terminate_process(proc: subprocess.Popen[str] | None, timeout: float = 8.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5.0)
    except OSError:
        pass


def _wait_for_ready(
    proc: subprocess.Popen[str],
    ready_file: Path,
    stderr_log: Path,
    timeout: float = 45.0,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if ready_file.exists():
            try:
                data = json.loads(ready_file.read_text(encoding="utf-8"))
                if data.get("status") == "ready":
                    return data
            except (OSError, json.JSONDecodeError):
                pass
        if proc.poll() is not None:
            tail = stderr_log.read_text(encoding="utf-8", errors="replace")[-3000:] if stderr_log.exists() else ""
            raise RuntimeError(
                f"E2E subprocess exited prematurely with code {proc.returncode}.\n--- stderr ---\n{tail}"
            )
        time.sleep(0.1)
    tail = stderr_log.read_text(encoding="utf-8", errors="replace")[-3000:] if stderr_log.exists() else ""
    _terminate_process(proc)
    raise TimeoutError(
        f"E2E subprocess did not become ready within {timeout}s ({ready_file}).\n--- stderr ---\n{tail}"
    )


def _preseed_errors_jsonl(log_dir: Path, size_mb: float) -> None:
    """Create a valid JSONL errors.jsonl file of approximately ``size_mb`` MB."""
    if size_mb <= 0:
        return
    log_dir.mkdir(parents=True, exist_ok=True)
    target = log_dir / "errors.jsonl"
    target_bytes = int(size_mb * 1024 * 1024)
    sample_line = (
        json.dumps(
            {
                "timestamp": "2026-11-20T00:00:00Z",
                "level": "ERROR",
                "logger": "e2e.preseed",
                "message": "preseeded historical log line " + ("x" * 200),
            }
        )
        + "\n"
    ).encode("utf-8")
    chunk = sample_line * 256
    written = 0
    with target.open("wb") as fh:
        while written < target_bytes:
            remaining = target_bytes - written
            if remaining >= len(chunk):
                fh.write(chunk)
                written += len(chunk)
            else:
                lines_needed = max(1, remaining // len(sample_line))
                buf = sample_line * lines_needed
                fh.write(buf)
                written += len(buf)
                break


class SessionClient:
    """Authenticated browser/SPA client with session cookie, broker header, and CSRF token."""

    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        broker: str = "fyers",
        timeout: float = 15.0,
    ) -> None:
        if httpx is None:
            raise RuntimeError("httpx is required to use SessionClient")
        self.base_url = base_url.rstrip("/")
        self.broker = broker
        self._client = httpx.Client(
            base_url=self.base_url,
            follow_redirects=False,
            timeout=timeout,
            headers={
                "X-OpenAlgo-Broker": broker,
                "Accept": "application/json",
            },
        )
        self.csrf_token: str = ""
        self._login(username, password)

    def _login(self, username: str, password: str) -> None:
        self.refresh_csrf()
        headers = {
            "X-OpenAlgo-Broker": self.broker,
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }
        form_data = {"username": username, "password": password}
        if self.csrf_token:
            headers["X-CSRFToken"] = self.csrf_token
            form_data["csrf_token"] = self.csrf_token
        resp = self._client.post(
            "/auth/login",
            data=form_data,
            headers=headers,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"SessionClient login failed for {username}/{self.broker}: "
                f"HTTP {resp.status_code} {resp.text[:300]}"
            )
        self.refresh_csrf()

    def refresh_csrf(self) -> str:
        resp = self._client.get(
            "/auth/csrf-token",
            headers={"X-OpenAlgo-Broker": self.broker, "Accept": "application/json"},
        )
        if resp.status_code == 200:
            try:
                self.csrf_token = str(resp.json().get("csrf_token", ""))
            except ValueError:
                self.csrf_token = ""
        return self.csrf_token

    def _merge_headers(self, extra: dict[str, str] | None, *, include_csrf: bool) -> dict[str, str]:
        hdrs = {"X-OpenAlgo-Broker": self.broker}
        if include_csrf and self.csrf_token:
            hdrs["X-CSRFToken"] = self.csrf_token
        if extra:
            hdrs.update(extra)
        return hdrs

    def get(self, path: str, **kw: Any) -> httpx.Response:
        headers = self._merge_headers(kw.pop("headers", None), include_csrf=False)
        return self._client.get(path, headers=headers, **kw)

    def post(
        self,
        path: str,
        *,
        json: Any = None,
        data: Any = None,
        files: Any = None,
        include_csrf: bool = True,
        **kw: Any,
    ) -> httpx.Response:
        headers = self._merge_headers(kw.pop("headers", None), include_csrf=include_csrf)
        return self._client.post(path, json=json, data=data, files=files, headers=headers, **kw)

    def put(self, path: str, *, include_csrf: bool = True, **kw: Any) -> httpx.Response:
        headers = self._merge_headers(kw.pop("headers", None), include_csrf=include_csrf)
        return self._client.put(path, headers=headers, **kw)

    def delete(self, path: str, *, include_csrf: bool = True, **kw: Any) -> httpx.Response:
        headers = self._merge_headers(kw.pop("headers", None), include_csrf=include_csrf)
        return self._client.delete(path, headers=headers, **kw)

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "SessionClient":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()


class OpenAlgoE2EInstance:
    """Handle to an isolated local OpenAlgo HTTP server subprocess."""

    def __init__(
        self,
        *,
        base_url: str,
        root_dir: Path,
        api_key: str,
        username: str,
        password: str,
        profile: str,
        paper_trading_only: bool,
        mock_quotes_path: Path,
        outbound_log_path: Path,
        proc: subprocess.Popen[str],
        stderr_file_handle: Any,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.root_dir = Path(root_dir)
        self.db_dir = self.root_dir / "db"
        self.log_dir = self.root_dir / "log"
        self.api_key = api_key
        self.username = username
        self.password = password
        self.profile = profile
        self.paper_trading_only = paper_trading_only
        self.mock_quotes_path = Path(mock_quotes_path)
        self.outbound_log_path = Path(outbound_log_path)
        self._proc = proc
        self._stderr_fh = stderr_file_handle
        self._session_clients: list[SessionClient] = []
        self._stopped = False

    def api_post(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        broker: str | None = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 15.0,
    ) -> httpx.Response:
        if httpx is None:
            raise RuntimeError("httpx is required to use OpenAlgoE2EInstance.api_post")
        req_headers = {"Accept": "application/json"}
        if broker:
            req_headers["X-OpenAlgo-Broker"] = broker
        if headers:
            req_headers.update(headers)
        url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"
        return httpx.post(
            url,
            json=payload if payload is not None else {},
            params=query,
            headers=req_headers,
            timeout=timeout,
            follow_redirects=False,
        )

    def api_get(
        self,
        path: str,
        *,
        broker: str | None = None,
        query: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float = 15.0,
    ) -> httpx.Response:
        if httpx is None:
            raise RuntimeError("httpx is required to use OpenAlgoE2EInstance.api_get")
        req_headers: dict[str, str] = {}
        if broker:
            req_headers["X-OpenAlgo-Broker"] = broker
        if headers:
            req_headers.update(headers)
        url = f"{self.base_url}{path if path.startswith('/') else '/' + path}"
        return httpx.get(
            url,
            params=query,
            headers=req_headers,
            timeout=timeout,
            follow_redirects=False,
        )

    def session_client(self, broker: str = "fyers") -> SessionClient:
        client = SessionClient(
            base_url=self.base_url,
            username=self.username,
            password=self.password,
            broker=broker,
        )
        self._session_clients.append(client)
        return client

    def set_mock_quote(
        self,
        broker: str,
        exchange: str,
        symbol: str,
        *,
        ltp: float,
        bid: float | None = None,
        ask: float | None = None,
        open_: float | None = None,
        high: float | None = None,
        low: float | None = None,
        prev_close: float | None = None,
        volume: int = 1000,
    ) -> None:
        current: dict[str, Any] = {}
        if self.mock_quotes_path.exists():
            try:
                current = json.loads(self.mock_quotes_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                current = {}
        key = f"{broker.lower()}|{exchange.upper()}|{symbol}"
        existing = dict(current.get(key) or DEFAULT_QUOTE_SEEDS.get(key) or {})
        tick = max(0.05, round(float(ltp) * 0.0005, 4))
        eff_bid = float(bid) if bid is not None else round(float(ltp) - tick, 4)
        eff_ask = float(ask) if ask is not None else round(float(ltp) + tick, 4)
        eff_prev = float(prev_close) if prev_close is not None else float(existing.get("prev_close", ltp))
        eff_open = float(open_) if open_ is not None else float(existing.get("open", eff_prev))
        eff_high = float(high) if high is not None else max(float(ltp), eff_open, eff_prev, eff_ask) * 1.02
        eff_low = float(low) if low is not None else min(float(ltp), eff_open, eff_prev, eff_bid) * 0.98
        existing.update(
            {
                "broker": broker.lower(),
                "exchange": exchange.upper(),
                "symbol": symbol,
                "ltp": float(ltp),
                "bid": eff_bid,
                "ask": eff_ask,
                "open": eff_open,
                "high": max(eff_high, float(ltp), eff_ask),
                "low": min(eff_low, float(ltp), eff_bid),
                "prev_close": eff_prev,
                "volume": int(volume),
            }
        )
        current[key] = existing
        tmp = self.mock_quotes_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(current, indent=2), encoding="utf-8")
        os.replace(tmp, self.mock_quotes_path)

    def outbound_calls(self) -> list[dict[str, Any]]:
        if not self.outbound_log_path.exists():
            return []
        rows: list[dict[str, Any]] = []
        for raw_line in self.outbound_log_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return rows

    def assert_zero_real_broker_order_calls(self) -> None:
        _assert_zero_real_broker_order_calls(self.outbound_log_path)

    def duckdb_files(self) -> list[Path]:
        return sorted(self.root_dir.rglob("*.duckdb"))

    def broker_db_path(self, broker: str, filename: str = "openalgo.db") -> Path:
        return self.db_dir / "brokers" / broker / filename

    def errors_jsonl_path(self) -> Path:
        return self.log_dir / "errors.jsonl"

    def read_error_lines(self) -> list[dict[str, Any]]:
        path = self.errors_jsonl_path()
        if not path.exists():
            return []
        entries: list[dict[str, Any]] = []
        for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            try:
                entries.append(json.loads(line))
            except json.JSONDecodeError:
                entries.append({"raw": line})
        return entries

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        for client in self._session_clients:
            try:
                client.close()
            except Exception:
                pass
        self._session_clients.clear()
        _terminate_process(self._proc)
        if self._stderr_fh is not None:
            try:
                self._stderr_fh.close()
            except Exception:
                pass


class WsProxyE2EInstance:
    """Handle to an isolated local websocket_proxy.server subprocess."""

    def __init__(
        self,
        *,
        ws_port: int,
        zmq_port: int,
        stats_file: Path,
        root_dir: Path,
        outbound_log_path: Path,
        proc: subprocess.Popen[str],
        stderr_file_handle: Any,
    ) -> None:
        self.ws_port = ws_port
        self.zmq_port = zmq_port
        self.ws_url = f"ws://127.0.0.1:{ws_port}"
        self.stats_file = Path(stats_file)
        self.root_dir = Path(root_dir)
        self.outbound_log_path = Path(outbound_log_path)
        self._proc = proc
        self._stderr_fh = stderr_file_handle
        self._zmq_ctx = None
        self._zmq_pub = None
        self._stopped = False

    def _ensure_publisher(self) -> Any:
        if self._zmq_pub is not None:
            return self._zmq_pub
        import zmq

        self._zmq_ctx = zmq.Context()
        self._zmq_pub = self._zmq_ctx.socket(zmq.PUB)
        self._zmq_pub.setsockopt(zmq.LINGER, 200)
        self._zmq_pub.connect(f"tcp://127.0.0.1:{self.zmq_port}")
        # Brief settle so ZMQ PUB<->SUB subscription handshake completes (slow-joiner).
        time.sleep(0.25)
        return self._zmq_pub

    def publish_tick(self, exchange: str, symbol: str, mode: str, payload: dict[str, Any]) -> None:
        pub = self._ensure_publisher()
        topic = f"{exchange}_{symbol}_{mode.upper()}"
        pub.send_multipart([topic.encode("utf-8"), json.dumps(payload).encode("utf-8")])

    def publish_order_update(self, topic_prefix: str, user_id: str, payload: dict[str, Any]) -> None:
        pub = self._ensure_publisher()
        topic = f"{topic_prefix}_{user_id}_orders"
        pub.send_multipart([topic.encode("utf-8"), json.dumps(payload).encode("utf-8")])

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        if self._zmq_pub is not None:
            try:
                self._zmq_pub.close(linger=0)
            except Exception:
                pass
            self._zmq_pub = None
        if self._zmq_ctx is not None:
            try:
                self._zmq_ctx.term()
            except Exception:
                pass
            self._zmq_ctx = None
        _terminate_process(self._proc)
        if self._stderr_fh is not None:
            try:
                self._stderr_fh.close()
            except Exception:
                pass


def _launch_server(
    root_dir: Path,
    *,
    profile: str = "cloud",
    paper_trading_only: bool = True,
    disable_historical_data: bool | None = None,
    extra_env: dict[str, str] | None = None,
    preseed_errors_mb: float = 0.0,
    mcp_http_enabled: bool = False,
) -> OpenAlgoE2EInstance:
    root_dir = Path(root_dir).resolve()
    root_dir.mkdir(parents=True, exist_ok=True)
    log_dir = root_dir / "log"
    log_dir.mkdir(parents=True, exist_ok=True)

    if preseed_errors_mb and preseed_errors_mb > 0:
        _preseed_errors_jsonl(log_dir, preseed_errors_mb)

    mock_quotes_path = root_dir / "mock_quotes.json"
    if not mock_quotes_path.exists():
        mock_quotes_path.write_text(json.dumps(DEFAULT_QUOTE_SEEDS, indent=2), encoding="utf-8")

    outbound_log_path = root_dir / "outbound_broker_calls.jsonl"
    port = _alloc_free_port()
    ready_file = root_dir / f"ready_{port}.json"
    if ready_file.exists():
        ready_file.unlink()
    stderr_log = root_dir / f"server_{port}.stderr.log"

    env = os.environ.copy()
    for polluted_key in (
        "APP_KEY",
        "API_KEY_PEPPER",
        "FERNET_SALT",
        "CSRF_ENABLED",
        "LOG_FORMAT",
        "DISABLE_HISTORICAL_DATA",
        "OPENALGO_PROFILE",
        "PAPER_TRADING_ONLY",
        "MCP_HTTP_ENABLED",
        "MCP_PUBLIC_URL",
        "DATABASE_URL",
        "SANDBOX_DATABASE_URL",
        "LOGS_DATABASE_URL",
        "LATENCY_DATABASE_URL",
        "HEALTH_DATABASE_URL",
        "HISTORIFY_DATABASE_PATH",
        "LOG_DIR",
    ):
        env.pop(polluted_key, None)
    env["PYTHONPATH"] = str(REPO_ROOT) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    if extra_env:
        for k, v in extra_env.items():
            env[str(k)] = str(v)

    eff_profile = str(extra_env["OPENALGO_PROFILE"]) if extra_env and "OPENALGO_PROFILE" in extra_env else profile
    if extra_env and "PAPER_TRADING_ONLY" in extra_env:
        eff_pto_arg = str(extra_env["PAPER_TRADING_ONLY"])
        eff_pto_bool = eff_pto_arg.strip().lower() not in ("false", "0", "no", "off")
    else:
        eff_pto_arg = "TRUE" if paper_trading_only else "FALSE"
        eff_pto_bool = bool(paper_trading_only)

    cmd = [
        sys.executable,
        "-u",
        str(_LAUNCHER_SCRIPT),
        "--mode",
        "server",
        "--root",
        str(root_dir),
        "--port",
        str(port),
        "--profile",
        eff_profile,
        "--paper-trading-only",
        eff_pto_arg,
        "--mcp-http-enabled",
        "true" if mcp_http_enabled else "false",
        "--username",
        DEFAULT_USERNAME,
        "--password",
        DEFAULT_PASSWORD,
        "--api-key",
        DEFAULT_API_KEY,
        "--mock-quotes",
        str(mock_quotes_path),
        "--outbound-log",
        str(outbound_log_path),
        "--ready-file",
        str(ready_file),
    ]
    if disable_historical_data is not None:
        if isinstance(disable_historical_data, bool):
            dhd_arg = "true" if disable_historical_data else "false"
        else:
            dhd_arg = str(disable_historical_data)
        cmd.extend(["--disable-historical-data", dhd_arg])

    stderr_fh = stderr_log.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        env=env,
        stdout=stderr_fh,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_ready(proc, ready_file, stderr_log, timeout=45.0)
    except Exception:
        stderr_fh.close()
        raise

    return OpenAlgoE2EInstance(
        base_url=f"http://127.0.0.1:{port}",
        root_dir=root_dir,
        api_key=DEFAULT_API_KEY,
        username=DEFAULT_USERNAME,
        password=DEFAULT_PASSWORD,
        profile=eff_profile,
        paper_trading_only=eff_pto_bool,
        mock_quotes_path=mock_quotes_path,
        outbound_log_path=outbound_log_path,
        proc=proc,
        stderr_file_handle=stderr_fh,
    )


def _launch_ws_proxy(
    root_dir: Path,
    *,
    server: OpenAlgoE2EInstance | None = None,
    extra_env: dict[str, str] | None = None,
) -> WsProxyE2EInstance:
    if server is not None:
        root_dir = server.root_dir
        mock_quotes_path = server.mock_quotes_path
        outbound_log_path = server.outbound_log_path
        seed_standalone = False
    else:
        root_dir = Path(root_dir).resolve()
        root_dir.mkdir(parents=True, exist_ok=True)
        mock_quotes_path = root_dir / "mock_quotes.json"
        if not mock_quotes_path.exists():
            mock_quotes_path.write_text(json.dumps(DEFAULT_QUOTE_SEEDS, indent=2), encoding="utf-8")
        outbound_log_path = root_dir / "outbound_broker_calls.jsonl"
        seed_standalone = True

    ws_port = _alloc_free_port()
    zmq_port = _alloc_free_port()
    stats_file = root_dir / "log" / f"ws_proxy_stats_{ws_port}.json"
    stats_file.parent.mkdir(parents=True, exist_ok=True)
    ready_file = root_dir / f"ws_ready_{ws_port}.json"
    stderr_log = root_dir / f"ws_proxy_{ws_port}.stderr.log"

    env = os.environ.copy()
    for polluted_key in (
        "APP_KEY",
        "API_KEY_PEPPER",
        "FERNET_SALT",
        "LOG_FORMAT",
        "DATABASE_URL",
        "SANDBOX_DATABASE_URL",
        "LOGS_DATABASE_URL",
        "LATENCY_DATABASE_URL",
        "HEALTH_DATABASE_URL",
        "LOG_DIR",
    ):
        env.pop(polluted_key, None)
    env["PYTHONPATH"] = str(REPO_ROOT) + (
        os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else ""
    )
    if extra_env:
        for k, v in extra_env.items():
            env[str(k)] = str(v)

    cmd = [
        sys.executable,
        "-u",
        str(_LAUNCHER_SCRIPT),
        "--mode",
        "ws_proxy",
        "--root",
        str(root_dir),
        "--ws-port",
        str(ws_port),
        "--zmq-port",
        str(zmq_port),
        "--username",
        DEFAULT_USERNAME,
        "--api-key",
        DEFAULT_API_KEY,
        "--mock-quotes",
        str(mock_quotes_path),
        "--outbound-log",
        str(outbound_log_path),
        "--ready-file",
        str(ready_file),
        "--stats-file",
        str(stats_file),
    ]
    if seed_standalone:
        cmd.append("--seed-standalone")

    stderr_fh = stderr_log.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
        env=env,
        stdout=stderr_fh,
        stderr=subprocess.STDOUT,
        text=True,
    )
    try:
        _wait_for_ready(proc, ready_file, stderr_log, timeout=30.0)
    except Exception:
        stderr_fh.close()
        raise

    return WsProxyE2EInstance(
        ws_port=ws_port,
        zmq_port=zmq_port,
        stats_file=stats_file,
        root_dir=root_dir,
        outbound_log_path=outbound_log_path,
        proc=proc,
        stderr_file_handle=stderr_fh,
    )


@pytest.fixture
def server_factory(tmp_path: Path) -> Callable[..., OpenAlgoE2EInstance]:
    """Factory for isolated OpenAlgoE2EInstance servers; enforces zero real-broker orders on teardown."""
    started: list[OpenAlgoE2EInstance] = []
    counter = 0

    def _factory(
        *,
        profile: str = "cloud",
        paper_trading_only: bool = True,
        disable_historical_data: bool | None = None,
        extra_env: dict[str, str] | None = None,
        preseed_errors_mb: float = 0.0,
        mcp_http_enabled: bool = False,
        root_dir: Path | None = None,
    ) -> OpenAlgoE2EInstance:
        nonlocal counter
        counter += 1
        inst_root = Path(root_dir) if root_dir is not None else (tmp_path / f"srv_{counter}")
        instance = _launch_server(
            inst_root,
            profile=profile,
            paper_trading_only=paper_trading_only,
            disable_historical_data=disable_historical_data,
            extra_env=extra_env,
            preseed_errors_mb=preseed_errors_mb,
            mcp_http_enabled=mcp_http_enabled,
        )
        started.append(instance)
        return instance

    try:
        yield _factory
    finally:
        errors: list[AssertionError] = []
        for inst in reversed(started):
            inst.stop()
            try:
                inst.assert_zero_real_broker_order_calls()
            except AssertionError as exc:
                errors.append(exc)
        if errors:
            raise errors[0]


@pytest.fixture(scope="module")
def cloud_server(tmp_path_factory: pytest.TempPathFactory) -> OpenAlgoE2EInstance:
    """Module-scoped isolated cloud+paper OpenAlgo server."""
    root = tmp_path_factory.mktemp("openalgo_e2e_cloud")
    inst = _launch_server(root, profile="cloud", paper_trading_only=True)
    try:
        yield inst
    finally:
        inst.stop()
        inst.assert_zero_real_broker_order_calls()


@pytest.fixture(scope="module")
def full_server(tmp_path_factory: pytest.TempPathFactory) -> OpenAlgoE2EInstance:
    """Module-scoped isolated full+paper OpenAlgo server."""
    root = tmp_path_factory.mktemp("openalgo_e2e_full")
    inst = _launch_server(root, profile="full", paper_trading_only=True)
    try:
        yield inst
    finally:
        inst.stop()
        inst.assert_zero_real_broker_order_calls()


@pytest.fixture
def ws_proxy_factory(tmp_path: Path) -> Callable[..., WsProxyE2EInstance]:
    """Factory for isolated local websocket_proxy.server subprocesses."""
    started: list[WsProxyE2EInstance] = []
    counter = 0

    def _factory(
        *,
        server: OpenAlgoE2EInstance | None = None,
        extra_env: dict[str, str] | None = None,
    ) -> WsProxyE2EInstance:
        nonlocal counter
        counter += 1
        inst_root = server.root_dir if server is not None else (tmp_path / f"ws_{counter}")
        instance = _launch_ws_proxy(inst_root, server=server, extra_env=extra_env)
        started.append(instance)
        return instance

    try:
        yield _factory
    finally:
        for inst in reversed(started):
            inst.stop()
            _assert_zero_real_broker_order_calls(inst.outbound_log_path)
