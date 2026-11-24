# E2E Test Infrastructure & 4-Tier Plan (`TEST_INFRA.md`)

**Project:** `AyushMehta292/openalgo-p1` (Multi-Broker Paper Trading Platform: Delta Exchange, Fyers, Groww)  
**Track:** Independent E2E Testing Track (`e2e_orch`, Feature F17)  
**Scope:** Requirement-driven, opaque-box, 4-tier E2E test suite under `test/e2e/`

---

## 1. Test Philosophy & Methodology

1. **Strict Opaque-Box Discipline:** Every test exercises the system exclusively via external observable boundaries:
   - HTTP REST APIs (`/api/v1/*`), public config (`/auth/app-config`), and session-authenticated routes (`/auth/*`, `/settings/*`, `/strategy/*`, `/python/*`, `/openscript/runner/*`, `/historify/*`, `/scalping/*`, `/health/*`, SPA routes).
   - WebSocket Proxy wire protocol (`127.0.0.1:<ephemeral_port>`, JSON frames per `websocket_proxy/server.py`).
   - CLI / Strategy subprocess execution (`strategies/live/*`, `openscript_host/openscript_runner.py`, `scripts/monitor_production.py`).
   - External filesystem & SQLite/DuckDB observables (`db/brokers/<broker>/*.db`, absence of `*.duckdb` in cloud, `errors.jsonl` rotation, `health.db` retention/payload bounds, atomic `.state.json` `< 10 KB`).
   - Process-level configuration flags (`OPENALGO_PROFILE=cloud|full`, `DISABLE_HISTORICAL_DATA`, `PAPER_TRADING_ONLY=true|false`).
   - **Zero implementation coupling:** Tests NEVER import private helpers to bypass public boundaries or assert internal call order.
2. **Absolute Safety (Zero Real-Money Risk):**
   - Every test runs against isolated `tmp_path` directories (`db/`, `log/`, `strategies/`, `mock_quotes.json`) — never touching repo `db/` or `log/`.
   - An outbound **Broker Network Firewall** (`test/e2e/_firewall.py`) intercepts ALL `httpx`, `requests.Session.send`, `urllib3.connectionpool.HTTPConnectionPool.urlopen`, `http.client`/`urllib`, `websocket.WebSocketApp`, and `websocket.create_connection` traffic to external broker hosts (`api.india.delta.exchange`, `api-t1.fyers.in`, `public.fyers.in`, `api.groww.in`, `growwapi-assets.groww.in`, `ws(s)://*`), serves deterministic local mock market quotes, records every outbound call to `outbound_broker_calls.jsonl`, and **hard-blocks & fails** any outbound live order/GTT/leverage/position-write HTTP request.
3. **4-Tier Testing Taxonomy:**
   - **Tier 1 (`@pytest.mark.tier1`) — Feature Coverage:** `>= 5` isolated happy-path tests per feature (11 features → `>= 55` tests).
   - **Tier 2 (`@pytest.mark.tier2`) — Boundary & Corner Cases (Category-Partition + BVA):** `>= 5` boundary/negative/error/concurrency tests per feature (`>= 55` tests).
   - **Tier 3 (`@pytest.mark.tier3`) — Cross-Feature Pairwise Combinations:** Systematic pairwise tests across profiles (`cloud`/`full`), safety locks (`PAPER_TRADING_ONLY`), brokers (`deltaexchange`/`fyers`/`groww`), WS proxy, sandbox, and strategies (`>= 20` tests).
   - **Tier 4 (`@pytest.mark.tier4`) — Real-World Application Scenarios:** `>= max(5, ceil(11/2))` = `>= 6` full multi-step end-to-end workloads (7 scenarios designed below).

---

## 2. Feature Inventory & Coverage Matrix (`N = 11` User-Facing Features)

Mapped 1:1 from `ORIGINAL_REQUEST.md` (including Q1–Q9 and historical-vs-live clarification) and `PROJECT.md` (F1–F16, F20):

| ID | Feature | `PROJECT.md` Ref | Core Observable Contract | Target Test File(s) |
|---|---|---|---|---|
| **E2E-F1** | Paper-Trading Hard Lock (`PAPER_TRADING_ONLY=true`) | F6, Q4 | All 5 toggle surfaces (`/api/v1/analyzer/toggle`, `/auth/analyzer-toggle`, `/settings/analyze-mode/0`, `/strategy/api/strategies/<id>/live`, `/mcp` `analyzer_toggle`) + Strategy Module `mode="live"` return `403 {"status":"error","code":"PAPER_TRADING_ONLY"}`; analyzer mode stays `analyze`; ZERO real-broker order calls leave process. | `test_tier1_safety_profiles.py`, `test_tier2_safety_profiles.py` |
| **E2E-F2** | Cloud Zero-Historical-Storage & Backtesting Lockout (`OPENALGO_PROFILE=cloud`) | F7, User Clarif. | Zero `*.duckdb` files created on disk; `403`/`503` `HISTORICAL_DATA_DISABLED` on `/api/v1/history`, `/api/v1/ticker/<sym>`, `/api/v1/sectorrotation*`, `/api/v1/portfolio/*`, `/api/v1/sip/*`, `/historify/*`, `/chart/test/api/history`, `/straddle*`, `/straddlepnl*`, `/ivchart*`, `/oiprofile*`, `/strategybuilder*`, `/scalping/api/history`, `/pnltracker*`, MCP history/indicator tools. | `test_tier1_safety_profiles.py`, `test_tier2_safety_profiles.py` |
| **E2E-F3** | Cloud Live-Trading Data Unhampered | F7, Q8, User Clarif. | Under `OPENALGO_PROFILE=cloud`, `POST /api/v1/{quotes,multiquotes,depth,optionchain,intervals,ping}`, all paper order/book/funds/position APIs, `GET /trading`, `GET /scalping`, and non-history `/scalping/api/*` remain 100% operational with zero DuckDB writes. | `test_tier1_safety_profiles.py`, `test_tier2_safety_profiles.py` |
| **E2E-F4** | Local Mode Full Parity (`OPENALGO_PROFILE=full`) | F8 | Under `OPENALGO_PROFILE=full`, `/auth/app-config` returns `profile="full"`, `historical_data_enabled=true`; `/api/v1/history`, `/api/v1/ticker/<sym>`, `/api/v1/portfolio/*`, `/api/v1/sip/*`, `/api/v1/sectorrotation*`, `/historify/*`, `/chart/test/api/history`, `/scalping/api/history`, and DuckDB storage work normally. | `test_tier1_safety_profiles.py`, `test_tier2_safety_profiles.py` |
| **E2E-F5** | Frontend Runtime Config & Profile/Paper UI Contract | F9, Q8 | Public unauthenticated `GET /auth/app-config` returns `{status, profile, historical_data_enabled, paper_trading_only, brokers}` with no secrets; cloud returns `503 HISTORICAL_DATA_DISABLED` on all history/backtest SPA routes while keeping `/trading`, `/scalping`, `/dashboard`, `/sandbox`, `/positions`, `/orderbook` at `200`; SPA HTML injects `X-OpenAlgo-Broker` hook. | `test_tier1_safety_profiles.py`, `test_tier2_safety_profiles.py` |
| **E2E-F6** | Multi-Broker Isolation & Concurrent Routing (`deltaexchange`, `fyers`, `groww`) | F10, F14, Q9 | Per-broker isolated SQLite DBs under `db/brokers/<broker>/`; explicit broker precedence (`/<broker>/...` > `?broker=` > `X-OpenAlgo-Broker` > shared API key default); `/python/*` per-broker `(broker, strategy_id)` isolation + `OPENALGO_BROKER` / `X-OpenAlgo-Broker` propagation; Delta (`CRYPTO`, `DELTA_BASE_URL`, `multiquotes`), Fyers (`NSE`/`NFO`/`MCX`), Groww (`NSE`/`NFO`) symbol/exchange contracts. | `test_tier1_multibroker_ops.py`, `test_tier2_multibroker_ops.py` |
| **E2E-F7** | Multi-Broker WebSocket Proxy & Streaming (`:8765`) | F11, Q5 | Ephemeral `websocket_proxy.server` JSON frames: `authenticate` with `"broker"`, `subscribe`/`unsubscribe` (`1|2|3` & `LTP|Quote|Depth`), `ping`/`pong`, `subscribe_orders`; concurrent `(user_id, broker)` connections across Delta, Fyers, Groww without evicting sibling brokers; tick/order-update fan-out tagged with `"broker"`. | `test_tier1_multibroker_ops.py`, `test_tier2_multibroker_ops.py` |
| **E2E-F8** | Multi-Broker Order Updates & Sandbox Execution / Square-Off | F12, F13, Q9 | Per-broker paper `placeorder` (MARKET/LIMIT/SL/SL-M), `modifyorder`, `cancelorder`, `placegttorder` (SINGLE/OCO), `orderbook`, `tradebook`, `positionbook`, `funds`; concurrent per-broker LIMIT/SL fill execution & MIS square-off across Delta/Fyers/Groww without singleton clobbering; per-broker order-update feeds without eviction. | `test_tier1_multibroker_ops.py`, `test_tier2_multibroker_ops.py` |
| **E2E-F9** | Zero-History Live-Bar Strategies & OpenScript Cloud Degradation | F15, F16, Q1, Q2, Q8 | `strategies/live/` runs without `pandas`/`numpy`, aggregates live OHLCV bars from `/api/v1/quotes`/WS with `X-OpenAlgo-Broker`, persists atomic `.state.json` `< 10 KB` (bounded rolling bars), makes ZERO `/api/v1/history` calls in cloud, supports `--backtest` in `OPENALGO_PROFILE=full`; OpenScript runner gracefully transitions to live-bar warmup on `403`/`503` without crashing or stalling. | `test_tier1_multibroker_ops.py`, `test_tier2_multibroker_ops.py` |
| **E2E-F10** | Runtime Stability & Bounded Storage Hygiene | F2, F3, F4, Q3 | Concurrent multi-broker API bursts produce ZERO `BrokerContextError` lines in `errors.jsonl` and ZERO HTTP 500s; `errors.jsonl` uses tail-bounded startup read + `RotatingFileHandler` (`10 MB × 3`); `health.db` enforces 2-day retention pruning, incremental vacuum compaction, and bounded `thread_details` payloads. | `test_tier1_multibroker_ops.py`, `test_tier2_multibroker_ops.py` |
| **E2E-F11** | Production 3-Hour Monitoring Contract | F20, Q6, Q7 | `scripts/monitor_production.py` outputs valid JSONL matching Interface Contract #6 (`timestamp_utc`, `check_number`, `overall_status`, `https`, `services`, `resources`, `brokers`, `strategies`, `errors_last_3h`, `alerts`), classifying healthy=`OK`, expired Fyers/Groww OAuth tokens=`WARN`, safety/service/disk/history/`BrokerContextError` breaches=`CRITICAL`. | `test_tier1_multibroker_ops.py`, `test_tier2_multibroker_ops.py` |

---

## 3. Test Architecture & Harness Contract (`test/e2e/`)

### 3.1 Runner Commands & Pytest Markers
Markers `tier1`, `tier2`, `tier3`, `tier4` are registered dynamically in `test/e2e/conftest.py` (`pytest_configure`) — no `pyproject.toml` edits required.

```bash
# Full E2E suite
.venv/bin/pytest test/e2e

# Per-tier execution
.venv/bin/pytest test/e2e -m tier1
.venv/bin/pytest test/e2e -m tier2
.venv/bin/pytest test/e2e -m tier3
.venv/bin/pytest test/e2e -m tier4

# Collection / syntax sanity check
.venv/bin/pytest --collect-only test/e2e
```

### 3.2 Module Layout (`test/e2e/`)
- `test/e2e/__init__.py` — package marker.
- `test/e2e/_firewall.py` — outbound broker HTTP/WS firewall (`BrokerNetworkFirewall`), canned quote/depth/optionchain/history responder driven by `<tmp>/mock_quotes.json`, call recorder (`outbound_broker_calls.jsonl`), and `assert_zero_real_broker_order_calls()`.
- `test/e2e/_server_launcher.py` — subprocess launcher for an isolated OpenAlgo Flask server (`OPENALGO_PROFILE=cloud|full`, `PAPER_TRADING_ONLY=true|false`) with isolated `<tmp>/db/` (`PROJECT_ROOT` redirected to `tmp_root` before app init so per-broker `db/brokers/<broker>/*.db` works genuinely), seeded admin user + shared API key + 3-broker `SymToken` catalog, and active `BrokerNetworkFirewall`.
- `test/e2e/conftest.py` — pytest marker registration + reusable opaque-box fixtures and client helpers (exact contract below).
- `test/e2e/test_tier1_safety_profiles.py` — Tier 1 happy-path tests for E2E-F1..F5 (`>= 25` tests).
- `test/e2e/test_tier1_multibroker_ops.py` — Tier 1 happy-path tests for E2E-F6..F11 (`>= 30` tests).
- `test/e2e/test_tier2_safety_profiles.py` — Tier 2 boundary/corner tests for E2E-F1..F5 (`>= 25` tests).
- `test/e2e/test_tier2_multibroker_ops.py` — Tier 2 boundary/corner tests for E2E-F6..F11 (`>= 30` tests).
- `test/e2e/test_tier3_pairwise.py` — Tier 3 cross-feature pairwise combination tests (`>= 20` tests).
- `test/e2e/test_tier4_scenarios.py` — Tier 4 real-world multi-step application workloads (`7` scenarios).

### 3.3 Exact Shared Fixture & Helper API Contract (`test/e2e/conftest.py`)
All Tier 1–4 test modules MUST use ONLY these public harness interfaces:

```python
BROKERS = ("fyers", "deltaexchange", "groww")
DEFAULT_SYMBOLS = {
    "deltaexchange": {"symbol": "BTCUSDFUT", "exchange": "CRYPTO", "product": "NRML", "ltp": 65000.0},
    "fyers": {"symbol": "SBIN", "exchange": "NSE", "product": "CNC", "ltp": 800.0},
    "groww": {"symbol": "SBIN", "exchange": "NSE", "product": "CNC", "ltp": 800.0},
}

class OpenAlgoE2EInstance:
    """Handle to an isolated local OpenAlgo HTTP server subprocess."""
    base_url: str                 # e.g., "http://127.0.0.1:<port>"
    root_dir: Path                # Isolated tmp_path acting as PROJECT_ROOT
    db_dir: Path                  # root_dir / "db"
    log_dir: Path                 # root_dir / "log"
    api_key: str                  # Shared API key seeded across all 3 brokers
    username: str                 # "e2e_admin"
    password: str                 # "E2ePass!2026"
    profile: str                  # "cloud" | "full"
    paper_trading_only: bool
    mock_quotes_path: Path        # JSON map {(broker, exchange, symbol) -> quote dict}
    outbound_log_path: Path       # JSONL of every intercepted outbound broker call

    def api_post(self, path: str, payload: dict | None = None, *, broker: str | None = None,
                 query: dict | None = None, headers: dict | None = None, timeout: float = 15.0) -> httpx.Response: ...
    def api_get(self, path: str, *, broker: str | None = None, query: dict | None = None,
                headers: dict | None = None, timeout: float = 15.0) -> httpx.Response: ...
    def session_client(self, broker: str = "fyers") -> SessionClient: ...
        # Logged-in httpx.Client with session cookie, X-OpenAlgo-Broker, and CSRF helper:
        #   client.get(path, **kw), client.post(path, json=..., data=..., files=..., **kw),
        #   client.delete(path, **kw), client.csrf_token: str
    def set_mock_quote(self, broker: str, exchange: str, symbol: str, *, ltp: float,
                       bid: float | None = None, ask: float | None = None,
                       open_: float | None = None, high: float | None = None,
                       low: float | None = None, prev_close: float | None = None,
                       volume: int = 1000) -> None: ...
    def outbound_calls(self) -> list[dict]: ...
    def assert_zero_real_broker_order_calls(self) -> None: ...
    def duckdb_files(self) -> list[Path]: ...
    def broker_db_path(self, broker: str, filename: str = "openalgo.db") -> Path: ...
    def errors_jsonl_path(self) -> Path: ...
    def read_error_lines(self) -> list[dict]: ...
    def stop(self) -> None: ...

class WsProxyE2EInstance:
    """Handle to an isolated local websocket_proxy.server subprocess."""
    ws_url: str                   # "ws://127.0.0.1:<ws_port>"
    ws_port: int
    zmq_port: int
    stats_file: Path
    root_dir: Path
    def publish_tick(self, exchange: str, symbol: str, mode: str, payload: dict) -> None: ...
    def publish_order_update(self, topic_prefix: str, user_id: str, payload: dict) -> None: ...
    def stop(self) -> None: ...

# Fixtures provided by test/e2e/conftest.py:
# - server_factory: Callable[..., OpenAlgoE2EInstance]
#     kwargs: profile="cloud", paper_trading_only=True, disable_historical_data=None,
#             extra_env=None, preseed_errors_mb=0.0, mcp_http_enabled=False
#     (automatically stops every started instance and asserts zero real-broker order calls on teardown)
# - cloud_server: OpenAlgoE2EInstance  (module-scoped or function-scoped isolated cloud+paper server)
# - full_server: OpenAlgoE2EInstance   (isolated full+paper local server)
# - ws_proxy_factory: Callable[..., WsProxyE2EInstance]
#     kwargs: server: OpenAlgoE2EInstance | None = None, extra_env: dict | None = None
```

---

## 4. Tier 4 Real-World Application Scenarios (7 End-to-End Workloads)

1. **T4-S1 — Full Simulated Multi-Broker Cloud Paper Trading Day (Delta `CRYPTO` + Fyers `NSE` + Groww `NSE`):**
   Boot `OPENALGO_PROFILE=cloud` + `PAPER_TRADING_ONLY=true`; verify `/auth/app-config`; place concurrent MARKET, LIMIT, and SL paper orders across all 3 brokers; drive simulated live quote movements to fill LIMIT/SL orders and compute realized/unrealized PnL; verify strict per-broker DB isolation, zero `*.duckdb` files, zero `BrokerContextError`s, zero real-broker order HTTP calls, and bounded disk footprint.
2. **T4-S2 — Live-Bar 5-EMA / 20-EMA Paper Strategy Lifecycle Under Cloud Lockout:**
   Run the tracked `strategies/live/` engine against a `cloud` server with `/api/v1/history` hard-blocked (`403`); feed deterministic rising/reversing live quote sequences across multiple bar intervals; verify atomic `.state.json` creation (`< 10 KB`, rolling cap enforced), live paper entry/exit order placement via `X-OpenAlgo-Broker`, restart state recovery, and zero `/api/v1/history` requests.
3. **T4-S3 — Full Local Mode (`OPENALGO_PROFILE=full`) Historical Download & Strategy Backtest Workflow:**
   Boot `OPENALGO_PROFILE=full`; verify `/auth/app-config` enables history; fetch historical candles via `/api/v1/history` & `/api/v1/ticker/<sym>`, exercise Historify/portfolio/SIP backtest endpoints, and execute the `strategies/live/` `--backtest` CLI workflow end-to-end while still keeping `PAPER_TRADING_ONLY=true` enforced.
4. **T4-S4 — Concurrent 3-Broker WebSocket Proxy Streaming + Order Update & Sandbox Fill Integration:**
   Boot isolated `OpenAlgoE2EInstance` + `WsProxyE2EInstance`; authenticate 3 concurrent WebSocket clients (`deltaexchange`, `fyers`, `groww`) with shared API key + explicit `"broker"`; subscribe to LTP/Quote/Depth & order updates; verify no broker evicts another, market ticks route with matching `"broker"`, and sandbox order lifecycle updates arrive cleanly.
5. **T4-S5 — Security & Safety Adversarial Assault Under Cloud Deployment:**
   Attempt live-mode escalation across EVERY surface (RESTX analyzer toggle, SPA analyzer toggle, Settings `mode=0`, Strategy Module `/live` & `/start` `mode="live"`, MCP `analyzer_toggle`) AND attempt historical/backtest bypass across `/api/v1/history`, `/api/v1/ticker`, `/scalping/api/history`, `/historify`, `/pnltracker/test_chart`; verify 100% rejection (`403`/`503`), unchanged `analyze_mode=True`, zero DuckDB files, and zero outbound real-broker orders.
6. **T4-S6 — High-Concurrency Multi-Broker Load + Storage Hygiene & Crash-Restart Recovery:**
   Fire concurrent multi-broker requests across Delta/Fyers/Groww (quotes, intervals, paper orders, books), inject oversized pre-existing `errors.jsonl` and aged `health.db` (>2 days old) records, restart server, verify fast non-OOM boot, `errors.jsonl` rotation cap (`<= 10 MB × 3`), `health.db` 2-day pruning & bounded `thread_details`, zero `BrokerContextError` storm, and preserved sandbox positions across restart.
7. **T4-S7 — Production 3-Hour Monitoring Probe End-to-End (`OK` -> `WARN` -> `CRITICAL` Escalation):**
   Run `scripts/monitor_production.py` across 3 sequential probe scenarios against isolated local targets: (1) all healthy → `overall_status="OK"`, (2) Fyers & Groww OAuth tokens expired while Delta paper active → `overall_status="WARN"` (non-critical per Q7), (3) history-block breach or paper-lock breach or disk critical → `overall_status="CRITICAL"` with severity-tagged alerts and valid 10-field JSONL records appended sequentially (`check_number` incrementing).
