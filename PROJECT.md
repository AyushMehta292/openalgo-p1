# Project: openalgo-p1 (Multi-Broker Paper Trading Platform for Delta Exchange, Fyers & Groww)

**Repository:** `AyushMehta292/openalgo-p1` (`/home/antigravity/AyushMehta292/openalgo-p1`)  
**Production Host:** `https://algo.alphafoxservices.com` (GCE `openalgo-p1-instance-20261115-081241`, `asia-south1-a`, `e2-micro` 952 MiB RAM + 2 GiB swap, 19 GB disk)  
**Baseline Commit:** `d7026a2` (forked from OpenAlgo v2.0.3.3 `a1b9e32`, PRs `#11`–`#24`)  
**Last Updated:** 2026-11-24

---

## Architecture

### System Overview
`openalgo-p1` is a concurrent 3-broker (`fyers`, `deltaexchange`, `groww`) algorithmic paper-trading platform built on Flask 3.1 + Flask-RESTX + Flask-SocketIO + SQLAlchemy 2.0 (SQLite per broker) + Gunicorn (`-w 1 --worker-class gthread --threads 64`) + WebSocket Proxy (`127.0.0.1:8765`) behind Caddy (`algo.alphafoxservices.com`), with a React 19 + TypeScript + Vite SPA frontend (`frontend/`).

### Key Architectural Invariants
1. **Per-Broker Isolation (`utils/broker_context.py`)**:
   - `BROKERS = ("fyers", "deltaexchange", "groww")`.
   - Shared admin/calendar/qty-freeze tables live in `db/shared/openalgo.db`.
   - Broker-specific state (`openalgo.db`, `sandbox.db`, `logs.db`, `latency.db`, `health.db`, `missed_exit.db`, `strategies/`, and local-only `historify.duckdb`) lives under `db/brokers/<broker>/`.
   - Every HTTP request, WebSocket session, background `ThreadPoolExecutor` job, `EventBus` subscriber, order-update adapter, and sandbox worker MUST propagate `broker_context(broker)`.
2. **Strict Paper-Trading-Only Enforcement (`PAPER_TRADING_ONLY=true`)**:
   - Real-money order execution is impossible across all 6 layers (config lock -> `settings_db` default & override -> toggle/strategy-live endpoint lockout -> order service & Strategy Module `force_live` block -> broker `order_api`/`gtt_api` nuclear kill-switch -> UI `PAPER ONLY (LOCKED)` indicator).
3. **Deployment vs. Local Historical Data & Storage Boundary (`OPENALGO_PROFILE=cloud` vs. `full`) — User Clarification Applied**:
   - **Core Rationale (User):** *"whatever data is required for live trading, it should be there. I asked not to load historic data because it may make the storage full on production"*.
   - **Deployment (`OPENALGO_PROFILE=cloud` or `DISABLE_HISTORICAL_DATA=true`)**:
     - **ZERO persistent historical data storage & ZERO backtesting / bulk historical loading:** `historify.duckdb` (`database/historify_db.py`) is hard-blocked (zero disk writes/reads); Historify, Portfolio Backtester, SIP Backtester, Sector Rotation, chart/analytics history blueprints (`/chart/test`, `/straddle*`, `/ivchart`, `/oiprofile`, `/strategybuilder`, `/scalping/api/history`, `/pnltracker/*` history), `POST /api/v1/history`, `GET /api/v1/ticker/<symbol>`, broker `get_history()`, boot-time `downtime_resume` bar fetches, Agent/MCP historical/backtest tools, and frontend Backtest/Historify UI pages are ALL disabled/blocked (`403`/`503`).
     - **Live-trading data is NEVER hampered:** live quotes (`get_quotes`, `get_multiquotes`, `depth`, `optionchain`), WebSocket live ticks (`:8765`), read-only static timeframe metadata (`/api/v1/intervals`, zero disk/history fetch), and live-bar aggregation with strictly fixed-cap rolling state (`<10 KB` per strategy) remain 100% available.
     - **Strict bounded disk budgets everywhere (Q3):** `errors.jsonl` rotated (`10 MB × 3`), `health.db` capped at 2 days + incremental vacuum + light payload, `journald` `SystemMaxUse=200M`, `syslog` `maxsize 100M`, atomic backups retained 7 days.
   - **Local (`OPENALGO_PROFILE=full`, default locally)**: 100% of historical data, Historify, DuckDB, charts, indicators, and backtesters remain intact and functional.
4. **Live-Bar Trading Strategies (Q1 & Q2 Applied)**:
   - Legacy production test strategies (`5ema_20261115225640.py`, `5ema.oscript`) and old paper positions may be stopped and cleaned up cleanly on deployment.
   - New tracked, lightweight, storage-safe strategy library (`strategies/live/`) aggregates live candles from live quotes/WebSocket ticks with strictly bounded rolling state (`<10 KB`) — requiring ZERO historical API calls or storage growth in cloud deployment while supporting local backtesting when `OPENALGO_PROFILE=full`.

---

## Code Layout

- `app.py` — Flask app factory, middleware (`bind_tab_broker`/`release_tab_broker`), blueprint registration, background initialization.
- `utils/` — Runtime helpers: `broker_context.py` (ContextVar + `BrokerContextThreadPoolExecutor`), `profile.py` (`OPENALGO_PROFILE`, `is_historical_data_disabled`, `is_paper_trading_only`), `tab_session.py`, `daily_token_check.py`, `latency_monitor.py`, `logging.py`, `event_bus.py`, `traffic_logger.py`, `thread_safe_cache.py` (`LockedTTLCache` mandatory; bare `cachetools.TTLCache` fails CI), `mcp_tool_registry.py`.
- `database/` — Per-broker SQLAlchemy DBs (`auth_db.py`, `settings_db.py`, `sandbox_db.py`, `latency_db.py`, `apilog_db.py`, `analyzer_db.py`, `health_db.py`, `strategy_module_db.py`) + local-only DuckDB (`historify_db.py`).
- `services/` — Core business logic: 13 order/GTT services, `history_service.py`, `intervals_service.py`, `analyzer_service.py`, `downtime_resume.py`, `order_update_service.py`, `websocket_service.py`, `market_data_service.py`, `indicator_service.py`, `flow_openalgo_client.py`, `telegram_bot_service.py`, `strategy_module/` (`order_dispatch.py`, `tick_feed.py`, etc.), `agent/tools/`.
- `restx_api/` — `/api/v1/*` Flask-RESTX namespaces (`__init__.py`, `history.py`, `ticker.py`, `intervals.py`, `sector_rotation.py`, `portfolio.py`, `sip.py`, `analyzer.py`, `strategy.py`, order/quote/position namespaces).
- `blueprints/` — Session-authenticated routes (`react_app.py`, `auth.py`, `settings.py`, `python_strategy.py`, `strategy_module.py`, `openscript_runner.py`, `historify.py`, `chart_test.py`, `scalping.py`, `strategy_chart.py`, `straddle_chart.py`, `custom_straddle.py`, `ivchart.py`, `oiprofile.py`, `pnltracker.py`, `missed_exit.py`, `mcp_http.py`, `playground.py`).
- `sandbox/` — Paper-trading execution engine (`websocket_execution_engine.py`, `execution_thread.py`, `execution_engine.py`, `order_manager.py`, `position_manager.py`, `fund_manager.py`, `squareoff_thread.py`, `squareoff_manager.py`, `gtt_manager.py`).
- `websocket_proxy/` — Standalone WebSocket market-data & order-update proxy (`server.py`, `connection_manager.py`, `order_adapter.py`) on `127.0.0.1:8765`.
- `broker/{deltaexchange,fyers,groww}/` — Target broker plugins (`api/{auth_api,data,funds,order_api,gtt_api,baseurl,rate_limiter}.py`, `database/master_contract_db.py`, `streaming/`, `mapping/`).
- `strategies/live/` — Tracked, zero-historical-data live-bar paper-trading strategies (5-EMA / 20-EMA Reversal engine + Delta Exchange, Fyers, Groww runners + local backtest harness).
- `mcp/` — Stdio & in-process HTTP MCP server (`mcpserver.py`).
- `frontend/` — React 19 + TypeScript + Vite SPA (`src/App.tsx`, `src/config/navigation.ts`, `src/lib/tools.ts`, `src/pages/*`, `src/components/*`, compiled into `dist/`).
- `scripts/` & `install/` — Setup, deployment (`deploy_gcp.sh`), production monitoring (`monitor_production.py`, `openalgo-monitor-3h.{service,timer}`), atomic DB backup, and Gunicorn scripts.
- `test/` — Backend `pytest` unit/integration tests + `test/e2e/` (opaque-box E2E suite owned by E2E Testing Track) + `frontend/src/**/*.test.*` & `frontend/e2e/`.

---

## Feature Inventory

Every feature discovered across Step 0 Survey (`explorer_survey_1`–`4`) and `ORIGINAL_REQUEST.md` (including Q1–Q9 user answers) is assigned to a milestone below (zero unassigned):

| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| F1 | Local Dev & Test Environment Setup | Create `.venv` via `uv sync` + `gunicorn`, install `frontend/node_modules` via `npm ci`, add repeatable setup script (`scripts/setup_dev.sh`). | M1 | survey_3, user |
| F2 | Fix `BrokerContextError` Background Thread Storm | Add `BrokerContextThreadPoolExecutor` & `_BROKER_SCOPED_SESSIONS` in `utils/broker_context.py`; propagate broker context in `utils/latency_monitor.py`, `database/apilog_db.py`, `database/analyzer_db.py`, `utils/event_bus.py` (+ audit remaining pools). | M1 | survey_3, survey_4 |
| F3 | Fix `errors.jsonl` Startup OOM Trap & Log Rotation | Replace full-file `errors_file.read_text().splitlines()` in `utils/logging.py:517` with tail-bounded read + `RotatingFileHandler` (10 MB × 3). | M1 | survey_3, Q3 |
| F4 | Fix `health.db` Unbounded Growth (1.25 GB Bloat) | Schedule/enforce retention pruning (2 days) + incremental vacuum + bound/trim oversized `thread_details` payload in `database/health_db.py` (strict storage bounds per Q3). | M1 | survey_3, Q3 |
| F5 | Repair Broken Backend CI Suite | Fix failing backend CI tests (`test_broker_keys.py`, `test_broker_callback_logging.py`, `gunicorn-boot`, `gthread-migration`, `eventlet-fallback`) caused by multi-broker/paper-default fork shift so CI is GREEN. | M1 | survey_1, survey_3 |
| F6 | Strict Paper-Trading-Only Lock (`PAPER_TRADING_ONLY`) | 6-layer enforcement (Q4): `utils/profile.py` flag (default `True` in cloud & local `.sample.env`), fix 4 `Settings(analyze_mode=False)` bugs in `database/settings_db.py`, hard override in `get_analyze_mode`/`set_analyze_mode`, block all 5 toggle-to-live surfaces (UI/REST/Settings/Telegram/MCP) + Strategy Module `/live` & `force_live=True` & direct `cancel_order`, broker-layer kill-switch in `broker/{deltaexchange,fyers,groww}/api/order_api.py` & `broker/fyers/api/gtt_api.py`. | M2 | survey_2, user, Q4 |
| F7 | Zero Historical Storage & Backtesting in Deployment (`OPENALGO_PROFILE=cloud`) | 5-layer lockout without hampering live trading (Q8 + user clarification): `is_historical_data_disabled()` helper; hard floor in `database/historify_db.py` (zero DuckDB disk creation/access), `services/history_service.py`, `broker/{deltaexchange,fyers,groww}/api/data.py:get_history`; gate RESTX (`history_ns`, `ticker_ns` direct bypass, `sector_rotation_ns`, `portfolio_ns`, `sip_ns`) while keeping static read-only `/api/v1/intervals` & live quotes/WS open; gate history blueprints (`historify_bp`, `chart_test_bp`, `straddle_bp`, `custom_straddle_bp`, `ivchart_bp`, `oiprofile_bp`, `strategy_chart_bp`, `/scalping/api/history`, `/pnltracker/*`), `downtime_resume.py` boot-time leak (`gap_unjudged`), `indicator_service.py`, `flow_openalgo_client.py`, Agent tools (5 files), MCP tools. | M2 | survey_1, survey_2, survey_3, user, Q8 |
| F8 | Local Mode Full Parity (`OPENALGO_PROFILE=full`) | Guarantee 100% of historical data, Historify/DuckDB, charts, indicators, and backtesting features remain fully intact and verified when running locally. | M2 | survey_2, user |
| F9 | Frontend Profile & Paper-Lock UI (`web-app-development`) | Expose `/auth/app-config` (`{profile, historical_data_enabled, paper_trading_only, brokers}`); guard SPA routes in `react_app.py` & `App.tsx`; keep live order/quote panels working on `/trading` & `/scalping` while disabling/hiding their historical/backtest panels and hiding pure history/backtest pages (`navigation.ts`, `tools.ts`, `Playground.tsx` `/history`); display `PAPER ONLY (LOCKED)` badge & lock Live toggles; rebuild `frontend/dist/`. | M2 | survey_2, user, Q8 |
| F10 | Multi-Broker `/api/v1/*` Routing & Python Strategy Broker Binding | Keep shared API key backwards-compatible (Q9) with automatic `X-OpenAlgo-Broker` injection for `/python` subprocesses + explicit `?broker=`/`X-OpenAlgo-Broker` precedence (`utils/broker_context.py`, `app.py`, `database/auth_db.py`); per-broker `(broker, strategy_id)` keying for `RUNNING_STRATEGIES`/`STOPPING_STRATEGIES`/`DELETING_STRATEGIES` in `blueprints/python_strategy.py`. | M3 | survey_1, Q9 |
| F11 | Multi-Broker WebSocket Proxy & Streaming (`:8765`) | Refactor `websocket_proxy/server.py`, `services/websocket_service.py`, `services/websocket_client.py`, `services/market_data_service.py`, and `RiskTickFeed` (`services/strategy_module/tick_feed.py`) to accept `broker` in WS auth, enter `broker_context(broker)`, and key adapters by `(user_id, broker)`. | M3 | survey_1, Q5 |
| F12 | Multi-Broker Order Update Feeds | Fix `services/order_update_service.py` to key `_ADAPTERS` by `(user_id, broker)` and stop evicting Fyers on Delta boot; wrap `PollingOrderUpdateAdapter._poll_loop` (Groww) in `broker_context(self.broker_name)`; harden Delta order-update integration. | M3 | survey_1 |
| F13 | Multi-Broker Sandbox Execution & Square-Off | Refactor `sandbox/websocket_execution_engine.py`, `sandbox/execution_thread.py`, and `sandbox/squareoff_thread.py` from global singletons to per-broker engines/schedulers so Delta Exchange, Fyers, and Groww all execute pending LIMIT/SL/GTT orders and square-off/settlement concurrently (keeping existing sandbox DB schema per Q9). | M3 | survey_2, Q9 |
| F14 | Targeted 3-Broker Hardening (Delta, Fyers, Groww) | Delta: `certifi` in `delta_websocket.py:332`, honor `DELTA_BASE_URL`/`DELTA_WS_URL` in `master_contract_db.py` & `delta_websocket.py`, add `get_multiquotes` helper; Fyers: fix `CERT_NONE` in `fyers_hsm_websocket.py:923`; Groww: add `_safe_emit` in `master_contract_db.py:921,931`, remove duplicate `get_holdings` in `order_api.py:565`. | M3 | survey_1 |
| F15 | Zero-History Live-Bar Paper Strategy Library (`strategies/live/`) | Build clean, lightweight (no `pandas`/`numpy` bloat), storage-safe tracked strategy library (`strategies/live/`) aggregating live OHLCV candles from live quotes/WS ticks with fixed-cap rolling state (`<10 KB` `.state.json`), implementing clean reference paper strategies (5-EMA / 20-EMA Reversal + configurable framework) for Delta Exchange (`CRYPTO`), Fyers (`NSE`/`NFO`/`MCX`), and Groww (`NSE`/`NFO`), plus local-only backtest mode when `OPENALGO_PROFILE=full`. | M4 | survey_1, survey_4, user, Q1, Q2 |
| F16 | OpenScript Live-Bar Graceful Degradation | Ensure `openscript_host/openscript_runner.py` handles cloud no-history mode gracefully (live-bar warmup without crashing on 403/503, never hampering live execution). | M4 | survey_2, survey_4, Q8 |
| F17 | Independent Opaque-Box E2E Test Suite (Tiers 1–4) | Independent E2E Testing Track designs & builds Category-Partition, BVA, Pairwise, and Real-World Workload E2E tests covering F1–F16 across cloud + local modes and all 3 brokers (`TEST_INFRA.md` -> `TEST_READY.md`). | E2E Track | user, pattern |
| F18 | Final Verification: 100% E2E Pass (Tiers 1–4) + Tier 5 Adversarial Hardening | Dedicated test runners execute Tiers 1–4, dedicated fixers resolve every failure, then white-box Challengers probe for live-order bypasses, history storage/API leaks, cross-broker races, and edge cases until zero gaps remain. | M5 | user, pattern |
| F19 | Production GCE VM Hygiene, Safe Deploy & Live Verification | Stop & remove legacy test strategies (Q1/Q2), atomic SQLite backup, pre-restart `errors.jsonl` truncation (OOM prevention), `journald`/`syslog`/`health.db` cleanup (~7 GB reclaimed + strict caps per Q3), `.env` hardening (`PAPER_TRADING_ONLY=TRUE`, `OPENALGO_PROFILE=cloud`, `CORS_ALLOWED_ORIGINS`), zero-clobber deploy (`scripts/deploy_gcp.sh`), enable multi-broker WS proxy on `:8765` fixing `/ws` 502 (Q5), atomic daily backup cron, notify user beforehand via Sentinel for Fyers/Groww logins when needed (Q7), live verification. | M6 | survey_3, survey_4, Q1–Q9 |
| F20 | 3-Hour Interval Monitoring for 2 Weeks (112 Checks) | Hybrid monitoring (Q6/Q7): upgraded on-VM `scripts/monitor_production.py` + `openalgo-monitor-3h.{service,timer}` (`00/3:00:00`, `Persistent=true`) + external black-box GitHub Actions `0 */3 * * *` monitor opening/updating GitHub Issue alerts on CRITICAL failures (and `WARN` for expired Fyers/Groww daily logins), capped for 14-day window. | M6 | survey_3, survey_4, Q6, Q7 |

---

## Milestones

| # | Name | Scope | Dependencies | Status |
|---|------|-------|--------------|--------|
| M1 | Dev Env, Runtime Storm & Hygiene | F1–F5: Local `.venv` + `node_modules` setup, `BrokerContextThreadPoolExecutor` storm fix, `errors.jsonl` OOM/rotation fix, `health.db` retention/bloat fix, CI backend test repair | none | IN_PROGRESS (`sub_orch_m1_r4`: `f3a7776f9ff1ad0fc3b9c8019910bd5c`) |
| M2 | Paper-Only Lock & Cloud No-History Lockout | F6–F9: 6-layer `PAPER_TRADING_ONLY`, 5-layer `OPENALGO_PROFILE=cloud` historical/storage lockout (keeping live trading & `/api/v1/intervals` unhampered), 100% `full` local parity, frontend `/auth/app-config` + UI guards + `frontend/dist` rebuild | M1 | PLANNED |
| M3 | Multi-Broker Core, WS Proxy, Sandbox & Broker Hardening | F10–F14: Multi-broker `/api/v1` routing, `python_strategy` per-broker state, multi-broker `websocket_proxy` (`:8765`) & `RiskTickFeed`, multi-broker `order_update_service`, per-broker Sandbox WS/squareoff, Delta/Fyers/Groww targeted fixes | M2 | PLANNED |
| M4 | Live-Only Multi-Broker Trading Strategies | F15–F16: Zero-history storage-safe live-bar strategy engine (`strategies/live/`) + Delta Exchange, Fyers, Groww reference paper strategies + local backtest mode + OpenScript cloud graceful warmup | M2, M3 | PLANNED |
| E2E | E2E Testing Track (Parallel) | F17: Independent opaque-box E2E test infrastructure (`TEST_INFRA.md`) + 4-tier test suite (`test/e2e/`) -> `TEST_READY.md` | Parallel with M1–M4 | DONE (`e2e_orch_r4` Wave 3 Gate PASS; `TEST_READY.md` published: 161 tests = 66 T1 + 66 T2 + 22 T3 + 7 T4, Auditor CLEAN; awaiting M1 branch commit for git push) |
| M5 | Final Milestone: E2E Pass (Tiers 1–4) + Tier 5 Adversarial | F18: Phase 1 sequential Tiers 1–4 100% pass; Phase 2 white-box Tier 5 Adversarial Challenger -> Worker -> Reviewer loop until zero gaps | M4, E2E (`TEST_READY.md`) | PLANNED |
| M6 | Production GCE Deploy, Live Verification & 3h/2w Monitoring | F19–F20: Stop legacy test strategies, VM disk/OOM hygiene, atomic backups, zero-clobber deploy to `algo.alphafoxservices.com`, `/ws` 502 fix, live safety verification (requesting Fyers/Groww logins beforehand per Q7), 3-hour / 2-week hybrid monitoring | M5 | PLANNED |

---

## Interface Contracts

### 1. Runtime Profile & Safety Policy (`utils/profile.py`)
```python
class HistoricalDataDisabledError(RuntimeError): ...
class PaperTradingOnlyError(RuntimeError): ...

def get_openalgo_profile() -> str: ...              # "full" (default) | "cloud"
def is_cloud_profile() -> bool: ...                 # True iff OPENALGO_PROFILE == "cloud"
def is_historical_data_disabled() -> bool: ...      # True if cloud OR DISABLE_HISTORICAL_DATA in ("1","true","yes","on")
def is_paper_trading_only() -> bool: ...            # True unless explicitly set to "false"/"0"/"no"/"off" in non-cloud, AND fail-safe True in cloud & .sample.env
def require_historical_data_enabled(feature: str = "") -> None: ...  # raises HistoricalDataDisabledError
def require_live_trading_allowed(action: str = "") -> None: ...      # raises PaperTradingOnlyError
```
- **HTTP Error Contract (blocked history in cloud):** Status `403` (API endpoints) or `503` (disabled page/blueprint routes), JSON body:
  `{"status": "error", "code": "HISTORICAL_DATA_DISABLED", "message": "Historical data and backtesting are disabled in cloud deployment (OPENALGO_PROFILE=cloud). Available locally."}`
- **Live-Trading Unhampered Contract in Cloud:** `POST /api/v1/quotes`, `POST /api/v1/multiquotes`, `POST /api/v1/depth`, `POST /api/v1/optionchain`, `POST /api/v1/intervals` (read-only static broker timeframe map, zero history/disk), WebSocket `:8765`, and all paper-mode order/position/book APIs MUST remain 100% functional in `OPENALGO_PROFILE=cloud`.
- **HTTP Error Contract (blocked live trading):** Status `403`, JSON body:
  `{"status": "error", "code": "PAPER_TRADING_ONLY", "message": "Live trading is disabled (PAPER_TRADING_ONLY=true). Paper trading only."}`

### 2. Frontend Runtime Config Contract (`GET /auth/app-config`)
- Unauthenticated-safe public JSON endpoint (no secrets):
  ```json
  {
    "status": "success",
    "profile": "cloud",
    "historical_data_enabled": false,
    "paper_trading_only": true,
    "brokers": ["fyers", "deltaexchange", "groww"]
  }
  ```

### 3. Background Broker Context Propagation (`utils/broker_context.py`)
```python
class BrokerContextThreadPoolExecutor(ThreadPoolExecutor):
    """Captures current_broker() at submit() time and enters broker_context(broker) on the worker thread."""
```
- `_BROKER_SCOPED_SESSIONS` MUST include `database.latency_db:latency_session`, `database.analyzer_db:db_session`, `database.apilog_db:db_session`, and `database.traffic_db:logs_session` in addition to existing sessions.

### 4. Multi-Broker WebSocket Auth Contract (`websocket_proxy/server.py`)
- WebSocket `authenticate` payload accepts optional `"broker": "fyers" | "deltaexchange" | "groww"` (backwards-compatible fallback via `select_broker_for_api_key` inside explicit `broker_context`).
- Internal adapter maps MUST be keyed by `(user_id, broker)` — connecting a second broker NEVER evicts an active broker's adapter.

### 5. Live-Bar Strategy State Contract (`strategies/live/`)
- Every live-bar strategy persists atomic JSON state (`.state.json` via write-temp + `os.replace`), strictly bounded `< 10 KB` on disk (fixed rolling cap e.g. `max_bars=50`), containing strategy trigger/stop/trail fields + rolling live OHLCV bars + EMA continuity values.
- In `OPENALGO_PROFILE=cloud`: ZERO calls to `/api/v1/history` and ZERO historical disk growth; uses `/api/v1/quotes` (or WS ticks) + `/api/v1/openposition` + `/api/v1/placeorder` with explicit `X-OpenAlgo-Broker: <broker>` header.
- In `OPENALGO_PROFILE=full` (local): supports `--backtest` CLI/mode using local historical data.

### 6. Production Monitoring JSONL Contract (`/opt/openalgo/log/monitor-3h.jsonl`)
- One JSON object per 3-hour run containing: `timestamp_utc`, `check_number`, `overall_status` (`"OK" | "WARN" | "CRITICAL"`), `https` (login/health/historify/history_blocked/ws), `services` (`openalgo`, `caddy`, `ws_port_8765`), `resources` (ram_avail_mb, swap_used_mb, disk_used_pct, disk_avail_gb), `brokers` (per-broker `analyze_mode`, `auth_status`, `sandbox_positions`, `sandbox_orders`, `missed_exits`), `strategies` (running PIDs, heartbeat age), `errors_last_3h` (`BrokerContextError` count, total error lines, `errors.jsonl` size MB, `health.db` size MB), `alerts` (list of severity-tagged alert objects; expired Fyers/Groww OAuth tokens = `WARN`, safety/service/disk/history breaches = `CRITICAL`).
