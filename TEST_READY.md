# E2E Test Suite Ready (`TEST_READY.md`)

**Status:** **READY** (Wave 3 Strict AND Gate **PASS** — `iter3_auditor=CLEAN`, `iter3_reviewer_1=APPROVE`, `iter3_reviewer_2=APPROVE`, `iter3_challenger_1=PASS`, `iter3_challenger_2=PASS`)  
**Project:** `AyushMehta292/openalgo-p1` (Feature F17 — Independent Opaque-Box 4-Tier E2E Suite)  
**Architecture Spec:** [`TEST_INFRA.md`](TEST_INFRA.md)  
**Suite Directory:** `test/e2e/` (10 files: `__init__.py`, `_firewall.py`, `_server_launcher.py`, `conftest.py`, 6 tier test modules)

---

## 1. Test Runner Commands

All markers (`tier1`, `tier2`, `tier3`, `tier4`) are registered dynamically in `test/e2e/conftest.py` (`pytest_configure`); no `pyproject.toml` changes are required.

```bash
# Full 4-Tier E2E suite (161 tests)
.venv/bin/pytest test/e2e

# Per-tier execution
.venv/bin/pytest test/e2e -m tier1   # 66 tests — Feature Coverage (Happy Path)
.venv/bin/pytest test/e2e -m tier2   # 66 tests — Boundary, Corner & Adversarial Cases
.venv/bin/pytest test/e2e -m tier3   # 22 tests — Cross-Feature Pairwise Combinations
.venv/bin/pytest test/e2e -m tier4   #  7 tests — Full Real-World Multi-Step Scenarios

# Collection & marker integrity check (zero unknown-marker warnings)
.venv/bin/pytest --collect-only -q -W error::pytest.PytestUnknownMarkWarning test/e2e
```

---

## 2. Verified Test Counts & Tier Breakdown

Independently verified via `.venv/bin/pytest --collect-only -q -W error::pytest.PytestUnknownMarkWarning test/e2e`:

| Tier | Marker | Verified Count | `TEST_INFRA.md` Minimum | Test Module(s) |
|---|---|---:|---:|---|
| **Tier 1** | `tier1` | **66** | `>= 55` (`>= 5` × 11 features) | `test_tier1_safety_profiles.py` (30), `test_tier1_multibroker_ops.py` (36) |
| **Tier 2** | `tier2` | **66** | `>= 55` (`>= 5` × 11 features) | `test_tier2_safety_profiles.py` (30), `test_tier2_multibroker_ops.py` (36) |
| **Tier 3** | `tier3` | **22** | `>= 20` | `test_tier3_pairwise.py` (22) |
| **Tier 4** | `tier4` | **7** | `7` (`>= max(5, ceil(11/2))`) | `test_tier4_scenarios.py` (7: `T4-S1`..`T4-S7`) |
| **Total** | — | **161** | `>= 137` | **0 unmarked, 0 pairwise tier overlaps, 0 collection errors/warnings** |

---

## 3. Feature Coverage Checklist (`N = 11` User-Facing Features)

| ID | Feature | `PROJECT.md` Ref | Tier 1 | Tier 2 | Tier 3 & Tier 4 Coverage | Status |
|---|---|---|---:|---:|---|---|
| **E2E-F1** | Paper-Trading Hard Lock (`PAPER_TRADING_ONLY=true`) across all 6 surfaces | F6, Q4 | 6 | 6 | `T3` pairwise + `T4-S1`, `T4-S3`, `T4-S5` | Verified |
| **E2E-F2** | Cloud Zero-Historical-Storage & Backtesting Lockout (`OPENALGO_PROFILE=cloud`) | F7, User Clarif. | 6 | 6 | `T3` pairwise + `T4-S1`, `T4-S2`, `T4-S5` | Verified |
| **E2E-F3** | Cloud Live-Trading Data Unhampered (quotes, intervals, paper orders, `/trading`, `/scalping`) | F7, Q8, User Clarif. | 6 | 6 | `T3` pairwise + `T4-S1`, `T4-S2`, `T4-S6` | Verified |
| **E2E-F4** | Local Mode Full Parity (`OPENALGO_PROFILE=full`, DuckDB, Historify, Portfolio/SIP Backtest) | F8 | 6 | 6 | `T3` pairwise + `T4-S3` | Verified |
| **E2E-F5** | Frontend Runtime Config (`/auth/app-config`) & Profile/Paper SPA Contract | F9, Q8 | 6 | 6 | `T3` pairwise + `T4-S1`, `T4-S3`, `T4-S5` | Verified |
| **E2E-F6** | Multi-Broker Isolation & Concurrent Routing (`deltaexchange`, `fyers`, `groww`) | F10, F14, Q9 | 6 | 6 | `T3` pairwise + `T4-S1`, `T4-S4`, `T4-S6` | Verified |
| **E2E-F7** | Multi-Broker WebSocket Proxy & Streaming (`:8765`, per-broker auth & fan-out) | F11, Q5 | 6 | 6 | `T3` pairwise + `T4-S4`, `T4-S7` | Verified |
| **E2E-F8** | Multi-Broker Order Updates & Sandbox Execution / Square-Off (`MARKET`/`LIMIT`/`SL`/`SL-M`/`GTT`) | F12, F13, Q9 | 6 | 6 | `T3` pairwise + `T4-S1`, `T4-S4`, `T4-S6` | Verified |
| **E2E-F9** | Zero-History Live-Bar Strategies (`strategies/live/`) & OpenScript Cloud Degradation | F15, F16, Q1, Q2, Q8 | 6 | 6 | `T3` pairwise + `T4-S2`, `T4-S3` | Verified |
| **E2E-F10** | Runtime Stability & Bounded Storage Hygiene (`BrokerContextError=0`, `errors.jsonl` rotation, `health.db` pruning) | F2, F3, F4, Q3 | 6 | 6 | `T3` pairwise + `T4-S1`, `T4-S6` | Verified |
| **E2E-F11** | Production 3-Hour Monitoring Contract (`scripts/monitor_production.py`, 10-field JSONL `OK`/`WARN`/`CRITICAL`) | F20, Q6, Q7 | 6 | 6 | `T3` pairwise + `T4-S7` | Verified |

---

## 4. Verified Safety, Isolation & Determinism Invariants

1. **Zero Real-Money Risk (`test/e2e/_firewall.py`):** Intercepts all 8 outbound transports (`httpx.HTTPTransport`, `httpx.AsyncHTTPTransport`, `utils.httpx_client._BrokerTransport`, `requests.Session.send`, `urllib3.connectionpool.HTTPConnectionPool.urlopen`, `http.client.HTTPConnection.request`, `websocket.WebSocketApp`, `websocket.create_connection`) with normalized host/path evasion defense (FQDN trailing dots, uppercase/ports, IPv6, multi-pass percent-decoding, `//` & `..` traversal, redirects, Delta `/orders/leverage`, Groww write `/v1/positions`). Empirically verified `0` escaped non-loopback sockets under `socket.socket.connect`/`connect_ex`/`create_connection` canaries. Every fixture teardown asserts `assert_zero_real_broker_order_calls()`.
2. **Complete Filesystem & Process Isolation (`test/e2e/_server_launcher.py`, `conftest.py`):** Every server and WebSocket proxy runs in an isolated `tmp_path` (`PROJECT_ROOT`, per-broker `db/brokers/<broker>/*.db`, `log/`, `mock_quotes.json`) on ephemeral loopback ports with zero repo `db/`/`log/`/`.env` open FDs and zero references to production hosts (`algo.alphafoxservices.com`, `34.93.242.143`, `gcloud`, `/opt/openalgo`).
3. **24/7 Calendar & IST Time Independence:** All NSE paper orders use `CNC` (and `NRML` for NFO short-sell legs; `"MIS"` retained exclusively in `test_t2_f8_gtt_oco_invalid_sl_ge_target_and_mis_product_rejected` for GTT MIS rejection), eliminating IST 15:15–09:00 MIS cutoff flakes. `T4-S3` weekly SIP uses a 25-day window verified `4` installments on `365/365` (and `366/366` leap-year) calendar days. All 22 multi-server/restart/WS+server tests carry `@pytest.mark.timeout(180)`.
4. **Strict Opaque-Box Hygiene:** `0` skips, `0` xfails, `0` fake mocks, `0` private app imports across all 6 `test_tier*.py` files, `0` HTTP-500-masking `or` assertions.
