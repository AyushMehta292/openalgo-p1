"""Install profile: full (default) or cloud (paper/live only, no Historify/backtests)."""

from __future__ import annotations

import os


def get_openalgo_profile() -> str:
    raw = os.environ.get("OPENALGO_PROFILE", "full")
    return (raw or "full").strip().strip("'\"").lower() or "full"


def is_cloud_profile() -> bool:
    """True when OPENALGO_PROFILE=cloud: no Historify/DuckDB downloads or backtest pages."""
    return get_openalgo_profile() == "cloud"
