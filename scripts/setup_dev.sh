#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

for cmd in uv node npm; do
  command -v "$cmd" >/dev/null 2>&1 || {
    echo "ERROR: required tool '$cmd' not found on PATH" >&2
    exit 1
  }
done

echo "[1/4] Syncing Python environment (.venv) via uv..."
uv sync --dev

echo "[2/4] Installing Gunicorn (and eventlet for CI parity) into .venv..."
if [ "${SKIP_EVENTLET:-0}" = "1" ]; then
  uv pip install --python "$ROOT_DIR/.venv/bin/python" "gunicorn>=25.0,<26"
else
  uv pip install --python "$ROOT_DIR/.venv/bin/python" "gunicorn>=25.0,<26" "eventlet==0.41.2"
fi

echo "[3/4] Installing frontend dependencies (frontend/node_modules)..."
if [ -f "$ROOT_DIR/frontend/package-lock.json" ]; then
  npm --prefix "$ROOT_DIR/frontend" ci
else
  npm --prefix "$ROOT_DIR/frontend" install
fi

echo "[4/4] Running quick sanity checks..."
if [ "${SKIP_EVENTLET:-0}" = "1" ]; then
  "$ROOT_DIR/.venv/bin/python" -c "import flask, sqlalchemy, pytest, gunicorn; print('Python OK:', flask.__version__, gunicorn.__version__)"
else
  "$ROOT_DIR/.venv/bin/python" -c "import flask, sqlalchemy, pytest, gunicorn, eventlet; print('Python OK:', flask.__version__, gunicorn.__version__, eventlet.__version__)"
fi
"$ROOT_DIR/.venv/bin/pytest" --version
"$ROOT_DIR/.venv/bin/ruff" --version
npm --prefix "$ROOT_DIR/frontend" exec -- tsc --version
npm --prefix "$ROOT_DIR/frontend" exec -- vitest --version

echo "Dev environment ready."
