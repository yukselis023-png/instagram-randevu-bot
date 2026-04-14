#!/bin/sh
set -eu
PORT_VALUE=${PORT:-8000}
LLM_BASE_URL_VALUE=${LLM_BASE_URL:-http://127.0.0.1:8045/v1}
SOCAT_PID=""
if echo "$LLM_BASE_URL_VALUE" | grep -q '127.0.0.1:8045'; then
  socat TCP-LISTEN:8045,bind=127.0.0.1,reuseaddr,fork TCP:host.docker.internal:8045 &
  SOCAT_PID=$!
fi
cleanup() {
  if [ -n "${SOCAT_PID:-}" ]; then
    kill "$SOCAT_PID" >/dev/null 2>&1 || true
  fi
}
trap cleanup INT TERM EXIT
exec uvicorn app.main:app --host 0.0.0.0 --port "$PORT_VALUE"
