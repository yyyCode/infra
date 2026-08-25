#!/usr/bin/env bash
#
# Acceptance script for the container task executor.
#
# Runs a series of checks against the runner's outputs. Against the naive
# happy-path runner.py these are EXPECTED TO FAIL — that is the starting
# state of the exercise. Each phase in PLAN.md makes one more check pass.
#
# Assumptions about a "production" runner (what these checks look for):
#   - a SQLite DB at ./state.db with a `tasks` table
#       columns: id, command, state, exit_code, log_path, attempts
#       state in (queued|running|succeeded|failed|timed_out)
#   - per-task logs under ./logs/<id>.log
#   - containers named runner-<id> so we can inspect / clean them
#
# Usage: ./accept.sh
set -u

DB="${DB:-state.db}"
LOG_DIR="${LOG_DIR:-logs}"

pass=0
fail=0

ok()   { echo "PASS: $1"; pass=$((pass+1)); }
bad()  { echo "FAIL: $1"; fail=$((fail+1)); }

have_sqlite() { command -v sqlite3 >/dev/null 2>&1; }
q() { sqlite3 "$DB" "$1" 2>/dev/null; }

echo "==== acceptance checks ===="

# ---- 1. concurrency & limits -------------------------------------------
# Memory-hog task must end up OOM-killed / failed, not succeeded, and the
# host must still be alive (i.e. we got here at all).
if have_sqlite; then
  mem_state=$(q "SELECT state FROM tasks WHERE command LIKE '%bytearray%' LIMIT 1;")
  if [ "$mem_state" = "failed" ] || [ "$mem_state" = "timed_out" ]; then
    ok "1. memory-hog task was contained (state=$mem_state)"
  else
    bad "1. memory-hog task not contained (state='${mem_state:-<none>}')"
  fi
else
  bad "1. sqlite3 not available / no state DB to inspect"
fi

# ---- 2. timeout kill ----------------------------------------------------
timeout_state=$(q "SELECT state FROM tasks WHERE command LIKE 'sleep 600%' LIMIT 1;")
if [ "$timeout_state" = "timed_out" ]; then
  ok "2. long task was timed out"
else
  bad "2. long task not timed out (state='${timeout_state:-<none>}')"
fi

# No leftover running containers from this batch.
leftover=$(docker ps -a --filter "name=runner-" --format '{{.Names}}' 2>/dev/null | wc -l)
if [ "$leftover" -eq 0 ]; then
  ok "2b. no leftover runner containers"
else
  bad "2b. $leftover runner container(s) left behind"
fi

# ---- 3. persistence & recovery -----------------------------------------
if [ -f "$DB" ]; then
  ok "3. state DB exists ($DB)"
else
  bad "3. state DB missing ($DB)"
fi

# ---- 4. failure classification -----------------------------------------
fail_rc=$(q "SELECT exit_code FROM tasks WHERE command LIKE '%exit 7%' LIMIT 1;")
if [ "$fail_rc" = "7" ]; then
  ok "4. failing task recorded correct exit code (7)"
else
  bad "4. failing task exit code wrong (got '${fail_rc:-<none>}')"
fi

# ---- 5. logs & artifacts -----------------------------------------------
if [ -d "$LOG_DIR" ] && [ "$(ls -A "$LOG_DIR" 2>/dev/null | wc -l)" -gt 0 ]; then
  ok "5. per-task logs present in $LOG_DIR/"
else
  bad "5. no per-task logs in $LOG_DIR/"
fi

# ---- 6. resource cleanup -----------------------------------------------
dangling=$(docker images -f "dangling=true" -q 2>/dev/null | wc -l)
if [ "$dangling" -eq 0 ]; then
  ok "6. no dangling images"
else
  bad "6. $dangling dangling image(s) present"
fi

echo "==========================="
echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
