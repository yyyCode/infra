#!/usr/bin/env python3
"""
Naive task executor (半成品 / happy-path only).

Reads a task list (one shell command per line) and runs each command inside
its own Docker container, one after another.

This version is INTENTIONALLY minimal. It works when every task is a short,
well-behaved command that succeeds quickly. It does NOT yet handle:
  - concurrency control or CPU / memory limits
  - task timeouts / killing runaway containers and their child processes
  - state persistence or crash recovery (no SQLite)
  - failure classification or retries
  - per-task log files
  - cleanup of exited containers / dangling images

Those gaps are exactly what accept.sh checks for and what PLAN.md schedules.

Usage:
    python runner.py [tasks_file]     # default: tasks.txt
"""

import subprocess
import sys

# Base image used to run every task. Must contain /bin/sh and python3.
IMAGE = "python:3.11-slim"


def read_tasks(path):
    """Return a list of shell-command strings, skipping blanks and # comments."""
    tasks = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tasks.append(line)
    return tasks


def run_task(task_id, command):
    """Run one command in its own container and wait for it to finish.

    Naive on purpose:
      - no --rm            -> exited containers pile up
      - no timeout         -> a hanging task blocks the whole batch forever
      - no --memory/--cpus -> a greedy task can starve the host
      - output only echoed -> nothing is persisted to disk
    """
    print(f"[task {task_id}] START: {command}")
    proc = subprocess.run(
        ["docker", "run", IMAGE, "/bin/sh", "-c", command],
        capture_output=True,
        text=True,
    )
    if proc.stdout:
        print(proc.stdout, end="")
    if proc.stderr:
        print(proc.stderr, end="", file=sys.stderr)
    status = "succeeded" if proc.returncode == 0 else "failed"
    print(f"[task {task_id}] END: {status} (exit={proc.returncode})")
    return proc.returncode


def main():
    tasks_path = sys.argv[1] if len(sys.argv) > 1 else "tasks.txt"
    tasks = read_tasks(tasks_path)
    print(f"Loaded {len(tasks)} task(s) from {tasks_path}\n")

    results = []
    for i, command in enumerate(tasks, start=1):
        rc = run_task(i, command)
        results.append((i, rc))

    print("\n==== summary ====")
    for task_id, rc in results:
        state = "ok" if rc == 0 else "FAIL"
        print(f"task {task_id}: {state} (exit={rc})")


if __name__ == "__main__":
    main()
