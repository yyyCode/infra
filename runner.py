#!/usr/bin/env python3
"""
Task executor — 阶段 0(脚手架 / 地基)。

在初版「仅跑顺利情况」的基础上,加入后续所有阶段都要依赖的地基:
  - SQLite 状态库(state.db):记录每个任务的 state / exit_code / log_path / attempts
  - 容器固定命名 runner-<id>,方便 inspect 与清理
  - logs/<id>.log:每个任务的 stdout/stderr 落独立日志文件
  - 执行拆两步:先「读清单 -> 入库为 queued」,再「取 queued -> 执行」

仍【刻意保留】的缺陷(留给后续阶段):
  - 串行执行,无并发控制                (阶段 1)
  - 无 --memory / --cpus 限额          (阶段 1)
  - 无超时强杀                          (阶段 2)
  - 无崩溃恢复(不会把残留 running 重入队) (阶段 3)
  - 无失败分类与重试                    (阶段 4)
  - 无机制化资源清理(退出容器/悬空镜像)  (阶段 6)

Usage:
    python3 runner.py [tasks_file]     # 默认 tasks.txt
"""

import os
import sqlite3
import subprocess
import sys
import time

IMAGE = "python:3.11-slim"
DB_PATH = "state.db"
LOG_DIR = "logs"


def init_db(db_path=DB_PATH):
    """建表并返回连接。state ∈ queued|running|succeeded|failed|timed_out。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tasks (
            id         INTEGER PRIMARY KEY,
            command    TEXT    NOT NULL,
            state      TEXT    NOT NULL DEFAULT 'queued',
            exit_code  INTEGER,
            log_path   TEXT,
            attempts   INTEGER NOT NULL DEFAULT 0,
            started_at REAL,
            ended_at   REAL
        )
        """
    )
    conn.commit()
    return conn


def read_tasks(path):
    """返回命令字符串列表,跳过空行与 # 注释。"""
    tasks = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            tasks.append(line)
    return tasks


def enqueue_tasks(conn, tasks):
    """读清单 -> 入库为 queued。表已有任务则跳过(简单幂等,避免重跑重复导入)。"""
    existing = conn.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    if existing:
        print(f"DB 已有 {existing} 条任务,跳过导入")
        return
    for command in tasks:
        conn.execute("INSERT INTO tasks (command, state) VALUES (?, 'queued')", (command,))
    conn.commit()
    print(f"已入库 {len(tasks)} 条任务(state=queued)")


def run_task(conn, task_id, command, log_dir=LOG_DIR):
    """在独立容器里串行执行一个任务,状态与输出分别落库、落日志文件。"""
    name = f"runner-{task_id}"
    log_path = os.path.join(log_dir, f"{task_id}.log")
    print(f"[task {task_id}] START: {command}  (log: {log_path})")

    # 兜底:清掉可能残留的同名容器,避免重名冲突。
    # (这里只为让容器命名可重跑;机制化清理留待阶段 6)
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    conn.execute(
        "UPDATE tasks SET state='running', attempts=attempts+1, started_at=?, log_path=? WHERE id=?",
        (time.time(), log_path, task_id),
    )
    conn.commit()

    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.run(
            ["docker", "run", "--name", name, IMAGE, "/bin/sh", "-c", command],
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )

    state = "succeeded" if proc.returncode == 0 else "failed"
    conn.execute(
        "UPDATE tasks SET state=?, exit_code=?, ended_at=? WHERE id=?",
        (state, proc.returncode, time.time(), task_id),
    )
    conn.commit()
    print(f"[task {task_id}] END: {state} (exit={proc.returncode})")
    return proc.returncode


def main():
    tasks_path = sys.argv[1] if len(sys.argv) > 1 else "tasks.txt"
    os.makedirs(LOG_DIR, exist_ok=True)
    conn = init_db()

    # 第一步:读清单入库
    tasks = read_tasks(tasks_path)
    print(f"从 {tasks_path} 读到 {len(tasks)} 条任务")
    enqueue_tasks(conn, tasks)

    # 第二步:取出所有 queued 任务串行执行(已 succeeded 的不是 queued,天然不重跑)
    rows = conn.execute(
        "SELECT id, command FROM tasks WHERE state='queued' ORDER BY id"
    ).fetchall()
    print(f"\n待执行 {len(rows)} 条任务\n")
    for task_id, command in rows:
        run_task(conn, task_id, command)

    print("\n==== summary ====")
    for row in conn.execute(
        "SELECT id, state, exit_code, log_path FROM tasks ORDER BY id"
    ):
        print(f"task {row[0]}: {row[1]} (exit={row[2]}) log={row[3]}")
    conn.close()


if __name__ == "__main__":
    main()
