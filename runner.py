#!/usr/bin/env python3
"""
Task executor — 阶段 3(状态持久化与崩溃恢复)。

在阶段 2(超时强杀)之上,新增:
  - 崩溃恢复:runner 被 kill -9 后重启,上次残留在 running 的任务(崩溃时在飞)
    会被重新入队执行;导入清单时按命令去重,不重复导入
  - 已成功绝不重跑:只执行 state='queued' 的任务,succeeded/failed/timed_out
    都不会再动(靠 DB 里的终态判断,天然幂等)
  - 启动清理:清掉上一轮遗留的 runner-* 容器,避免重名冲突

关键认知:状态即时落库(queued->running->终态),所以任何时刻被 kill -9,
DB 都反映真实进度;重启时"running"就代表"上次没跑完",据此恢复。

仍【刻意保留】的缺陷(留给后续阶段):
  - 无失败分类与重试                    (阶段 4)
  - 无机制化资源清理(退出容器/悬空镜像)  (阶段 6)

Usage:
    python3 runner.py [tasks_file] [--concurrency N] [--memory M] [--cpus C] [--timeout S]
    python3 runner.py --resume        # 只恢复未完成任务,不导入新清单
"""

import argparse
import os
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

IMAGE = "python:3.11-slim"
DB_PATH = "state.db"
LOG_DIR = "logs"

# 默认限额(可被命令行覆盖)
DEFAULT_CONCURRENCY = 2
DEFAULT_MEMORY = "256m"
DEFAULT_CPUS = "1.0"
DEFAULT_TIMEOUT = 30          # 单个任务超时秒数
STOP_GRACE = 5               # docker stop 的宽限秒数,超过则 kill 兜底

# 保护 SQLite 写入:默认连接非线程安全,并发下用一把锁串行化写。
_db_lock = threading.Lock()


def db_write(conn, sql, params=()):
    """并发安全的写:一把锁串行化,避免多线程同时写坏连接。"""
    with _db_lock:
        conn.execute(sql, params)
        conn.commit()


def init_db(db_path=DB_PATH):
    """建表并返回连接。state ∈ queued|running|succeeded|failed|timed_out。

    check_same_thread=False:连接要在线程池的多个 worker 间共享,写入用
    _db_lock 串行化保证安全。
    """
    conn = sqlite3.connect(db_path, check_same_thread=False)
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
    """读清单 -> 入库为 queued。按命令去重:已在库里的命令不重复导入。

    去重让「重跑同一份清单」是安全的——已 succeeded 的任务不会因再次导入而
    被重置回 queued 重跑。新增的命令行会作为新任务入队。
    """
    existing = {row[0] for row in conn.execute("SELECT command FROM tasks")}
    added = 0
    with _db_lock:
        for command in tasks:
            if command in existing:
                continue
            conn.execute("INSERT INTO tasks (command, state) VALUES (?, 'queued')", (command,))
            existing.add(command)   # 同一清单内重复行也只入一次
            added += 1
        conn.commit()
    skipped = len(tasks) - added
    print(f"已入库 {added} 条新任务(state=queued),跳过 {skipped} 条已存在")


def recover_interrupted(conn):
    """崩溃恢复:把上次残留在 running 的任务重新入队。

    runner 正常结束时不会留下 running(每个任务都会落终态)。若重启后仍见到
    running,说明上次是被 kill -9 之类打断、任务没跑完 —— 将其重置为 queued
    以便本轮重新执行。已 succeeded/failed/timed_out 的一律不动。
    """
    rows = conn.execute("SELECT id FROM tasks WHERE state='running'").fetchall()
    if not rows:
        return
    ids = [r[0] for r in rows]
    with _db_lock:
        conn.execute(
            "UPDATE tasks SET state='queued' WHERE state='running'"
        )
        conn.commit()
    print(f"检测到 {len(ids)} 个未完成任务(上次崩溃残留),已重置为 queued: {ids}")


def was_oom_killed(name):
    """查容器 State.OOMKilled,判断是否因内存超限被杀。"""
    r = subprocess.run(
        ["docker", "inspect", "-f", "{{.State.OOMKilled}}", name],
        capture_output=True,
        text=True,
    )
    return r.returncode == 0 and r.stdout.strip() == "true"


def kill_container(name):
    """真正杀掉容器:先 docker stop 给宽限(SIGTERM),再 docker kill 兜底(SIGKILL)。

    容器是独立 PID namespace,杀掉容器 = 杀掉里面 PID 1 及其派生的所有子进程,
    不会留下游离的子进程或僵尸容器。stop 已能回收正常进程,kill 兜底应对
    忽略 SIGTERM 的进程。
    """
    subprocess.run(
        ["docker", "stop", "-t", str(STOP_GRACE), name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    # stop 之后容器一般已退出;kill 兜底(已停的容器 kill 会报错,忽略即可)。
    subprocess.run(
        ["docker", "kill", name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def run_task(conn, task_id, command, limits, log_dir=LOG_DIR):
    """在独立容器里执行一个任务(带资源限额 + 超时强杀)。

    limits: dict(memory, cpus, timeout)。可被线程池并发调用,写库走 db_write 串行化。
    """
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

    db_write(
        conn,
        "UPDATE tasks SET state='running', attempts=attempts+1, started_at=?, log_path=? WHERE id=?",
        (time.time(), log_path, task_id),
    )

    timed_out = False
    # 用 Popen 而非 run:超时后我们要主动杀容器,而不是只等客户端。
    # --memory 限内存,--memory-swap 与之相等禁止用 swap 绕过,--cpus 限 CPU。
    with open(log_path, "w", encoding="utf-8") as log:
        proc = subprocess.Popen(
            [
                "docker", "run", "--name", name,
                "--memory", limits["memory"],
                "--memory-swap", limits["memory"],
                "--cpus", limits["cpus"],
                IMAGE, "/bin/sh", "-c", command,
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
            text=True,
        )
        try:
            returncode = proc.wait(timeout=limits["timeout"])
        except subprocess.TimeoutExpired:
            # 关键:超时后 `docker run` 客户端还连着,容器仍在后台跑。
            # 必须显式杀容器,才能连同派生的子进程一起干掉。
            timed_out = True
            kill_container(name)
            proc.wait()          # 容器被杀后,客户端随之退出,回收它
            returncode = proc.returncode

    if timed_out:
        state = "timed_out"
        oom = False
    else:
        # 退出码 137 通常是被 SIGKILL(OOM 常见),再查 OOMKilled 确认。
        oom = returncode == 137 and was_oom_killed(name)
        state = "succeeded" if returncode == 0 else "failed"

    db_write(
        conn,
        "UPDATE tasks SET state=?, exit_code=?, ended_at=? WHERE id=?",
        (state, returncode, time.time(), task_id),
    )
    tag = " [OOM-killed]" if oom else (" [timeout-killed]" if timed_out else "")
    print(f"[task {task_id}] END: {state} (exit={returncode}){tag}")
    return returncode


def parse_args():
    p = argparse.ArgumentParser(description="容器任务执行器(阶段 3)")
    p.add_argument("tasks", nargs="?", default="tasks.txt", help="任务清单文件(默认 tasks.txt)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                   help=f"最大并发任务数(默认 {DEFAULT_CONCURRENCY})")
    p.add_argument("--memory", default=DEFAULT_MEMORY,
                   help=f"每个容器内存限额(默认 {DEFAULT_MEMORY})")
    p.add_argument("--cpus", default=DEFAULT_CPUS,
                   help=f"每个容器 CPU 限额(默认 {DEFAULT_CPUS})")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                   help=f"单个任务超时秒数,超时强杀容器(默认 {DEFAULT_TIMEOUT})")
    p.add_argument("--resume", action="store_true",
                   help="只恢复未完成任务,不导入新清单")
    return p.parse_args()


def cleanup_stale_containers():
    """启动时清掉上一轮遗留的 runner-* 容器,避免重名冲突。"""
    r = subprocess.run(
        ["docker", "ps", "-aq", "--filter", "name=^runner-"],
        capture_output=True, text=True,
    )
    ids = r.stdout.split()
    if ids:
        subprocess.run(["docker", "rm", "-f", *ids],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"启动清理:移除 {len(ids)} 个遗留 runner-* 容器")


def main():
    args = parse_args()
    os.makedirs(LOG_DIR, exist_ok=True)
    conn = init_db()

    # 启动清理:去掉上一轮残留容器(重名会导致 docker run 失败)
    cleanup_stale_containers()

    # 崩溃恢复:把上次残留 running 的任务重置为 queued 以便重跑
    recover_interrupted(conn)

    # 第一步:读清单入库(--resume 时跳过,只跑恢复出来的未完成任务)
    if args.resume:
        print("--resume:跳过清单导入,只执行未完成任务")
    else:
        tasks = read_tasks(args.tasks)
        print(f"从 {args.tasks} 读到 {len(tasks)} 条任务")
        enqueue_tasks(conn, tasks)

    # 第二步:取所有 queued(已 succeeded 的不是 queued,天然不重跑),并发执行。
    rows = conn.execute(
        "SELECT id, command FROM tasks WHERE state='queued' ORDER BY id"
    ).fetchall()
    limits = {"memory": args.memory, "cpus": args.cpus, "timeout": args.timeout}
    print(f"\n待执行 {len(rows)} 条任务,并发={args.concurrency},"
          f"限额 memory={args.memory} cpus={args.cpus} timeout={args.timeout}s\n")

    # ThreadPoolExecutor 的 max_workers 就是真正的并发上限:
    # 池里最多 N 个线程,即最多 N 个 docker run 同时在飞。
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for task_id, command in rows:
            pool.submit(run_task, conn, task_id, command, limits)

    print("\n==== summary ====")
    for row in conn.execute(
        "SELECT id, state, exit_code, log_path FROM tasks ORDER BY id"
    ):
        print(f"task {row[0]}: {row[1]} (exit={row[2]}) log={row[3]}")
    conn.close()


if __name__ == "__main__":
    main()
