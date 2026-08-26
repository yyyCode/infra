#!/usr/bin/env python3
"""
Task executor — 阶段 6(资源清理)。

在阶段 5(日志与产物 + 按 ID 查询)之上,新增机制化清理(而非最后手动清一遍):
  - 容器不堆积:每个任务容器在结果判定后立即 docker rm(用 try/finally 保证
    异常路径也删)。不用 --rm 是因为超时后需要先 docker inspect 读 OOMKilled,
    --rm 会抢先删掉容器导致读不到状态。
  - 悬空镜像不增长:批次结束后 docker image prune -f 清理 dangling 镜像。
  - 启动兜底:cleanup_stale_containers 在启动时清掉上次异常退出的残留容器。
  - --no-prune 可关闭批次结束时的镜像清理。

生命周期:谁创建谁负责删 —— run_once 里起容器,finally 里删容器;
main 批次结束统一 prune 镜像;启动清理兜底掉进程崩溃时来不及删的残留。

Usage:
    python3 runner.py [tasks_file] [--concurrency N] [--memory M] [--cpus C]
                      [--timeout S] [--max-retries N] [--no-prune]
    python3 runner.py --resume        # 只恢复未完成任务,不导入新清单
    python3 runner.py --status [ID]   # 查所有/单个任务状态
    python3 runner.py --log ID        # 查单个任务日志
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
DEFAULT_MAX_RETRIES = 2       # 可重试失败的最大重试次数(不含首次执行)

# docker CLI 自身错误(容器没起来)的退出码。125=docker run 命令本身失败;
# 126/127=入口点不可执行/找不到,通常也是环境问题而非任务逻辑失败。
DOCKER_INFRA_EXIT_CODES = {125, 126, 127}

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


def classify(returncode, timed_out, oom):
    """把一次执行结果分类,返回 (category, is_retryable)。

    category ∈ ok | infra | oom | timeout | task
      - ok      : 成功
      - infra   : 容器没起来(docker 退出码 125/126/127),可重试(退避)
      - oom     : 内存超限被杀,确定性失败,不重试
      - timeout : 超时被强杀,确定性失败,不重试
      - task    : 容器起来了但命令非零退出,可重试(短间隔)
    """
    if returncode == 0:
        return "ok", False
    if timed_out:
        return "timeout", False
    if oom:
        return "oom", False
    if returncode in DOCKER_INFRA_EXIT_CODES:
        return "infra", True
    return "task", True


def run_once(task_id, command, limits, log_path, attempt=1):
    """执行一次容器任务,返回 (returncode, timed_out, oom)。不写库。

    日志文件顶部写入自解释头部(任务 ID、命令、第几次尝试、时间),便于
    单独打开某个日志文件就能知道它是哪个任务、哪次尝试的产物。
    """
    name = f"runner-{task_id}"
    # 兜底:清掉可能残留的同名容器,避免重名冲突(重试时上一次的残留)。
    subprocess.run(
        ["docker", "rm", "-f", name],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )

    timed_out = False
    try:
        # 用 Popen 而非 run:超时后我们要主动杀容器,而不是只等客户端。
        # --memory 限内存,--memory-swap 与之相等禁止用 swap 绕过,--cpus 限 CPU。
        with open(log_path, "w", encoding="utf-8") as log:
            ts = time.strftime("%Y-%m-%d %H:%M:%S")
            log.write(f"# task {task_id} | attempt {attempt} | {ts}\n")
            log.write(f"# command: {command}\n")
            log.write("# ---- output ----\n")
            log.flush()   # 确保头部在容器输出之前落盘
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
                # 超时后 `docker run` 客户端还连着,容器仍在后台跑。必须显式杀容器。
                timed_out = True
                kill_container(name)
                proc.wait()
                returncode = proc.returncode

        # OOM 判定必须在删容器之前(要读 docker inspect State.OOMKilled)。
        oom = (not timed_out) and returncode == 137 and was_oom_killed(name)
        return returncode, timed_out, oom
    finally:
        # 谁创建谁负责删:无论成功/失败/超时/异常,都移除这个容器,不堆积。
        subprocess.run(
            ["docker", "rm", "-f", name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )


def run_task(conn, task_id, command, limits, log_dir=LOG_DIR):
    """执行一个任务,按失败类别分策略重试,最终状态与退出码落库。

    limits: dict(memory, cpus, timeout, max_retries)。线程池并发调用,写库串行化。
    """
    log_path = os.path.join(log_dir, f"{task_id}.log")
    max_retries = limits["max_retries"]
    print(f"[task {task_id}] START: {command}  (log: {log_path})")

    attempt = 0
    while True:
        attempt += 1
        db_write(
            conn,
            "UPDATE tasks SET state='running', attempts=attempts+1, started_at=?, log_path=? WHERE id=?",
            (time.time(), log_path, task_id),
        )
        returncode, timed_out, oom = run_once(task_id, command, limits, log_path, attempt)
        category, retryable = classify(returncode, timed_out, oom)

        if category == "ok":
            state = "succeeded"
            break
        if retryable and attempt <= max_retries:
            # infra 用指数退避(2,4,8…),task 用短固定间隔。
            delay = 2 ** attempt if category == "infra" else 1
            print(f"[task {task_id}] {category} 失败(exit={returncode}),"
                  f"第 {attempt}/{max_retries} 次重试,{delay}s 后重试")
            time.sleep(delay)
            continue
        # 不可重试,或重试已用尽 -> 落终态
        state = "timed_out" if category == "timeout" else "failed"
        break

    db_write(
        conn,
        "UPDATE tasks SET state=?, exit_code=?, ended_at=? WHERE id=?",
        (state, returncode, time.time(), task_id),
    )
    tag = f" [{category}]" if category != "ok" else ""
    print(f"[task {task_id}] END: {state} (exit={returncode}){tag} attempts={attempt}")
    return returncode


def show_status(conn, task_id=None):
    """打印任务状态。task_id 为 None 打印全部表格,否则打印单个任务详情。"""
    if task_id is None:
        rows = conn.execute(
            "SELECT id, state, exit_code, attempts, log_path FROM tasks ORDER BY id"
        ).fetchall()
        if not rows:
            print("(无任务)")
            return
        print(f"{'ID':>3}  {'STATE':<10} {'EXIT':>4} {'TRY':>3}  LOG")
        for r in rows:
            print(f"{r[0]:>3}  {r[1]:<10} {str(r[2] if r[2] is not None else '-'):>4} "
                  f"{r[3]:>3}  {r[4] or '-'}")
        return

    row = conn.execute(
        "SELECT id, command, state, exit_code, attempts, log_path, started_at, ended_at "
        "FROM tasks WHERE id=?", (task_id,)
    ).fetchone()
    if not row:
        print(f"任务 {task_id} 不存在")
        return
    print(f"任务 ID   : {row[0]}")
    print(f"命令      : {row[1]}")
    print(f"状态      : {row[2]}")
    print(f"退出码    : {row[3] if row[3] is not None else '-'}")
    print(f"尝试次数  : {row[4]}")
    print(f"日志路径  : {row[5] or '-'}")


def show_log(conn, task_id):
    """打印单个任务的日志文件内容。"""
    row = conn.execute(
        "SELECT log_path FROM tasks WHERE id=?", (task_id,)
    ).fetchone()
    if not row:
        print(f"任务 {task_id} 不存在")
        return
    log_path = row[0]
    if not log_path or not os.path.exists(log_path):
        print(f"任务 {task_id} 暂无日志(log_path={log_path or '-'})")
        return
    with open(log_path, encoding="utf-8") as f:
        sys.stdout.write(f.read())


def parse_args():
    p = argparse.ArgumentParser(description="容器任务执行器(阶段 6)")
    p.add_argument("tasks", nargs="?", default="tasks.txt", help="任务清单文件(默认 tasks.txt)")
    p.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY,
                   help=f"最大并发任务数(默认 {DEFAULT_CONCURRENCY})")
    p.add_argument("--memory", default=DEFAULT_MEMORY,
                   help=f"每个容器内存限额(默认 {DEFAULT_MEMORY})")
    p.add_argument("--cpus", default=DEFAULT_CPUS,
                   help=f"每个容器 CPU 限额(默认 {DEFAULT_CPUS})")
    p.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT,
                   help=f"单个任务超时秒数,超时强杀容器(默认 {DEFAULT_TIMEOUT})")
    p.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                   help=f"可重试失败的最大重试次数(默认 {DEFAULT_MAX_RETRIES})")
    p.add_argument("--resume", action="store_true",
                   help="只恢复未完成任务,不导入新清单")
    p.add_argument("--status", nargs="?", type=int, const=-1, default=None,
                   metavar="ID",
                   help="查询任务状态:不带 ID 打印全部,带 ID 打印单个详情")
    p.add_argument("--log", type=int, default=None, metavar="ID",
                   help="打印指定任务的日志内容")
    p.add_argument("--no-prune", action="store_true",
                   help="批次结束后不清理悬空镜像")
    return p.parse_args()


def prune_images():
    """批次结束后清理悬空(dangling)镜像,避免无限增长。"""
    r = subprocess.run(
        ["docker", "image", "prune", "-f"],
        capture_output=True, text=True,
    )
    out = (r.stdout or "").strip()
    if out:
        print(f"镜像清理:{out.splitlines()[-1]}")


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

    # 查询模式:只读,不执行任务、不碰 docker,查完即退出。
    if args.log is not None:
        show_log(conn, args.log)
        conn.close()
        return
    if args.status is not None:
        show_status(conn, None if args.status == -1 else args.status)
        conn.close()
        return

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
    limits = {"memory": args.memory, "cpus": args.cpus,
              "timeout": args.timeout, "max_retries": args.max_retries}
    print(f"\n待执行 {len(rows)} 条任务,并发={args.concurrency},"
          f"限额 memory={args.memory} cpus={args.cpus} timeout={args.timeout}s "
          f"max_retries={args.max_retries}\n")

    # ThreadPoolExecutor 的 max_workers 就是真正的并发上限:
    # 池里最多 N 个线程,即最多 N 个 docker run 同时在飞。
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for task_id, command in rows:
            pool.submit(run_task, conn, task_id, command, limits)

    # 批次结束:清理悬空镜像(容器已在各任务的 finally 里删除)。
    if not args.no_prune:
        prune_images()

    print("\n==== summary ====")
    for row in conn.execute(
        "SELECT id, state, exit_code, log_path FROM tasks ORDER BY id"
    ):
        print(f"task {row[0]}: {row[1]} (exit={row[2]}) log={row[3]}")
    conn.close()


if __name__ == "__main__":
    main()
