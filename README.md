# 容器任务执行器 · Infra 实操题

在独立 Docker 容器里批量执行任务的执行器,支持并发控制、资源限额、超时强杀、
崩溃恢复、失败重试、日志留存和资源清理。已按 [`PLAN.md`](PLAN.md) 的 6 个阶段
从「仅跑通最顺利情况」的初版改造到生产可用的最低标准。

## 文件

- `runner.py` — 任务执行器。读任务清单,在独立容器里并发执行,状态落 SQLite。
- `tasks.txt` — 测试任务:顺利任务 + 超时/吃内存/多子进程/故意失败的麻烦任务。
- `accept.sh` — 6 项验收检查。
- `PLAN.md` — 分阶段改造计划书。
- `tests/` — 不依赖 docker 的自动化测试(分类/重试/查询/清理)。

## 运行

需要本机 Docker 在运行,Python 3.10+(仅标准库)。首次会拉取 `python:3.11-slim` 镜像。

```bash
python3 runner.py tasks.txt --concurrency 2 --timeout 10
```

常用参数:

- `--concurrency N` — 最大并发任务数(默认 2)
- `--memory M` / `--cpus C` — 每个容器的资源限额(默认 256m / 1.0)
- `--timeout S` — 单任务超时秒数,超时强杀容器(默认 30)
- `--max-retries N` — 可重试失败的重试上限(默认 2)
- `--resume` — 只恢复未完成任务,不导入新清单
- `--no-prune` — 批次结束不清理悬空镜像

查询(只读,不需要 docker):

```bash
python3 runner.py --status        # 所有任务状态表
python3 runner.py --status 4      # 任务 4 详情
python3 runner.py --log 4         # 任务 4 日志
```

崩溃恢复:进程被 `kill -9` 后重启,未完成任务自动恢复,已成功任务不重跑。

## 测试

```bash
python3 -m unittest tests.test_retry tests.test_query tests.test_cleanup
```

## accept.sh 通过情况

6 项验收对应 `PLAN.md` 的 6 个阶段,均已实现:

1. 并发与限额 — 并发受 `--concurrency` 限制,容器带 CPU/内存限额,OOM 被识别为失败
2. 超时强杀 — 超时后 `docker stop`→`docker kill`,杀掉整棵子进程树,不留僵尸容器
3. 状态持久化与崩溃恢复 — 状态落 SQLite,kill -9 后可恢复,已成功不重跑
4. 失败分类与重试 — 区分 infra/task/oom/timeout,分策略重试,重试受限
5. 日志与产物 — 每任务独立日志,`--status`/`--log` 按 ID 查询
6. 资源清理 — try/finally 删容器、批次结束 prune 悬空镜像、启动兜底清理

## 分支

- `main` — 主线。
- `initial-version` — 初版存档,记录「仅跑通最顺利情况」的起点,便于对照修改过程。
