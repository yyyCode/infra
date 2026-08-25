# 容器任务执行器 · Infra 实操题

在独立 Docker 容器里批量执行任务的执行器。当前为**初版(仅跑通最顺利情况)**,
后续按 [`PLAN.md`](PLAN.md) 分阶段改造到生产可用。

## 文件

- `runner.py` — 任务执行器。读任务清单,逐行在容器里 `docker run` 执行。
- `tasks.txt` — 测试任务:顺利任务 + 超时/吃内存/多子进程/故意失败的麻烦任务。
- `accept.sh` — 6 项验收检查(对初版预期不通过)。
- `PLAN.md` — 分阶段改造计划书。

## 运行

需要本机 Docker 在运行,Python 3.10+。首次会拉取 `python:3.11-slim` 镜像。

跑最顺利的情况(只挑乖任务):

```bash
printf '%s\n' \
  'echo "hello from task A"' \
  'python3 -c "print(sum(range(1000)))"' \
  'sh -c "for i in 1 2 3; do echo tick \$i; done"' \
  > happy.txt

python runner.py happy.txt
```

跑完整清单(会暴露初版的问题:`sleep 600` 卡住整批、吃内存无限额、退出容器堆积):

```bash
python runner.py tasks.txt      # 默认即 tasks.txt
```

卡住时自救:`Ctrl-C`,再清理残留容器 `docker ps -aq | xargs docker rm -f`。

## accept.sh 通过情况

初版 6 项**均未通过**,这是练习起点。每个阶段(见 `PLAN.md`)让 `accept.sh` 多过一项。

## 分支

- `main` — 主线,持续按计划改造。
- `initial-version` — 初版存档,记录「仅跑通最顺利情况」的起点,便于对照修改过程。
