#!/usr/bin/env python3
"""
阶段 4(失败分类与重试)的自动化测试。

不依赖 docker:mock 掉 run_once,只验证分类逻辑与重试策略。
运行:
    python3 tests/test_retry.py
    python3 -m unittest tests.test_retry
"""

import os
import sys
import tempfile
import unittest

# 让测试能 import 到项目根目录下的 runner.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import runner  # noqa: E402


class TestClassify(unittest.TestCase):
    """classify(returncode, timed_out, oom) -> (category, retryable)"""

    def test_success(self):
        self.assertEqual(runner.classify(0, False, False), ("ok", False))

    def test_infra_exit_codes(self):
        for rc in (125, 126, 127):
            self.assertEqual(runner.classify(rc, False, False), ("infra", True))

    def test_oom_not_retryable(self):
        self.assertEqual(runner.classify(137, False, True), ("oom", False))

    def test_timeout_not_retryable(self):
        # 超时优先于退出码判断
        self.assertEqual(runner.classify(137, True, False), ("timeout", False))

    def test_task_failure_retryable(self):
        self.assertEqual(runner.classify(7, False, False), ("task", True))
        self.assertEqual(runner.classify(2, False, False), ("task", True))


class RetryTestBase(unittest.TestCase):
    """为重试测试搭一个内存 DB + 临时日志目录 + mock run_once。"""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log_dir = os.path.join(self.tmp, "logs")
        os.makedirs(self.log_dir)
        self.conn = runner.init_db(os.path.join(self.tmp, "t.db"))
        # 记录每个任务实际执行了几次
        self.calls = {}
        # 关掉退避 sleep,避免测试变慢
        self._orig_sleep = runner.time.sleep
        runner.time.sleep = lambda s: None
        # 保存并替换 run_once
        self._orig_run_once = runner.run_once
        runner.run_once = self._fake_run_once

    def tearDown(self):
        runner.time.sleep = self._orig_sleep
        runner.run_once = self._orig_run_once
        self.conn.close()

    def _fake_run_once(self, task_id, command, limits, log_path):
        """按命令关键字返回不同的 (returncode, timed_out, oom)。"""
        n = self.calls.get(task_id, 0) + 1
        self.calls[task_id] = n
        open(log_path, "w").close()  # run_task 只关心文件路径存在
        # 注意:用精确前缀匹配,避免命令名互相包含子串导致误判。
        if command.startswith("infra-recover"):
            # 前两次 infra 失败(125),第三次成功
            return (0, False, False) if n >= 3 else (125, False, False)
        if command.startswith("infra-always"):
            return (125, False, False)
        if command.startswith("task-fail"):
            return (7, False, False)
        if command.startswith("oom"):
            return (137, False, True)
        if command.startswith("timeout"):
            return (137, True, False)
        # 其余(如 success-task)视为成功
        return (0, False, False)

    def _enqueue_and_run(self, commands, max_retries=2):
        runner.enqueue_tasks(self.conn, commands)
        rows = self.conn.execute(
            "SELECT id, command FROM tasks WHERE state='queued' ORDER BY id"
        ).fetchall()
        limits = {"memory": "256m", "cpus": "1.0",
                  "timeout": 30, "max_retries": max_retries}
        for task_id, command in rows:
            runner.run_task(self.conn, task_id, command, limits, self.log_dir)

    def _row(self, command):
        return self.conn.execute(
            "SELECT state, exit_code, attempts FROM tasks WHERE command=?",
            (command,),
        ).fetchone()


class TestRetryBehaviour(RetryTestBase):

    def test_success_runs_once(self):
        self._enqueue_and_run(["success-task"])
        state, code, attempts = self._row("success-task")
        self.assertEqual((state, code, attempts), ("succeeded", 0, 1))

    def test_infra_retries_then_succeeds(self):
        self._enqueue_and_run(["infra-recover"], max_retries=2)
        state, code, attempts = self._row("infra-recover")
        self.assertEqual(state, "succeeded")
        self.assertEqual(attempts, 3)  # 首次 + 2 次重试后成功

    def test_task_failure_exhausts_retries(self):
        self._enqueue_and_run(["task-fail"], max_retries=2)
        state, code, attempts = self._row("task-fail")
        self.assertEqual(state, "failed")
        self.assertEqual(code, 7)       # 记录真实退出码
        self.assertEqual(attempts, 3)   # 首次 + 2 次重试

    def test_infra_always_exhausts_retries(self):
        self._enqueue_and_run(["infra-always"], max_retries=2)
        state, code, attempts = self._row("infra-always")
        self.assertEqual(state, "failed")
        self.assertEqual(code, 125)
        self.assertEqual(attempts, 3)

    def test_oom_not_retried(self):
        self._enqueue_and_run(["oom-hog"], max_retries=2)
        state, code, attempts = self._row("oom-hog")
        self.assertEqual((state, code, attempts), ("failed", 137, 1))

    def test_timeout_not_retried(self):
        self._enqueue_and_run(["timeout-long"], max_retries=2)
        state, code, attempts = self._row("timeout-long")
        self.assertEqual((state, attempts), ("timed_out", 1))

    def test_max_retries_zero(self):
        # max_retries=0 时,可重试失败也只跑一次
        self._enqueue_and_run(["task-fail"], max_retries=0)
        state, code, attempts = self._row("task-fail")
        self.assertEqual((state, attempts), ("failed", 1))

    def test_succeeded_not_rerun(self):
        # 已成功的任务再次入队执行,不应重跑(沿用阶段3幂等)
        self._enqueue_and_run(["success-task"], max_retries=2)
        self.assertEqual(self.calls[1], 1)
        # 再次导入同一清单 + 再跑一轮 queued(应没有 queued 可跑)
        runner.enqueue_tasks(self.conn, ["success-task"])
        rows = self.conn.execute(
            "SELECT id FROM tasks WHERE state='queued'"
        ).fetchall()
        self.assertEqual(rows, [])
        self.assertEqual(self.calls[1], 1)  # 执行次数没变


if __name__ == "__main__":
    unittest.main(verbosity=2)
