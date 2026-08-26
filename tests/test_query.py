#!/usr/bin/env python3
"""
阶段 5(日志与产物 + 按 ID 查询)的自动化测试。

不依赖 docker:直接构造 DB 与日志文件,验证 show_status / show_log。
运行:
    python3 -m unittest tests.test_query
"""

import io
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stdout

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import runner  # noqa: E402


class TestQuery(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log_dir = os.path.join(self.tmp, "logs")
        os.makedirs(self.log_dir)
        self.conn = runner.init_db(os.path.join(self.tmp, "t.db"))
        runner.enqueue_tasks(self.conn, ["echo hi", "sleep 600", "exit 7"])
        # 任务 1:成功,带日志文件
        self.log1 = os.path.join(self.log_dir, "1.log")
        with open(self.log1, "w", encoding="utf-8") as f:
            f.write("# task 1 | attempt 1\n# command: echo hi\n# ---- output ----\nhi\n")
        self.conn.execute(
            "UPDATE tasks SET state='succeeded',exit_code=0,attempts=1,log_path=? WHERE id=1",
            (self.log1,))
        self.conn.execute(
            "UPDATE tasks SET state='timed_out',exit_code=137,attempts=1 WHERE id=2")
        self.conn.execute(
            "UPDATE tasks SET state='failed',exit_code=7,attempts=3 WHERE id=3")
        self.conn.commit()

    def tearDown(self):
        self.conn.close()

    def _capture(self, fn, *a):
        buf = io.StringIO()
        with redirect_stdout(buf):
            fn(*a)
        return buf.getvalue()

    def test_status_all_lists_every_task(self):
        out = self._capture(runner.show_status, self.conn, None)
        self.assertIn("succeeded", out)
        self.assertIn("timed_out", out)
        self.assertIn("failed", out)
        # 表头存在
        self.assertIn("STATE", out)

    def test_status_single_shows_detail(self):
        out = self._capture(runner.show_status, self.conn, 3)
        self.assertIn("failed", out)
        self.assertIn("7", out)       # 退出码
        self.assertIn("echo hi" if False else "exit 7", out)  # 命令

    def test_status_missing_task(self):
        out = self._capture(runner.show_status, self.conn, 99)
        self.assertIn("99", out)
        self.assertIn("不存在", out)

    def test_log_prints_content(self):
        out = self._capture(runner.show_log, self.conn, 1)
        self.assertIn("command: echo hi", out)
        self.assertIn("hi", out)

    def test_log_missing_file(self):
        # 任务 2 没有 log_path
        out = self._capture(runner.show_log, self.conn, 2)
        self.assertIn("暂无日志", out)

    def test_log_missing_task(self):
        out = self._capture(runner.show_log, self.conn, 99)
        self.assertIn("不存在", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
