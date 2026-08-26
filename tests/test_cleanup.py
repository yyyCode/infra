#!/usr/bin/env python3
"""
阶段 6(资源清理)的自动化测试。

不依赖 docker:mock subprocess,验证
  - run_once 在正常/异常路径都会 docker rm 容器(try/finally)
  - OOM 判定在删容器之前完成(inspect 先于 rm)
运行:
    python3 -m unittest tests.test_cleanup
"""

import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import runner  # noqa: E402


class _P:
    def __init__(self, rc=0, out=""):
        self.returncode = rc
        self.stdout = out


class _FakePopen:
    def __init__(self, cmd, **kw):
        self.cmd = cmd
        self.returncode = None

    def wait(self, timeout=None):
        self.returncode = 0
        return 0


class TestCleanup(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.log_dir = os.path.join(self.tmp, "logs")
        os.makedirs(self.log_dir)
        self.calls = []            # 记录 docker 子命令调用顺序
        self._orig_run = runner.subprocess.run
        self._orig_popen = runner.subprocess.Popen
        runner.subprocess.run = self._fake_run
        runner.subprocess.Popen = _FakePopen

    def tearDown(self):
        runner.subprocess.run = self._orig_run
        runner.subprocess.Popen = self._orig_popen

    def _fake_run(self, cmd, **kw):
        # 记录 docker 动作:rm / inspect / prune
        if cmd[:3] == ["docker", "rm", "-f"]:
            self.calls.append(("rm", cmd[3]))
        elif cmd[:2] == ["docker", "inspect"]:
            self.calls.append(("inspect", cmd[-1]))
            return _P(0, "false")
        elif cmd[:3] == ["docker", "image", "prune"]:
            self.calls.append(("prune", None))
            return _P(0, "Total reclaimed space: 0B")
        return _P(0, "")

    def _limits(self):
        return {"memory": "256m", "cpus": "1.0", "timeout": 30}

    def test_container_removed_on_success(self):
        runner.run_once(1, "echo hi", self._limits(),
                        os.path.join(self.log_dir, "1.log"), 1)
        # finally 里应有针对 runner-1 的 rm
        self.assertIn(("rm", "runner-1"), self.calls)

    def test_container_removed_on_exception(self):
        # 让 Popen 抛错,模拟 docker 不可用;finally 仍需删容器
        runner.subprocess.Popen = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom"))
        with self.assertRaises(RuntimeError):
            runner.run_once(2, "echo x", self._limits(),
                            os.path.join(self.log_dir, "2.log"), 1)
        self.assertIn(("rm", "runner-2"), self.calls)

    def test_inspect_before_rm_for_oom(self):
        # returncode=137 会触发 OOM inspect;inspect 必须在最后一次 rm 之前
        class Popen137(_FakePopen):
            def wait(self, timeout=None):
                self.returncode = 137
                return 137
        runner.subprocess.Popen = Popen137
        runner.run_once(3, "hog", self._limits(),
                        os.path.join(self.log_dir, "3.log"), 1)
        actions = [c[0] for c in self.calls]
        self.assertIn("inspect", actions)
        # 最后一个动作应是 rm(finally),且 inspect 在它之前
        self.assertEqual(actions[-1], "rm")
        self.assertLess(actions.index("inspect"), len(actions) - 1)

    def test_prune_images_runs(self):
        runner.prune_images()
        self.assertIn(("prune", None), self.calls)


if __name__ == "__main__":
    unittest.main(verbosity=2)
