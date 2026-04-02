import os
import stat
import tempfile
import unittest
from unittest.mock import patch

from libsweep.probe import probe_binary, probe_binary_rundown, _find_binary, _get_version_string


class TestFindBinary(unittest.TestCase):
    def test_finds_in_extra_dirs(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "mybinary")
            open(path, "w").close()
            os.chmod(path, stat.S_IRWXU)
            result = _find_binary("mybinary", [d])
            self.assertEqual(result, os.path.realpath(path))

    def test_returns_empty_when_not_found(self):
        result = _find_binary("__nonexistent_binary_xyz__", [])
        self.assertEqual(result, "")

    def test_skips_non_executable(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "notexec")
            open(path, "w").close()
            os.chmod(path, stat.S_IRUSR)
            result = _find_binary("notexec", [d])
            self.assertEqual(result, "")


class TestGetVersionString(unittest.TestCase):
    def test_returns_empty_for_empty_path(self):
        vs, rc = _get_version_string("")
        self.assertEqual(vs, "")
        self.assertEqual(rc, -1)

    def test_captures_first_line(self):
        import subprocess
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=0,
                stdout="Python 3.9.7\nAdditional info\n",
                stderr="",
            )
            vs, rc = _get_version_string("/usr/bin/python3")
            self.assertEqual(vs, "Python 3.9.7")
            self.assertEqual(rc, 0)

    def test_falls_back_to_stderr(self):
        import subprocess
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess(
                args=[], returncode=1,
                stdout="",
                stderr="gcc version 11.3.0\n",
            )
            vs, rc = _get_version_string("/usr/bin/gcc")
            self.assertEqual(vs, "gcc version 11.3.0")
            self.assertEqual(rc, 1)

    def test_timeout_returns_minus_two(self):
        import subprocess
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd=[], timeout=3)):
            vs, rc = _get_version_string("/usr/bin/something")
            self.assertEqual(vs, "")
            self.assertEqual(rc, -2)


class TestProbeBinary(unittest.TestCase):
    def test_present_binary(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "myprog")
            open(path, "w").close()
            os.chmod(path, stat.S_IRWXU)
            import subprocess
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = subprocess.CompletedProcess(
                    args=[], returncode=0, stdout="myprog 1.0\n", stderr=""
                )
                result = probe_binary("myprog", [d])
            self.assertTrue(result["present"])
            self.assertEqual(result["version_string"], "myprog 1.0")
            self.assertEqual(result["query"], "myprog")

    def test_missing_binary(self):
        result = probe_binary("__no_such_binary_xyz__", [])
        self.assertFalse(result["present"])
        self.assertEqual(result["path"], "")
        self.assertEqual(result["version_string"], "")


class TestProbeBinaryRundown(unittest.TestCase):
    def test_collects_executables(self):
        with tempfile.TemporaryDirectory() as d:
            for name in ["alpha", "beta", "gamma"]:
                p = os.path.join(d, name)
                open(p, "w").close()
                os.chmod(p, stat.S_IRWXU)
            # non-executable should be excluded
            non_exec = os.path.join(d, "notexec")
            open(non_exec, "w").close()
            os.chmod(non_exec, stat.S_IRUSR)

            with patch.dict(os.environ, {"PATH": d}):
                result = probe_binary_rundown([])

            self.assertIn("alpha", result["manifest"])
            self.assertIn("beta", result["manifest"])
            self.assertIn("gamma", result["manifest"])
            self.assertNotIn("notexec", result["manifest"])
            self.assertEqual(result["manifest_binary_count"], 3)

    def test_first_occurrence_wins(self):
        with tempfile.TemporaryDirectory() as d1, tempfile.TemporaryDirectory() as d2:
            for d, content in [(d1, "/first"), (d2, "/second")]:
                p = os.path.join(d, "samebinary")
                open(p, "w").close()
                os.chmod(p, stat.S_IRWXU)

            with patch.dict(os.environ, {"PATH": d1 + os.pathsep + d2}):
                result = probe_binary_rundown([])

            self.assertIn("samebinary", result["manifest"])
            self.assertIn(d1, result["manifest"]["samebinary"]["path"])


if __name__ == "__main__":
    unittest.main()
