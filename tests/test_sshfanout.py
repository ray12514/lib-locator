import unittest

from sshfanout import classify_ssh_failure, default_ssh_config


class TestSSHFanout(unittest.TestCase):
    def test_base_args_detach_stdin(self) -> None:
        cfg = default_ssh_config()
        args = cfg.base_args()
        self.assertIn("-n", args)
        self.assertIn("StdinNull=yes", args)

    def test_classify_ok(self) -> None:
        self.assertEqual(classify_ssh_failure(0, ""), "ok")

    def test_classify_transport_errors(self) -> None:
        self.assertEqual(classify_ssh_failure(255, "ssh: Could not resolve hostname foo"), "dns")
        self.assertEqual(classify_ssh_failure(255, "Permission denied (publickey)."), "policy_denied")
        self.assertEqual(classify_ssh_failure(255, "Connection timed out"), "timeout")

    def test_classify_remote_exec_error(self) -> None:
        self.assertEqual(classify_ssh_failure(1, "Traceback: boom"), "remote_exec_error")
        self.assertEqual(classify_ssh_failure(127, "bash: python3: command not found"), "remote_exec_error")
        self.assertEqual(classify_ssh_failure(1, "Permission denied: /opt/tool"), "remote_exec_error")


if __name__ == "__main__":
    unittest.main()
