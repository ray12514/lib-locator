import unittest

from slurm import classify_node, select_compute_nodes, state_is_online


class TestSlurmStateAndClass(unittest.TestCase):
    def test_state_is_online(self) -> None:
        self.assertTrue(state_is_online("idle"))
        self.assertTrue(state_is_online("mix"))
        self.assertFalse(state_is_online("down"))
        self.assertFalse(state_is_online("draining"))

    def test_classify_node(self) -> None:
        self.assertEqual(classify_node("dtn01", "compute"), "transfer")
        self.assertEqual(classify_node("node01", "transfer"), "transfer")
        self.assertEqual(classify_node("node01", "compute"), "compute")


class TestSlurmSelection(unittest.TestCase):
    def test_select_compute_nodes(self) -> None:
        inv = {
            "node001": {
                "state": "idle",
                "resources_available.nodetype": "compute",
                "resources_available.compute": "1",
            },
            "dtn01": {
                "state": "idle",
                "resources_available.nodetype": "transfer",
                "resources_available.compute": "0",
            },
            "node002": {
                "state": "down",
                "resources_available.nodetype": "compute",
                "resources_available.compute": "1",
            },
        }

        selected, skipped = select_compute_nodes(
            inv,
            online_only=True,
            compute_flag_only=True,
        )

        self.assertEqual(selected, ["node001"])
        reasons = {(node, reason) for node, reason, *_ in skipped}
        self.assertIn(("dtn01", "non_compute"), reasons)
        self.assertIn(("node002", "offline_or_down"), reasons)


if __name__ == "__main__":
    unittest.main()
