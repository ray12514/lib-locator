import unittest
from types import SimpleNamespace
from unittest.mock import patch

from slurm import classify_node, resolve_node_type, select_compute_nodes, slurm_inventory, state_is_online


class TestSlurmStateAndClass(unittest.TestCase):
    def test_state_is_online(self) -> None:
        self.assertTrue(state_is_online("idle"))
        self.assertTrue(state_is_online("mix"))
        self.assertFalse(state_is_online("down"))
        self.assertFalse(state_is_online("draining"))

    def test_classify_node(self) -> None:
        self.assertEqual(classify_node("dtn01", "compute"), "transfer")
        self.assertEqual(classify_node("jean-dtn01", "", "transfer"), "transfer")
        self.assertEqual(classify_node("jean-v01", "", "viz"), "visualization")
        self.assertEqual(classify_node("jean-v01", "", "", "srd:1"), "visualization")
        self.assertEqual(classify_node("jean-lm01", "", "", "lm:1"), "bigmem")
        self.assertEqual(classify_node("node01", "transfer"), "transfer")
        self.assertEqual(classify_node("node01", "compute"), "compute")

    def test_resolve_node_type(self) -> None:
        self.assertEqual(resolve_node_type("jean675", "aiml", "standard,interactive"), "aiml")
        self.assertEqual(resolve_node_type("jean675", "", "high,debug"), "compute")
        self.assertEqual(resolve_node_type("jean-dtn01", "", "transfer"), "transfer")
        self.assertEqual(resolve_node_type("jean-v01", "", "interactive", "srd:1"), "visualization")
        self.assertEqual(resolve_node_type("jean-lm01", "", "standard", "lm:1"), "bigmem")
        self.assertEqual(resolve_node_type("jean-hp01", "", "standard", "highperf:1"), "highperf")


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

    def test_inventory_fills_missing_nodetype_from_class(self) -> None:
        raw = "\n".join([
            "node001|idle|compute|(null)|(null)",
            "dtn01|idle|transfer|(null)|(null)",
        ])
        with patch("slurm.run", return_value=SimpleNamespace(returncode=0, stdout=raw, stderr="")):
            _, _, inv = slurm_inventory()

        self.assertEqual(inv["node001"]["resources_available.nodetype"], "compute")
        self.assertEqual(inv["dtn01"]["resources_available.nodetype"], "transfer")

    def test_inventory_aggregates_partitions_and_marks_transfer(self) -> None:
        raw = "\n".join([
            "jean675|allocated|background|(null)|(null)",
            "jean675|allocated|standard*|(null)|(null)",
            "jean-dtn01|idle|transfer|(null)|(null)",
            "jean-v01|idle|interactive|(null)|srd:1",
        ])
        with patch("slurm.run", return_value=SimpleNamespace(returncode=0, stdout=raw, stderr="")):
            _, _, inv = slurm_inventory()

        self.assertEqual(inv["jean675"]["resources_available.compute"], "1")
        self.assertEqual(inv["jean-dtn01"]["resources_available.nodetype"], "transfer")
        self.assertEqual(inv["jean-dtn01"]["resources_available.compute"], "0")
        self.assertEqual(inv["jean675"]["scheduler.partition"], "background,standard")
        self.assertEqual(inv["jean-v01"]["resources_available.nodetype"], "visualization")
        self.assertEqual(inv["jean-v01"]["scheduler.gres"], "srd:1")

    def test_inventory_falls_back_when_gres_format_unavailable(self) -> None:
        first = SimpleNamespace(returncode=1, stdout="", stderr="bad format")
        second = SimpleNamespace(returncode=0, stdout="node001|idle|standard|(null)", stderr="")
        with patch("slurm.run", side_effect=[first, second]):
            _, _, inv = slurm_inventory()

        self.assertIn("node001", inv)
        self.assertEqual(inv["node001"].get("scheduler.gres", ""), "")


if __name__ == "__main__":
    unittest.main()
