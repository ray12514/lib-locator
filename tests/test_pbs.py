import unittest
from types import SimpleNamespace
from unittest.mock import patch

from pbs import classify_node, parse_pbsnodes_a, pbs_inventory, select_compute_nodes, state_is_online


class TestPBSParsing(unittest.TestCase):
    def test_parse_pbsnodes_a(self) -> None:
        raw = """
node001
    state = free
    resources_available.nodetype = compute
    resources_available.compute = 1

dtn01
    state = free
    resources_available.nodetype = transfer
    resources_available.compute = 0
""".strip()

        inv = parse_pbsnodes_a(raw)

        self.assertIn("node001", inv)
        self.assertEqual(inv["node001"]["state"], "free")
        self.assertEqual(inv["node001"]["resources_available.compute"], "1")
        self.assertEqual(inv["dtn01"]["resources_available.nodetype"], "transfer")

    def test_state_is_online(self) -> None:
        self.assertTrue(state_is_online("free"))
        self.assertTrue(state_is_online("job-exclusive,busy"))
        self.assertFalse(state_is_online("down"))
        self.assertFalse(state_is_online("free,offline"))

    def test_classify_node_aliases(self) -> None:
        self.assertEqual(classify_node("jean-dtn01", ""), "transfer")
        self.assertEqual(classify_node("ruth-g01", "vis,viz"), "visualization")
        self.assertEqual(classify_node("node01", "compute", bigmem="1"), "bigmem")


class TestPBSSelection(unittest.TestCase):
    def test_select_compute_nodes_online_and_compute_flag_only(self) -> None:
        inv = {
            "node001": {
                "state": "free",
                "resources_available.nodetype": "compute",
                "resources_available.compute": "1",
            },
            "dtn01": {
                "state": "free",
                "resources_available.nodetype": "transfer",
                "resources_available.compute": "0",
            },
            "node002": {
                "state": "offline",
                "resources_available.nodetype": "compute",
                "resources_available.compute": "1",
            },
            "node003": {
                "state": "free",
                "resources_available.nodetype": "compute",
                "resources_available.compute": "0",
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
        self.assertIn(("node003", "non_compute"), reasons)
        self.assertIn(("node002", "offline_or_down"), reasons)

    def test_inventory_fills_missing_nodetype(self) -> None:
        raw = """
node001
    state = free
    resources_available.compute = 1

dtn01
    state = free
    resources_available.compute = 0
""".strip()

        with patch("pbs.run", return_value=SimpleNamespace(returncode=0, stdout=raw, stderr="")):
            _, _, inv = pbs_inventory()

        self.assertEqual(inv["node001"]["resources_available.nodetype"], "compute")
        self.assertEqual(inv["dtn01"]["resources_available.nodetype"], "transfer")


if __name__ == "__main__":
    unittest.main()
