import unittest

from libsweep.report import build_rundown_section


class TestReportRundown(unittest.TestCase):
    def test_disabled_returns_empty(self) -> None:
        out = build_rundown_section(
            enabled=False,
            triggered=False,
            reference_node="",
            reference_role="",
            scanned_nodes=[],
            discrepancy_rows=[],
            discrepancy_csv="",
            nodes_txt="",
        )
        self.assertEqual(out, "")

    def test_triggered_contains_summary(self) -> None:
        out = build_rundown_section(
            enabled=True,
            triggered=True,
            reference_node="login01",
            reference_role="login",
            scanned_nodes=[
                {"node": "login01", "role": "login", "status": "scanned"},
                {"node": "c1", "role": "compute", "status": "scanned"},
                {"node": "c2", "role": "compute", "status": "error"},
            ],
            discrepancy_rows=[
                {"lib_root": "libfoo", "discrepancy_kind": "majors_diff",
                 "reference_majors": "6", "node_majors": "5"},
                {"lib_root": "libbar", "discrepancy_kind": "missing_on_node",
                 "reference_majors": "1", "node_majors": ""},
            ],
            discrepancy_csv="out_rundown_discrepancies.csv",
            nodes_txt="out_rundown_nodes.txt",
        )
        self.assertIn("discrepancy rundown", out)
        self.assertIn("login01 (login)", out)
        self.assertIn("Compared node: c1", out)
        self.assertIn("Libraries differing from reference: 2", out)
        self.assertIn("libfoo", out)
        self.assertIn("SONAME majors differ", out)
        self.assertIn("libbar", out)
        self.assertIn("missing on compute node", out)
        self.assertIn("out_rundown_discrepancies.csv", out)

    def test_not_triggered_says_consistent(self) -> None:
        out = build_rundown_section(
            enabled=True,
            triggered=False,
            reference_node="",
            reference_role="",
            scanned_nodes=[],
            discrepancy_rows=[],
            discrepancy_csv="",
            nodes_txt="out_rundown_nodes.txt",
        )
        self.assertIn("All nodes consistent", out)
        self.assertNotIn("Compared node", out)


if __name__ == "__main__":
    unittest.main()
