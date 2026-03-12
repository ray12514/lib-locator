import unittest

from report import build_rundown_section


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
                {"node": "login01", "status": "scanned"},
                {"node": "c1", "status": "scanned"},
                {"node": "c2", "status": "error"},
            ],
            discrepancy_rows=[
                {"discrepancy_kind": "majors_diff"},
                {"discrepancy_kind": "versions_diff"},
            ],
            discrepancy_csv="out_rundown_discrepancies.csv",
            nodes_txt="out_rundown_nodes.txt",
        )
        self.assertIn("discrepancy_rundown", out)
        self.assertIn("Reference node: login01 (login)", out)
        self.assertIn("Discrepancies found: 2", out)


if __name__ == "__main__":
    unittest.main()
