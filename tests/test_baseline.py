import unittest

from baseline import compute_baseline_majors, majors_set_from_row


class TestBaseline(unittest.TestCase):
    def test_majors_set_from_row(self) -> None:
        row = {"majors": " 62, 8,abc, 62 ,, "}
        self.assertEqual(majors_set_from_row(row), {8, 62})

    def test_baseline_major_override_wins(self) -> None:
        rows = [{"majors": "8,9", "primary_major": "8"}]
        out = compute_baseline_majors("libjpeg", rows, "login-consensus", 62)
        self.assertEqual(out, {62})

    def test_pinned_major_from_query_wins(self) -> None:
        rows = [{"majors": "8,9", "primary_major": "8"}]
        out = compute_baseline_majors("libjpeg.so.62", rows, "login-union", None)
        self.assertEqual(out, {62})

    def test_login_consensus_uses_primary_major_mode(self) -> None:
        rows = [
            {"majors": "8,9", "primary_major": "9"},
            {"majors": "8", "primary_major": "8"},
            {"majors": "8,10", "primary_major": "8"},
        ]
        out = compute_baseline_majors("libjpeg", rows, "login-consensus", None)
        self.assertEqual(out, {8})

    def test_union_and_intersection(self) -> None:
        rows = [
            {"majors": "8,9", "primary_major": "8"},
            {"majors": "8,10", "primary_major": "8"},
        ]
        u = compute_baseline_majors("libjpeg", rows, "login-union", None)
        i = compute_baseline_majors("libjpeg", rows, "login-intersection", None)
        self.assertEqual(u, {8, 9, 10})
        self.assertEqual(i, {8})

    def test_none_or_no_login_rows(self) -> None:
        rows = [{"majors": "8,9", "primary_major": "8"}]
        self.assertEqual(compute_baseline_majors("libjpeg", rows, "none", None), set())
        self.assertEqual(compute_baseline_majors("libjpeg", [], "login-union", None), set())


if __name__ == "__main__":
    unittest.main()
