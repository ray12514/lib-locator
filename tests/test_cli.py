import unittest

from cli import normalize_node_type


class TestCliHelpers(unittest.TestCase):
    def test_normalize_node_type(self) -> None:
        self.assertEqual(normalize_node_type(""), "standard")
        self.assertEqual(normalize_node_type("  "), "standard")
        self.assertEqual(normalize_node_type("gpu"), "gpu")
        self.assertEqual(normalize_node_type("gpu,ib"), "gpu")
        self.assertEqual(normalize_node_type("GPU,IB"), "gpu")


if __name__ == "__main__":
    unittest.main()
