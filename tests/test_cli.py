import unittest

from cli import normalize_node_type


class TestCliHelpers(unittest.TestCase):
    def test_normalize_node_type(self) -> None:
        self.assertEqual(normalize_node_type(""), "compute")
        self.assertEqual(normalize_node_type("  "), "compute")
        self.assertEqual(normalize_node_type("gpu"), "gpu")
        self.assertEqual(normalize_node_type("gpu,ib"), "gpu")
        self.assertEqual(normalize_node_type("GPU,IB"), "gpu")
        self.assertEqual(normalize_node_type("jean-transfer"), "transfer")
        self.assertEqual(normalize_node_type("vis,viz"), "visualization")
        self.assertEqual(normalize_node_type("compute:bigmem"), "bigmem")


if __name__ == "__main__":
    unittest.main()
