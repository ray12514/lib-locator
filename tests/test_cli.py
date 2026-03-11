import unittest

from cli import classify_scheduler_node, normalize_node_type


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

    def test_classify_scheduler_node(self) -> None:
        pbs_meta_vis = {
            "resources_available.nodetype": "vis,viz",
            "resources_available.clustertype": "debug,all12,bp,batch,long",
            "resources_available.bigmem": "0",
            "resources_available.compute": "0",
        }
        pbs_meta_bigmem = {
            "resources_available.nodetype": "compute",
            "resources_available.bigmem": "1",
            "resources_available.compute": "1",
        }
        slurm_meta_transfer = {
            "resources_available.nodetype": "",
            "scheduler.partition": "transfer",
        }
        slurm_meta_compute = {
            "resources_available.nodetype": "",
            "scheduler.partition": "standard,interactive",
        }

        self.assertEqual(classify_scheduler_node("pbs", "ruth-g01", pbs_meta_vis), "visualization")
        self.assertEqual(classify_scheduler_node("pbs", "node001", pbs_meta_bigmem), "bigmem")
        self.assertEqual(classify_scheduler_node("slurm", "jean-dtn01", slurm_meta_transfer), "transfer")
        self.assertEqual(classify_scheduler_node("slurm", "jean675", slurm_meta_compute), "compute")


if __name__ == "__main__":
    unittest.main()
