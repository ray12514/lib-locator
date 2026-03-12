import unittest

from cli import (
    build_discrepancy_representatives,
    clamp_workers,
    classify_scheduler_node,
    compare_rundown_manifests,
    normalize_node_type,
    resolve_scheduler_node_type,
    select_rundown_reference_node,
)


class TestCliHelpers(unittest.TestCase):
    def test_clamp_workers(self) -> None:
        self.assertEqual(clamp_workers(0), 1)
        self.assertEqual(clamp_workers(1), 1)
        self.assertEqual(clamp_workers(32), 32)
        self.assertEqual(clamp_workers(129), 128)

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

    def test_resolve_scheduler_node_type(self) -> None:
        pbs_meta = {
            "resources_available.nodetype": "aiml,highperf",
            "resources_available.clustertype": "debug,batch,long",
            "resources_available.bigmem": "0",
            "resources_available.compute": "1",
        }
        pbs_bigmem = {
            "resources_available.nodetype": "aiml,highperf",
            "resources_available.clustertype": "debug,batch,long",
            "resources_available.bigmem": "1",
            "resources_available.compute": "1",
        }
        pbs_transfer = {
            "resources_available.nodetype": "compute",
            "resources_available.clustertype": "transfer,batch,long",
            "resources_available.bigmem": "0",
            "resources_available.compute": "0",
        }
        slurm_feature = {
            "resources_available.nodetype": "aiml",
            "scheduler.partition": "standard,interactive",
        }
        slurm_generic_partition = {
            "resources_available.nodetype": "",
            "scheduler.partition": "high,debug",
        }
        slurm_transfer_partition = {
            "resources_available.nodetype": "",
            "scheduler.partition": "transfer",
        }

        self.assertEqual(resolve_scheduler_node_type("pbs", "node001", pbs_meta), "aiml")
        self.assertEqual(resolve_scheduler_node_type("pbs", "node001", pbs_bigmem), "bigmem")
        self.assertEqual(resolve_scheduler_node_type("pbs", "node001", pbs_transfer), "transfer")
        self.assertEqual(resolve_scheduler_node_type("slurm", "jean675", slurm_feature), "aiml")
        self.assertEqual(resolve_scheduler_node_type("slurm", "jean675", slurm_generic_partition), "compute")
        self.assertEqual(resolve_scheduler_node_type("slurm", "jean-dtn01", slurm_transfer_partition), "transfer")

    def test_build_discrepancy_representatives(self) -> None:
        rows = [
            {"node": "n1", "lib_query": "liba", "result": "inconsistent", "found_majors": "1", "missing_required_majors": "2"},
            {"node": "n2", "lib_query": "liba", "result": "inconsistent", "found_majors": "1", "missing_required_majors": "2"},
            {"node": "n3", "lib_query": "liba", "result": "missing", "found_majors": "", "missing_required_majors": "2"},
            {"node": "n4", "lib_query": "liba", "result": "consistent", "found_majors": "2", "missing_required_majors": ""},
        ]
        reps = build_discrepancy_representatives(rows)
        self.assertEqual(len(reps), 2)
        by_result = {r["result"]: r for r in reps}
        self.assertEqual(by_result["inconsistent"]["node"], "n1")
        self.assertEqual(by_result["inconsistent"]["group_size"], 2)
        self.assertEqual(by_result["missing"]["node"], "n3")

    def test_select_rundown_reference_node(self) -> None:
        login_rows = [
            {"node": "login02", "result": "observed"},
            {"node": "login01", "result": "observed"},
        ]
        compute_rows = [
            {"node": "c1", "result": "consistent"},
            {"node": "c2", "result": "inconsistent"},
        ]
        node, role = select_rundown_reference_node(login_rows, compute_rows, {"login01"})
        self.assertEqual((node, role), ("login02", "login"))

    def test_compare_rundown_manifests(self) -> None:
        reference = {
            "liba": {"majors": [1], "versions": ["1.0"], "variants_count": 2},
            "libb": {"majors": [2], "versions": ["2.1"], "variants_count": 1},
        }
        node_manifest = {
            "liba": {"majors": [1], "versions": ["1.1"], "variants_count": 1},
            "libc": {"majors": [3], "versions": ["3.0"], "variants_count": 1},
        }
        trigger = {
            "lib_query": "libz",
            "result": "inconsistent",
            "found_majors": "1",
            "missing_required_majors": "2",
        }
        rows = compare_rundown_manifests("login01", reference, "c2", node_manifest, trigger)
        kinds = {(r["lib_root"], r["discrepancy_kind"]) for r in rows}
        self.assertIn(("liba", "versions_diff"), kinds)
        self.assertIn(("libb", "missing_on_node"), kinds)
        self.assertIn(("libc", "extra_on_node"), kinds)


if __name__ == "__main__":
    unittest.main()
