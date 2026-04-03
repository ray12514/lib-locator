import argparse
import csv
import json
import os
import re
import shutil
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict, List, Set, Tuple

from .sshfanout import default_ssh_config, ssh_with_retries, short_hostname
from .pbs import (
    classify_node as pbs_classify_node,
    pbs_inventory,
    resolve_node_type as pbs_resolve_node_type,
    select_compute_nodes as pbs_select_compute_nodes,
)
from .slurm import (
    classify_node as slurm_classify_node,
    resolve_node_type as slurm_resolve_node_type,
    select_compute_nodes as slurm_select_compute_nodes,
    slurm_inventory,
)
from .probe import probe_node, probe_rundown, probe_binary, probe_binary_rundown
from .baseline import compute_baseline_majors, compute_binary_baseline
from .report import (
    build_report, build_rundown_section, write_node_lists, write_scheduler_skipped,
    build_binary_report, write_binary_node_lists,
)


EXAMPLES = """Library sweep examples:
  Inventory only (no compatibility judgement):
    libsweep --lib libjpeg --scope all --login-auto --baseline-from none --workers 32

  Sweep all and require SONAME major 62:
    libsweep --lib libjpeg.so.62 --scope all --login-auto --baseline-from login-consensus --workers 32

  Force baseline major 62:
    libsweep --lib libjpeg --scope all --login-auto --baseline-major 62 --workers 32

  Multiple libraries, with discrepancy rundown:
    libsweep --lib libjpeg --lib libpng --scope all --login-auto --discrepancy-rundown --workers 32

  Slurm compute-only inventory:
    libsweep --scheduler slurm --lib libjpeg --scope compute --workers 32

Binary sweep examples:
  Check python3 and mpirun are consistent across all compute nodes:
    libsweep --binary python3 --binary mpirun --scope all --login-auto --workers 32

  Binary inventory only (no version baseline):
    libsweep --binary python3 --scope all --login-auto --baseline-from none --workers 32

  Binary sweep with discrepancy rundown (full PATH manifest diff):
    libsweep --binary python3 --scope all --login-auto --discrepancy-rundown --workers 32

  Print this examples menu:
    libsweep --examples
"""

MAX_WORKERS = 128


# ---------------------------------------------------------------------------
# Scheduler helpers
# ---------------------------------------------------------------------------

def detect_scheduler(mode: str) -> str:
    if mode in ("pbs", "slurm"):
        return mode
    if any(k in os.environ for k in ("SLURM_JOB_ID", "SLURM_CLUSTER_NAME", "SLURM_NTASKS")):
        return "slurm"
    if any(k in os.environ for k in ("PBS_JOBID", "PBS_NODEFILE", "PBS_ENVIRONMENT")):
        return "pbs"
    if shutil.which("sinfo"):
        return "slurm"
    if shutil.which("pbsnodes"):
        return "pbs"
    return "pbs"


def classify_scheduler_node(active_scheduler: str, node: str, meta: Dict[str, str]) -> str:
    nodetype = meta.get("resources_available.nodetype", "")
    if active_scheduler == "slurm":
        return slurm_classify_node(
            node, nodetype,
            meta.get("scheduler.partition", ""),
            meta.get("scheduler.gres", ""),
        )
    return pbs_classify_node(
        node, nodetype,
        meta.get("resources_available.clustertype", ""),
        meta.get("resources_available.bigmem", ""),
        meta.get("resources_available.compute", ""),
    )


def resolve_scheduler_node_type(active_scheduler: str, node: str, meta: Dict[str, str]) -> str:
    nodetype = meta.get("resources_available.nodetype", "")
    if active_scheduler == "slurm":
        return slurm_resolve_node_type(
            node, nodetype,
            meta.get("scheduler.partition", ""),
            meta.get("scheduler.gres", ""),
        )
    return pbs_resolve_node_type(
        node, nodetype,
        meta.get("resources_available.clustertype", ""),
        meta.get("resources_available.bigmem", ""),
        meta.get("resources_available.compute", ""),
    )


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def clamp_workers(requested: int, cap: int = MAX_WORKERS) -> int:
    if requested < 1:
        return 1
    return requested if requested <= cap else cap


def configure_thread_stack_size() -> str:
    kb_raw = os.environ.get("LIBSWEEP_THREAD_STACK_KB", "1024").strip()
    if not kb_raw:
        return "default"
    try:
        kb = int(kb_raw)
    except ValueError:
        return "default"
    if kb <= 0:
        return "default"
    try:
        threading.stack_size(kb * 1024)
        return f"{kb}KiB"
    except (ValueError, RuntimeError):
        return "default"


def write_csv(path: str, fieldnames: List[str], rows: List[Dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in sorted(rows, key=lambda x: (x.get("lib_query") or x.get("binary_query", ""), x.get("node", ""))):
            w.writerow({k: row.get(k, "") for k in fieldnames})


def json_lines_only(stdout: str) -> List[str]:
    out = []
    for ln in (stdout or "").splitlines():
        s = ln.strip()
        if s.startswith("{") and s.endswith("}"):
            out.append(s)
    return out


def add_bool_option(ap: argparse.ArgumentParser, name: str, default: bool, help_text: str = "") -> None:
    if hasattr(argparse, "BooleanOptionalAction"):
        ap.add_argument(name, action=argparse.BooleanOptionalAction, default=default, help=help_text)
        return
    dest = name.lstrip("-").replace("-", "_")
    ap.add_argument(name, dest=dest, action="store_true", default=default, help=help_text)
    ap.add_argument(f"--no-{name[2:]}", dest=dest, action="store_false")


# ---------------------------------------------------------------------------
# Discrepancy rundown helpers
# ---------------------------------------------------------------------------

def discrepancy_signature(row: Dict) -> tuple:
    return (
        str(row.get("lib_query", "")),
        str(row.get("result", "")),
        str(row.get("found_majors", "")),
        str(row.get("missing_required_majors", "")),
    )


def build_discrepancy_representatives(compute_rows: List[Dict]) -> List[Dict]:
    groups: Dict[tuple, List[Dict]] = {}
    for row in compute_rows:
        if row.get("result") not in ("inconsistent", "missing"):
            continue
        sig = discrepancy_signature(row)
        groups.setdefault(sig, []).append(row)

    reps: List[Dict] = []
    for sig, rows in groups.items():
        nodes = sorted({str(r.get("node", "")) for r in rows if r.get("node")})
        if not nodes:
            continue
        reps.append({
            "node": nodes[0],
            "group_size": len(nodes),
            "group_nodes": ",".join(nodes),
            "lib_query": sig[0],
            "result": sig[1],
            "found_majors": sig[2],
            "missing_required_majors": sig[3],
        })
    reps.sort(key=lambda r: (r["lib_query"], r["result"], r["node"]))
    return reps


def select_rundown_reference_node(
    login_rows: List[Dict], compute_rows: List[Dict], avoid_nodes: Set[str]
) -> Tuple[str, str]:
    login_nodes = sorted({
        str(r.get("node", ""))
        for r in login_rows
        if r.get("result") == "observed" and r.get("node")
    })
    for n in login_nodes:
        if n not in avoid_nodes:
            return n, "login"
    if login_nodes:
        return login_nodes[0], "login"

    compute_consistent = sorted({
        str(r.get("node", ""))
        for r in compute_rows
        if r.get("result") == "consistent" and r.get("node")
    })
    for n in compute_consistent:
        if n not in avoid_nodes:
            return n, "compute"
    if compute_consistent:
        return compute_consistent[0], "compute"

    compute_ok = sorted({
        str(r.get("node", ""))
        for r in compute_rows
        if r.get("result") in ("consistent", "inconsistent", "missing") and r.get("node")
    })
    for n in compute_ok:
        if n not in avoid_nodes:
            return n, "compute"
    if compute_ok:
        return compute_ok[0], "compute"

    return "", ""


def _int_set(values) -> Set[int]:
    out: Set[int] = set()
    for v in values or []:
        try:
            out.add(int(v))
        except (TypeError, ValueError):
            continue
    return out


def _str_set(values) -> Set[str]:
    return {str(v) for v in (values or []) if str(v)}


def _int_csv(values: Set[int]) -> str:
    return ",".join(str(v) for v in sorted(values))


def _str_csv(values: Set[str]) -> str:
    return ",".join(sorted(values))


def _make_rundown_row(
    reference_node: str, node: str, lib_root: str, kind: str,
    ref_majors: str, node_majors: str,
    ref_versions: str, node_versions: str,
    ref_variants: str, node_variants: str,
    trigger: Dict,
) -> Dict:
    return {
        "reference_node": reference_node,
        "node": node,
        "lib_root": lib_root,
        "discrepancy_kind": kind,
        "reference_majors": ref_majors,
        "node_majors": node_majors,
        "reference_versions": ref_versions,
        "node_versions": node_versions,
        "reference_variants": ref_variants,
        "node_variants": node_variants,
        "trigger_lib_query": trigger.get("lib_query", ""),
        "trigger_result": trigger.get("result", ""),
        "trigger_found_majors": trigger.get("found_majors", ""),
        "trigger_missing_required_majors": trigger.get("missing_required_majors", ""),
    }


def compare_rundown_manifests(
    reference_node: str, reference_manifest: Dict,
    node: str, node_manifest: Dict,
    trigger: Dict,
) -> List[Dict]:
    rows: List[Dict] = []
    roots = sorted(set(reference_manifest.keys()) | set(node_manifest.keys()))
    for root in roots:
        ref = reference_manifest.get(root)
        cur = node_manifest.get(root)
        ref_dict = ref if isinstance(ref, dict) else {}
        cur_dict = cur if isinstance(cur, dict) else {}

        if ref is None:
            rows.append(_make_rundown_row(
                reference_node, node, root, "extra_on_node",
                ref_majors="",
                node_majors=_int_csv(_int_set(cur_dict.get("majors", []))),
                ref_versions="",
                node_versions=_str_csv(_str_set(cur_dict.get("versions", []))),
                ref_variants="0",
                node_variants=str(cur_dict.get("variants_count", 0)),
                trigger=trigger,
            ))
            continue

        if cur is None:
            rows.append(_make_rundown_row(
                reference_node, node, root, "missing_on_node",
                ref_majors=_int_csv(_int_set(ref_dict.get("majors", []))),
                node_majors="",
                ref_versions=_str_csv(_str_set(ref_dict.get("versions", []))),
                node_versions="",
                ref_variants=str(ref_dict.get("variants_count", 0)),
                node_variants="0",
                trigger=trigger,
            ))
            continue

        ref_majors = _int_set(ref_dict.get("majors", []))
        cur_majors = _int_set(cur_dict.get("majors", []))
        ref_versions = _str_set(ref_dict.get("versions", []))
        cur_versions = _str_set(cur_dict.get("versions", []))

        if ref_majors != cur_majors:
            rows.append(_make_rundown_row(
                reference_node, node, root, "majors_diff",
                ref_majors=_int_csv(ref_majors),
                node_majors=_int_csv(cur_majors),
                ref_versions=_str_csv(ref_versions),
                node_versions=_str_csv(cur_versions),
                ref_variants=str(ref_dict.get("variants_count", 0)),
                node_variants=str(cur_dict.get("variants_count", 0)),
                trigger=trigger,
            ))
            continue

        if ref_versions != cur_versions:
            rows.append(_make_rundown_row(
                reference_node, node, root, "versions_diff",
                ref_majors=_int_csv(ref_majors),
                node_majors=_int_csv(cur_majors),
                ref_versions=_str_csv(ref_versions),
                node_versions=_str_csv(cur_versions),
                ref_variants=str(ref_dict.get("variants_count", 0)),
                node_variants=str(cur_dict.get("variants_count", 0)),
                trigger=trigger,
            ))

    return rows


def compare_binary_rundown_manifests(
    reference_node: str, reference_manifest: Dict,
    node: str, node_manifest: Dict,
    trigger: Dict,
) -> List[Dict]:
    """Diff two binary manifests {name: {path}} and return discrepancy rows."""
    rows: List[Dict] = []
    names = sorted(set(reference_manifest.keys()) | set(node_manifest.keys()))
    trigger_fields = {
        "trigger_binary_query": trigger.get("binary_query", ""),
        "trigger_result": trigger.get("result", ""),
        "trigger_version_string": trigger.get("version_string", ""),
        "trigger_required_version": trigger.get("required_version", ""),
    }
    for name in names:
        ref = reference_manifest.get(name)
        cur = node_manifest.get(name)
        if ref is None:
            rows.append(dict(
                reference_node=reference_node, node=node, binary_name=name,
                discrepancy_kind="extra_on_node",
                reference_path="", node_path=(cur or {}).get("path", ""),
                **trigger_fields,
            ))
        elif cur is None:
            rows.append(dict(
                reference_node=reference_node, node=node, binary_name=name,
                discrepancy_kind="missing_on_node",
                reference_path=(ref or {}).get("path", ""), node_path="",
                **trigger_fields,
            ))
        elif ref.get("path") != cur.get("path"):
            rows.append(dict(
                reference_node=reference_node, node=node, binary_name=name,
                discrepancy_kind="path_diff",
                reference_path=ref.get("path", ""), node_path=cur.get("path", ""),
                **trigger_fields,
            ))
    return rows


# ---------------------------------------------------------------------------
# main() pipeline stages
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        description="Cluster library and binary inventory and compatibility sweep",
        epilog=EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    ap.add_argument("--examples", action="store_true", help="Show usage examples and exit")

    # Library sweep
    ap.add_argument("--lib", action="append", required=False,
                    help="Repeatable library query. Examples: libjpeg  libjpeg.so.62  (mutually exclusive with --binary)")
    ap.add_argument("--dirs", action="append", default=[], help="Extra directory globs to scan for libraries")
    ap.add_argument("--no-ldconfig", action="store_true", help="Skip ldconfig -p lookup")

    # Binary sweep
    ap.add_argument("--binary", action="append", default=[],
                    help="Repeatable binary name to locate. Examples: python3  mpirun  (mutually exclusive with --lib)")
    ap.add_argument("--binary-dirs", action="append", default=[],
                    help="Extra directories to search for binaries (supplements PATH)")

    ap.add_argument("--scope", choices=["login", "compute", "all"], default=None,
                    help="Nodes to probe. Default: all (or compute when running inside a scheduler job)")
    ap.add_argument("--scheduler", choices=["auto", "pbs", "slurm"], default="auto",
                    help="Scheduler inventory backend (default: auto-detect)")
    ap.add_argument("--login-auto", action="store_true",
                    help="Auto-discover login nodes by prefixNN pattern via SSH")
    ap.add_argument("--login-prefix", default=None, help="Override inferred login node hostname prefix")
    ap.add_argument("--login-width", type=int, default=None, help="Override numeric suffix width for login node discovery")
    ap.add_argument("--login-max", type=int, default=50, help="Max login hosts to probe (default: 50)")
    ap.add_argument("--login-stop-after-gap", type=int, default=6,
                    help="Stop login discovery after this many consecutive misses (default: 6)")

    add_bool_option(ap, "--pbs-online-only", True,
                    "Include only online PBS nodes (default: enabled)")
    add_bool_option(ap, "--pbs-compute-flag-only", True,
                    "Exclude transfer-class nodes while keeping other online classes (default: enabled)")

    ap.add_argument("--baseline-from",
                    choices=["login-consensus", "login-union", "login-intersection", "none"],
                    default="login-consensus",
                    help="How to derive the baseline from login node data (default: login-consensus)")
    ap.add_argument("--baseline-major", type=int, default=None,
                    help="Hard override for required SONAME major version (library sweep only)")

    ap.add_argument("--remote-python", default="python3",
                    help="Remote Python interpreter to use (default: python3)")
    ap.add_argument("--workers", type=int, default=32,
                    help="Parallel SSH fanout workers (default: 32, max: 128)")
    ap.add_argument("--ssh-timeout", type=int, default=120,
                    help="Per-node SSH timeout in seconds (default: 120)")
    ap.add_argument("--retries", type=int, default=2,
                    help="Retry count for transient SSH failures (default: 2)")
    ap.add_argument("--discrepancy-rundown-workers", type=int, default=8,
                    help="Worker pool size for the discrepancy rundown scan (default: 8)")

    ap.add_argument("--ssh-hostkey", choices=["accept-new", "no", "yes"], default="accept-new",
                    help="SSH StrictHostKeyChecking mode (default: accept-new)")
    ap.add_argument("--ssh-known-hosts", default=None,
                    help="Override known_hosts file path")
    add_bool_option(ap, "--ssh-control-master", False,
                    "Enable OpenSSH connection reuse (default: disabled)")
    add_bool_option(ap, "--remote-low-priority", True,
                    "Run remote probes with low CPU priority via nice -n 19 (default: enabled)")

    ap.add_argument("--out-prefix", default="lib_sweep",
                    help="Output filename prefix (default: lib_sweep)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Show sweep plan and example probe command; do not run SSH probes")
    ap.add_argument("--write-node-lists", action="store_true",
                    help="Write per-library/binary inconsistent/missing/error node list files")
    ap.add_argument("--detail", choices=["concise", "full"], default="concise",
                    help="CSV/report detail level (default: concise)")
    ap.add_argument("--write-json-summary", action="store_true",
                    help="Write a compact JSON summary report")
    add_bool_option(ap, "--discrepancy-rundown", False,
                    "When inconsistency/missing is detected, SSH to one bad node and one "
                    "reference node, diff their full manifests, and write a discrepancy report.")

    ap.add_argument("--probe", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--probe-rundown", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--probe-binary", action="store_true", help=argparse.SUPPRESS)
    ap.add_argument("--probe-binary-rundown", action="store_true", help=argparse.SUPPRESS)

    return ap


def _setup_ssh_config(args):
    cfg = default_ssh_config()
    cfg.hostkey_mode = args.ssh_hostkey
    if args.ssh_known_hosts:
        cfg.known_hosts = os.path.expanduser(args.ssh_known_hosts)
        os.makedirs(os.path.dirname(cfg.known_hosts), exist_ok=True)
    cfg.control_master = bool(args.ssh_control_master)
    return cfg


def _discover_nodes(scope: str, active_scheduler: str, args, cfg):
    """Discover login nodes (by SSH scan) and compute nodes (via scheduler inventory).

    Returns (login_nodes, compute_nodes, node_inv, scheduler_skipped).
    """
    login_nodes: List[str] = []
    if scope in ("login", "all"):
        host = short_hostname(os.uname().nodename)
        if args.login_auto:
            m = re.match(r"^(.+?)(\d+)$", host)
            prefix = args.login_prefix or (m.group(1) if m else host)
            width = args.login_width or (len(m.group(2)) if m else 2)
            found = []
            last_success = 0
            for i in range(1, args.login_max + 1):
                cand = f"{prefix}{i:0{width}d}"
                p, _ = ssh_with_retries(cand, ["true"], cfg, timeout=8, retries=0)
                if p.returncode == 0:
                    found.append(cand)
                    last_success = i
                elif last_success > 0 and (i - last_success) >= args.login_stop_after_gap:
                    break
            login_nodes = sorted(set(found + ([host] if host.startswith(prefix) else [])))
        else:
            login_nodes = [host]

    compute_nodes: List[str] = []
    node_inv: Dict[str, Dict[str, str]] = {}
    scheduler_skipped = []
    if scope in ("compute", "all"):
        if active_scheduler == "slurm":
            _, _, inv = slurm_inventory()
            compute_nodes, scheduler_skipped = slurm_select_compute_nodes(
                inv,
                online_only=args.pbs_online_only,
                compute_flag_only=args.pbs_compute_flag_only,
            )
        else:
            _, _, inv = pbs_inventory()
            compute_nodes, scheduler_skipped = pbs_select_compute_nodes(
                inv,
                online_only=args.pbs_online_only,
                compute_flag_only=args.pbs_compute_flag_only,
            )
        node_inv = inv

    return login_nodes, compute_nodes, node_inv, scheduler_skipped


def _print_dry_run(scope, active_scheduler, cfg, args, ts, login_nodes, compute_nodes,
                   scheduler_skipped, requested_workers, thread_stack_setting, mode="library"):
    print(f"DRY RUN {ts}")
    print(f"Scope: {scope}")
    print(f"Scheduler: {active_scheduler}")
    print(f"SSH control master: {'enabled' if cfg.control_master else 'disabled'}")
    print(f"Workers: requested={requested_workers} effective={args.workers} cap={MAX_WORKERS}")
    print(f"Thread stack: {thread_stack_setting}")
    print(
        f"Discrepancy rundown: {'enabled' if args.discrepancy_rundown else 'disabled'} "
        f"workers={clamp_workers(args.discrepancy_rundown_workers)}"
    )
    if mode == "binary":
        print(f"Binaries: {args.binary}")
    else:
        print(f"Libraries: {args.lib}")
    print(f"Login nodes: {len(login_nodes)} sample: {', '.join(login_nodes[:20])}")
    print(f"Compute nodes selected: {len(compute_nodes)} sample: {', '.join(compute_nodes[:20])}")
    print(f"Scheduler skipped: {len(scheduler_skipped)}")
    ex_node = login_nodes[0] if login_nodes else (compute_nodes[0] if compute_nodes else "<node>")
    if mode == "binary":
        cmd = ["ssh", ex_node, args.remote_python, "-m", "libsweep", "--probe-binary"]
        for b in args.binary:
            cmd += ["--binary", b]
    else:
        cmd = ["ssh", ex_node, args.remote_python, "-m", "libsweep", "--probe"]
        for lib in args.lib:
            cmd += ["--lib", lib]
    print("Example probe command:")
    print("  " + " ".join(cmd))


def _run_fanout(
    login_nodes: List[str], compute_nodes: List[str], args, cfg
) -> Tuple[List[Dict], List[Dict]]:
    """SSH fan-out to all login and compute nodes. Returns (ok_records, error_records)."""

    def sweep_node(node: str, role: str) -> List[Dict]:
        argv = []
        if args.remote_low_priority:
            argv += ["nice", "-n", "19"]
        argv += [args.remote_python, "-m", "libsweep", "--probe"]
        for lib in args.lib:
            argv += ["--lib", lib]
        for d in args.dirs:
            argv += ["--dirs", d]
        if args.no_ldconfig:
            argv += ["--no-ldconfig"]

        p, kind = ssh_with_retries(node, argv, cfg, timeout=args.ssh_timeout, retries=args.retries)
        node = short_hostname(node)

        if p.returncode != 0:
            status = "probe_error" if kind == "remote_exec_error" else "ssh_error"
            detail = (p.stderr or "").strip()[:240]
            if not detail and kind == "remote_exec_error":
                detail = f"remote command exited with rc {p.returncode}"
            return [{
                "role": role, "node": node, "lib_query": lib,
                "status": status, "ssh_rc": str(p.returncode),
                "ssh_error_kind": kind, "ssh_error_detail": detail,
            } for lib in args.lib]

        recs = []
        for s in json_lines_only(p.stdout):
            try:
                d = json.loads(s)
                d["_role"] = role
                recs.append(d)
            except json.JSONDecodeError:
                continue

        by_q = {r.get("query"): r for r in recs if r.get("query")}
        out = []
        for lib in args.lib:
            r = by_q.get(lib)
            if r:
                r["_role"] = role
                out.append(r)
            else:
                out.append({
                    "_role": role, "node": node, "query": lib, "present": False,
                    "majors": [], "versions": [], "primary_major": "",
                    "primary_version": "", "primary_target": "", "variants_count": 0,
                })
        return out

    ok_records: List[Dict] = []
    error_records: List[Dict] = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {}
        for n in login_nodes:
            futs[ex.submit(sweep_node, n, "login")] = (short_hostname(n), "login")
        for n in compute_nodes:
            futs[ex.submit(sweep_node, n, "compute")] = (short_hostname(n), "compute")

        for fut in as_completed(futs):
            node_name, role = futs[fut]
            try:
                rows = fut.result()
            except Exception as exn:
                for lib in args.lib:
                    error_records.append({
                        "role": role, "node": node_name, "lib_query": lib,
                        "status": "internal_error", "ssh_rc": "-1",
                        "ssh_error_kind": "internal_error",
                        "ssh_error_detail": str(exn)[:240],
                    })
                continue
            for r in rows:
                if r.get("status") in ("ssh_error", "probe_error"):
                    error_records.append(r)
                else:
                    ok_records.append(r)

    return ok_records, error_records


def _build_rows(
    ok_records: List[Dict], error_records: List[Dict], args, node_inv: Dict, active_scheduler: str
) -> Tuple[List[Dict], List[Dict], Dict[str, Set[int]]]:
    """Build login rows, compute rows, and per-library baselines from raw probe records."""

    # Login rows and baseline inputs
    login_rows: List[Dict] = []
    login_ok_by_lib: Dict[str, List[Dict]] = {lib: [] for lib in args.lib}
    for r in ok_records:
        if r.get("_role") != "login":
            continue
        node = short_hostname(r.get("node", ""))
        libq = r.get("query", "")
        row = {
            "node": node,
            "node_type": "login",
            "lib_query": libq,
            "result": "observed",
            "issue_detail": "",
            "required_majors": "",
            "found_majors": ",".join(str(m) for m in (r.get("majors") or [])),
            "missing_required_majors": "",
            "primary_major": str(r.get("primary_major", "") if r.get("primary_major") is not None else ""),
            "primary_version": str(r.get("primary_version", "") or ""),
            "primary_target": str(r.get("primary_target", "") or ""),
            "error_kind": "",
            "error_detail": "",
        }
        login_rows.append(row)
        login_ok_by_lib[libq].append(row)

    baselines = {
        lib: compute_baseline_majors(lib, login_ok_by_lib.get(lib, []), args.baseline_from, args.baseline_major)
        for lib in args.lib
    }

    # Compute rows from successful probes
    compute_rows: List[Dict] = []
    for r in ok_records:
        if r.get("_role") != "compute":
            continue
        node = short_hostname(r.get("node", ""))
        libq = r.get("query", "")
        meta = node_inv.get(node, {})
        node_class = classify_scheduler_node(active_scheduler, node, meta)
        node_type = resolve_scheduler_node_type(active_scheduler, node, meta)

        majors_list = r.get("majors") or []
        majors_csv = ",".join(str(m) for m in majors_list)
        present = bool(r.get("present"))

        baseline = baselines.get(libq, set())
        baseline_csv = ",".join(str(m) for m in sorted(baseline))
        missing = sorted(baseline - {int(m) for m in majors_list if isinstance(m, (int, float))})

        if not present:
            result = "missing"
            missing_csv = baseline_csv
            issue_detail = f"required={baseline_csv or 'none'} found=none"
        elif not missing:
            result = "consistent"
            missing_csv = ""
            issue_detail = ""
        else:
            result = "inconsistent"
            missing_csv = ",".join(str(m) for m in missing)
            issue_detail = f"required={baseline_csv or 'none'} found={majors_csv or 'none'}"

        row = {
            "node": node,
            "node_type": node_type,
            "lib_query": libq,
            "result": result,
            "issue_detail": issue_detail,
            "required_majors": baseline_csv,
            "found_majors": majors_csv,
            "missing_required_majors": missing_csv,
            "primary_major": str(r.get("primary_major", "") if r.get("primary_major") is not None else ""),
            "primary_version": str(r.get("primary_version", "") or ""),
            "primary_target": str(r.get("primary_target", "") or ""),
            "error_kind": "",
            "error_detail": "",
        }
        if args.detail == "full":
            row.update({
                "node_class": node_class,
                "scheduler": active_scheduler,
                "scheduler_partition": meta.get("scheduler.partition", ""),
                "scheduler_gres": meta.get("scheduler.gres", ""),
                "pbs_state": meta.get("state", ""),
                "pbs_nodetype": meta.get("resources_available.nodetype", ""),
                "pbs_compute_flag": meta.get("resources_available.compute", "").strip(),
            })
            row["versions"] = ",".join(str(v) for v in (r.get("versions") or []))
            row["variants_count"] = str(r.get("variants_count", ""))
        compute_rows.append(row)

    # Attach error records to the appropriate row list
    for e in error_records:
        role = e.get("role", "compute")
        node = short_hostname(e.get("node", ""))
        libq = e.get("lib_query", "")
        meta = node_inv.get(node, {})
        node_class = classify_scheduler_node(active_scheduler, node, meta)
        node_type = resolve_scheduler_node_type(active_scheduler, node, meta)

        row = {
            "node": node,
            "node_type": "login" if role == "login" else node_type,
            "lib_query": libq,
            "result": "unreachable",
            "issue_detail": e.get("ssh_error_kind", "ssh_error"),
            "required_majors": "",
            "found_majors": "",
            "missing_required_majors": "",
            "primary_major": "",
            "primary_version": "",
            "primary_target": "",
            "error_kind": e.get("ssh_error_kind", "ssh_error"),
            "error_detail": e.get("ssh_error_detail", ""),
        }
        if args.detail == "full":
            row.update({
                "node_class": node_class,
                "scheduler": active_scheduler if role == "compute" else "local",
                "scheduler_partition": meta.get("scheduler.partition", ""),
                "scheduler_gres": meta.get("scheduler.gres", ""),
                "pbs_state": meta.get("state", ""),
                "pbs_nodetype": meta.get("resources_available.nodetype", ""),
                "pbs_compute_flag": meta.get("resources_available.compute", "").strip(),
            })
            row["versions"] = ""
            row["variants_count"] = ""
        if role == "login":
            login_rows.append(row)
        else:
            compute_rows.append(row)

    return login_rows, compute_rows, baselines


def _run_discrepancy_rundown(
    compute_rows: List[Dict], login_rows: List[Dict], rundown_enabled: bool,
    args, cfg, out_prefix: str,
) -> Dict:
    """Run the full-library manifest scan on representative flagged nodes.

    When triggered, each flagged node and a reference node (usually a login node)
    receive a --probe-rundown call that inventories *all* shared libraries on the
    system — not just the queried ones.  The manifests are then diffed to surface
    broader library environment differences that may explain the primary discrepancy.

    Returns a dict with keys: enabled, triggered, reference_node, reference_role,
    rows, nodes, csv_path, nodes_txt_path.
    """
    csv_path = f"{out_prefix}_rundown_discrepancies.csv"
    nodes_txt_path = f"{out_prefix}_rundown_nodes.txt"

    result: Dict = {
        "enabled": rundown_enabled,
        "triggered": False,
        "reference_node": "",
        "reference_role": "",
        "rows": [],
        "nodes": [],
        "csv_path": csv_path,
        "nodes_txt_path": nodes_txt_path,
    }

    if not rundown_enabled:
        return result

    # Pick the single first flagged node as the one bad node
    flagged_nodes = sorted({
        str(r.get("node", ""))
        for r in compute_rows
        if r.get("result") in ("inconsistent", "missing") and r.get("node")
    })
    if not flagged_nodes:
        result["nodes"].append({
            "node": "", "role": "", "status": "skipped",
            "note": "no inconsistent/missing rows found; discrepancy rundown not triggered",
        })
        return result

    result["triggered"] = True
    bad_node = flagged_nodes[0]
    trigger = next(
        (r for r in compute_rows
         if str(r.get("node", "")) == bad_node and r.get("result") in ("inconsistent", "missing")),
        {"lib_query": "", "result": "", "found_majors": "", "missing_required_majors": ""},
    )

    ref_node, ref_role = select_rundown_reference_node(login_rows, compute_rows, {bad_node})
    result["reference_node"] = ref_node
    result["reference_role"] = ref_role

    result["nodes"].append({
        "node": bad_node, "role": "compute", "status": "planned",
        "note": (
            f"trigger lib={trigger.get('lib_query', '')} result={trigger.get('result', '')}"
        ),
    })

    if not ref_node:
        result["nodes"].append({
            "node": "", "role": "", "status": "skipped",
            "note": "no suitable reference node for discrepancy rundown",
        })
        return result

    result["nodes"].append({
        "node": ref_node, "role": ref_role,
        "status": "planned_reference", "note": "reference_manifest",
    })

    # Scan exactly two nodes: reference + the one bad node
    scan_plan: Dict[str, str] = {ref_node: ref_role}
    if bad_node != ref_node:
        scan_plan[bad_node] = "compute"

    def probe_full_manifest(node: str, role: str) -> Dict:
        """SSH to a node and collect its full shared-library manifest."""
        argv = []
        if args.remote_low_priority:
            argv += ["nice", "-n", "19"]
        argv += [args.remote_python, "-m", "libsweep", "--probe-rundown"]
        for d in args.dirs:
            argv += ["--dirs", d]
        if args.no_ldconfig:
            argv += ["--no-ldconfig"]

        p, kind = ssh_with_retries(node, argv, cfg, timeout=args.ssh_timeout, retries=args.retries)
        short = short_hostname(node)
        if p.returncode != 0:
            return {"node": short, "role": role, "status": "error", "kind": kind,
                    "detail": (p.stderr or "").strip()[:240], "manifest": {}}

        payload = None
        for s in json_lines_only(p.stdout):
            try:
                d = json.loads(s)
            except json.JSONDecodeError:
                continue
            if isinstance(d.get("manifest"), dict):
                payload = d
                break

        if payload is None:
            return {"node": short, "role": role, "status": "error", "kind": "parse_error",
                    "detail": "probe-rundown returned no manifest", "manifest": {}}

        return {"node": short, "role": role, "status": "ok", "kind": "ok",
                "detail": "", "manifest": payload.get("manifest", {})}

    # Fan out full-manifest probes
    manifest_by_node: Dict[str, Dict] = {}
    workers = max(1, min(clamp_workers(args.discrepancy_rundown_workers), len(scan_plan)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(probe_full_manifest, node, role): (node, role)
            for node, role in scan_plan.items()
        }
        for fut in as_completed(futs):
            node, role = futs[fut]
            short = short_hostname(node)
            try:
                res = fut.result()
            except Exception as exn:
                result["nodes"].append({
                    "node": short, "role": role, "status": "error",
                    "note": f"internal_error: {str(exn)[:160]}",
                })
                continue

            if res.get("status") == "ok":
                manifest_by_node[short] = res["manifest"]
                result["nodes"].append({
                    "node": short, "role": role, "status": "scanned",
                    "note": f"manifest_lib_count={len(res['manifest'])}",
                })
            else:
                result["nodes"].append({
                    "node": short, "role": role, "status": "error",
                    "note": f"{res.get('kind', 'error')}: {res.get('detail', '')}",
                })

    # Diff a flagged node's manifest against the reference.
    # Use flagged_nodes[0] (bad_node) if its probe succeeded; otherwise fall
    # through the list and probe remaining candidates sequentially until one
    # succeeds.
    ref_short = short_hostname(ref_node)
    ref_manifest = manifest_by_node.get(ref_short)
    if isinstance(ref_manifest, dict) and ref_manifest:
        node_manifest = None
        actual_bad_node = None
        for candidate in flagged_nodes:
            cshort = short_hostname(candidate)
            if cshort == ref_short:
                continue
            m = manifest_by_node.get(cshort)
            if isinstance(m, dict) and m:
                node_manifest = m
                actual_bad_node = cshort
                break
            # bad_node was already attempted in the initial fan-out; don't retry it
            if candidate == bad_node:
                continue
            res = probe_full_manifest(candidate, "compute")
            result["nodes"].append({
                "node": cshort, "role": "compute",
                "status": "scanned" if res.get("status") == "ok" else "error",
                "note": (
                    "fallback manifest_lib_count={}".format(len(res["manifest"]))
                    if res.get("status") == "ok"
                    else "fallback {}: {}".format(res.get("kind", "error"), res.get("detail", ""))
                ),
            })
            if res.get("status") == "ok":
                manifest_by_node[cshort] = res["manifest"]
                node_manifest = res["manifest"]
                actual_bad_node = cshort
                break

        if node_manifest is not None:
            actual_trigger = next(
                (r for r in compute_rows
                 if short_hostname(str(r.get("node", ""))) == actual_bad_node
                 and r.get("result") in ("inconsistent", "missing")),
                trigger,
            )
            result["rows"].extend(
                compare_rundown_manifests(
                    reference_node=ref_short,
                    reference_manifest=ref_manifest,
                    node=actual_bad_node,
                    node_manifest=node_manifest,
                    trigger=actual_trigger,
                )
            )
    else:
        result["nodes"].append({
            "node": ref_short, "role": ref_role, "status": "error",
            "note": "reference_manifest_unavailable",
        })

    return result


def _write_outputs(
    scope: str, active_scheduler: str, out_prefix: str, ts: str, args,
    login_nodes: List[str], compute_nodes: List[str],
    login_rows: List[Dict], compute_rows: List[Dict],
    scheduler_skipped, baselines: Dict, rundown: Dict,
) -> int:
    """Write all output files and print a summary. Returns exit code (0, 1, or 2)."""

    login_csv = f"{out_prefix}_login.csv"
    compute_csv = f"{out_prefix}_compute.csv"
    report_txt = f"{out_prefix}_report.txt"
    skipped_txt = f"{out_prefix}_{active_scheduler}_skipped.txt"

    concise_fields = ["node", "node_type", "lib_query", "result", "issue_detail"]
    full_fields = [
        "node", "node_type", "node_class", "scheduler",
        "scheduler_partition", "scheduler_gres",
        "pbs_state", "pbs_nodetype", "pbs_compute_flag",
        "lib_query", "result", "issue_detail",
        "required_majors", "found_majors", "missing_required_majors",
        "primary_major", "primary_version", "primary_target",
        "error_kind", "error_detail", "versions", "variants_count",
    ]
    fields = full_fields if args.detail == "full" else concise_fields

    if scope in ("login", "all"):
        write_csv(login_csv, fields, login_rows)
    if scope in ("compute", "all"):
        write_csv(compute_csv, fields, compute_rows)

    write_scheduler_skipped(skipped_txt, scheduler_skipped)

    if rundown["enabled"]:
        with open(rundown["nodes_txt_path"], "w", encoding="utf-8") as f:
            f.write("node\trole\tstatus\tnote\n")
            for r in rundown["nodes"]:
                f.write(f"{r.get('node','')}\t{r.get('role','')}\t{r.get('status','')}\t{r.get('note','')}\n")
        if rundown["triggered"] and rundown["reference_node"]:
            rundown_fields = [
                "reference_node", "node", "lib_root", "discrepancy_kind",
                "reference_majors", "node_majors",
                "reference_versions", "node_versions",
                "reference_variants", "node_variants",
                "trigger_lib_query", "trigger_result",
                "trigger_found_majors", "trigger_missing_required_majors",
            ]
            write_csv(rundown["csv_path"], rundown_fields, rundown["rows"])

    node_list_files: Dict[str, Dict[str, str]] = {}
    if args.write_node_lists and scope in ("compute", "all"):
        for lib in args.lib:
            node_list_files[lib] = write_node_lists(out_prefix, lib, compute_rows)

    report = build_report(
        ts=ts,
        scope=scope,
        scheduler=active_scheduler,
        baseline_from=args.baseline_from,
        baseline_major=str(args.baseline_major) if args.baseline_major is not None else "(none)",
        workers=args.workers,
        retries=args.retries,
        login_nodes=len(login_nodes),
        compute_nodes=len(compute_nodes),
        scheduler_skipped_count=len(scheduler_skipped),
        libs=args.lib,
        login_rows=login_rows,
        compute_rows=compute_rows,
        baselines=baselines,
        node_list_files=node_list_files,
    )
    report += build_rundown_section(
        enabled=rundown["enabled"],
        triggered=rundown["triggered"],
        reference_node=rundown["reference_node"],
        reference_role=rundown["reference_role"],
        scanned_nodes=rundown["nodes"],
        discrepancy_rows=rundown["rows"],
        discrepancy_csv=rundown["csv_path"] if rundown["triggered"] and rundown["reference_node"] else "",
        nodes_txt=rundown["nodes_txt_path"] if rundown["enabled"] else "",
    )
    with open(report_txt, "w", encoding="utf-8") as f:
        f.write(report)

    summary_json = ""
    if args.write_json_summary:
        summary_json = f"{out_prefix}_summary.json"
        by_lib = {}
        for lib in args.lib:
            c_ok = [r for r in compute_rows if r.get("lib_query") == lib and r.get("result") != "unreachable"]
            c_err = [r for r in compute_rows if r.get("lib_query") == lib and r.get("result") == "unreachable"]
            by_lib[lib] = {
                "compute_ok": len(c_ok),
                "compute_errors": len(c_err),
                "consistent": sum(1 for r in c_ok if r.get("result") == "consistent"),
                "inconsistent": sum(1 for r in c_ok if r.get("result") == "inconsistent"),
                "missing": sum(1 for r in c_ok if r.get("result") == "missing"),
                "unreachable": len(c_err),
            }
        summary = {
            "ts": ts, "scheduler": active_scheduler, "scope": scope,
            "baseline_from": args.baseline_from, "baseline_major": args.baseline_major,
            "login_nodes": len(login_nodes), "compute_nodes": len(compute_nodes),
            "scheduler_skipped": len(scheduler_skipped), "libs": by_lib,
        }
        with open(summary_json, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, sort_keys=True)

    # Print summary
    print(f"Wrote login CSV:   {login_csv}" if scope in ("login", "all") else "Login scope disabled")
    print(f"Wrote compute CSV: {compute_csv}" if scope in ("compute", "all") else "Compute scope disabled")
    print(f"Wrote report:      {report_txt}")
    print(f"Wrote scheduler skipped: {skipped_txt}")
    if rundown["enabled"]:
        if rundown["triggered"] and rundown["reference_node"]:
            print(f"Wrote discrepancy rundown CSV: {rundown['csv_path']}")
        print(f"Wrote discrepancy rundown nodes: {rundown['nodes_txt_path']}")
    if summary_json:
        print(f"Wrote JSON summary: {summary_json}")

    # Exit code
    total_inconsistent = sum(1 for r in compute_rows if r.get("result") == "inconsistent")
    total_missing = sum(1 for r in compute_rows if r.get("result") == "missing")
    total_errors = sum(1 for r in compute_rows if r.get("result") == "unreachable")
    if total_errors > 0:
        return 2
    if (total_inconsistent + total_missing) > 0:
        return 1
    return 0


# ---------------------------------------------------------------------------
# Binary pipeline stages
# ---------------------------------------------------------------------------

def _run_binary_fanout(
    login_nodes: List[str], compute_nodes: List[str], args, cfg
) -> Tuple[List[Dict], List[Dict]]:
    """Binary sweep SSH fan-out. Returns (ok_records, error_records)."""

    def sweep_node(node: str, role: str) -> List[Dict]:
        argv = []
        if args.remote_low_priority:
            argv += ["nice", "-n", "19"]
        argv += [args.remote_python, "-m", "libsweep", "--probe-binary"]
        for b in args.binary:
            argv += ["--binary", b]
        for d in (args.binary_dirs or []):
            argv += ["--binary-dirs", d]

        p, kind = ssh_with_retries(node, argv, cfg, timeout=args.ssh_timeout, retries=args.retries)
        node = short_hostname(node)

        if p.returncode != 0:
            status = "probe_error" if kind == "remote_exec_error" else "ssh_error"
            detail = (p.stderr or "").strip()[:240]
            if not detail and kind == "remote_exec_error":
                detail = f"remote command exited with rc {p.returncode}"
            return [{
                "role": role, "node": node, "binary_query": b,
                "status": status, "ssh_rc": str(p.returncode),
                "ssh_error_kind": kind, "ssh_error_detail": detail,
            } for b in args.binary]

        recs = []
        for s in json_lines_only(p.stdout):
            try:
                d = json.loads(s)
                d["_role"] = role
                recs.append(d)
            except json.JSONDecodeError:
                continue

        by_q = {r.get("query"): r for r in recs if r.get("query")}
        out = []
        for b in args.binary:
            r = by_q.get(b)
            if r:
                r["_role"] = role
                out.append(r)
            else:
                out.append({
                    "_role": role, "node": node, "query": b, "present": False,
                    "path": "", "version_string": "", "version_rc": -1,
                })
        return out

    ok_records: List[Dict] = []
    error_records: List[Dict] = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {}
        for n in login_nodes:
            futs[ex.submit(sweep_node, n, "login")] = (short_hostname(n), "login")
        for n in compute_nodes:
            futs[ex.submit(sweep_node, n, "compute")] = (short_hostname(n), "compute")

        for fut in as_completed(futs):
            node_name, role = futs[fut]
            try:
                rows = fut.result()
            except Exception as exn:
                for b in args.binary:
                    error_records.append({
                        "role": role, "node": node_name, "binary_query": b,
                        "status": "internal_error", "ssh_rc": "-1",
                        "ssh_error_kind": "internal_error",
                        "ssh_error_detail": str(exn)[:240],
                    })
                continue
            for r in rows:
                if r.get("status") in ("ssh_error", "probe_error"):
                    error_records.append(r)
                else:
                    ok_records.append(r)

    return ok_records, error_records


def _build_binary_rows(
    ok_records: List[Dict], error_records: List[Dict], args, node_inv: Dict, active_scheduler: str
) -> Tuple[List[Dict], List[Dict], Dict[str, str]]:
    """Build binary login rows, compute rows, and per-binary version baselines."""

    login_rows: List[Dict] = []
    login_ok_by_binary: Dict[str, List[Dict]] = {b: [] for b in args.binary}

    for r in ok_records:
        if r.get("_role") != "login":
            continue
        node = short_hostname(r.get("node", ""))
        bq = r.get("query", "")
        row = {
            "node": node,
            "node_type": "login",
            "binary_query": bq,
            "result": "observed",
            "present": bool(r.get("present")),
            "path": str(r.get("path", "") or ""),
            "version_string": str(r.get("version_string", "") or ""),
            "required_version": "",
            "issue_detail": "",
            "error_kind": "",
            "error_detail": "",
        }
        login_rows.append(row)
        if bq in login_ok_by_binary:
            login_ok_by_binary[bq].append(row)

    baselines: Dict[str, str] = {
        b: compute_binary_baseline(b, login_ok_by_binary.get(b, []), args.baseline_from, None)
        for b in args.binary
    }

    compute_rows: List[Dict] = []
    for r in ok_records:
        if r.get("_role") != "compute":
            continue
        node = short_hostname(r.get("node", ""))
        bq = r.get("query", "")
        meta = node_inv.get(node, {})
        node_type = resolve_scheduler_node_type(active_scheduler, node, meta)

        present = bool(r.get("present"))
        version_string = str(r.get("version_string", "") or "")
        path = str(r.get("path", "") or "")
        baseline = baselines.get(bq, "")

        if not present:
            result = "missing"
            issue_detail = f"required_version={baseline or 'none'} found=none"
        elif baseline and version_string != baseline:
            result = "inconsistent"
            issue_detail = f"required={baseline} found={version_string or '(unknown)'}"
        else:
            result = "consistent"
            issue_detail = ""

        compute_rows.append({
            "node": node,
            "node_type": node_type,
            "binary_query": bq,
            "result": result,
            "present": present,
            "path": path,
            "version_string": version_string,
            "required_version": baseline,
            "issue_detail": issue_detail,
            "error_kind": "",
            "error_detail": "",
        })

    for e in error_records:
        role = e.get("role", "compute")
        node = short_hostname(e.get("node", ""))
        bq = e.get("binary_query") or e.get("query", "")
        meta = node_inv.get(node, {})
        node_type = resolve_scheduler_node_type(active_scheduler, node, meta)

        row = {
            "node": node,
            "node_type": "login" if role == "login" else node_type,
            "binary_query": bq,
            "result": "unreachable",
            "present": False,
            "path": "",
            "version_string": "",
            "required_version": "",
            "issue_detail": e.get("ssh_error_kind", "ssh_error"),
            "error_kind": e.get("ssh_error_kind", "ssh_error"),
            "error_detail": e.get("ssh_error_detail", ""),
        }
        if role == "login":
            login_rows.append(row)
        else:
            compute_rows.append(row)

    return login_rows, compute_rows, baselines


def _run_binary_discrepancy_rundown(
    compute_rows: List[Dict], login_rows: List[Dict], rundown_enabled: bool,
    args, cfg, out_prefix: str,
) -> Dict:
    """Full binary manifest scan on one flagged node vs one reference node."""
    csv_path = f"{out_prefix}_binary_rundown_discrepancies.csv"
    nodes_txt_path = f"{out_prefix}_binary_rundown_nodes.txt"

    result: Dict = {
        "enabled": rundown_enabled,
        "triggered": False,
        "reference_node": "",
        "reference_role": "",
        "rows": [],
        "nodes": [],
        "csv_path": csv_path,
        "nodes_txt_path": nodes_txt_path,
    }

    if not rundown_enabled:
        return result

    flagged = sorted({
        str(r.get("node", ""))
        for r in compute_rows
        if r.get("result") in ("inconsistent", "missing") and r.get("node")
    })
    if not flagged:
        result["nodes"].append({
            "node": "", "role": "", "status": "skipped",
            "note": "no inconsistent/missing rows; binary rundown not triggered",
        })
        return result

    result["triggered"] = True
    bad_node = flagged[0]
    trigger = next(
        (r for r in compute_rows
         if str(r.get("node", "")) == bad_node and r.get("result") in ("inconsistent", "missing")),
        {"binary_query": "", "result": "", "version_string": "", "required_version": ""},
    )

    ref_node, ref_role = select_rundown_reference_node(login_rows, compute_rows, {bad_node})
    result["reference_node"] = ref_node
    result["reference_role"] = ref_role

    result["nodes"].append({
        "node": bad_node, "role": "compute", "status": "planned",
        "note": f"trigger binary={trigger.get('binary_query', '')} result={trigger.get('result', '')}",
    })

    if not ref_node:
        result["nodes"].append({
            "node": "", "role": "", "status": "skipped",
            "note": "no suitable reference node",
        })
        return result

    result["nodes"].append({
        "node": ref_node, "role": ref_role,
        "status": "planned_reference", "note": "reference_manifest",
    })

    scan_plan: Dict[str, str] = {ref_node: ref_role}
    if bad_node != ref_node:
        scan_plan[bad_node] = "compute"

    def probe_full_binary_manifest(node: str, role: str) -> Dict:
        argv = []
        if args.remote_low_priority:
            argv += ["nice", "-n", "19"]
        argv += [args.remote_python, "-m", "libsweep", "--probe-binary-rundown"]
        for d in (args.binary_dirs or []):
            argv += ["--binary-dirs", d]

        p, kind = ssh_with_retries(node, argv, cfg, timeout=args.ssh_timeout, retries=args.retries)
        short = short_hostname(node)
        if p.returncode != 0:
            return {"node": short, "role": role, "status": "error", "kind": kind,
                    "detail": (p.stderr or "").strip()[:240], "manifest": {}}

        payload = None
        for s in json_lines_only(p.stdout):
            try:
                d = json.loads(s)
            except json.JSONDecodeError:
                continue
            if isinstance(d.get("manifest"), dict):
                payload = d
                break

        if payload is None:
            return {"node": short, "role": role, "status": "error", "kind": "parse_error",
                    "detail": "probe-binary-rundown returned no manifest", "manifest": {}}

        return {"node": short, "role": role, "status": "ok", "kind": "ok",
                "detail": "", "manifest": payload.get("manifest", {})}

    manifest_by_node: Dict[str, Dict] = {}
    workers = max(1, min(clamp_workers(args.discrepancy_rundown_workers), len(scan_plan)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(probe_full_binary_manifest, node, role): (node, role)
            for node, role in scan_plan.items()
        }
        for fut in as_completed(futs):
            node, role = futs[fut]
            short = short_hostname(node)
            try:
                res = fut.result()
            except Exception as exn:
                result["nodes"].append({
                    "node": short, "role": role, "status": "error",
                    "note": f"internal_error: {str(exn)[:160]}",
                })
                continue
            if res.get("status") == "ok":
                manifest_by_node[short] = res["manifest"]
                result["nodes"].append({
                    "node": short, "role": role, "status": "scanned",
                    "note": f"manifest_binary_count={len(res['manifest'])}",
                })
            else:
                result["nodes"].append({
                    "node": short, "role": role, "status": "error",
                    "note": f"{res.get('kind', 'error')}: {res.get('detail', '')}",
                })

    ref_short = short_hostname(ref_node)
    ref_manifest = manifest_by_node.get(ref_short)
    if isinstance(ref_manifest, dict) and ref_manifest:
        node_manifest = None
        actual_bad_node = None
        for candidate in flagged:
            cshort = short_hostname(candidate)
            if cshort == ref_short:
                continue
            m = manifest_by_node.get(cshort)
            if isinstance(m, dict) and m:
                node_manifest = m
                actual_bad_node = cshort
                break
            # bad_node was already attempted in the initial fan-out; don't retry it
            if candidate == bad_node:
                continue
            res = probe_full_binary_manifest(candidate, "compute")
            result["nodes"].append({
                "node": cshort, "role": "compute",
                "status": "scanned" if res.get("status") == "ok" else "error",
                "note": (
                    "fallback manifest_binary_count={}".format(len(res["manifest"]))
                    if res.get("status") == "ok"
                    else "fallback {}: {}".format(res.get("kind", "error"), res.get("detail", ""))
                ),
            })
            if res.get("status") == "ok":
                manifest_by_node[cshort] = res["manifest"]
                node_manifest = res["manifest"]
                actual_bad_node = cshort
                break

        if node_manifest is not None:
            actual_trigger = next(
                (r for r in compute_rows
                 if short_hostname(str(r.get("node", ""))) == actual_bad_node
                 and r.get("result") in ("inconsistent", "missing")),
                trigger,
            )
            result["rows"].extend(
                compare_binary_rundown_manifests(
                    reference_node=ref_short,
                    reference_manifest=ref_manifest,
                    node=actual_bad_node,
                    node_manifest=node_manifest,
                    trigger=actual_trigger,
                )
            )
    else:
        result["nodes"].append({
            "node": ref_short, "role": ref_role, "status": "error",
            "note": "reference_manifest_unavailable",
        })

    return result


def _write_binary_outputs(
    scope: str, active_scheduler: str, out_prefix: str, ts: str, args,
    login_nodes: List[str], compute_nodes: List[str],
    login_rows: List[Dict], compute_rows: List[Dict],
    scheduler_skipped, baselines: Dict[str, str], rundown: Dict,
) -> int:
    """Write binary sweep output files and print summary. Returns exit code."""

    login_csv = f"{out_prefix}_binary_login.csv"
    compute_csv = f"{out_prefix}_binary_compute.csv"
    report_txt = f"{out_prefix}_binary_report.txt"
    skipped_txt = f"{out_prefix}_{active_scheduler}_skipped.txt"

    binary_fields = [
        "node", "node_type", "binary_query", "result", "issue_detail",
        "present", "path", "version_string", "required_version",
        "error_kind", "error_detail",
    ]

    if scope in ("login", "all"):
        write_csv(login_csv, binary_fields, login_rows)
    if scope in ("compute", "all"):
        write_csv(compute_csv, binary_fields, compute_rows)

    write_scheduler_skipped(skipped_txt, scheduler_skipped)

    if rundown["enabled"]:
        with open(rundown["nodes_txt_path"], "w", encoding="utf-8") as f:
            f.write("node\trole\tstatus\tnote\n")
            for r in rundown["nodes"]:
                f.write(
                    f"{r.get('node','')}\t{r.get('role','')}\t"
                    f"{r.get('status','')}\t{r.get('note','')}\n"
                )
        if rundown["triggered"] and rundown["reference_node"]:
            binary_rundown_fields = [
                "reference_node", "node", "binary_name", "discrepancy_kind",
                "reference_path", "node_path",
                "trigger_binary_query", "trigger_result",
                "trigger_version_string", "trigger_required_version",
            ]
            write_csv(rundown["csv_path"], binary_rundown_fields, rundown["rows"])

    node_list_files: Dict[str, Dict[str, str]] = {}
    if args.write_node_lists and scope in ("compute", "all"):
        for b in args.binary:
            node_list_files[b] = write_binary_node_lists(out_prefix, b, compute_rows)

    report = build_binary_report(
        ts=ts,
        scope=scope,
        scheduler=active_scheduler,
        baseline_from=args.baseline_from,
        workers=args.workers,
        retries=args.retries,
        login_nodes=len(login_nodes),
        compute_nodes=len(compute_nodes),
        scheduler_skipped_count=len(scheduler_skipped),
        binaries=args.binary,
        login_rows=login_rows,
        compute_rows=compute_rows,
        baselines=baselines,
        node_list_files=node_list_files,
    )
    report += build_rundown_section(
        enabled=rundown["enabled"],
        triggered=rundown["triggered"],
        reference_node=rundown["reference_node"],
        reference_role=rundown["reference_role"],
        scanned_nodes=rundown["nodes"],
        discrepancy_rows=rundown["rows"],
        discrepancy_csv=rundown["csv_path"] if rundown["triggered"] and rundown["reference_node"] else "",
        nodes_txt=rundown["nodes_txt_path"] if rundown["enabled"] else "",
    )
    with open(report_txt, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"Wrote binary login CSV:   {login_csv}" if scope in ("login", "all") else "Login scope disabled")
    print(f"Wrote binary compute CSV: {compute_csv}" if scope in ("compute", "all") else "Compute scope disabled")
    print(f"Wrote report:      {report_txt}")
    print(f"Wrote scheduler skipped: {skipped_txt}")
    if rundown["enabled"]:
        if rundown["triggered"] and rundown["reference_node"]:
            print(f"Wrote binary discrepancy rundown CSV: {rundown['csv_path']}")
        print(f"Wrote binary discrepancy rundown nodes: {rundown['nodes_txt_path']}")

    total_inconsistent = sum(1 for r in compute_rows if r.get("result") == "inconsistent")
    total_missing = sum(1 for r in compute_rows if r.get("result") == "missing")
    total_errors = sum(1 for r in compute_rows if r.get("result") == "unreachable")
    if total_errors > 0:
        return 2
    if (total_inconsistent + total_missing) > 0:
        return 1
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    ap = _build_arg_parser()
    args = ap.parse_args()

    if args.examples:
        print(EXAMPLES)
        return

    if args.lib and args.binary:
        ap.error("--lib and --binary cannot be used together in the same sweep")
    if (not args.lib and not args.binary
            and not args.probe_rundown and not args.probe_binary
            and not args.probe_binary_rundown):
        ap.error("--lib or --binary is required unless --examples is used")

    requested_workers = args.workers
    args.workers = clamp_workers(args.workers)
    if args.workers != requested_workers:
        print(
            f"warning: clamping --workers from {requested_workers} to {args.workers} (cap={MAX_WORKERS})",
            file=sys.stderr,
        )

    # Remote probe modes — run on the target node via SSH, then exit
    if args.probe_rundown:
        ts = datetime.now(timezone.utc).isoformat()
        r = probe_rundown(args.dirs, args.no_ldconfig)
        r["ts_utc"] = ts
        print(json.dumps(r, sort_keys=True))
        return

    if args.probe:
        ts = datetime.now(timezone.utc).isoformat()
        for lib in args.lib:
            r = probe_node(lib, args.dirs, args.no_ldconfig)
            r["ts_utc"] = ts
            print(json.dumps(r, sort_keys=True))
        return

    if args.probe_binary_rundown:
        ts = datetime.now(timezone.utc).isoformat()
        r = probe_binary_rundown(args.binary_dirs or [])
        r["ts_utc"] = ts
        print(json.dumps(r, sort_keys=True))
        return

    if args.probe_binary:
        ts = datetime.now(timezone.utc).isoformat()
        for b in (args.binary or []):
            r = probe_binary(b, args.binary_dirs or [])
            r["ts_utc"] = ts
            print(json.dumps(r, sort_keys=True))
        return

    # Orchestration
    in_scheduler_job = any(
        k in os.environ for k in ("PBS_JOBID", "PBS_NODEFILE", "PBS_ENVIRONMENT", "SLURM_JOB_ID", "SLURM_NTASKS")
    )
    scope = args.scope or ("compute" if in_scheduler_job else "all")
    active_scheduler = detect_scheduler(args.scheduler)
    cfg = _setup_ssh_config(args)
    thread_stack_setting = configure_thread_stack_size()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_prefix = f"{args.out_prefix}_{ts}"

    login_nodes, compute_nodes, node_inv, scheduler_skipped = _discover_nodes(scope, active_scheduler, args, cfg)
    compute_nodes = [n for n in compute_nodes if n not in set(login_nodes)]

    rundown_enabled = bool(args.discrepancy_rundown and scope in ("compute", "all"))

    if args.binary:
        if args.dry_run:
            _print_dry_run(scope, active_scheduler, cfg, args, ts,
                           login_nodes, compute_nodes, scheduler_skipped,
                           requested_workers, thread_stack_setting, mode="binary")
            return
        ok_records, error_records = _run_binary_fanout(login_nodes, compute_nodes, args, cfg)
        login_rows, compute_rows, baselines = _build_binary_rows(
            ok_records, error_records, args, node_inv, active_scheduler
        )
        rundown = _run_binary_discrepancy_rundown(
            compute_rows, login_rows, rundown_enabled, args, cfg, out_prefix
        )
        exit_code = _write_binary_outputs(
            scope, active_scheduler, out_prefix, ts, args,
            login_nodes, compute_nodes, login_rows, compute_rows,
            scheduler_skipped, baselines, rundown,
        )
    else:
        if args.dry_run:
            _print_dry_run(scope, active_scheduler, cfg, args, ts,
                           login_nodes, compute_nodes, scheduler_skipped,
                           requested_workers, thread_stack_setting)
            return
        ok_records, error_records = _run_fanout(login_nodes, compute_nodes, args, cfg)
        login_rows, compute_rows, baselines = _build_rows(
            ok_records, error_records, args, node_inv, active_scheduler
        )
        rundown = _run_discrepancy_rundown(
            compute_rows, login_rows, rundown_enabled, args, cfg, out_prefix
        )
        exit_code = _write_outputs(
            scope, active_scheduler, out_prefix, ts, args,
            login_nodes, compute_nodes, login_rows, compute_rows,
            scheduler_skipped, baselines, rundown,
        )

    if exit_code:
        sys.exit(exit_code)
