import argparse
import csv
import json
import os
import re
import shutil
import sys
import threading
from datetime import datetime, timezone
from typing import Dict, List, Set

from sshfanout import default_ssh_config, ssh_with_retries, short_hostname
from pbs import (
    classify_node as pbs_classify_node,
    pbs_inventory,
    resolve_node_type as pbs_resolve_node_type,
    select_compute_nodes as pbs_select_compute_nodes,
)
from slurm import (
    classify_node as slurm_classify_node,
    resolve_node_type as slurm_resolve_node_type,
    select_compute_nodes as slurm_select_compute_nodes,
    slurm_inventory,
)
from probe import probe_node, probe_rundown
from baseline import compute_baseline_majors
from report import build_report, build_rundown_section, write_node_lists, write_pbs_skipped


EXAMPLES = """Examples:
  Inventory only (no compatibility judgement):
    ./libsweep --lib libjpeg --scope all --login-auto --baseline-from none --workers 32

  Sweep all and require SONAME major 62:
    ./libsweep --lib libjpeg.so.62 --scope all --login-auto --baseline-from login-consensus --workers 32

  Force baseline major 62:
    ./libsweep --lib libjpeg --scope all --login-auto --baseline-major 62 --workers 32

  Slurm inventory:
    ./libsweep --scheduler slurm --lib libjpeg --scope compute --workers 32

  Print this examples menu:
    ./libsweep --examples
"""

MAX_WORKERS = 128


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

def write_csv(path: str, fieldnames: List[str], rows: List[Dict]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in sorted(rows, key=lambda x: (x.get("lib_query",""), x.get("node",""))):
            w.writerow({k: row.get(k, "") for k in fieldnames})

def json_lines_only(stdout: str) -> List[str]:
    out = []
    for ln in (stdout or "").splitlines():
        s = ln.strip()
        if s.startswith("{") and s.endswith("}"):
            out.append(s)
    return out


def normalize_node_type(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return "compute"
    toks = [t for t in re.split(r"[^a-z0-9]+", s.lower()) if t]
    if not toks:
        return "compute"
    tokset = set(toks)
    if {"transfer", "xfer", "dtn", "dnt", "datatransfer"} & tokset:
        return "transfer"
    if {"visualization", "visual", "viz", "vis"} & tokset:
        return "visualization"
    if {"bigmem", "highmem", "hmem", "largemem"} & tokset:
        return "bigmem"
    return toks[0]


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


def classify_scheduler_node(active_scheduler: str, node: str, meta: Dict[str, str]) -> str:
    nodetype = meta.get("resources_available.nodetype", "")
    if active_scheduler == "slurm":
        return slurm_classify_node(node, nodetype, meta.get("scheduler.partition", ""))
    return pbs_classify_node(
        node,
        nodetype,
        meta.get("resources_available.clustertype", ""),
        meta.get("resources_available.bigmem", ""),
        meta.get("resources_available.compute", ""),
    )


def resolve_scheduler_node_type(active_scheduler: str, node: str, meta: Dict[str, str]) -> str:
    nodetype = meta.get("resources_available.nodetype", "")
    if active_scheduler == "slurm":
        return slurm_resolve_node_type(node, nodetype, meta.get("scheduler.partition", ""))
    return pbs_resolve_node_type(
        node,
        nodetype,
        meta.get("resources_available.clustertype", ""),
        meta.get("resources_available.bigmem", ""),
        meta.get("resources_available.compute", ""),
    )


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
        reps.append(
            {
                "node": nodes[0],
                "group_size": len(nodes),
                "group_nodes": ",".join(nodes),
                "lib_query": sig[0],
                "result": sig[1],
                "found_majors": sig[2],
                "missing_required_majors": sig[3],
            }
        )
    reps.sort(key=lambda r: (r["lib_query"], r["result"], r["node"]))
    return reps


def select_rundown_reference_node(login_rows: List[Dict], compute_rows: List[Dict], avoid_nodes: Set[str]) -> tuple:
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


def compare_rundown_manifests(reference_node: str, reference_manifest: Dict, node: str, node_manifest: Dict, trigger: Dict) -> List[Dict]:
    rows: List[Dict] = []
    roots = sorted(set(reference_manifest.keys()) | set(node_manifest.keys()))
    for root in roots:
        ref = reference_manifest.get(root)
        cur = node_manifest.get(root)
        ref_dict = ref if isinstance(ref, dict) else {}
        cur_dict = cur if isinstance(cur, dict) else {}

        if ref is None:
            rows.append(
                {
                    "reference_node": reference_node,
                    "node": node,
                    "lib_root": root,
                    "discrepancy_kind": "extra_on_node",
                    "reference_majors": "",
                    "node_majors": _int_csv(_int_set(cur_dict.get("majors", []))),
                    "reference_versions": "",
                    "node_versions": _str_csv(_str_set(cur_dict.get("versions", []))),
                    "reference_variants": "0",
                    "node_variants": str(cur_dict.get("variants_count", 0)),
                    "trigger_lib_query": trigger.get("lib_query", ""),
                    "trigger_result": trigger.get("result", ""),
                    "trigger_found_majors": trigger.get("found_majors", ""),
                    "trigger_missing_required_majors": trigger.get("missing_required_majors", ""),
                }
            )
            continue

        if cur is None:
            rows.append(
                {
                    "reference_node": reference_node,
                    "node": node,
                    "lib_root": root,
                    "discrepancy_kind": "missing_on_node",
                    "reference_majors": _int_csv(_int_set(ref_dict.get("majors", []))),
                    "node_majors": "",
                    "reference_versions": _str_csv(_str_set(ref_dict.get("versions", []))),
                    "node_versions": "",
                    "reference_variants": str(ref_dict.get("variants_count", 0)),
                    "node_variants": "0",
                    "trigger_lib_query": trigger.get("lib_query", ""),
                    "trigger_result": trigger.get("result", ""),
                    "trigger_found_majors": trigger.get("found_majors", ""),
                    "trigger_missing_required_majors": trigger.get("missing_required_majors", ""),
                }
            )
            continue

        ref_majors = _int_set(ref_dict.get("majors", []))
        cur_majors = _int_set(cur_dict.get("majors", []))
        ref_versions = _str_set(ref_dict.get("versions", []))
        cur_versions = _str_set(cur_dict.get("versions", []))

        if ref_majors != cur_majors:
            rows.append(
                {
                    "reference_node": reference_node,
                    "node": node,
                    "lib_root": root,
                    "discrepancy_kind": "majors_diff",
                    "reference_majors": _int_csv(ref_majors),
                    "node_majors": _int_csv(cur_majors),
                    "reference_versions": _str_csv(ref_versions),
                    "node_versions": _str_csv(cur_versions),
                    "reference_variants": str(ref_dict.get("variants_count", 0)),
                    "node_variants": str(cur_dict.get("variants_count", 0)),
                    "trigger_lib_query": trigger.get("lib_query", ""),
                    "trigger_result": trigger.get("result", ""),
                    "trigger_found_majors": trigger.get("found_majors", ""),
                    "trigger_missing_required_majors": trigger.get("missing_required_majors", ""),
                }
            )
            continue

        if ref_versions != cur_versions:
            rows.append(
                {
                    "reference_node": reference_node,
                    "node": node,
                    "lib_root": root,
                    "discrepancy_kind": "versions_diff",
                    "reference_majors": _int_csv(ref_majors),
                    "node_majors": _int_csv(cur_majors),
                    "reference_versions": _str_csv(ref_versions),
                    "node_versions": _str_csv(cur_versions),
                    "reference_variants": str(ref_dict.get("variants_count", 0)),
                    "node_variants": str(cur_dict.get("variants_count", 0)),
                    "trigger_lib_query": trigger.get("lib_query", ""),
                    "trigger_result": trigger.get("result", ""),
                    "trigger_found_majors": trigger.get("found_majors", ""),
                    "trigger_missing_required_majors": trigger.get("missing_required_majors", ""),
                }
            )

    return rows


def add_bool_option(ap: argparse.ArgumentParser, name: str, default: bool, help_text: str = "") -> None:
    if hasattr(argparse, "BooleanOptionalAction"):
        ap.add_argument(name, action=argparse.BooleanOptionalAction, default=default, help=help_text)
        return
    dest = name.lstrip("-").replace("-", "_")
    ap.add_argument(name, dest=dest, action="store_true", default=default, help=help_text)
    ap.add_argument(f"--no-{name[2:]}", dest=dest, action="store_false")

def main():
    ap = argparse.ArgumentParser(
        description="Cluster library inventory and compatibility sweep",
        epilog=EXAMPLES,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    ap.add_argument("--examples", action="store_true", help="Show usage examples and exit")

    ap.add_argument("--lib", action="append", required=False,
                    help="Repeatable. Examples: libjpeg OR jpeg OR libjpeg.so.62")
    ap.add_argument("--dirs", action="append", default=[], help="Extra directory globs")
    ap.add_argument("--no-ldconfig", action="store_true", help="Skip ldconfig -p")

    ap.add_argument("--scope", choices=["login","compute","all"], default=None,
                    help="Default: all (or compute if inside PBS job)")
    ap.add_argument("--scheduler", choices=["auto", "pbs", "slurm"], default="auto")
    ap.add_argument("--login-auto", action="store_true", help="Auto-discover login nodes prefixNN via SSH")
    ap.add_argument("--login-prefix", default=None)
    ap.add_argument("--login-width", type=int, default=None)
    ap.add_argument("--login-max", type=int, default=50)
    ap.add_argument("--login-stop-after-gap", type=int, default=6)

    add_bool_option(ap, "--pbs-online-only", True)
    add_bool_option(ap, "--pbs-compute-flag-only", True)

    ap.add_argument("--baseline-from", choices=["login-consensus","login-union","login-intersection","none"], default="login-consensus")
    ap.add_argument("--baseline-major", type=int, default=None)

    ap.add_argument("--remote-python", default="python3")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--ssh-timeout", type=int, default=120)
    ap.add_argument("--retries", type=int, default=2)
    ap.add_argument("--discrepancy-rundown-workers", type=int, default=8)

    ap.add_argument("--ssh-hostkey", choices=["accept-new","no","yes"], default="accept-new")
    ap.add_argument("--ssh-known-hosts", default=None)
    add_bool_option(ap, "--ssh-control-master", False, "Enable OpenSSH connection reuse (default: disabled)")
    add_bool_option(ap, "--remote-low-priority", True, "Run remote probe with low CPU priority via nice (default: enabled)")

    ap.add_argument("--out-prefix", default="lib_sweep")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--write-node-lists", action="store_true")
    ap.add_argument(
        "--detail",
        choices=["concise", "full"],
        default="concise",
        help="CSV/report detail level (default: concise)",
    )
    ap.add_argument("--write-json-summary", action="store_true", help="Write JSON summary report")
    add_bool_option(
        ap,
        "--discrepancy-rundown",
        False,
        "When inconsistency/missing is detected, compare standard-lib manifests on representative flagged nodes.",
    )

    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--probe-rundown", action="store_true")

    args = ap.parse_args()

    if args.examples:
        print(EXAMPLES)
        return

    if not args.lib and not args.probe_rundown:
        ap.error("--lib is required unless --examples is used")

    requested_workers = args.workers
    args.workers = clamp_workers(args.workers)
    if args.workers != requested_workers:
        print(
            f"warning: clamping --workers from {requested_workers} to {args.workers} (cap={MAX_WORKERS})",
            file=sys.stderr,
        )

    # Probe mode
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

    in_scheduler_job = any(
        k in os.environ for k in ("PBS_JOBID", "PBS_NODEFILE", "PBS_ENVIRONMENT", "SLURM_JOB_ID", "SLURM_NTASKS")
    )
    scope = args.scope or ("compute" if in_scheduler_job else "all")
    active_scheduler = detect_scheduler(args.scheduler)

    cfg = default_ssh_config()
    cfg.hostkey_mode = args.ssh_hostkey
    if args.ssh_known_hosts:
        cfg.known_hosts = os.path.expanduser(args.ssh_known_hosts)
        os.makedirs(os.path.dirname(cfg.known_hosts), exist_ok=True)
    cfg.control_master = bool(args.ssh_control_master)

    thread_stack_setting = configure_thread_stack_size()

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_prefix = f"{args.out_prefix}_{ts}"
    script_path = os.path.realpath(__import__("sys").argv[0])

    # login nodes
    host = short_hostname(os.uname().nodename)
    login_nodes: List[str] = []
    if scope in ("login","all"):
        if args.login_auto:
            m = re.match(r"^(.+?)(\d+)$", host)
            prefix = args.login_prefix or (m.group(1) if m else host)
            width = args.login_width or (len(m.group(2)) if m else 2)

            found = []
            last_success = 0
            for i in range(1, args.login_max + 1):
                cand = f"{prefix}{i:0{width}d}"
                p, kind = ssh_with_retries(cand, ["true"], cfg, timeout=8, retries=0)
                if p.returncode == 0:
                    found.append(cand)
                    last_success = i
                else:
                    if last_success > 0 and (i - last_success) >= args.login_stop_after_gap:
                        break
            login_nodes = sorted(set(found + ([host] if host.startswith(prefix) else [])))
        else:
            login_nodes = [host]

    # compute nodes
    compute_nodes: List[str] = []
    node_inv: Dict[str, Dict[str, str]] = {}
    scheduler_skipped = []
    if scope in ("compute","all"):
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

    compute_nodes = [n for n in compute_nodes if n not in set(login_nodes)]

    # dry run
    if args.dry_run:
        print(f"DRY RUN {ts}")
        print(f"Scope: {scope}")
        print(f"Scheduler: {active_scheduler}")
        print(f"SSH control master: {'enabled' if cfg.control_master else 'disabled'}")
        print(f"Workers: requested={requested_workers} effective={args.workers} cap={MAX_WORKERS}")
        print(f"Thread stack: {thread_stack_setting}")
        print(
            "Discrepancy rundown: "
            f"{'enabled' if args.discrepancy_rundown else 'disabled'} "
            f"workers={clamp_workers(args.discrepancy_rundown_workers)}"
        )
        print(f"Libraries: {args.lib}")
        print(f"Login nodes: {len(login_nodes)} sample: {', '.join(login_nodes[:20])}")
        print(f"Compute nodes selected: {len(compute_nodes)} sample: {', '.join(compute_nodes[:20])}")
        print(f"Scheduler skipped: {len(scheduler_skipped)}")
        ex_node = login_nodes[0] if login_nodes else (compute_nodes[0] if compute_nodes else '<node>')
        cmd = ["ssh", ex_node, args.remote_python, script_path, "--probe"]
        for lib in args.lib:
            cmd += ["--lib", lib]
        print("Example probe command:")
        print("  " + " ".join(cmd))
        return

    # sweep fanout
    def sweep_node(node: str, role: str):
        argv = []
        if args.remote_low_priority:
            argv += ["nice", "-n", "19"]
        argv += [args.remote_python, script_path, "--probe"]
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
                "role": role,
                "node": node,
                "lib_query": lib,
                "status": status,
                "ssh_rc": str(p.returncode),
                "ssh_error_kind": kind,
                "ssh_error_detail": detail,
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
                out.append({"_role": role, "node": node, "query": lib, "present": False, "majors": [], "versions": [], "primary_major": "", "primary_version": "", "primary_target": "", "variants_count": 0})
        return out

    from concurrent.futures import ThreadPoolExecutor, as_completed
    ok_records = []
    error_records = []

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {}
        for n in login_nodes:
            fut = ex.submit(sweep_node, n, "login")
            futs[fut] = (short_hostname(n), "login")
        for n in compute_nodes:
            fut = ex.submit(sweep_node, n, "compute")
            futs[fut] = (short_hostname(n), "compute")

        for fut in as_completed(futs):
            node_name, role = futs[fut]
            try:
                rows = fut.result()
            except Exception as exn:
                for lib in args.lib:
                    error_records.append({
                        "role": role,
                        "node": node_name,
                        "lib_query": lib,
                        "status": "internal_error",
                        "ssh_rc": "-1",
                        "ssh_error_kind": "internal_error",
                        "ssh_error_detail": str(exn)[:240],
                    })
                continue

            for r in rows:
                if r.get("status") in ("ssh_error", "probe_error"):
                    error_records.append(r)
                else:
                    ok_records.append(r)

    # Build login rows (for baseline derivation)
    login_rows = []
    login_ok_by_lib = {lib: [] for lib in args.lib}
    for r in ok_records:
        if r.get("_role") != "login":
            continue
        node = short_hostname(r.get("node",""))
        libq = r.get("query","")
        row = {
            "node": node,
            "node_type": "login",
            "lib_query": libq,
            "result": "observed",
            "issue_detail": "",
            "required_majors": "",
            "found_majors": ",".join(str(m) for m in (r.get("majors") or [])),
            "missing_required_majors": "",
            "primary_major":str(r.get("primary_major","") if r.get("primary_major") is not None else ""),
            "primary_version":str(r.get("primary_version","") or ""),
            "primary_target":str(r.get("primary_target","") or ""),
            "error_kind":"",
            "error_detail":"",
        }
        login_rows.append(row)
        login_ok_by_lib[libq].append(row)

    # baselines
    baselines = {
        lib: compute_baseline_majors(lib, login_ok_by_lib.get(lib, []), args.baseline_from, args.baseline_major)
        for lib in args.lib
    }

    # compute rows
    compute_rows = []
    for r in ok_records:
        if r.get("_role") != "compute":
            continue
        node = short_hostname(r.get("node",""))
        libq = r.get("query","")
        meta = node_inv.get(node, {})
        pbs_state = meta.get("state","")
        pbs_nodetype = meta.get("resources_available.nodetype","")
        pbs_compute_flag = meta.get("resources_available.compute","").strip()
        scheduler_partition = meta.get("scheduler.partition", "")
        node_class = classify_scheduler_node(active_scheduler, node, meta)
        node_type = resolve_scheduler_node_type(active_scheduler, node, meta)

        majors_list = r.get("majors") or []
        majors_csv = ",".join(str(m) for m in majors_list)
        present = bool(r.get("present"))

        baseline = baselines.get(libq, set())
        baseline_csv = ",".join(str(m) for m in sorted(baseline))
        missing = sorted(baseline - set(int(m) for m in majors_list if isinstance(m,int)))

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
            "lib_query":libq,
            "result": result,
            "issue_detail": issue_detail,
            "required_majors": baseline_csv,
            "found_majors": majors_csv,
            "missing_required_majors": missing_csv,
            "primary_major":str(r.get("primary_major","") if r.get("primary_major") is not None else ""),
            "primary_version":str(r.get("primary_version","") or ""),
            "primary_target":str(r.get("primary_target","") or ""),
            "error_kind":"",
            "error_detail":"",
        }
        if args.detail == "full":
            row.update({
                "node_class": node_class,
                "scheduler": active_scheduler,
                "scheduler_partition": scheduler_partition,
                "pbs_state": pbs_state,
                "pbs_nodetype": pbs_nodetype,
                "pbs_compute_flag": pbs_compute_flag,
            })
            row["versions"] = ",".join(str(v) for v in (r.get("versions") or []))
            row["variants_count"] = str(r.get("variants_count",""))
        compute_rows.append(row)

    # attach errors
    for e in error_records:
        role = e.get("role", "compute")
        node = short_hostname(e.get("node",""))
        libq = e.get("lib_query","")
        meta = node_inv.get(node, {})
        pbs_state = meta.get("state","")
        pbs_nodetype = meta.get("resources_available.nodetype","")
        pbs_compute_flag = meta.get("resources_available.compute","").strip()
        scheduler_partition = meta.get("scheduler.partition", "")
        node_class = classify_scheduler_node(active_scheduler, node, meta)
        node_type = resolve_scheduler_node_type(active_scheduler, node, meta)

        row = {
            "node": node,
            "node_type": node_type,
            "lib_query":libq,
            "result": "unreachable",
            "issue_detail": e.get("ssh_error_kind", "ssh_error"),
            "required_majors": "",
            "found_majors": "",
            "missing_required_majors": "",
            "primary_major":"",
            "primary_version":"",
            "primary_target":"",
            "error_kind":e.get("ssh_error_kind","ssh_error"),
            "error_detail":e.get("ssh_error_detail",""),
        }
        if args.detail == "full":
            row.update({
                "node_class": node_class,
                "scheduler": active_scheduler if role == "compute" else "local",
                "scheduler_partition": scheduler_partition,
                "pbs_state": pbs_state,
                "pbs_nodetype": pbs_nodetype,
                "pbs_compute_flag": pbs_compute_flag,
            })
            row["versions"]=""
            row["variants_count"]=""
        if role == "login":
            row["result"] = "unreachable"
            row["node_type"] = "login"
            login_rows.append(row)
        else:
            compute_rows.append(row)

    rundown_rows: List[Dict] = []
    rundown_nodes: List[Dict] = []
    rundown_csv = f"{out_prefix}_rundown_discrepancies.csv"
    rundown_nodes_txt = f"{out_prefix}_rundown_nodes.txt"
    rundown_enabled = bool(args.discrepancy_rundown and scope in ("compute", "all"))
    rundown_triggered = False
    rundown_reference_node = ""
    rundown_reference_role = ""

    if rundown_enabled:
        reps = build_discrepancy_representatives(compute_rows)
        if reps:
            rundown_triggered = True
            rep_nodes = {r["node"] for r in reps}
            rundown_reference_node, rundown_reference_role = select_rundown_reference_node(login_rows, compute_rows, rep_nodes)

            triggers_by_node: Dict[str, List[Dict]] = {}
            for rep in reps:
                triggers_by_node.setdefault(rep["node"], []).append(rep)

            for rep in reps:
                rundown_nodes.append(
                    {
                        "node": rep["node"],
                        "role": "compute",
                        "status": "planned",
                        "note": (
                            f"trigger lib={rep.get('lib_query','')} result={rep.get('result','')} "
                            f"group_size={rep.get('group_size', 1)}"
                        ),
                    }
                )

            if rundown_reference_node:
                rundown_nodes.append(
                    {
                        "node": rundown_reference_node,
                        "role": rundown_reference_role,
                        "status": "planned_reference",
                        "note": "reference_manifest",
                    }
                )

                scan_plan: Dict[str, str] = {rundown_reference_node: rundown_reference_role}
                for node in sorted(triggers_by_node.keys()):
                    scan_plan.setdefault(node, "compute")

                def rundown_probe_node(node: str, role: str) -> Dict:
                    argv = []
                    if args.remote_low_priority:
                        argv += ["nice", "-n", "19"]
                    argv += [args.remote_python, script_path, "--probe-rundown"]
                    for d in args.dirs:
                        argv += ["--dirs", d]
                    if args.no_ldconfig:
                        argv += ["--no-ldconfig"]

                    p, kind = ssh_with_retries(node, argv, cfg, timeout=args.ssh_timeout, retries=args.retries)
                    short = short_hostname(node)
                    if p.returncode != 0:
                        return {
                            "node": short,
                            "role": role,
                            "status": "error",
                            "kind": kind,
                            "detail": (p.stderr or "").strip()[:240],
                            "manifest": {},
                        }

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
                        return {
                            "node": short,
                            "role": role,
                            "status": "error",
                            "kind": "parse_error",
                            "detail": "probe-rundown returned no manifest",
                            "manifest": {},
                        }

                    return {
                        "node": short,
                        "role": role,
                        "status": "ok",
                        "kind": "ok",
                        "detail": "",
                        "manifest": payload.get("manifest", {}),
                    }

                from concurrent.futures import ThreadPoolExecutor, as_completed

                manifest_by_node: Dict[str, Dict] = {}
                workers = clamp_workers(args.discrepancy_rundown_workers)
                max_workers = max(1, min(workers, len(scan_plan)))
                with ThreadPoolExecutor(max_workers=max_workers) as ex:
                    futs = {
                        ex.submit(rundown_probe_node, node, role): (node, role)
                        for node, role in scan_plan.items()
                    }
                    for fut in as_completed(futs):
                        node, role = futs[fut]
                        try:
                            res = fut.result()
                        except Exception as exn:
                            short = short_hostname(node)
                            rundown_nodes.append(
                                {
                                    "node": short,
                                    "role": role,
                                    "status": "error",
                                    "note": f"internal_error: {str(exn)[:160]}",
                                }
                            )
                            continue

                        short = short_hostname(node)
                        if res.get("status") == "ok":
                            manifest_by_node[short] = res.get("manifest", {})
                            lib_count = len(res.get("manifest", {}) or {})
                            rundown_nodes.append(
                                {
                                    "node": short,
                                    "role": role,
                                    "status": "scanned",
                                    "note": f"manifest_lib_count={lib_count}",
                                }
                            )
                        else:
                            rundown_nodes.append(
                                {
                                    "node": short,
                                    "role": role,
                                    "status": "error",
                                    "note": f"{res.get('kind','error')}: {res.get('detail','')}",
                                }
                            )

                ref_short = short_hostname(rundown_reference_node)
                ref_manifest = manifest_by_node.get(ref_short)
                if isinstance(ref_manifest, dict) and ref_manifest:
                    for rep in reps:
                        node = rep["node"]
                        if node == ref_short:
                            continue
                        node_manifest = manifest_by_node.get(node)
                        if not isinstance(node_manifest, dict) or not node_manifest:
                            continue
                        rundown_rows.extend(
                            compare_rundown_manifests(
                                reference_node=ref_short,
                                reference_manifest=ref_manifest,
                                node=node,
                                node_manifest=node_manifest,
                                trigger=rep,
                            )
                        )
                else:
                    rundown_nodes.append(
                        {
                            "node": ref_short,
                            "role": rundown_reference_role,
                            "status": "error",
                            "note": "reference_manifest_unavailable",
                        }
                    )
            else:
                rundown_nodes.append(
                    {
                        "node": "",
                        "role": "",
                        "status": "skipped",
                        "note": "no suitable reference node for discrepancy rundown",
                    }
                )
        else:
            rundown_nodes.append(
                {
                    "node": "",
                    "role": "",
                    "status": "skipped",
                    "note": "no inconsistent/missing rows found; discrepancy rundown not triggered",
                }
            )

    # output files
    login_csv = f"{out_prefix}_login.csv"
    compute_csv = f"{out_prefix}_compute.csv"
    report_txt = f"{out_prefix}_report.txt"
    skipped_txt = f"{out_prefix}_{active_scheduler}_skipped.txt"

    concise_fields = [
        "node",
        "node_type",
        "lib_query",
        "result",
        "issue_detail",
    ]
    full_fields = [
        "node",
        "node_type",
        "node_class",
        "scheduler",
        "scheduler_partition",
        "pbs_state",
        "pbs_nodetype",
        "pbs_compute_flag",
        "lib_query",
        "result",
        "issue_detail",
        "required_majors",
        "found_majors",
        "missing_required_majors",
        "primary_major",
        "primary_version",
        "primary_target",
        "error_kind",
        "error_detail",
        "versions",
        "variants_count",
    ]
    selected_fields = full_fields if args.detail == "full" else concise_fields

    if scope in ("login","all"):
        write_csv(login_csv, selected_fields, login_rows)
    if scope in ("compute","all"):
        write_csv(compute_csv, selected_fields, compute_rows)

    write_pbs_skipped(skipped_txt, scheduler_skipped)

    if rundown_enabled:
        with open(rundown_nodes_txt, "w", encoding="utf-8") as f:
            f.write("node\trole\tstatus\tnote\n")
            for r in rundown_nodes:
                f.write(
                    f"{r.get('node','')}\t{r.get('role','')}\t{r.get('status','')}\t{r.get('note','')}\n"
                )
        if rundown_triggered and rundown_reference_node:
            rundown_fields = [
                "reference_node",
                "node",
                "lib_root",
                "discrepancy_kind",
                "reference_majors",
                "node_majors",
                "reference_versions",
                "node_versions",
                "reference_variants",
                "node_variants",
                "trigger_lib_query",
                "trigger_result",
                "trigger_found_majors",
                "trigger_missing_required_majors",
            ]
            write_csv(rundown_csv, rundown_fields, rundown_rows)

    node_list_files: Dict[str, Dict[str,str]] = {}
    if args.write_node_lists and scope in ("compute","all"):
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
        enabled=rundown_enabled,
        triggered=rundown_triggered,
        reference_node=rundown_reference_node,
        reference_role=rundown_reference_role,
        scanned_nodes=rundown_nodes,
        discrepancy_rows=rundown_rows,
        discrepancy_csv=rundown_csv if rundown_enabled and rundown_triggered and rundown_reference_node else "",
        nodes_txt=rundown_nodes_txt if rundown_enabled else "",
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
            "ts": ts,
            "scheduler": active_scheduler,
            "scope": scope,
            "baseline_from": args.baseline_from,
            "baseline_major": args.baseline_major,
            "login_nodes": len(login_nodes),
            "compute_nodes": len(compute_nodes),
            "scheduler_skipped": len(scheduler_skipped),
            "libs": by_lib,
        }
        with open(summary_json, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, sort_keys=True)

    print(f"Wrote login CSV:   {login_csv}" if scope in ("login","all") else "Login scope disabled")
    print(f"Wrote compute CSV: {compute_csv}" if scope in ("compute","all") else "Compute scope disabled")
    print(f"Wrote report:      {report_txt}")
    print(f"Wrote scheduler skipped: {skipped_txt}")
    if rundown_enabled:
        if rundown_triggered and rundown_reference_node:
            print(f"Wrote discrepancy rundown CSV: {rundown_csv}")
        print(f"Wrote discrepancy rundown nodes: {rundown_nodes_txt}")
    if args.write_json_summary:
        print(f"Wrote JSON summary: {summary_json}")

    total_inconsistent = sum(1 for r in compute_rows if r.get("result") == "inconsistent")
    total_missing = sum(1 for r in compute_rows if r.get("result") == "missing")
    total_errors = sum(1 for r in compute_rows if r.get("result") == "unreachable")
    if total_errors > 0:
        sys.exit(2)
    if (total_inconsistent + total_missing) > 0:
        sys.exit(1)
