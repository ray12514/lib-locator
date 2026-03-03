import argparse
import csv
import json
import os
import re
import shutil
import sys
from datetime import datetime, timezone
from typing import Dict, List, Set

from sshfanout import default_ssh_config, ssh_with_retries, short_hostname
from pbs import pbs_inventory, select_compute_nodes as pbs_select_compute_nodes, classify_node as pbs_classify_node
from slurm import slurm_inventory, select_compute_nodes as slurm_select_compute_nodes, classify_node as slurm_classify_node
from probe import probe_node
from baseline import compute_baseline_majors
from report import write_pbs_skipped, write_node_lists, build_report


EXAMPLES = """Examples:
  Inventory only (no compatibility judgement):
    python3 lib_sweep.py --lib libjpeg --scope all --login-auto --baseline-from none --workers 32

  Sweep all and require SONAME major 62:
    python3 lib_sweep.py --lib libjpeg.so.62 --scope all --login-auto --baseline-from login-consensus --workers 32

  Force baseline major 62:
    python3 lib_sweep.py --lib libjpeg --scope all --login-auto --baseline-major 62 --workers 32

  Slurm inventory:
    python3 lib_sweep.py --scheduler slurm --lib libjpeg --scope compute --workers 32

  Print this examples menu:
    python3 lib_sweep.py --examples
"""


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
            w.writerow(row)

def json_lines_only(stdout: str) -> List[str]:
    out = []
    for ln in (stdout or "").splitlines():
        s = ln.strip()
        if s.startswith("{") and s.endswith("}"):
            out.append(s)
    return out

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

    ap.add_argument("--pbs-online-only", action=argparse.BooleanOptionalAction, default=True)
    ap.add_argument("--pbs-compute-flag-only", action=argparse.BooleanOptionalAction, default=True)

    ap.add_argument("--baseline-from", choices=["login-consensus","login-union","login-intersection","none"], default="login-consensus")
    ap.add_argument("--baseline-major", type=int, default=None)

    ap.add_argument("--remote-python", default="python3")
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--ssh-timeout", type=int, default=120)
    ap.add_argument("--retries", type=int, default=2)

    ap.add_argument("--ssh-hostkey", choices=["accept-new","no","yes"], default="accept-new")
    ap.add_argument("--ssh-known-hosts", default=None)
    ap.add_argument("--ssh-control-master", action="store_true")

    ap.add_argument("--out-prefix", default="lib_sweep")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--write-node-lists", action="store_true")
    ap.add_argument("--verbose-csv", action="store_true")
    ap.add_argument("--write-json-summary", action="store_true", help="Write JSON summary report")

    ap.add_argument("--probe", action="store_true")

    args = ap.parse_args()

    if args.examples:
        print(EXAMPLES)
        return

    if not args.lib:
        ap.error("--lib is required unless --examples is used")

    # Probe mode
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

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_prefix = f"{args.out_prefix}_{ts}"

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
        print(f"Libraries: {args.lib}")
        print(f"Login nodes: {len(login_nodes)} sample: {', '.join(login_nodes[:20])}")
        print(f"Compute nodes selected: {len(compute_nodes)} sample: {', '.join(compute_nodes[:20])}")
        print(f"Scheduler skipped: {len(scheduler_skipped)}")
        ex_node = login_nodes[0] if login_nodes else (compute_nodes[0] if compute_nodes else '<node>')
        cmd = ["ssh", ex_node, args.remote_python, os.path.realpath(__import__('sys').argv[0]), "--probe"]
        for lib in args.lib:
            cmd += ["--lib", lib]
        print("Example probe command:")
        print("  " + " ".join(cmd))
        return

    # sweep fanout
    def sweep_node(node: str, role: str):
        argv = [args.remote_python, os.path.realpath(__import__('sys').argv[0]), "--probe"]
        for lib in args.lib:
            argv += ["--lib", lib]
        for d in args.dirs:
            argv += ["--dirs", d]
        if args.no_ldconfig:
            argv += ["--no-ldconfig"]

        p, kind = ssh_with_retries(node, argv, cfg, timeout=args.ssh_timeout, retries=args.retries)
        node = short_hostname(node)

        if p.returncode != 0:
            return [{
                "role": role,
                "node": node,
                "lib_query": lib,
                "status": "ssh_error",
                "ssh_rc": str(p.returncode),
                "ssh_error_kind": kind,
                "ssh_error_detail": (p.stderr or "").strip()[:240],
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
                if r.get("status") == "ssh_error":
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
            "role":"login","node":node,"node_class":"login",
            "scheduler":"local","scheduler_partition":"",
            "pbs_state":"","pbs_nodetype":"","pbs_compute_flag":"",
            "lib_query":libq,
            "present":str(bool(r.get("present"))),
            "compatibility":"n/a",
            "baseline_majors":"",
            "missing_baseline_majors":"",
            "primary_major":str(r.get("primary_major","") if r.get("primary_major") is not None else ""),
            "primary_version":str(r.get("primary_version","") or ""),
            "primary_target":str(r.get("primary_target","") or ""),
            "majors":",".join(str(m) for m in (r.get("majors") or [])),
            "status":"ok","ssh_rc":"0","ssh_error_kind":"ok","ssh_error_detail":""
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
        node_class = slurm_classify_node(node, pbs_nodetype) if active_scheduler == "slurm" else pbs_classify_node(node, pbs_nodetype)

        majors_list = r.get("majors") or []
        majors_csv = ",".join(str(m) for m in majors_list)
        present = bool(r.get("present"))

        baseline = baselines.get(libq, set())
        baseline_csv = ",".join(str(m) for m in sorted(baseline))
        missing = sorted(baseline - set(int(m) for m in majors_list if isinstance(m,int)))

        if not present:
            compatibility = "missing"
            missing_csv = baseline_csv
        elif not baseline:
            compatibility = "n/a"
            missing_csv = ""
        elif not missing:
            compatibility = "compatible"
            missing_csv = ""
        else:
            compatibility = "incompatible"
            missing_csv = ",".join(str(m) for m in missing)

        row = {
            "role":"compute","node":node,"node_class":node_class,
            "scheduler":active_scheduler,"scheduler_partition":scheduler_partition,
            "pbs_state":pbs_state,"pbs_nodetype":pbs_nodetype,"pbs_compute_flag":pbs_compute_flag,
            "lib_query":libq,
            "present":str(present),
            "compatibility":compatibility,
            "baseline_majors":baseline_csv,
            "missing_baseline_majors":missing_csv,
            "primary_major":str(r.get("primary_major","") if r.get("primary_major") is not None else ""),
            "primary_version":str(r.get("primary_version","") or ""),
            "primary_target":str(r.get("primary_target","") or ""),
            "majors":majors_csv,
            "status":"ok","ssh_rc":"0","ssh_error_kind":"ok","ssh_error_detail":""
        }
        if args.verbose_csv:
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
        node_class = slurm_classify_node(node, pbs_nodetype) if active_scheduler == "slurm" else pbs_classify_node(node, pbs_nodetype)

        row = {
            "role":role,"node":node,"node_class":node_class,
            "scheduler":active_scheduler if role == "compute" else "local",
            "scheduler_partition":scheduler_partition,
            "pbs_state":pbs_state,"pbs_nodetype":pbs_nodetype,"pbs_compute_flag":pbs_compute_flag,
            "lib_query":libq,
            "present":"",
            "compatibility":"",
            "baseline_majors":"",
            "missing_baseline_majors":"",
            "primary_major":"",
            "primary_version":"",
            "primary_target":"",
            "majors":"",
            "status":"ssh_error",
            "ssh_rc":e.get("ssh_rc",""),
            "ssh_error_kind":e.get("ssh_error_kind","ssh_error"),
            "ssh_error_detail":e.get("ssh_error_detail",""),
        }
        if args.verbose_csv:
            row["versions"]=""
            row["variants_count"]=""
        if role == "login":
            row["compatibility"] = "n/a"
            login_rows.append(row)
        else:
            compute_rows.append(row)

    # output files
    login_csv = f"{out_prefix}_login.csv"
    compute_csv = f"{out_prefix}_compute.csv"
    report_txt = f"{out_prefix}_report.txt"
    skipped_txt = f"{out_prefix}_{active_scheduler}_skipped.txt"

    base_fields = [
        "role","node","node_class","scheduler","scheduler_partition","pbs_state","pbs_nodetype","pbs_compute_flag",
        "lib_query","present","compatibility","baseline_majors","missing_baseline_majors",
        "primary_major","primary_version","primary_target","majors",
        "status","ssh_rc","ssh_error_kind","ssh_error_detail",
    ]
    verbose_fields = base_fields + ["versions","variants_count"]

    if scope in ("login","all"):
        write_csv(login_csv, base_fields, login_rows)
    if scope in ("compute","all"):
        write_csv(compute_csv, (verbose_fields if args.verbose_csv else base_fields), compute_rows)

    write_pbs_skipped(skipped_txt, scheduler_skipped)

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
    with open(report_txt, "w", encoding="utf-8") as f:
        f.write(report)

    summary_json = ""
    if args.write_json_summary:
        summary_json = f"{out_prefix}_summary.json"
        by_lib = {}
        for lib in args.lib:
            c_ok = [r for r in compute_rows if r.get("lib_query") == lib and r.get("status") == "ok"]
            c_err = [r for r in compute_rows if r.get("lib_query") == lib and r.get("status") != "ok"]
            by_lib[lib] = {
                "compute_ok": len(c_ok),
                "compute_errors": len(c_err),
                "compatible": sum(1 for r in c_ok if r.get("compatibility") == "compatible"),
                "incompatible": sum(1 for r in c_ok if r.get("compatibility") == "incompatible"),
                "missing": sum(1 for r in c_ok if r.get("compatibility") == "missing"),
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
    if args.write_json_summary:
        print(f"Wrote JSON summary: {summary_json}")

    total_incompatible = sum(1 for r in compute_rows if r.get("compatibility") == "incompatible")
    total_missing = sum(1 for r in compute_rows if r.get("compatibility") == "missing")
    total_errors = sum(1 for r in compute_rows if r.get("status") != "ok")
    if total_errors > 0:
        sys.exit(2)
    if (total_incompatible + total_missing) > 0:
        sys.exit(1)
