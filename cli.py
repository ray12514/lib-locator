import argparse
import csv
import json
import os
import re
from datetime import datetime, timezone
from typing import Dict, List, Set

from .sshfanout import default_ssh_config, ssh_with_retries, short_hostname
from .pbs import pbs_inventory, select_compute_nodes, classify_node
from .probe import probe_node
from .baseline import compute_baseline_majors
from .report import write_pbs_skipped, write_node_lists, build_report

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
    ap = argparse.ArgumentParser()

    ap.add_argument("--lib", action="append", required=True,
                    help="Repeatable. Examples: libjpeg OR jpeg OR libjpeg.so.62")
    ap.add_argument("--dirs", action="append", default=[], help="Extra directory globs")
    ap.add_argument("--no-ldconfig", action="store_true", help="Skip ldconfig -p")

    ap.add_argument("--scope", choices=["login","compute","all"], default=None,
                    help="Default: all (or compute if inside PBS job)")
    ap.add_argument("--login-auto", action="store_true", help="Auto-discover login nodes prefixNN via SSH")
    ap.add_argument("--login-prefix", default=None)
    ap.add_argument("--login-width", type=int, default=None)
    ap.add_argument("--login-max", type=int, default=50)
    ap.add_argument("--login-stop-after-gap", type=int, default=6)

    ap.add_argument("--pbs-online-only", action="store_true", default=True)
    ap.add_argument("--pbs-compute-flag-only", action="store_true", default=True)

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

    ap.add_argument("--probe", action="store_true")

    args = ap.parse_args()

    # Probe mode
    if args.probe:
        ts = datetime.now(timezone.utc).isoformat()
        for lib in args.lib:
            r = probe_node(lib, args.dirs, args.no_ldconfig)
            r["ts_utc"] = ts
            print(json.dumps(r, sort_keys=True))
        return

    in_pbs = any(k in os.environ for k in ("PBS_JOBID","PBS_NODEFILE","PBS_ENVIRONMENT"))
    scope = args.scope or ("compute" if in_pbs else "all")

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
    pbs_inv: Dict[str, Dict[str, str]] = {}
    pbs_skipped = []
    if scope in ("compute","all"):
        _, _, inv = pbs_inventory()
        pbs_inv = inv
        compute_nodes, pbs_skipped = select_compute_nodes(inv, online_only=args.pbs_online_only, compute_flag_only=args.pbs_compute_flag_only)

    compute_nodes = [n for n in compute_nodes if n not in set(login_nodes)]

    # dry run
    if args.dry_run:
        print(f"DRY RUN {ts}")
        print(f"Scope: {scope}")
        print(f"Libraries: {args.lib}")
        print(f"Login nodes: {len(login_nodes)} sample: {', '.join(login_nodes[:20])}")
        print(f"Compute nodes selected: {len(compute_nodes)} sample: {', '.join(compute_nodes[:20])}")
        print(f"PBS skipped: {len(pbs_skipped)}")
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
        futs = []
        for n in login_nodes:
            futs.append(ex.submit(sweep_node, n, "login"))
        for n in compute_nodes:
            futs.append(ex.submit(sweep_node, n, "compute"))

        for fut in as_completed(futs):
            for r in fut.result():
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
        meta = pbs_inv.get(node, {})
        pbs_state = meta.get("state","")
        pbs_nodetype = meta.get("resources_available.nodetype","")
        pbs_compute_flag = meta.get("resources_available.compute","").strip()
        node_class = classify_node(node, pbs_nodetype)

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
        node = short_hostname(e.get("node",""))
        libq = e.get("lib_query","")
        meta = pbs_inv.get(node, {})
        pbs_state = meta.get("state","")
        pbs_nodetype = meta.get("resources_available.nodetype","")
        pbs_compute_flag = meta.get("resources_available.compute","").strip()
        node_class = classify_node(node, pbs_nodetype)

        row = {
            "role":"compute","node":node,"node_class":node_class,
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
        compute_rows.append(row)

    # output files
    login_csv = f"{out_prefix}_login.csv"
    compute_csv = f"{out_prefix}_compute.csv"
    report_txt = f"{out_prefix}_report.txt"
    skipped_txt = f"{out_prefix}_pbs_skipped.txt"

    base_fields = [
        "role","node","node_class","pbs_state","pbs_nodetype","pbs_compute_flag",
        "lib_query","present","compatibility","baseline_majors","missing_baseline_majors",
        "primary_major","primary_version","primary_target","majors",
        "status","ssh_rc","ssh_error_kind","ssh_error_detail",
    ]
    verbose_fields = base_fields + ["versions","variants_count"]

    if scope in ("login","all"):
        write_csv(login_csv, base_fields, login_rows)
    if scope in ("compute","all"):
        write_csv(compute_csv, (verbose_fields if args.verbose_csv else base_fields), compute_rows)

    write_pbs_skipped(skipped_txt, pbs_skipped)

    node_list_files: Dict[str, Dict[str,str]] = {}
    if args.write_node_lists and scope in ("compute","all"):
        for lib in args.lib:
            node_list_files[lib] = write_node_lists(out_prefix, lib, compute_rows)

    report = build_report(
        ts=ts,
        scope=scope,
        baseline_from=args.baseline_from,
        baseline_major=str(args.baseline_major) if args.baseline_major is not None else "(none)",
        workers=args.workers,
        retries=args.retries,
        login_nodes=len(login_nodes),
        compute_nodes=len(compute_nodes),
        pbs_skipped_count=len(pbs_skipped),
        libs=args.lib,
        login_rows=login_rows,
        compute_rows=compute_rows,
        node_list_files=node_list_files,
    )
    with open(report_txt, "w", encoding="utf-8") as f:
        f.write(report)

    print(f"Wrote login CSV:   {login_csv}" if scope in ("login","all") else "Login scope disabled")
    print(f"Wrote compute CSV: {compute_csv}" if scope in ("compute","all") else "Compute scope disabled")
    print(f"Wrote report:      {report_txt}")
    print(f"Wrote PBS skipped: {skipped_txt}")
