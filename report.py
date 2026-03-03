import os
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple

def sanitize_name(s: str) -> str:
    import re
    return re.sub(r"[^A-Za-z0-9_-]+", "_", s)

def sample(nodes: List[str], limit: int = 25) -> str:
    nodes = sorted(nodes)
    if len(nodes) <= limit:
        return ", ".join(nodes)
    return ", ".join(nodes[:limit]) + f", ... (+{len(nodes)-limit} more)"

def write_pbs_skipped(path: str, skipped: List[Tuple[str,str,str,str,str]]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write("node\treason\tstate\tnode_class\tpbs_nodetype\n")
        seen = set()
        for row in sorted(skipped):
            if row in seen:
                continue
            seen.add(row)
            n, reason, st, cls, nt = row
            f.write(f"{n}\t{reason}\t{st}\t{cls}\t{nt}\n")

def write_node_lists(out_prefix: str, lib: str, compute_rows: List[Dict]) -> Dict[str,str]:
    tag = sanitize_name(lib)
    files = {}

    ok = [r for r in compute_rows if r.get("lib_query")==lib and r.get("status")=="ok"]
    err = [r for r in compute_rows if r.get("lib_query")==lib and r.get("status")!="ok"]

    incompatible = sorted({r["node"] for r in ok if r.get("compatibility")=="incompatible"})
    missing = sorted({r["node"] for r in ok if r.get("compatibility")=="missing"})

    fn = f"{out_prefix}_compute_{tag}_incompatible.txt"
    with open(fn, "w", encoding="utf-8") as f:
        f.write("\n".join(incompatible) + ("\n" if incompatible else ""))
    files["incompatible"] = fn

    fn = f"{out_prefix}_compute_{tag}_missing.txt"
    with open(fn, "w", encoding="utf-8") as f:
        f.write("\n".join(missing) + ("\n" if missing else ""))
    files["missing"] = fn

    by_kind = defaultdict(list)
    for r in err:
        by_kind[r.get("ssh_error_kind","ssh_error")].append(r["node"])

    for kind, nodes in by_kind.items():
        fn = f"{out_prefix}_compute_{tag}_errors_{sanitize_name(kind)}.txt"
        with open(fn, "w", encoding="utf-8") as f:
            nodes = sorted(set(nodes))
            f.write("\n".join(nodes) + ("\n" if nodes else ""))
        files[f"errors_{kind}"] = fn

    return files

def build_report(
    ts: str,
    scope: str,
    scheduler: str,
    baseline_from: str,
    baseline_major: str,
    workers: int,
    retries: int,
    login_nodes: int,
    compute_nodes: int,
    scheduler_skipped_count: int,
    libs: List[str],
    login_rows: List[Dict],
    compute_rows: List[Dict],
    baselines: Dict[str, Set[int]],
    node_list_files: Dict[str, Dict[str,str]],
) -> str:
    lines = []
    lines.append(f"Library sweep report: {ts}")
    lines.append(f"Scope: {scope}")
    lines.append(f"Scheduler: {scheduler}")
    lines.append(f"Workers: {workers}   Retries: {retries}")
    lines.append(f"Baseline-from: {baseline_from}")
    lines.append(f"Baseline-major override: {baseline_major}")
    lines.append(f"Login nodes: {login_nodes}   Compute nodes selected: {compute_nodes}")
    lines.append(f"Scheduler skipped (down/offline/non-compute): {scheduler_skipped_count}")
    lines.append("")

    for lib in libs:
        lines.append(f"=== {lib} ===")
        l_ok = [r for r in login_rows if r.get("lib_query")==lib and r.get("status")=="ok"]
        c_ok = [r for r in compute_rows if r.get("lib_query")==lib and r.get("status")=="ok"]
        c_err = [r for r in compute_rows if r.get("lib_query")==lib and r.get("status")!="ok"]

        baseline = ",".join(str(m) for m in sorted(baselines.get(lib, set())))
        lines.append(f"Baseline majors required: {baseline if baseline else '(none)'}")

        if l_ok:
            tgt = Counter(r.get("primary_target","") for r in l_ok if r.get("primary_target"))
            maj = Counter(r.get("primary_major","") for r in l_ok if r.get("primary_major")!="")
            ver = Counter(r.get("primary_version","") for r in l_ok if r.get("primary_version"))
            lines.append(f"Login consensus file: {tgt.most_common(1)[0][0] if tgt else '(none)'}")
            lines.append("Login primary_major counts: " + ", ".join(f"{k}:{v}" for k,v in maj.most_common(6)))
            lines.append("Login primary_version counts: " + ", ".join(f"{k}:{v}" for k,v in ver.most_common(6)))
        else:
            lines.append("Login: (no data or login scope disabled)")

        lines.append("")

        present = [r for r in c_ok if r.get("present")=="True"]
        compat = [r for r in c_ok if r.get("compatibility")=="compatible"]
        incompat = [r for r in c_ok if r.get("compatibility")=="incompatible"]
        missing = [r for r in c_ok if r.get("compatibility")=="missing"]

        lines.append(f"Compute OK: {len(c_ok)}   Compute errors: {len(c_err)}")
        lines.append(f"  present: {len(present)}   compatible: {len(compat)}   incompatible: {len(incompat)}   missing: {len(missing)}")

        if incompat:
            by_class = Counter(r.get("node_class","") for r in incompat)
            by_majors = Counter(r.get("majors","") for r in incompat)
            lines.append("  Incompatible by node_class: " + ", ".join(f"{k}:{v}" for k,v in by_class.most_common()))
            lines.append("  Top incompatible majors_seen:")
            for k,v in by_majors.most_common(6):
                lines.append(f"    {v:>4} : {k}")

        if c_err:
            by_kind = Counter(r.get("ssh_error_kind","ssh_error") for r in c_err)
            lines.append("  Errors by ssh_error_kind: " + ", ".join(f"{k}:{v}" for k,v in by_kind.most_common()))
            lines.append(f"  Error sample: {sample([r['node'] for r in c_err])}")

        fdict = node_list_files.get(lib, {})
        if fdict:
            lines.append("  Node lists:")
            for k, fn in sorted(fdict.items()):
                lines.append(f"    {k}: {os.path.basename(fn)}")

        lines.append("")

    return "\n".join(lines) + "\n"
