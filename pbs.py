from typing import Dict, List, Set, Tuple
from sshfanout import run, short_hostname

def parse_pbsnodes_a(output: str) -> Dict[str, Dict[str, str]]:
    inv: Dict[str, Dict[str, str]] = {}
    cur = None
    for line in output.splitlines():
        if not line.strip():
            continue
        if line and (not line[0].isspace()) and (":" not in line):
            cur = short_hostname(line.strip())
            inv.setdefault(cur, {})
            continue
        if cur is None:
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            inv[cur][k.strip()] = v.strip()
    return inv

def state_is_online(state: str) -> bool:
    # busy is online; only down/offline are excluded
    toks = {t.strip().lower() for t in (state or "").split(",") if t.strip()}
    return not ({"down", "offline"} & toks)

def nodetype_tokens(nodetype: str) -> List[str]:
    return [t.strip().lower() for t in (nodetype or "").split(",") if t.strip()]

def classify_node(host: str, nodetype: str) -> str:
    h = (host or "").lower()
    nt = (nodetype or "").lower()
    if h.startswith(("dtn", "dnt")) or "transfer" in nt:
        return "transfer"
    toks = nodetype_tokens(nodetype)
    return toks[0] if toks else "compute"

def pbs_inventory() -> Tuple[List[str], List[str], Dict[str, Dict[str, str]]]:
    try:
        p = run(["pbsnodes", "-a"], timeout=120)
    except FileNotFoundError:
        return [], [], {}
    if p.returncode != 0:
        return [], [], {}
    inv = parse_pbsnodes_a(p.stdout)
    for node, meta in inv.items():
        raw_nodetype = meta.get("resources_available.nodetype", "")
        if not (raw_nodetype or "").strip():
            meta["resources_available.nodetype"] = classify_node(node, raw_nodetype)
    all_nodes = list(inv.keys())
    online = [n for n in all_nodes if state_is_online(inv.get(n, {}).get("state",""))]
    return all_nodes, online, inv

def select_compute_nodes(inv: Dict[str, Dict[str, str]], *, online_only: bool, compute_flag_only: bool) -> Tuple[List[str], List[Tuple[str,str,str,str,str]]]:
    all_nodes = sorted(inv.keys())
    online_nodes = [n for n in all_nodes if state_is_online(inv.get(n, {}).get("state",""))]
    candidates = online_nodes if online_only else all_nodes

    selected: List[str] = []
    skipped: List[Tuple[str,str,str,str,str]] = []  # node,reason,state,class,nodetype

    for n in candidates:
        meta = inv.get(n, {})
        st = meta.get("state", "")
        nodetype = meta.get("resources_available.nodetype", "")
        nclass = classify_node(n, nodetype)
        compute_flag = meta.get("resources_available.compute", "").strip()

        if compute_flag_only:
            if compute_flag == "1":
                selected.append(n)
            else:
                skipped.append((n, "non_compute", st, nclass, nodetype))
        else:
            selected.append(n)

    if online_only:
        offline = sorted(set(all_nodes) - set(online_nodes))
        for n in offline:
            meta = inv.get(n, {})
            st = meta.get("state", "")
            nodetype = meta.get("resources_available.nodetype", "")
            nclass = classify_node(n, nodetype)
            skipped.append((n, "offline_or_down", st, nclass, nodetype))

    # de-dupe preserve order
    seen = set()
    out = []
    for n in selected:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out, skipped
