from typing import Dict, List, Tuple

from sshfanout import run, short_hostname


def _split_features(features: str) -> List[str]:
    raw = (features or "").strip()
    if not raw or raw == "(null)":
        return []
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def _nodetype_from_features(features: str) -> str:
    toks = _split_features(features)
    return toks[0] if toks else ""


def state_is_online(state: str) -> bool:
    s = (state or "").strip().lower()
    blocked = (
        "down",
        "drain",
        "draining",
        "drained",
        "fail",
        "failing",
        "maint",
        "power_down",
        "powered_down",
        "powering_down",
        "no_resp",
        "unknown",
    )
    return not any(tok in s for tok in blocked)


def classify_node(host: str, nodetype: str) -> str:
    h = (host or "").lower()
    nt = (nodetype or "").lower()
    if h.startswith(("dtn", "dnt")) or "transfer" in nt:
        return "transfer"
    return nt if nt else "compute"


def slurm_inventory() -> Tuple[List[str], List[str], Dict[str, Dict[str, str]]]:
    try:
        p = run(["sinfo", "-N", "-h", "-o", "%N|%T|%P|%f"], timeout=120)
    except FileNotFoundError:
        return [], [], {}

    if p.returncode != 0:
        return [], [], {}

    inv: Dict[str, Dict[str, str]] = {}
    for line in p.stdout.splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = line.split("|", 3)
        if len(parts) != 4:
            continue
        node_raw, state_raw, partition_raw, features_raw = parts
        node = short_hostname(node_raw.strip())
        state = state_raw.strip().lower()
        partition = partition_raw.strip().replace("*", "")
        nodetype = _nodetype_from_features(features_raw)
        node_class = classify_node(node, nodetype)
        compute_flag = "0" if node_class == "transfer" else "1"

        inv[node] = {
            "state": state,
            "resources_available.nodetype": nodetype,
            "resources_available.compute": compute_flag,
            "scheduler.partition": partition,
            "scheduler.features": features_raw.strip(),
        }

    all_nodes = sorted(inv.keys())
    online_nodes = [n for n in all_nodes if state_is_online(inv.get(n, {}).get("state", ""))]
    return all_nodes, online_nodes, inv


def select_compute_nodes(inv: Dict[str, Dict[str, str]], *, online_only: bool, compute_flag_only: bool) -> Tuple[List[str], List[Tuple[str, str, str, str, str]]]:
    all_nodes = sorted(inv.keys())
    online_nodes = [n for n in all_nodes if state_is_online(inv.get(n, {}).get("state", ""))]
    candidates = online_nodes if online_only else all_nodes

    selected: List[str] = []
    skipped: List[Tuple[str, str, str, str, str]] = []  # node,reason,state,class,nodetype

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

    seen = set()
    out = []
    for n in selected:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out, skipped
