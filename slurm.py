import re
from typing import Dict, List, Set, Tuple

from sshfanout import run, short_hostname


def _split_features(features: str) -> List[str]:
    raw = (features or "").strip()
    if not raw or raw == "(null)":
        return []
    return [t.strip().lower() for t in raw.split(",") if t.strip()]


def _tokenize(raw: str) -> List[str]:
    return [t for t in re.split(r"[^a-z0-9]+", (raw or "").lower()) if t]


def _nodetype_from_features(features: str) -> str:
    toks = _split_features(features)
    return toks[0] if toks else ""


def _gres_tokens(gres: str) -> List[str]:
    raw = (gres or "").strip().lower()
    if not raw or raw in {"(null)", "null", "n/a"}:
        return []
    toks = [t for t in _tokenize(raw) if t and not t.isdigit() and t not in {"null", "n", "a"}]
    return toks


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


def resolve_node_type(host: str, nodetype: str, partition: str = "", gres: str = "") -> str:
    host_toks = set(_tokenize(host))
    host_transfer = any(t.startswith(("dtn", "dnt")) for t in host_toks)
    nodetype_toks = set(_tokenize(nodetype))
    partition_toks = set(_tokenize(partition))
    gres_toks_list = _gres_tokens(gres)
    gres_toks = set(gres_toks_list)
    combined = nodetype_toks | partition_toks | gres_toks

    if host_transfer or ({"transfer", "xfer", "dtn", "dnt", "datatransfer"} & (combined | host_toks)):
        return "transfer"
    if {"visualization", "visual", "viz", "vis", "srd"} & combined:
        return "visualization"
    if {"bigmem", "highmem", "hmem", "largemem", "lm"} & combined:
        return "bigmem"

    if nodetype_toks:
        return _tokenize(nodetype)[0]
    if gres_toks_list:
        return gres_toks_list[0]
    return "compute"


def classify_node(host: str, nodetype: str, partition: str = "", gres: str = "") -> str:
    node_type = resolve_node_type(host, nodetype, partition, gres)
    if node_type in {"transfer", "visualization", "bigmem"}:
        return node_type
    return "compute"


def slurm_inventory() -> Tuple[List[str], List[str], Dict[str, Dict[str, str]]]:
    fmt_with_gres = "%N|%T|%P|%f|%G"
    fmt_legacy = "%N|%T|%P|%f"
    try:
        p = run(["sinfo", "-N", "-h", "-o", fmt_with_gres], timeout=120)
    except FileNotFoundError:
        return [], [], {}

    has_gres = True
    if p.returncode != 0:
        p = run(["sinfo", "-N", "-h", "-o", fmt_legacy], timeout=120)
        has_gres = False
        if p.returncode != 0:
            return [], [], {}

    inv: Dict[str, Dict[str, str]] = {}
    state_by_node: Dict[str, Set[str]] = {}
    partition_by_node: Dict[str, Set[str]] = {}
    features_by_node: Dict[str, Set[str]] = {}
    gres_by_node: Dict[str, Set[str]] = {}
    class_by_node: Dict[str, str] = {}
    nodetype_by_node: Dict[str, str] = {}
    for line in p.stdout.splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        if has_gres:
            parts = line.split("|", 4)
            if len(parts) != 5:
                continue
            node_raw, state_raw, partition_raw, features_raw, gres_raw = parts
        else:
            parts = line.split("|", 3)
            if len(parts) != 4:
                continue
            node_raw, state_raw, partition_raw, features_raw = parts
            gres_raw = ""
        node = short_hostname(node_raw.strip())
        state = state_raw.strip().lower()
        partition = partition_raw.strip().replace("*", "")
        nodetype = _nodetype_from_features(features_raw)
        node_type = resolve_node_type(node, nodetype, partition, gres_raw)
        node_class = classify_node(node, nodetype, partition, gres_raw)
        if not nodetype:
            nodetype = node_type

        state_by_node.setdefault(node, set()).add(state)
        if partition:
            partition_by_node.setdefault(node, set()).add(partition)
        feature = features_raw.strip()
        if feature and feature != "(null)":
            features_by_node.setdefault(node, set()).add(feature)
        gres = gres_raw.strip()
        if gres and gres != "(null)":
            gres_by_node.setdefault(node, set()).add(gres)

        prev_class = class_by_node.get(node, "")
        if prev_class == "transfer" or node_class == "transfer":
            class_by_node[node] = "transfer"
        elif prev_class == "bigmem" or node_class == "bigmem":
            class_by_node[node] = "bigmem"
        elif prev_class == "visualization" or node_class == "visualization":
            class_by_node[node] = "visualization"
        elif not prev_class:
            class_by_node[node] = node_class

        if node not in nodetype_by_node or nodetype_by_node[node] in ("", "compute"):
            nodetype_by_node[node] = nodetype

    for node in sorted(state_by_node.keys()):
        node_class = class_by_node.get(node, "compute")
        nodetype = nodetype_by_node.get(node, "") or node_class
        compute_flag = "0" if node_class in ("transfer", "visualization") else "1"
        states = sorted(state_by_node.get(node, set()))
        partitions = sorted(partition_by_node.get(node, set()))
        features = sorted(features_by_node.get(node, set()))
        gresses = sorted(gres_by_node.get(node, set()))

        inv[node] = {
            "state": ",".join(states),
            "resources_available.nodetype": nodetype,
            "resources_available.compute": compute_flag,
            "scheduler.partition": ",".join(partitions),
            "scheduler.features": ",".join(features),
            "scheduler.gres": ",".join(gresses),
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
        partition = meta.get("scheduler.partition", "")
        gres = meta.get("scheduler.gres", "")
        nclass = classify_node(n, nodetype, partition, gres)
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
            partition = meta.get("scheduler.partition", "")
            gres = meta.get("scheduler.gres", "")
            nclass = classify_node(n, nodetype, partition, gres)
            skipped.append((n, "offline_or_down", st, nclass, nodetype))

    seen = set()
    out = []
    for n in selected:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out, skipped
