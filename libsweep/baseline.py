from collections import Counter
from typing import Dict, List, Optional, Set
from .probe import pinned_major_from_query

def majors_set_from_row(row: Dict) -> Set[int]:
    s = (row.get("majors") or "").strip()
    out: Set[int] = set()
    if not s:
        return out
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.add(int(part))
        except ValueError:
            pass
    return out

def compute_baseline_majors(
    lib_query: str,
    login_ok_rows: List[Dict],
    baseline_from: str,
    baseline_major_override: Optional[int],
) -> Set[int]:
    if baseline_major_override is not None:
        return {baseline_major_override}

    pinned = pinned_major_from_query(lib_query)
    if pinned is not None:
        return {pinned}

    if baseline_from == "none" or not login_ok_rows:
        return set()

    sets = []
    union: Set[int] = set()
    primary_majors = []
    for r in login_ok_rows:
        ms = majors_set_from_row(r)
        if ms:
            sets.append(ms)
            union |= ms
        pm = (r.get("primary_major") or "").strip()
        if pm:
            try:
                primary_majors.append(int(pm))
            except ValueError:
                pass

    if baseline_from == "login-consensus":
        if primary_majors:
            return {Counter(primary_majors).most_common(1)[0][0]}
        return union

    if baseline_from == "login-union":
        return union

    if baseline_from == "login-intersection":
        if not sets:
            return set()
        inter = sets[0].copy()
        for s in sets[1:]:
            inter &= s
        return inter

    return set()
