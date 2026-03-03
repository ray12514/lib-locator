import os
import re
from typing import Dict, List, Optional, Set, Tuple

DEFAULT_DIR_GLOBS = ["/lib*", "/usr/lib*", "/usr/local/lib*", "/opt/*/lib*"]

def normalize_root_and_prefix(lib_query: str) -> Tuple[str, str, Optional[int]]:
    q = lib_query.strip()
    base = q if q.startswith("lib") else ("lib" + q)
    pinned_major = None

    if ".so." in base:
        left, right = base.split(".so.", 1)
        m = re.match(r"(\d+)", right)
        if m:
            pinned_major = int(m.group(1))
            prefix = f"{left}.so.{m.group(1)}"
        else:
            prefix = f"{left}.so"
        return left, prefix, pinned_major

    if base.endswith(".so"):
        return base[:-3], base, None

    return base, base + ".so", None

def major_from_text(s: str) -> Optional[int]:
    m = re.search(r"\.so\.(\d+)", s)
    return int(m.group(1)) if m else None

def version_suffix(basename: str) -> str:
    m = re.search(r"\.so\.(\d+(?:\.\d+)*)$", basename)
    return m.group(1) if m else ""

def pinned_major_from_query(lib_query: str) -> Optional[int]:
    m = re.search(r"\.so\.(\d+)", lib_query)
    return int(m.group(1)) if m else None

def probe_node(lib_query: str, extra_dirs: List[str], no_ldconfig: bool) -> Dict:
    import glob
    import subprocess

    root, prefix, pinned_major = normalize_root_and_prefix(lib_query)
    dirs = DEFAULT_DIR_GLOBS + extra_dirs

    def safe_run(cmd: List[str]):
        try:
            p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=False)
            return p.returncode, p.stdout, p.stderr
        except FileNotFoundError:
            return 127, "", "not found"

    def ldconfig_paths():
        if no_ldconfig:
            return []
        rc, out, _ = safe_run(["ldconfig", "-p"])
        if rc != 0 or not out:
            return []
        paths = []
        for line in out.splitlines():
            line = line.strip()
            if "=>" not in line:
                continue
            left, right = line.split("=>", 1)
            libname = left.split()[0].strip()
            if libname.startswith(root + ".so"):
                paths.append(right.strip().split()[0])
        return sorted(set(paths))

    def fs_paths():
        found = set()
        for dglob in dirs:
            for base in glob.glob(dglob):
                pat = os.path.join(base.rstrip("/"), prefix + "*")
                for f in glob.glob(pat):
                    if os.path.exists(f):
                        found.add(f)
        return sorted(found)

    soname_cache: Dict[str, str] = {}

    def soname_of(path: str) -> str:
        if path in soname_cache:
            return soname_cache[path]
        rc, out, _ = safe_run(["readelf", "-d", path])
        if rc == 0 and out:
            for line in out.splitlines():
                if "SONAME" in line:
                    m = re.search(r"\[(.*?)\]", line)
                    if m:
                        soname_cache[path] = m.group(1)
                        return soname_cache[path]
        rc, out, _ = safe_run(["objdump", "-p", path])
        if rc == 0 and out:
            for line in out.splitlines():
                if line.strip().startswith("SONAME"):
                    parts = line.split()
                    if len(parts) >= 2:
                        soname_cache[path] = parts[1]
                        return soname_cache[path]
        soname_cache[path] = ""
        return soname_cache[path]

    candidates = []
    seen = set()
    for p in ldconfig_paths():
        if p not in seen:
            seen.add(p)
            candidates.append(p)
    for p in fs_paths():
        if p not in seen:
            seen.add(p)
            candidates.append(p)

    majors: Set[int] = set()
    versions: Set[str] = set()

    primary_path = candidates[0] if candidates else ""
    primary_target = os.path.realpath(primary_path) if primary_path else ""
    primary_base = os.path.basename(primary_target) if primary_target else ""
    primary_soname = soname_of(primary_target) if (primary_target and os.path.isfile(primary_target)) else ""
    primary_major = major_from_text(primary_soname) or major_from_text(primary_base) or (major_from_text(os.path.basename(primary_path)) if primary_path else None)
    primary_version = version_suffix(primary_base) if primary_base else ""

    for p in candidates:
        tgt = os.path.realpath(p)
        tbase = os.path.basename(tgt)
        v = version_suffix(tbase)
        if v:
            versions.add(v)
        son = soname_of(tgt) if os.path.isfile(tgt) else ""
        maj = major_from_text(son) or major_from_text(tbase) or major_from_text(os.path.basename(p))
        if maj is not None:
            majors.add(maj)

    return {
        "node": os.uname().nodename.split(".",1)[0],
        "query": lib_query,
        "pinned_major": pinned_major,
        "present": bool(candidates),
        "primary_target": primary_base,
        "primary_soname": primary_soname,
        "primary_major": primary_major,
        "primary_version": primary_version,
        "majors": sorted(majors),
        "versions": sorted(versions),
        "variants_count": len(candidates),
    }
