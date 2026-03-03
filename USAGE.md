# lib_sweep Usage Guide

This guide covers all user-facing options and common workflows for `lib_sweep.py`.

## Quick start

Run from the project directory (or use full path to `lib_sweep.py`):

```bash
python3 lib_sweep.py --help
python3 lib_sweep.py --examples
```

Typical first run:

```bash
python3 lib_sweep.py --lib libjpeg --scope all --login-auto --dry-run
```

Then run the real scan:

```bash
python3 lib_sweep.py --lib libjpeg --scope all --login-auto --workers 32 --out-prefix libjpeg_scan
```

## How compatibility works

- `--lib libname` (for example `libjpeg`) inventories available SONAME majors.
- `--lib libname.so.62` pins required major `62`.
- For compute nodes:
  - `missing`: library not present
  - `compatible`: node contains all required baseline majors
  - `incompatible`: library present but missing one or more baseline majors
  - `n/a`: no baseline requirement (for example `--baseline-from none`)

Baseline priority (highest first):

1. `--baseline-major`
2. pinned major in query (`libfoo.so.<N>`)
3. derived from login rows via `--baseline-from`

## Scheduler behavior

Use `--scheduler` to control inventory source:

- `auto` (default): detects Slurm/PBS from environment or available commands
- `slurm`: uses `sinfo`
- `pbs`: uses `pbsnodes -a`

For your environment, Slurm nodetype is read from Slurm node features and mapped into the same `nodetype` field used by PBS logic.

## Common workflows

Inventory only (no compatibility judgment):

```bash
python3 lib_sweep.py \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --baseline-from none \
  --workers 32 \
  --out-prefix jpeg_inventory
```

Require a specific major from query:

```bash
python3 lib_sweep.py --lib libjpeg.so.62 --scope all --login-auto --workers 32
```

Force baseline major explicitly:

```bash
python3 lib_sweep.py --lib libjpeg --scope all --login-auto --baseline-major 62
```

Slurm compute-only sweep:

```bash
python3 lib_sweep.py --scheduler slurm --lib libjpeg --scope compute --workers 32
```

PBS compute-only sweep:

```bash
python3 lib_sweep.py --scheduler pbs --lib libjpeg --scope compute --workers 32
```

Multiple libraries in one run:

```bash
python3 lib_sweep.py --lib libjpeg --lib libpng --lib libstdc++.so.6 --scope all --login-auto
```

## Option reference

Core:

- `--lib`: repeatable library query (required unless `--examples`)
- `--dirs`: extra directory globs to scan in addition to defaults
- `--no-ldconfig`: skip `ldconfig -p` lookup
- `--scope {login,compute,all}`: default is `all` (or `compute` inside scheduler jobs)
- `--scheduler {auto,pbs,slurm}`: scheduler inventory backend

Login discovery:

- `--login-auto`: discover login nodes by prefix+number pattern
- `--login-prefix`: override inferred prefix
- `--login-width`: override numeric width
- `--login-max`: max login hosts to probe
- `--login-stop-after-gap`: stop scan after this many misses beyond last hit

Compute node selection:

- `--pbs-online-only` / `--no-pbs-online-only`: include only online nodes (default enabled)
- `--pbs-compute-flag-only` / `--no-pbs-compute-flag-only`: include only nodes marked compute-eligible (default enabled)

Baseline:

- `--baseline-from {login-consensus,login-union,login-intersection,none}`
- `--baseline-major <N>`: hard override for required major

Execution and SSH:

- `--remote-python`: remote interpreter (default `python3`)
- `--workers`: parallel SSH fanout workers
- `--ssh-timeout`: per-node timeout (seconds)
- `--retries`: retry count for transient SSH failures
- `--ssh-hostkey {accept-new,no,yes}`
- `--ssh-known-hosts`: override known_hosts path
- `--ssh-control-master`: enable OpenSSH connection sharing

Output and diagnostics:

- `--out-prefix`: output filename prefix
- `--dry-run`: show plan and example probe command; no remote probe execution
- `--write-node-lists`: write incompatible/missing/error node list files per lib
- `--verbose-csv`: include extra columns (`versions`, `variants_count`)
- `--write-json-summary`: write compact JSON summary report
- `--examples`: print curated examples and exit

Internal:

- `--probe`: internal per-node probe mode used by SSH fanout

## Output files

`<prefix>_<timestamp>_login.csv`
- Login probe results and baseline derivation context.

`<prefix>_<timestamp>_compute.csv`
- Compute compatibility results, scheduler metadata, and SSH status.

`<prefix>_<timestamp>_report.txt`
- Human-readable run summary and per-library rollup.

`<prefix>_<timestamp>_<scheduler>_skipped.txt`
- Nodes excluded as offline/down/non-compute.

Optional outputs:

- `*_compute_<lib>_incompatible.txt`
- `*_compute_<lib>_missing.txt`
- `*_compute_<lib>_errors_<kind>.txt`
- `*_summary.json` (if `--write-json-summary`)

## Exit codes (automation friendly)

- `0`: no compute errors, no incompatible/missing nodes
- `1`: incompatibilities and/or missing libraries found
- `2`: probe/SSH/internal errors found

## Troubleshooting

- `pbsnodes` or `sinfo` not found:
  - force scheduler via `--scheduler pbs` or `--scheduler slurm`
  - ensure scheduler CLI is available on the login host
- SSH issues:
  - start with `--dry-run`
  - increase `--ssh-timeout` and `--retries`
  - check `ssh_error_kind` and `ssh_error_detail` columns in compute CSV
- Unexpected baseline behavior:
  - use `--baseline-major` for strict enforcement
  - or `--baseline-from none` for inventory-only runs
