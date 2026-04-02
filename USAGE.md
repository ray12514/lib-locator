# libsweep Usage Guide

This guide covers all user-facing options and common workflows for `libsweep`.

## Quick start

```bash
libsweep --help
libsweep --examples
```

Without a pip install, run directly:

```bash
python3 -m libsweep --help
```

Typical first run (library sweep):

```bash
libsweep --lib libjpeg --scope all --login-auto --dry-run
```

Then run the real scan:

```bash
libsweep --lib libjpeg --scope all --login-auto --workers 32 --out-prefix libjpeg_scan
```

Typical first run (binary sweep):

```bash
libsweep --binary python3 --binary mpirun --scope all --login-auto --dry-run
```

## Modes

### Library sweep (`--lib`)

- `--lib libname` inventories available SONAME majors.
- `--lib libname.so.62` pins required major `62`.
- For compute nodes:
  - `consistent`: node contains all required SONAME majors
  - `inconsistent`: library present but missing one or more required SONAME majors
  - `missing`: library not present
  - `unreachable`: node probe failed (SSH/probe error), not counted as missing

Library baseline priority (highest first):

1. `--baseline-major`
2. pinned major in query (`libfoo.so.<N>`)
3. derived from login rows via `--baseline-from`

### Binary sweep (`--binary`)

- `--binary python3 --binary mpirun` checks that each named executable is present and at the same version on every compute node as on the login nodes.
- Version is detected by running `binary --version` with a 3-second timeout; the first line of stdout+stderr is recorded.
- For compute nodes:
  - `consistent`: binary present and version matches baseline
  - `inconsistent`: binary present but version differs from baseline
  - `missing`: binary not found in PATH
  - `unreachable`: node probe failed (SSH/probe error)

Binary baseline is the consensus (most common) version string observed across login nodes.

`--lib` and `--binary` are mutually exclusive in a single run.

## Scheduler behavior

Use `--scheduler` to control inventory source:

- `auto` (default): detects Slurm/PBS from environment or available commands
- `slurm`: uses `sinfo`
- `pbs`: uses `pbsnodes -a`

For your environment, Slurm nodetype is derived from Slurm features and GRES (with GRES used as fallback when `%f` is null), then mapped into the same `nodetype` field used by PBS logic.

## Common workflows

### Library sweep

Inventory only (no compatibility judgment):

```bash
libsweep \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --baseline-from none \
  --workers 32 \
  --out-prefix jpeg_inventory
```

Require a specific major from query:

```bash
libsweep --lib libjpeg.so.62 --scope all --login-auto --workers 32
```

Force baseline major explicitly:

```bash
libsweep --lib libjpeg --scope all --login-auto --baseline-major 62
```

Slurm compute-only sweep:

```bash
libsweep --scheduler slurm --lib libjpeg --scope compute --workers 32
```

Discrepancy-triggered follow-up rundown (representative flagged nodes only):

```bash
libsweep \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --discrepancy-rundown \
  --discrepancy-rundown-workers 8
```

PBS compute-only sweep:

```bash
libsweep --scheduler pbs --lib libjpeg --scope compute --workers 32
```

Multiple libraries in one run:

```bash
libsweep --lib libjpeg --lib libpng --lib libstdc++.so.6 --scope all --login-auto
```

### Binary sweep

Check that `python3` and `mpirun` are consistent across all compute nodes:

```bash
libsweep \
  --binary python3 \
  --binary mpirun \
  --scope all \
  --login-auto \
  --baseline-from login-consensus \
  --workers 32 \
  --out-prefix binaries_scan
```

Binary inventory only (no version baseline, just presence):

```bash
libsweep \
  --binary python3 \
  --scope all \
  --login-auto \
  --baseline-from none \
  --workers 32 \
  --out-prefix python3_inventory
```

Binary sweep with discrepancy rundown (full PATH manifest diff on one bad vs one good node):

```bash
libsweep \
  --binary python3 \
  --scope all \
  --login-auto \
  --discrepancy-rundown \
  --out-prefix python3_with_rundown
```

## Option reference

Core:

- `--lib`: repeatable library query (mutually exclusive with `--binary`)
- `--binary`: repeatable binary name to sweep (e.g. `python3`, `mpirun`; mutually exclusive with `--lib`)
- `--dirs`: extra directory globs to scan for libraries
- `--binary-dirs`: extra directories to search for binaries (supplements PATH)
- `--no-ldconfig`: skip `ldconfig -p` lookup (library sweep only)
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
- `--pbs-compute-flag-only` / `--no-pbs-compute-flag-only`: when enabled (default), excludes transfer-class nodes while keeping other online node classes (for example bigmem/visualization)

Baseline:

- `--baseline-from {login-consensus,login-union,login-intersection,none}`
- `--baseline-major <N>`: hard override for required SONAME major (library sweep only)

Execution and SSH:

- `--remote-python`: remote interpreter (default `python3`)
- `--workers`: parallel SSH fanout workers
- `--ssh-timeout`: per-node timeout (seconds)
- `--retries`: retry count for transient SSH failures
- `--discrepancy-rundown` / `--no-discrepancy-rundown`: on inconsistency/missing, run lightweight all-lib manifest comparison on representative flagged nodes
- `--discrepancy-rundown-workers`: worker pool size for follow-up rundown scan
- `--ssh-hostkey {accept-new,no,yes}`
- `--ssh-known-hosts`: override known_hosts path
- `--ssh-control-master` / `--no-ssh-control-master`: enable/disable OpenSSH connection sharing (default disabled)
- `--remote-low-priority` / `--no-remote-low-priority`: run remote probes with `nice -n 19` (default enabled)

Output and diagnostics:

- `--out-prefix`: output filename prefix
- `--dry-run`: show plan and example probe command; no remote probe execution
- `--write-node-lists`: write inconsistent/missing/error node list files per lib
- `--detail {concise,full}`: default concise output, full adds scheduler/debug fields
- `--write-json-summary`: write compact JSON summary report
- `--examples`: print curated examples and exit

Internal:

- `--probe`: internal per-node library probe mode used by SSH fanout
- `--probe-binary`: internal per-node binary probe mode used by SSH fanout
- `--probe-binary-rundown`: internal full PATH scan mode used by discrepancy rundown

## Output files

### Library sweep

`<prefix>_<timestamp>_login.csv`
- Login probe results and baseline derivation context.

`<prefix>_<timestamp>_compute.csv`
- Compute consistency results with concise columns by default (`node`, `node_type`, `lib_query`, `result`, `issue_detail`).

`<prefix>_<timestamp>_report.txt`
- Human-readable run summary and per-library rollup.

`<prefix>_<timestamp>_<scheduler>_skipped.txt`
- Nodes excluded as offline/down/non-compute.

Optional outputs:
- `*_compute_<lib>_inconsistent.txt`
- `*_compute_<lib>_missing.txt`
- `*_compute_<lib>_errors_<kind>.txt`
- `*_rundown_discrepancies.csv` (if `--discrepancy-rundown`)
- `*_rundown_nodes.txt` (if `--discrepancy-rundown`)
- `*_summary.json` (if `--write-json-summary`)

### Binary sweep

`<prefix>_<timestamp>_binary_login.csv`
- Login binary probe results (path, version_string per binary per login node).

`<prefix>_<timestamp>_binary_compute.csv`
- Compute binary consistency results (`node`, `node_type`, `binary_query`, `result`, `path`, `version_string`).

`<prefix>_<timestamp>_binary_report.txt`
- Human-readable binary sweep summary.

`<prefix>_<timestamp>_<scheduler>_skipped.txt`
- Same scheduler skipped file as library sweep.

Optional outputs:
- `*_binary_<name>_inconsistent.txt`
- `*_binary_<name>_missing.txt`
- `*_binary_<name>_errors_<kind>.txt`
- `*_rundown_discrepancies.csv` (if `--discrepancy-rundown`; PATH manifest diff between one bad and one reference node)
- `*_rundown_nodes.txt` (if `--discrepancy-rundown`)

## Exit codes (automation friendly)

- `0`: no unreachable nodes, no inconsistent/missing nodes
- `1`: one or more inconsistent and/or missing nodes found
- `2`: one or more unreachable (SSH/probe/internal) nodes found

## Troubleshooting

- `pbsnodes` or `sinfo` not found:
  - force scheduler via `--scheduler pbs` or `--scheduler slurm`
  - ensure scheduler CLI is available on the login host
- SSH issues:
  - start with `--dry-run`
  - increase `--ssh-timeout` and `--retries`
  - check `issue_detail` in concise mode or `error_kind`/`error_detail` in `--detail full`

## Safety notes

- Remote probing is read-only: it runs `ldconfig -p`, filesystem glob checks, and ELF metadata reads.
- No scheduler/job control actions are performed on compute nodes.
- By default, remote probes run with low CPU priority (`nice -n 19`) to minimize impact on running jobs.
- Unexpected baseline behavior:
  - use `--baseline-major` for strict enforcement
  - or `--baseline-from none` for inventory-only runs
