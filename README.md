# lib_sweep

A PBS-focused (for now) library inventory + compatibility sweep tool.

Now supports PBS and Slurm scheduler inventory.

## What it does

**Library sweep** — find dynamic library (`.so`) variants across the cluster:
- `--lib libjpeg` — general inventory of SONAME majors
- `--lib libjpeg.so.62` — require SONAME major 62
- Classifies each compute node as `consistent`, `inconsistent`, `missing`, or `unreachable`

**Binary sweep** — locate executables and check version consistency:
- `--binary python3 --binary mpirun` — check that named binaries are present and at the same version on every compute node as on login nodes
- Reports path, version string, and consistent/inconsistent/missing per node

Both modes produce:
- `*_login.csv` / `*_compute.csv`
- `*_report.txt`
- `*_<scheduler>_skipped.txt` (down/offline/non-compute)
- Optional discrepancy rundown: compares one flagged compute node against one good reference node (`*_rundown_discrepancies.csv`, `*_rundown_nodes.txt`)
- Optional node lists (inconsistent/missing/errors by kind)

For full documentation, see `USAGE.md`.

## Install

**pip install (recommended):**
```bash
pip install /shared/tools/lib_locator
# or editable/dev mode:
pip install -e /shared/tools/lib_locator
```

After install, the `libsweep` command is available directly:
```bash
libsweep --help
```

**Without installing** (run from the project directory):
```bash
python3 -m libsweep --help
```

For shared HPC clusters, install once on the shared filesystem so all nodes can reach it:
```bash
pip install --prefix /shared/tools/lib_locator_install /shared/tools/lib_locator
```
Then ensure `/shared/tools/lib_locator_install/lib/pythonX.Y/site-packages` is on `PYTHONPATH` on all nodes.

## Example runs

### Dry run (no SSH)
```
libsweep --lib libjpeg --scope all --login-auto --baseline-major 62 --dry-run
```

### Sweep all (login + compute), require SONAME major 62
```
libsweep \
  --lib libjpeg.so.62 \
  --scope all \
  --login-auto \
  --baseline-from login-consensus \
  --workers 32 \
  --retries 2 \
  --write-node-lists \
  --out-prefix jpeg_req62
```

### Inventory only (no compatibility judgement)
```
libsweep \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --baseline-from none \
  --workers 32 \
  --out-prefix jpeg_inventory
```

### Slurm compute sweep
```
libsweep \
  --scheduler slurm \
  --lib libjpeg \
  --scope compute \
  --workers 32 \
  --out-prefix jpeg_slurm
```

### Discrepancy-triggered standard-lib rundown
```
libsweep \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --discrepancy-rundown \
  --discrepancy-rundown-workers 8 \
  --out-prefix jpeg_with_rundown
```

### Binary sweep — check python3 and mpirun on all compute nodes
```
libsweep \
  --binary python3 \
  --binary mpirun \
  --scope all \
  --login-auto \
  --baseline-from login-consensus \
  --workers 32 \
  --out-prefix binaries_scan
```

### Binary sweep dry run
```
libsweep --binary python3 --scope all --login-auto --dry-run
```

### Show usage examples
```
libsweep --examples --lib libjpeg
```

## Notes
- Host key prompts are avoided by default using:
  - `StrictHostKeyChecking=accept-new`
  - `UserKnownHostsFile=~/.cache/lib_sweep/known_hosts`
- If your OpenSSH doesn't support `accept-new`, run with:
  - `--ssh-hostkey no`
- Use `--scheduler auto` (default) to detect Slurm/PBS automatically.

## Exit codes
- `0`: no unreachable nodes, no inconsistent/missing nodes
- `1`: one or more inconsistent and/or missing nodes found
- `2`: one or more probe/SSH/internal errors

## Tests
Run the lightweight unit tests:

```
python3 -m unittest discover -s tests -v
```
