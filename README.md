# lib_sweep

A PBS-focused (for now) library inventory + compatibility sweep tool.

Now supports PBS and Slurm scheduler inventory.

## What it does
- Sweeps login nodes and/or compute nodes
- Finds dynamic library variants for a query like:
  - `libjpeg` (general inventory)
  - `libjpeg.so.62` (pins SONAME major 62 as required)
- Compares compute nodes against a baseline derived from login nodes (or a forced major)
- Produces:
  - `*_login.csv`
  - `*_compute.csv`
  - `*_report.txt`
  - `*_<scheduler>_skipped.txt` (down/offline/non-compute)
  - Optional discrepancy rundown files:
    - `*_rundown_discrepancies.csv`
    - `*_rundown_nodes.txt`
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
