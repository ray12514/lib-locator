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

## Install (no pip)
Place this project directory on a shared path visible to login/compute nodes.

Use the included `libsweep` launcher from this directory:

```bash
./libsweep --help
```

Optional: add it to your PATH as a command.

User-local install:

```bash
mkdir -p ~/.local/bin
ln -sf /shared/tools/lib_locator/libsweep ~/.local/bin/libsweep
```

System-wide install:

```bash
sudo ln -sf /shared/tools/lib_locator/libsweep /usr/local/bin/libsweep
```

Example:
```
/shared/tools/lib_locator/
  lib_sweep.py
  cli.py
  probe.py
  pbs.py
  sshfanout.py
  baseline.py
  report.py
```

## Example runs

### Dry run (no SSH)
```
/shared/tools/lib_locator/libsweep --lib libjpeg --scope all --login-auto --baseline-major 62 --dry-run
```

### Sweep all (login + compute), require SONAME major 62
```
/shared/tools/lib_locator/libsweep \
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
/shared/tools/lib_locator/libsweep \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --baseline-from none \
  --workers 32 \
  --out-prefix jpeg_inventory
```

### Slurm compute sweep
```
/shared/tools/lib_locator/libsweep \
  --scheduler slurm \
  --lib libjpeg \
  --scope compute \
  --workers 32 \
  --out-prefix jpeg_slurm
```

### Discrepancy-triggered standard-lib rundown
```
/shared/tools/lib_locator/libsweep \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --discrepancy-rundown \
  --discrepancy-rundown-workers 8 \
  --out-prefix jpeg_with_rundown
```

### Show usage examples
```
/shared/tools/lib_locator/libsweep --examples --lib libjpeg
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
