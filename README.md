# lib_sweep

A PBS-focused (for now) library inventory + compatibility sweep tool.

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
  - `*_pbs_skipped.txt` (down/offline/non-compute)
  - Optional node lists (incompatible/missing/errors by kind)

## Install (no pip)
Place this project directory on a shared path visible to login/compute nodes and run `lib_sweep.py` from that directory.

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
python3 /shared/tools/lib_sweep.py --lib libjpeg --scope all --login-auto --baseline-major 62 --dry-run
```

### Sweep all (login + compute), require SONAME major 62
```
python3 /shared/tools/lib_sweep.py \
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
python3 /shared/tools/lib_sweep.py \
  --lib libjpeg \
  --scope all \
  --login-auto \
  --baseline-from none \
  --workers 32 \
  --out-prefix jpeg_inventory
```

## Notes
- Host key prompts are avoided by default using:
  - `StrictHostKeyChecking=accept-new`
  - `UserKnownHostsFile=~/.cache/lib_sweep/known_hosts`
- If your OpenSSH doesn't support `accept-new`, run with:
  - `--ssh-hostkey no`

## Tests
Run the lightweight unit tests:

```
python3 -m unittest discover -s tests -v
```
