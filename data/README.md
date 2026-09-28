# Data

- `archive/ribs_cpu.csv.gz` is a preserved historical CPU output snapshot. It
  predates the correctness fixes and is not a validated expected result or
  performance baseline. Keep it compressed; restore with
  `gzip -dk data/archive/ribs_cpu.csv.gz` from the repository root if needed.
- `generated/` holds local input datasets produced by the generator scripts.
  These files are ignored by Git. Previously generated root `rel.txt` and
  `ann.txt` were moved here during the layout cleanup.
- `bench/` holds the existing local benchmark datasets, moved from the former
  root `bench/` directory and still ignored by Git.

Small, versioned correctness fixtures live in `tests/fixtures/`. New simulation
output belongs in `outputs/`; the pre-cleanup CSV results are preserved locally
under `outputs/previous/`.

The uncompressed historical archive has SHA-256:

```text
22f5999e59f9697ab5b877851da13f90013edf2ae7ab43821f8f2f01ce329540
```
