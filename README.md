# pyHDDSectorIsolation

Windows desktop utility (PySide6) for stress-writing disk space in fixed-size chunks, then reading it back to measure performance and isolate bad areas.

The new version is split into two standalone GUI applications:

| Module | Purpose |
| --- | --- |
| `module1.py` — **Sector Stage Runner** | Writes every chunk, then reads every non-ghost chunk and verifies its SHA-256 hash. Produces `stage1.json` and `stage2.json` next to the app. |
| `module2.py` — **Sector Performance Analyzer** | Loads the two JSON files from the app folder, shows write/read maps, classifies files against configurable thresholds, and renames BAD files on the target drive. |

There is no CLI, no session/resume logic, and no `session_state.json`.

## Important warning

This tool performs real write operations and can consume most of the free space on the selected target path. Use it only on disks/folders you intentionally want to test.

Module 1 does **not** clean `<target>\sectors` before starting, and it does not rename GOOD/BAD files. Remove old test files yourself before a fresh run if you need an accurate free-space calculation.

`stage1.json` and `stage2.json` are written to the **app folder** (the folder containing the exes/scripts), not to the target drive. The app folder must be writable; otherwise Module 1 shows an error and does not start. Old JSON files left in `<target>\sectors` by earlier versions are not read or removed.

## Requirements

- Windows
- Python 3.x
- Dependencies from `requirements.txt`:
  - `PySide6`
  - `pywin32`
  - `ag95[LoggingScripts]`

## Quick start

### Option A: Use the provided installer script

```powershell
powershell -ExecutionPolicy Bypass -File .\_install\install.ps1
```

This creates `venv`, sets up `activate.bat` / `activate.ps1`, copies `pip.ini`, installs `pip` / `uv`, and installs the requirements.

### Option B: Manual setup

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

## Run from source

```powershell
.\activate.ps1
python .\module1.py
```

When the first module finishes, close it and start the analyzer:

```powershell
python .\module2.py
```

When running from source, the app folder is the folder containing `module1.py` / `module2.py` (the repository folder).

## Module 1 — Sector Stage Runner

1. Select the target folder/disk.
2. Set the chunk size in MB (default `100`).
3. Click **Start**.
4. Optional: use **Pause** / **Resume**. Pause takes effect after the current chunk finishes.
5. Wait for both stages to complete. Closing the window is blocked while a stage is running or paused.

### Stage 1 — write

- The stage JSON files are saved in the app folder, not on the target drive. They are overwritten on every run, so copy them if you need to keep a run's results.
- Calculates the chunk count from free space:
  `max(1, int(free * 0.995) // chunk_size_bytes)`.
- Writes each chunk to `<target>\sectors\<20-digit index>.dat` using write-through I/O in 1 MB blocks, then flushes and fsyncs.
- `write_time_s` is measured from the first write to flush end.
- `write_speed_mbps = chunk_size_bytes / write_time_s / (1024 * 1024)`.
- If a write fails, the chunk is a **ghost**:
  - module 1 stops writing that chunk,
  - waits indefinitely for the drive to answer a small probe write,
  - allocates the full chunk file with `SetEndOfFile` (no payload write),
  - records `write_time_s = -1`, `write_speed_mbps = -1`, `ghost_status = 1`,
  - continues with the next chunk.
- There is no speed threshold and no write timeout.

`<app folder>\stage1.json`:

```json
{
  "entries": [
    {
      "file": "00000000000000000001.dat",
      "write_time_s": 0.031,
      "write_speed_mbps": 1282.1,
      "ghost_status": 0
    },
    {
      "file": "00000000000000000002.dat",
      "write_time_s": -1,
      "write_speed_mbps": -1,
      "ghost_status": 1
    }
  ]
}
```

### Stage 2 — read and verify

- Ghost entries are skipped but still appear in `stage2.json`.
- Non-ghost files are opened with `FILE_FLAG_NO_BUFFERING`, read in 1 MB blocks, and hashed.
- `read_time_s` is measured from the first read to the last byte.
- `read_speed_mbps = chunk_size_bytes / read_time_s / (1024 * 1024)`.
- `hash_ok` is `true` when the SHA-256 matches the deterministic payload, `false` on mismatch or read failure, and `null` for skipped ghosts.
- A read exception records `read_time_s = -1`, `read_speed_mbps = -1`, `hash_ok = false` and processing continues.

`<app folder>\stage2.json`:

```json
{
  "entries": [
    {
      "file": "00000000000000000001.dat",
      "read_time_s": 0.0289,
      "read_speed_mbps": 1430.5,
      "ghost_status": 0,
      "hash_ok": true
    },
    {
      "file": "00000000000000000002.dat",
      "read_time_s": -1,
      "read_speed_mbps": -1,
      "ghost_status": 1,
      "hash_ok": null
    }
  ]
}
```

## Module 2 — Sector Performance Analyzer

1. Select the target folder/disk containing the `sectors` folder. This path is used only for renaming BAD files.
2. Click **Open**. The app loads `stage1.json` and `stage2.json` from the app folder. Both must exist, and the target path must be a valid folder; otherwise an error is shown and the analysis controls stay disabled.
3. Switch between **Write** and **Read** views.
4. Hover any square to see its file, write/read values, hash result, and ghost status.
5. Adjust thresholds/checkboxes; counts and the BAD list update automatically.
6. Click **Confirm** to rename BAD files, then the app exits.

The JSON files do not record which target drive produced them, so make sure the selected target is the same one used by Module 1. Otherwise Module 2 will look for the BAD files on the wrong drive.

### Map colors

- **Write view**: non-ghost squares are colored by `write_speed_mbps`.
- **Read view**: non-ghost squares are colored by `read_speed_mbps`.
- Lowest speed → red `(220, 50, 50)`, highest speed → green `(50, 200, 50)`, linear interpolation in between.
- If all valid speeds are equal (range 0), all valid non-ghost squares are green.
- Ghosts → violet `(150, 50, 200)`.
- Non-ghosts without usable data → neutral gray `(220, 220, 220)`.

### Classification

Defaults:

- Min Write Speed: `1.0 MB/s`
- Max Write Time: `30 s`
- Min Read Speed: `1.0 MB/s`
- Max Read Time: `30 s`
- **Ghost Files → BAD**: checked
- **Read Failures → BAD**: checked
- **Hash Mismatches → BAD**: checked

Rules:

- A ghost is BAD only when **Ghost Files → BAD** is checked. Thresholds are never applied to ghosts.
- A non-ghost read failure (`read_time_s = -1`) is BAD only when **Read Failures → BAD** is checked.
- A completed read with `hash_ok = false` is BAD only when **Hash Mismatches → BAD** is checked.
- Write thresholds apply when write data is available.
- Read thresholds apply when read data is available.
- Comparisons are strict: speed below the minimum or time above the maximum is BAD; equality is GOOD.
- BAD includes ghosts when the ghost checkbox is on, so `Total = GOOD + BAD`.

The counts line shows:

```text
Total: N | GOOD: G | BAD: B | Ghosts: X
```

### Rename behavior

BAD files are renamed in `<target>\sectors`:

```text
BAD_00000000000000000005.dat
BAD_00000000000000000005_2.dat
BAD_00000000000000000005_3.dat
```

- The app renames what it can and reports `Renamed N of M files`.
- If a file is missing or locked, failures are listed and the window stays open so the rename can be retried.
- If there are no BAD files, it shows `Nothing to BAD rename` and exits.
- On complete success, it shows the summary and exits.
- Files that were already renamed on a previous run are recognized and not double-prefixed.

## Build executables (PyInstaller)

```powershell
.\BUILD_release.bat
```

This builds one shared onedir bundle:

```text
dist\pyHDDSectorIsolation\
  module1.exe
  module2.exe
  _internal\
```

Both executables are side by side and share the same `_internal` folder. Keep the folder together when copying or moving the applications. The build script removes the old `build` and `dist` folders first.

Build config is in `pyHDDSectorIsolation.spec`.

## Project entry points

- `module1.py` — Sector Stage Runner window, write/read worker, JSON output in the app folder
- `module2.py` — Sector Performance Analyzer window, grid, thresholds, app-folder JSON loading, target-drive rename logic
