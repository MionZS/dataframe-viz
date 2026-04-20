# Daily & Monthly Pipeline Optimization

## Overview

The pipeline is now optimized for **daily operation with monthly data aggregation**. Two modes are available:

### 1. **Daily Mode** (lightweight, fast)
- Runs **simplified pipeline only** (no moving window)
- Executes `daily_simple.bat` every day
- Checks which months need computation
- Computes only missing months (incremental)
- Concatenates all results into single CSV

### 2. **Full Mode** (comprehensive, slower)
- Runs **both simplified + full pipelines**
- Executes `generate_all_months.bat` periodically (weekly or as needed)
- Produces detailed moving-window analytics
- Used for full historical analysis

---

## How It Works

### Automatic Month Detection

**src/month_checker.py** analyzes:
- **Available data**: Counts Diario files by month; marks complete when all days (28/29/30/31) exist
- **Computed months**: Scans `municipio_daily/` output directory for already-generated files
- **Missing months**: Calculates difference (available - computed)

Example output:
```
Available data (Diario):
  Months: ['2026-01', '2026-02', '2026-03']

Pipeline 'simple':
  Computed:   ['2026-01', '2026-02']
  To compute: ['2026-03']
  Status: Need to compute 1 month(s)
```

### Daily Orchestrator

**src/daily_orchestrator.py** executes the plan:
1. **Check availability** → calls month_checker
2. **Plan computation** → determines which months to run
3. **Run pipelines** → executes only missing months (parallelizable)
4. **Concatenate** → merges all months into single Indicador_comunicacao.csv
5. **Verify** → checks output integrity (columns, data types, duplicates, no nulls)

If a month is already computed, it **skips it entirely** → very fast on subsequent runs.

---

## Usage Patterns

### Pattern A: Daily Fast Updates (Recommended)

```bash
# Run every weekday (Monday-Friday) around 11 PM
daily_simple.bat
```

**What it does:**
- Checks if new data arrived for current/previous month
- Computes only missing days/months
- Takes ~5-10 minutes for a new month
- Subsequent runs take ~30 seconds (skips existing months)

**Schedule with Task Scheduler:**
```
Program: C:\Windows\System32\cmd.exe
Arguments: /c d:\Projects\visualizer-tuis\daily_simple.bat
Schedule: Daily, 23:00 (or adjust as needed)
```

### Pattern B: Weekly Full Analysis

```bash
# Run once a week (e.g., Monday morning)
generate_all_months.bat
```

**What it does:**
- Computes both simple + full pipelines
- May take 30-60 minutes first time (all months)
- Subsequent runs skip already-computed months (~1 minute)
- Produces moving-window analytics

---

## Performance Optimization

### Cold Start (first run)
- All 3 months (Jan-Mar): **~40-60 minutes** (both pipelines)
- Simple pipeline only: **~20-30 minutes**

### Warm Start (incremental run)
- New month only: **~5-10 minutes** (simple)
- Existing months: **skipped** (0 seconds)

### Example: First Wednesday of Month
1. New month data becomes available (say Feb 1 gets Feb data)
2. Run daily orchestrator → detects Feb is now complete
3. Runs Feb for both pipelines → 10-15 minutes
4. Concatenates (Jan+Feb): 1 second
5. Output is current

### Subsequent Runs Same Month
1. Data for Feb 15 arrives
2. Run daily orchestrator → Feb already computed, all Jan/Feb complete
3. Skips computation (nothing to do) → 10 seconds

---

## Monthly Checking Algorithm

The system automatically:

1. **Detects month completion** based on Diario file count:
   ```
   Jan (31 days): File count == 31 ✓ Complete
   Feb (28 days): File count == 28 ✓ Complete  
   Mar (25 days): File count == 25 ✗ Incomplete (expected 31)
   ```

2. **Compares with outputs**: Each pipeline tracks which months are in output directory

3. **Determines action**:
   - All available months computed? → ✓ Skip (run fast-path)
   - New month detected? → Compute it
   - Month already computed? → Skip it

4. **Handles edge cases**:
   - Partial month (data arrives gradually) → Waits until complete
   - Missing dates in a month → Marked incomplete
   - Re-run same month? → Overwrites output (safe)

---

## Daily Workflow Example

```
2026-03-07 (Friday, 11 PM)
========================
Run: daily_simple.bat

1. Check available data
   → Jan: 31/31 ✓, Feb: 28/28 ✓, Mar: 7/31 ✗

2. Check computed
   → Jan ✓, Feb ✓, Mar ✗

3. Plan: Compute Mar (when complete)
   → Mar is incomplete, wait

4. No computation needed
   → Output: Indicador_comunicacao.csv (Jan+Feb only)
   → Runtime: 10 seconds (all skipped)

---

2026-03-31 (Tuesday, 11 PM)
========================
Run: daily_simple.bat

1. Check available data
   → Jan: 31/31 ✓, Feb: 28/28 ✓, Mar: 31/31 ✓

2. Check computed
   → Jan ✓, Feb ✓, Mar ✗

3. Plan: Compute Mar
   → Mar is complete, run it

4. Compute + concatenate + verify
   → Runtime: 8 minutes (Mar computed + concatenation)
   → Output: Indicador_comunicacao.csv (Jan+Feb+Mar)
```

---

## Configuration

### For Daily Mode

Leave config as-is. The daily orchestrator will:
- Auto-detect months
- Run only new months
- Concatenate all

### For Custom Scheduling

Edit batch files to adjust parameters:

```batch
REM Run simple pipeline only
python -m src.daily_orchestrator <config> <diario_dir> <output_dir> "simple"

REM Run full pipeline only
python -m src.daily_orchestrator <config> <diario_dir> <output_dir> "full"

REM Run both (default)
python -m src.daily_orchestrator <config> <diario_dir> <output_dir> "simple,full"
```

---

## Verification

After every run, integrity is checked:
- ✓ File exists and has content
- ✓ All expected columns present
- ✓ DISP values are binary (0 or 1)
- ✓ No null values
- ✓ No duplicate rows
- ✓ Correct data types

Example output:
```
✓ File exists: True
  Size: 2,456,789 bytes
  Rows: 24,187
  Columns: 6

✓ All expected columns present

✓ DISP range: 0.0000–1.0000
  ✓ All values are binary (0 or 1)

✓ No null values

✓ No duplicates

✓ VALIDATION PASSED — output file is OK
```

---

## FAQ

**Q: How often should I run daily_simple.bat?**
A: Daily (every 24 hours). The system automatically detects what's new and skips old months.

**Q: Can I run it multiple times per day?**
A: Yes, it's safe. It will skip months already computed and return in ~10 seconds.

**Q: What if data for a day is late (arrives after midnight)?**
A: No problem. The month remains marked as incomplete until all days arrive. Once complete (next run), it will be computed.

**Q: Should I run generate_all_months.bat monthly?**
A: Only if you need the full pipeline. For daily dashboards, daily_simple.bat is sufficient.

**Q: Can I parallelize multiple months?**
A: Currently no (single process per month). Future optimization: spawn parallel Python processes for independent months.

**Q: How much disk space?**
A: Each month ≈ 500 MB (CSV/Parquet intermediate files). Final concatenated output ≈ 1.5 GB for 3 months.

---

## Example: Task Scheduler Setup (Windows)

1. Open Task Scheduler
2. Create Basic Task:
   - Name: `Smart Meter Pipeline Daily`
   - Trigger: Daily, 23:00
   - Action: Start a program
     - Program: `cmd.exe`
     - Arguments: `/c d:\Projects\visualizer-tuis\daily_simple.bat`
     - Start in: `d:\Projects\visualizer-tuis`

3. Save and enable
4. Test: Right-click → Run

Done! Pipeline will run every night automatically.

---

## Summary

| Aspect | Daily Mode | Full Mode |
|--------|-----------|-----------|
| **Frequency** | Daily (every 24h) | Weekly or as-needed |
| **Pipeline** | Simplified only | Both (simple + full) |
| **Speed** | 5-15 min (new month), 30 sec (warm) | 30-60 min (cold), 1 min (warm) |
| **Output Features** | Single-day lookback | 5-day moving window |
| **Use Case** | Dashboards, daily monitoring | Analysis, historical audit |
| **Batch File** | `daily_simple.bat` | `generate_all_months.bat` |
