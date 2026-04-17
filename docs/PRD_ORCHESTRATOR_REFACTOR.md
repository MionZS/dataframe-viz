# PRD: Unified Smart Meter Pipeline Orchestrator

> **Version:** 2.0  
> **Status:** In Planning  
> **Target Ship:** 2026-Q2  
> **Last Updated:** 2026-04-06

---

## Executive Summary

Transform the current **multi-script, multi-entry-point pipeline** into a **seamless single-orchestrator system** with:

- One unified CLI entry point (`orchestrate.py`)
- Modular, reusable phase functions
- Built-in validation, error handling, and observability
- Optional parallelization for Phase 2 (per-date DISP) and Phase 3 aggregation
- Enhanced troubleshooting with pre/post-phase checkpoints

**Outcome:** Reduce user friction from "which script do I call?" to "run `orchestrate.py`."

---

## 1. Problem Statement

**Current Challenges:**

1. **Multiple Entry Points**  
   - `src/pipeline_orchestrator.py` (main flow)
   - `src/enrich_dates.py` (Phase 1 standalone)
   - `scripts/export_date_ranges.py` (optional staging)
   - `scripts/concat_indicador.py` (final concat)
   - `main.py` (CLI dispatcher, underutilized)
   
   → Users unclear which to call first; hard to read/maintain.

2. **No Unified CLI**  
   - No `--validate`, `--dry-run`, `--skip-phases` flags
   - No built-in help or progress tracking
   - Hard to test individual phases in isolation

3. **Phase Dependencies Not Enforced**  
   - Phase 3 silently fails if Phase 2 output missing
   - No schema validation between phases
   - Difficult to resume after partial failure

4. **Configuration Scattered**  
   - `pipeline.yaml` for main config
   - Hardcoded paths in some scripts
   - Export config historically in `export_config.json` (now dynamic, but still separate pattern)

5. **No Built-In Observability**  
   - Logging scattered across modules
   - Hard to trace data lineage
   - No phase-by-phase row counts or memory snapshots

---

## 2. Vision: Unified Orchestrator Flow

```
user$ python orchestrate.py --config config/pipeline.yaml [--phase PHASE] [--dry-run] [--validate]

orchestrate.py (main orchestrator)
    ├── Parse CLI args + load config
    ├── Validate inputs (file existence, schema, reference dates)
    ├── Phase 1: Enrichment
    │   ├── Check: ORCA raw exists, reference date valid
    │   ├── Run: process_orca() + process_sanplat() (from enrich_dates module)
    │   ├── Output: *_com_datas.csv
    │   └── Checkpoint: row counts, sample row
    ├── Phase 2: Moving Window
    │   ├── Check: enriched files exist, reference dates match
    │   ├── Run: compute_disp() for each date
    │   ├── Merge ORCA+SANPLAT → mixed/disp_*.csv
    │   └── Checkpoint: DISP value distribution, null checks
    ├── Phase 3: Join & Aggregate
    │   ├── Check: disp_*.csv exists, Diário files present
    │   ├── Run: join_with_diario() + join_with_medidores() + aggregate()
    │   ├── Output: municipio_YYYY-MM.csv + .parquet
    │   └── Checkpoint: aggregation sums match DISP input row counts
    ├── [Optional] Export & Concat
    │   ├── (Re)slice enriched files by date window
    │   ├── Concatenate all monthly municipio_*.* files
    │   └── Output: Indicador_comunicacao.csv
    └── Report: Summary, row counts, elapsed time
```

---

## 3. Architecture: Modular Phase Refactoring

### 3.1 New Structure

```
src/
├── orchestrate.py            ← NEW: Main entry point + CLI
├── phases/
│   ├── __init__.py
│   ├── phase1_enrichment.py  ← Refactored enrich_dates.py
│   ├── phase2_moving_window.py ← Refactored moving_window.py
│   ├── phase3_join_aggregate.py ← Refactored join_daily.py
│   └── phase4_export_concat.py ← NEW: export + concat logic
├── core/
│   ├── __init__.py
│   ├── validation.py         ← NEW: Pre/post-phase checks
│   ├── checkpoint.py         ← NEW: Phase completion markers
│   ├── config_loader.py      ← NEW: Unified config parsing
│   └── errors.py             ← NEW: Domain-specific exceptions
├── enrich_dates.py           ← Keep for backward compat (imports from phases/phase1_enrichment)
├── moving_window.py          ← Keep for backward compat
├── join_daily.py             ← Keep for backward compat
├── sink_manager.py           ← Keep as-is
└── memory_monitor.py         ← Keep as-is

scripts/
├── concat_indicador.py       ← Keep; also callable from orchestrate.py
└── export_date_ranges.py     ← Keep; also callable from orchestrate.py

main.py                         ← Delegate to orchestrate.py
orchestrate.py                 ← NEW: Main CLI (also in src/)
```

### 3.2 Phase Module Interface

Each phase module exposes:

```python
# phase1_enrichment.py
def run(config: dict, logger: logging.Logger) -> PhaseResult:
    """Run Phase 1. Return PhaseResult with metadata."""
    pass

# phase2_moving_window.py
def run(config: dict, enriched_disp_cache: dict, logger: logging.Logger) -> PhaseResult:
    """Run Phase 2. Optional cache for memoization."""
    pass

# ... etc
```

**PhaseResult:**
```python
@dataclass
class PhaseResult:
    phase_name: str
    status: str  # "success" | "failed" | "skipped"
    error: Optional[str]
    row_count_in: int
    row_count_out: int
    files_created: List[str]
    duration_sec: float
    checkpoint_data: dict  # Validation snapshots
```

---

## 4. CLI Interface

```bash
# Full pipeline (all phases)
python orchestrate.py --config config/pipeline.yaml

# Single phase (for debugging)
python orchestrate.py --config config/pipeline.yaml --phase 1
python orchestrate.py --config config/pipeline.yaml --phase 2

# Validate only (no execution)
python orchestrate.py --config config/pipeline.yaml --validate

# Dry run (parse config, check files, print plan, exit)
python orchestrate.py --config config/pipeline.yaml --dry-run

# Skip phase 1 enrichment (use existing *_com_datas.csv)
python orchestrate.py --config config/pipeline.yaml --skip 1

# Export + concat only (no Phases 1–3)
python orchestrate.py --config config/pipeline.yaml --export-only

# Verbose logging
python orchestrate.py --config config/pipeline.yaml -v DEBUG

# Output report to file
python orchestrate.py --config config/pipeline.yaml --report report_2026-04-06.json
```

---

## 5. Validation Layer (Phase Checkpoints)

### Pre-Phase Checks
```python
# phase2_validation.py
def validate_phase1_output(config) -> Validation:
    """Check Phase 1 outputs before Phase 2."""
    checks = [
        FileExistsCheck("orca_enriched"),
        FileExistsCheck("sanplat_enriched"),
        SchemaCheck("orca_enriched", required_cols=["NIO", "DD/MM/YYYY"]),
        DateRangeCheck("orca_enriched", expected_range=config["date_range"]),
    ]
    return Validation(checks)
```

### Post-Phase Checkpoints
```python
# After Phase 2, before Phase 3:
checkpoint = {
    "disp_total_rows": 1_433_589,
    "disp_communicating": 1_208_651,
    "disp_null_count": 0,
    "date_range": ("2026-02-01", "2026-02-28"),
}
# Save to data/trusted/mixed/.checkpoint_phase2_2026-02.json
```

### Fast Validation Resume
```bash
# Skip Phase 1–2, resume from checkpoint
python orchestrate.py --config config/pipeline.yaml \
  --resume-from data/trusted/mixed/.checkpoint_phase2_2026-02.json
```

---

## 6. Error Handling & Recovery

### Custom Exception Hierarchy
```python
class PipelineException(Exception):
    """Base."""
    pass

class ValidationError(PipelineException):
    """Pre-phase check failed."""
    pass

class PhaseExecutionError(PipelineException):
    """Phase runtime error."""
    pass

class CheckpointError(PipelineException):
    """Post-phase validation failed."""
    pass
```

### Global Error Handler
```python
def main():
    try:
        result = orchestrate(config, phases=[1, 2, 3, 4])
        print(f"✓ Pipeline succeeded in {result.total_time:.1f}s")
    except ValidationError as e:
        print(f"✗ Validation failed: {e}"); sys.exit(1)
    except PhaseExecutionError as e:
        print(f"✗ Phase {e.phase} failed: {e.message}"); sys.exit(2)
    except CheckpointError as e:
        print(f"✗ Post-phase check failed: {e}"); sys.exit(3)
    except Exception as e:
        logger.exception("Unexpected error"); sys.exit(99)
```

---

## 7. Implementation Roadmap

### Phase A: Modularization (Week 1)
1. Create `src/phases/` folder structure
2. Move Phase 1 logic to `phase1_enrichment.py`; wrap in `run()` function
3. Move Phase 2 logic to `phase2_moving_window.py`; wrap in `run()` function
4. Move Phase 3 logic to `phase3_join_aggregate.py`; wrap in `run()` function
5. Update `enrich_dates.py`, `moving_window.py`, `join_daily.py` to import from phases/* (backward compat)
6. Add tests for each phase

### Phase B: Orchestrator Core (Week 2)
1. Create `src/core/config_loader.py` — unified YAML + CLI arg parsing
2. Create `src/core/validation.py` — pre/post-phase checks
3. Create `src/core/checkpoint.py` — save/load phase results
4. Create `src/orchestrate.py` — main orchestrator logic
5. Add CLI arg parser (argparse)
6. Integrate error handling

### Phase C: CLI & Testing (Week 3)
1. Add CLI flags: `--phase`, `--dry-run`, `--validate`, `--skip`, `--export-only`, `--resume-from`
2. Add logging verbosity levels
3. Add report generation (JSON summary)
4. Write integration tests (e2e)
5. Manual testing of each CLI path

### Phase D: Documentation & Launch (Week 4)
1. Update README with new CLI usage
2. Create troubleshooting guide
3. Document checkpoint/resume workflow
4. Deprecate old entry points (but keep for backward compat)
5. Release as v2.0

---

## 8. Backward Compatibility

**Keep working:**
- `python -m src.pipeline_orchestrator config/pipeline.yaml`
- `python src/enrich_dates.py --all`
- `python scripts/export_date_ranges.py config/pipeline.yaml`
- `python scripts/concat_indicador.py`

**Redirect to new orchestrator:**
```python
# main.py
if __name__ == "__main__":
    import sys
    from src.orchestrate import main as orchestrate_main
    sys.exit(orchestrate_main())
```

---

## 9. Benefits

| Benefit | Impact |
|---------|--------|
| **Single CLI entry point** | Reduced user confusion; easier onboarding |
| **Built-in validation** | Catch errors early; fail fast |
| **Phase checkpoints** | Resume after failure; audit trail |
| **Unified logging** | Better troubleshooting; trace data lineage |
| **Optional parallelization** | Potential 2–3x speedup for Phase 2–3 |
| **Backward compat** | Existing scripts still work; gradual migration |

---

## 10. Success Criteria

✓ Single `orchestrate.py` CLI runs full pipeline  
✓ All CLI flags work (`--phase`, `--dry-run`, `--validate`, etc.)  
✓ Phase checkpoints saved/loaded correctly  
✓ Error messages actionable (not cryptic stack traces)  
✓ Backward-compatible (old entry points still work)  
✓ Documentation updated  
✓ All tests pass (unit + integration)

---

## 11. Risks & Mitigations

| Risk | Likelihood | Mitigation |
|------|------------|-----------|
| Refactoring introduces regression | Medium | Extensive testing; parallel testing with old system |
| Performance regression | Low | Benchmark Phase 2–3 before/after |
| User friction during transition | Low | Keep old entry points; gradual deprecation |

---

## Appendix: Example Usage (Future)

```bash
# New user experience (post-refactoring)

$ cd visualizer-tuis

$ # Check config
$ cat config/pipeline.yaml | grep target_month
target_month: "2026-04"

$ # Validate inputs
$ python orchestrate.py --config config/pipeline.yaml --validate
[INFO] Validating inputs...
[OK] ORCA raw file exists
[OK] SANPLAT ref date valid (01/04/2026)
[OK] Diário directory has 90 files (date range 2026-02-01 to 2026-04-30)
[OK] MEDIDORES list 2M+ rows; brand filter will retain ~1.5M smart meters
✓ All validations passed

$ # Dry run (no execution)
$ python orchestrate.py --config config/pipeline.yaml --dry-run
[INFO] Phase 1: Enrich (ORCA + SANPLAT)
  Input: ORCA (1.4M meters × 90 days), SANPLAT (584K meters × 90 days)
  Output: *_com_datas.csv (~2GB total)
  Est. time: 30s
[INFO] Phase 2: Moving Window (5-day OR per meter per day)
  Input: *_com_datas.csv
  Dates: 2026-04-01 to 2026-04-30 (30 days)
  Output: disp_2026-04.csv, mixed/disp_2026-04.csv (~800MB)
  Est. time: 180s (parallelizable)
[INFO] Phase 3: Join & Aggregate
  Input: disp_2026-04.csv, Diário (2026-04-01 to 2026-04-30), MEDIDORES
  Output: municipio_2026-04.csv (1000+ rows)
  Est. time: 60s
[INFO] Phase 4: Export & Concat
  Input: All municipio_*.csv
  Output: Indicador_comunicacao.csv
  Est. time: 5s
[INFO] Total est. time: ~5 min. Proceed? (y/n) y

$ # Full run
$ python orchestrate.py --config config/pipeline.yaml
[Phase 1/4] Enrichment...  ✓ (28s, 2.0GB written)
[Phase 2/4] Moving Window...  ✓ (185s, 1.4M×30 DISP flags)
[Phase 3/4] Join & Aggregate...  ✓ (58s, 47K rows written)
[Phase 4/4] Export & Concat...  ✓ (4s, Indicador_comunicacao.csv)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total: 275s (4m 35s)
Outputs: 
  - municipio_2026-04.csv (1.2MB)
  - municipio_2026-04.parquet (450KB)
  - Indicador_comunicacao.csv (12MB, 6 months of data)
✓ Pipeline succeeded

$ # Export outputs directory
$ ls -lh data/trusted/
total 1.3G
drwxr-xr-x ORCA/
drwxr-xr-x SANPLAT/
drwxr-xr-x mixed/
drwxr-xr-x municipio_daily/
-rw-r--r-- 12M Indicador_comunicacao.csv
```

