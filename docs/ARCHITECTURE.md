# Smart Meter Pipeline — Architecture & Current State

> **Last Updated:** 2026-04-06  
> **Status:** Production Ready (April 2026 run completed)

---

## 1. Overview

This repository implements a **three-phase ETL pipeline** for smart meter communication analysis across Paraná municipalities. The pipeline:

- **Reads** ORCA and SANPLAT communication logs (relative-day format)
- **Enriches** them with absolute dates from reference files
- **Computes** 5-day moving-window availability flags per meter per day
- **Joins** with Diário (master meter→municipality mapping), MEDIDORES (meter brands)
- **Aggregates** to produce daily municipality-level indicators
- **Exports** CSV + Parquet, concatenates to `Indicador_comunicacao.csv`

**Key metric:** For each [municipality, smart-brand] pair, per day:
- `CONTAGEM_COMM`: meters communicating (DISP=1)
- `CONTAGEM_TOT`: all smart meters in universe
- `DISP`: communication percentage

---

## 2. Current Data Layout

### Source Data
```
data/raw/
  ├── ORCA/
  │   └── Dados_Comunicacao.parquet     # 1.4M meters, relative columns 90–1
  └── CIS/
      └── Diario/
          ├── Diario_2026-02-01.parquet # Daily meter→municipio join table
          ├── Diario_2026-02-02.parquet
          └── ...

data/refined/
  ├── SANPLAT/
  │   ├── Dados_Comunicacao_SANPLAT.csv # 584K meters, relative columns 91–1
  │   └── Data_Referencia_2.csv         # Reference date (e.g., 01/04/2026)
  └── CIS/
      └── MEDIDORES.parquet             # NIO → INTELIGENTE brand mapping

data/trusted/
  └── CIS/
      └── Data_Referencia.csv           # ORCA reference date anchor
```

### Intermediate & Final Outputs
```
data/trusted/
  ├── ORCA/
  │   ├── Dados_Comunicacao_com_datas.csv  # Phase 1: enriched dates
  │   ├── disp_2026-02.csv                 # Phase 2: per-NIO DISP flags
  │   └── exports/
  │
  ├── SANPLAT/
  │   ├── Dados_Comunicacao_SANPLAT_com_datas.csv
  │   ├── disp_2026-02.csv
  │   └── exports/
  │
  ├── mixed/
  │   └── disp_2026-02.csv                 # Phase 2: concatenated ORCA+SANPLAT
  │
  ├── municipio_daily/
  │   ├── municipio_2026-02.csv            # Phase 3: daily metrics (CSV, ignored)
  │   ├── municipio_2026-02.parquet        # Phase 3: daily metrics (Parquet, ignored)
  │   ├── municipio_2026-03.csv
  │   └── municipio_2026-03.parquet
  │
  └── Indicador_comunicacao.csv            # Final: concatenated all months
```

### .gitignore Policy

- **Ignored:** All `data/` folder (raw + refined + trusted)
- **municipio_daily explicit ignore:**
  ```
  data/trusted/municipio_daily/
  data/trusted/municipio_daily/*.csv
  data/trusted/municipio_daily/*.parquet
  ```
- **Rationale:** Outputs reproducible from config; keep repo small

---

## 3. Configuration

Single source of truth: `config/pipeline.yaml`

```yaml
target_month: "2026-03"           # Month to process (YYYY-MM)
moving_window_days: 5             # Days in OR window
sink_queue_limit: 3               # Max concurrent streams
memory_threshold_percent: 70      # RAM threshold before abort
output_format: parquet            # csv or parquet
orca_binarize: true               # Binarize ORCA decimals → 0/1

paths:
  orca_raw: "data/raw/ORCA/Dados_Comunicacao.parquet"
  sanplat_refined: "data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv"
  orca_ref_date: "data/trusted/CIS/Data_Referencia.csv"
  sanplat_ref_date: "data/trusted/SANPLAT/Data_Referencia_2.csv"
  orca_enriched: "data/trusted/ORCA/Dados_Comunicacao_com_datas.csv"
  sanplat_enriched: "data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv"
  diario_dir: "D:/Projects/visualizer-tuis/data/raw/CIS/Diario"
  medidores: "data/refined/CIS/MEDIDORES.parquet"
  mixed_output: "data/trusted/mixed"
  municipio_daily_output: "data/trusted/municipio_daily"
```

---

## 4. Script Organization (Current)

```
src/
├── pipeline_orchestrator.py    # Entry point: Phases 1–3
├── enrich_dates.py             # Phase 1: date enrichment
├── moving_window.py            # Phase 2: DISP flags
├── join_daily.py               # Phase 3: join & aggregate
├── sink_manager.py             # Infrastructure: streaming I/O
└── memory_monitor.py           # Infrastructure: memory tracking

scripts/
├── export_date_ranges.py       # Slice enriched files by date range
└── concat_indicador.py         # Concatenate monthly outputs

main.py                         # CLI dispatcher
```

---

## 5. Data Flow (Current: Multi-Script)

```
User edits config/pipeline.yaml (target_month, paths)
         ↓
[Phase 1: Enrichment]
  Input:  ORCA.parquet, SANPLAT.csv + reference dates
  Output: *_com_datas.csv (enriched, date-headed columns)
         ↓
[Optional: Export date windows]
  Input:  *_com_datas.csv
  Output: exports/*_window_*.csv (staging slices)
         ↓
[Phase 2: Moving Window]
  Input:  *_com_datas.csv
  Output: disp_*.csv (per-meter binary DISP flags)
         ↓
[Phase 3: Join & Aggregate]
  Input:  disp_*.csv, Diario/*.parquet, MEDIDORES.parquet
  Output: municipio_YYYY-MM.csv (daily municipality metrics)
         ↓
[Concatenate All Months]
  Input:  municipio_*.csv
  Output: Indicador_comunicacao.csv (final report)
```

---

## 6. Execution Paths (Current)

### Full Pipeline
```bash
python -m src.pipeline_orchestrator config/pipeline.yaml
# Runs Phases 1–3 end-to-end
```

### Enrich Only
```bash
python src/enrich_dates.py --all
```

### Export Date Range
```bash
python scripts/export_date_ranges.py config/pipeline.yaml
# Generates date-windowed slices (staging)
```

### Concatenate Outputs
```bash
python scripts/concat_indicador.py [--drop-duplicates]
```

---

## 7. Key Technical Decisions

| Decision | Rationale | Tradeoff |
|----------|-----------|----------|
| Three-phase split | Clear responsibilities | Extra I/O |
| Merge ORCA+SANPLAT before Diário join | Prevents row duplication | Adds merge step |
| Per-origin DISP files | Audit trail for validation | Extra files |
| Lazy Polars evaluation | Memory efficient | Errors at `.collect()` time |
| Single config file | No sync issues | New users must know about it |
| Ignore municipio_daily | Reproducible from config | Need explicit ignore rules |

---

## 8. Known Limitations

| Issue | Severity |
|-------|----------|
| Multiple entry points (unclear which script to call) | Medium |
| No unified CLI / single orchestrator | Medium |
| SANPLAT column rename in enrichment (implicit) | Low |
| No built-in validation layer (schema checks) | Medium |
| Phase dependencies not enforced | Medium |

---

## 9. Testing

```bash
pytest -v tests/
# test_enrich.py, test_moving_window.py, test_join_daily.py
```

---

## 10. Dependencies

- `polars` — DataFrame I/O, lazy evaluation
- `pyyaml` — Config parsing
- `psutil` — Memory monitoring

---

## 11. Next Phase: Unified Orchestrator

(See `PRD_ORCHESTRATOR_REFACTOR.md` for detailed refactoring plan)

**Goal:** Single CLI entry point with modular phases, built-in validation, parallel execution.

