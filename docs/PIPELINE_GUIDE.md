# Smart Meter Communication Pipeline — Step-by-Step Guide

> **Last updated:** 2026-02-26  
> **Config:** `config/pipeline.yaml`  
> **Entry point:** `src/pipeline_orchestrator.py → run()`

---

## Table of Contents

1. [Overview](#overview)
2. [Data Sources](#data-sources)
3. [Phase 1 — Enrichment](#phase-1--enrichment)
4. [Phase 2 — Moving Window](#phase-2--moving-window)
5. [Phase 3 — Join & Aggregate](#phase-3--join--aggregate)
6. [Final Output Schema](#final-output-schema)
7. [Diagrams](#diagrams)

---

## Overview

The pipeline answers one question: **for each municipality and smart-meter
brand, how many meters communicated on a given day, and what percentage is
that of the total?**

It runs in **3 sequential phases**:

| Phase | Purpose | Output |
|-------|---------|--------|
| 1 — Enrichment | Rename relative day columns → absolute dates | `trusted/ORCA/…_com_datas.csv`, `trusted/SANPLAT/…_com_datas.csv` |
| 2 — Moving Window | Compute per-NIO binary DISP flag (0/1) per day | `trusted/ORCA/disp_YYYY-MM.csv`, `trusted/SANPLAT/disp_YYYY-MM.csv`, `trusted/mixed/disp_YYYY-MM.csv` |
| 3 — Join & Aggregate | Join with Diario + MEDIDORES, group, compute DISP % | `trusted/municipio_daily/municipio_YYYY-MM.csv` (+ `.parquet`) |

**DISP (availability %) is NOT computed until Phase 3**, at the
municipality+brand level. The per-NIO binary flag from Phase 2 is just an
intermediate 0/1 that feeds the count.

---

## Data Sources

| Source | Path | Columns of interest | Notes |
|--------|------|---------------------|-------|
| **ORCA raw** | `data/raw/ORCA/Dados_Comunicacao.parquet` | NIO, 90…1 (relative days) | ~1.4M meters. European decimals (0/0.25/0.5/0.75/1) — must binarize |
| **SANPLAT refined** | `data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv` | NIO, 91…1 (relative days) | ~584K meters. Already binary 0/1 |
| **ORCA ref date** | `data/trusted/CIS/Data_Referencia.csv` | Single date value | Anchor for column→date mapping |
| **SANPLAT ref date** | `data/trusted/SANPLAT/Data_Referencia_2.csv` | Single date value | Anchor for column→date mapping |
| **Diário** | `D:/dados/Diario/Diario_YYYY-MM-DD.parquet` | NIO, MUNICIPIO | ~1.97M meters/day. Source of truth for the meter universe |
| **MEDIDORES** | `data/refined/CIS/MEDIDORES.parquet` | NIO, INTELIGENTE | ~2M rows. INTELIGENTE = meter brand string |

### Smart Meter Brands (filter set)

Only these three values in the INTELIGENTE column are kept:

- **Hexing**
- **Nansen**
- **Nansen Ipiranga**

Everything else (e.g. "WALK BY", "Convencional", "Inteligente Sem informação", null) is **discarded** via an inner join.

---

## Phase 1 — Enrichment

**Module:** `src/enrich_dates.py`  
**Functions:** `process_orca()`, `process_sanplat()`

### What happens

1. Read the **reference date** from the ref CSV (single date value).
2. Load the raw data file (parquet for ORCA, CSV for SANPLAT).
3. Identify all **numeric column names** (90, 89, …, 1) — these are
   relative day offsets.
4. Compute the absolute date for each relative column:
   - Column `max_col` (90 or 91) = reference_date − 1
   - Each subsequent column = one day earlier.
5. **Rename** every numeric column to its absolute date string (`DD/MM/YYYY`).
6. Write the enriched CSV with date-headed columns.

### What stays / what's erased

| Kept | Erased |
|------|--------|
| NIO column (unchanged) | Relative numeric column names (replaced by dates) |
| All data values (unchanged) | Nothing — values are preserved exactly |

### Decision: No DISP calculation here

The enrichment phase only renames columns. It does **not** compute
availability. DISP cannot be accurately computed at this stage because we
don't yet know which municipality each meter belongs to — that information
comes from the Diário join in Phase 3.

### Output

```
trusted/ORCA/Dados_Comunicacao_com_datas.csv
trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv
```

Each file has columns: `NIO, DD/MM/YYYY, DD/MM/YYYY, …`

---

## Phase 2 — Moving Window

**Module:** `src/moving_window.py`  
**Function:** `compute_disp()`

### What happens

For each day D in the target month (e.g. 2026-01-01 through 2026-01-31):

1. Load the enriched CSV (from Phase 1).
2. Build a **column→date mapping** from the column headers.
3. Find the 5 columns whose dates fall in **[D−5, D−1]** — the look-back
   window.
4. **Binarize** (ORCA only): any value > 0 → 1, accounting for European
   decimals (comma → dot → float).
5. Compute: `DISP(NIO, D) = max(col[D-5], col[D-4], col[D-3], col[D-2], col[D-1])`
   - This is a logical OR: if the meter communicated on **any** of the 5
     preceding days, DISP = 1.
6. Result: a 2-column frame `[NIO, DISP]` where DISP ∈ {0, 1}.

### Why 5 days?

Smart meters may communicate intermittently. A 5-day window smooths out
gaps — if a meter talked at least once in the last 5 days, it's considered
"communicating" for day D.

### Per-origin intermediates

Each origin is processed independently, then tagged:

```
trusted/ORCA/disp_2026-01.csv       → [NIO, DISP, DATA]
trusted/SANPLAT/disp_2026-01.csv    → [NIO, DISP, DATA]
```

### Mixed concatenation

Both origins are concatenated with an ORIGEM tag:

```
trusted/mixed/disp_2026-01.csv → [NIO, DISP, DATA, ORIGEM]
```

This mixed file is the input to Phase 3.

### What stays / what's erased

| Kept | Erased |
|------|--------|
| NIO | All 90 date columns (reduced to single DISP) |
| DISP (0 or 1) | Raw communication values |
| DATA (date string) | — |
| ORIGEM (in mixed) | — |

---

## Phase 3 — Join & Aggregate

**Module:** `src/pipeline_orchestrator.py` (orchestration),
`src/join_daily.py` (joins)  
**Key functions:** `_process_date()`, `join_with_diario()`,
`join_with_medidores()`, `aggregate()`

This phase runs **once per day** in the target month (31 iterations for
January). Each day follows this pipeline:

### Step 3.1 — Filter mixed DISP for the day

```python
day_disp = mixed_lazy.filter(pl.col("DATA") == "2026-01-15").collect()
```

Split by ORIGEM because each origin's meters must be joined with the same
Diário file but tagged separately.

### Step 3.2 — Join with Diário (LEFT JOIN from Diário)

**Function:** `join_with_diario()`  
**Join type:** LEFT JOIN — **Diário is the left table.**

```
Diário [NIO, MUNICIPIO]  ←LEFT JOIN→  DISP [NIO, DISP]
                          on NIO
```

**Why left from Diário?** The Diário is the source of truth for the meter
universe. Every meter in the Diário appears in the output. Meters in DISP
but not in Diário are dropped (they don't exist in the official universe).
Meters in Diário but not in DISP get `DISP = 0` (not communicating).

NIO normalization: both sides cast NIO to string and strip leading zeros
(`"0043138963"` → `"43138963"`) so different zero-padding doesn't break
the join.

**Result columns:** `[NIO, MUNICIPIO, ORIGEM, DISP]`

This is done **per ORIGEM** (once for ORCA rows, once for SANPLAT rows),
then the results are concatenated.

### Step 3.3 — Join with MEDIDORES (INNER JOIN)

**Function:** `join_with_medidores()`  
**Join type:** INNER JOIN — only smart-meter brands survive.

```
Joined [NIO, MUNICIPIO, ORIGEM, DISP]  ←INNER JOIN→  MEDIDORES [NIO, INTELIGENTE]
                                         on NIO
```

Before the join, MEDIDORES is:
1. Cast INTELIGENTE to Utf8 (may be Categorical in source).
2. **Filtered** to only rows where INTELIGENTE ∈ {Hexing, Nansen, Nansen Ipiranga}.

**What gets dropped:** Every meter whose brand is NOT one of the 3 target
brands. This includes "WALK BY", "Convencional", "Inteligente Sem
informação", and any NIO not found in MEDIDORES at all.

**What survives:** Only meters with a known smart-meter brand.

NIO normalization is applied here too (strip leading zeros).

**Result columns:** `[NIO, MUNICIPIO, ORIGEM, DISP, INTELIGENTE]`

### Step 3.4 — Aggregate

**Function:** `aggregate()`

Group by `[MUNICIPIO, INTELIGENTE]` and compute:

| Output Column | Computation |
|---------------|-------------|
| **MUNICIPIO** | Group key — municipality name |
| **INTELIGENTE** | Group key — brand name (Hexing / Nansen / Nansen Ipiranga) |
| **CONTAGEM_COMM** | `SUM(DISP)` — count of NIOs that communicated (DISP=1) |
| **CONTAGEM_TOT** | `COUNT(NIO)` — total NIOs in this municipality+brand group |
| **DISP** | `CONTAGEM_COMM / CONTAGEM_TOT` — availability ratio (0.0–1.0), rounded to 4 decimals |
| **DATA** | The date being processed (YYYY-MM-DD) |

**ORIGEM is dropped** from the output — it was only needed to route the
Diário join. The final grouping is purely by municipality and brand.

### Step 3.5 — Stream Sink

**Module:** `src/sink_manager.py`

Each day's aggregated DataFrame is appended to a **single CSV file** via
streaming:

1. `open_stream()` — creates/truncates the output CSV.
2. `submit(agg)` — appends rows (header written only on first call).
3. `close_stream()` — flushes and closes.
4. `stream_to_parquet()` — lazy CSV→Parquet conversion (zero extra RAM).

**Memory monitor** (`src/memory_monitor.py`) checks RSS before/after each
day. Aborts if threshold exceeded (default 70% of total RAM).

---

## Final Output Schema

**File:** `trusted/municipio_daily/municipio_2026-01.csv` (and `.parquet`)

| Column | Type | Example | Description |
|--------|------|---------|-------------|
| MUNICIPIO | Utf8 | `"CURITIBA"` | Municipality name (from Diário) |
| INTELIGENTE | Utf8 | `"Hexing"` | Smart-meter brand |
| CONTAGEM_COMM | Int64 | `45230` | NIOs that communicated (DISP=1) |
| CONTAGEM_TOT | Int64 | `52100` | Total NIOs in this group |
| DISP | Float64 | `0.8682` | Availability = comm / total |
| DATA | Utf8 | `"2026-01-15"` | Date (YYYY-MM-DD) |

Each municipality can have **up to 3 rows per day** (one per brand).
A full month (31 days) × ~400 municipalities × 3 brands ≈ **up to ~37,200
rows** in the output (actual count depends on brand coverage).

---

## Diagrams

### Full Pipeline Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                        PHASE 1: ENRICHMENT                         │
│                                                                     │
│  ORCA raw (.parquet)  ──┐                                           │
│  + ref date CSV         ├──→  Rename cols 90..1 → DD/MM/YYYY       │
│                         │     Output: ORCA_com_datas.csv            │
│                         │                                           │
│  SANPLAT refined (.csv) ┤                                           │
│  + ref date CSV         ├──→  Rename cols 91..1 → DD/MM/YYYY       │
│                         │     Output: SANPLAT_com_datas.csv         │
└─────────────────────────┼───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    PHASE 2: MOVING WINDOW                          │
│                                                                     │
│  For each day D in target month:                                    │
│    ┌──────────────────────────────────┐                              │
│    │ Select 5 columns: D-5 … D-1     │                              │
│    │ Binarize (ORCA only)            │                              │
│    │ DISP = max(5 cols) → 0 or 1    │                              │
│    │ Output: [NIO, DISP]            │                              │
│    └──────────────────────────────────┘                              │
│                                                                     │
│  Per-origin: ORCA/disp_YYYY-MM.csv, SANPLAT/disp_YYYY-MM.csv      │
│  Concatenated: mixed/disp_YYYY-MM.csv  [NIO, DISP, DATA, ORIGEM]  │
└─────────────────────────┼───────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────────────────┐
│              PHASE 3: JOIN & AGGREGATE (per day)                   │
│                                                                     │
│  ┌─────────────┐    ┌───────────────────────────┐                   │
│  │ mixed DISP  │───→│ Filter: DATA = "2026-01-D"│                   │
│  │ (lazy scan) │    │ Split by ORIGEM            │                   │
│  └─────────────┘    └─────────┬─────────────────┘                   │
│                               │                                     │
│                               ▼                                     │
│  ┌──────────────────────────────────────────────────┐               │
│  │  JOIN WITH DIÁRIO (LEFT from Diário)             │               │
│  │                                                    │               │
│  │  Diário [NIO, MUNICIPIO]                          │               │
│  │    LEFT JOIN                                       │               │
│  │  DISP [NIO, DISP]                                │               │
│  │    → fill_null(DISP, 0)                           │               │
│  │    → add ORIGEM tag                               │               │
│  │                                                    │               │
│  │  Result: [NIO, MUNICIPIO, ORIGEM, DISP]           │               │
│  └──────────────────────┬───────────────────────────┘               │
│                         │                                           │
│                         ▼                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │  JOIN WITH MEDIDORES (INNER)                      │               │
│  │                                                    │               │
│  │  MEDIDORES pre-filtered to:                       │               │
│  │    Hexing, Nansen, Nansen Ipiranga ONLY           │               │
│  │                                                    │               │
│  │  Joined [NIO, MUNICIPIO, ORIGEM, DISP]            │               │
│  │    INNER JOIN                                      │               │
│  │  MEDIDORES [NIO, INTELIGENTE]                     │               │
│  │                                                    │               │
│  │  ❌ DROPPED: meters not in target brands           │               │
│  │  ✓  KEPT: only smart-meter NIOs                   │               │
│  │                                                    │               │
│  │  Result: [NIO, MUNICIPIO, ORIGEM, DISP,           │               │
│  │           INTELIGENTE]                             │               │
│  └──────────────────────┬───────────────────────────┘               │
│                         │                                           │
│                         ▼                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │  AGGREGATE                                        │               │
│  │                                                    │               │
│  │  GROUP BY [MUNICIPIO, INTELIGENTE]                │               │
│  │                                                    │               │
│  │  CONTAGEM_COMM = SUM(DISP)      ← communicating  │               │
│  │  CONTAGEM_TOT  = COUNT(NIO)     ← total meters   │               │
│  │  DISP = CONTAGEM_COMM / CONTAGEM_TOT  ← ratio    │               │
│  │  DATA = "2026-01-DD"                              │               │
│  │                                                    │               │
│  │  ❌ ORIGEM dropped (not in group keys)             │               │
│  └──────────────────────┬───────────────────────────┘               │
│                         │                                           │
│                         ▼                                           │
│  ┌──────────────────────────────────────────────────┐               │
│  │  STREAM SINK → append to municipio_2026-01.csv   │               │
│  └──────────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  CSV → Parquet        │
              │  (lazy, zero-RAM)     │
              └───────────────────────┘
```

### Join Detail: Where columns come from

```
NIO             ← Diário (source of truth)
MUNICIPIO       ← Diário (LEFT JOIN)
DISP (0/1)      ← Moving Window (Phase 2), fill_null → 0
ORIGEM          ← Literal tag added during Diário join (used internally, dropped at aggregate)
INTELIGENTE     ← MEDIDORES (INNER JOIN, pre-filtered to 3 brands)
CONTAGEM_COMM   ← SUM(DISP) per group
CONTAGEM_TOT    ← COUNT(NIO) per group
DISP (ratio)    ← CONTAGEM_COMM / CONTAGEM_TOT (computed at aggregate, replaces binary flag)
DATA            ← Literal date string added at aggregate
```

### Data Volume at Each Step (typical day)

```
Step                          Rows (approx)
─────────────────────────────────────────────
mixed DISP for 1 day          ~2.0M (ORCA) + ~584K (SANPLAT) = ~2.6M
After Diário LEFT JOIN         ~1.97M × 2 origins = ~3.94M
After MEDIDORES INNER JOIN     ~3.80M (non-smart brands dropped)
After aggregate                ~400 municipalities × ≤3 brands ≈ ~1,200 rows
```

### Example Output Row

```
MUNICIPIO,INTELIGENTE,CONTAGEM_COMM,CONTAGEM_TOT,DISP,DATA
CURITIBA,Hexing,45230,52100,0.8682,2026-01-15
CURITIBA,Nansen,12400,15800,0.7848,2026-01-15
CURITIBA,Nansen Ipiranga,890,1020,0.8725,2026-01-15
LONDRINA,Hexing,8900,10200,0.8725,2026-01-15
...
```
