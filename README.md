# Smart Meter Communication Pipeline

Repository for the monthly consolidation pipeline that computes communication availability by municipality and smart-meter brand.

## What This Pipeline Produces

For each day in `target_month`, output rows grouped by:

- `MUNICIPIO`
- `INTELIGENTE` (brand: `Hexing`, `Nansen`, `Nansen Ipiranga`)

With metrics:

- `CONTAGEM_COMM` = communicating meters (`SUM(DISP)`)
- `CONTAGEM_TOT` = total meters (`COUNT(NIO)`)
- `DISP` = `CONTAGEM_COMM / CONTAGEM_TOT`

## Pipeline Phases

1. Enrichment (`src/enrich_dates.py`)
- Rename relative day columns (`90..1` / `91..1`) to absolute `DD/MM/YYYY`.

2. Moving Window (`src/moving_window.py`)
- Compute binary per-meter communication flag using 5-day OR logic.

3. Join & Aggregate (`src/join_daily.py`, `src/pipeline_orchestrator.py`)
- Merge ORCA + SANPLAT day DISP by `NIO`.
- One LEFT join with Diario (meter universe + `MUNICIPIO`).
- INNER join with MEDIDORES filtered to target brands.
- Aggregate by `[MUNICIPIO, INTELIGENTE]`.

## Key Paths

- Config: `config/pipeline.yaml`
- Main entry: `src/pipeline_orchestrator.py`
- Main outputs:
    - `data/trusted/municipio_daily/municipio_YYYY-MM.csv`
    - `data/trusted/municipio_daily/municipio_YYYY-MM.parquet`

## Run

```bash
python src/pipeline_orchestrator.py
```

Or with custom config path:

```bash
python src/pipeline_orchestrator.py config/pipeline.yaml
```

## Documentation

- Setup: `docs/SETUP.md`
- Pipeline guide: `docs/PIPELINE_GUIDE.md`
- Mermaid flow: `docs/PIPELINE_FLOW.md`
- Enrichment details: `docs/ENRICH_GUIDE.md`

