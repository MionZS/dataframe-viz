# Enrichment Guide

This guide explains how enriched communication files are generated and why they are required by the pipeline.

## Purpose

Enrichment converts relative day columns (for example `90..1` or `91..1`) into absolute date column names (`DD/MM/YYYY`).

The moving-window phase depends on these absolute date headers to select the correct `D-5 .. D-1` columns for each target day.

## Inputs and Outputs

### ORCA

- Input data: `data/raw/ORCA/Dados_Comunicacao.parquet`
- Reference date: `data/trusted/CIS/Data_Referencia.csv`
- Output enriched file: `data/trusted/ORCA/Dados_Comunicacao_com_datas.csv`

### SANPLAT

- Input data: `data/refined/SANPLAT/Dados_Comunicacao_SANPLAT.csv`
- Reference date: `data/trusted/SANPLAT/Data_Referencia_2.csv`
- Output enriched file: `data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv`

## How Enrichment Works

1. Read the reference date from the reference CSV.
2. Detect numeric columns in the communication file (`90..1` or `91..1`).
3. Treat max numeric column as `reference_date - 1`.
4. Rename each numeric column to an absolute date string (`DD/MM/YYYY`).
5. Save enriched CSV to the trusted path.

Important: Current implementation renames columns directly. It does not insert a secondary header row.

## Running Enrichment

### Option 1: Full pipeline (recommended)

Run the orchestrator; enrichment is phase 1 and runs automatically:

```bash
python src/pipeline_orchestrator.py
```

### Option 2: Direct enrichment script

Run enrichment only:

```bash
python src/enrich_dates.py --all
```

ORCA only:

```bash
python src/enrich_dates.py --orca
```

SANPLAT only:

```bash
python src/enrich_dates.py --sanplat
```

## Verifying Enriched Files

Check that date-named columns exist in output files:

- `data/trusted/ORCA/Dados_Comunicacao_com_datas.csv`
- `data/trusted/SANPLAT/Dados_Comunicacao_SANPLAT_com_datas.csv`

You should see columns like `25/01/2026`, `26/01/2026`, etc., plus `NIO`.

## Common Issues

### Missing reference file

If a reference CSV is missing, enrichment for that source is skipped.

### No numeric columns found

Input file must contain numeric relative-date columns (for example `90..1` or `91..1`).

### Wrong date mapping

Confirm the reference date file value and format (`YYYY-MM-DD`, `DD-MM-YYYY`, or `DD/MM/YYYY`).

## Why This Matters for Final Metrics

Without enrichment, moving-window cannot reliably identify `D-5 .. D-1` date columns.

That cascades into incorrect per-meter communication flags, which then affects:

- `CONTAGEM_COMM`
- `CONTAGEM_TOT`
- Final `DISP` by `[MUNICIPIO, INTELIGENTE]`
