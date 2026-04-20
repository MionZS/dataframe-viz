@echo off
REM Daily orchestrator - runs incremental computation and concatenation
REM
REM This script:
REM   1. Checks available Diario data by month
REM   2. Determines which months need computation (for full pipeline)
REM   3. Runs only missing months
REM   4. Concatenates all results
REM   5. Verifies output integrity
REM
REM Output: data/trusted/Indicador_comunicacao_full.csv
REM
REM This runs the FULL pipeline (with moving window).
REM For faster daily runs, use: daily_simple.bat (single-day lookback)

setlocal enabledelayedexpansion

cd /d d:\Projects\visualizer-tuis

echo.
echo ============================================================
echo Full Pipeline Orchestrator - Incremental Computation
echo ============================================================
echo.

REM Run daily orchestrator with full pipeline only
python -m src.daily_orchestrator config/pipeline.yaml "D:/Projects/visualizer-tuis/data/raw/CIS/Diario" data/trusted/municipio_daily "full" "D:/dados/OneDrive - copel.com/BIs Projetos Especiais - Documentos/General/Comunicação MIs/Fontes"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================================
    echo SUCCESS: Pipeline completed successfully
    echo Output: data/trusted/Indicador_comunicacao_full.csv
    echo ============================================================
) else (
    echo.
    echo ============================================================
    echo WARNING: Some operations completed with errors
    echo ============================================================
)
