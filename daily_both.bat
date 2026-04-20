@echo off
REM Run BOTH simple and full pipelines
REM
REM This script:
REM   1. Checks available Diario data by month
REM   2. Determines which months need computation (for both pipelines)
REM   3. Runs only missing months for both pipelines
REM   4. Concatenates results for each pipeline
REM   5. Verifies both output files
REM
REM Outputs:
REM   - data/trusted/Indicador_comunicacao_simple.csv (single-day lookback)
REM   - data/trusted/Indicador_comunicacao_full.csv (5-day moving window)
REM
REM Use this for comprehensive analysis.
REM For daily use, prefer: daily_simple.bat (faster)

setlocal enabledelayedexpansion

cd /d d:\Projects\visualizer-tuis

echo.
echo ============================================================
echo Daily Orchestrator - BOTH Pipelines
echo ============================================================
echo.

REM Run daily orchestrator with both pipelines
python -m src.daily_orchestrator config/pipeline.yaml "D:/Projects/visualizer-tuis/data/raw/CIS/Diario" data/trusted/municipio_daily "simple,full" "D:/dados/OneDrive - copel.com/BIs Projetos Especiais - Documentos/General/Comunicação MIs/Fontes"

if %ERRORLEVEL% EQU 0 (
    echo.
    echo ============================================================
    echo SUCCESS: Both pipelines completed successfully
    echo.
    echo Outputs:
    echo   Simple: data/trusted/Indicador_comunicacao_simple.csv
    echo   Full:   data/trusted/Indicador_comunicacao_full.csv
    echo ============================================================
) else (
    echo.
    echo ============================================================
    echo WARNING: Some operations completed with errors
    echo ============================================================
)
