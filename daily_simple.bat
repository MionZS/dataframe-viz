@echo off
REM Daily run - simple pipeline only (faster, for daily use)
REM
REM This is a lightweight version for daily runs.
REM It runs the simplified pipeline only (skips moving window).
REM Perfect for scheduled daily execution when you just need fast updates.
REM
REM Output: data/trusted/Indicador_comunicacao_simple.csv

cd /d d:\Projects\visualizer-tuis

echo.
echo ============================================================
echo Daily Run - Simplified Pipeline Only
echo ============================================================
echo.

REM Run daily orchestrator with simple pipeline only
python -m src.daily_orchestrator config/pipeline.yaml "D:/Projects/visualizer-tuis/data/raw/CIS/Diario" data/trusted/municipio_daily "simple"

echo.
echo Done! Output: data/trusted/Indicador_comunicacao_simple.csv
echo.
