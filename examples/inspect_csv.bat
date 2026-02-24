@echo off
REM Quick start script for Data Inspector (Windows)

setlocal enabledelayedexpansion

REM Get the directory of this script
for %%I in ("%~dp0.") do set SCRIPT_DIR=%%~fI

REM Activate virtual environment if it exists
if exist "%SCRIPT_DIR%\..\venv\Scripts\activate.bat" (
    call "%SCRIPT_DIR%\..\venv\Scripts\activate.bat"
) else if exist "%SCRIPT_DIR%\venv\Scripts\activate.bat" (
    call "%SCRIPT_DIR%\venv\Scripts\activate.bat"
)

REM Run the inspector
python "%SCRIPT_DIR%\..\visualizers\data_inspector.py" %*

endlocal
