@echo off
cd /d "%~dp0"
set "PYTHONUTF8=1"
set "PYTHONIOENCODING=utf-8"
if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" "scripts\launch_streamlit.py"
) else (
    python "scripts\launch_streamlit.py"
)
