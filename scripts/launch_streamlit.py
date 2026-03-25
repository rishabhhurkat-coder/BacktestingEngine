import subprocess
import sys
import time
import urllib.request
import webbrowser
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
APP_PATH = BASE_DIR / "streamlit_app.py"
VENV_PYTHON = BASE_DIR / ".venv" / "Scripts" / "python.exe"
APP_URL = "http://localhost:8501"
HEALTH_URL = f"{APP_URL}/_stcore/health"

python_executable = sys.executable
if VENV_PYTHON.exists() and Path(sys.executable).resolve() != VENV_PYTHON.resolve():
    python_executable = str(VENV_PYTHON)


def is_app_running() -> bool:
    try:
        with urllib.request.urlopen(HEALTH_URL, timeout=2) as response:
            return response.status == 200 and response.read().decode("utf-8", "ignore").strip().lower() == "ok"
    except Exception:
        return False


def open_browser() -> None:
    webbrowser.open(APP_URL)


def main() -> None:
    if is_app_running():
        open_browser()
        return

    creation_flags = 0
    if sys.platform.startswith("win"):
        creation_flags = (
            getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )

    cmd = [
        python_executable,
        "-m",
        "streamlit",
        "run",
        str(APP_PATH),
        "--server.address=localhost",
        "--server.port=8501",
        "--server.headless=true",
    ]
    subprocess.Popen(
        cmd,
        cwd=BASE_DIR,
        creationflags=creation_flags,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(30):
        time.sleep(1)
        if is_app_running():
            open_browser()
            return

    open_browser()


if __name__ == "__main__":
    main()
