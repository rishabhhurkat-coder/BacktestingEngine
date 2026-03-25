from __future__ import annotations

import argparse
import hashlib
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import zipfile
from pathlib import Path
from tkinter import BOTH, LEFT, RIGHT, X, StringVar, Tk, ttk


APP_NAME = "EMA 200 Trades - Local"
LAUNCHER_FILE_NAME = "Run BackTestingEngine.bat"
PRESERVE_NAMES = {"Main Folder", ".venv"}
SHORTCUT_NAME = f"{APP_NAME}.lnk"
REQUIREMENTS_HASH_FILE = ".requirements.sha256"


class ProgressWindow:
    def __init__(self) -> None:
        self.root = Tk()
        self.root.title("Updating EMA 200 Trades - Local")
        self.root.geometry("520x150")
        self.root.resizable(False, False)
        self.root.attributes("-topmost", True)
        self.status_var = StringVar(value="Preparing update...")
        self.detail_var = StringVar(value="")
        container = ttk.Frame(self.root, padding=16)
        container.pack(fill=BOTH, expand=True)
        ttk.Label(container, text="EMA 200 Trades - Local", font=("Segoe UI", 14, "bold")).pack(anchor="w")
        ttk.Label(container, textvariable=self.status_var, font=("Segoe UI", 10)).pack(anchor="w", pady=(8, 4))
        self.progress = ttk.Progressbar(container, mode="determinate", maximum=100)
        self.progress.pack(fill=X, expand=True, pady=(2, 8))
        ttk.Label(container, textvariable=self.detail_var, font=("Segoe UI", 9)).pack(anchor="w")
        self.error: str | None = None

    def set_progress(self, value: float, status: str, detail: str = "") -> None:
        self.progress["value"] = max(0, min(100, value))
        self.status_var.set(status)
        self.detail_var.set(detail)
        self.root.update_idletasks()

    def show_error(self, message: str) -> None:
        self.error = message
        self.progress["value"] = 0
        self.status_var.set("Update failed")
        self.detail_var.set(message)
        self.root.update_idletasks()

    def close(self) -> None:
        self.root.destroy()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-dir", required=True)
    parser.add_argument("--asset-url", required=True)
    parser.add_argument("--target-version", required=True)
    parser.add_argument("--wait-pid", required=True, type=int)
    return parser.parse_args()


def wait_for_process_to_exit(pid: int, timeout_seconds: int = 12) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if str(pid) not in (result.stdout or ""):
            return
        time.sleep(0.5)
    subprocess.run(
        ["taskkill", "/PID", str(pid), "/T", "/F"],
        capture_output=True,
        text=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    time.sleep(1)


def download_asset(asset_url: str, destination: Path, window: ProgressWindow) -> None:
    request = urllib.request.Request(
        asset_url,
        headers={"User-Agent": "EMA-200-Trades-Local-Updater"},
    )
    with urllib.request.urlopen(request, timeout=30) as response, destination.open("wb") as target:
        total_length = int(response.headers.get("Content-Length", "0") or 0)
        downloaded = 0
        while True:
            chunk = response.read(1024 * 256)
            if not chunk:
                break
            target.write(chunk)
            downloaded += len(chunk)
            if total_length > 0:
                percent = downloaded / total_length
                window.set_progress(
                    10 + (percent * 35),
                    "Downloading update...",
                    f"{downloaded // 1024} KB of {total_length // 1024} KB",
                )


def file_sha256(file_path: Path) -> str:
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def copy_updated_files(source_dir: Path, target_dir: Path, window: ProgressWindow) -> None:
    all_paths = [path for path in source_dir.rglob("*") if path.name not in {"__pycache__"}]
    total = max(1, len(all_paths))
    processed = 0
    for path in all_paths:
        relative = path.relative_to(source_dir)
        if relative.parts and relative.parts[0] in PRESERVE_NAMES:
            processed += 1
            continue
        destination = target_dir / relative
        if path.is_dir():
            destination.mkdir(parents=True, exist_ok=True)
        else:
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)
        processed += 1
        window.set_progress(
            50 + ((processed / total) * 25),
            "Installing files...",
            relative.as_posix(),
        )


def ensure_shortcut(app_dir: Path) -> None:
    shortcut_path = Path.home() / "Desktop" / SHORTCUT_NAME
    icon_path = app_dir / "assets" / "ema_200_trades_local.ico"
    target_path = app_dir / LAUNCHER_FILE_NAME
    shortcut_text = str(shortcut_path).replace("'", "''")
    target_text = str(target_path).replace("'", "''")
    app_dir_text = str(app_dir).replace("'", "''")
    icon_text = str(icon_path).replace("'", "''")
    powershell = (
        "$shell = New-Object -ComObject WScript.Shell; "
        f"$shortcut = $shell.CreateShortcut('{shortcut_text}'); "
        f"$shortcut.TargetPath = '{target_text}'; "
        f"$shortcut.WorkingDirectory = '{app_dir_text}'; "
        + (
            f"$shortcut.IconLocation = '{icon_text},0'; "
            if icon_path.exists()
            else ""
        )
        + "$shortcut.Save()"
    )
    subprocess.run(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", powershell],
        check=False,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )


def sync_python_dependencies(app_dir: Path, window: ProgressWindow) -> None:
    venv_python = app_dir / ".venv" / "Scripts" / "python.exe"
    python_executable = str(venv_python) if venv_python.exists() else sys.executable
    requirements_path = app_dir / "requirements.txt"
    if not requirements_path.exists():
        return
    requirements_hash = file_sha256(requirements_path)
    requirements_hash_path = app_dir / REQUIREMENTS_HASH_FILE
    previous_hash = (
        requirements_hash_path.read_text(encoding="utf-8").strip()
        if requirements_hash_path.exists()
        else ""
    )
    if venv_python.exists() and previous_hash == requirements_hash:
        window.set_progress(78, "Reusing existing Python packages...", requirements_path.name)
        return

    window.set_progress(78, "Updating Python packages...", requirements_path.name)
    subprocess.run(
        [python_executable, "-m", "pip", "install", "-r", str(requirements_path)],
        check=True,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    requirements_hash_path.write_text(requirements_hash, encoding="utf-8")


def cache_package_copy(app_dir: Path, package_path: Path, asset_url: str) -> None:
    asset_name = Path(urllib.parse.urlparse(asset_url).path).name or package_path.name
    dist_dir = app_dir / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(package_path, dist_dir / asset_name)


def main() -> int:
    args = parse_args()
    app_dir = Path(args.app_dir).resolve()
    asset_url = str(args.asset_url)
    wait_pid = int(args.wait_pid)

    temp_dir = Path(tempfile.mkdtemp(prefix="ema200-update-"))
    zip_path = temp_dir / "update.zip"
    extract_dir = temp_dir / "extract"
    window = ProgressWindow()

    def worker() -> None:
        try:
            window.set_progress(2, "Closing running app...", "Preparing updater")
            wait_for_process_to_exit(wait_pid)
            window.set_progress(10, "Downloading update...", args.target_version)
            download_asset(asset_url, zip_path, window)
            cache_package_copy(app_dir, zip_path, asset_url)
            window.set_progress(47, "Extracting update...", zip_path.name)
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, "r") as archive:
                archive.extractall(extract_dir)
            root_entries = list(extract_dir.iterdir())
            source_dir = (
                root_entries[0]
                if len(root_entries) == 1 and root_entries[0].is_dir()
                else extract_dir
            )
            copy_updated_files(source_dir, app_dir, window)

            sync_python_dependencies(app_dir, window)

            window.set_progress(92, "Refreshing shortcut...", SHORTCUT_NAME)
            ensure_shortcut(app_dir)
            launcher = app_dir / LAUNCHER_FILE_NAME
            window.set_progress(98, "Restarting app...", launcher.name)
            if launcher.exists():
                subprocess.Popen(
                    [str(launcher)],
                    cwd=app_dir,
                    creationflags=getattr(subprocess, "DETACHED_PROCESS", 0)
                    | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            window.set_progress(100, "Update complete", f"Version {args.target_version}")
            time.sleep(1.2)
            shutil.rmtree(temp_dir, ignore_errors=True)
            window.root.after(0, window.close)
        except Exception as exc:
            window.show_error(str(exc))

    threading.Thread(target=worker, daemon=True).start()
    window.root.mainloop()
    return 1 if window.error else 0


if __name__ == "__main__":
    raise SystemExit(main())
