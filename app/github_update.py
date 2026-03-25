from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path
from typing import Any


VERSION_FILE_NAME = "app_version.json"
DEFAULT_RELEASE_ASSET_NAME = "EMA-200-Trades-Local-package.zip"


def load_app_version_info(base_dir: Path) -> dict[str, Any]:
    version_path = Path(base_dir) / VERSION_FILE_NAME
    if not version_path.exists():
        return {
            "app_name": "EMA 200 Trades - Local",
            "version": "0.0.0",
            "github": {
                "repo": "",
                "release_asset_name": DEFAULT_RELEASE_ASSET_NAME,
            },
        }
    try:
        payload = json.loads(version_path.read_text(encoding="utf-8"))
    except Exception:
        return {
            "app_name": "EMA 200 Trades - Local",
            "version": "0.0.0",
            "github": {
                "repo": "",
                "release_asset_name": DEFAULT_RELEASE_ASSET_NAME,
            },
        }
    github_config = payload.get("github") if isinstance(payload.get("github"), dict) else {}
    payload["github"] = {
        "repo": str(github_config.get("repo") or "").strip(),
        "release_asset_name": str(
            github_config.get("release_asset_name") or DEFAULT_RELEASE_ASSET_NAME
        ).strip(),
    }
    payload["version"] = str(payload.get("version") or "0.0.0").strip()
    payload["app_name"] = str(payload.get("app_name") or "EMA 200 Trades - Local").strip()
    return payload


def save_app_version_info(base_dir: Path, payload: dict[str, Any]) -> None:
    version_path = Path(base_dir) / VERSION_FILE_NAME
    version_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _version_key(value: str) -> tuple[int, ...]:
    parts = [int(part) for part in re.findall(r"\d+", str(value or ""))]
    return tuple(parts) if parts else (0,)


def is_newer_version(current_version: str, candidate_version: str) -> bool:
    current_key = _version_key(current_version)
    candidate_key = _version_key(candidate_version)
    if candidate_key != current_key:
        return candidate_key > current_key
    return str(candidate_version or "").strip() != str(current_version or "").strip()


def fetch_latest_release_info(base_dir: Path, timeout: int = 5) -> dict[str, Any] | None:
    version_info = load_app_version_info(base_dir)
    github_config = version_info.get("github") if isinstance(version_info.get("github"), dict) else {}
    repo = str(github_config.get("repo") or "").strip()
    if not repo:
        return None
    asset_name = str(github_config.get("release_asset_name") or DEFAULT_RELEASE_ASSET_NAME).strip()
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/releases/latest",
        headers={
            "User-Agent": "EMA-200-Trades-Local-Updater",
            "Accept": "application/vnd.github+json",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))
    tag_name = str(payload.get("tag_name") or payload.get("name") or "").strip()
    version = tag_name.lstrip("vV").strip()
    assets = payload.get("assets") if isinstance(payload.get("assets"), list) else []
    chosen_asset = None
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        if str(asset.get("name") or "").strip() == asset_name:
            chosen_asset = asset
            break
    if chosen_asset is None:
        for asset in assets:
            if not isinstance(asset, dict):
                continue
            if str(asset.get("name") or "").strip().lower().endswith(".zip"):
                chosen_asset = asset
                break
    if chosen_asset is None:
        return None
    return {
        "repo": repo,
        "version": version or tag_name,
        "tag_name": tag_name,
        "asset_name": str(chosen_asset.get("name") or "").strip(),
        "asset_url": str(chosen_asset.get("browser_download_url") or "").strip(),
        "html_url": str(payload.get("html_url") or "").strip(),
        "published_at": str(payload.get("published_at") or "").strip(),
        "current_version": version_info.get("version") or "0.0.0",
    }


def launch_update_installer(base_dir: Path, asset_url: str, target_version: str) -> subprocess.Popen[Any]:
    helper_path = Path(base_dir) / "scripts" / "apply_github_update.py"
    venv_python = Path(base_dir) / ".venv" / "Scripts" / "python.exe"
    python_executable = str(venv_python) if venv_python.exists() else sys.executable
    creation_flags = 0
    if sys.platform.startswith("win"):
        creation_flags = (
            getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )
    return subprocess.Popen(
        [
            python_executable,
            str(helper_path),
            "--app-dir",
            str(base_dir),
            "--asset-url",
            str(asset_url),
            "--target-version",
            str(target_version),
            "--wait-pid",
            str(os.getpid()),
        ],
        cwd=base_dir,
        creationflags=creation_flags,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
