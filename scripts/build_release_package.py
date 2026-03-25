from __future__ import annotations

import datetime as dt
import json
import zipfile
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
DIST_DIR = ROOT / "dist"
VERSION_PATH = ROOT / "app_version.json"
PACKAGE_NAME = "EMA-200-Trades-Local-package.zip"
EXCLUDE_PARTS = {".venv", "__pycache__", "node_modules", "dist"}


def next_version() -> str:
    now = dt.datetime.now()
    return now.strftime("%Y.%m.%d.%H%M%S")


def load_version_payload() -> dict:
    if VERSION_PATH.exists():
        return json.loads(VERSION_PATH.read_text(encoding="utf-8"))
    return {
        "app_name": "EMA 200 Trades - Local",
        "version": "0.0.0",
        "github": {
            "repo": "rishabhhurkat-coder/BacktestingEngine",
            "release_asset_name": PACKAGE_NAME,
        },
    }


def build_zip(package_path: Path) -> None:
    with zipfile.ZipFile(package_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
        for path in sorted(ROOT.rglob("*")):
            if any(part in EXCLUDE_PARTS for part in path.parts):
                continue
            if path.is_dir():
                continue
            archive.write(path, path.relative_to(ROOT).as_posix())


def main() -> None:
    payload = load_version_payload()
    payload["version"] = next_version()
    github_config = payload.get("github") if isinstance(payload.get("github"), dict) else {}
    github_config["release_asset_name"] = PACKAGE_NAME
    payload["github"] = github_config
    VERSION_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    DIST_DIR.mkdir(parents=True, exist_ok=True)
    package_path = DIST_DIR / PACKAGE_NAME
    build_zip(package_path)
    print(f"Version: {payload['version']}")
    print(f"Package: {package_path}")


if __name__ == "__main__":
    main()
