from __future__ import annotations

import json
import mimetypes
import os
import subprocess
import sys
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
VERSION_PATH = ROOT / "app_version.json"
PACKAGE_PATH = ROOT / "dist" / "EMA-200-Trades-Local-package.zip"
GIT_EXE = Path(r"C:\Program Files\Git\cmd\git.exe")
BUILD_SCRIPT = ROOT / "scripts" / "build_release_package.py"


def load_version_payload() -> dict:
    return json.loads(VERSION_PATH.read_text(encoding="utf-8"))


def github_request(url: str, token: str, *, method: str = "GET", data: bytes | None = None, headers: dict[str, str] | None = None) -> dict:
    request_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "EMA-200-Release-Publisher",
    }
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(url, data=data, method=method, headers=request_headers)
    with urllib.request.urlopen(request, timeout=60) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {}


def github_request_raw(url: str, token: str, *, method: str = "GET", data: bytes | None = None, headers: dict[str, str] | None = None) -> bytes:
    request_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "EMA-200-Release-Publisher",
    }
    if headers:
        request_headers.update(headers)
    request = urllib.request.Request(url, data=data, method=method, headers=request_headers)
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def run_git(args: list[str], workdir: Path) -> None:
    subprocess.run([str(GIT_EXE), *args], cwd=workdir, check=True)


def ensure_release(repo: str, tag_name: str, token: str) -> dict:
    try:
        return github_request(
            f"https://api.github.com/repos/{repo}/releases/tags/{urllib.parse.quote(tag_name)}",
            token,
        )
    except Exception:
        payload = {
            "tag_name": tag_name,
            "name": tag_name,
            "draft": False,
            "prerelease": False,
            "generate_release_notes": False,
        }
        return github_request(
            f"https://api.github.com/repos/{repo}/releases",
            token,
            method="POST",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )


def delete_existing_asset(repo: str, release: dict, asset_name: str, token: str) -> None:
    assets = release.get("assets") if isinstance(release.get("assets"), list) else []
    for asset in assets:
        if not isinstance(asset, dict):
            continue
        if str(asset.get("name") or "").strip() != asset_name:
            continue
        asset_id = asset.get("id")
        if not asset_id:
            continue
        github_request(
            f"https://api.github.com/repos/{repo}/releases/assets/{asset_id}",
            token,
            method="DELETE",
        )


def upload_asset(release: dict, package_path: Path, token: str) -> None:
    upload_url_template = str(release.get("upload_url") or "").strip()
    upload_url = upload_url_template.split("{", 1)[0]
    content_type = mimetypes.guess_type(package_path.name)[0] or "application/zip"
    params = urllib.parse.urlencode({"name": package_path.name})
    with package_path.open("rb") as source:
        github_request_raw(
            f"{upload_url}?{params}",
            token,
            method="POST",
            data=source.read(),
            headers={
                "Content-Type": content_type,
                "Accept": "application/vnd.github+json",
            },
        )


def main() -> None:
    token = str(os.environ.get("GITHUB_TOKEN") or "").strip()
    if not token:
        raise SystemExit("GITHUB_TOKEN is not set.")

    if not GIT_EXE.exists():
        raise SystemExit("Git is not installed at the expected path.")

    subprocess.run([sys.executable, str(BUILD_SCRIPT)], cwd=ROOT, check=True)

    if not PACKAGE_PATH.exists():
        raise SystemExit(f"Package not found: {PACKAGE_PATH}")

    payload = load_version_payload()
    github_config = payload.get("github") if isinstance(payload.get("github"), dict) else {}
    repo = str(github_config.get("repo") or "").strip()
    asset_name = str(github_config.get("release_asset_name") or PACKAGE_PATH.name).strip()
    version = str(payload.get("version") or "").strip()
    tag_name = f"v{version}"

    if not repo or not version:
        raise SystemExit("Repository or version info is missing.")

    release = ensure_release(repo, tag_name, token)
    delete_existing_asset(repo, release, asset_name, token)
    refreshed_release = ensure_release(repo, tag_name, token)
    upload_asset(refreshed_release, PACKAGE_PATH, token)
    print(f"Published {asset_name} to {repo} release {tag_name}")


if __name__ == "__main__":
    main()
