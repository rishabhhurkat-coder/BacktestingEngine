from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import streamlit as st

try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
except ImportError:  # pragma: no cover - depends on environment packages
    service_account = None
    build = None
    HttpError = Exception

GOOGLE_DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive"]


@dataclass(frozen=True)
class GoogleDriveConfig:
    raw_folder_id: str
    input_folder_id: str
    output_folder_id: str
    service_account_info: dict[str, Any]
    shared_drive_id: str | None = None


@dataclass(frozen=True)
class GoogleDriveFolderInfo:
    folder_id: str
    name: str


@dataclass(frozen=True)
class GoogleDriveConnectionStatus:
    configured: bool
    connected: bool
    message: str
    raw_folder: GoogleDriveFolderInfo | None = None
    input_folder: GoogleDriveFolderInfo | None = None
    output_folder: GoogleDriveFolderInfo | None = None


def _normalize_secret_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(key): val for key, val in value.items()}
    try:
        return {str(key): val for key, val in dict(value).items()}
    except Exception:
        return {}


def _read_service_account_info(secret_mapping: dict[str, Any]) -> dict[str, Any] | None:
    raw_info = secret_mapping.get("service_account_info")
    if isinstance(raw_info, str):
        try:
            parsed = json.loads(raw_info)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None
    if isinstance(raw_info, dict):
        return raw_info
    try:
        return dict(raw_info) if raw_info is not None else None
    except Exception:
        return None


def load_google_drive_config() -> GoogleDriveConfig | None:
    try:
        secrets = st.secrets
    except Exception:
        return None

    try:
        secret_mapping = _normalize_secret_mapping(secrets.get("google_drive", {}))
    except Exception:
        return None
    if not secret_mapping:
        return None

    raw_folder_id = str(secret_mapping.get("raw_folder_id") or "").strip()
    input_folder_id = str(secret_mapping.get("input_folder_id") or "").strip()
    output_folder_id = str(secret_mapping.get("output_folder_id") or "").strip()
    shared_drive_id = str(secret_mapping.get("shared_drive_id") or "").strip() or None
    service_account_info = _read_service_account_info(secret_mapping)

    if not raw_folder_id or not input_folder_id or not output_folder_id or not service_account_info:
        return None

    return GoogleDriveConfig(
        raw_folder_id=raw_folder_id,
        input_folder_id=input_folder_id,
        output_folder_id=output_folder_id,
        shared_drive_id=shared_drive_id,
        service_account_info=service_account_info,
    )


@st.cache_resource(show_spinner=False)
def build_google_drive_service(config: GoogleDriveConfig):
    if service_account is None or build is None:
        raise RuntimeError("Google Drive packages are not installed yet.")
    credentials = service_account.Credentials.from_service_account_info(
        config.service_account_info,
        scopes=GOOGLE_DRIVE_SCOPES,
    )
    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _fetch_folder_info(service, folder_id: str) -> GoogleDriveFolderInfo:
    response = service.files().get(
        fileId=folder_id,
        fields="id,name,mimeType",
        supportsAllDrives=True,
    ).execute()
    mime_type = str(response.get("mimeType") or "")
    if mime_type != "application/vnd.google-apps.folder":
        raise ValueError(f"Item is not a folder: {folder_id}")
    return GoogleDriveFolderInfo(
        folder_id=str(response.get("id") or folder_id),
        name=str(response.get("name") or folder_id),
    )


@st.cache_data(show_spinner=False)
def get_google_drive_connection_status() -> GoogleDriveConnectionStatus:
    config = load_google_drive_config()
    if config is None:
        return GoogleDriveConnectionStatus(
            configured=False,
            connected=False,
            message="Google Drive is not configured in Streamlit secrets yet.",
        )

    try:
        service = build_google_drive_service(config)
        raw_folder = _fetch_folder_info(service, config.raw_folder_id)
        input_folder = _fetch_folder_info(service, config.input_folder_id)
        output_folder = _fetch_folder_info(service, config.output_folder_id)
    except HttpError as exc:
        return GoogleDriveConnectionStatus(
            configured=True,
            connected=False,
            message=f"Google Drive API error: {exc.status_code if hasattr(exc, 'status_code') else exc}",
        )
    except Exception as exc:
        return GoogleDriveConnectionStatus(
            configured=True,
            connected=False,
            message=f"Google Drive connection failed: {exc}",
        )

    return GoogleDriveConnectionStatus(
        configured=True,
        connected=True,
        message="Google Drive connection is ready.",
        raw_folder=raw_folder,
        input_folder=input_folder,
        output_folder=output_folder,
    )
