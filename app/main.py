from __future__ import annotations

import calendar
import html
from io import BytesIO
import shutil
import subprocess
import sys
import time
import traceback
import urllib.request
from pathlib import Path
import tempfile
from typing import Any
from uuid import uuid4

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import streamlit as st
from plotly.subplots import make_subplots
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import Image, LongTable, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from .component import tv_chart_component, build_dir
from .data_pipeline import extract_symbol, process_raw_folder
from .google_drive import (
    download_google_drive_files_to_dir,
    get_google_drive_connection_status,
    list_google_drive_folder_files,
    upload_google_drive_file,
)

BASE_DIR = Path(__file__).resolve().parent.parent
APP_ENTRY = BASE_DIR / "streamlit_app.py"

if __name__ == "__main__" and "streamlit.web.bootstrap" not in sys.modules:
    creation_flags = 0
    if sys.platform.startswith("win"):
        creation_flags = (
            getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )

    subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", str(APP_ENTRY)],
        cwd=BASE_DIR,
        creationflags=creation_flags,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    raise SystemExit(0)

DATA_DIR = BASE_DIR

BUY_COLOR = "#089981"
SELL_COLOR = "#f23645"
BUY_DARK_COLOR = "#0b7a5a"
SELL_DARK_COLOR = "#b91c1c"
EMA_COLOR = "#2962ff"
GRID_COLOR = "rgba(209, 213, 219, 0.55)"
SIDEBAR_BG = "#f6f8fb"
MAX_CANDLES = 3000
DEFAULT_RECENT_DAYS = 5
TIMEFRAME_TEXT = "3 minute candles"
TIMEFRAME_LABEL = "3m"
SESSION_START = "09.15"
SESSION_END = "15.27"
CHART_HEIGHT = 700
SUPPORTED_DATA_EXTENSIONS = (".csv", ".xlsx", ".xlsm", ".xlsb")
SAVED_SIGNAL_COLUMNS = [
    "Symbol",
    "Date",
    "Time",
    "Open",
    "High",
    "Low",
    "Close",
    "EMA",
    "Signal",
    "Qty",
    "TimeChart",
    "TimeEpoch",
    "SignalKey",
]
SAVED_SIGNAL_REQUIRED_COLUMNS = [
    "Symbol",
    "Date",
    "Time",
    "Open",
    "High",
    "Low",
    "Close",
    "EMA",
    "Signal",
    "Qty",
]


def normalize_time(value: Any) -> str:
    text = str(value).strip().replace(":", ".")
    parts = text.split(".")
    if len(parts) != 2:
        raise ValueError(f"Invalid time value: {value}")

    hour = int(parts[0])
    minute_text = parts[1]
    minute = int(minute_text)

    if len(minute_text) == 1:
        minute *= 10

    return f"{hour:02d}.{minute:02d}"


def time_to_minutes(value: str) -> int:
    hour_text, minute_text = normalize_time(value).split(".")
    return int(hour_text) * 60 + int(minute_text)


def month_start(value: Any):
    timestamp = pd.Timestamp(value)
    return timestamp.replace(day=1).date()


def month_end(value: Any):
    timestamp = pd.Timestamp(value)
    last_day = calendar.monthrange(timestamp.year, timestamp.month)[1]
    return timestamp.replace(day=last_day).date()


def next_month_end(value: Any):
    timestamp = pd.Timestamp(value)
    if timestamp.month == 12:
        year = timestamp.year + 1
        month = 1
    else:
        year = timestamp.year
        month = timestamp.month + 1
    last_day = calendar.monthrange(year, month)[1]
    return pd.Timestamp(year=year, month=month, day=last_day).date()


def compute_chart_window_end(start_date: Any, limit_date: Any):
    start = pd.Timestamp(start_date).date()
    limit = pd.Timestamp(limit_date).date()
    if start == month_start(start):
        return min(limit, month_end(start))
    return min(limit, next_month_end(start))


SESSION_START_MINUTES = time_to_minutes(SESSION_START)
SESSION_END_MINUTES = time_to_minutes(SESSION_END)


def is_supported_data_file(file_path: Path) -> bool:
    return file_path.is_file() and file_path.suffix.lower() in SUPPORTED_DATA_EXTENSIONS


def list_supported_data_files(folder: Path) -> list[Path]:
    if not folder.exists():
        return []
    return [
        file_path
        for file_path in sorted(folder.iterdir())
        if is_supported_data_file(file_path)
    ]


def filter_supported_google_drive_files(file_infos: list[Any]) -> list[Any]:
    supported_files: list[Any] = []
    for file_info in file_infos:
        suffix = Path(str(getattr(file_info, "name", ""))).suffix.lower()
        if suffix in SUPPORTED_DATA_EXTENSIONS:
            supported_files.append(file_info)
    return supported_files


def group_google_drive_files_by_symbol(file_infos: list[Any]) -> dict[str, list[Any]]:
    grouped_files: dict[str, list[Any]] = {}
    for file_info in file_infos:
        symbol = extract_symbol(str(getattr(file_info, "name", "")))
        grouped_files.setdefault(symbol, []).append(file_info)
    return dict(sorted(grouped_files.items(), key=lambda item: item[0].lower()))


def display_symbol(symbol: Any) -> str:
    return str(symbol or "").upper()


def trigger_drive_process_dialog() -> None:
    choice = st.session_state.get("drive_process_choice_widget")
    if choice == "Yes":
        st.session_state.show_drive_process_dialog = True
    elif choice == "No":
        st.session_state.drive_input_sync_choice = None


def read_tabular_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    return read_tabular_source(file_path, suffix)


def read_tabular_source(source: Any, suffix: str) -> pd.DataFrame:
    if suffix == ".csv":
        return pd.read_csv(source)
    if suffix in {".xlsx", ".xlsm"}:
        return pd.read_excel(source)
    if suffix == ".xlsb":
        return pd.read_excel(source, engine="pyxlsb")
    raise ValueError(f"Unsupported file type: {suffix}")


def write_tabular_file(df: pd.DataFrame, file_path: Path) -> None:
    file_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        df.to_csv(file_path, index=False)
        return
    if suffix in {".xlsx", ".xlsm"}:
        df.to_excel(file_path, index=False)
        return
    raise ValueError(f"Unsupported file type: {file_path.suffix}")


def find_data_file_by_stem(folder: Path, stem: str) -> Path | None:
    stem_lookup = stem.casefold()
    for file_path in list_supported_data_files(folder):
        if file_path.stem.casefold() == stem_lookup:
            return file_path
    return None


def csv_path_for_stem(folder: Path, stem: str) -> Path:
    return folder / f"{stem}.csv"


def remove_other_matching_data_files(folder: Path, stem: str, keep_path: Path) -> None:
    stem_lookup = stem.casefold()
    keep_name = keep_path.name.casefold()
    for file_path in list_supported_data_files(folder):
        if file_path.stem.casefold() != stem_lookup:
            continue
        if file_path.name.casefold() == keep_name:
            continue
        try:
            file_path.unlink()
        except OSError:
            continue


def tabular_mime_type(file_path: Path) -> str:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return "text/csv"
    if suffix == ".xlsb":
        return "application/vnd.ms-excel.sheet.binary.macroEnabled.12"
    return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


def normalize_chart_replay_state(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"active": False, "index": None, "showStartLine": False}

    active = bool(value.get("active"))
    raw_index = value.get("index")
    try:
        index = None if raw_index is None else max(0, int(raw_index))
    except (TypeError, ValueError):
        index = None

    show_start_line = active and bool(value.get("showStartLine"))
    return {
        "active": active,
        "index": index if active else None,
        "showStartLine": show_start_line,
    }


@st.cache_data(show_spinner=False)
def list_symbols(data_dir: str) -> dict[str, str]:
    folder = Path(data_dir)
    return {file_path.stem: str(file_path) for file_path in list_supported_data_files(folder)}


def resolve_data_dir(path_value: str) -> Path:
    raw_path = str(path_value or "").strip().strip('"').strip("'")
    if not raw_path:
        return DATA_DIR

    folder = Path(raw_path).expanduser()
    if not folder.is_absolute():
        folder = BASE_DIR / folder

    return folder.resolve()


def resolve_main_workspace_dir(path_value: str) -> Path:
    folder = resolve_data_dir(path_value)
    if folder.name in {"Raw Files", "Input Files", "Output Files"}:
        return folder.parent
    return folder


def browse_for_folder(current_path: str) -> str | None:
    if str(current_path or "").strip():
        initial_dir = resolve_data_dir(current_path)
    else:
        initial_dir = BASE_DIR
    if not initial_dir.exists():
        initial_dir = BASE_DIR

    if sys.platform.startswith("win"):
        escaped_path = str(initial_dir).replace("'", "''")
        script = f"""
Add-Type -TypeDefinition @'
using System;
using System.IO;
using System.Runtime.InteropServices;

[Flags]
public enum FileOpenDialogOptions : uint
{{
    FOS_OVERWRITEPROMPT = 0x00000002,
    FOS_STRICTFILETYPES = 0x00000004,
    FOS_NOCHANGEDIR = 0x00000008,
    FOS_PICKFOLDERS = 0x00000020,
    FOS_FORCEFILESYSTEM = 0x00000040,
    FOS_ALLNONSTORAGEITEMS = 0x00000080,
    FOS_NOVALIDATE = 0x00000100,
    FOS_ALLOWMULTISELECT = 0x00000200,
    FOS_PATHMUSTEXIST = 0x00000800,
    FOS_FILEMUSTEXIST = 0x00001000,
    FOS_CREATEPROMPT = 0x00002000,
    FOS_SHAREAWARE = 0x00004000,
    FOS_NOREADONLYRETURN = 0x00008000,
    FOS_NOTESTFILECREATE = 0x00010000,
    FOS_HIDEMRUPLACES = 0x00020000,
    FOS_HIDEPINNEDPLACES = 0x00040000,
    FOS_NODEREFERENCELINKS = 0x00100000,
    FOS_OKBUTTONNEEDSINTERACTION = 0x00200000,
    FOS_DONTADDTORECENT = 0x02000000,
    FOS_FORCESHOWHIDDEN = 0x10000000,
    FOS_DEFAULTNOMINIMODE = 0x20000000,
    FOS_FORCEPREVIEWPANEON = 0x40000000
}}

public enum SIGDN : uint
{{
    FILESYSPATH = 0x80058000
}}

[ComImport]
[Guid("42f85136-db7e-439c-85f1-e4075d135fc8")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
public interface IFileDialog
{{
    [PreserveSig] int Show(IntPtr parent);
    [PreserveSig] int SetFileTypes(uint cFileTypes, IntPtr rgFilterSpec);
    [PreserveSig] int SetFileTypeIndex(uint iFileType);
    [PreserveSig] int GetFileTypeIndex(out uint piFileType);
    [PreserveSig] int Advise(IntPtr pfde, out uint pdwCookie);
    [PreserveSig] int Unadvise(uint dwCookie);
    [PreserveSig] int SetOptions(FileOpenDialogOptions fos);
    [PreserveSig] int GetOptions(out FileOpenDialogOptions pfos);
    [PreserveSig] int SetDefaultFolder(IShellItem psi);
    [PreserveSig] int SetFolder(IShellItem psi);
    [PreserveSig] int GetFolder(out IShellItem ppsi);
    [PreserveSig] int GetCurrentSelection(out IShellItem ppsi);
    [PreserveSig] int SetFileName([MarshalAs(UnmanagedType.LPWStr)] string pszName);
    [PreserveSig] int GetFileName([MarshalAs(UnmanagedType.LPWStr)] out string pszName);
    [PreserveSig] int SetTitle([MarshalAs(UnmanagedType.LPWStr)] string pszTitle);
    [PreserveSig] int SetOkButtonLabel([MarshalAs(UnmanagedType.LPWStr)] string pszText);
    [PreserveSig] int SetFileNameLabel([MarshalAs(UnmanagedType.LPWStr)] string pszLabel);
    [PreserveSig] int GetResult(out IShellItem ppsi);
    [PreserveSig] int AddPlace(IShellItem psi, uint fdap);
    [PreserveSig] int SetDefaultExtension([MarshalAs(UnmanagedType.LPWStr)] string pszDefaultExtension);
    [PreserveSig] int Close(int hr);
    [PreserveSig] int SetClientGuid(ref Guid guid);
    [PreserveSig] int ClearClientData();
    [PreserveSig] int SetFilter(IntPtr pFilter);
}}

[ComImport]
[Guid("d57c7288-d4ad-4768-be02-9d969532d960")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
public interface IFileOpenDialog : IFileDialog
{{
    [PreserveSig] new int Show(IntPtr parent);
    [PreserveSig] new int SetFileTypes(uint cFileTypes, IntPtr rgFilterSpec);
    [PreserveSig] new int SetFileTypeIndex(uint iFileType);
    [PreserveSig] new int GetFileTypeIndex(out uint piFileType);
    [PreserveSig] new int Advise(IntPtr pfde, out uint pdwCookie);
    [PreserveSig] new int Unadvise(uint dwCookie);
    [PreserveSig] new int SetOptions(FileOpenDialogOptions fos);
    [PreserveSig] new int GetOptions(out FileOpenDialogOptions pfos);
    [PreserveSig] new int SetDefaultFolder(IShellItem psi);
    [PreserveSig] new int SetFolder(IShellItem psi);
    [PreserveSig] new int GetFolder(out IShellItem ppsi);
    [PreserveSig] new int GetCurrentSelection(out IShellItem ppsi);
    [PreserveSig] new int SetFileName([MarshalAs(UnmanagedType.LPWStr)] string pszName);
    [PreserveSig] new int GetFileName([MarshalAs(UnmanagedType.LPWStr)] out string pszName);
    [PreserveSig] new int SetTitle([MarshalAs(UnmanagedType.LPWStr)] string pszTitle);
    [PreserveSig] new int SetOkButtonLabel([MarshalAs(UnmanagedType.LPWStr)] string pszText);
    [PreserveSig] new int SetFileNameLabel([MarshalAs(UnmanagedType.LPWStr)] string pszLabel);
    [PreserveSig] new int GetResult(out IShellItem ppsi);
    [PreserveSig] new int AddPlace(IShellItem psi, uint fdap);
    [PreserveSig] new int SetDefaultExtension([MarshalAs(UnmanagedType.LPWStr)] string pszDefaultExtension);
    [PreserveSig] new int Close(int hr);
    [PreserveSig] new int SetClientGuid(ref Guid guid);
    [PreserveSig] new int ClearClientData();
    [PreserveSig] new int SetFilter(IntPtr pFilter);
    [PreserveSig] int GetResults(out IntPtr ppenum);
    [PreserveSig] int GetSelectedItems(out IntPtr ppsai);
}}

[ComImport]
[Guid("43826d1e-e718-42ee-bc55-a1e261c37bfe")]
[InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
public interface IShellItem
{{
    [PreserveSig] int BindToHandler(IntPtr pbc, ref Guid bhid, ref Guid riid, out IntPtr ppv);
    [PreserveSig] int GetParent(out IShellItem ppsi);
    [PreserveSig] int GetDisplayName(SIGDN sigdnName, [MarshalAs(UnmanagedType.LPWStr)] out string ppszName);
    [PreserveSig] int GetAttributes(uint sfgaoMask, out uint psfgaoAttribs);
    [PreserveSig] int Compare(IShellItem psi, uint hint, out int piOrder);
}}

[ComImport]
[Guid("DC1C5A9C-E88A-4DDE-A5A1-60F82A20AEF7")]
public class FileOpenDialogRCW
{{
}}

public static class NativeMethods
{{
    [DllImport("shell32.dll", CharSet = CharSet.Unicode, PreserveSig = false)]
    public static extern void SHCreateItemFromParsingName(
        [MarshalAs(UnmanagedType.LPWStr)] string pszPath,
        IntPtr pbc,
        ref Guid riid,
        [MarshalAs(UnmanagedType.Interface)] out IShellItem ppv
    );
}}

public static class ExplorerFolderPicker
{{
    public static string Show(string initialPath, string title)
    {{
        IFileOpenDialog dialog = null;
        IShellItem initialItem = null;
        IShellItem resultItem = null;

        try
        {{
            dialog = (IFileOpenDialog)new FileOpenDialogRCW();
            FileOpenDialogOptions options;
            dialog.GetOptions(out options);
            options |= FileOpenDialogOptions.FOS_PICKFOLDERS;
            options |= FileOpenDialogOptions.FOS_FORCEFILESYSTEM;
            options |= FileOpenDialogOptions.FOS_PATHMUSTEXIST;
            options |= FileOpenDialogOptions.FOS_DONTADDTORECENT;
            dialog.SetOptions(options);
            dialog.SetTitle(title);
            dialog.SetOkButtonLabel("Select Folder");

            if (!string.IsNullOrWhiteSpace(initialPath) && Directory.Exists(initialPath))
            {{
                Guid shellItemGuid = typeof(IShellItem).GUID;
                NativeMethods.SHCreateItemFromParsingName(initialPath, IntPtr.Zero, ref shellItemGuid, out initialItem);
                dialog.SetFolder(initialItem);
                dialog.SetDefaultFolder(initialItem);
            }}

            const int ERROR_CANCELLED = unchecked((int)0x800704C7);
            int hr = dialog.Show(IntPtr.Zero);
            if (hr == ERROR_CANCELLED)
            {{
                return null;
            }}
            if (hr != 0)
            {{
                Marshal.ThrowExceptionForHR(hr);
            }}

            dialog.GetResult(out resultItem);
            if (resultItem == null)
            {{
                return null;
            }}

            string selectedPath;
            resultItem.GetDisplayName(SIGDN.FILESYSPATH, out selectedPath);
            return selectedPath;
        }}
        finally
        {{
            if (resultItem != null) Marshal.ReleaseComObject(resultItem);
            if (initialItem != null) Marshal.ReleaseComObject(initialItem);
            if (dialog != null) Marshal.ReleaseComObject(dialog);
        }}
    }}
}}
'@ -Language CSharp
$selected = [ExplorerFolderPicker]::Show('{escaped_path}', 'Select folder containing CSV files')
if ($selected) {{
    Write-Output $selected
}}
""".strip()
        try:
            result = subprocess.run(
                ["powershell", "-NoProfile", "-STA", "-Command", script],
                capture_output=True,
                text=True,
                check=False,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except Exception:
            result = None

        if result is not None and result.returncode == 0:
            selected = result.stdout.strip()
            return selected or None

    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception:
        return None

    root = tk.Tk()
    root.withdraw()
    root.attributes("-topmost", True)
    root.update()
    try:
        selected = filedialog.askdirectory(
            parent=root,
            initialdir=str(initial_dir),
            title="Select folder containing CSV files",
        )
    finally:
        root.destroy()

    return selected or None


def can_write_to_directory(folder: Path) -> bool:
    try:
        folder.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=folder, prefix=".write_test_", delete=True):
            pass
    except Exception:
        return False
    return True


def ensure_workspace_dirs(main_dir: Path) -> tuple[Path, Path, Path]:
    raw_dir = main_dir / "Raw Files"
    input_dir = main_dir / "Input Files"
    output_dir = main_dir / "Output Files"
    raw_dir.mkdir(parents=True, exist_ok=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return raw_dir, input_dir, output_dir


def clear_supported_data_files(folder: Path) -> None:
    for file_path in list_supported_data_files(folder):
        try:
            file_path.unlink()
        except OSError:
            continue


def build_upload_signature(uploaded_files: list[Any]) -> tuple[tuple[str, int], ...]:
    signature: list[tuple[str, int]] = []
    for uploaded_file in uploaded_files:
        file_size = getattr(uploaded_file, "size", None)
        signature.append((str(uploaded_file.name), int(file_size or 0)))
    return tuple(sorted(signature))


def normalize_processed_input_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    required = ["Date", "Time", "Open", "High", "Low", "Close", "EMA"]
    safe_df = raw_df.copy()
    safe_df.columns = safe_df.columns.str.strip()
    missing = [column for column in required if column not in safe_df.columns]
    if missing:
        raise ValueError(f"Missing required processed-input columns: {', '.join(missing)}")

    safe_df = safe_df.loc[:, required].copy()
    safe_df["Date"] = safe_df["Date"].astype(str).str.strip()
    safe_df["Time"] = safe_df["Time"].map(normalize_time)
    numeric_columns = ["Open", "High", "Low", "Close", "EMA"]
    safe_df[numeric_columns] = safe_df[numeric_columns].apply(pd.to_numeric, errors="coerce")
    safe_df["DateObj"] = pd.to_datetime(safe_df["Date"], format="%d-%b-%y", errors="coerce")
    safe_df = safe_df.dropna(subset=["DateObj", "Time", *numeric_columns])
    safe_df = safe_df.sort_values(["DateObj", "Time"], kind="stable")
    safe_df = safe_df.drop_duplicates(subset=["Date", "Time"], keep="last").reset_index(drop=True)
    return safe_df


def build_trade_table_download_bytes(
    saved_signals: list[dict[str, Any]],
    symbol: str,
    default_qty: int,
) -> bytes | None:
    trade_df = build_saved_signals_trade_table(
        saved_signals,
        symbol=symbol,
        default_qty=default_qty,
    )
    if trade_df.empty:
        return None

    trade_df.insert(0, "Scrip", symbol)

    export_columns = [
        "Scrip",
        "Sr.No",
        "Date",
        "Time",
        "Trade",
        "Price",
        "Entry Date",
        "Entry Time",
        "Entry Price",
        "Exit Date",
        "Exit Time",
        "Exit Price",
        "Qty",
        "PL Points",
        "PL Amt",
        "Candle Analysis",
    ]
    export_df = trade_df.loc[:, [col for col in export_columns if col in trade_df.columns]].copy()
    return export_df.to_csv(index=False).encode("utf-8")


def update_trade_data_in_google_drive(
    drive_status: Any,
    symbol: str,
    trade_data_bytes: bytes | None,
) -> tuple[str, str, dict[str, Any] | None]:
    if trade_data_bytes is None:
        return "warning", f"No trade data is available to update for {display_symbol(symbol)}.", None
    if not getattr(drive_status, "connected", False) or getattr(drive_status, "output_folder", None) is None:
        return "error", "Google Drive Output Files are not connected yet.", None

    file_name = f"{symbol}.csv"
    list_google_drive_folder_files.clear()
    existing_drive_files = {
        file_info.name.casefold()
        for file_info in filter_supported_google_drive_files(
            list_google_drive_folder_files(drive_status.output_folder.folder_id)
        )
    }
    if file_name.casefold() not in existing_drive_files:
        return (
            "warning",
            f"{display_symbol(symbol)} does not exist yet in Google Drive Output Files. Download it below and add it manually.",
            {
                "file_name": file_name,
                "data": trade_data_bytes,
                "mime": "text/csv",
            },
        )

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            upload_google_drive_file(
                folder_id=drive_status.output_folder.folder_id,
                file_name=file_name,
                content=trade_data_bytes,
                mime_type="text/csv",
            )
            list_google_drive_folder_files.clear()
            return "success", f"Updated Google Drive Output Files for {display_symbol(symbol)}.", None
        except Exception as exc:
            last_error = exc
            error_text = str(exc)
            retryable_error = any(
                token in error_text
                for token in (
                    "SSL:",
                    "RECORD_LAYER_FAILURE",
                    "Connection reset",
                    "EOF occurred",
                    "timed out",
                    "Timeout",
                    "temporarily unavailable",
                )
            )
            if retryable_error and attempt < 2:
                time.sleep(1.2 * (attempt + 1))
                continue
            break

    return "error", f"Could not update Google Drive Output Files for {display_symbol(symbol)}: {last_error}", None


@st.dialog("Process Drive Raw Files", width="large")
def render_drive_process_dialog(
    symbol_names: list[str],
    symbol_files: dict[str, list[Any]],
    main_dir: Path,
    drive_input_folder_id: str,
) -> None:
    try:
        st.caption("Select one or more scrips from Google Drive Raw Files to process into Input Files.")
        st.multiselect(
            "Select Drive Scrips to Process",
            symbol_names,
            key="drive_selected_symbols",
            format_func=display_symbol,
            help="Only the selected scrips will be processed from Google Drive Raw Files.",
        )

        feedback_level = st.session_state.get("drive_dialog_feedback_level")
        feedback_message = str(st.session_state.get("drive_dialog_feedback_message") or "").strip()
        if feedback_level and feedback_message:
            feedback_fn = {
                "success": st.success,
                "warning": st.warning,
                "error": st.error,
            }.get(feedback_level, st.info)
            feedback_fn(feedback_message)

        manual_downloads = st.session_state.get("drive_manual_input_downloads") or []
        if manual_downloads:
            st.markdown("**Manual Input File Downloads**")
            st.caption("These processed CSV files could not be created automatically in Google Drive. Download them and add them manually to Drive Input Files.")
            for item in manual_downloads:
                file_name = str(item.get("file_name") or "processed_input.csv")
                file_bytes = item.get("data") or b""
                mime_type = str(item.get("mime") or "text/csv")
                st.download_button(
                    f"Download {file_name}",
                    data=file_bytes,
                    file_name=file_name,
                    mime=mime_type,
                    width="stretch",
                    key=f"drive-dialog-download-{file_name}",
                )

        process_col, cancel_col = st.columns(2, gap="small")
        with process_col:
            if st.button("Process Selected Drive Scrips", width="stretch"):
                _, input_dir, output_dir = ensure_workspace_dirs(main_dir)
                try:
                    level, message, manual_downloads = process_selected_drive_raw_symbols(
                        selected_symbols=list(st.session_state.get("drive_selected_symbols", [])),
                        symbol_files=symbol_files,
                        input_dir=input_dir,
                        drive_input_folder_id=drive_input_folder_id,
                    )
                except Exception as exc:
                    level, message, manual_downloads = "error", f"Google Drive processing failed: {exc}", []
                st.session_state.drive_dialog_feedback_level = level
                st.session_state.drive_dialog_feedback_message = message
                st.session_state.drive_manual_input_downloads = manual_downloads
                st.session_state.main_dir_path_input = str(main_dir)
                st.session_state.data_dir_path_input = str(input_dir)
                st.session_state.output_dir_path_input = str(output_dir)
                st.session_state.selected_symbol_restore = None
                list_google_drive_folder_files.clear()
                list_symbols.clear()
                load_data.clear()
                st.rerun()
        with cancel_col:
            if st.button("Cancel", width="stretch"):
                st.session_state.drive_dialog_feedback_level = None
                st.session_state.drive_dialog_feedback_message = ""
                st.session_state.drive_manual_input_downloads = []
                st.session_state.show_drive_process_dialog = False
                st.rerun()
    except Exception as exc:
        st.error(f"Drive processing dialog error: {exc}")
        with st.expander("Show full error details", expanded=True):
            st.code(traceback.format_exc(), language="python")


def build_processing_feedback(summary) -> tuple[str, str]:
    parts: list[str] = []
    if summary.processed:
        parts.append(f"Processed {len(summary.processed)} scrip(s)")
    if summary.skipped:
        parts.append(f"Skipped {len(summary.skipped)} up-to-date scrip(s)")
    if summary.errors:
        parts.append(f"Errors: {len(summary.errors)}")

    if not parts:
        parts.append("No files were processed.")

    if summary.errors and not summary.processed and not summary.skipped:
        level = "error"
    elif summary.errors:
        level = "warning"
    else:
        level = "success"

    message = ". ".join(parts)
    if summary.errors:
        message = f"{message}. {summary.errors[0]}"
    return level, message


def process_selected_drive_raw_symbols(
    selected_symbols: list[str],
    symbol_files: dict[str, list[Any]],
    input_dir: Path,
    drive_input_folder_id: str,
) -> tuple[str, str, list[dict[str, Any]]]:
    if not str(drive_input_folder_id or "").strip():
        return "error", "Google Drive Input Files folder is not available.", []
    if not selected_symbols:
        return "warning", "Please select at least one scrip to process.", []

    missing_symbols = [symbol for symbol in selected_symbols if symbol not in symbol_files]
    if missing_symbols:
        joined = ", ".join(sorted(missing_symbols))
        return "error", f"Selected scrips were not found in Google Drive Raw Files: {joined}", []

    temp_root = Path(tempfile.mkdtemp(prefix="ema_drive_process_"))
    temp_raw_dir = temp_root / "Raw Files"
    temp_input_dir = temp_root / "Input Files"
    temp_raw_dir.mkdir(parents=True, exist_ok=True)
    temp_input_dir.mkdir(parents=True, exist_ok=True)

    try:
        drive_input_files = {
            file_info.name.casefold(): file_info
            for file_info in filter_supported_google_drive_files(
                list_google_drive_folder_files(drive_input_folder_id)
            )
        }
        selected_files: list[Any] = []
        for symbol in selected_symbols:
            selected_files.extend(symbol_files.get(symbol, []))

        download_google_drive_files_to_dir(selected_files, temp_raw_dir)
        summary = process_raw_folder(temp_raw_dir, temp_input_dir)
        missing_drive_targets: list[str] = []
        manual_downloads: list[dict[str, Any]] = []
        uploaded_drive_targets: list[str] = []

        for symbol in selected_symbols:
            processed_csv_path = temp_input_dir / f"{symbol}.csv"
            if not processed_csv_path.exists():
                continue
            target_csv_path = csv_path_for_stem(input_dir, symbol)
            target_csv_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(processed_csv_path, target_csv_path)
            remove_other_matching_data_files(input_dir, symbol, target_csv_path)
            drive_file_name = target_csv_path.name
            if drive_file_name.casefold() not in drive_input_files:
                missing_drive_targets.append(drive_file_name)
                manual_downloads.append(
                    {
                        "file_name": drive_file_name,
                        "data": processed_csv_path.read_bytes(),
                        "mime": "text/csv",
                    }
                )
                continue
            upload_google_drive_file(
                folder_id=drive_input_folder_id,
                file_name=drive_file_name,
                content=processed_csv_path.read_bytes(),
                mime_type="text/csv",
            )
            uploaded_drive_targets.append(drive_file_name)

        level, message = build_processing_feedback(summary)
        if uploaded_drive_targets:
            uploaded_preview = ", ".join(uploaded_drive_targets[:5])
            extra_uploaded = max(0, len(uploaded_drive_targets) - 5)
            uploaded_suffix = f" (+{extra_uploaded} more)" if extra_uploaded else ""
            message = f"{message}. Updated in Google Drive Input Files: {uploaded_preview}{uploaded_suffix}"
        if missing_drive_targets:
            missing_preview = ", ".join(missing_drive_targets[:5])
            extra_missing = max(0, len(missing_drive_targets) - 5)
            missing_suffix = f" (+{extra_missing} more)" if extra_missing else ""
            message = (
                f"{message}. Could not create new files in Google Drive Input Files: "
                f"{missing_preview}{missing_suffix}. "
                "Download these files below and add them manually, or pre-create them in My Drive, or move the folder to a Shared Drive."
            )
            if level == "success":
                level = "warning"
        return level, message, manual_downloads
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def sync_google_drive_input_files_to_dir(
    drive_status: Any,
    target_dir: Path,
) -> tuple[str, str, int]:
    if not getattr(drive_status, "connected", False) or getattr(drive_status, "input_folder", None) is None:
        return "warning", "Google Drive Input Files are not connected yet.", 0

    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        drive_input_files = filter_supported_google_drive_files(
            list_google_drive_folder_files(drive_status.input_folder.folder_id)
        )
    except Exception as exc:
        return "error", f"Could not read Google Drive Input Files: {exc}", 0

    if not drive_input_files:
        clear_supported_data_files(target_dir)
        return "warning", "No supported files were found in Google Drive Input Files.", 0

    temp_dir = Path(tempfile.mkdtemp(prefix="ema_drive_input_sync_"))
    try:
        download_google_drive_files_to_dir(drive_input_files, temp_dir)
        clear_supported_data_files(target_dir)
        for downloaded_path in list_supported_data_files(temp_dir):
            target_path = target_dir / downloaded_path.name
            target_path.write_bytes(downloaded_path.read_bytes())
        return "success", f"Loaded {len(drive_input_files)} input file(s) from Google Drive.", len(drive_input_files)
    except Exception as exc:
        return "error", f"Could not sync Google Drive Input Files: {exc}", 0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def sync_google_drive_output_files_to_dir(
    drive_status: Any,
    target_dir: Path,
) -> tuple[str, str, int]:
    if not getattr(drive_status, "connected", False) or getattr(drive_status, "output_folder", None) is None:
        return "warning", "Google Drive Output Files are not connected yet.", 0

    target_dir.mkdir(parents=True, exist_ok=True)
    try:
        drive_output_files = filter_supported_google_drive_files(
            list_google_drive_folder_files(drive_status.output_folder.folder_id)
        )
    except Exception as exc:
        return "error", f"Could not read Google Drive Output Files: {exc}", 0

    if not drive_output_files:
        clear_supported_data_files(target_dir)
        return "success", "No saved output files were found in Google Drive Output Files.", 0

    temp_dir = Path(tempfile.mkdtemp(prefix="ema_drive_output_sync_"))
    try:
        download_google_drive_files_to_dir(drive_output_files, temp_dir)
        clear_supported_data_files(target_dir)
        for downloaded_path in list_supported_data_files(temp_dir):
            target_path = target_dir / downloaded_path.name
            target_path.write_bytes(downloaded_path.read_bytes())
        return "success", f"Loaded {len(drive_output_files)} output file(s) from Google Drive.", len(drive_output_files)
    except Exception as exc:
        return "error", f"Could not sync Google Drive Output Files: {exc}", 0
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def reload_selected_drive_output_for_symbol(
    drive_status: Any,
    symbol: str,
    output_dir: Path,
    input_df: pd.DataFrame,
) -> tuple[str, str, list[dict[str, Any]] | None]:
    if not getattr(drive_status, "connected", False) or getattr(drive_status, "output_folder", None) is None:
        return "error", "Google Drive Output Files are not connected yet.", None

    list_google_drive_folder_files.clear()
    drive_output_files = filter_supported_google_drive_files(
        list_google_drive_folder_files(drive_status.output_folder.folder_id)
    )
    matching_file = next(
        (
            file_info
            for file_info in drive_output_files
            if Path(file_info.name).stem.casefold() == symbol.casefold()
        ),
        None,
    )
    if matching_file is None:
        return "warning", f"No Google Drive Output file exists yet for {display_symbol(symbol)}.", None

    output_dir.mkdir(parents=True, exist_ok=True)
    target_csv_path = csv_path_for_stem(output_dir, symbol)
    downloaded_paths = download_google_drive_files_to_dir([matching_file], output_dir)
    downloaded_path = Path(downloaded_paths[0]) if downloaded_paths else target_csv_path
    if downloaded_path != target_csv_path and downloaded_path.exists():
        target_csv_path.write_bytes(downloaded_path.read_bytes())
        try:
            downloaded_path.unlink()
        except OSError:
            pass
    remove_other_matching_data_files(output_dir, symbol, target_csv_path)

    loaded_saved_signals = load_saved_signals_file(target_csv_path, symbol, input_df=input_df)
    persisted_saved_signals = persist_saved_signals_file(target_csv_path, symbol, loaded_saved_signals)
    return "success", f"Reloaded Google Drive Output data for {display_symbol(symbol)}.", persisted_saved_signals


def build_saved_signal_timestamp(date_value: Any, time_value: Any) -> pd.Timestamp:
    normalized_time = normalize_time(time_value).replace(".", ":")
    timestamp = pd.to_datetime(
        f"{str(date_value).strip()} {normalized_time}",
        format="%d-%b-%y %H:%M",
        errors="coerce",
    )
    if pd.isna(timestamp):
        raise ValueError(f"Invalid saved signal date/time: {date_value} {time_value}")
    return timestamp


def empty_saved_signals_df() -> pd.DataFrame:
    return pd.DataFrame(columns=SAVED_SIGNAL_COLUMNS)


def output_signal_csv_path(output_dir: Path, symbol: str) -> Path:
    return csv_path_for_stem(output_dir, symbol)


def ensure_output_signal_file(output_dir: Path, symbol: str) -> Path:
    csv_path = output_signal_csv_path(output_dir, symbol)
    existing_path = find_data_file_by_stem(output_dir, symbol)
    if existing_path and existing_path.exists() and existing_path != csv_path:
        raw_df = read_tabular_file(existing_path)
        normalized_df = normalize_saved_signals_df(raw_df, symbol)
        write_tabular_file(normalized_df, csv_path)
        remove_other_matching_data_files(output_dir, symbol, csv_path)
    elif not csv_path.exists():
        write_tabular_file(empty_saved_signals_df(), csv_path)
    return csv_path


def normalize_saved_signals_df(raw_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if raw_df.empty:
        return empty_saved_signals_df()

    safe_df = raw_df.copy()
    safe_df.columns = safe_df.columns.str.strip()
    missing = [column for column in SAVED_SIGNAL_REQUIRED_COLUMNS if column not in safe_df.columns]
    if missing:
        raise ValueError(f"Missing required saved-signal columns: {', '.join(missing)}")

    safe_df = safe_df.loc[:, [column for column in safe_df.columns if column in SAVED_SIGNAL_COLUMNS]].copy()
    safe_df["Symbol"] = safe_df["Symbol"].astype(str).str.strip()
    distinct_symbols = {value for value in safe_df["Symbol"] if value and value.lower() != "nan"}
    if not distinct_symbols:
        safe_df["Symbol"] = symbol
    elif distinct_symbols != {symbol}:
        found_symbols = ", ".join(sorted(distinct_symbols))
        raise ValueError(f"Saved-signal file symbols ({found_symbols}) do not match selected scrip {symbol}.")

    safe_df["Symbol"] = symbol
    safe_df["Date"] = safe_df["Date"].astype(str).str.strip()
    safe_df["Time"] = safe_df["Time"].map(normalize_time)
    safe_df["Signal"] = (
        safe_df["Signal"]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace({"B": "BUY", "S": "SELL"})
    )

    numeric_columns = ["Open", "High", "Low", "Close", "EMA", "Qty"]
    safe_df[numeric_columns] = safe_df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    timestamps = safe_df.apply(
        lambda row: build_saved_signal_timestamp(row["Date"], row["Time"]),
        axis=1,
    )
    safe_df["Timestamp"] = pd.to_datetime(timestamps, errors="coerce")
    safe_df = safe_df.dropna(subset=["Timestamp", *numeric_columns])
    safe_df = safe_df.loc[safe_df["Signal"].isin(["BUY", "SELL"])].copy()

    if safe_df.empty:
        return empty_saved_signals_df()

    safe_df["Date"] = safe_df["Timestamp"].dt.strftime("%d-%b-%y")
    safe_df["Time"] = safe_df["Timestamp"].dt.strftime("%H.%M")
    safe_df["Qty"] = safe_df["Qty"].round().astype(int)
    safe_df["TimeChart"] = safe_df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M")
    safe_df["TimeEpoch"] = (
        safe_df["Timestamp"]
        .dt.tz_localize("Asia/Calcutta")
        .dt.tz_convert("UTC")
        .astype("int64")
        // 10**9
    ).astype(int)
    safe_df["SignalKey"] = (
        safe_df["Date"]
        + "|"
        + safe_df["Time"]
        + "|"
        + safe_df["Open"].round(4).astype(str)
        + "|"
        + safe_df["Close"].round(4).astype(str)
    )

    safe_df = safe_df.loc[:, SAVED_SIGNAL_COLUMNS]
    safe_df = safe_df.drop_duplicates(subset=["SignalKey"], keep="last")
    safe_df = safe_df.sort_values(["TimeEpoch", "SignalKey"], kind="stable").reset_index(drop=True)
    return safe_df


def normalize_trade_export_to_saved_signals_df(
    raw_df: pd.DataFrame,
    symbol: str,
    input_df: pd.DataFrame,
) -> pd.DataFrame:
    safe_df = raw_df.copy()
    safe_df.columns = safe_df.columns.str.strip()
    required = ["Date", "Time", "Trade"]
    missing = [column for column in required if column not in safe_df.columns]
    if missing:
        raise ValueError(f"Missing required trade-export columns: {', '.join(missing)}")

    lookup_df = input_df.copy()
    lookup_df["LookupKey"] = (
        lookup_df["DateLabel"].astype(str).str.strip()
        + "|"
        + lookup_df["TimeLabel"].astype(str).str.strip()
    )
    lookup_df = lookup_df.drop_duplicates(subset=["LookupKey"], keep="last")
    lookup = lookup_df.set_index("LookupKey")

    records: list[dict[str, Any]] = []
    for _, row in safe_df.iterrows():
        date_label = str(row.get("Date") or "").strip()
        try:
            time_label = normalize_time(row.get("Time"))
        except Exception:
            continue
        trade_value = str(row.get("Trade") or "").strip().upper()
        signal = {"B": "BUY", "S": "SELL", "BUY": "BUY", "SELL": "SELL"}.get(trade_value)
        if not signal:
            continue

        lookup_key = f"{date_label}|{time_label}"
        if lookup_key not in lookup.index:
            continue
        candle = lookup.loc[lookup_key]
        qty_raw = row.get("Qty", 1)
        try:
            qty = int(float(qty_raw))
        except (TypeError, ValueError):
            qty = 1

        records.append(
            {
                "Symbol": symbol,
                "Date": str(candle["DateLabel"]),
                "Time": str(candle["TimeLabel"]),
                "Open": float(candle["Open"]),
                "High": float(candle["High"]),
                "Low": float(candle["Low"]),
                "Close": float(candle["Close"]),
                "EMA": float(candle["EMA"]),
                "Signal": signal,
                "Qty": qty,
                "TimeChart": str(candle["TimeChart"]),
                "TimeEpoch": int(candle["TimeEpoch"]),
                "SignalKey": str(candle["SignalKey"]),
            }
        )

    if not records:
        return empty_saved_signals_df()
    return normalize_saved_signals_df(pd.DataFrame(records), symbol)


def load_saved_signals_file(csv_path: Path, symbol: str, input_df: pd.DataFrame | None = None) -> list[dict[str, Any]]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return []

    raw_df = read_tabular_file(csv_path)
    try:
        normalized_df = normalize_saved_signals_df(raw_df, symbol)
    except ValueError:
        if input_df is None:
            raise
        normalized_df = normalize_trade_export_to_saved_signals_df(raw_df, symbol, input_df)
    return normalized_df.to_dict("records")


def persist_saved_signals_file(csv_path: Path, symbol: str, saved_signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_df = normalize_saved_signals_df(pd.DataFrame(saved_signals), symbol) if saved_signals else empty_saved_signals_df()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_tabular_file(normalized_df, csv_path)
    return normalized_df.to_dict("records")


def apply_saved_signals_state(saved_signals: list[dict[str, Any]], symbol: str, output_csv_path: Path) -> None:
    st.session_state.saved_signals = saved_signals
    st.session_state.saved_signals_symbol = symbol
    st.session_state.saved_signals_output_csv = str(output_csv_path)
    st.session_state.saved_signals_selected_row = None
    st.session_state.saved_signals_selected_rows = []
    st.session_state.latest_signal = saved_signals[-1] if saved_signals else None


@st.cache_data(show_spinner=False)
def load_data(csv_path: str) -> pd.DataFrame:
    df = read_tabular_file(Path(csv_path))
    df.columns = df.columns.str.strip()

    required = ["Date", "Time", "Open", "High", "Low", "Close", "EMA"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    df = df.loc[:, required].copy()
    df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%y", errors="coerce")
    df["Time"] = df["Time"].map(normalize_time)
    df["TimeMinutes"] = df["Time"].map(time_to_minutes)
    df["Timestamp"] = pd.to_datetime(
        df["Date"].dt.strftime("%Y-%m-%d") + " " + df["Time"].str.replace(".", ":", regex=False),
        format="%Y-%m-%d %H:%M",
        errors="coerce",
    )

    numeric_columns = ["Open", "High", "Low", "Close", "EMA"]
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    df = df.dropna(subset=["Date", "Timestamp", *numeric_columns])
    df = df.loc[
        (df["TimeMinutes"] >= SESSION_START_MINUTES)
        & (df["TimeMinutes"] <= SESSION_END_MINUTES)
    ].sort_values("Timestamp", kind="stable")

    df["Signal"] = df["Close"].ge(df["Open"]).map({True: "BUY", False: "SELL"})
    df["DateLabel"] = df["Timestamp"].dt.strftime("%d-%b-%y")
    df["TimeLabel"] = df["Timestamp"].dt.strftime("%H.%M")
    df["TimeChart"] = df["Timestamp"].dt.strftime("%Y-%m-%d %H:%M")
    df["TimeEpoch"] = (
        pd.to_datetime(df["Timestamp"])
        .dt.tz_localize("Asia/Calcutta")
        .dt.tz_convert("UTC")
        .astype("int64")
        // 10**9
    ).astype(int)
    df["SignalKey"] = (
        df["DateLabel"]
        + "|"
        + df["TimeLabel"]
        + "|"
        + df["Open"].round(4).astype(str)
        + "|"
        + df["Close"].round(4).astype(str)
    )

    return df.reset_index(drop=True)


def prepare_candle_data(df: pd.DataFrame, from_date: Any, to_date: Any) -> tuple[pd.DataFrame, list[dict[str, Any]], bool]:
    filtered_df = df.loc[
        (df["Timestamp"].dt.date >= from_date) & (df["Timestamp"].dt.date <= to_date)
    ]
    was_limited = len(filtered_df) > MAX_CANDLES
    if was_limited:
        filtered_df = filtered_df.copy()
        filtered_df["MonthPeriod"] = filtered_df["Timestamp"].dt.to_period("M")
        month_frames: list[pd.DataFrame] = []
        running_total = 0

        for _, month_df in filtered_df.groupby("MonthPeriod", sort=True):
            month_len = len(month_df)
            if month_frames and running_total + month_len > MAX_CANDLES:
                break
            if not month_frames and month_len > MAX_CANDLES:
                month_frames.append(month_df.head(MAX_CANDLES))
                running_total = MAX_CANDLES
                break
            month_frames.append(month_df)
            running_total += month_len

        if month_frames:
            filtered_df = pd.concat(month_frames, ignore_index=True)
        else:
            filtered_df = filtered_df.head(MAX_CANDLES)

        filtered_df = filtered_df.drop(columns=["MonthPeriod"], errors="ignore")

    filtered_df = filtered_df.reset_index(drop=True)

    candle_data = (
        filtered_df.loc[:, ["TimeEpoch", "Open", "High", "Low", "Close"]]
        .rename(
            columns={
                "TimeEpoch": "time",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
            }
        )
        .astype(
            {
                "time": "int64",
                "open": "float64",
                "high": "float64",
                "low": "float64",
                "close": "float64",
            }
        )
        .to_dict("records")
    )

    return filtered_df, candle_data, was_limited


def prepare_ema_data(filtered_df: pd.DataFrame) -> list[dict[str, Any]]:
    return (
        filtered_df.loc[:, ["TimeEpoch", "EMA"]]
        .rename(columns={"TimeEpoch": "time", "EMA": "value"})
        .astype({"time": "int64", "value": "float64"})
        .to_dict("records")
    )


def build_signal_record(symbol: str, row: pd.Series) -> dict[str, Any]:
    signal = "BUY" if row["Close"] >= row["Open"] else "SELL"
    return {
        "Symbol": symbol,
        "Date": row["DateLabel"],
        "Time": row["TimeLabel"],
        "Open": float(row["Open"]),
        "High": float(row["High"]),
        "Low": float(row["Low"]),
        "Close": float(row["Close"]),
        "EMA": float(row["EMA"]),
        "Signal": signal,
        "Qty": int(st.session_state.get("qty", 1) or 1),
        "TimeChart": row["TimeChart"],
        "TimeEpoch": int(row["TimeEpoch"]),
        "SignalKey": row["SignalKey"],
    }


def save_signal(signal_record: dict[str, Any], output_csv_path: Path) -> bool:
    existing_keys = {item["SignalKey"] for item in st.session_state.saved_signals}
    if signal_record["SignalKey"] in existing_keys:
        st.session_state.latest_signal = signal_record
        return False

    updated_signals = [*st.session_state.saved_signals, signal_record]
    try:
        persisted_signals = persist_saved_signals_file(output_csv_path, signal_record["Symbol"], updated_signals)
    except Exception as exc:
        st.error(f"Could not update saved-signal file: {exc}")
        return False
    apply_saved_signals_state(persisted_signals, signal_record["Symbol"], output_csv_path)
    return True


def remove_signal(signal_record: dict[str, Any], output_csv_path: Path) -> bool:
    existing_keys = {item["SignalKey"] for item in st.session_state.saved_signals}
    if signal_record["SignalKey"] not in existing_keys:
        return False

    updated_signals = [
        item for item in st.session_state.saved_signals
        if item["SignalKey"] != signal_record["SignalKey"]
    ]
    try:
        persisted_signals = persist_saved_signals_file(
            output_csv_path,
            signal_record["Symbol"],
            updated_signals,
        )
    except Exception as exc:
        st.error(f"Could not update saved-signal file: {exc}")
        return False
    apply_saved_signals_state(persisted_signals, signal_record["Symbol"], output_csv_path)
    return True


def chart_click_token(symbol: str, signal_key: str) -> str:
    return f"{symbol}|{signal_key}"


def build_markers(symbol: str) -> list[dict[str, Any]]:
    markers: list[dict[str, Any]] = []
    for item in st.session_state.saved_signals:
        if item["Symbol"] != symbol:
            continue
        if item["Signal"] == "BUY":
            markers.append(
                {
                    "time": int(item["TimeEpoch"]),
                    "position": "belowBar",
                    "shape": "arrowUp",
                    "color": BUY_COLOR,
                    "text": "BUY",
                    "size": 1.2,
                }
            )
        else:
            markers.append(
                {
                    "time": int(item["TimeEpoch"]),
                    "position": "aboveBar",
                    "shape": "arrowDown",
                    "color": SELL_COLOR,
                    "text": "SELL",
                    "size": 1.2,
                }
            )
    return markers


def build_chart(
    candle_data: list[dict[str, Any]],
    ema_data: list[dict[str, Any]],
    symbol: str,
) -> list[dict[str, Any]]:
    chart_options = {
        "height": CHART_HEIGHT,
        "layout": {
            "background": {"type": "solid", "color": "white"},
            "textColor": "#475569",
            "fontFamily": "Segoe UI, sans-serif",
            "fontSize": 12,
        },
        "grid": {
            "vertLines": {"color": GRID_COLOR},
            "horzLines": {"color": GRID_COLOR},
        },
        "crosshair": {"mode": 0},
        "rightPriceScale": {
            "borderColor": "rgba(203, 213, 225, 0.9)",
            "scaleMargins": {"top": 0.1, "bottom": 0.1},
        },
        "timeScale": {
            "borderColor": "rgba(203, 213, 225, 0.9)",
            "timeVisible": True,
            "secondsVisible": False,
            "barSpacing": 8,
            "minBarSpacing": 3,
            "rightOffset": 8,
            "lockVisibleTimeRangeOnResize": True,
            "borderVisible": True,
        },
        "localization": {
            "locale": "en-IN",
        },
    }

    series = [
        {
            "type": "Candlestick",
            "data": candle_data,
            "options": {
                "upColor": BUY_COLOR,
                "downColor": SELL_COLOR,
                "borderVisible": False,
                "wickUpColor": BUY_COLOR,
                "wickDownColor": SELL_COLOR,
                "priceLineVisible": False,
                "lastValueVisible": False,
            },
            "markers": build_markers(symbol),
        },
        {
            "type": "Line",
            "data": ema_data,
            "options": {
                "color": EMA_COLOR,
                "lineWidth": 2,
                "priceLineVisible": False,
                "lastValueVisible": False,
                "crosshairMarkerVisible": False,
            },
        },
    ]

    return [{"chart": chart_options, "series": series}]


def _match_clicked_value(chart_df: pd.DataFrame, value: Any) -> pd.Series | None:
    if value is None or chart_df.empty:
        return None

    if isinstance(value, (int, float)):
        matched = chart_df.loc[chart_df["TimeEpoch"] == int(value)]
        if not matched.empty:
            return matched.iloc[-1]
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.isdigit():
        matched = chart_df.loc[chart_df["TimeEpoch"] == int(text)]
        if not matched.empty:
            return matched.iloc[-1]

    matched = chart_df.loc[chart_df["TimeChart"] == text]
    if not matched.empty:
        return matched.iloc[-1]

    parsed_ts = pd.to_datetime(text, errors="coerce")
    if pd.isna(parsed_ts):
        return None

    normalized = parsed_ts.strftime("%Y-%m-%d %H:%M")
    matched = chart_df.loc[chart_df["TimeChart"] == normalized]
    if not matched.empty:
        return matched.iloc[-1]

    matched = chart_df.loc[chart_df["TimeEpoch"] == int(parsed_ts.timestamp())]
    if not matched.empty:
        return matched.iloc[-1]

    return None


def parse_clicked_row(chart_event: Any, chart_df: pd.DataFrame):

    if not chart_event:
        return None

    try:
        epoch = chart_event.get("epoch") or chart_event.get("time")
        epoch = int(float(epoch))
    except Exception:
        return None

    diff = (chart_df["TimeEpoch"] - epoch).abs()
    nearest_idx = diff.idxmin()

    return chart_df.loc[nearest_idx]


def _scalarize(value: Any):
    if isinstance(value, pd.Series):
        if value.empty:
            return None
        return value.iloc[0]
    if isinstance(value, pd.Index):
        if value.empty:
            return None
        return value[0]
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value
    if isinstance(value, pd.Timedelta):
        if pd.isna(value):
            return None
        return value
    if isinstance(value, pd.DataFrame):
        if value.empty:
            return None
        return value.iloc[0, 0]
    if isinstance(value, (list, tuple, set)):
        if not value:
            return None
        return _scalarize(next(iter(value)))
    try:
        import numpy as np
    except Exception:
        np = None
    if np is not None and isinstance(value, np.ndarray):
        if value.size == 0:
            return None
        return _scalarize(value.flat[0])
    return value


def reset_clicked_candle() -> None:
    st.session_state.clicked_date = None
    st.session_state.clicked_time = None
    st.session_state.clicked_epoch = None


def sync_clicked_candle_with_view(chart_df: pd.DataFrame) -> None:
    clicked_epoch = _scalarize(st.session_state.get("clicked_epoch"))
    clicked_date = _scalarize(st.session_state.get("clicked_date"))
    clicked_time = _scalarize(st.session_state.get("clicked_time"))

    if clicked_epoch is None and (clicked_date is None or clicked_time is None):
        return

    mask = chart_df["TimeEpoch"] == clicked_epoch if clicked_epoch is not None else (
        (chart_df["DateLabel"] == clicked_date) & (chart_df["TimeLabel"] == clicked_time)
    )
    if not mask.any():
        reset_clicked_candle()


def _format_table_dates(table_df: pd.DataFrame) -> pd.DataFrame:
    if "Date" not in table_df.columns:
        return table_df
    safe_df = _ensure_unique_columns(table_df)
    date_values = safe_df.loc[:, "Date"]
    if isinstance(date_values, pd.DataFrame):
        date_values = date_values.iloc[:, 0]

    parsed = pd.to_datetime(date_values, errors="coerce", format="%d-%b-%y")
    mask = parsed.notna()
    if mask.any():
        formatted = parsed.dt.strftime("%d-%b-%y")
        safe_df.loc[mask, "Date"] = formatted[mask]

    if (~mask).any():
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            fallback = pd.to_datetime(date_values.loc[~mask], errors="coerce", dayfirst=True)
        fallback_mask = fallback.notna()
        if fallback_mask.any():
            safe_df.loc[~mask, "Date"] = fallback.dt.strftime("%d-%b-%y").fillna(date_values.loc[~mask].astype(str))
        else:
            safe_df.loc[~mask, "Date"] = date_values.loc[~mask].astype(str)

    safe_df["Date"] = safe_df["Date"].astype(str)
    return safe_df


def _sync_saved_signals_selection() -> None:
    state = st.session_state.get("saved-signals-table")
    rows: list[int] = []
    cells: list[Any] = []
    if state is not None:
        if isinstance(state, dict):
            rows = list(state.get("selection", {}).get("rows", []))
            cells = list(state.get("selection", {}).get("cells", []))
        else:
            try:
                rows = list(state.selection.rows)
            except Exception:
                try:
                    rows = list(state["selection"]["rows"])
                except Exception:
                    rows = []
            try:
                cells = list(state.selection.cells)
            except Exception:
                try:
                    cells = list(state["selection"]["cells"])
                except Exception:
                    cells = []

    selected_rows = [int(row) for row in rows]
    if not selected_rows and cells:
        derived_rows: set[int] = set()
        for cell in cells:
            if isinstance(cell, (list, tuple)) and cell:
                derived_rows.add(int(cell[0]))
            elif isinstance(cell, dict) and "row" in cell:
                derived_rows.add(int(cell["row"]))
        selected_rows = sorted(derived_rows)

    st.session_state.saved_signals_selected_rows = selected_rows
    st.session_state.saved_signals_selected_row = selected_rows[0] if selected_rows else None


def build_saved_signals_trade_table(
    saved_signals: list[dict[str, Any]],
    symbol: str,
    default_qty: int,
) -> pd.DataFrame:
    if not saved_signals:
        return pd.DataFrame(
            columns=[
                "SignalKey",
                "Sr.No",
                "Date",
                "Time",
                "Trade",
                "Price",
                "Entry Date",
                "Entry Time",
                "Entry Price",
                "Exit Date",
                "Exit Time",
                "Exit Price",
                "Qty",
                "PL Points",
                "PL Amt",
                "Candle Analysis",
            ]
        )

    source_df = pd.DataFrame(saved_signals)
    if "Symbol" in source_df.columns:
        source_df = source_df[source_df["Symbol"] == symbol].copy()
    if source_df.empty:
        return pd.DataFrame(
            columns=[
                "SignalKey",
                "Sr.No",
                "Date",
                "Time",
                "Trade",
                "Price",
                "Entry Date",
                "Entry Time",
                "Entry Price",
                "Exit Date",
                "Exit Time",
                "Exit Price",
                "Qty",
                "PL Points",
                "PL Amt",
                "Candle Analysis",
            ]
        )

    source_df = source_df.sort_values(["TimeEpoch", "SignalKey"], kind="stable").reset_index(drop=True)
    source_df["Trade"] = source_df["Signal"].map({"BUY": "B", "SELL": "S"}).fillna(source_df["Signal"])

    records: list[dict[str, Any]] = []
    for idx, row in source_df.iterrows():
        next_row = source_df.iloc[idx + 1] if idx + 1 < len(source_df) else None
        qty = int(row["Qty"]) if "Qty" in row and pd.notna(row["Qty"]) else int(default_qty)
        price = float(row["Close"])

        exit_date = ""
        exit_time = ""
        exit_price: float | None = None
        pl_points: float | None = None
        pl_amt: float | None = None

        if next_row is not None:
            exit_date = str(next_row["Date"])
            exit_time = str(next_row["Time"])
            next_trade = str(next_row["Trade"])
            exit_price = float(next_row["Close"])
            current_trade = str(row["Trade"])
            if (current_trade == "B" and next_trade == "S") or (current_trade == "S" and next_trade == "B"):
                pl_points = exit_price - price if current_trade == "B" else price - exit_price
                pl_amt = qty * pl_points

        records.append(
            {
                "SignalKey": row["SignalKey"],
                "Sr.No": idx + 1,
                "Date": str(row["Date"]),
                "Time": str(row["Time"]),
                "Trade": str(row["Trade"]),
                "Price": price,
                "Entry Date": str(row["Date"]),
                "Entry Time": str(row["Time"]),
                "Entry Price": price,
                "Exit Date": exit_date,
                "Exit Time": exit_time,
                "Exit Price": exit_price,
                "Qty": qty,
                "PL Points": pl_points,
                "PL Amt": pl_amt,
                "Candle Analysis": "",
            }
        )

    trade_df = pd.DataFrame(records)
    trade_df = _ensure_unique_columns(trade_df)
    for date_col in ["Date", "Entry Date", "Exit Date"]:
        if date_col in trade_df.columns:
            formatted = _format_table_dates(trade_df.loc[:, [date_col]].rename(columns={date_col: "Date"}))
            trade_df[date_col] = formatted["Date"]
    return trade_df


def _table_height_for_rows(row_count: int, min_height: int = 180) -> int:
    header_height = 32
    row_height = 32
    padding = 12
    if row_count <= 0:
        return 120
    height = header_height + (row_height * row_count) + padding
    return min(CHART_HEIGHT, height)


def _concat_non_empty_frames(frames: list[pd.DataFrame], *, fallback_columns: list[str] | None = None) -> pd.DataFrame:
    cleaned_frames: list[pd.DataFrame] = []
    for frame in frames:
        if frame is None or frame.empty:
            continue
        cleaned = frame.dropna(axis=1, how="all").copy()
        if cleaned.empty:
            continue
        cleaned_frames.append(cleaned)
    if not cleaned_frames:
        return pd.DataFrame(columns=fallback_columns or [])
    result = pd.concat(cleaned_frames, ignore_index=True)
    if fallback_columns is not None:
        result = result.reindex(columns=fallback_columns)
    return result


def _normalize_dashboard_scrips(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Scrip" not in df.columns:
        return df
    normalized = df.copy()
    normalized["Scrip"] = normalized["Scrip"].fillna("").astype(str).str.strip().str.upper()
    return normalized


def _load_output_dashboard_rows(output_dir: Path) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for file_path in list_supported_data_files(output_dir):
        try:
            df = read_tabular_file(file_path)
        except Exception:
            continue
        if df.empty:
            continue
        safe_df = df.copy()
        safe_df.columns = safe_df.columns.astype(str).str.strip()
        if "Scrip" not in safe_df.columns:
            safe_df["Scrip"] = file_path.stem
        safe_df["Scrip"] = safe_df["Scrip"].fillna(file_path.stem).astype(str).str.strip()
        for column in ["Qty", "Price", "Entry Price", "Exit Price", "PL Points", "PL Amt"]:
            if column in safe_df.columns:
                safe_df[column] = pd.to_numeric(safe_df[column], errors="coerce")
        frames.append(safe_df)
    if not frames:
        return pd.DataFrame()
    return _concat_non_empty_frames(frames)


def build_output_dashboard_summary(output_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    output_df = _load_output_dashboard_rows(output_dir)
    if output_df.empty:
        return {
            "scrip_files": 0,
            "trade_rows": 0,
            "closed_trades": 0,
            "open_trades": 0,
            "total_pl_points": 0.0,
            "total_pl_amt": 0.0,
        }, pd.DataFrame(
            columns=[
                "Scrip",
                "Trades",
                "Closed Trades",
                "Open Trades",
                "Wins",
                "Losses",
                "Total PL Points",
                "Total PL Amt",
                "Win Rate %",
            ]
        )

    working_df = output_df.copy()
    closed_mask = working_df.get("PL Amt", pd.Series(dtype=float)).notna()
    working_df["Is Closed"] = closed_mask
    working_df["Is Open"] = ~closed_mask
    working_df["Is Win"] = closed_mask & (working_df.get("PL Amt", 0).fillna(0) > 0)
    working_df["Is Loss"] = closed_mask & (working_df.get("PL Amt", 0).fillna(0) < 0)

    summary_df = (
        working_df.groupby("Scrip", dropna=False)
        .agg(
            Trades=("Scrip", "size"),
            Closed_Trades=("Is Closed", "sum"),
            Open_Trades=("Is Open", "sum"),
            Wins=("Is Win", "sum"),
            Losses=("Is Loss", "sum"),
            Total_PL_Points=("PL Points", "sum"),
            Total_PL_Amt=("PL Amt", "sum"),
        )
        .reset_index()
    )
    summary_df["Win Rate %"] = summary_df.apply(
        lambda row: (float(row["Wins"]) / float(row["Closed_Trades"]) * 100.0) if float(row["Closed_Trades"]) else 0.0,
        axis=1,
    )
    summary_df = summary_df.rename(
        columns={
            "Closed_Trades": "Closed Trades",
            "Open_Trades": "Open Trades",
            "Total_PL_Points": "Total PL Points",
            "Total_PL_Amt": "Total PL Amt",
        }
    ).sort_values(["Total PL Amt", "Scrip"], ascending=[False, True], kind="stable").reset_index(drop=True)

    metrics = {
        "scrip_files": int(summary_df["Scrip"].nunique()),
        "trade_rows": int(len(working_df)),
        "closed_trades": int(working_df["Is Closed"].sum()),
        "open_trades": int(working_df["Is Open"].sum()),
        "total_pl_points": float(pd.to_numeric(working_df.get("PL Points"), errors="coerce").fillna(0).sum()),
        "total_pl_amt": float(pd.to_numeric(working_df.get("PL Amt"), errors="coerce").fillna(0).sum()),
    }
    return metrics, summary_df


@st.dialog("Output Dashboard", width="large")
def render_output_dashboard_dialog(output_dir: Path) -> None:
    try:
        render_interactive_output_dashboard(output_dir)
    except Exception as exc:
        st.error(f"Dashboard error: {exc}")
        with st.expander("Show full error details", expanded=True):
            st.code(traceback.format_exc(), language="python")


DASHBOARD_OUTPUT_COLUMNS = [
    "Scrip",
    "Sr.No",
    "Date",
    "Time",
    "Trade",
    "Price",
    "Entry Date",
    "Entry Time",
    "Entry Price",
    "Exit Date",
    "Exit Time",
    "Exit Price",
    "Qty",
    "PL Points",
    "PL Amt",
    "Candle Analysis",
]
DASHBOARD_EPSILON = 1e-9
CARD_STYLE = """
<style>
.card {
    padding: 8px 12px;
    border-radius: 12px;
    background-color: #f9fafb;
    border: 1px solid #e5e7eb;
    margin-bottom: 8px;
    min-height: 56px;
    box-sizing: border-box;
    display: flex;
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
    gap: 10px;
}
.card-title {
    font-size: 13px;
    color: #6b7280;
    font-weight: 600;
    line-height: 1.25;
    flex: 0 1 46%;
}
.card-value {
    font-size: 18px;
    font-weight: 600;
    color: #0f172a;
    line-height: 1.15;
    word-break: break-word;
    text-align: right;
    flex: 1 1 54%;
}
.card-green { color: #16a34a; }
.card-red { color: #dc2626; }
</style>
"""


def dashboard_folder_signature(folder: Path) -> tuple[tuple[str, int, int], ...]:
    if not folder.exists() or not folder.is_dir():
        return ()
    signature: list[tuple[str, int, int]] = []
    for file_path in list_supported_data_files(folder):
        try:
            stat = file_path.stat()
        except OSError:
            continue
        signature.append((file_path.name, int(stat.st_mtime_ns), int(stat.st_size)))
    return tuple(signature)


def dashboard_strategy_dirs(output_dir: Path) -> list[Path]:
    if not output_dir.exists() or not output_dir.is_dir():
        return []
    folders: list[Path] = []
    for child in sorted(output_dir.iterdir(), key=lambda item: item.name.lower()):
        if child.is_dir() and dashboard_folder_signature(child):
            folders.append(child)
    return folders


def dashboard_normalize_time_text(value: Any) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        return normalize_time(text).replace(".", ":")
    except Exception:
        return text.replace(".", ":")


def dashboard_parse_timestamp(date_series: pd.Series, time_series: pd.Series) -> pd.Series:
    date_text = date_series.fillna("").astype(str).str.strip()
    time_text = time_series.map(dashboard_normalize_time_text)
    timestamp = pd.to_datetime(
        (date_text + " " + time_text).str.strip(),
        format="%d-%b-%y %H:%M",
        errors="coerce",
    )
    missing_time_mask = time_text.eq("")
    if missing_time_mask.any():
        timestamp.loc[missing_time_mask] = pd.to_datetime(
            date_text.loc[missing_time_mask],
            format="%d-%b-%y",
            errors="coerce",
        )
    return timestamp


def empty_dashboard_trade_df() -> pd.DataFrame:
    return pd.DataFrame(columns=DASHBOARD_OUTPUT_COLUMNS + [
        "Strategy",
        "Source File",
        "Entry Timestamp",
        "Exit Timestamp",
        "is_open",
        "is_closed",
        "is_win",
        "is_loss",
    ])


def normalize_dashboard_trade_df(raw_df: pd.DataFrame, file_path: Path, strategy_name: str | None = None) -> pd.DataFrame:
    if raw_df.empty:
        return empty_dashboard_trade_df().iloc[0:0].copy()

    safe_df = raw_df.copy()
    safe_df.columns = safe_df.columns.astype(str).str.strip()
    for column in DASHBOARD_OUTPUT_COLUMNS:
        if column not in safe_df.columns:
            safe_df[column] = pd.NA
    safe_df = safe_df.loc[:, DASHBOARD_OUTPUT_COLUMNS].copy()
    safe_df["Scrip"] = safe_df["Scrip"].fillna(file_path.stem).astype(str).str.strip()
    safe_df.loc[safe_df["Scrip"].eq(""), "Scrip"] = file_path.stem
    safe_df["Strategy"] = str(strategy_name or "Current")
    safe_df["Source File"] = file_path.name

    for numeric_column in ["Sr.No", "Price", "Entry Price", "Exit Price", "Qty", "PL Points", "PL Amt"]:
        safe_df[numeric_column] = pd.to_numeric(safe_df[numeric_column], errors="coerce")

    safe_df["Entry Timestamp"] = dashboard_parse_timestamp(safe_df["Entry Date"], safe_df["Entry Time"])
    safe_df["Exit Timestamp"] = dashboard_parse_timestamp(safe_df["Exit Date"], safe_df["Exit Time"])
    safe_df["is_open"] = safe_df["Exit Price"].isna()
    safe_df["is_closed"] = ~safe_df["is_open"]
    safe_df["is_win"] = safe_df["PL Points"].gt(0).fillna(False)
    safe_df["is_loss"] = safe_df["PL Points"].lt(0).fillna(False)
    return safe_df


@st.cache_data(show_spinner=False)
def load_dashboard_trade_rows(
    folder_path: str,
    file_signature: tuple[tuple[str, int, int], ...],
    strategy_name: str | None = None,
) -> pd.DataFrame:
    folder = Path(folder_path)
    if not file_signature or not folder.exists():
        return empty_dashboard_trade_df().copy()

    frames: list[pd.DataFrame] = []
    for file_name, _, _ in file_signature:
        file_path = folder / file_name
        if not file_path.exists():
            continue
        try:
            raw_df = read_tabular_file(file_path)
        except Exception:
            continue
        normalized_df = normalize_dashboard_trade_df(raw_df, file_path, strategy_name=strategy_name)
        if not normalized_df.empty:
            frames.append(normalized_df)
    if not frames:
        return empty_dashboard_trade_df().copy()
    return _concat_non_empty_frames(frames, fallback_columns=list(empty_dashboard_trade_df().columns))


def filter_dashboard_trade_rows(
    trade_df: pd.DataFrame,
    start_date: Any,
    end_date: Any,
    include_open_trades: bool,
    selected_scrips: list[str] | None = None,
) -> pd.DataFrame:
    if trade_df.empty:
        return trade_df.copy()

    filtered_df = trade_df.copy()
    entry_dates = filtered_df["Entry Timestamp"].dt.date
    start = pd.Timestamp(start_date).date()
    end = pd.Timestamp(end_date).date()
    filtered_df = filtered_df[(entry_dates >= start) & (entry_dates <= end)].copy()
    if selected_scrips:
        valid_scrips = {str(scrip).strip() for scrip in selected_scrips if str(scrip).strip()}
        if valid_scrips:
            filtered_df = filtered_df[filtered_df["Scrip"].astype(str).isin(valid_scrips)].copy()
    if not include_open_trades:
        filtered_df = filtered_df[filtered_df["is_closed"]].copy()
    return filtered_df.reset_index(drop=True)


def compute_dashboard_sharpe(closed_df: pd.DataFrame) -> float:
    if closed_df.empty or "PL Amt" not in closed_df.columns:
        return 0.0
    returns = pd.to_numeric(closed_df["PL Amt"], errors="coerce").dropna()
    if returns.empty:
        return 0.0
    std_return = float(returns.std())
    if std_return == 0.0:
        return 0.0
    return (float(returns.mean()) / std_return) * (len(returns) ** 0.5)


def build_dashboard_equity_curve(closed_df: pd.DataFrame) -> pd.DataFrame:
    if closed_df.empty:
        return pd.DataFrame(columns=["Entry Timestamp", "PL Amt", "Equity Curve", "Peak", "Drawdown"])
    equity_df = closed_df.copy()
    equity_df = equity_df.sort_values(["Entry Timestamp", "Scrip", "Sr.No"], kind="stable").reset_index(drop=True)
    equity_df["PL Amt"] = pd.to_numeric(equity_df["PL Amt"], errors="coerce").fillna(0.0)
    equity_df["Equity Curve"] = equity_df["PL Amt"].cumsum()
    equity_df["Peak"] = equity_df["Equity Curve"].cummax()
    equity_df["Drawdown"] = equity_df["Equity Curve"] - equity_df["Peak"]
    return equity_df


def compute_dashboard_drawdown_duration(equity_df: pd.DataFrame) -> int:
    if equity_df.empty or "Drawdown" not in equity_df.columns:
        return 0
    max_duration = 0
    current_duration = 0
    for is_drawdown in equity_df["Drawdown"].fillna(0).lt(0):
        if bool(is_drawdown):
            current_duration += 1
            max_duration = max(max_duration, current_duration)
        else:
            current_duration = 0
    return int(max_duration)


def compute_dashboard_expectancy_metrics(closed_df: pd.DataFrame) -> dict[str, float]:
    if closed_df.empty or "PL Amt" not in closed_df.columns:
        return {
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "loss_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "expectancy": 0.0,
            "risk_reward_ratio": 0.0,
        }

    returns = pd.to_numeric(closed_df["PL Amt"], errors="coerce").dropna()
    if returns.empty:
        return {
            "wins": 0,
            "losses": 0,
            "win_rate": 0.0,
            "loss_rate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "expectancy": 0.0,
            "risk_reward_ratio": 0.0,
        }

    wins = returns[returns > 0]
    losses = returns[returns < 0]
    total_closed = len(returns)
    win_rate = float(len(wins)) / float(total_closed) if total_closed else 0.0
    loss_rate = float(len(losses)) / float(total_closed) if total_closed else 0.0
    avg_win = float(wins.mean()) if not wins.empty else 0.0
    avg_loss = abs(float(losses.mean())) if not losses.empty else DASHBOARD_EPSILON
    expectancy = (win_rate * avg_win) - (loss_rate * avg_loss)
    risk_reward_ratio = (avg_win / avg_loss) if avg_loss else 0.0
    return {
        "wins": int(len(wins)),
        "losses": int(len(losses)),
        "win_rate": win_rate * 100.0,
        "loss_rate": loss_rate * 100.0,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "expectancy": expectancy,
        "risk_reward_ratio": risk_reward_ratio,
        "avg_net": float(returns.mean()) if not returns.empty else 0.0,
    }


def format_inr(value: Any) -> str:
    if pd.isna(value):
        return "₹ 0"
    value = int(round(float(value)))
    sign = "-" if value < 0 else ""
    value = abs(value)
    s = str(value)
    if len(s) > 3:
        last3 = s[-3:]
        rest = s[:-3]
        rest = ",".join([rest[max(i - 2, 0):i] for i in range(len(rest), 0, -2)][::-1])
        formatted = rest + "," + last3
    else:
        formatted = s
    return f"{sign}₹ {formatted}"


def format_inr_compact(value: Any) -> str:
    if pd.isna(value):
        return "₹ 0"
    value = float(value)
    sign = "-" if value < 0 else ""
    value = abs(value)

    def _format_number(number: float, *, decimals: int = 1) -> str:
        text = f"{number:.{decimals}f}"
        return text.rstrip("0").rstrip(".")

    if value >= 1e7:
        return f"{sign}₹ {_format_number(value / 1e7)} Cr"
    if value >= 1e5:
        lakh_value = value / 1e5
        decimals = 0 if lakh_value >= 10 else 1
        return f"{sign}₹ {_format_number(lakh_value, decimals=decimals)} Lacs"
    if value >= 1e3:
        return f"{sign}₹ {_format_number(value / 1e3)} K"
    return f"{sign}₹ {int(round(value))}"


def render_dashboard_section_header(
    title: str,
    *,
    download_data: bytes | None = None,
    download_filename: str | None = None,
    download_label: str = "📄 Download Dashboard Summary",
    download_mime: str = "application/pdf",
    download_key: str | None = None,
) -> None:
    left_col, right_col = st.columns([7, 3])
    with left_col:
        st.markdown(f"### {title}")
    if download_data is not None and download_filename:
        with right_col:
            st.download_button(
                download_label,
                data=download_data,
                file_name=download_filename,
                mime=download_mime,
                width="stretch",
                key=download_key,
            )


def get_dashboard_pdf_fonts() -> tuple[str, str]:
    regular_name = "Helvetica"
    bold_name = "Helvetica-Bold"
    font_cache_dir = Path(tempfile.gettempdir()) / "ema_trade_viewer_fonts"
    font_cache_dir.mkdir(parents=True, exist_ok=True)
    font_candidates = [
        (
            "DashboardUnicode",
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"),
            "DashboardUnicode-Bold",
            Path("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"),
        ),
        (
            "DashboardUnicode",
            Path("/usr/share/fonts/truetype/noto/NotoSans-Regular.ttf"),
            "DashboardUnicode-Bold",
            Path("/usr/share/fonts/truetype/noto/NotoSans-Bold.ttf"),
        ),
        (
            "DashboardUnicode",
            Path("C:/Windows/Fonts/arial.ttf"),
            "DashboardUnicode-Bold",
            Path("C:/Windows/Fonts/arialbd.ttf"),
        ),
        (
            "DashboardUnicode",
            Path("C:/Windows/Fonts/segoeui.ttf"),
            "DashboardUnicode-Bold",
            Path("C:/Windows/Fonts/segoeuib.ttf"),
        ),
    ]
    remote_fallbacks = [
        (
            "DashboardUnicode",
            "https://raw.githubusercontent.com/googlefonts/noto-fonts/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf",
            "DashboardUnicode-Bold",
            "https://raw.githubusercontent.com/googlefonts/noto-fonts/main/hinted/ttf/NotoSans/NotoSans-Bold.ttf",
        ),
    ]

    for reg_name, reg_path, bold_reg_name, bold_path in font_candidates:
        try:
            if not reg_path.exists():
                continue
            if reg_name not in pdfmetrics.getRegisteredFontNames():
                pdfmetrics.registerFont(TTFont(reg_name, str(reg_path)))
            if bold_path.exists() and bold_reg_name not in pdfmetrics.getRegisteredFontNames():
                pdfmetrics.registerFont(TTFont(bold_reg_name, str(bold_path)))
            regular_name = reg_name
            bold_name = bold_reg_name if bold_path.exists() else reg_name
            break
        except Exception:
            continue
    else:
        for reg_name, reg_url, bold_reg_name, bold_url in remote_fallbacks:
            reg_path = font_cache_dir / Path(reg_url).name
            bold_path = font_cache_dir / Path(bold_url).name
            try:
                if not reg_path.exists():
                    urllib.request.urlretrieve(reg_url, reg_path)
                if not bold_path.exists():
                    urllib.request.urlretrieve(bold_url, bold_path)
                if reg_name not in pdfmetrics.getRegisteredFontNames():
                    pdfmetrics.registerFont(TTFont(reg_name, str(reg_path)))
                if bold_reg_name not in pdfmetrics.getRegisteredFontNames():
                    pdfmetrics.registerFont(TTFont(bold_reg_name, str(bold_path)))
                regular_name = reg_name
                bold_name = bold_reg_name
                break
            except Exception:
                continue
    return regular_name, bold_name


def _pdf_escape_text(value: Any) -> str:
    if value is None:
        return "-"
    text = str(value).strip()
    return html.escape(text or "-")


def _format_dashboard_pdf_value(column: str, value: Any) -> str:
    if pd.isna(value):
        return "-"
    if column in {
        "Total PL", "Avg Profit Per Trade", "Avg Loss Per Trade", "Avg Net Profit Per Trade",
        "Max Drawdown", "PL Amt", "Total PL Amt", "Profit / Loss", "Total Profit / Loss",
        "Entry Price", "Exit Price", "Price",
    }:
        return format_inr(float(value))
    if column == "Win Rate %":
        return f"{float(value):.2f}%"
    if column in {"Sharpe Ratio", "Risk Reward Ratio", "Score"}:
        return f"{float(value):.2f}"
    if column in {"Rank", "Sr.No", "Qty", "Trades", "Closed Trades", "Open Trades", "Wins", "Losses", "Drawdown Duration", "Max DD Duration", "Total Profit Trades", "Total Loss Trades", "Total Trades"}:
        return f"{int(float(value))}"
    return str(value)


def _build_dashboard_pdf_table(
    df: pd.DataFrame,
    columns: list[str],
    *,
    title: str,
    regular_font: str,
    bold_font: str,
    roomy: bool = False,
) -> list[Any]:
    styles = getSampleStyleSheet()
    if df.empty:
        empty_heading = ParagraphStyle("PdfEmptyHeading", parent=styles["Heading3"], fontName=bold_font)
        empty_body = ParagraphStyle("PdfEmptyBody", parent=styles["BodyText"], fontName=regular_font)
        return [Paragraph(f"<b>{_pdf_escape_text(title)}</b>", empty_heading), Paragraph("No data available", empty_body)]

    safe_columns = [column for column in columns if column in df.columns]
    if not safe_columns:
        empty_heading = ParagraphStyle("PdfMissingHeading", parent=styles["Heading3"], fontName=bold_font)
        empty_body = ParagraphStyle("PdfMissingBody", parent=styles["BodyText"], fontName=regular_font)
        return [Paragraph(f"<b>{_pdf_escape_text(title)}</b>", empty_heading), Paragraph("No valid columns available", empty_body)]

    header_font_size = 9 if roomy else 8.5
    body_font_size = 8.6 if roomy else 8
    header_style = ParagraphStyle(
        "PdfHeader",
        parent=styles["BodyText"],
        fontName=bold_font,
        fontSize=header_font_size,
        leading=header_font_size + 2,
        textColor=colors.white,
    )
    body_style = ParagraphStyle(
        "PdfBody",
        parent=styles["BodyText"],
        fontName=regular_font,
        fontSize=body_font_size,
        leading=body_font_size + 2,
        textColor=colors.HexColor("#0f172a"),
    )

    rows: list[list[Any]] = [[Paragraph(_pdf_escape_text(column), header_style) for column in safe_columns]]
    for _, row in df.loc[:, safe_columns].iterrows():
        rows.append([
            Paragraph(_pdf_escape_text(_format_dashboard_pdf_value(column, row[column])), body_style)
            for column in safe_columns
        ])

    available_width = landscape(A4)[0] - (18 * mm * 2)
    if roomy:
        weights = []
        for column in safe_columns:
            if column in {"Scrip", "Strategy", "Candle Analysis"}:
                weights.append(1.6)
            elif column in {"Total Profit / Loss", "Total PL Amt", "Win Rate %", "Avg Profit Per Trade", "Avg Loss Per Trade", "Avg Net Profit Per Trade"}:
                weights.append(1.25)
            else:
                weights.append(1.0)
        total_weight = sum(weights) or len(safe_columns)
        col_widths = [(available_width * weight) / total_weight for weight in weights]
    else:
        col_width = available_width / max(len(safe_columns), 1)
        col_widths = [col_width] * len(safe_columns)
    table = LongTable(rows, colWidths=col_widths, repeatRows=1)
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f172a")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f8fafc")]),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#d1d5db")),
                ("LEFTPADDING", (0, 0), (-1, -1), 7 if roomy else 5),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7 if roomy else 5),
                ("TOPPADDING", (0, 0), (-1, -1), 5 if roomy else 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5 if roomy else 4),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    table_heading = ParagraphStyle("PdfTableHeading", parent=styles["Heading3"], fontName=bold_font)
    return [Paragraph(f"<b>{_pdf_escape_text(title)}</b>", table_heading), Spacer(1, 3 * mm), table]


def _build_dashboard_pdf_chart_pages(
    chart_specs: list[tuple[str, Any]],
    *,
    regular_font: str,
    bold_font: str,
) -> list[Any]:
    styles = getSampleStyleSheet()
    section_style = ParagraphStyle("PdfChartSection", parent=styles["Heading2"], fontName=bold_font, fontSize=12, leading=14)
    chart_title_style = ParagraphStyle("PdfChartTitle", parent=styles["Heading3"], fontName=bold_font, fontSize=11, leading=13)
    body_style = ParagraphStyle("PdfChartBody", parent=styles["BodyText"], fontName=regular_font, fontSize=9, leading=11)
    elements: list[Any] = []
    max_width = landscape(A4)[0] - (18 * mm * 2)
    max_height = landscape(A4)[1] - (18 * mm * 2) - (18 * mm)

    valid_specs = [(title, fig) for title, fig in chart_specs if fig is not None]
    if not valid_specs:
        return [Paragraph("<b>Charts</b>", section_style), Paragraph("No charts available", body_style)]

    for index, (chart_title, fig) in enumerate(valid_specs):
        if index > 0:
            elements.append(PageBreak())
        if index == 0:
            elements.append(Paragraph("<b>Charts</b>", section_style))
            elements.append(Spacer(1, 2 * mm))
        elements.append(Paragraph(_pdf_escape_text(chart_title), chart_title_style))
        elements.append(Spacer(1, 2 * mm))
        try:
            image_bytes = pio.to_image(fig, format="png", width=1600, height=900, scale=2)
            chart_image = Image(BytesIO(image_bytes))
            chart_image._restrictSize(max_width, max_height)
            elements.append(chart_image)
        except Exception as exc:
            elements.append(Paragraph(f"Chart export unavailable: {_pdf_escape_text(exc)}", body_style))
    return elements


def _build_dashboard_pdf_grouped_details(
    detail_df: pd.DataFrame,
    detail_columns: list[str],
    *,
    detail_title: str,
    detail_group_column: str | None,
    regular_font: str,
    bold_font: str,
) -> list[Any]:
    if detail_df.empty:
        return _build_dashboard_pdf_table(
            detail_df,
            detail_columns,
            title=detail_title,
            regular_font=regular_font,
            bold_font=bold_font,
        )

    if not detail_group_column or detail_group_column not in detail_df.columns:
        return _build_dashboard_pdf_table(
            detail_df,
            detail_columns,
            title=detail_title,
            regular_font=regular_font,
            bold_font=bold_font,
        )

    elements: list[Any] = []
    grouped_df = detail_df.copy()
    group_values = grouped_df[detail_group_column].fillna("Unknown").astype(str)
    grouped_df = grouped_df.assign(__group_value=group_values)
    for index, (group_value, group_df) in enumerate(grouped_df.groupby("__group_value", sort=True)):
        if index > 0:
            elements.append(PageBreak())
        title = f"{detail_title} - {group_value}"
        table_df = group_df.drop(columns=["__group_value"], errors="ignore")
        elements.extend(
            _build_dashboard_pdf_table(
                table_df,
                detail_columns,
                title=title,
                regular_font=regular_font,
                bold_font=bold_font,
            )
        )
    return elements


def build_dashboard_pdf_report(
    *,
    report_title: str,
    output_dir: Path,
    filters_text: str,
    kpi_items: list[tuple[str, Any]],
    advanced_items: list[tuple[str, Any]],
    summary_df: pd.DataFrame,
    detail_df: pd.DataFrame,
    summary_columns: list[str],
    detail_columns: list[str],
    detail_title: str = "Detailed Data",
    chart_specs: list[tuple[str, Any]] | None = None,
    detail_group_column: str | None = None,
) -> bytes:
    regular_font, bold_font = get_dashboard_pdf_fonts()
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "PdfTitle",
        parent=styles["Title"],
        fontName=bold_font,
        fontSize=18,
        leading=22,
        textColor=colors.HexColor("#0f172a"),
    )
    meta_style = ParagraphStyle(
        "PdfMeta",
        parent=styles["BodyText"],
        fontName=regular_font,
        fontSize=8.5,
        leading=10,
        textColor=colors.HexColor("#475569"),
    )
    metric_style = ParagraphStyle(
        "PdfMetric",
        parent=styles["BodyText"],
        fontName=regular_font,
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#0f172a"),
    )
    section_style = ParagraphStyle(
        "PdfSection",
        parent=styles["Heading2"],
        fontName=bold_font,
        fontSize=12,
        leading=14,
        textColor=colors.HexColor("#0f172a"),
    )

    def metric_color(label: str, value: Any) -> str:
        if label in {"Max Drawdown", "Avg Loss Per Trade", "DD Date", "Total Loss Trades"}:
            return "#b91c1c"
        if isinstance(value, (int, float)) and not isinstance(value, bool) and float(value) > 0 and label in {
            "Total PL", "Win Rate %", "Risk Reward Ratio", "Sharpe Ratio", "Avg Profit Per Trade", "Avg Net Profit Per Trade",
        }:
            return "#15803d"
        return "#0f172a"

    def build_metric_table(items: list[tuple[str, Any]]) -> Table:
        cells: list[Any] = []
        for title, value in items:
            display = _format_dashboard_pdf_value(title, value)
            color = metric_color(title, value)
            markup = (
                f"{_pdf_escape_text(title)}<br/>"
                f"<font name=\"{bold_font}\" color=\"{color}\">{_pdf_escape_text(display)}</font>"
            )
            cells.append(Paragraph(markup, metric_style))
        row_size = 3
        rows = [cells[index:index + row_size] for index in range(0, len(cells), row_size)]
        if rows and len(rows[-1]) < row_size:
            rows[-1].extend([""] * (row_size - len(rows[-1])))
        table = Table(rows, colWidths=[(landscape(A4)[0] - (18 * mm * 2)) / row_size] * row_size)
        table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f8fafc")),
                    ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#d1d5db")),
                    ("INNERGRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#e2e8f0")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 7),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                    ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
                ]
            )
        )
        return table

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(A4),
        leftMargin=18 * mm,
        rightMargin=18 * mm,
        topMargin=14 * mm,
        bottomMargin=14 * mm,
    )
    generated_at = pd.Timestamp.now().strftime("%d-%b-%Y %I:%M %p")
    elements: list[Any] = [
        Paragraph(_pdf_escape_text(report_title), title_style),
        Spacer(1, 2 * mm),
        Paragraph(f"Generated: {_pdf_escape_text(generated_at)}", meta_style),
        Paragraph(f"Filters: {_pdf_escape_text(filters_text)}", meta_style),
        Spacer(1, 4 * mm),
        Paragraph("KPI Overview", section_style),
        build_metric_table(kpi_items),
        Spacer(1, 4 * mm),
        Paragraph("Advanced Metrics", section_style),
        build_metric_table(advanced_items),
    ]
    elements.append(PageBreak())
    elements.extend(
        _build_dashboard_pdf_table(
            summary_df,
            summary_columns,
            title="Summary",
            regular_font=regular_font,
            bold_font=bold_font,
            roomy=True,
        )
    )
    if chart_specs:
        elements.append(PageBreak())
        elements.extend(
            _build_dashboard_pdf_chart_pages(
                chart_specs,
                regular_font=regular_font,
                bold_font=bold_font,
            )
        )
    elements.append(PageBreak())
    elements.extend(
        _build_dashboard_pdf_grouped_details(
            detail_df,
            detail_columns,
            detail_title=detail_title,
            detail_group_column=detail_group_column,
            regular_font=regular_font,
            bold_font=bold_font,
        )
    )
    doc.build(elements)
    buffer.seek(0)
    return buffer.getvalue()


def _normalize_dashboard_score_series(series: pd.Series, *, inverse: bool = False) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0).astype(float)
    if numeric.empty:
        return numeric
    min_value = float(numeric.min())
    max_value = float(numeric.max())
    if max_value - min_value <= DASHBOARD_EPSILON:
        normalized = pd.Series(1.0, index=numeric.index, dtype=float)
    else:
        normalized = (numeric - min_value) / (max_value - min_value)
    if inverse:
        return 1.0 - normalized
    return normalized


def apply_strategy_scorecard(comparison_df: pd.DataFrame) -> pd.DataFrame:
    if comparison_df.empty:
        return comparison_df.copy()

    scored_df = comparison_df.copy()
    normalized_pl = _normalize_dashboard_score_series(scored_df["Total PL Amt"])
    normalized_sharpe = _normalize_dashboard_score_series(scored_df["Sharpe Ratio"])
    normalized_expectancy = _normalize_dashboard_score_series(scored_df["Avg Net Profit Per Trade"])
    normalized_winrate = _normalize_dashboard_score_series(scored_df["Win Rate %"])
    inverse_drawdown = _normalize_dashboard_score_series(scored_df["Max Drawdown"].abs(), inverse=True)
    inverse_dd_duration = _normalize_dashboard_score_series(scored_df["Drawdown Duration"], inverse=True)

    scored_df["Score"] = (
        (0.25 * normalized_pl)
        + (0.20 * normalized_sharpe)
        + (0.20 * normalized_expectancy)
        + (0.15 * normalized_winrate)
        + (0.10 * inverse_drawdown)
        + (0.10 * inverse_dd_duration)
    )
    scored_df["Rank"] = scored_df["Score"].rank(method="dense", ascending=False).astype(int)
    scored_df = scored_df.sort_values(
        ["Score", "Total PL Amt", "Strategy"],
        ascending=[False, False, True],
        kind="stable",
    ).reset_index(drop=True)
    return scored_df


def build_dashboard_metrics(filtered_df: pd.DataFrame) -> dict[str, Any]:
    closed_df = filtered_df[filtered_df["is_closed"]].copy() if not filtered_df.empty else filtered_df.copy()
    equity_df = build_dashboard_equity_curve(closed_df)
    closed_trades = int(filtered_df["is_closed"].sum()) if "is_closed" in filtered_df.columns else 0
    expectancy_metrics = compute_dashboard_expectancy_metrics(closed_df)
    max_drawdown = float(equity_df["Drawdown"].min()) if not equity_df.empty else 0.0
    dd_date = "-"
    if not equity_df.empty and "Drawdown" in equity_df.columns:
        dd_index = equity_df["Drawdown"].idxmin()
        if pd.notna(dd_index) and dd_index in equity_df.index:
            dd_row = equity_df.loc[dd_index]
            dd_date_value = dd_row.get("Entry Date")
            if pd.notna(dd_date_value):
                dd_date = str(dd_date_value)
            elif pd.notna(dd_row.get("Entry Timestamp")):
                dd_date = pd.Timestamp(dd_row["Entry Timestamp"]).strftime("%d-%b-%Y")
    return {
        "total_scrips": int(filtered_df["Scrip"].nunique()) if not filtered_df.empty else 0,
        "total_trades": int(len(filtered_df)),
        "closed_trades": closed_trades,
        "open_trades": int(filtered_df["is_open"].sum()) if "is_open" in filtered_df.columns else 0,
        "total_pl_points": float(pd.to_numeric(filtered_df.get("PL Points"), errors="coerce").fillna(0).sum()) if not filtered_df.empty else 0.0,
        "total_pl_amt": float(pd.to_numeric(filtered_df.get("PL Amt"), errors="coerce").fillna(0).sum()) if not filtered_df.empty else 0.0,
        "win_rate": float(expectancy_metrics["win_rate"]),
        "wins": int(expectancy_metrics["wins"]),
        "losses": int(expectancy_metrics["losses"]),
        "sharpe_ratio": compute_dashboard_sharpe(closed_df),
        "max_drawdown": max_drawdown,
        "max_drawdown_duration": compute_dashboard_drawdown_duration(equity_df),
        "avg_profit_per_trade": float(expectancy_metrics["avg_win"]),
        "avg_loss_per_trade": float(expectancy_metrics["avg_loss"]) if int(expectancy_metrics["losses"]) else 0.0,
        "avg_net_profit_per_trade": float(expectancy_metrics["avg_net"]),
        "risk_reward_ratio": float(expectancy_metrics["risk_reward_ratio"]),
        "avg_win": float(expectancy_metrics["avg_win"]),
        "avg_loss": float(expectancy_metrics["avg_loss"]),
        "dd_date": dd_date,
        "equity_df": equity_df,
    }


def apply_dashboard_cost_model(
    filtered_df: pd.DataFrame,
    *,
    estimated_charges_per_trade: float = 0.0,
    prop_dashboard_enabled: bool = False,
    avg_value_traded_per_lot: float = 0.0,
    leverage: float = 1.0,
    interest_rate_pct: float = 12.0,
    selected_scrip_count: int = 0,
) -> tuple[pd.DataFrame, dict[str, float]]:
    adjusted_df = filtered_df.copy()
    charges_per_trade = max(float(estimated_charges_per_trade or 0.0), 0.0)
    scrip_count = max(int(selected_scrip_count or 0), 0)
    capital = max(float(avg_value_traded_per_lot or 0.0), 0.0) * 0.25 * 0.20 * scrip_count
    leverage_value = max(float(leverage or 0.0), 0.0)
    interest_rate_value = max(float(interest_rate_pct or 0.0), 0.0)
    monthly_interest_total = capital * leverage_value * (interest_rate_value / 100.0) * scrip_count if prop_dashboard_enabled else 0.0

    if adjusted_df.empty:
        return adjusted_df, {
            "estimated_charges_per_trade": charges_per_trade,
            "total_estimated_charges": 0.0,
            "capital": capital,
            "leverage": leverage_value,
            "interest_rate_pct": interest_rate_value,
            "monthly_interest_total": monthly_interest_total,
            "total_interest_deducted": 0.0,
            "roi_pct": 0.0,
        }

    base_pl = pd.to_numeric(adjusted_df.get("PL Amt"), errors="coerce")
    closed_mask = base_pl.notna()
    adjusted_df["Gross PL Amount"] = base_pl
    adjusted_df["Estimated Charges"] = 0.0
    adjusted_df.loc[closed_mask, "Estimated Charges"] = charges_per_trade
    adjusted_df["Net PL Amount"] = base_pl - adjusted_df["Estimated Charges"]
    adjusted_df["PL Amt"] = adjusted_df["Net PL Amount"]
    adjusted_df["is_win"] = closed_mask & adjusted_df["PL Amt"].gt(0).fillna(False)
    adjusted_df["is_loss"] = closed_mask & adjusted_df["PL Amt"].lt(0).fillna(False)

    total_pl_amt = float(pd.to_numeric(adjusted_df.get("PL Amt"), errors="coerce").fillna(0).sum())
    interest_months = pd.to_datetime(adjusted_df.loc[closed_mask, "Entry Timestamp"], errors="coerce").dt.to_period("M").dropna().astype(str).unique().tolist()
    total_interest_deducted = monthly_interest_total * len(interest_months) if prop_dashboard_enabled else 0.0
    roi_base = capital * leverage_value
    roi_pct = ((total_pl_amt - total_interest_deducted) / roi_base * 100.0) if prop_dashboard_enabled and roi_base > 0 else 0.0

    return adjusted_df, {
        "estimated_charges_per_trade": charges_per_trade,
        "total_estimated_charges": float(adjusted_df["Estimated Charges"].sum()),
        "capital": capital,
        "leverage": leverage_value,
        "interest_rate_pct": interest_rate_value,
        "monthly_interest_total": monthly_interest_total,
        "total_interest_deducted": float(total_interest_deducted),
        "roi_pct": roi_pct,
    }


def build_dashboard_summary_table(filtered_df: pd.DataFrame) -> pd.DataFrame:
    if filtered_df.empty:
        return pd.DataFrame(columns=[
            "Scrip",
            "Trades",
            "Closed Trades",
            "Open Trades",
            "Wins",
            "Losses",
            "Total PL Points",
            "Total PL Amt",
            "Win Rate %",
        ])
    summary_df = (
        filtered_df.groupby("Scrip", dropna=False)
        .agg(
            Trades=("Scrip", "size"),
            Closed_Trades=("is_closed", "sum"),
            Open_Trades=("is_open", "sum"),
            Wins=("is_win", "sum"),
            Losses=("is_loss", "sum"),
            Total_PL_Points=("PL Points", "sum"),
            Total_PL_Amt=("PL Amt", "sum"),
        )
        .reset_index()
    )
    summary_df["Win Rate %"] = summary_df.apply(
        lambda row: (float(row["Wins"]) / float(row["Closed_Trades"]) * 100.0) if float(row["Closed_Trades"]) else 0.0,
        axis=1,
    )
    return summary_df.rename(
        columns={
            "Closed_Trades": "Closed Trades",
            "Open_Trades": "Open Trades",
            "Total_PL_Points": "Total PL Points",
            "Total_PL_Amt": "Total PL Amt",
        }
    ).sort_values(["Total PL Amt", "Scrip"], ascending=[False, True], kind="stable").reset_index(drop=True)


def style_dashboard_table(table_df: pd.DataFrame) -> pd.io.formats.style.Styler:
    safe_df = table_df.copy()
    currency_columns = {
        "Price", "Entry Price", "Exit Price", "PL Amt", "Total PL Amt",
        "Total Profit / Loss", "Total Profit/Loss", "Profit / Loss",
        "Avg Profit Per Trade", "Avg Loss Per Trade", "Avg Net Profit Per Trade",
        "Max Drawdown", "Total PL", "Gross PL Amount", "Estimated Charges", "Charges",
        "Net PL Amount", "Interest Deducted",
    }
    number_columns = {
        "PL Points", "Total PL Points", "Win Rate %", "Avg PL Points", "Sharpe Ratio",
        "Risk-Reward Ratio", "Risk Reward Ratio", "Score",
    }

    def fmt_value(column: str, value: Any) -> str:
        if pd.isna(value):
            return ""
        if column in {"Rank", "Sr.No", "Qty", "Trades", "Closed Trades", "Open Trades", "Wins", "Losses", "Drawdown Duration", "Max DD Duration", "Total Profit Trades", "Total Loss Trades"}:
            return f"{int(value)}"
        if column in currency_columns:
            return format_inr(value)
        if column in number_columns:
            return f"{float(value):.2f}"
        return str(value)

    formatters = {column: (lambda value, col=column: fmt_value(col, value)) for column in safe_df.columns}

    def row_styles(row: pd.Series) -> list[str]:
        styles = [""] * len(row)
        for idx, column in enumerate(row.index):
            if column not in {
                "PL Amt", "Total PL Amt", "PL Points", "Total PL Points", "Avg PL Points",
                "Total PL", "Score", "Max Drawdown", "Profit / Loss", "Total Profit / Loss",
                "Avg Profit Per Trade", "Avg Loss Per Trade", "Avg Net Profit Per Trade",
                "Gross PL Amount", "Estimated Charges", "Charges", "Net PL Amount", "Interest Deducted",
            }:
                continue
            value = row[column]
            if pd.isna(value):
                continue
            if column in {"Estimated Charges", "Charges", "Interest Deducted"}:
                if float(value) > 0:
                    styles[idx] = "color: #b91c1c; font-weight: 700;"
                continue
            if float(value) > 0:
                styles[idx] = "color: #15803d; font-weight: 700;"
            elif float(value) < 0:
                styles[idx] = "color: #b91c1c; font-weight: 700;"
        if "Rank" in row.index and pd.notna(row["Rank"]) and int(row["Rank"]) == 1:
            styles = [style + " background-color: rgba(34,197,94,0.12);" for style in styles]
        return styles

    return (
        safe_df.style
        .apply(row_styles, axis=1)
        .format(formatters)
        .set_properties(**{"text-align": "center"})
        .set_table_styles(
            [
                {"selector": "th", "props": [("padding", "10px 12px"), ("font-size", "13px")]},
                {"selector": "td", "props": [("padding", "9px 12px"), ("font-size", "13px")]},
            ]
        )
    )


def render_dashboard_metric(
    cell,
    label: str,
    value: Any,
    *,
    percent: bool = False,
) -> dict[str, str]:
    numeric_value: float | None = None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric_value = float(value)

    currency_labels = {
        "Total PL Amount",
        "Total Profit / Loss",
        "Avg Profit Per Trade",
        "Avg Loss Per Trade",
        "Avg Net Profit Per Trade",
        "Max Drawdown",
        "Total PL",
    }

    if percent and numeric_value is not None:
        display_value = f"{numeric_value:.2f}%"
    elif label in currency_labels and numeric_value is not None:
        display_value = format_inr(numeric_value)
    elif label in {"Sharpe Ratio", "Risk Reward Ratio"} and numeric_value is not None:
        display_value = f"{numeric_value:.2f}"
    elif isinstance(value, int):
        display_value = f"{value}"
    elif isinstance(value, float):
        display_value = f"{value:.2f}"
    else:
        display_value = str(value)

    cell.metric(label, display_value)
    metric_color = "#dc2626" if (numeric_value is not None and numeric_value < 0) else "#0f172a"
    return {"label": label, "value": display_value, "color": metric_color}


def build_dashboard_summary_column_config() -> dict[str, Any]:
    return {
        "Scrip": st.column_config.TextColumn("Scrip", width="large"),
        "Trades": st.column_config.NumberColumn("Trades", width="small"),
        "Closed Trades": st.column_config.NumberColumn("Closed Trades", width="small"),
        "Open Trades": st.column_config.NumberColumn("Open Trades", width="small"),
        "Wins": st.column_config.NumberColumn("Wins", width="small"),
        "Losses": st.column_config.NumberColumn("Losses", width="small"),
        "Win Rate %": st.column_config.TextColumn("Win Rate %", width="medium"),
        "Total Profit / Loss": st.column_config.TextColumn("Total Profit / Loss", width="medium"),
    }


def build_time_analysis_table(
    filtered_df: pd.DataFrame,
    granularity: str,
    *,
    prop_dashboard_enabled: bool = False,
    monthly_interest_total: float = 0.0,
) -> pd.DataFrame:
    if filtered_df.empty:
        return pd.DataFrame(
            columns=[
                "Period", "Trades", "Wins", "Losses", "Win Rate %", "Total Profit / Loss",
                "Avg Profit Per Trade", "Avg Loss Per Trade", "Avg Net Profit Per Trade",
            ]
        )

    working_df = filtered_df.copy()
    timestamps = pd.to_datetime(working_df["Entry Timestamp"], errors="coerce")
    if granularity == "Year":
        working_df["Period"] = timestamps.dt.strftime("%Y")
    elif granularity == "Quarter":
        working_df["Period"] = timestamps.dt.to_period("Q").astype(str)
    elif granularity == "Month":
        working_df["Period"] = timestamps.dt.strftime("%b %Y")
    elif granularity == "Week":
        iso_year = timestamps.dt.isocalendar().year.astype(str)
        iso_week = timestamps.dt.isocalendar().week.astype(str).str.zfill(2)
        working_df["Period"] = "W" + iso_week + " " + iso_year
    else:
        working_df["Period"] = timestamps.dt.strftime("%d-%b-%Y")

    grouped = (
        working_df.groupby("Period", dropna=False)
        .agg(
            Trades=("Scrip", "size"),
            Wins=("is_win", "sum"),
            Losses=("is_loss", "sum"),
            Total_PL_Amt=("PL Amt", "sum"),
            Avg_Profit=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").where(pd.to_numeric(s, errors="coerce") > 0).dropna().mean()),
            Avg_Loss=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").where(pd.to_numeric(s, errors="coerce") < 0).dropna().abs().mean()),
            Avg_Net=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").dropna().mean()),
        )
        .reset_index()
    )
    grouped["Win Rate %"] = grouped.apply(
        lambda row: (float(row["Wins"]) / float(row["Trades"]) * 100.0) if float(row["Trades"]) else 0.0,
        axis=1,
    )
    grouped = grouped.rename(
        columns={
            "Total_PL_Amt": "Total Profit / Loss",
            "Avg_Profit": "Avg Profit Per Trade",
            "Avg_Loss": "Avg Loss Per Trade",
            "Avg_Net": "Avg Net Profit Per Trade",
        }
    )
    if prop_dashboard_enabled and monthly_interest_total > 0:
        month_period_df = working_df.loc[working_df["is_closed"], ["Period", "Entry Timestamp"]].copy()
        month_period_df["Interest Month"] = pd.to_datetime(month_period_df["Entry Timestamp"], errors="coerce").dt.to_period("M").astype(str)
        month_period_df = month_period_df[
            month_period_df["Period"].notna()
            & month_period_df["Interest Month"].notna()
            & month_period_df["Interest Month"].ne("NaT")
        ].drop_duplicates()
        if not month_period_df.empty:
            month_period_counts = month_period_df.groupby("Interest Month")["Period"].nunique()
            month_period_df["Interest Deducted"] = month_period_df["Interest Month"].map(
                lambda month_value: monthly_interest_total / float(month_period_counts.get(month_value, 1))
            )
            interest_by_period = month_period_df.groupby("Period")["Interest Deducted"].sum()
            grouped["Interest Deducted"] = grouped["Period"].map(interest_by_period).fillna(0.0)
            grouped["Total Profit / Loss"] = grouped["Total Profit / Loss"] - grouped["Interest Deducted"]
            grouped["Avg Net Profit Per Trade"] = grouped.apply(
                lambda row: (float(row["Total Profit / Loss"]) / float(row["Trades"])) if float(row["Trades"]) else 0.0,
                axis=1,
            )
        else:
            grouped["Interest Deducted"] = 0.0
    return grouped.sort_values("Period", kind="stable").reset_index(drop=True)


def build_scrip_analysis_table(filtered_df: pd.DataFrame) -> pd.DataFrame:
    if filtered_df.empty:
        return pd.DataFrame(
            columns=[
                "Scrip", "Trades", "Wins", "Losses", "Win Rate %", "Total Profit / Loss",
                "Avg Profit Per Trade", "Avg Loss Per Trade", "Avg Net Profit Per Trade",
                "Best Trade", "Worst Trade",
            ]
        )

    grouped = (
        filtered_df.groupby("Scrip", dropna=False)
        .agg(
            Trades=("Scrip", "size"),
            Wins=("is_win", "sum"),
            Losses=("is_loss", "sum"),
            Total_PL_Amt=("PL Amt", "sum"),
            Avg_Profit=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").where(pd.to_numeric(s, errors="coerce") > 0).dropna().mean()),
            Avg_Loss=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").where(pd.to_numeric(s, errors="coerce") < 0).dropna().abs().mean()),
            Avg_Net=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").dropna().mean()),
            Best_Trade=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").dropna().max()),
            Worst_Trade=("PL Amt", lambda s: pd.to_numeric(s, errors="coerce").dropna().min()),
        )
        .reset_index()
    )
    grouped["Win Rate %"] = grouped.apply(
        lambda row: (float(row["Wins"]) / float(row["Trades"]) * 100.0) if float(row["Trades"]) else 0.0,
        axis=1,
    )
    grouped = grouped.rename(
        columns={
            "Total_PL_Amt": "Total Profit / Loss",
            "Avg_Profit": "Avg Profit Per Trade",
            "Avg_Loss": "Avg Loss Per Trade",
            "Avg_Net": "Avg Net Profit Per Trade",
            "Best_Trade": "Best Trade",
            "Worst_Trade": "Worst Trade",
        }
    )
    return grouped.sort_values(["Total Profit / Loss", "Scrip"], ascending=[False, True], kind="stable").reset_index(drop=True)


def _build_pivot_period_column(filtered_df: pd.DataFrame, granularity: str) -> pd.Series:
    timestamps = pd.to_datetime(filtered_df["Entry Timestamp"], errors="coerce")
    if granularity == "Year":
        return timestamps.dt.strftime("%Y")
    if granularity == "Quarter":
        return timestamps.dt.to_period("Q").astype(str)
    if granularity == "Month":
        return timestamps.dt.strftime("%b %Y")
    if granularity == "Week":
        iso_year = timestamps.dt.isocalendar().year.astype(str)
        iso_week = timestamps.dt.isocalendar().week.astype(str).str.zfill(2)
        return "W" + iso_week + " " + iso_year
    return timestamps.dt.strftime("%d-%b-%Y")


def build_pivot_analysis_table(filtered_df: pd.DataFrame, granularity: str, value_metric: str) -> pd.DataFrame:
    if filtered_df.empty:
        return pd.DataFrame()

    working_df = filtered_df.copy()
    working_df["Pivot Period"] = _build_pivot_period_column(working_df, granularity)
    if value_metric == "Total Profit / Loss":
        pivot_df = pd.pivot_table(
            working_df,
            index="Pivot Period",
            columns="Scrip",
            values="PL Amt",
            aggfunc="sum",
            fill_value=0.0,
        )
    elif value_metric == "Trades":
        pivot_df = pd.pivot_table(
            working_df,
            index="Pivot Period",
            columns="Scrip",
            values="Scrip",
            aggfunc="count",
            fill_value=0,
        )
    elif value_metric == "Win Rate %":
        win_rate_df = (
            working_df.groupby(["Pivot Period", "Scrip"], dropna=False)
            .agg(Trades=("Scrip", "size"), Wins=("is_win", "sum"))
            .reset_index()
        )
        win_rate_df["Value"] = win_rate_df.apply(
            lambda row: (float(row["Wins"]) / float(row["Trades"]) * 100.0) if float(row["Trades"]) else 0.0,
            axis=1,
        )
        pivot_df = pd.pivot_table(
            win_rate_df,
            index="Pivot Period",
            columns="Scrip",
            values="Value",
            aggfunc="first",
            fill_value=0.0,
        )
    else:
        avg_net_df = (
            working_df.groupby(["Pivot Period", "Scrip"], dropna=False)["PL Amt"]
            .mean()
            .reset_index(name="Value")
        )
        pivot_df = pd.pivot_table(
            avg_net_df,
            index="Pivot Period",
            columns="Scrip",
            values="Value",
            aggfunc="first",
            fill_value=0.0,
        )

    pivot_df = pivot_df.sort_index(kind="stable")
    pivot_df.columns = [str(column) for column in pivot_df.columns]
    pivot_df = pivot_df.reset_index().rename(columns={"Pivot Period": granularity})
    return pivot_df


def style_pivot_table(pivot_df: pd.DataFrame, value_metric: str) -> pd.io.formats.style.Styler:
    safe_df = pivot_df.copy()
    value_columns = [column for column in safe_df.columns if column != safe_df.columns[0]]

    def fmt_value(column: str, value: Any) -> str:
        if pd.isna(value):
            return ""
        if column not in value_columns:
            return str(value)
        if value_metric in {"Total Profit / Loss", "Avg Net Profit Per Trade"}:
            return format_inr(value)
        if value_metric == "Win Rate %":
            return f"{float(value):.2f}%"
        return f"{int(round(float(value)))}"

    formatters = {column: (lambda value, col=column: fmt_value(col, value)) for column in safe_df.columns}

    def row_styles(row: pd.Series) -> list[str]:
        styles = [""] * len(row)
        for idx, column in enumerate(row.index):
            if column not in value_columns:
                continue
            value = row[column]
            if pd.isna(value):
                continue
            if value_metric in {"Total Profit / Loss", "Avg Net Profit Per Trade"}:
                if float(value) > 0:
                    styles[idx] = "color: #15803d; font-weight: 700;"
                elif float(value) < 0:
                    styles[idx] = "color: #b91c1c; font-weight: 700;"
        return styles

    return (
        safe_df.style
        .apply(row_styles, axis=1)
        .format(formatters)
        .set_properties(**{"text-align": "center"})
        .set_table_styles(
            [
                {"selector": "th", "props": [("padding", "10px 12px"), ("font-size", "13px")]},
                {"selector": "td", "props": [("padding", "9px 12px"), ("font-size", "13px")]},
            ]
        )
    )


def render_dashboard_box(
    cell,
    title: str,
    value: Any,
    *,
    percent: bool = False,
    force_green: bool = False,
    force_red: bool = False,
) -> None:
    numeric_value: float | None = None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric_value = float(value)

    currency_labels = {
        "Total PL",
        "Avg Profit Per Trade",
        "Avg Loss Per Trade",
        "Avg Net Profit Per Trade",
        "Max Drawdown",
        "Interest / Month",
    }

    if percent and numeric_value is not None:
        display_value = f"{numeric_value:.2f}%"
    elif title in currency_labels and numeric_value is not None:
        display_value = format_inr(numeric_value)
    elif title in {"Sharpe Ratio", "Risk Reward Ratio"} and numeric_value is not None:
        display_value = f"{numeric_value:.2f}"
    elif isinstance(value, int):
        display_value = str(value)
    elif isinstance(value, float):
        display_value = f"{value:.2f}"
    else:
        display_value = str(value)

    color_class = ""
    if force_red:
        color_class = "card-red"
    elif force_green:
        color_class = "card-green"

    cell.markdown(
        f"""
        <div class="card">
            <div class="card-title">{html.escape(title)}</div>
            <div class="card-value {color_class}">{html.escape(display_value)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


LIGHT_POSITIVE = "#15803d"
LIGHT_NEGATIVE = "#b91c1c"
LIGHT_NEUTRAL = "#e5e7eb"
LIGHT_LINES = ["#2563eb", "#0f766e", "#7c3aed", "#ea580c", "#0891b2", "#db2777"]


def style_dashboard_chart(fig, *, height: int = 360, xaxis_title: str = "", yaxis_title: str = "", hovermode: str | None = "x unified"):
    fig.update_layout(
        height=height,
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        coloraxis_showscale=False,
        paper_bgcolor="rgba(255,255,255,0)",
        plot_bgcolor="rgba(248,250,252,0.55)",
        margin=dict(l=20, r=20, t=48, b=20),
        font=dict(color="#334155"),
        title_font=dict(size=16, color="#0f172a"),
        legend=dict(bgcolor="rgba(255,255,255,0.75)"),
    )
    if hovermode is not None:
        fig.update_layout(hovermode=hovermode)
    fig.update_xaxes(showgrid=False, zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="rgba(148,163,184,0.18)", zeroline=False)
    for trace in fig.data:
        trace_type = getattr(trace, "type", "")
        if trace_type == "bar":
            trace.opacity = 0.9
            if hasattr(trace, "marker") and getattr(trace.marker, "line", None) is not None:
                trace.marker.line.width = 0
        elif trace_type == "scatter":
            if hasattr(trace, "line"):
                trace.line.width = 3
        elif trace_type == "pie":
            trace.opacity = 0.9
    return fig


def build_single_dashboard_chart_specs(summary_df: pd.DataFrame, filtered_df: pd.DataFrame, metrics: dict[str, Any]) -> tuple[list[tuple[str, Any]], list[tuple[str, Any]]]:
    sorted_summary_df = summary_df.sort_values(["Total PL Amt", "Scrip"], ascending=[False, True], kind="stable").reset_index(drop=True)
    win_loss_df = pd.DataFrame({
        "Outcome": ["Wins", "Losses"],
        "Count": [int(filtered_df["is_win"].sum()), int(filtered_df["is_loss"].sum())],
    })

    pnl_fig = px.bar(
        sorted_summary_df,
        x="Scrip",
        y="Total PL Amt",
        color="Total PL Amt",
        color_continuous_scale=[LIGHT_NEGATIVE, "#f8fafc", LIGHT_POSITIVE],
        title="Profit / Loss by Scrip",
    )
    style_dashboard_chart(pnl_fig, height=340, yaxis_title="Profit / Loss", hovermode="x unified")

    win_loss_fig = px.pie(
        win_loss_df,
        values="Count",
        names="Outcome",
        hole=0.55,
        color="Outcome",
        color_discrete_map={"Wins": LIGHT_POSITIVE, "Losses": LIGHT_NEGATIVE},
        title="Win vs Loss",
    )
    style_dashboard_chart(win_loss_fig, height=340, hovermode=None)

    if metrics["equity_df"].empty:
        equity_curve_fig = None
    else:
        equity_curve_fig = px.area(
            metrics["equity_df"],
            x="Entry Timestamp",
            y="Equity Curve",
            title="Equity Curve",
            color_discrete_sequence=[LIGHT_LINES[0]],
        )
        equity_curve_fig.update_traces(
            line=dict(color=LIGHT_LINES[0], width=3),
            fillcolor="rgba(37,99,235,0.30)",
        )
        equity_curve_fig.update_xaxes(
            rangeslider_visible=False,
            rangeselector=dict(
                buttons=[
                    dict(count=1, label="1M", step="month", stepmode="backward"),
                    dict(count=3, label="3M", step="month", stepmode="backward"),
                    dict(count=6, label="6M", step="month", stepmode="backward"),
                    dict(count=1, label="1Y", step="year", stepmode="backward"),
                    dict(step="all", label="ALL"),
                ]
            ),
        )
        style_dashboard_chart(equity_curve_fig, height=460, yaxis_title="Equity Curve")

    top_row = [
        ("Profit / Loss by Scrip", pnl_fig),
        ("Win vs Loss", win_loss_fig),
    ]
    bottom_row = [
        ("Equity Curve", equity_curve_fig),
    ]
    return top_row, bottom_row


def build_strategy_dashboard_chart_specs(comparison_df: pd.DataFrame, strategy_equity_df: pd.DataFrame) -> tuple[list[tuple[str, Any]], list[tuple[str, Any]]]:
    if strategy_equity_df.empty:
        equity_fig = None
    else:
        equity_fig = px.line(
            strategy_equity_df.sort_values(["Entry Timestamp", "Strategy"], kind="stable"),
            x="Entry Timestamp",
            y="Equity Curve",
            color="Strategy",
            title="Equity Curve Comparison",
            color_discrete_sequence=LIGHT_LINES,
        )
        style_dashboard_chart(equity_fig, height=360, yaxis_title="Equity")

    total_pl_fig = px.bar(
        comparison_df,
        x="Strategy",
        y="Total PL Amt",
        color="Total PL Amt",
        color_continuous_scale=[LIGHT_NEGATIVE, "#f8fafc", LIGHT_POSITIVE],
        title="Profit / Loss Comparison",
    )
    style_dashboard_chart(total_pl_fig, height=360, yaxis_title="Total Profit / Loss", hovermode="x unified")

    sharpe_fig = px.bar(
        comparison_df,
        x="Strategy",
        y="Sharpe Ratio",
        color="Sharpe Ratio",
        color_continuous_scale=[LIGHT_NEGATIVE, "#f8fafc", LIGHT_POSITIVE],
        title="Sharpe Comparison",
    )
    style_dashboard_chart(sharpe_fig, height=360, yaxis_title="Sharpe Ratio", hovermode="x unified")

    drawdown_fig = px.bar(
        comparison_df,
        x="Strategy",
        y="Max Drawdown",
        color="Max Drawdown",
        color_continuous_scale=[LIGHT_POSITIVE, "#f8fafc", LIGHT_NEGATIVE],
        title="Max Drawdown Comparison",
    )
    style_dashboard_chart(drawdown_fig, height=360, yaxis_title="Max Drawdown", hovermode="x unified")

    expectancy_fig = px.bar(
        comparison_df,
        x="Strategy",
        y="Avg Net Profit Per Trade",
        color="Avg Net Profit Per Trade",
        color_continuous_scale=[LIGHT_NEGATIVE, "#f8fafc", LIGHT_POSITIVE],
        title="Avg Net Profit Per Trade Comparison",
    )
    style_dashboard_chart(expectancy_fig, height=360, yaxis_title="Avg Net Profit / Trade", hovermode="x unified")

    dd_duration_fig = px.bar(
        comparison_df,
        x="Strategy",
        y="Drawdown Duration",
        color="Drawdown Duration",
        color_continuous_scale=[LIGHT_POSITIVE, "#f8fafc", LIGHT_NEGATIVE],
        title="Drawdown Duration Comparison",
    )
    style_dashboard_chart(dd_duration_fig, height=360, yaxis_title="Drawdown Duration", hovermode="x unified")

    overview = [
        ("Equity Curve Comparison", equity_fig),
        ("Profit / Loss Comparison", total_pl_fig),
        ("Sharpe Comparison", sharpe_fig),
    ]
    detailed = overview + [
        ("Max Drawdown Comparison", drawdown_fig),
        ("Avg Net Profit Per Trade Comparison", expectancy_fig),
        ("Drawdown Duration Comparison", dd_duration_fig),
    ]
    return overview, detailed


def render_detailed_charts_panel(title: str, chart_specs: list[tuple[str, Any]]) -> None:
    st.markdown(
        """
        <style>
        div[data-testid="stDialog"] > div[role="dialog"] {
            width: 96vw !important;
            max-width: 96vw !important;
        }
        div[data-testid="stDialog"] section[tabindex="0"] {
            max-height: 90vh !important;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )
    header_col, close_col = st.columns([0.82, 0.18])
    with header_col:
        st.markdown(f"### {title}")
    with close_col:
        if st.button("Close Detailed View", key=f"close-{title}"):
            st.session_state["dashboard_chart_focus"] = None
            st.rerun()
    for index in range(0, len(chart_specs), 2):
        row_cols = st.columns(2)
        for cell, (chart_title, fig) in zip(row_cols, chart_specs[index:index + 2]):
            with cell:
                st.markdown(f"#### {chart_title}")
                if fig is None:
                    st.info("No data available")
                else:
                    st.plotly_chart(fig, width="stretch")


def build_strategy_comparison_dashboard(
    output_dir: Path,
    start_date: Any,
    end_date: Any,
    include_open_trades: bool,
    selected_scrips: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    strategy_rows: list[dict[str, Any]] = []
    equity_rows: list[pd.DataFrame] = []
    for strategy_dir in dashboard_strategy_dirs(output_dir):
        strategy_df = load_dashboard_trade_rows(
            str(strategy_dir),
            dashboard_folder_signature(strategy_dir),
            strategy_name=strategy_dir.name,
        )
        filtered_df = filter_dashboard_trade_rows(
            strategy_df,
            start_date,
            end_date,
            include_open_trades,
            selected_scrips=selected_scrips,
        )
        metrics = build_dashboard_metrics(filtered_df)
        strategy_rows.append(
            {
                "Strategy": strategy_dir.name,
                "Trades": int(len(filtered_df)),
                "Closed Trades": int(metrics["closed_trades"]),
                "Open Trades": int(metrics["open_trades"]),
                "Total Profit Trades": int(metrics["wins"]),
                "Total Loss Trades": int(metrics["losses"]),
                "Total PL Points": float(metrics["total_pl_points"]),
                "Win Rate %": float(metrics["win_rate"]),
                "Total PL Amt": float(metrics["total_pl_amt"]),
                "Sharpe Ratio": float(metrics["sharpe_ratio"]),
                "Max Drawdown": float(metrics["max_drawdown"]),
                "Drawdown Duration": int(metrics["max_drawdown_duration"]),
                "Avg Profit Per Trade": float(metrics["avg_profit_per_trade"]),
                "Avg Loss Per Trade": float(metrics["avg_loss_per_trade"]),
                "Avg Net Profit Per Trade": float(metrics["avg_net_profit_per_trade"]),
                "Risk Reward Ratio": float(metrics["risk_reward_ratio"]),
                "DD Date": metrics["dd_date"],
            }
        )
        if not metrics["equity_df"].empty:
            equity_rows.append(
                metrics["equity_df"].loc[:, ["Entry Timestamp", "Equity Curve"]]
                .assign(Strategy=strategy_dir.name)
            )
    comparison_df = pd.DataFrame(strategy_rows)
    if not comparison_df.empty:
        comparison_df = apply_strategy_scorecard(comparison_df)
    equity_df = pd.concat(equity_rows, ignore_index=True) if equity_rows else pd.DataFrame(columns=["Entry Timestamp", "Equity Curve", "Strategy"])
    return comparison_df, equity_df


def render_interactive_output_dashboard(output_dir: Path) -> None:
    st.markdown(CARD_STYLE, unsafe_allow_html=True)
    root_signature = dashboard_folder_signature(output_dir)
    strategy_dirs = dashboard_strategy_dirs(output_dir)
    prop_dashboard_enabled = st.toggle(
        "Show Prop Dashboard",
        value=False,
        key="dashboard_prop_mode",
    )
    strategy_mode_enabled = False

    if not root_signature and not strategy_dirs:
        st.warning("No Output Files found")
        return

    root_df = _normalize_dashboard_scrips(load_dashboard_trade_rows(str(output_dir), root_signature, strategy_name="Current"))
    if root_df.empty and not strategy_dirs:
        st.info("No trade data available")
        return

    reference_frames: list[pd.DataFrame] = []
    if not root_df.empty:
        reference_frames.append(root_df)
    for strategy_dir in strategy_dirs:
        strategy_df = _normalize_dashboard_scrips(load_dashboard_trade_rows(
            str(strategy_dir),
            dashboard_folder_signature(strategy_dir),
            strategy_name=strategy_dir.name,
        ))
        if not strategy_df.empty:
            reference_frames.append(strategy_df)
    if not reference_frames:
        st.info("No trade data available")
        return

    reference_df = pd.concat(reference_frames, ignore_index=True)
    valid_entry_timestamps = reference_df["Entry Timestamp"].dropna()
    if valid_entry_timestamps.empty:
        st.info("No valid trade data available")
        return

    min_entry_date = valid_entry_timestamps.min().date()
    max_entry_date = valid_entry_timestamps.max().date()

    with st.container():
        st.markdown("### Filters")
        filter_col_a, filter_col_b, filter_col_c = st.columns([1.0, 1.0, 1.3])
        with filter_col_a:
            filter_from_date = st.date_input(
                "Entry Date From",
                value=min_entry_date,
                min_value=min_entry_date,
                max_value=max_entry_date,
                format="DD/MM/YYYY",
                key="dashboard_filter_from_date",
            )
        with filter_col_b:
            filter_to_date = st.date_input(
                "Entry Date To",
                value=max_entry_date,
                min_value=min_entry_date,
                max_value=max_entry_date,
                format="DD/MM/YYYY",
                key="dashboard_filter_to_date",
            )
        available_scrips = sorted(reference_df["Scrip"].dropna().astype(str).unique().tolist(), key=str.lower)
        scrip_filter_key = "dashboard_filter_scrips"
        select_all_label = "SELECT ALL"
        if scrip_filter_key not in st.session_state:
            st.session_state[scrip_filter_key] = [select_all_label]
        else:
            prior_selection = [str(value or "").upper() for value in st.session_state.get(scrip_filter_key, [])]
            chosen_scrips = [value for value in prior_selection if value != select_all_label and value in available_scrips]
            st.session_state[scrip_filter_key] = [select_all_label] if select_all_label in prior_selection or len(chosen_scrips) == len(available_scrips) else chosen_scrips
        with filter_col_c:
            raw_selected_scrips = st.multiselect(
                "Scrip",
                options=[select_all_label] + available_scrips,
                key=scrip_filter_key,
                help="This filter applies to KPI, charts, summary, drill-down, and PDF report.",
            )
            selected_dashboard_scrips = available_scrips if select_all_label in raw_selected_scrips else raw_selected_scrips
        include_open_trades = False
        if filter_from_date > filter_to_date:
            st.warning("From date cannot be after To date.")
            return
    if not selected_dashboard_scrips:
        st.warning("Please select at least one scrip.")
        return
    selected_scrips_text = select_all_label if len(selected_dashboard_scrips) == len(available_scrips) else ", ".join(display_symbol(scrip) for scrip in selected_dashboard_scrips)
    input_header_col, input_popover_col = st.columns([0.75, 0.25])
    with input_header_col:
        if prop_dashboard_enabled:
            st.caption("Prop mode deducts estimated charges on every trade and monthly interest from the net profit calculations.")
    with input_popover_col:
        with st.popover("Dashboard Inputs"):
            estimated_charges_per_trade = st.number_input(
                "Estimated Charges / Trade",
                min_value=0.0,
                value=float(st.session_state.get("dashboard_estimated_charges_per_trade", 0.0)),
                step=10.0,
                key="dashboard_estimated_charges_per_trade",
            )
            avg_value_traded_per_lot = 0.0
            leverage = 1.0
            interest_rate_pct = 12.0
            capital_preview = 0.0
            if prop_dashboard_enabled:
                avg_value_traded_per_lot = st.number_input(
                    "Average Value Traded Per Lot",
                    min_value=0.0,
                    value=float(st.session_state.get("dashboard_prop_avg_value_traded", 1000000.0)),
                    step=50000.0,
                    key="dashboard_prop_avg_value_traded",
                )
                leverage = st.number_input(
                    "Leverage",
                    min_value=0.0,
                    value=float(st.session_state.get("dashboard_prop_leverage", 1.0)),
                    step=0.5,
                    key="dashboard_prop_leverage",
                )
                interest_rate_pct = st.number_input(
                    "Interest Rate (%)",
                    min_value=0.0,
                    value=float(st.session_state.get("dashboard_prop_interest_rate", 12.0)),
                    step=0.5,
                    key="dashboard_prop_interest_rate",
                )
                capital_preview = avg_value_traded_per_lot * 0.25 * 0.20 * len(selected_dashboard_scrips)
                st.metric("Capital", format_inr(capital_preview))
                st.caption("Capital = Value Traded x 25% x 20% x No. of Scrips")
                st.caption("Monthly Interest = Capital x Leverage x Interest Rate x No. of Scrips")
            else:
                st.caption("Current mode deducts only the estimated charges per trade.")
    filters_text = (
        f"Entry Date: {filter_from_date:%d-%b-%Y} to {filter_to_date:%d-%b-%Y} | "
        f"Scrips: {selected_scrips_text} | Include Open Trades: No"
    )

    if strategy_mode_enabled and strategy_dirs:
        comparison_df, strategy_equity_df = build_strategy_comparison_dashboard(
            output_dir=output_dir,
            start_date=filter_from_date,
            end_date=filter_to_date,
            include_open_trades=include_open_trades,
            selected_scrips=selected_dashboard_scrips,
        )
        if comparison_df.empty:
            st.warning("No data available")
            return

        with st.container():
            comparison_metrics = {
                "total_scrips": int(len(comparison_df)),
                "total_trades": int(pd.to_numeric(comparison_df["Trades"], errors="coerce").fillna(0).sum()),
                "total_pl_amt": float(pd.to_numeric(comparison_df["Total PL Amt"], errors="coerce").fillna(0).sum()),
                "win_rate": float(pd.to_numeric(comparison_df["Win Rate %"], errors="coerce").fillna(0).mean()) if not comparison_df.empty else 0.0,
                "sharpe_ratio": float(pd.to_numeric(comparison_df["Sharpe Ratio"], errors="coerce").fillna(0).mean()) if not comparison_df.empty else 0.0,
                "max_drawdown": float(pd.to_numeric(comparison_df["Max Drawdown"], errors="coerce").fillna(0).min()) if not comparison_df.empty else 0.0,
                "avg_profit_per_trade": float(pd.to_numeric(comparison_df["Avg Profit Per Trade"], errors="coerce").fillna(0).mean()) if not comparison_df.empty else 0.0,
                "avg_loss_per_trade": float(pd.to_numeric(comparison_df["Avg Loss Per Trade"], errors="coerce").fillna(0).mean()) if not comparison_df.empty else 0.0,
                "avg_net_profit_per_trade": float(pd.to_numeric(comparison_df["Avg Net Profit Per Trade"], errors="coerce").fillna(0).mean()) if not comparison_df.empty else 0.0,
                "risk_reward_ratio": float(pd.to_numeric(comparison_df["Risk Reward Ratio"], errors="coerce").fillna(0).mean()) if not comparison_df.empty else 0.0,
                "profit_trades": int(pd.to_numeric(comparison_df["Total Profit Trades"], errors="coerce").fillna(0).sum()) if not comparison_df.empty else 0,
                "loss_trades": int(pd.to_numeric(comparison_df["Total Loss Trades"], errors="coerce").fillna(0).sum()) if not comparison_df.empty else 0,
            }
            best_dd_date = comparison_df.loc[comparison_df["Rank"].eq(1), "DD Date"].iloc[0] if not comparison_df.empty and comparison_df["Rank"].eq(1).any() else "-"
            comparison_download_df = comparison_df.rename(columns={"Total PL Amt": "Total Profit / Loss"})
            strategy_chart_specs, strategy_extra_chart_specs = build_strategy_dashboard_chart_specs(comparison_df, strategy_equity_df)
            render_dashboard_section_header(
                "KPI Overview",
            )
            metric_row_1 = st.columns(3)
            render_dashboard_box(metric_row_1[0], "Strategies", comparison_metrics["total_scrips"])
            render_dashboard_box(metric_row_1[1], "Total PL", comparison_metrics["total_pl_amt"], force_green=comparison_metrics["total_pl_amt"] > 0)
            render_dashboard_box(metric_row_1[2], "Win Rate %", comparison_metrics["win_rate"], percent=True, force_green=comparison_metrics["win_rate"] > 0)
            metric_row_2 = st.columns(3)
            render_dashboard_box(metric_row_2[0], "Risk Reward Ratio", comparison_metrics["risk_reward_ratio"], force_green=comparison_metrics["risk_reward_ratio"] > 0)
            render_dashboard_box(metric_row_2[1], "Max Drawdown", comparison_metrics["max_drawdown"], force_red=True)
            render_dashboard_box(metric_row_2[2], "DD Date", best_dd_date, force_red=True)

        with st.container():
            st.markdown("### Advanced Metrics")
            advanced_row_1 = st.columns(3)
            render_dashboard_box(advanced_row_1[0], "Avg Profit Per Trade", comparison_metrics["avg_profit_per_trade"], force_green=comparison_metrics["avg_profit_per_trade"] > 0)
            render_dashboard_box(advanced_row_1[1], "Avg Loss Per Trade", comparison_metrics["avg_loss_per_trade"], force_red=True)
            render_dashboard_box(advanced_row_1[2], "Avg Net Profit Per Trade", comparison_metrics["avg_net_profit_per_trade"], force_green=comparison_metrics["avg_net_profit_per_trade"] > 0)
            advanced_row_2 = st.columns(3)
            render_dashboard_box(advanced_row_2[0], "Total Profit Trades", comparison_metrics["profit_trades"])
            render_dashboard_box(advanced_row_2[1], "Total Loss Trades", comparison_metrics["loss_trades"], force_red=True)
            render_dashboard_box(advanced_row_2[2], "Total Trades", comparison_metrics["total_trades"])
            advanced_row_3 = st.columns(3)
            render_dashboard_box(advanced_row_3[0], "Sharpe Ratio", comparison_metrics["sharpe_ratio"], force_green=comparison_metrics["sharpe_ratio"] > 0)
            advanced_row_3[1].empty()
            advanced_row_3[2].empty()

        with st.container():
            st.markdown("### Charts")
            all_strategy_chart_specs = strategy_chart_specs + strategy_extra_chart_specs
            for index in range(0, len(all_strategy_chart_specs), 2):
                row_cols = st.columns(2)
                for cell, (_, fig) in zip(row_cols, all_strategy_chart_specs[index:index + 2]):
                    if fig is None:
                        cell.info("No data available")
                    else:
                        cell.plotly_chart(fig, width="stretch")

        with st.container():
            st.markdown("### Strategy Comparison")
            table_columns = [
                "Rank",
                "Strategy",
                "Trades",
                "Total Profit Trades",
                "Total Loss Trades",
                "Win Rate %",
                "Total PL Amt",
                "Avg Profit Per Trade",
                "Avg Loss Per Trade",
                "Avg Net Profit Per Trade",
                "Sharpe Ratio",
                "Max Drawdown",
                "Drawdown Duration",
                "Risk Reward Ratio",
                "DD Date",
                "Score",
            ]
            comparison_display_df = comparison_df.loc[:, table_columns].rename(
                columns={"Total PL Amt": "Total Profit / Loss"}
            )
            st.dataframe(
                style_dashboard_table(comparison_display_df),
                width="stretch",
                hide_index=True,
                height=_table_height_for_rows(len(comparison_display_df)),
            )
        return

    filtered_df = filter_dashboard_trade_rows(
        root_df,
        filter_from_date,
        filter_to_date,
        include_open_trades,
        selected_scrips=selected_dashboard_scrips,
    )
    if filtered_df.empty:
        st.warning("No data available")
        return

    filtered_df, cost_metrics = apply_dashboard_cost_model(
        filtered_df,
        estimated_charges_per_trade=estimated_charges_per_trade,
        prop_dashboard_enabled=prop_dashboard_enabled,
        avg_value_traded_per_lot=avg_value_traded_per_lot,
        leverage=leverage,
        interest_rate_pct=interest_rate_pct,
        selected_scrip_count=len(selected_dashboard_scrips),
    )

    metrics = build_dashboard_metrics(filtered_df)
    metrics["roi_pct"] = float(cost_metrics["roi_pct"])
    metrics["monthly_interest_total"] = float(cost_metrics["monthly_interest_total"])
    metrics["capital"] = float(cost_metrics["capital"])
    summary_df = build_dashboard_summary_table(filtered_df)

    with st.container():
        summary_display_df = summary_df.rename(columns={"Total PL Amt": "Total Profit / Loss"})
        detail_display_df = filtered_df.rename(columns={"PL Amt": "Profit / Loss"})
        top_chart_specs, lower_chart_specs = build_single_dashboard_chart_specs(summary_df, filtered_df, metrics)
        render_dashboard_section_header(
            "KPI Overview",
        )
        metric_row_1 = st.columns(3)
        render_dashboard_box(metric_row_1[0], "Total Scrips", metrics["total_scrips"])
        render_dashboard_box(metric_row_1[1], "Total PL", metrics["total_pl_amt"], force_green=metrics["total_pl_amt"] > 0)
        render_dashboard_box(metric_row_1[2], "Win Rate %", metrics["win_rate"], percent=True, force_green=metrics["win_rate"] > 0)
        metric_row_2 = st.columns(3)
        render_dashboard_box(metric_row_2[0], "Risk Reward Ratio", metrics["risk_reward_ratio"], force_green=metrics["risk_reward_ratio"] > 0)
        render_dashboard_box(metric_row_2[1], "Max Drawdown", metrics["max_drawdown"], force_red=True)
        render_dashboard_box(metric_row_2[2], "DD Date", metrics["dd_date"], force_red=True)

    with st.container():
        st.markdown("### Advanced Metrics")
        advanced_row_1 = st.columns(3)
        render_dashboard_box(advanced_row_1[0], "Avg Profit Per Trade", metrics["avg_profit_per_trade"], force_green=metrics["avg_profit_per_trade"] > 0)
        render_dashboard_box(advanced_row_1[1], "Avg Loss Per Trade", metrics["avg_loss_per_trade"], force_red=True)
        render_dashboard_box(advanced_row_1[2], "Avg Net Profit Per Trade", metrics["avg_net_profit_per_trade"], force_green=metrics["avg_net_profit_per_trade"] > 0)
        advanced_row_2 = st.columns(3)
        render_dashboard_box(advanced_row_2[0], "Total Profit Trades", metrics["wins"])
        render_dashboard_box(advanced_row_2[1], "Total Loss Trades", metrics["losses"], force_red=True)
        render_dashboard_box(advanced_row_2[2], "Total Trades", metrics["total_trades"])
        advanced_row_3 = st.columns(3)
        render_dashboard_box(advanced_row_3[0], "Sharpe Ratio", metrics["sharpe_ratio"], force_green=metrics["sharpe_ratio"] > 0)
        if prop_dashboard_enabled:
            render_dashboard_box(advanced_row_3[1], "ROI %", metrics["roi_pct"], percent=True, force_green=metrics["roi_pct"] > 0)
            render_dashboard_box(advanced_row_3[2], "Interest / Month", metrics["monthly_interest_total"], force_red=metrics["monthly_interest_total"] > 0)
        else:
            advanced_row_3[1].empty()
            advanced_row_3[2].empty()

    with st.container():
        st.markdown("### Charts")
        top_left, top_right = st.columns([1.2, 0.8])
        for cell, (chart_title, fig) in zip((top_left, top_right), top_chart_specs):
            with cell:
                st.markdown(f"#### {chart_title}")
                if fig is None:
                    st.info("No data available")
                else:
                    st.plotly_chart(fig, width="stretch")
        for chart_title, fig in lower_chart_specs:
            st.markdown(f"#### {chart_title}")
            if fig is None:
                st.info("No data available")
            else:
                st.plotly_chart(fig, width="stretch")

    with st.container():
        st.markdown("### Time Analysis")
        time_granularity = st.selectbox(
            "Group By Time",
            options=["Year", "Quarter", "Month", "Week", "Day"],
            index=2,
            key="dashboard_time_analysis_granularity",
        )
        time_analysis_df = build_time_analysis_table(
            filtered_df,
            time_granularity,
            prop_dashboard_enabled=prop_dashboard_enabled,
            monthly_interest_total=float(cost_metrics["monthly_interest_total"]),
        )
        st.dataframe(
            style_dashboard_table(time_analysis_df),
            width="stretch",
            hide_index=True,
            height=_table_height_for_rows(len(time_analysis_df)),
        )

    with st.container():
        st.markdown("### Scrip Analysis")
        scrip_analysis_df = build_scrip_analysis_table(filtered_df)
        st.dataframe(
            style_dashboard_table(scrip_analysis_df),
            width="stretch",
            hide_index=True,
            height=_table_height_for_rows(len(scrip_analysis_df)),
        )

    with st.container():
        st.markdown("### Pivot View")
        pivot_col_a, pivot_col_b = st.columns([1.0, 1.0])
        with pivot_col_a:
            pivot_granularity = st.selectbox(
                "Time Group",
                options=["Year", "Quarter", "Month", "Week", "Day"],
                index=2,
                key="dashboard_pivot_granularity",
            )
        with pivot_col_b:
            pivot_value_metric = st.selectbox(
                "Value",
                options=["Total Profit / Loss", "Trades", "Win Rate %", "Avg Net Profit Per Trade"],
                index=0,
                key="dashboard_pivot_value_metric",
            )
        pivot_df = build_pivot_analysis_table(filtered_df, pivot_granularity, pivot_value_metric)
        if pivot_df.empty:
            st.info("No data available")
        else:
            st.dataframe(
                style_pivot_table(pivot_df, pivot_value_metric),
                width="stretch",
                hide_index=True,
                height=_table_height_for_rows(len(pivot_df), min_height=220),
            )

    with st.container():
        st.markdown("### Scrip Trade Details")
        drilldown_df = filtered_df.copy()
        if drilldown_df.empty:
            st.warning("No data available")
            return
        detail_columns = [
            "Scrip", "Sr.No", "Entry Date", "Entry Time", "Trade",
            "Entry Price", "Exit Date", "Exit Time", "Exit Price",
            "PL Points", "Qty", "Gross PL Amount", "Estimated Charges",
            "Net PL Amount", "Candle Analysis",
        ]
        detail_columns = [column for column in detail_columns if column in drilldown_df.columns]
        detail_display_df = drilldown_df.loc[:, detail_columns].rename(
            columns={"Estimated Charges": "Charges"}
        )
        st.dataframe(
            style_dashboard_table(detail_display_df),
            width="stretch",
            hide_index=True,
            height=_table_height_for_rows(len(detail_display_df)),
        )




def _ensure_unique_columns(table_df: pd.DataFrame) -> pd.DataFrame:
    safe_df = table_df.copy()
    safe_df.reset_index(drop=True, inplace=True)
    columns = [str(col) for col in safe_df.columns]
    seen: dict[str, int] = {}
    new_columns: list[str] = []
    for col in columns:
        count = seen.get(col, 0)
        if count == 0:
            new_columns.append(col)
        else:
            new_columns.append(f"{col}.{count}")
        seen[col] = count + 1
    safe_df.columns = new_columns
    return safe_df

def style_saved_signals_table(
    table_df: pd.DataFrame,
    selected_rows: list[int] | None = None,
) -> pd.io.formats.style.Styler:
    safe_df = _ensure_unique_columns(table_df.reset_index(drop=True))
    selected_row_set = {int(row) for row in (selected_rows or [])}

    def fmt_money(value: Any) -> str:
        return "" if pd.isna(value) else f"{float(value):.2f}"

    def fmt_qty(value: Any) -> str:
        return "" if pd.isna(value) else f"{int(value)}"

    def style_rows(row: pd.Series):
        styles = [""] * len(row)
        trade_value = row.get("Trade")
        if trade_value == "B":
            styles = ["background-color: rgba(8,153,129,0.12);"] * len(row)
        elif trade_value == "S":
            styles = ["background-color: rgba(242,54,69,0.12);"] * len(row)

        price_idx = list(row.index).index("Price") if "Price" in row.index else None
        trade_idx = list(row.index).index("Trade") if "Trade" in row.index else None
        pl_points_idx = list(row.index).index("PL Points") if "PL Points" in row.index else None
        pl_amt_idx = list(row.index).index("PL Amt") if "PL Amt" in row.index else None
        color = BUY_DARK_COLOR if trade_value == "B" else SELL_DARK_COLOR
        if trade_idx is not None:
            styles[trade_idx] = styles[trade_idx] + f" font-weight:700; color: {color};"
        if price_idx is not None:
            styles[price_idx] = styles[price_idx] + f" font-weight:700; color: {color};"
        pl_points = row.get("PL Points")
        if pl_points_idx is not None and pd.notna(pl_points):
            pl_color = BUY_DARK_COLOR if float(pl_points) >= 0 else SELL_DARK_COLOR
            styles[pl_points_idx] = styles[pl_points_idx] + f" font-weight:700; color: {pl_color};"
        pl_amt = row.get("PL Amt")
        if pl_amt_idx is not None and pd.notna(pl_amt):
            pl_amt_color = BUY_DARK_COLOR if float(pl_amt) >= 0 else SELL_DARK_COLOR
            styles[pl_amt_idx] = styles[pl_amt_idx] + f" font-weight:700; color: {pl_amt_color};"

        if int(row.name) in selected_row_set:
            highlight = " background-color: rgba(59,130,246,0.18); box-shadow: inset 0 0 0 2px rgba(37,99,235,0.8);"
            styles = [style + highlight for style in styles]
            if trade_idx is not None:
                styles[trade_idx] = styles[trade_idx] + " font-size: 1.02em;"
            if price_idx is not None:
                styles[price_idx] = styles[price_idx] + " font-size: 1.02em;"

        return styles

    return (
        safe_df.style
        .apply(style_rows, axis=1)
        .format(
            {
                "Price": fmt_money,
                "Entry Price": fmt_money,
                "Exit Price": fmt_money,
                "PL Points": fmt_money,
                "PL Amt": fmt_money,
                "Qty": fmt_qty,
            }
        )
        .set_properties(**{"text-align": "center"})
    )

def main() -> None:
    st.set_page_config(page_title="EMA Trade Viewer", layout="wide")
    is_windows = sys.platform.startswith("win")
    cloud_workspace_root = Path(tempfile.gettempdir()) / "ema_trade_viewer_uploads"

    st.session_state.setdefault("saved_signals", [])
    st.session_state.setdefault("latest_signal", None)
    st.session_state.setdefault("chart_reset_nonce", 0)
    st.session_state.setdefault("clicked_index", None)
    st.session_state.setdefault("clicked_date", None)
    st.session_state.setdefault("clicked_time", None)
    st.session_state.setdefault("clicked_epoch", None)
    st.session_state.setdefault(
        "chart_replay_state",
        {"active": False, "index": None, "showStartLine": False},
    )
    st.session_state.setdefault("show_filters", True)
    st.session_state.setdefault("confirm_clear_all", False)
    st.session_state.setdefault("saved_signals_selected_row", None)
    st.session_state.setdefault("saved_signals_selected_rows", [])
    st.session_state.setdefault("saved_signals_symbol", None)
    st.session_state.setdefault("saved_signals_output_csv", None)
    st.session_state.setdefault("show_saved_signals_panel", True)
    st.session_state.setdefault("build_signature", None)
    st.session_state.setdefault("qty", 1)
    st.session_state.setdefault("last_chart_click_token", None)
    st.session_state.setdefault("last_chart_click_at", 0.0)
    st.session_state.setdefault("chart_window_start", None)
    st.session_state.setdefault("filter_source_from", None)
    st.session_state.setdefault("filter_source_to", None)
    st.session_state.setdefault("chart_zoomed", False)
    st.session_state.setdefault("main_dir_path_input", "")
    st.session_state.setdefault("data_dir_path_input", "")
    st.session_state.setdefault("output_dir_path_input", "")
    st.session_state.setdefault("process_feedback_level", None)
    st.session_state.setdefault("process_feedback_message", "")
    st.session_state.setdefault("drive_manual_input_downloads", [])
    st.session_state.setdefault("filter_data_dir", None)
    st.session_state.setdefault("filter_output_dir", None)
    st.session_state.setdefault("selected_symbol", None)
    st.session_state.setdefault("selected_symbol_restore", None)
    st.session_state.setdefault("cloud_workspace_session_id", str(uuid4()))
    st.session_state.setdefault("show_drive_process_dialog", False)
    st.session_state.setdefault("drive_process_choice_widget", "No")
    st.session_state.setdefault("drive_selected_symbols", [])
    st.session_state.setdefault("drive_dialog_feedback_level", None)
    st.session_state.setdefault("drive_dialog_feedback_message", "")
    st.session_state.setdefault("drive_input_sync_choice", None)
    st.session_state.setdefault("drive_input_sync_file_count", 0)
    st.session_state.setdefault("drive_output_sync_completed", False)
    st.session_state.setdefault("output_reload_feedback_level", None)
    st.session_state.setdefault("output_reload_feedback_message", "")
    st.session_state.setdefault("output_update_feedback_level", None)
    st.session_state.setdefault("output_update_feedback_message", "")
    st.session_state.setdefault("output_update_manual_download", None)
    cloud_workspace_dir = cloud_workspace_root / st.session_state.cloud_workspace_session_id
    drive_status = get_google_drive_connection_status()
    drive_raw_files: list[Any] = []
    drive_raw_symbol_files: dict[str, list[Any]] = {}
    drive_raw_symbol_names: list[str] = []
    drive_raw_files_error = ""
    if drive_status.connected and drive_status.raw_folder is not None:
        try:
            drive_raw_files = filter_supported_google_drive_files(
                list_google_drive_folder_files(drive_status.raw_folder.folder_id)
            )
            drive_raw_symbol_files = group_google_drive_files_by_symbol(drive_raw_files)
            drive_raw_symbol_names = list(drive_raw_symbol_files.keys())
        except Exception as exc:
            drive_raw_files_error = str(exc)
    selected_drive_symbols = [
        symbol
        for symbol in st.session_state.get("drive_selected_symbols", [])
        if symbol in drive_raw_symbol_names
    ]
    if selected_drive_symbols != st.session_state.get("drive_selected_symbols", []):
        st.session_state.drive_selected_symbols = selected_drive_symbols
    if (
        not st.session_state.main_dir_path_input
        and not is_windows
    ):
        ensure_workspace_dirs(cloud_workspace_dir)
        st.session_state.main_dir_path_input = str(cloud_workspace_dir)
        st.session_state.data_dir_path_input = str(cloud_workspace_dir / "Input Files")
        st.session_state.output_dir_path_input = str(cloud_workspace_dir / "Output Files")

    st.markdown(
        f"""
        <style>
        .block-container {{
            padding-top: 0.35rem;
            padding-bottom: 0.6rem;
            padding-left: 1rem;
            padding-right: 0.6rem;
            max-width: 100%;
        }}
        [data-testid="stSidebar"] {{
            background: {SIDEBAR_BG};
        }}
        .tv-title {{
            font-size: 1.4rem;
            font-weight: 700;
            color: #0f172a;
            line-height: 1.15;
            margin-bottom: 0.15rem;
        }}
        .tv-subtitle {{
            font-size: 0.92rem;
            color: #64748b;
        }}
        .header-title-line {{
            display: flex;
            align-items: baseline;
            gap: 0.8rem;
            flex-wrap: wrap;
            margin-top: 2.4rem;
        }}
        .header-range-inline {{
            font-size: 0.88rem;
            font-weight: 500;
            color: #64748b;
        }}
        .header-signal-stack {{
            display: flex;
            flex-direction: column;
            align-items: flex-end;
            justify-content: center;
            gap: 0.25rem;
            width: 100%;
            min-width: 0;
        }}
        .header-signal-chip-row {{
            display: flex;
            justify-content: flex-start;
            align-items: center;
            gap: 0.75rem;
            width: 100%;
            margin-top: 2.4rem;
            margin-bottom: 0.1rem;
            padding-left: 0%;
            margin-left: -14%;
        }}
        .header-signal-chip-time {{
            font-size: 0.9rem;
            font-weight: 600;
            color: #64748b;
            white-space: nowrap;
        }}
        .header-ohlc-line,
        .header-ema-line {{
            font-size: 0.95rem;
            font-weight: 600;
            color: #0f172a;
            text-align: right;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }}
        .header-signal-time {{
            font-size: 0.9rem;
            font-weight: 600;
            color: #64748b;
            text-align: right;
        }}
        .header-spacer {{
            height: 2.4rem;
        }}
        .signal-chip {{
            display: inline-block;
            padding: 0.55rem 0.85rem;
            border-radius: 999px;
            color: white;
            font-weight: 700;
            font-size: 0.95rem;
            white-space: nowrap;
        }}
        .signal-chip-placeholder {{
            min-height: 2.35rem;
            width: 100%;
        }}
        .confirm-clear {{
            background: #fff8db;
            color: #9a6700;
            border: 1px solid #f0d98a;
            border-radius: 0.65rem;
            padding: 0.55rem 0.75rem;
            font-size: 0.82rem;
            line-height: 1.25;
            text-align: center;
            margin-top: 0.5rem;
            margin-bottom: 0.45rem;
        }}
        [data-testid="stDataFrame"] div[role="columnheader"] > div,
        [data-testid="stDataFrame"] div[role="gridcell"] > div {{
            display: flex !important;
            justify-content: center !important;
            align-items: center !important;
            width: 100% !important;
        }}
        [data-testid="stDataFrame"] div[role="gridcell"] span,
        [data-testid="stDataFrame"] div[role="columnheader"] span {{
            text-align: center !important;
            width: 100% !important;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )

    if st.session_state.show_filters:
        with st.sidebar:
            st.header("Filters")
            st.caption(f"Timeframe: {TIMEFRAME_TEXT}")
            st.caption("Session: 09:15 - 15:27")
            st.markdown("**Google Drive**")
            if drive_status.connected:
                if drive_raw_files_error:
                    st.warning(f"Could not read Drive raw files: {drive_raw_files_error}")
                elif drive_raw_files:
                    st.radio(
                        "Process Raw Files from Google Drive?",
                        ["No", "Yes"],
                        horizontal=True,
                        key="drive_process_choice_widget",
                        on_change=trigger_drive_process_dialog,
                        help="Choose Yes when you want to process selected raw scrips from Google Drive.",
                    )
                else:
                    st.caption("No supported raw files found in Google Drive yet.")
            elif drive_status.configured:
                st.warning(drive_status.message)
            else:
                st.info(drive_status.message)
            if is_windows:
                if st.button("Main Folder", width="stretch"):
                    selected_folder = browse_for_folder(st.session_state.main_dir_path_input)
                    if selected_folder:
                        selected_main_dir = resolve_main_workspace_dir(selected_folder)
                        st.session_state.main_dir_path_input = str(selected_main_dir)
                        st.session_state.data_dir_path_input = str(selected_main_dir / "Input Files")
                        st.session_state.output_dir_path_input = str(selected_main_dir / "Output Files")
                        st.session_state.selected_symbol = None
                        st.rerun()
            else:
                raw_dir, input_dir, output_dir = ensure_workspace_dirs(cloud_workspace_dir)
                st.session_state.main_dir_path_input = str(cloud_workspace_dir)
                st.session_state.data_dir_path_input = str(input_dir)
                st.session_state.output_dir_path_input = str(output_dir)
                if drive_status.connected and not st.session_state.get("drive_output_sync_completed"):
                    output_level, output_message, output_count = sync_google_drive_output_files_to_dir(drive_status, output_dir)
                    if output_level == "error":
                        st.session_state.process_feedback_level = "error"
                        st.session_state.process_feedback_message = output_message
                    else:
                        st.session_state.drive_output_sync_completed = True
                        if output_count:
                            st.session_state.saved_signals = []
                            st.session_state.saved_signals_symbol = None
                            st.session_state.saved_signals_output_csv = None
                            st.session_state.latest_signal = None
                        list_google_drive_folder_files.clear()
                        list_symbols.clear()
                        load_data.clear()
                        st.rerun()
                if (
                    drive_status.connected
                    and drive_raw_files
                    and st.session_state.get("drive_process_choice_widget") == "No"
                    and st.session_state.get("drive_input_sync_choice") != "No"
                ):
                    level, message, file_count = sync_google_drive_input_files_to_dir(drive_status, input_dir)
                    st.session_state.process_feedback_level = level
                    st.session_state.process_feedback_message = message
                    st.session_state.drive_input_sync_choice = "No"
                    st.session_state.drive_input_sync_file_count = file_count
                    if level != "error":
                        list_google_drive_folder_files.clear()
                        list_symbols.clear()
                        load_data.clear()
                        st.rerun()

            if is_windows:
                if st.button("Process Input Files", width="stretch"):
                    selected_main_raw = str(st.session_state.get("main_dir_path_input") or "").strip()
                    if not selected_main_raw:
                        st.session_state.process_feedback_level = "error"
                        st.session_state.process_feedback_message = "Please select the Main Folder first."
                        st.rerun()

                    selected_main_dir = resolve_main_workspace_dir(selected_main_raw)
                    raw_dir = selected_main_dir / "Raw Files"
                    input_dir = selected_main_dir / "Input Files"
                    output_dir = selected_main_dir / "Output Files"

                    if not selected_main_dir.exists() or not selected_main_dir.is_dir():
                        st.session_state.process_feedback_level = "error"
                        st.session_state.process_feedback_message = f"Main folder not found: {selected_main_dir}"
                        st.rerun()

                    if not raw_dir.exists():
                        st.session_state.process_feedback_level = "error"
                        st.session_state.process_feedback_message = f"Raw Files folder not found in {selected_main_dir}"
                        st.rerun()

                    if not raw_dir.is_dir():
                        st.session_state.process_feedback_level = "error"
                        st.session_state.process_feedback_message = f"Raw Files is not a folder in {selected_main_dir}"
                        st.rerun()

                    input_dir.mkdir(parents=True, exist_ok=True)
                    output_dir.mkdir(parents=True, exist_ok=True)

                    summary = process_raw_folder(raw_dir, input_dir)
                    level, message = build_processing_feedback(summary)
                    st.session_state.process_feedback_level = level
                    st.session_state.process_feedback_message = message
                    st.session_state.data_dir_path_input = str(input_dir)
                    st.session_state.output_dir_path_input = str(output_dir)
                    st.session_state.selected_symbol = None
                    list_symbols.clear()
                    load_data.clear()
                    st.rerun()

            feedback_level = st.session_state.get("process_feedback_level")
            feedback_message = str(st.session_state.get("process_feedback_message") or "").strip()
            if feedback_level and feedback_message:
                feedback_fn = {
                    "success": st.success,
                    "warning": st.warning,
                    "error": st.error,
                }.get(feedback_level, st.info)
                feedback_fn(feedback_message)
                st.session_state.process_feedback_level = None
                st.session_state.process_feedback_message = ""

    if st.session_state.get("show_drive_process_dialog"):
        workspace_dir_raw = str(st.session_state.get("main_dir_path_input") or "").strip()
        workspace_dir = (
            resolve_main_workspace_dir(workspace_dir_raw)
            if workspace_dir_raw
            else cloud_workspace_dir
        )
        render_drive_process_dialog(
            symbol_names=drive_raw_symbol_names,
            symbol_files=drive_raw_symbol_files,
            main_dir=workspace_dir,
            drive_input_folder_id=drive_status.input_folder.folder_id if drive_status.input_folder else "",
        )

    main_dir_raw = str(st.session_state.get("main_dir_path_input") or "").strip()
    if not main_dir_raw:
        st.error("Please select the Main Folder.")
        return

    main_dir = resolve_main_workspace_dir(main_dir_raw)
    if not main_dir.exists():
        st.error(f"Main folder not found: {main_dir}")
        return
    if not main_dir.is_dir():
        st.error(f"Selected main path is not a folder: {main_dir}")
        return

    raw_dir = main_dir / "Raw Files"
    data_dir = main_dir / "Input Files"
    output_dir = main_dir / "Output Files"
    st.session_state.data_dir_path_input = str(data_dir)
    st.session_state.output_dir_path_input = str(output_dir)

    if not raw_dir.exists():
        st.error(f"Raw Files folder not found in {main_dir}")
        return
    if not raw_dir.is_dir():
        st.error(f"Raw Files is not a folder in {main_dir}")
        return

    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not data_dir.is_dir():
        st.error(f"Input Files is not a folder in {main_dir}")
        return
    if not output_dir.is_dir():
        st.error(f"Output Files is not a folder in {main_dir}")
        return
    if not can_write_to_directory(output_dir):
        st.error(f"Output folder is not writable: {output_dir}")
        return

    symbols = list_symbols(str(data_dir))
    if not symbols:
        raw_symbols = list_supported_data_files(raw_dir)
        if raw_symbols:
            if is_windows:
                st.error("No processed supported data files found in Input Files. Click Process Input Files.")
            else:
                st.info("No processed files are available in Google Drive Input Files yet. Choose 'Yes' to process Drive raw files.")
        else:
            if drive_status.connected and drive_raw_files:
                st.info("Google Drive raw files were found. Choose 'Yes' in 'Process Raw Files from Google Drive?' to open the processing popup.")
            elif is_windows:
                st.error(f"No raw supported data files found in {raw_dir}")
            else:
                st.info("No supported Google Drive raw files were found yet.")
        return

    symbol_names = list(symbols.keys())
    requested_symbol_restore = st.session_state.get("selected_symbol_restore")
    if requested_symbol_restore in symbol_names:
        st.session_state.selected_symbol = requested_symbol_restore
    st.session_state.selected_symbol_restore = None
    if st.session_state.selected_symbol not in symbol_names:
        st.session_state.selected_symbol = symbol_names[0]

    if st.session_state.show_filters:
        with st.sidebar:
            symbol = st.selectbox(
                "Select Scrip",
                symbol_names,
                key="selected_symbol",
                format_func=display_symbol,
            )
            st.number_input(
                "Qty",
                min_value=1,
                step=1,
                format="%d",
                key="qty",
            )
    else:
        symbol = st.selectbox(
            "Select Scrip",
            symbol_names,
            key="selected_symbol",
            format_func=display_symbol,
        )

    df = load_data(symbols[symbol])

    output_csv_path = output_signal_csv_path(output_dir, symbol)
    if (
        st.session_state.get("saved_signals_symbol") != symbol
        or st.session_state.get("saved_signals_output_csv") != str(output_csv_path)
    ):
        st.session_state.output_reload_feedback_level = None
        st.session_state.output_reload_feedback_message = ""
        st.session_state.output_update_feedback_level = None
        st.session_state.output_update_feedback_message = ""
        st.session_state.output_update_manual_download = None
        try:
            ensure_output_signal_file(output_dir, symbol)
            loaded_saved_signals = load_saved_signals_file(output_csv_path, symbol, input_df=df)
            persisted_saved_signals = persist_saved_signals_file(output_csv_path, symbol, loaded_saved_signals)
        except Exception as exc:
            st.error(f"Saved-signal file error for {symbol}: {exc}")
            return
        apply_saved_signals_state(persisted_saved_signals, symbol, output_csv_path)
        st.session_state.confirm_clear_all = False

    trade_download_bytes = build_trade_table_download_bytes(
        st.session_state.saved_signals,
        symbol=symbol,
        default_qty=int(st.session_state.get("qty", 1) or 1),
    )

    min_date = df["Date"].dt.date.min()
    max_date = df["Date"].dt.date.max()
    default_from_date = min_date

    if (
        st.session_state.get("filter_symbol") != symbol
        or st.session_state.get("filter_data_dir") != str(data_dir)
        or st.session_state.get("filter_output_dir") != str(output_dir)
    ):
        st.session_state.filter_symbol = symbol
        st.session_state.filter_data_dir = str(data_dir)
        st.session_state.filter_output_dir = str(output_dir)
        st.session_state.filter_from_date = default_from_date
        st.session_state.filter_to_date = max_date
        st.session_state.chart_window_start = default_from_date
        st.session_state.filter_source_from = default_from_date
        st.session_state.filter_source_to = max_date

    st.session_state.setdefault("filter_from_date", default_from_date)
    st.session_state.setdefault("filter_to_date", max_date)
    st.session_state.filter_from_date = min(max(st.session_state.filter_from_date, min_date), max_date)
    st.session_state.filter_to_date = min(max(st.session_state.filter_to_date, min_date), max_date)
    if st.session_state.filter_from_date > st.session_state.filter_to_date:
        st.session_state.filter_to_date = st.session_state.filter_from_date

    if st.session_state.show_filters:
        with st.sidebar:
            from_date = st.date_input(
                "From Date",
                min_value=min_date,
                max_value=max_date,
                format="DD/MM/YYYY",
                key="filter_from_date",
            )
            to_date = st.date_input(
                "To Date",
                min_value=min_date,
                max_value=max_date,
                format="DD/MM/YYYY",
                key="filter_to_date",
            )
            st.markdown("<div style='height: 1.2rem;'></div>", unsafe_allow_html=True)
            if st.button("See Dashboard", width="stretch", key="open-output-dashboard"):
                render_output_dashboard_dialog(output_dir)
    else:
        from_date = st.date_input(
            "From Date",
            min_value=min_date,
            max_value=max_date,
            format="DD/MM/YYYY",
            key="filter_from_date",
        )
        to_date = st.date_input(
            "To Date",
            min_value=min_date,
            max_value=max_date,
            format="DD/MM/YYYY",
            key="filter_to_date",
        )

    requested_from_date = from_date
    requested_to_date = to_date

    if (
        st.session_state.chart_window_start is None
        or st.session_state.filter_source_from != requested_from_date
        or st.session_state.filter_source_to != requested_to_date
    ):
        st.session_state.chart_window_start = requested_from_date
        st.session_state.filter_source_from = requested_from_date
        st.session_state.filter_source_to = requested_to_date

    from_date = max(min_date, st.session_state.chart_window_start)
    to_date = min(max_date, compute_chart_window_end(from_date, requested_to_date))

    if from_date > to_date:
        st.error("From Date cannot be after To Date.")
        return

    chart_df, candle_data, was_limited = prepare_candle_data(df, from_date, to_date)
    if chart_df.empty:
        st.warning("No candles available for the selected date range.")
        return

    sync_clicked_candle_with_view(chart_df)
    ema_data = prepare_ema_data(chart_df)

    latest_row = chart_df.iloc[-1]
    range_start = from_date
    range_end = to_date
    range_label = f"{pd.to_datetime(range_start):%d-%b-%Y} to {pd.to_datetime(range_end):%d-%b-%Y}"
    navigation_limit_date = min(max_date, requested_to_date)
    next_from_date = to_date
    next_to_date = min(max_date, compute_chart_window_end(next_from_date, navigation_limit_date))

    col_left, col_center, col_right = st.columns([1.8, 2, 4.2], gap="small")
    with col_left:
        header_left_placeholder = st.empty()
    with col_center:
        st.markdown("<div class='header-spacer'></div>", unsafe_allow_html=True)
        btn_left, btn_mid, btn_right = st.columns([0.05, 1.9, 1.9], gap="small")
        with btn_left:
            st.empty()
        with btn_mid:
            next_month_clicked = st.button(
                "Next Month",
                width="stretch",
                key="header-next-month",
            )
        with btn_right:
            trades_clicked = st.button(
                "Trades",
                width="stretch",
                key="header-trades-toggle",
            )
        if next_month_clicked and next_from_date < navigation_limit_date and next_to_date > next_from_date:
            st.session_state.chart_window_start = next_from_date
            st.rerun()
        if trades_clicked:
            st.session_state.show_saved_signals_panel = not st.session_state.show_saved_signals_panel
            st.rerun()
    with col_right:
        header_right_placeholder = st.empty()

    col_date, col_time = st.columns([5, 3], gap="small")
    with col_date:
        st.write("")
    with col_time:
        st.write("")

    if st.session_state.show_saved_signals_panel:
        chart_col, table_col = st.columns([4.6, 1.0], gap="small")
    else:
        chart_col = st.container()
        table_col = None

    try:
        js_dir = Path(build_dir) / "static" / "js"
        js_files = sorted(js_dir.glob("main.*.js"))
        build_signature = js_files[-1].name if js_files else "dev"
    except Exception:
        build_signature = "dev"
    st.session_state.build_signature = build_signature

    with chart_col:
        chart_event = tv_chart_component(
            candles=candle_data,
            ema=ema_data,
            markers=build_markers(symbol),
            replayState=normalize_chart_replay_state(st.session_state.get("chart_replay_state")),
            key=f"tv-lite-{symbol}-{st.session_state.chart_reset_nonce}-{build_signature}",
            height=CHART_HEIGHT,
        )

    # --- CHART CLICK LOGIC ---
    if chart_event:
        chart_event_type = chart_event.get("eventType") if isinstance(chart_event, dict) else None

        if isinstance(chart_event, dict):
            if "zoomed" in chart_event:
                st.session_state.chart_zoomed = bool(chart_event.get("zoomed"))
            if "replayState" in chart_event:
                st.session_state.chart_replay_state = normalize_chart_replay_state(
                    chart_event.get("replayState")
                )
        clicked_row = parse_clicked_row(chart_event, chart_df)

        if clicked_row is not None:
            st.session_state.clicked_date = clicked_row["DateLabel"]
            st.session_state.clicked_time = clicked_row["TimeLabel"]
            st.session_state.clicked_epoch = int(clicked_row["TimeEpoch"])

            # store clicked candle info
            st.session_state.clicked_info = {
                "Date": clicked_row["DateLabel"],
                "Time": clicked_row["TimeLabel"],
                "Open": clicked_row["Open"],
                "High": clicked_row["High"],
                "Low": clicked_row["Low"],
                "Close": clicked_row["Close"],
                "EMA": clicked_row["EMA"]
            }

            signal_record = build_signal_record(symbol, clicked_row)
            signal_token = chart_click_token(symbol, signal_record["SignalKey"])
            now_monotonic = time.monotonic()
            previous_token = st.session_state.get("last_chart_click_token")
            previous_at = float(st.session_state.get("last_chart_click_at") or 0.0)
            signal_exists = any(
                item["SignalKey"] == signal_record["SignalKey"]
                for item in st.session_state.saved_signals
            )
            repeated_quick_click = (
                chart_event_type != "chart_double_click"
                and signal_exists
                and previous_token == signal_token
                and (now_monotonic - previous_at) <= 0.8
            )

            if chart_event_type == "chart_double_click" or repeated_quick_click:
                remove_signal(signal_record, output_csv_path)
                st.session_state.last_chart_click_token = None
                st.session_state.last_chart_click_at = 0.0
            else:
                save_signal(signal_record, output_csv_path)
                st.session_state.last_chart_click_token = signal_token
                st.session_state.last_chart_click_at = now_monotonic

    latest_signal = st.session_state.latest_signal
    signal_chip_html = "<div class='signal-chip-placeholder'></div>"
    if latest_signal and latest_signal["Symbol"] == symbol:
        chip_color = BUY_COLOR if latest_signal["Signal"] == "BUY" else SELL_COLOR
        signal_chip_html = (
            f"<div class='header-signal-chip-row'>"
            f"<span class='signal-chip' style='background:{chip_color};'>{latest_signal['Signal']} SIGNAL</span>"
            f"<span class='header-signal-chip-time'>{latest_signal['Date']} {latest_signal['Time']}</span>"
            f"</div>"
        )

    header_left_placeholder.markdown(
        (
            "<div class='header-title-line'>"
            f"<span class='tv-title'>{display_symbol(symbol)} · {TIMEFRAME_LABEL} · NSE</span>"
            f"<span class='header-range-inline'>{range_label}</span>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )
    header_right_placeholder.markdown(
        f"<div class='header-signal-stack'>{signal_chip_html}</div>",
        unsafe_allow_html=True,
    )

    # Saved Signals panel moved to the right column.
    if table_col is not None:
        with table_col:
            st.markdown("**Saved Signals**")
            if st.session_state.saved_signals:
                saved_view = build_saved_signals_trade_table(
                    st.session_state.saved_signals,
                    symbol=symbol,
                    default_qty=int(st.session_state.get("qty", 1) or 1),
                )
                saved_column_order = (
                    [
                        "Sr.No",
                        "Date",
                        "Time",
                        "Trade",
                        "Price",
                        "Entry Date",
                        "Entry Time",
                        "Entry Price",
                        "Exit Date",
                        "Exit Time",
                        "Exit Price",
                        "Qty",
                        "PL Points",
                        "PL Amt",
                        "Candle Analysis",
                    ]
                    if st.session_state.get("chart_zoomed", False)
                    else ["Date", "Time", "Trade", "Price", "Candle Analysis"]
                )
                selected_rows = [
                    int(row)
                    for row in st.session_state.get("saved_signals_selected_rows", [])
                    if isinstance(row, int) or str(row).isdigit()
                ]
                selected_keys = [
                    saved_view.iloc[row]["SignalKey"]
                    for row in selected_rows
                    if 0 <= row < len(saved_view)
                ]
                if st.button(
                    "Remove Selected",
                    width="stretch",
                    disabled=not bool(selected_keys),
                    key="remove-selected-saved-signals",
                ):
                    updated_signals = [
                        item for item in st.session_state.saved_signals
                        if item["SignalKey"] not in set(selected_keys)
                    ]
                    try:
                        persisted_signals = persist_saved_signals_file(output_csv_path, symbol, updated_signals)
                    except Exception as exc:
                        st.error(f"Could not update saved-signal file: {exc}")
                    else:
                        apply_saved_signals_state(persisted_signals, symbol, output_csv_path)
                        st.rerun()
                selection = st.dataframe(
                    style_saved_signals_table(saved_view, selected_rows=selected_rows),
                    width="stretch",
                    height=_table_height_for_rows(len(saved_view)),
                    hide_index=True,
                    on_select=_sync_saved_signals_selection,
                    selection_mode="multi-cell",
                    key="saved-signals-table",
                    column_order=saved_column_order,
                )
            else:
                st.caption("Click a candle to automatically create BUY or SELL signal.")

            st.markdown("**Reload Data**")
            if st.button("Reload Data", width="stretch", key="reload-output-data"):
                try:
                    reload_level, reload_message, persisted_saved_signals = reload_selected_drive_output_for_symbol(
                        drive_status=drive_status,
                        symbol=symbol,
                        output_dir=output_dir,
                        input_df=df,
                    )
                except Exception as exc:
                    reload_level = "error"
                    reload_message = f"Could not reload Google Drive Output data for {display_symbol(symbol)}: {exc}"
                    persisted_saved_signals = None
                st.session_state.output_reload_feedback_level = reload_level
                st.session_state.output_reload_feedback_message = reload_message
                if persisted_saved_signals is not None:
                    st.session_state.selected_symbol_restore = symbol
                    apply_saved_signals_state(persisted_saved_signals, symbol, output_csv_path)
                    st.session_state.confirm_clear_all = False
                    st.rerun()
            reload_feedback_level = st.session_state.get("output_reload_feedback_level")
            reload_feedback_message = str(st.session_state.get("output_reload_feedback_message") or "").strip()
            if reload_feedback_level and reload_feedback_message:
                reload_feedback_fn = {
                    "success": st.success,
                    "warning": st.warning,
                    "error": st.error,
                }.get(reload_feedback_level, st.info)
                reload_feedback_fn(reload_feedback_message)

            st.markdown("**Update Data**")
            if st.button("Update Data", width="stretch", key="update-trade-data"):
                level, message, manual_download = update_trade_data_in_google_drive(
                    drive_status=drive_status,
                    symbol=symbol,
                    trade_data_bytes=trade_download_bytes,
                )
                st.session_state.output_update_feedback_level = level
                st.session_state.output_update_feedback_message = message
                st.session_state.output_update_manual_download = manual_download
                if level == "success":
                    st.session_state.drive_output_sync_completed = False
                    list_google_drive_folder_files.clear()
            output_feedback_level = st.session_state.get("output_update_feedback_level")
            output_feedback_message = str(st.session_state.get("output_update_feedback_message") or "").strip()
            if output_feedback_level and output_feedback_message:
                feedback_fn = {
                    "success": st.success,
                    "warning": st.warning,
                    "error": st.error,
                }.get(output_feedback_level, st.info)
                feedback_fn(output_feedback_message)
            output_manual_download = st.session_state.get("output_update_manual_download")
            if isinstance(output_manual_download, dict):
                file_name = str(output_manual_download.get("file_name") or f"{symbol}.csv")
                file_bytes = output_manual_download.get("data") or b""
                mime_type = str(output_manual_download.get("mime") or "text/csv")
                st.download_button(
                    f"Download {file_name}",
                    data=file_bytes,
                    file_name=file_name,
                    mime=mime_type,
                    width="stretch",
                    key=f"download-missing-output-{file_name}",
                )

if __name__ == "__main__":
    main()
