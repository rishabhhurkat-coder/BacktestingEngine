from __future__ import annotations

import calendar
import shutil
import subprocess
import sys
import time
from pathlib import Path
import tempfile
from typing import Any
from uuid import uuid4

import pandas as pd
import streamlit as st

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
                use_container_width=True,
                key=f"drive-dialog-download-{file_name}",
            )

    process_col, cancel_col = st.columns(2, gap="small")
    with process_col:
        if st.button("Process Selected Drive Scrips", use_container_width=True):
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
            st.session_state.selected_symbol = None
            list_google_drive_folder_files.clear()
            list_symbols.clear()
            load_data.clear()
            st.rerun()
    with cancel_col:
        if st.button("Cancel", use_container_width=True):
            st.session_state.drive_dialog_feedback_level = None
            st.session_state.drive_dialog_feedback_message = ""
            st.session_state.drive_manual_input_downloads = []
            st.session_state.show_drive_process_dialog = False
            st.rerun()


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
                if st.button("Main Folder", use_container_width=True):
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
                if st.button("Process Input Files", use_container_width=True):
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
                use_container_width=True,
                key="header-next-month",
            )
        with btn_right:
            trades_clicked = st.button(
                "Trades",
                use_container_width=True,
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
            if st.button("Reload Data", use_container_width=True, key="reload-output-data"):
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
            if st.button("Update Data", use_container_width=True, key="update-trade-data"):
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
                    use_container_width=True,
                    key=f"download-missing-output-{file_name}",
                )

if __name__ == "__main__":
    main()
