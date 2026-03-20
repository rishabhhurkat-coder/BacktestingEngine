from __future__ import annotations

import calendar
import io
import subprocess
import sys
from pathlib import Path
import tempfile
from typing import Any
from uuid import uuid4
import zipfile

import pandas as pd
import streamlit as st

from component import tv_chart_component, build_dir
from data_pipeline import process_raw_folder

BASE_DIR = Path(__file__).resolve().parent

if __name__ == "__main__" and "streamlit.web.bootstrap" not in sys.modules:
    creation_flags = 0
    if sys.platform.startswith("win"):
        creation_flags = (
            getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )

    subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", str(BASE_DIR / "streamlit_app.py")],
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
UPLOAD_DATA_TYPES = ["csv", "xlsx", "xlsm", "xlsb"]
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


def folder_has_supported_data_files(folder: Path) -> bool:
    return bool(list_supported_data_files(folder))


def sync_uploaded_data_folder(uploaded_files: list[Any], target_dir: Path) -> int:
    return sync_uploaded_data_folder_as(uploaded_files, target_dir, convert_to_csv=False)


def sync_uploaded_data_folder_as(
    uploaded_files: list[Any],
    target_dir: Path,
    *,
    convert_to_csv: bool,
) -> int:
    target_dir.mkdir(parents=True, exist_ok=True)
    clear_supported_data_files(target_dir)
    for uploaded_file in uploaded_files:
        uploaded_name = Path(str(uploaded_file.name)).name
        if convert_to_csv:
            target_path = csv_path_for_stem(target_dir, Path(uploaded_name).stem)
            uploaded_df = read_tabular_source(
                io.BytesIO(uploaded_file.getbuffer()),
                Path(uploaded_name).suffix.lower(),
            )
            write_tabular_file(uploaded_df, target_path)
        else:
            target_path = target_dir / uploaded_name
            target_path.write_bytes(uploaded_file.getbuffer())
    return len(uploaded_files)


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


def merge_processed_input_dirs(existing_input_dir: Path, new_input_dir: Path) -> None:
    existing_input_dir.mkdir(parents=True, exist_ok=True)
    for new_csv_path in list_supported_data_files(new_input_dir):
        existing_path = find_data_file_by_stem(existing_input_dir, new_csv_path.stem)
        target_csv_path = csv_path_for_stem(existing_input_dir, new_csv_path.stem)
        new_df = normalize_processed_input_df(read_tabular_file(new_csv_path))
        if existing_path and existing_path.exists():
            existing_df = normalize_processed_input_df(read_tabular_file(existing_path))
            merged_df = normalize_processed_input_df(pd.concat([existing_df, new_df], ignore_index=True))
        else:
            merged_df = new_df
        write_tabular_file(merged_df.drop(columns=["DateObj"], errors="ignore"), target_csv_path)
        remove_other_matching_data_files(existing_input_dir, new_csv_path.stem, target_csv_path)


def process_cloud_uploaded_files(
    uploaded_raw_files: list[Any],
    uploaded_input_files: list[Any],
    uploaded_output_files: list[Any],
    process_uploaded_input: bool,
    main_dir: Path,
) -> tuple[str, str]:
    raw_dir, input_dir, output_dir = ensure_workspace_dirs(main_dir)
    messages: list[str] = []
    level = "success"

    if uploaded_output_files:
        sync_uploaded_data_folder_as(uploaded_output_files, output_dir, convert_to_csv=True)
        messages.append(f"Loaded {len(uploaded_output_files)} output file(s) as CSV")

    if uploaded_input_files:
        sync_uploaded_data_folder_as(uploaded_input_files, input_dir, convert_to_csv=True)
        messages.append(f"Loaded {len(uploaded_input_files)} input file(s) as CSV")

    if uploaded_raw_files:
        sync_uploaded_data_folder(uploaded_raw_files, raw_dir)
        messages.append(f"Loaded {len(uploaded_raw_files)} raw file(s)")

        if process_uploaded_input or not uploaded_input_files:
            temp_processed_dir = Path(tempfile.mkdtemp(prefix="ema_processed_"))
            try:
                summary = process_raw_folder(raw_dir, temp_processed_dir)
                if uploaded_input_files and process_uploaded_input:
                    merge_processed_input_dirs(input_dir, temp_processed_dir)
                    messages.append("Merged processed raw files with uploaded input files")
                else:
                    clear_supported_data_files(input_dir)
                    merge_processed_input_dirs(input_dir, temp_processed_dir)
                summary_level, summary_message = build_processing_feedback(summary)
                if summary_level == "warning" and level == "success":
                    level = "warning"
                elif summary_level == "error":
                    level = "error"
                messages.append(summary_message)
            finally:
                for temp_csv_path in list_supported_data_files(temp_processed_dir):
                    try:
                        temp_csv_path.unlink()
                    except OSError:
                        continue
                try:
                    temp_processed_dir.rmdir()
                except OSError:
                    pass
        else:
            messages.append("Using uploaded input files as CSV")
    elif uploaded_input_files:
        messages.append("Using uploaded input files as CSV")

    if not messages:
        return "warning", "No files were uploaded."
    return level, ". ".join(messages)


def sync_uploaded_raw_files(uploaded_files: list[Any], main_dir: Path) -> tuple[str, str]:
    raw_dir, input_dir, _ = ensure_workspace_dirs(main_dir)
    clear_supported_data_files(raw_dir)
    clear_supported_data_files(input_dir)

    for uploaded_file in uploaded_files:
        target_path = raw_dir / Path(str(uploaded_file.name)).name
        target_path.write_bytes(uploaded_file.getbuffer())

    summary = process_raw_folder(raw_dir, input_dir)
    return build_processing_feedback(summary)


def build_output_trades_zip(output_dir: Path) -> bytes | None:
    csv_paths = list_supported_data_files(output_dir)
    if not csv_paths:
        return None

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for csv_path in csv_paths:
            archive.writestr(csv_path.name, csv_path.read_bytes())
    return buffer.getvalue()


def read_file_bytes(file_path: Path) -> bytes | None:
    if not file_path.exists():
        return None
    return file_path.read_bytes()


@st.dialog("Upload Files", width="large")
def render_cloud_upload_dialog(main_dir: Path) -> None:
    st.caption("Upload any combination of raw, input, and output folders.")
    uploaded_raw_files = st.file_uploader(
        "Upload Raw Files Folder",
        type=UPLOAD_DATA_TYPES,
        accept_multiple_files="directory",
        key=f"cloud_raw_uploads_{st.session_state.cloud_uploader_nonce}",
    )
    uploaded_input_files = st.file_uploader(
        "Upload Input Files Folder",
        type=UPLOAD_DATA_TYPES,
        accept_multiple_files="directory",
        key=f"cloud_input_uploads_{st.session_state.cloud_input_uploader_nonce}",
    )
    uploaded_output_files = st.file_uploader(
        "Upload Output Files Folder",
        type=UPLOAD_DATA_TYPES,
        accept_multiple_files="directory",
        key=f"cloud_output_uploads_{st.session_state.cloud_output_uploader_nonce}",
    )

    process_choice = "No"
    if uploaded_input_files:
        process_choice = st.radio(
            "Process raw files and merge with uploaded input files?",
            ["No", "Yes"],
            horizontal=True,
            key="cloud_process_choice",
        )

    action_col, cancel_col = st.columns(2, gap="small")
    with action_col:
        if st.button("Use Uploaded Files", use_container_width=True):
            with st.spinner("Applying uploaded files..."):
                level, message = process_cloud_uploaded_files(
                    uploaded_raw_files=uploaded_raw_files or [],
                    uploaded_input_files=uploaded_input_files or [],
                    uploaded_output_files=uploaded_output_files or [],
                    process_uploaded_input=(process_choice == "Yes"),
                    main_dir=main_dir,
                )
            st.session_state.process_feedback_level = level
            st.session_state.process_feedback_message = message
            st.session_state.selected_symbol = None
            st.session_state.show_upload_dialog = False
            list_symbols.clear()
            load_data.clear()
            st.rerun()
    with cancel_col:
        if st.button("Cancel", use_container_width=True):
            st.session_state.show_upload_dialog = False
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


def load_saved_signals_file(csv_path: Path, symbol: str) -> list[dict[str, Any]]:
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return []

    raw_df = read_tabular_file(csv_path)
    normalized_df = normalize_saved_signals_df(raw_df, symbol)
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
                "Exit Date",
                "Exit Time",
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
                "Exit Date",
                "Exit Time",
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
                "Exit Date": exit_date,
                "Exit Time": exit_time,
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
    st.session_state.setdefault("chart_window_start", None)
    st.session_state.setdefault("filter_source_from", None)
    st.session_state.setdefault("filter_source_to", None)
    st.session_state.setdefault("chart_zoomed", False)
    st.session_state.setdefault("main_dir_path_input", "")
    st.session_state.setdefault("data_dir_path_input", "")
    st.session_state.setdefault("output_dir_path_input", "")
    st.session_state.setdefault("process_feedback_level", None)
    st.session_state.setdefault("process_feedback_message", "")
    st.session_state.setdefault("filter_data_dir", None)
    st.session_state.setdefault("filter_output_dir", None)
    st.session_state.setdefault("selected_symbol", None)
    st.session_state.setdefault("cloud_workspace_session_id", str(uuid4()))
    st.session_state.setdefault("cloud_raw_upload_signature", ())
    st.session_state.setdefault("cloud_uploader_nonce", 0)
    st.session_state.setdefault("cloud_input_uploader_nonce", 0)
    st.session_state.setdefault("cloud_output_uploader_nonce", 0)
    st.session_state.setdefault("show_upload_dialog", False)
    cloud_workspace_dir = cloud_workspace_root / st.session_state.cloud_workspace_session_id
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
                has_processed_input_files = folder_has_supported_data_files(input_dir)
                st.session_state.main_dir_path_input = str(cloud_workspace_dir)
                st.session_state.data_dir_path_input = str(input_dir)
                st.session_state.output_dir_path_input = str(output_dir)
                if has_processed_input_files:
                    st.success("Files are ready for this browser session.")

                if st.button("Upload Files", use_container_width=True):
                    st.session_state.show_upload_dialog = True
                    st.rerun()

                if st.button("Reset Uploaded Files", use_container_width=True):
                    clear_supported_data_files(raw_dir)
                    clear_supported_data_files(input_dir)
                    clear_supported_data_files(output_dir)
                    st.session_state.cloud_raw_upload_signature = ()
                    st.session_state.cloud_uploader_nonce += 1
                    st.session_state.cloud_input_uploader_nonce += 1
                    st.session_state.cloud_output_uploader_nonce += 1
                    st.session_state.selected_symbol = None
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

    if not is_windows and st.session_state.show_upload_dialog:
        render_cloud_upload_dialog(cloud_workspace_dir)

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
            st.error("No processed supported data files found in Input Files. Click Process Input Files.")
        else:
            if is_windows:
                st.error(f"No raw supported data files found in {raw_dir}")
            else:
                st.info("Select your raw-files folder from your computer to begin.")
        return

    symbol_names = list(symbols.keys())
    if st.session_state.selected_symbol not in symbol_names:
        st.session_state.selected_symbol = symbol_names[0]

    if st.session_state.show_filters:
        with st.sidebar:
            symbol = st.selectbox("Select Scrip", symbol_names, key="selected_symbol")
            st.number_input(
                "Qty",
                min_value=1,
                step=1,
                format="%d",
                key="qty",
            )
    else:
        symbol = st.selectbox("Select Scrip", symbol_names, key="selected_symbol")

    output_csv_path = output_signal_csv_path(output_dir, symbol)
    if (
        st.session_state.get("saved_signals_symbol") != symbol
        or st.session_state.get("saved_signals_output_csv") != str(output_csv_path)
    ):
        try:
            ensure_output_signal_file(output_dir, symbol)
            loaded_saved_signals = load_saved_signals_file(output_csv_path, symbol)
            persisted_saved_signals = persist_saved_signals_file(output_csv_path, symbol, loaded_saved_signals)
        except Exception as exc:
            st.error(f"Saved-signal file error for {symbol}: {exc}")
            return
        apply_saved_signals_state(persisted_saved_signals, symbol, output_csv_path)
        st.session_state.confirm_clear_all = False

    trade_download_bytes = read_file_bytes(output_csv_path)
    input_download_path = Path(symbols[symbol])
    input_download_bytes = read_file_bytes(input_download_path)

    df = load_data(symbols[symbol])

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
            if chart_event_type == "chart_double_click":
                remove_signal(signal_record, output_csv_path)
            else:
                save_signal(signal_record, output_csv_path)

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
            f"<span class='tv-title'>{symbol} · {TIMEFRAME_LABEL} · NSE</span>"
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
                        "Exit Date",
                        "Exit Time",
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

            st.markdown("**Download Data**")
            st.download_button(
                "Trade Data",
                data=trade_download_bytes or b"",
                file_name=output_csv_path.name,
                mime=tabular_mime_type(output_csv_path),
                use_container_width=True,
                disabled=trade_download_bytes is None,
                key="download-trade-data",
            )
            st.download_button(
                "Updated Input Data",
                data=input_download_bytes or b"",
                file_name=input_download_path.name,
                mime=tabular_mime_type(input_download_path),
                use_container_width=True,
                disabled=input_download_bytes is None,
                key="download-input-data",
            )

if __name__ == "__main__":
    main()
