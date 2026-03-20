from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

DATE_OUTPUT_FORMAT = "%d-%b-%y"
TIME_OUTPUT_FORMAT = "%H.%M"
SUPPORTED_DATA_EXTENSIONS = (".csv", ".xlsx", ".xlsm", ".xlsb")


@dataclass
class ProcessedSymbolResult:
    symbol: str
    output_path: Path
    source_count: int
    row_count: int
    refreshed: bool


@dataclass
class ProcessingSummary:
    processed: list[ProcessedSymbolResult]
    skipped: list[ProcessedSymbolResult]
    errors: list[str]


def extract_symbol(file_name: str) -> str:
    name = Path(file_name).stem
    name = name.replace("NSE_", "")
    name = re.sub(r",.*", "", name)
    return name.capitalize()


def list_raw_symbols(raw_dir: Path) -> dict[str, list[Path]]:
    symbol_files: dict[str, list[Path]] = {}
    source_paths = [
        source_path
        for source_path in sorted(raw_dir.iterdir())
        if source_path.is_file() and source_path.suffix.lower() in SUPPORTED_DATA_EXTENSIONS
    ]
    for source_path in source_paths:
        symbol = extract_symbol(source_path.name)
        symbol_files.setdefault(symbol, []).append(source_path)
    return symbol_files


def read_tabular_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(file_path)
    if suffix in {".xlsx", ".xlsm"}:
        return pd.read_excel(file_path)
    if suffix == ".xlsb":
        return pd.read_excel(file_path, engine="pyxlsb")
    raise ValueError(f"Unsupported file type: {file_path.suffix}")


def normalize_cached_time_series(series: pd.Series) -> pd.Series:
    text = series.astype(str).str.strip().str.replace(":", ".", regex=False)
    parts = text.str.extract(r"^(\d{1,2})(?:\.(\d{1,2}))?$")

    hour = pd.to_numeric(parts[0], errors="coerce")
    minute = pd.to_numeric(parts[1], errors="coerce")
    minute = minute.fillna(0)
    one_digit_minute = parts[1].str.len() == 1
    minute = minute.where(~one_digit_minute, minute * 10)

    valid = hour.between(0, 23) & minute.between(0, 59)
    out = text.copy()
    out.loc[valid] = (
        hour.loc[valid].astype(int).astype(str).str.zfill(2)
        + "."
        + minute.loc[valid].astype(int).astype(str).str.zfill(2)
    )
    return out


def clean_symbol(files: list[Path]) -> pd.DataFrame:
    dfs = [read_tabular_file(csv_path) for csv_path in files]
    df = pd.concat(dfs, ignore_index=True)

    normalized_columns = {column: str(column).strip() for column in df.columns}
    df = df.rename(columns=normalized_columns)

    if "time" not in df.columns:
        raise ValueError("Column 'time' not found in selected data file")

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time", kind="stable")

    rename_map = {
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "ema": "EMA",
        "Open": "Open",
        "High": "High",
        "Low": "Low",
        "Close": "Close",
        "EMA": "EMA",
    }
    df = df.rename(columns=rename_map)

    required = ["Open", "High", "Low", "Close", "EMA"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    df = df[df["EMA"].notna()]
    df = df[df["EMA"] != 0]

    df["Date"] = df["time"].dt.strftime(DATE_OUTPUT_FORMAT)
    df["Time"] = df["time"].dt.strftime(TIME_OUTPUT_FORMAT)
    df = df.drop_duplicates(subset=["Date", "Time"])

    out = df.loc[:, ["Date", "Time", "Open", "High", "Low", "Close", "EMA"]].copy()
    out["Date"] = out["Date"].astype(str).str.strip()
    out["Time"] = normalize_cached_time_series(out["Time"])
    out["DateObj"] = pd.to_datetime(out["Date"], format=DATE_OUTPUT_FORMAT, errors="coerce")
    out = out.dropna(subset=["DateObj"]).sort_values(["DateObj", "Time"], kind="stable")
    return out.reset_index(drop=True)


def should_refresh_clean_file(source_files: list[Path], output_path: Path) -> bool:
    if not output_path.exists():
        return True

    try:
        output_mtime = output_path.stat().st_mtime
        return any(source_path.stat().st_mtime > output_mtime for source_path in source_files)
    except OSError:
        return True


def process_symbol_files(source_files: list[Path], output_path: Path) -> ProcessedSymbolResult:
    refreshed = should_refresh_clean_file(source_files, output_path)
    if refreshed:
        cleaned_df = clean_symbol(source_files)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        cleaned_df.drop(columns=["DateObj"], errors="ignore").to_csv(output_path, index=False)

    cached_df = pd.read_csv(
        output_path,
        dtype={
            "Date": "string",
            "Time": "string",
        },
    )
    row_count = len(cached_df)
    return ProcessedSymbolResult(
        symbol=output_path.stem,
        output_path=output_path,
        source_count=len(source_files),
        row_count=row_count,
        refreshed=refreshed,
    )


def process_raw_folder(raw_dir: Path, input_dir: Path) -> ProcessingSummary:
    raw_dir = Path(raw_dir)
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)

    symbol_files = list_raw_symbols(raw_dir)
    if not symbol_files:
        return ProcessingSummary(processed=[], skipped=[], errors=["No raw supported data files found."])

    processed: list[ProcessedSymbolResult] = []
    skipped: list[ProcessedSymbolResult] = []
    errors: list[str] = []

    for symbol, source_files in sorted(symbol_files.items(), key=lambda item: item[0].lower()):
        output_path = input_dir / f"{symbol}.csv"
        try:
            result = process_symbol_files(source_files, output_path)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
            continue

        if result.refreshed:
            processed.append(result)
        else:
            skipped.append(result)

    return ProcessingSummary(processed=processed, skipped=skipped, errors=errors)
