from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

DATE_OUTPUT_FORMAT = "%d-%b-%y"
TIME_OUTPUT_FORMAT = "%H.%M"
SUPPORTED_DATA_EXTENSIONS = (".csv", ".xlsx", ".xlsm", ".xlsb")
CORE_OUTPUT_COLUMNS = ["Date", "Time", "Open", "High", "Low", "Close"]
CORE_SOURCE_COLUMN_ALIASES = {
    "open": "Open",
    "high": "High",
    "low": "Low",
    "close": "Close",
}


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


@dataclass(frozen=True)
class InstrumentIdentity:
    exchange: str
    symbol: str
    timeframe_value: str
    timeframe_label: str
    storage_stem: str


def format_timeframe_label(timeframe_value: Any) -> str:
    raw_value = str(timeframe_value or "").strip().lower().replace(" ", "")
    if not raw_value:
        return ""
    if raw_value.endswith("m"):
        return raw_value
    if raw_value.endswith("min"):
        return raw_value[:-3] + "m"
    if raw_value.endswith("minute"):
        return raw_value[:-6] + "m"
    if raw_value.endswith("h"):
        return raw_value
    return f"{raw_value}m"


def build_instrument_storage_stem(exchange: str, symbol: str, timeframe_label: str) -> str:
    safe_exchange = re.sub(r"[^A-Za-z0-9]+", "", str(exchange or "").upper()) or "NA"
    safe_symbol = re.sub(r"[^A-Za-z0-9]+", "", str(symbol or "").upper()) or "UNKNOWN"
    safe_timeframe = re.sub(r"[^A-Za-z0-9]+", "", str(timeframe_label or "").lower()) or "na"
    return f"{safe_exchange}__{safe_symbol}__{safe_timeframe}"


def parse_instrument_identity(file_name: str) -> InstrumentIdentity:
    name = Path(file_name).stem.strip()
    composite_match = re.match(r"^(?P<exchange>[A-Za-z0-9]+)__(?P<symbol>[A-Za-z0-9]+)__(?P<timeframe>[A-Za-z0-9]+)$", name)
    if composite_match:
        exchange = composite_match.group("exchange").upper()
        symbol = composite_match.group("symbol").upper()
        timeframe_label = format_timeframe_label(composite_match.group("timeframe"))
        return InstrumentIdentity(
            exchange=exchange,
            symbol=symbol,
            timeframe_value=timeframe_label[:-1] if timeframe_label.endswith("m") else timeframe_label,
            timeframe_label=timeframe_label,
            storage_stem=build_instrument_storage_stem(exchange, symbol, timeframe_label),
        )

    raw_match = re.match(
        r"^(?:(?P<exchange>[A-Za-z0-9]+)_)?(?P<symbol>[A-Za-z0-9&\-_]+)\s*,\s*(?P<timeframe>\d+)\b",
        name,
        flags=re.IGNORECASE,
    )
    if raw_match:
        exchange = (raw_match.group("exchange") or "NSE").upper()
        symbol = re.sub(r"[^A-Za-z0-9]+", "", raw_match.group("symbol") or "").upper() or "UNKNOWN"
        timeframe_value = str(raw_match.group("timeframe") or "").strip()
        timeframe_label = format_timeframe_label(timeframe_value)
        return InstrumentIdentity(
            exchange=exchange,
            symbol=symbol,
            timeframe_value=timeframe_value,
            timeframe_label=timeframe_label,
            storage_stem=build_instrument_storage_stem(exchange, symbol, timeframe_label),
        )

    cleaned_name = re.sub(r"[^A-Za-z0-9]+", "", name).upper() or "UNKNOWN"
    return InstrumentIdentity(
        exchange="NSE",
        symbol=cleaned_name,
        timeframe_value="3",
        timeframe_label="3m",
        storage_stem=build_instrument_storage_stem("NSE", cleaned_name, "3m"),
    )


def extract_symbol(file_name: str) -> str:
    return parse_instrument_identity(file_name).symbol.capitalize()


def list_raw_symbols(raw_dir: Path) -> dict[str, list[Path]]:
    symbol_files: dict[str, list[Path]] = {}
    source_paths = [
        source_path
        for source_path in sorted(raw_dir.iterdir())
        if source_path.is_file() and source_path.suffix.lower() in SUPPORTED_DATA_EXTENSIONS
    ]
    for source_path in source_paths:
        storage_stem = parse_instrument_identity(source_path.name).storage_stem
        symbol_files.setdefault(storage_stem, []).append(source_path)
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


def strip_duplicate_suffix(column_name: Any) -> str:
    return re.sub(r"\.\d+$", "", str(column_name).strip())


def normalize_indicator_family(column_name: Any) -> str:
    base = strip_duplicate_suffix(column_name)
    normalized = re.sub(r"[\s_\-]+", " ", base).strip().lower()
    if normalized in {
        "up trend",
        "down trend",
        "super trend",
        "supertrend",
        "super trend up",
        "supertrend up",
        "super trend down",
        "supertrend down",
        "super trend dn",
        "supertrend dn",
    }:
        return "super trend"
    return normalized


def is_core_source_column(column_name: Any) -> bool:
    return normalize_indicator_family(column_name) in {"time", "open", "high", "low", "close"}


def format_indicator_label(text: str) -> str:
    normalized = re.sub(r"[\s_\-]+", " ", str(text).strip())
    if not normalized:
        return ""
    parts = normalized.split(" ")
    formatted_parts: list[str] = []
    for part in parts:
        if part.isupper():
            formatted_parts.append(part)
        elif part.isalpha() and len(part) <= 3:
            formatted_parts.append(part.upper())
        else:
            formatted_parts.append(part.title())
    return " ".join(formatted_parts)


def default_indicator_label(column_name: Any) -> str:
    base = strip_duplicate_suffix(column_name)
    normalized_base = re.sub(r"[\s_\-]+", " ", base).strip().lower()
    normalized_family = normalize_indicator_family(column_name)
    if normalized_base in {"up trend", "super trend up", "supertrend up"}:
        return "SuperTrend Up"
    if normalized_base in {"down trend", "super trend down", "supertrend down", "super trend dn", "supertrend dn"}:
        return "SuperTrend Down"
    if normalized_family == "ema":
        return "EMA"
    return format_indicator_label(strip_duplicate_suffix(column_name))


def list_raw_indicator_columns(raw_dir: Path) -> list[str]:
    raw_dir = Path(raw_dir)
    source_paths = [
        source_path
        for source_path in sorted(raw_dir.iterdir())
        if source_path.is_file() and source_path.suffix.lower() in SUPPORTED_DATA_EXTENSIONS
    ]
    if not source_paths:
        return []

    raw_df = read_tabular_file(source_paths[0])
    columns = [str(column).strip() for column in raw_df.columns]
    return [
        column
        for column in columns
        if column and not is_core_source_column(column) and not column.lower().startswith("unnamed:")
    ]


def inspect_indicator_requirements(raw_dir: Path) -> list[dict[str, Any]]:
    indicator_columns = list_raw_indicator_columns(raw_dir)
    grouped_columns: dict[str, list[str]] = {}
    for column in indicator_columns:
        family_key = normalize_indicator_family(column)
        grouped_columns.setdefault(family_key, []).append(column)

    requirements: list[dict[str, Any]] = []
    for family_key, columns in sorted(grouped_columns.items(), key=lambda item: item[0]):
        if family_key == "super trend":
            continue
        if len(columns) <= 1:
            continue
        family_label = format_indicator_label(strip_duplicate_suffix(columns[0]))
        requirements.append(
            {
                "family_key": family_key,
                "family_label": family_label,
                "columns": columns,
            }
        )
    return requirements


def resolve_indicator_label_map(
    source_columns: list[str],
    configured_labels: dict[str, str] | None = None,
) -> dict[str, str]:
    configured_labels = configured_labels or {}
    resolved: dict[str, str] = {}
    used_labels: set[str] = set()

    for source_column in source_columns:
        requested_label = str(configured_labels.get(source_column) or default_indicator_label(source_column)).strip()
        label = requested_label or default_indicator_label(source_column)
        base_label = label
        counter = 2
        while label in used_labels:
            label = f"{base_label} {counter}"
            counter += 1
        resolved[source_column] = label
        used_labels.add(label)

    return resolved


def select_primary_ema_column(indicator_columns: list[str]) -> str | None:
    ema_columns = [
        column
        for column in indicator_columns
        if normalize_indicator_family(column).startswith("ema")
    ]
    if not ema_columns:
        return None

    def ema_sort_key(column_name: str) -> tuple[int, str]:
        match = re.search(r"(\d+)", column_name)
        magnitude = int(match.group(1)) if match else -1
        return (magnitude, column_name.lower())

    return sorted(ema_columns, key=ema_sort_key, reverse=True)[0]


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


def clean_symbol(files: list[Path], indicator_labels: dict[str, str] | None = None) -> pd.DataFrame:
    dfs = [read_tabular_file(csv_path) for csv_path in files]
    df = pd.concat(dfs, ignore_index=True)

    normalized_columns = {column: str(column).strip() for column in df.columns}
    df = df.rename(columns=normalized_columns)

    if "time" not in df.columns:
        raise ValueError("Column 'time' not found in selected data file")

    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time"]).sort_values("time", kind="stable")

    indicator_source_columns = [
        str(column).strip()
        for column in df.columns
        if str(column).strip()
        and not is_core_source_column(column)
        and str(column).strip() != "time"
        and not str(column).strip().lower().startswith("unnamed:")
    ]
    indicator_output_map = resolve_indicator_label_map(indicator_source_columns, indicator_labels)

    rename_map = {
        column: CORE_SOURCE_COLUMN_ALIASES.get(normalize_indicator_family(column), column)
        for column in df.columns
    }
    rename_map.update(indicator_output_map)
    df = df.rename(columns=rename_map)

    required = ["Open", "High", "Low", "Close"]
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")

    indicator_output_columns = list(dict.fromkeys(indicator_output_map.values()))
    numeric_columns = [*required, *indicator_output_columns]
    if numeric_columns:
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

    primary_ema_column = select_primary_ema_column(indicator_output_columns)
    if primary_ema_column and primary_ema_column in df.columns:
        df = df[df[primary_ema_column].notna()]
        df = df[df[primary_ema_column] != 0]

    df["Date"] = df["time"].dt.strftime(DATE_OUTPUT_FORMAT)
    df["Time"] = df["time"].dt.strftime(TIME_OUTPUT_FORMAT)
    df = df.drop_duplicates(subset=["Date", "Time"])

    ordered_columns = [*CORE_OUTPUT_COLUMNS]
    if primary_ema_column:
        ordered_columns.append("EMA")
    ordered_columns.extend(
        column
        for column in indicator_output_columns
        if column not in ordered_columns
    )

    out = df.loc[:, [*CORE_OUTPUT_COLUMNS, *indicator_output_columns]].copy()
    if primary_ema_column and primary_ema_column in out.columns and primary_ema_column != "EMA":
        out.insert(6, "EMA", out[primary_ema_column])
    out["Date"] = out["Date"].astype(str).str.strip()
    out["Time"] = normalize_cached_time_series(out["Time"])
    out["DateObj"] = pd.to_datetime(out["Date"], format=DATE_OUTPUT_FORMAT, errors="coerce")
    out = out.dropna(subset=["DateObj"]).sort_values(["DateObj", "Time"], kind="stable")
    return out.loc[:, [column for column in [*ordered_columns, "DateObj"] if column in out.columns]].reset_index(drop=True)


def should_refresh_clean_file(source_files: list[Path], output_path: Path) -> bool:
    if not output_path.exists():
        return True

    try:
        output_mtime = output_path.stat().st_mtime
        return any(source_path.stat().st_mtime > output_mtime for source_path in source_files)
    except OSError:
        return True


def process_symbol_files(
    source_files: list[Path],
    output_path: Path,
    indicator_labels: dict[str, str] | None = None,
) -> ProcessedSymbolResult:
    refreshed = should_refresh_clean_file(source_files, output_path)
    if refreshed:
        cleaned_df = clean_symbol(source_files, indicator_labels=indicator_labels)
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


def process_raw_folder(
    raw_dir: Path,
    input_dir: Path,
    indicator_labels: dict[str, str] | None = None,
) -> ProcessingSummary:
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
            result = process_symbol_files(source_files, output_path, indicator_labels=indicator_labels)
        except Exception as exc:
            errors.append(f"{symbol}: {exc}")
            continue

        if result.refreshed:
            processed.append(result)
        else:
            skipped.append(result)

    return ProcessingSummary(processed=processed, skipped=skipped, errors=errors)
