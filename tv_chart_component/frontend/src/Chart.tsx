import React, { useEffect, useRef, useState } from "react";
import {
  CandlestickData,
  IChartApi,
  ISeriesApi,
  LineData,
  LineStyle,
  SeriesMarker,
  TickMarkType,
  Time,
  UTCTimestamp,
  createChart,
} from "lightweight-charts";
import { Streamlit } from "streamlit-component-lib";

export type Candle = {
  time: string | number;
  open: number;
  high: number;
  low: number;
  close: number;
};

export type EmaPoint = {
  time: string | number;
  value: number;
};

export type Marker = {
  time: string | number;
  position: "aboveBar" | "belowBar" | "inBar";
  shape: "arrowUp" | "arrowDown" | "circle" | "square";
  color: string;
  text?: string;
};

type ReplayState = {
  active?: boolean;
  index?: number | null;
  showStartLine?: boolean;
};

type ChartProps = {
  candles: Candle[];
  ema: EmaPoint[];
  markers?: Marker[];
  height?: number;
  replayState?: ReplayState | null;
};

type ModalMode = "goto" | "replay" | "leaveReplay" | null;

const IST_TIMEZONE = "Asia/Kolkata";

const IST_TIME_FORMAT = new Intl.DateTimeFormat("en-IN", {
  timeZone: IST_TIMEZONE,
  hour: "2-digit",
  minute: "2-digit",
});

const IST_DATE_TIME_FORMAT = new Intl.DateTimeFormat("en-IN", {
  timeZone: IST_TIMEZONE,
  day: "2-digit",
  month: "short",
  year: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
});

const DAY_KEY_FORMAT = new Intl.DateTimeFormat("en-CA", {
  timeZone: IST_TIMEZONE,
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
});

const toTimestamp = (value: string | number): UTCTimestamp | null => {
  if (typeof value === "number" && Number.isFinite(value)) {
    return Math.trunc(value) as UTCTimestamp;
  }

  const text = String(value).trim();
  if (!text) {
    return null;
  }

  if (/^\d+$/.test(text)) {
    return Number(text) as UTCTimestamp;
  }

  const normalized = text.includes("T") ? text : text.replace(" ", "T");
  const parsed = Date.parse(normalized);
  if (!Number.isNaN(parsed)) {
    return Math.floor(parsed / 1000) as UTCTimestamp;
  }

  const parts = text.split(" ");
  if (parts.length !== 2) {
    return null;
  }
  const [datePart, timePart] = parts;
  const [year, month, day] = datePart.split("-").map(Number);
  const [hour, minute] = timePart.split(":").map(Number);
  if ([year, month, day, hour, minute].some((v) => Number.isNaN(v))) {
    return null;
  }
  return Math.floor(Date.UTC(year, month - 1, day, hour, minute) / 1000) as UTCTimestamp;
};

const toTimestampAny = (value: Time | string | number): UTCTimestamp | null => {
  if (typeof value === "number") {
    return toTimestamp(value);
  }
  if (typeof value === "string") {
    return toTimestamp(value);
  }
  if (value && typeof value === "object") {
    const businessDay = value as { year?: number; month?: number; day?: number };
    if (
      typeof businessDay.year === "number" &&
      typeof businessDay.month === "number" &&
      typeof businessDay.day === "number"
    ) {
      return Math.floor(
        Date.UTC(businessDay.year, businessDay.month - 1, businessDay.day, 0, 0) / 1000
      ) as UTCTimestamp;
    }
  }
  return null;
};

const formatIstTime = (timestamp: UTCTimestamp | null, withDate: boolean): string => {
  if (timestamp === null) {
    return "";
  }
  const date = new Date(timestamp * 1000);
  return withDate ? IST_DATE_TIME_FORMAT.format(date) : IST_TIME_FORMAT.format(date);
};

const dayKey = (timestamp: UTCTimestamp): string =>
  DAY_KEY_FORMAT.format(new Date(timestamp * 1000));

const sessionStartTimestamp = (dateValue: string): UTCTimestamp | null => {
  if (!dateValue) {
    return null;
  }
  const [year, month, day] = dateValue.split("-").map(Number);
  if ([year, month, day].some((value) => Number.isNaN(value))) {
    return null;
  }
  return Math.floor(Date.UTC(year, month - 1, day, 3, 45, 0) / 1000) as UTCTimestamp;
};

const inferInterval = (candleData: CandlestickData<UTCTimestamp>[]): number => {
  for (let i = 1; i < candleData.length; i += 1) {
    const delta = candleData[i].time - candleData[i - 1].time;
    if (delta > 0) {
      return delta;
    }
  }
  return 180;
};

const computeSessionBreakTimes = (
  candleData: CandlestickData<UTCTimestamp>[]
): UTCTimestamp[] => {
  if (candleData.length === 0) {
    return [];
  }

  const breaks: UTCTimestamp[] = [];
  let previousDay = dayKey(candleData[0].time);
  for (let i = 1; i < candleData.length; i += 1) {
    const currentDay = dayKey(candleData[i].time);
    if (currentDay !== previousDay) {
      breaks.push(candleData[i].time);
      previousDay = currentDay;
    }
  }
  return breaks;
};

const normalizeCandles = (candles: Candle[]): CandlestickData<UTCTimestamp>[] =>
  candles
    .map((item) => {
      const ts = toTimestamp(item.time);
      if (ts === null) {
        return null;
      }
      return {
        time: ts,
        open: item.open,
        high: item.high,
        low: item.low,
        close: item.close,
      };
    })
    .filter((item): item is CandlestickData<UTCTimestamp> => item !== null);

const normalizeEma = (ema: EmaPoint[]): LineData<UTCTimestamp>[] =>
  ema
    .map((item) => {
      const ts = toTimestamp(item.time);
      if (ts === null) {
        return null;
      }
      return {
        time: ts,
        value: item.value,
      };
    })
    .filter((item): item is LineData<UTCTimestamp> => item !== null);

const normalizeMarkers = (markers: Marker[]): SeriesMarker<UTCTimestamp>[] =>
  markers
    .map((item): SeriesMarker<UTCTimestamp> | null => {
      const ts = toTimestamp(item.time);
      if (ts === null) {
        return null;
      }
      const marker: SeriesMarker<UTCTimestamp> = {
        time: ts,
        position: item.position,
        shape: item.shape,
        color: item.color,
      };
      if (item.text) {
        marker.text = item.text;
      }
      return marker;
    })
    .filter((item): item is SeriesMarker<UTCTimestamp> => item !== null);

const normalizeReplayState = (value: ReplayState | null | undefined): ReplayState => {
  const active = Boolean(value?.active);
  const rawIndex = value?.index;
  const index =
    rawIndex === null || rawIndex === undefined || Number.isNaN(Number(rawIndex))
      ? null
      : Math.max(0, Math.trunc(Number(rawIndex)));
  const showStartLine = active && Boolean(value?.showStartLine);
  return {
    active,
    index: active ? index : null,
    showStartLine,
  };
};

const Chart = ({
  candles,
  ema,
  markers = [],
  height = 600,
  replayState,
}: ChartProps): React.ReactElement => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const emaSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const lastCandleTsRef = useRef<UTCTimestamp | null>(null);
  const fullCandleDataRef = useRef<CandlestickData<UTCTimestamp>[]>([]);
  const fullEmaDataRef = useRef<LineData<UTCTimestamp>[]>([]);
  const fullMarkerDataRef = useRef<SeriesMarker<UTCTimestamp>[]>([]);
  const intervalRef = useRef<number>(180);
  const replayIndexRef = useRef<number | null>(null);
  const replayActiveRef = useRef(false);
  const showReplayStartLineRef = useRef(false);
  const sessionBreakTimesRef = useRef<UTCTimestamp[]>([]);
  const resetViewRef = useRef<(() => void) | null>(null);
  const hasFitRef = useRef(false);
  const isReadyRef = useRef(false);
  const zoomedStateRef = useRef(false);
  const pendingClickTimerRef = useRef<number | null>(null);
  const pendingClickEpochRef = useRef<UTCTimestamp | null>(null);

  const [modalMode, setModalMode] = useState<ModalMode>(null);
  const [dateValue, setDateValue] = useState("");
  const [sessionBreakXs, setSessionBreakXs] = useState<number[]>([]);
  const [replayCursorX, setReplayCursorX] = useState<number | null>(null);
  const [replayActive, setReplayActive] = useState(false);
  const [showReplayStartLine, setShowReplayStartLine] = useState(false);

  const getIsZoomed = (): boolean => {
    const chart = chartRef.current;
    const candleData = fullCandleDataRef.current;
    if (!chart || candleData.length < 2) {
      return false;
    }
    if (replayActiveRef.current) {
      return true;
    }

    const range = chart.timeScale().getVisibleRange();
    if (!range) {
      return false;
    }

    const visibleFrom = toTimestampAny(range.from);
    const visibleTo = toTimestampAny(range.to);
    if (visibleFrom === null || visibleTo === null) {
      return false;
    }

    const fullSpan = candleData[candleData.length - 1].time - candleData[0].time;
    const visibleSpan = Math.max(0, visibleTo - visibleFrom);
    const interval = intervalRef.current || inferInterval(candleData);
    return visibleSpan < fullSpan - interval * 2;
  };

  const emitComponentEvent = (payload: Record<string, unknown> = {}) => {
    Streamlit.setComponentValue({
      ...payload,
      zoomed: getIsZoomed(),
      replayState: {
        active: replayActiveRef.current,
        index: replayIndexRef.current,
        showStartLine: showReplayStartLineRef.current,
      },
    });
  };

  const emitZoomStateIfChanged = () => {
    const zoomed = getIsZoomed();
    if (zoomed === zoomedStateRef.current) {
      return;
    }
    zoomedStateRef.current = zoomed;
    emitComponentEvent({ zoomed });
  };

  const updateOverlayPositions = () => {
    const chart = chartRef.current;
    if (!chart) {
      return;
    }
    const timeScale = chart.timeScale();
    const nextSessionBreakXs = sessionBreakTimesRef.current.reduce<number[]>(
      (positions, time) => {
        const x = timeScale.timeToCoordinate(time);
        if (x !== null) {
          positions.push(Number(x));
        }
        return positions;
      },
      []
    );
    setSessionBreakXs(nextSessionBreakXs);

    if (replayActiveRef.current && replayIndexRef.current !== null && showReplayStartLineRef.current) {
      const candle = fullCandleDataRef.current[replayIndexRef.current];
      if (candle) {
        const replayX = timeScale.timeToCoordinate(candle.time);
        setReplayCursorX(replayX === null ? null : Number(replayX));
      }
    } else {
      setReplayCursorX(null);
    }
  };

  const setReplayWindow = (index: number) => {
    const chart = chartRef.current;
    const candleData = fullCandleDataRef.current;
    if (!chart || candleData.length === 0) {
      return;
    }

    const boundedIndex = Math.min(Math.max(index, 0), candleData.length - 1);
    const interval = intervalRef.current || inferInterval(candleData);
    const fromIndex = Math.max(0, boundedIndex - 80);
    const from = candleData[fromIndex]?.time ?? candleData[0].time;
    const to = (candleData[boundedIndex].time + interval * 8) as UTCTimestamp;
    chart.timeScale().setVisibleRange({ from, to });
    setTimeout(updateOverlayPositions, 0);
  };

  const applyVisibleData = (preserveRange: boolean) => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const emaSeries = emaSeriesRef.current;
    if (!chart || !candleSeries || !emaSeries) {
      return;
    }

    const allCandles = fullCandleDataRef.current;
    const allEma = fullEmaDataRef.current;
    const allMarkers = fullMarkerDataRef.current;
    const currentRange = preserveRange ? chart.timeScale().getVisibleRange() : null;

    let visibleCandles = allCandles;
    let visibleEma = allEma;
    let visibleMarkers = allMarkers;

    if (replayActiveRef.current && replayIndexRef.current !== null && allCandles.length > 0) {
      const replayIndex = Math.min(replayIndexRef.current, allCandles.length - 1);
      const cutoffTime = allCandles[replayIndex].time;
      visibleCandles = allCandles.slice(0, replayIndex + 1);
      visibleEma = allEma.filter((item) => item.time <= cutoffTime);
      visibleMarkers = allMarkers.filter((item) => item.time <= cutoffTime);
    }

    candleSeries.setData(visibleCandles);
    emaSeries.setData(visibleEma);
    candleSeries.setMarkers(visibleMarkers);

    sessionBreakTimesRef.current = computeSessionBreakTimes(visibleCandles);

    if (!replayActiveRef.current) {
      if (!hasFitRef.current && visibleCandles.length > 0) {
        chart.timeScale().fitContent();
        hasFitRef.current = true;
      } else if (currentRange) {
        chart.timeScale().setVisibleRange(currentRange);
      }
    } else if (replayIndexRef.current !== null) {
      setReplayWindow(replayIndexRef.current);
    }

    if (!replayActiveRef.current) {
      setReplayCursorX(null);
    }

    updateOverlayPositions();
    emitZoomStateIfChanged();
    Streamlit.setFrameHeight(height);
  };

  const closeModal = () => {
    setModalMode(null);
  };

  const openDateModal = (mode: Exclude<ModalMode, null>) => {
    const lastTs = lastCandleTsRef.current;
    if (lastTs) {
      const d = new Date(lastTs * 1000);
      const pad = (value: number) => String(value).padStart(2, "0");
      setDateValue(`${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`);
    }
    setModalMode(mode);
  };

  const resetReplay = () => {
    replayIndexRef.current = null;
    replayActiveRef.current = false;
    showReplayStartLineRef.current = false;
    setReplayActive(false);
    setShowReplayStartLine(false);
    applyVisibleData(false);
    emitComponentEvent({ eventType: "replay_state" });
  };

  const resetCurrentView = () => {
    const chart = chartRef.current;
    if (!chart) {
      return;
    }

    if (replayActiveRef.current && replayIndexRef.current !== null) {
      setReplayWindow(replayIndexRef.current);
      return;
    }

    chart.timeScale().fitContent();
    setTimeout(updateOverlayPositions, 0);
  };

  const startReplayFromDate = (selectedDate: string) => {
    const allCandles = fullCandleDataRef.current;
    if (allCandles.length === 0) {
      closeModal();
      return;
    }

    const targetSessionStart = sessionStartTimestamp(selectedDate);
    const selectedDay = selectedDate;

    let index = allCandles.findIndex((item) => dayKey(item.time) === selectedDay);
    if (index < 0 && targetSessionStart !== null) {
      index = allCandles.findIndex((item) => item.time >= targetSessionStart);
    }
    if (index < 0) {
      index = allCandles.length - 1;
    }

    replayIndexRef.current = index;
    replayActiveRef.current = true;
    showReplayStartLineRef.current = true;
    setReplayActive(true);
    setShowReplayStartLine(true);
    applyVisibleData(false);
    emitComponentEvent({ eventType: "replay_state" });
    closeModal();
  };

  const goToDate = (selectedDate: string) => {
    const ts = sessionStartTimestamp(selectedDate);
    const chart = chartRef.current;
    if (!chart || ts === null) {
      closeModal();
      return;
    }

    const timeScale = chart.timeScale();
    const range = timeScale.getVisibleRange();
    let span = 6 * 60 * 60;
    if (range) {
      const rangeFrom = toTimestampAny(range.from);
      const rangeTo = toTimestampAny(range.to);
      if (rangeFrom !== null && rangeTo !== null) {
        span = Math.max(60, rangeTo - rangeFrom);
      }
    }
    timeScale.setVisibleRange({
      from: (ts - span / 2) as UTCTimestamp,
      to: (ts + span / 2) as UTCTimestamp,
    });
    closeModal();
  };

  const stepReplayForward = () => {
    if (!replayActiveRef.current) {
      return;
    }
    const allCandles = fullCandleDataRef.current;
    if (allCandles.length === 0 || replayIndexRef.current === null) {
      return;
    }
    if (replayIndexRef.current >= allCandles.length - 1) {
      return;
    }
    showReplayStartLineRef.current = false;
    setShowReplayStartLine(false);
    replayIndexRef.current += 1;
    applyVisibleData(false);
    emitComponentEvent({ eventType: "replay_state" });
  };

  useEffect(() => {
    if (!isReadyRef.current) {
      Streamlit.setComponentReady();
      isReadyRef.current = true;
    }
  }, []);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) {
      return;
    }

    if (chartRef.current) {
      chartRef.current.applyOptions({ height });
      Streamlit.setFrameHeight(height);
      updateOverlayPositions();
      return;
    }

    const chart = createChart(container, {
      width: container.clientWidth || 800,
      height,
      layout: {
        background: { color: "white" },
        textColor: "#475569",
        fontFamily: "Segoe UI, sans-serif",
        fontSize: 12,
      },
      localization: {
        locale: "en-IN",
        timeFormatter: (time: Time) => formatIstTime(toTimestampAny(time), true),
      },
      grid: {
        vertLines: { visible: false, color: "rgba(0, 0, 0, 0)" },
        horzLines: { visible: false, color: "rgba(0, 0, 0, 0)" },
      },
      crosshair: {
        vertLine: {
          visible: true,
          width: 1,
          style: LineStyle.Dashed,
          color: "rgba(15, 23, 42, 0.45)",
          labelVisible: true,
          labelBackgroundColor: "#111827",
        },
        horzLine: {
          visible: true,
          width: 1,
          style: LineStyle.Dashed,
          color: "rgba(15, 23, 42, 0.45)",
          labelVisible: true,
          labelBackgroundColor: "#111827",
        },
      },
      rightPriceScale: {
        borderColor: "rgba(203, 213, 225, 0.9)",
        scaleMargins: { top: 0.1, bottom: 0.1 },
      },
      timeScale: {
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 8,
        barSpacing: 8,
        minBarSpacing: 3,
        lockVisibleTimeRangeOnResize: true,
        borderVisible: true,
        borderColor: "rgba(203, 213, 225, 0.9)",
        tickMarkFormatter: (time: Time, _tick: TickMarkType, _locale: string) =>
          formatIstTime(toTimestampAny(time), false),
      },
    });

    const candleSeries = chart.addCandlestickSeries({
      upColor: "#089981",
      downColor: "#f23645",
      borderVisible: false,
      wickUpColor: "#089981",
      wickDownColor: "#f23645",
      priceLineVisible: false,
      lastValueVisible: true,
    });

    const emaSeries = chart.addLineSeries({
      color: "#2962ff",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: true,
      crosshairMarkerVisible: false,
    });

    chart.subscribeClick((param) => {
      const clicked = toTimestampAny(param.time as Time | undefined);
      if (clicked === null) {
        return;
      }

      if (
        pendingClickTimerRef.current !== null &&
        pendingClickEpochRef.current === clicked
      ) {
        window.clearTimeout(pendingClickTimerRef.current);
        pendingClickTimerRef.current = null;
        pendingClickEpochRef.current = null;
        emitComponentEvent({ eventType: "chart_double_click", epoch: clicked });
        return;
      }

      if (
        pendingClickTimerRef.current !== null &&
        pendingClickEpochRef.current !== null
      ) {
        window.clearTimeout(pendingClickTimerRef.current);
        emitComponentEvent({
          eventType: "chart_click",
          epoch: pendingClickEpochRef.current,
        });
      }

      pendingClickEpochRef.current = clicked;
      pendingClickTimerRef.current = window.setTimeout(() => {
        emitComponentEvent({ eventType: "chart_click", epoch: clicked });
        pendingClickTimerRef.current = null;
        pendingClickEpochRef.current = null;
      }, 250);
    });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    emaSeriesRef.current = emaSeries;
    resetViewRef.current = resetCurrentView;

    const handleResize = () => {
      if (!containerRef.current || !chartRef.current) {
        return;
      }
      chartRef.current.applyOptions({ width: containerRef.current.clientWidth || 800 });
      updateOverlayPositions();
    };
    window.addEventListener("resize", handleResize);

    const handleKeyDown = (event: KeyboardEvent) => {
      const key = event.key.toLowerCase();
      if (event.altKey && key === "g") {
        event.preventDefault();
        openDateModal("goto");
        return;
      }
      if (event.altKey && key === "r") {
        event.preventDefault();
        resetViewRef.current?.();
        return;
      }
      if (event.shiftKey && event.key === "ArrowRight") {
        event.preventDefault();
        stepReplayForward();
      }
    };
    window.addEventListener("keydown", handleKeyDown);
    document.addEventListener("keydown", handleKeyDown);

    const handleFocusClick = () => {
      container.focus();
    };
    container.setAttribute("tabindex", "0");
    container.addEventListener("click", handleFocusClick);
    const handleVisibleRangeChange = () => {
      updateOverlayPositions();
      emitZoomStateIfChanged();
    };
    chart.timeScale().subscribeVisibleTimeRangeChange(handleVisibleRangeChange);

    Streamlit.setFrameHeight(height);

    return () => {
      window.removeEventListener("resize", handleResize);
      window.removeEventListener("keydown", handleKeyDown);
      document.removeEventListener("keydown", handleKeyDown);
      container.removeEventListener("click", handleFocusClick);
      if (pendingClickTimerRef.current !== null) {
        window.clearTimeout(pendingClickTimerRef.current);
        pendingClickTimerRef.current = null;
        pendingClickEpochRef.current = null;
      }
      chart.timeScale().unsubscribeVisibleTimeRangeChange(handleVisibleRangeChange);
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      emaSeriesRef.current = null;
      resetViewRef.current = null;
      sessionBreakTimesRef.current = [];
      setSessionBreakXs([]);
      setReplayCursorX(null);
    };
  }, [height]);

  useEffect(() => {
    fullCandleDataRef.current = normalizeCandles(candles);
    fullEmaDataRef.current = normalizeEma(ema);
    fullMarkerDataRef.current = normalizeMarkers(markers);
    intervalRef.current = inferInterval(fullCandleDataRef.current);

    if (fullCandleDataRef.current.length > 0) {
      lastCandleTsRef.current =
        fullCandleDataRef.current[fullCandleDataRef.current.length - 1].time;
    } else {
      lastCandleTsRef.current = null;
    }

    if (
      replayActiveRef.current &&
      replayIndexRef.current !== null &&
      replayIndexRef.current >= fullCandleDataRef.current.length
    ) {
      replayIndexRef.current = Math.max(0, fullCandleDataRef.current.length - 1);
    }

    applyVisibleData(true);
  }, [candles, ema, markers, height]);

  useEffect(() => {
    const nextReplayState = normalizeReplayState(replayState);
    const maxIndex =
      fullCandleDataRef.current.length > 0 ? fullCandleDataRef.current.length - 1 : null;
    const nextIndex =
      nextReplayState.active && nextReplayState.index !== null && maxIndex !== null
        ? Math.min(nextReplayState.index, maxIndex)
        : nextReplayState.active
          ? nextReplayState.index
          : null;

    const changed =
      replayActiveRef.current !== nextReplayState.active ||
      replayIndexRef.current !== nextIndex ||
      showReplayStartLineRef.current !== nextReplayState.showStartLine;

    if (!changed) {
      return;
    }

    replayActiveRef.current = nextReplayState.active;
    replayIndexRef.current = nextIndex;
    showReplayStartLineRef.current = nextReplayState.showStartLine;
    setReplayActive(nextReplayState.active);
    setShowReplayStartLine(nextReplayState.showStartLine);
    applyVisibleData(true);
  }, [replayState]);

  const handleDateSubmit = () => {
    if (!dateValue) {
      closeModal();
      return;
    }
    if (modalMode === "goto") {
      goToDate(dateValue);
      return;
    }
    if (modalMode === "replay") {
      startReplayFromDate(dateValue);
      return;
    }
  };

  const handleReplayButtonClick = () => {
    if (replayActive) {
      setModalMode("leaveReplay");
      return;
    }
    openDateModal("replay");
  };

  return (
    <div style={{ position: "relative", width: "100%", height }}>
      <div
        style={{
          position: "absolute",
          top: 10,
          left: 10,
          zIndex: 6,
          display: "flex",
          gap: "8px",
          alignItems: "center",
        }}
      >
        <button
          onClick={handleReplayButtonClick}
          style={{
            display: "inline-flex",
            alignItems: "center",
            gap: "6px",
            padding: "6px 10px",
            borderRadius: "8px",
            border: "1px solid #cbd5e1",
            background: "rgba(255,255,255,0.95)",
            color: "#0f172a",
            fontWeight: 600,
            cursor: "pointer",
          }}
        >
          <span>{replayActive ? "x" : "<<"}</span>
          <span>{replayActive ? "Exit Replay" : "Replay"}</span>
        </button>
        {replayActive && (
          <div
            style={{
              padding: "6px 10px",
              borderRadius: "8px",
              background: "rgba(37, 99, 235, 0.12)",
              color: "#1d4ed8",
              fontSize: "12px",
              fontWeight: 700,
            }}
          >
            Shift + Right Arrow
          </div>
        )}
      </div>

      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />

      {sessionBreakXs.map((x, index) => (
        <div
          key={`session-break-${index}-${x}`}
          style={{
            position: "absolute",
            top: 0,
            bottom: 0,
            left: `${x}px`,
            borderLeft: "1px dashed rgba(15, 23, 42, 0.55)",
            pointerEvents: "none",
            zIndex: 2,
          }}
        />
      ))}

      {replayCursorX !== null && (
        <div
          style={{
            position: "absolute",
            top: 0,
            bottom: 0,
            left: `${replayCursorX}px`,
            borderLeft: "2px solid #2563eb",
            pointerEvents: "none",
            zIndex: 3,
          }}
        />
      )}

      {modalMode && (
        <div
          style={{
            position: "absolute",
            inset: 0,
            background: "rgba(15, 23, 42, 0.35)",
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            zIndex: 8,
          }}
        >
          <div
            style={{
              background: "white",
              padding: "16px",
              borderRadius: "10px",
              boxShadow: "0 10px 30px rgba(0,0,0,0.2)",
              minWidth: "280px",
            }}
          >
            {modalMode === "leaveReplay" ? (
              <>
                <div style={{ fontWeight: 700, marginBottom: "8px", fontSize: "22px" }}>
                  Leave current replay?
                </div>
                <div style={{ color: "#475569", marginBottom: "16px", lineHeight: 1.5 }}>
                  This will exit replay mode and restore the full chart.
                </div>
                <div style={{ display: "flex", gap: "8px", justifyContent: "flex-end" }}>
                  <button
                    onClick={closeModal}
                    style={{
                      padding: "8px 16px",
                      borderRadius: "6px",
                      border: "1px solid #cbd5e1",
                      background: "white",
                      color: "#0f172a",
                      fontWeight: 700,
                      cursor: "pointer",
                    }}
                  >
                    Stay
                  </button>
                  <button
                    onClick={() => {
                      closeModal();
                      resetReplay();
                    }}
                    style={{
                      padding: "8px 16px",
                      borderRadius: "6px",
                      border: "none",
                      background: "#111827",
                      color: "white",
                      fontWeight: 700,
                      cursor: "pointer",
                    }}
                  >
                    Leave
                  </button>
                </div>
              </>
            ) : (
              <>
                <div style={{ fontWeight: 700, marginBottom: "8px" }}>
                  {modalMode === "goto" ? "Go to Date" : "Replay From Date"}
                </div>
                <input
                  type="date"
                  value={dateValue}
                  onChange={(event) => setDateValue(event.target.value)}
                  style={{
                    width: "100%",
                    padding: "8px",
                    borderRadius: "6px",
                    border: "1px solid #cbd5e1",
                  }}
                />
                <div style={{ display: "flex", gap: "8px", marginTop: "12px" }}>
                  <button
                    onClick={handleDateSubmit}
                    style={{
                      flex: 1,
                      padding: "8px",
                      borderRadius: "6px",
                      border: "none",
                      background: "#2563eb",
                      color: "white",
                      fontWeight: 700,
                      cursor: "pointer",
                    }}
                  >
                    {modalMode === "goto" ? "Go" : "Start Replay"}
                  </button>
                  <button
                    onClick={closeModal}
                    style={{
                      flex: 1,
                      padding: "8px",
                      borderRadius: "6px",
                      border: "1px solid #cbd5e1",
                      background: "white",
                      color: "#0f172a",
                      fontWeight: 700,
                      cursor: "pointer",
                    }}
                  >
                    Cancel
                  </button>
                </div>
              </>
            )}
          </div>
        </div>
      )}
    </div>
  );
};

export default Chart;
