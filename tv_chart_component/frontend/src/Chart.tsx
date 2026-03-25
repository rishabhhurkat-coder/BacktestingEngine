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
  WhitespaceData,
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

export type IndicatorPoint = {
  time: string | number;
  value?: number | null;
};

export type IndicatorSeries = {
  id: string;
  name: string;
  column?: string;
  color?: string;
  lineWidth?: number;
  data: IndicatorPoint[];
};

export type Marker = {
  time: string | number;
  position: "aboveBar" | "belowBar" | "inBar";
  shape: "arrowUp" | "arrowDown" | "circle" | "square";
  color: string;
  text?: string;
};

type ChartProps = {
  candles: Candle[];
  indicators?: IndicatorSeries[];
  markers?: Marker[];
  chartType?: "Candlestick" | "Line Chart";
  height?: number;
};

type ModalMode = "goto" | null;

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
  const parsed = Date.parse(`${dateValue}T00:00:00+05:30`);
  if (Number.isNaN(parsed)) {
    return null;
  }
  return Math.floor(parsed / 1000) as UTCTimestamp;
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

const normalizeLineSeries = (
  points: IndicatorPoint[]
): Array<LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>> =>
  points
    .map((item) => {
      const ts = toTimestamp(item.time);
      if (ts === null) {
        return null;
      }
      if (item.value === null || item.value === undefined || Number.isNaN(Number(item.value))) {
        return {
          time: ts,
        };
      }
      return {
        time: ts,
        value: Number(item.value),
      };
    })
    .filter((item): item is LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp> => item !== null);

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

const hasLineValue = (
  point: LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>
): point is LineData<UTCTimestamp> => "value" in point && typeof point.value === "number";

const hexToRgba = (color: string, alpha: number): string => {
  const value = String(color || "").trim();
  const normalized = value.startsWith("#") ? value.slice(1) : value;
  if (/^[0-9a-f]{6}$/i.test(normalized)) {
    const r = Number.parseInt(normalized.slice(0, 2), 16);
    const g = Number.parseInt(normalized.slice(2, 4), 16);
    const b = Number.parseInt(normalized.slice(4, 6), 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }
  if (/^[0-9a-f]{3}$/i.test(normalized)) {
    const r = Number.parseInt(normalized[0] + normalized[0], 16);
    const g = Number.parseInt(normalized[1] + normalized[1], 16);
    const b = Number.parseInt(normalized[2] + normalized[2], 16);
    return `rgba(${r}, ${g}, ${b}, ${alpha})`;
  }
  return value || `rgba(41, 98, 255, ${alpha})`;
};

const formatOhlcValue = (value: number): string => value.toFixed(2);

const buildCloseLineData = (
  candleData: CandlestickData<UTCTimestamp>[]
): LineData<UTCTimestamp>[] =>
  candleData.map((item) => ({
    time: item.time,
    value: item.close,
  }));

const Chart = ({
  candles,
  indicators = [],
  markers = [],
  chartType = "Candlestick",
  height = 600,
}: ChartProps): React.ReactElement => {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const overlayCanvasRef = useRef<HTMLCanvasElement | null>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const closeSeriesRef = useRef<ISeriesApi<"Line"> | null>(null);
  const indicatorSeriesRefs = useRef<Map<string, ISeriesApi<"Line">>>(new Map());
  const lastCandleTsRef = useRef<UTCTimestamp | null>(null);
  const fullCandleDataRef = useRef<CandlestickData<UTCTimestamp>[]>([]);
  const fullCloseLineDataRef = useRef<LineData<UTCTimestamp>[]>([]);
  const fullIndicatorDataRef = useRef<
    Array<{
      id: string;
      name: string;
      column?: string;
      color: string;
      lineWidth: number;
      data: Array<LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>>;
    }>
  >([]);
  const fullMarkerDataRef = useRef<SeriesMarker<UTCTimestamp>[]>([]);
  const candleIndexByTimeRef = useRef<Map<number, number>>(new Map());
  const intervalRef = useRef<number>(180);
  const sessionBreakTimesRef = useRef<UTCTimestamp[]>([]);
  const resetViewRef = useRef<(() => void) | null>(null);
  const hasFitRef = useRef(false);
  const isReadyRef = useRef(false);
  const zoomedStateRef = useRef(false);
  const chartTypeRef = useRef<"Candlestick" | "Line Chart">("Candlestick");
  const pendingClickTimerRef = useRef<number | null>(null);
  const pendingClickEpochRef = useRef<UTCTimestamp | null>(null);

  const [modalMode, setModalMode] = useState<ModalMode>(null);
  const [dateValue, setDateValue] = useState("");
  const [sessionBreakXs, setSessionBreakXs] = useState<number[]>([]);
  const [hoveredCandle, setHoveredCandle] = useState<CandlestickData<UTCTimestamp> | null>(null);
  const [visibleLastCandle, setVisibleLastCandle] = useState<CandlestickData<UTCTimestamp> | null>(null);

  const clearPendingClick = () => {
    if (pendingClickTimerRef.current !== null) {
      window.clearTimeout(pendingClickTimerRef.current);
      pendingClickTimerRef.current = null;
    }
    pendingClickEpochRef.current = null;
  };

  const applyMainSeriesMode = () => {
    const candleSeries = candleSeriesRef.current;
    const closeSeries = closeSeriesRef.current;
    if (!candleSeries || !closeSeries) {
      return;
    }
    const isLineChart = chartTypeRef.current === "Line Chart";
    candleSeries.applyOptions({
      upColor: isLineChart ? "rgba(8, 153, 129, 0)" : "#089981",
      downColor: isLineChart ? "rgba(242, 54, 69, 0)" : "#f23645",
      borderVisible: false,
      wickUpColor: isLineChart ? "rgba(8, 153, 129, 0)" : "#089981",
      wickDownColor: isLineChart ? "rgba(242, 54, 69, 0)" : "#f23645",
      lastValueVisible: !isLineChart,
      priceLineVisible: false,
    });
    closeSeries.applyOptions({
      visible: isLineChart,
      color: "#0f172a",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: isLineChart,
      crosshairMarkerVisible: false,
    });
  };

  const getIsZoomed = (): boolean => {
    const chart = chartRef.current;
    const candleData = fullCandleDataRef.current;
    if (!chart || candleData.length < 2) {
      return false;
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

  const drawSuperTrendFill = () => {
    const canvas = overlayCanvasRef.current;
    const container = containerRef.current;
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    if (!canvas || !container || !chart || !candleSeries) {
      return;
    }

    const width = Math.max(1, Math.round(container.clientWidth || 0));
    const canvasHeight = Math.max(1, Math.round(height));
    const dpr = window.devicePixelRatio || 1;
    const pixelWidth = Math.max(1, Math.round(width * dpr));
    const pixelHeight = Math.max(1, Math.round(canvasHeight * dpr));

    if (canvas.width !== pixelWidth || canvas.height !== pixelHeight) {
      canvas.width = pixelWidth;
      canvas.height = pixelHeight;
      canvas.style.width = `${width}px`;
      canvas.style.height = `${canvasHeight}px`;
    }

    const ctx = canvas.getContext("2d");
    if (!ctx) {
      return;
    }

    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, canvasHeight);

    const allIndicators = fullIndicatorDataRef.current;
    const bullSeriesList = allIndicators.filter(
      (item) => item.column === "Super Trend Bull" || item.id.includes(":bull")
    );
    const bearSeriesList = allIndicators.filter(
      (item) => item.column === "Super Trend Bear" || item.id.includes(":bear")
    );
    if (bullSeriesList.length === 0 && bearSeriesList.length === 0) {
      return;
    }

    const buildSegments = (
      seriesList: Array<{
        data: Array<LineData<UTCTimestamp> | WhitespaceData<UTCTimestamp>>;
        color: string;
      }>
    ): Array<{ color: string; points: Array<{ x: number; trendY: number; closeY: number }> }> =>
      seriesList
        .map((series) => {
          const points = series.data.reduce<Array<{ x: number; trendY: number; closeY: number }>>(
            (accumulator, point) => {
              if (!hasLineValue(point)) {
                return accumulator;
              }
              const time = Number(point.time);
              const candleIndex = candleIndexByTimeRef.current.get(time);
              if (candleIndex === undefined) {
                return accumulator;
              }
              const candle = fullCandleDataRef.current[candleIndex];
              const x = chart.timeScale().timeToCoordinate(candle.time);
              const trendY = candleSeries.priceToCoordinate(Number(point.value));
              const closeY = candleSeries.priceToCoordinate(candle.close);
              if (x === null || trendY === null || closeY === null) {
                return accumulator;
              }
              accumulator.push({
                x: Number(x),
                trendY: Number(trendY),
                closeY: Number(closeY),
              });
              return accumulator;
            },
            []
          );
          return { color: series.color, points };
        })
        .filter((item) => item.points.length > 1);

    const drawSegments = (
      segments: Array<{ color: string; points: Array<{ x: number; trendY: number; closeY: number }> }>
    ) => {
      for (const segment of segments) {
        const yValues = segment.points.flatMap((point) => [point.trendY, point.closeY]);
        const minY = Math.min(...yValues);
        const maxY = Math.max(...yValues);
        const gradient = ctx.createLinearGradient(0, minY, 0, maxY);
        gradient.addColorStop(0, hexToRgba(segment.color, 0.02));
        gradient.addColorStop(0.5, hexToRgba(segment.color, 0.12));
        gradient.addColorStop(1, hexToRgba(segment.color, 0.24));

        ctx.beginPath();
        ctx.moveTo(segment.points[0].x, segment.points[0].trendY);
        for (let i = 1; i < segment.points.length; i += 1) {
          ctx.lineTo(segment.points[i].x, segment.points[i].trendY);
        }
        for (let i = segment.points.length - 1; i >= 0; i -= 1) {
          ctx.lineTo(segment.points[i].x, segment.points[i].closeY);
        }
        ctx.closePath();
        ctx.fillStyle = gradient;
        ctx.fill();
      }
    };

    drawSegments(buildSegments(bullSeriesList));
    drawSegments(buildSegments(bearSeriesList));
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
    drawSuperTrendFill();
  };

  const applyVisibleData = (preserveRange: boolean) => {
    const chart = chartRef.current;
    const candleSeries = candleSeriesRef.current;
    const closeSeries = closeSeriesRef.current;
    if (!chart || !candleSeries || !closeSeries) {
      return;
    }

    const allCandles = fullCandleDataRef.current;
    const closeLineData = fullCloseLineDataRef.current;
    const allIndicators = fullIndicatorDataRef.current;
    const allMarkers = fullMarkerDataRef.current;
    const currentRange = preserveRange ? chart.timeScale().getVisibleRange() : null;
    candleSeries.setData(allCandles);
    closeSeries.setData(closeLineData);
    applyMainSeriesMode();
    const indicatorSeriesMap = indicatorSeriesRefs.current;
    const nextIndicatorIds = new Set(allIndicators.map((item) => item.id));
    for (const [seriesId, series] of Array.from(indicatorSeriesMap.entries())) {
      if (!nextIndicatorIds.has(seriesId)) {
        chart.removeSeries(series);
        indicatorSeriesMap.delete(seriesId);
      }
    }
    for (const indicator of allIndicators) {
      const safeLineWidth = Math.max(1, Math.min(4, Math.round(indicator.lineWidth))) as 1 | 2 | 3 | 4;
      let series = indicatorSeriesMap.get(indicator.id);
      if (!series) {
        series = chart.addLineSeries({
          color: indicator.color,
          lineWidth: safeLineWidth,
          priceLineVisible: false,
          lastValueVisible: true,
          crosshairMarkerVisible: false,
        });
        indicatorSeriesMap.set(indicator.id, series);
      } else {
        series.applyOptions({
          color: indicator.color,
          lineWidth: safeLineWidth,
        });
      }
      series.setData(indicator.data);
    }
    if (chartTypeRef.current === "Line Chart") {
      candleSeries.setMarkers([]);
      closeSeries.setMarkers(allMarkers);
    } else {
      candleSeries.setMarkers(allMarkers);
      closeSeries.setMarkers([]);
    }

    sessionBreakTimesRef.current = computeSessionBreakTimes(allCandles);

    if (!hasFitRef.current && allCandles.length > 0) {
      chart.timeScale().fitContent();
      hasFitRef.current = true;
    } else if (currentRange) {
      chart.timeScale().setVisibleRange(currentRange);
    }

    setVisibleLastCandle(allCandles.length > 0 ? allCandles[allCandles.length - 1] : null);
    updateOverlayPositions();
    emitZoomStateIfChanged();
    Streamlit.setFrameHeight(height);
  };

  const closeModal = () => {
    setModalMode(null);
  };

  const openDateModal = () => {
    const lastTs = lastCandleTsRef.current;
    if (lastTs) {
      const d = new Date(lastTs * 1000);
      const pad = (value: number) => String(value).padStart(2, "0");
      setDateValue(`${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`);
    }
    setModalMode("goto");
  };

  const resetCurrentView = () => {
    const chart = chartRef.current;
    if (!chart) {
      return;
    }

    chart.timeScale().fitContent();
    setTimeout(updateOverlayPositions, 0);
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
    clearPendingClick();
    emitComponentEvent({ eventType: "view_reset" });
    closeModal();
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
    const closeSeries = chart.addLineSeries({
      visible: false,
      color: "#0f172a",
      lineWidth: 2,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: false,
    });
    applyMainSeriesMode();

    chart.subscribeCrosshairMove((param) => {
      const candleSeries = candleSeriesRef.current;
      if (!candleSeries || !param.point || !param.time) {
        setHoveredCandle(null);
        return;
      }

      const candlePoint = param.seriesData?.get?.(candleSeries) as
        | CandlestickData<UTCTimestamp>
        | undefined;
      if (candlePoint) {
        setHoveredCandle(candlePoint);
        return;
      }
      const hoveredTs = toTimestampAny(param.time);
      if (hoveredTs === null) {
        setHoveredCandle(null);
        return;
      }
      const candleIndex = candleIndexByTimeRef.current.get(Number(hoveredTs));
      setHoveredCandle(
        candleIndex === undefined ? null : fullCandleDataRef.current[candleIndex] ?? null
      );
    });

    chart.subscribeClick((param) => {
      const candleSeries = candleSeriesRef.current;
      const point = param.point;
      const clicked =
        toTimestampAny(param.time as Time | undefined) ??
        toTimestampAny(chart.timeScale().coordinateToTime(point?.x ?? 0));
      if (!candleSeries || !point || clicked === null) {
        return;
      }

      let candlePoint = param.seriesData?.get?.(candleSeries) as
        | CandlestickData<UTCTimestamp>
        | undefined;
      if (!candlePoint) {
        const candleIndex = candleIndexByTimeRef.current.get(Number(clicked));
        candlePoint =
          candleIndex === undefined ? undefined : fullCandleDataRef.current[candleIndex];
      }
      if (!candlePoint) {
        return;
      }

      const candleX = chart.timeScale().timeToCoordinate(candlePoint.time);
      const openY = candleSeries.priceToCoordinate(candlePoint.open);
      const closeY = candleSeries.priceToCoordinate(candlePoint.close);
      const highY = candleSeries.priceToCoordinate(candlePoint.high);
      const lowY = candleSeries.priceToCoordinate(candlePoint.low);
      if (clicked === null) {
        return;
      }
      if (
        candleX === null ||
        openY === null ||
        closeY === null ||
        highY === null ||
        lowY === null
      ) {
        return;
      }

      const withinX = Math.abs(point.x - candleX) <= 9;
      const wickTop = Math.min(highY, lowY) - 3;
      const wickBottom = Math.max(highY, lowY) + 3;
      const withinY = point.y >= wickTop && point.y <= wickBottom;
      if (!withinX || !withinY) {
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
    closeSeriesRef.current = closeSeries;
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
        openDateModal();
        return;
      }
      if (event.altKey && key === "r") {
        event.preventDefault();
        resetViewRef.current?.();
        return;
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
      clearPendingClick();
      indicatorSeriesRefs.current.forEach((series) => {
        chart.removeSeries(series);
      });
      indicatorSeriesRefs.current.clear();
      chart.timeScale().unsubscribeVisibleTimeRangeChange(handleVisibleRangeChange);
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      closeSeriesRef.current = null;
      resetViewRef.current = null;
      sessionBreakTimesRef.current = [];
      setSessionBreakXs([]);
      setHoveredCandle(null);
      setVisibleLastCandle(null);
    };
  }, [height]);

  useEffect(() => {
    fullCandleDataRef.current = normalizeCandles(candles);
    fullCloseLineDataRef.current = buildCloseLineData(fullCandleDataRef.current);
    fullIndicatorDataRef.current = indicators.map((indicator) => ({
      id: indicator.id,
      name: indicator.name,
      column: indicator.column,
      color: indicator.color ?? "#2962ff",
      lineWidth: indicator.lineWidth ?? 2,
      data: normalizeLineSeries(indicator.data ?? []),
    }));
    fullMarkerDataRef.current = normalizeMarkers(markers);
    chartTypeRef.current = chartType;
    candleIndexByTimeRef.current = new Map(
      fullCandleDataRef.current.map((item, index) => [Number(item.time), index])
    );
    intervalRef.current = inferInterval(fullCandleDataRef.current);

    if (fullCandleDataRef.current.length > 0) {
      lastCandleTsRef.current =
        fullCandleDataRef.current[fullCandleDataRef.current.length - 1].time;
    } else {
      lastCandleTsRef.current = null;
    }

    setHoveredCandle(null);
    applyVisibleData(true);
  }, [candles, indicators, markers, chartType, height]);

  const handleDateSubmit = () => {
    if (!dateValue) {
      closeModal();
      return;
    }
    goToDate(dateValue);
  };

  const displayCandle = hoveredCandle ?? visibleLastCandle;
  const ohlcColor =
    displayCandle && displayCandle.close >= displayCandle.open ? "#089981" : "#f23645";
  const previousClose =
    displayCandle !== null
      ? (() => {
          const candleIndex = candleIndexByTimeRef.current.get(Number(displayCandle.time));
          if (candleIndex === undefined || candleIndex <= 0) {
            return null;
          }
          return fullCandleDataRef.current[candleIndex - 1]?.close ?? null;
        })()
      : null;
  const changeValue =
    displayCandle !== null && previousClose !== null ? displayCandle.close - previousClose : null;
  const percentChange =
    changeValue !== null && previousClose !== null && previousClose !== 0
      ? (changeValue / previousClose) * 100
      : null;
  const changePrefix = changeValue !== null && changeValue > 0 ? "+" : "";
  return (
    <div style={{ position: "relative", width: "100%", height }}>
      {displayCandle && (
        <div
          style={{
            position: "absolute",
            top: 10,
            left: 12,
            zIndex: 6,
            display: "flex",
            gap: "10px",
            alignItems: "center",
            color: ohlcColor,
            fontSize: "17.5px",
            fontWeight: 700,
            fontFamily: "Consolas, 'Courier New', monospace",
            letterSpacing: "0.01em",
            background: "rgba(255,255,255,0.72)",
            padding: "4px 8px",
            borderRadius: "6px",
            pointerEvents: "none",
            flexWrap: "wrap",
          }}
        >
          <span>O {formatOhlcValue(displayCandle.open)}</span>
          <span>H {formatOhlcValue(displayCandle.high)}</span>
          <span>L {formatOhlcValue(displayCandle.low)}</span>
          <span>C {formatOhlcValue(displayCandle.close)}</span>
          {changeValue !== null && percentChange !== null && (
            <span>
              {changePrefix}
              {formatOhlcValue(changeValue)} ({changePrefix}
              {percentChange.toFixed(2)}%)
            </span>
          )}
        </div>
      )}

      <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
      <canvas
        ref={overlayCanvasRef}
        style={{
          position: "absolute",
          inset: 0,
          width: "100%",
          height: "100%",
          pointerEvents: "none",
          zIndex: 3,
        }}
      />

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
            <div style={{ fontWeight: 700, marginBottom: "8px" }}>Go to Date</div>
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
                Go
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
          </div>
        </div>
      )}
    </div>
  );
};

export default Chart;
