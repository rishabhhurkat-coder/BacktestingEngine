import React from "react";
import ReactDOM from "react-dom/client";
import { withStreamlitConnection } from "streamlit-component-lib";
import Chart, { Candle, EmaPoint, Marker } from "./Chart";

type StreamlitArgs = {
  candles?: Candle[];
  ema?: EmaPoint[];
  markers?: Marker[];
  height?: number;
  replayState?: {
    active?: boolean;
    index?: number | null;
    showStartLine?: boolean;
  } | null;
};

const App = (props: any): React.ReactElement => {
  const args = (props?.args ?? {}) as StreamlitArgs;
  const candles = args.candles ?? [];
  const ema = args.ema ?? [];
  const markers = args.markers ?? [];
  const height = args.height ?? 600;
  const replayState = args.replayState ?? null;

  return (
    <Chart
      candles={candles}
      ema={ema}
      markers={markers}
      height={height}
      replayState={replayState}
    />
  );
};

const ConnectedApp = withStreamlitConnection(App);

const root = ReactDOM.createRoot(document.getElementById("root") as HTMLElement);
root.render(<ConnectedApp />);
