import React, { useState, useEffect } from "react";
import {
  TimeseriesChart,
  ChartPalette,
} from "@cloudflare/kumo/components/chart";
import * as echarts from "echarts/core";
import { LineChart, BarChart } from "echarts/charts";
import {
  GridComponent,
  TooltipComponent,
  BrushComponent,
  ToolboxComponent,
  AriaComponent,
} from "echarts/components";
import { CanvasRenderer } from "echarts/renderers";
echarts.use([
  LineChart,
  BarChart,
  GridComponent,
  TooltipComponent,
  BrushComponent,
  ToolboxComponent,
  AriaComponent,
  CanvasRenderer,
]);
function useDarkMode() {
  const [dark, setDark] = useState(
    document.documentElement.dataset.mode === "dark",
  );
  useEffect(() => {
    const observer = new MutationObserver(() =>
      setDark(document.documentElement.dataset.mode === "dark"),
    );
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-mode"],
    });
    return () => observer.disconnect();
  }, []);
  return dark;
}
function SeriesChart({ points, fields, type = "line", unit }) {
  const dark = useDarkMode();
  if (!points?.length)
    return (
      <p className="analytics-empty">No metrics in the selected window.</p>
    );
  const data = fields.map(([key, name], index) => ({
    name,
    color: ChartPalette.categorical(index, dark),
    data: points.map((p) => [Number(p.ts), Number(p[key]) || 0]),
  }));
  return (
    <TimeseriesChart
      echarts={echarts}
      data={data}
      type={type}
      isDarkMode={dark}
      height={240}
      yAxisName={unit}
      yAxisMinInterval={unit === "Requests" ? 1 : undefined}
      tooltipValueFormat={(value) =>
        unit === "ms" ? `${value.toFixed(2)} ms` : value.toLocaleString()
      }
      ariaDescription={`${fields.map((f) => f[1]).join(" and ")} over time, measured in ${unit}.`}
    />
  );
}
export function AnalyticsChart({ points }) {
  return (
    <div className="performance-charts">
      <SeriesChart
        points={points}
        fields={[
          ["count", "Requests"],
          ["errorCount", "Errors"],
        ]}
        unit="Requests"
      />
      <SeriesChart
        points={points}
        fields={[
          ["avgMs", "Average duration"],
          ["p95Ms", "P95 duration"],
        ]}
        unit="ms"
      />
    </div>
  );
}
export function RequestsBarChart({ points }) {
  return (
    <SeriesChart
      points={points}
      fields={[["count", "Requests"]]}
      type="bar"
      unit="Requests"
    />
  );
}
