"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useDashboard } from "@/lib/store";

interface ChartApi {
  applyOptions: (options: Record<string, unknown>) => void;
  addSeries: (type: unknown, options?: Record<string, unknown>) => SeriesApi;
  timeScale: () => { fitContent: () => void };
  remove: () => void;
}

interface SeriesApi {
  update: (data: { time: number; value: number }) => void;
}

export function PriceChartPanel() {
  const { ticks } = useDashboard();
  const tickKeys = Object.keys(ticks);
  const [selectedKey, setSelectedKey] = useState<string>("");
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<ChartApi | null>(null);
  const seriesRef = useRef<SeriesApi | null>(null);
  const observerRef = useRef<ResizeObserver | null>(null);

  // Auto-select first key
  useEffect(() => {
    if (!selectedKey && tickKeys.length > 0) {
      setSelectedKey(tickKeys[0]);
    }
  }, [selectedKey, tickKeys]);

  // Create chart
  const initChart = useCallback(async () => {
    if (!containerRef.current) return;

    // Clean up existing chart
    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
      seriesRef.current = null;
    }

    try {
      const { createChart, LineSeries } = await import("lightweight-charts");

      const chart = createChart(containerRef.current, {
        width: containerRef.current.clientWidth,
        height: containerRef.current.clientHeight,
        layout: {
          background: { color: "#0a0a0f" },
          textColor: "#8888aa",
          fontFamily: "JetBrains Mono, monospace",
          fontSize: 10,
        },
        grid: {
          vertLines: { color: "#1a1a2e" },
          horzLines: { color: "#1a1a2e" },
        },
        timeScale: {
          timeVisible: true,
          secondsVisible: true,
          borderColor: "#2a2a3e",
        },
        rightPriceScale: {
          borderColor: "#2a2a3e",
        },
        crosshair: {
          horzLine: { color: "#00d4ff", style: 3, width: 1 },
          vertLine: { color: "#00d4ff", style: 3, width: 1 },
        },
      });

      const series = chart.addSeries(LineSeries, {
        color: "#00d4ff",
        lineWidth: 2,
        priceLineVisible: true,
        priceLineColor: "#00d4ff",
        lastValueVisible: true,
      });

      chartRef.current = chart as unknown as ChartApi;
      seriesRef.current = series as unknown as SeriesApi;

      // ResizeObserver
      const observer = new ResizeObserver((entries) => {
        for (const entry of entries) {
          const { width, height } = entry.contentRect;
          (chart as unknown as ChartApi).applyOptions({ width, height });
        }
      });
      observer.observe(containerRef.current);
      observerRef.current = observer;
    } catch (e) {
      console.error("Failed to load lightweight-charts:", e);
    }
  }, []);

  useEffect(() => {
    initChart();
    return () => {
      if (observerRef.current) {
        observerRef.current.disconnect();
      }
      if (chartRef.current) {
        chartRef.current.remove();
        chartRef.current = null;
        seriesRef.current = null;
      }
    };
  }, [initChart]);

  // Re-create series when symbol changes
  useEffect(() => {
    if (chartRef.current && seriesRef.current) {
      // Remove old chart and recreate to reset series data
      initChart();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedKey]);

  // Update chart with new tick data
  useEffect(() => {
    if (!selectedKey || !seriesRef.current) return;
    const tick = ticks[selectedKey];
    if (!tick) return;

    const midPrice = parseFloat(tick.mid_price);
    if (isNaN(midPrice)) return;

    const time = Math.floor(Date.now() / 1000);
    seriesRef.current.update({ time, value: midPrice });
  }, [ticks, selectedKey]);

  return (
    <div className="flex flex-col gap-2 h-full">
      {/* Symbol selector */}
      <select
        value={selectedKey}
        onChange={(e) => setSelectedKey(e.target.value)}
        className="bg-bg-primary border border-border-panel rounded px-2 py-1 text-xs text-text-primary font-mono focus:outline-none focus:border-accent-cyan shrink-0"
      >
        {tickKeys.length === 0 && <option value="">No symbols available</option>}
        {tickKeys.map((key) => (
          <option key={key} value={key}>
            {key}
          </option>
        ))}
      </select>

      {/* Chart container */}
      <div ref={containerRef} className="flex-1 min-h-0" />

      {tickKeys.length === 0 && (
        <p className="text-text-muted text-sm text-center">
          Waiting for market data to plot...
        </p>
      )}
    </div>
  );
}
