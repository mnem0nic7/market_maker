"use client";

import { useState, useCallback, useEffect } from "react";
import {
  ResponsiveGridLayout,
  useContainerWidth,
  verticalCompactor,
  type Layout,
  type ResponsiveLayouts,
} from "react-grid-layout";

const STORAGE_KEY = "dashboard-layouts";

type Breakpoint = "lg" | "md" | "sm" | "xs";

const defaultLayouts: ResponsiveLayouts<Breakpoint> = {
  lg: [
    { i: "market-data", x: 0, y: 0, w: 6, h: 4, minW: 3, minH: 3 },
    { i: "positions", x: 6, y: 0, w: 6, h: 4, minW: 3, minH: 3 },
    { i: "order-book", x: 0, y: 4, w: 4, h: 5, minW: 3, minH: 4 },
    { i: "order-entry", x: 4, y: 4, w: 4, h: 5, minW: 3, minH: 4 },
    { i: "strategies", x: 8, y: 4, w: 4, h: 5, minW: 3, minH: 3 },
    { i: "price-chart", x: 0, y: 9, w: 8, h: 5, minW: 4, minH: 4 },
    { i: "risk", x: 8, y: 9, w: 4, h: 5, minW: 3, minH: 3 },
    { i: "orders", x: 0, y: 14, w: 8, h: 4, minW: 4, minH: 3 },
    { i: "event-log", x: 8, y: 14, w: 4, h: 4, minW: 3, minH: 3 },
    { i: "strategy-form", x: 0, y: 18, w: 4, h: 6, minW: 3, minH: 5 },
    { i: "ai-assistant", x: 4, y: 18, w: 4, h: 6, minW: 3, minH: 4 },
  ],
};

function loadLayouts(): ResponsiveLayouts<Breakpoint> {
  if (typeof window === "undefined") return defaultLayouts;
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    return saved ? JSON.parse(saved) : defaultLayouts;
  } catch {
    return defaultLayouts;
  }
}

interface DashboardGridProps {
  children: React.ReactNode;
}

export function DashboardGrid({ children }: DashboardGridProps) {
  const { width, containerRef } = useContainerWidth({ initialWidth: 1200 });
  const [clientReady, setClientReady] = useState(false);
  const [layouts, setLayouts] = useState<ResponsiveLayouts<Breakpoint>>(defaultLayouts);

  // Only render the grid after hydration completes — avoids SSR mismatch on
  // computed transforms/widths/heights which depend on actual window size.
  useEffect(() => {
    setLayouts(loadLayouts());
    setClientReady(true);
  }, []);

  const onLayoutChange = useCallback((_layout: Layout, allLayouts: ResponsiveLayouts<Breakpoint>) => {
    setLayouts(allLayouts);
    try { localStorage.setItem(STORAGE_KEY, JSON.stringify(allLayouts)); } catch {}
  }, []);

  if (!clientReady) {
    return <div ref={containerRef} />;
  }

  return (
    <div ref={containerRef}>
      <ResponsiveGridLayout
        className="layout"
        width={width}
        layouts={layouts}
        breakpoints={{ lg: 1200, md: 996, sm: 768, xs: 480 }}
        cols={{ lg: 12, md: 10, sm: 6, xs: 4 }}
        rowHeight={40}
        onLayoutChange={onLayoutChange}
        dragConfig={{ handle: ".cursor-move" }}
        compactor={verticalCompactor}
        margin={[8, 8]}
      >
        {children}
      </ResponsiveGridLayout>
    </div>
  );
}
