"use client";

import type { ReactNode } from "react";

interface PanelProps {
  title: string;
  children: ReactNode;
  className?: string;
  headerRight?: ReactNode;
}

export function Panel({ title, children, className = "", headerRight }: PanelProps) {
  return (
    <div className={`flex flex-col bg-bg-panel border border-border-panel rounded-lg overflow-hidden h-full ${className}`}>
      <div className="flex items-center justify-between px-3 py-2 border-b border-border-panel bg-bg-panel/80 cursor-move select-none shrink-0">
        <span className="text-xs font-semibold uppercase tracking-wider text-text-secondary">{title}</span>
        {headerRight}
      </div>
      <div className="flex-1 overflow-auto p-3">{children}</div>
    </div>
  );
}
