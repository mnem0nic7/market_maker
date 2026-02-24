"use client";

import { useState, useCallback } from "react";
import { useDashboard } from "@/lib/store";
import { api } from "@/lib/api";

function pnlColor(value: number): string {
  if (value > 0) return "text-accent-green";
  if (value < 0) return "text-accent-red";
  return "text-text-primary";
}

interface MetricRowProps {
  label: string;
  value: string;
  colorClass?: string;
}

function MetricRow({ label, value, colorClass = "text-text-primary" }: MetricRowProps) {
  return (
    <div className="flex items-center justify-between py-1.5 border-b border-border-panel/50">
      <span className="text-xs text-text-secondary">{label}</span>
      <span className={`text-xs font-mono ${colorClass}`}>{value}</span>
    </div>
  );
}

export function RiskDashboardPanel() {
  const { risk } = useDashboard();
  const [killSwitchActive, setKillSwitchActive] = useState(false);
  const [confirmArm, setConfirmArm] = useState(false);
  const [loading, setLoading] = useState(false);

  const handleKillSwitch = useCallback(async () => {
    if (!killSwitchActive && !confirmArm) {
      setConfirmArm(true);
      return;
    }

    setLoading(true);
    try {
      if (killSwitchActive) {
        const res = await api.resetKillSwitch();
        setKillSwitchActive(res.active);
      } else {
        const res = await api.activateKillSwitch();
        setKillSwitchActive(res.active);
      }
      setConfirmArm(false);
    } catch (e) {
      console.error("Kill switch error:", e);
    } finally {
      setLoading(false);
    }
  }, [killSwitchActive, confirmArm]);

  const handleCancelConfirm = useCallback(() => {
    setConfirmArm(false);
  }, []);

  return (
    <div className="flex flex-col gap-4 h-full">
      {/* Risk Metrics */}
      <div className="flex-1">
        {risk ? (
          <div className="flex flex-col">
            <MetricRow
              label="Net Exposure"
              value={risk.net_exposure}
            />
            <MetricRow
              label="Gross Exposure"
              value={risk.gross_exposure}
            />
            <MetricRow
              label="Daily PnL"
              value={risk.daily_pnl}
              colorClass={pnlColor(parseFloat(risk.daily_pnl))}
            />
            <MetricRow
              label="Max Drawdown"
              value={risk.max_drawdown}
              colorClass="text-accent-red"
            />
            <MetricRow
              label="VaR 95%"
              value={risk.total_var_95}
              colorClass="text-accent-amber"
            />
            <MetricRow
              label="VaR 99%"
              value={risk.total_var_99}
              colorClass="text-accent-red"
            />
          </div>
        ) : (
          <p className="text-text-muted text-sm">No risk data available</p>
        )}
      </div>

      {/* Kill Switch */}
      <div className="border-t border-border-panel pt-3">
        <div className="text-[10px] uppercase tracking-wider text-text-muted mb-2">
          Emergency Kill Switch
        </div>

        {confirmArm && !killSwitchActive && (
          <div className="bg-accent-red/10 border border-accent-red/30 rounded p-2 mb-2">
            <p className="text-xs text-accent-red">
              This will cancel ALL open orders. Click again to confirm.
            </p>
            <button
              type="button"
              onClick={handleCancelConfirm}
              className="text-[10px] text-text-muted hover:text-text-secondary mt-1"
            >
              Cancel
            </button>
          </div>
        )}

        <button
          type="button"
          disabled={loading}
          onClick={handleKillSwitch}
          className={`w-full py-2 rounded font-semibold text-sm transition-colors ${
            killSwitchActive
              ? "bg-accent-green text-bg-primary hover:bg-accent-green/80"
              : confirmArm
                ? "bg-accent-red text-bg-primary hover:bg-accent-red/80 animate-pulse"
                : "bg-accent-red/20 text-accent-red hover:bg-accent-red/30 border border-accent-red/50"
          } ${loading ? "opacity-50 cursor-not-allowed" : ""}`}
        >
          {loading
            ? "..."
            : killSwitchActive
              ? "RESET KILL SWITCH"
              : confirmArm
                ? "CONFIRM KILL SWITCH"
                : "ACTIVATE KILL SWITCH"}
        </button>
      </div>
    </div>
  );
}
