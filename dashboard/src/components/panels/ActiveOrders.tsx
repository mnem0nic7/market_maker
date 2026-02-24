"use client";

import { useDashboard } from "@/lib/store";
import type { OrderStatus } from "@/lib/types";

function statusColor(status: OrderStatus): string {
  switch (status) {
    case "New":
    case "Submitted":
      return "text-accent-cyan";
    case "PartiallyFilled":
      return "text-accent-amber";
    case "Filled":
      return "text-accent-green";
    case "Cancelled":
    case "Expired":
      return "text-text-muted";
    case "Rejected":
      return "text-accent-red";
    case "Pending":
    case "PendingCancel":
    default:
      return "text-text-secondary";
  }
}

function sideColor(side: string): string {
  return side === "Buy" ? "text-accent-green" : "text-accent-red";
}

export function ActiveOrdersPanel() {
  const { orders } = useDashboard();

  const sorted = [...orders].sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
  );

  if (sorted.length === 0) {
    return <p className="text-text-muted text-sm">No orders</p>;
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-xs">
        <thead>
          <tr className="text-text-secondary border-b border-border-panel">
            <th className="text-left py-1.5 px-2">ID</th>
            <th className="text-left py-1.5 px-2">Exchange</th>
            <th className="text-left py-1.5 px-2">Symbol</th>
            <th className="text-left py-1.5 px-2">Side</th>
            <th className="text-left py-1.5 px-2">Type</th>
            <th className="text-right py-1.5 px-2">Price</th>
            <th className="text-right py-1.5 px-2">Qty</th>
            <th className="text-right py-1.5 px-2">Filled</th>
            <th className="text-left py-1.5 px-2">Status</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((order) => (
            <tr
              key={order.client_order_id}
              className="border-b border-border-panel/50 hover:bg-bg-hover transition-colors"
            >
              <td
                className="py-1.5 px-2 font-mono text-text-primary"
                title={order.client_order_id}
              >
                {order.client_order_id.substring(0, 8)}
              </td>
              <td className="py-1.5 px-2 text-text-secondary">{order.exchange}</td>
              <td className="py-1.5 px-2 font-mono text-text-primary">
                {order.symbol.base}/{order.symbol.quote}
              </td>
              <td className={`py-1.5 px-2 font-mono font-semibold ${sideColor(order.side)}`}>
                {order.side === "Buy" ? "BUY" : "SELL"}
              </td>
              <td className="py-1.5 px-2 text-text-secondary">{order.order_type}</td>
              <td className="py-1.5 px-2 font-mono text-right text-text-primary">
                {order.price ?? "MKT"}
              </td>
              <td className="py-1.5 px-2 font-mono text-right text-text-primary">
                {order.quantity}
              </td>
              <td className="py-1.5 px-2 font-mono text-right text-text-secondary">
                {order.filled_quantity}
              </td>
              <td className={`py-1.5 px-2 font-mono font-semibold ${statusColor(order.status)}`}>
                {order.status}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
