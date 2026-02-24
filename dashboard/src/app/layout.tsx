import type { Metadata } from "next";
import { DashboardProvider } from "@/lib/store";
import "./globals.css";

export const metadata: Metadata = {
  title: "Trading Dashboard",
  description: "Market Maker Trading Desk",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-bg-primary text-text-primary antialiased min-h-screen">
        <DashboardProvider>{children}</DashboardProvider>
      </body>
    </html>
  );
}
