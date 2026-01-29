"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { useParams } from "next/navigation";
import { useWebSocket } from "@/hooks/useWebSocket";
import {
  Clock,
  Activity,
  ShieldAlert,
  Globe,
  ArrowLeft,
  TrendingUp,
} from "lucide-react";
import Link from "next/link";

/* ---------------- FORMATTERS ---------------- */



const fmtCur = (v?: number | null) => {
  if (v == null) return "—";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
};

const fmtPct = (n?: number | null) =>
  n == null ? "—" : `${(n * 100).toFixed(2)}%`;

const fmtFX = (n?: number | null) =>
  n == null ? "—" : n.toFixed(4);

/* ---------------- METRIC CARD WITH ANIMATION FIX ---------------- */
function MetricCard({
  label,
  value,
  variant = "neutral",
  flashTrigger,
}: {
  label: string;
  value: React.ReactNode;
  variant?: "neutral" | "pnl";
  flashTrigger?: number | null;
}) {
  const [flash, setFlash] = useState<"up" | "down" | null>(null);
  const prevValue = useRef<number | null>(null);

  useEffect(() => {
    // comparison logic: only flash if we have a baseline to compare against
    if (prevValue.current !== null && flashTrigger !== null && flashTrigger !== undefined) {
      if (flashTrigger > prevValue.current) setFlash("up");
      else if (flashTrigger < prevValue.current) setFlash("down");

      const timer = setTimeout(() => setFlash(null), 800);
      return () => clearTimeout(timer);
    }

    // Update baseline for next socket tick
    if (flashTrigger !== null && flashTrigger !== undefined) {
      prevValue.current = flashTrigger;
    }
  }, [flashTrigger]);

  const flashClass = flash === "up" ? "animate-flash-green" : flash === "down" ? "animate-flash-red" : "";

  return (
    <div className={`p-4 sm:p-5 border border-slate-800 rounded-2xl bg-slate-900/40 backdrop-blur-md transition-all duration-500 ${flashClass}`}>
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-2">
        {label}
      </p>
      <div className={`text-xl sm:text-2xl font-mono font-bold tracking-tighter ${variant === "pnl"
        ? (flashTrigger ?? 0) >= 0 ? "text-emerald-400" : "text-red-400"
        : "text-white"
        }`}>
        {value}
      </div>
    </div>
  );
}

/* ---------------- MAIN PAGE ---------------- */
export default function TickerPage() {
  const { ticker } = useParams();
  const [data, setData] = useState<any>(null);
  const [wsUrl, setWsUrl] = useState<string | null>(null);
  const [fxEnabled, setFxEnabled] = useState<boolean | null>(null);

  const handleUpdate = (update: any) => {
    setData(update);
  };

  useEffect(() => {
    if (!ticker) return;

    async function fetchConfigAndData() {
      try {
        const res = await fetch("/api/config");
        const config = await res.json();

        const enabled = config.forceStream || isFXTradingTime();
        setFxEnabled(enabled);

        if (!enabled) return;

        setWsUrl(`${config.wsBaseUrl}/fx/ticker/${ticker}/`);

        const initialRes = await fetch(
          `/api/fx/intraday/ticker?ticker=${ticker}`,
          { cache: "no-store" }
        );

        if (initialRes.ok) {
          handleUpdate(await initialRes.json());
        }
      } catch (e) {
        console.error("Failed to load config or data:", e);
      }
    }

    fetchConfigAndData();
  }, [ticker]);

  useWebSocket(wsUrl, handleUpdate);

  // ---- RETURN LOGIC (FIXED) ----
  if (fxEnabled === null) return <LoadingState />;
  if (fxEnabled === false) return <MarketClosedView />;
  if (!data) return <LoadingState />;

  const rangePercent =
    data.high != null && data.low != null && data.close != null && data.high !== data.low
      ? ((data.close - data.low) / (data.high - data.low)) * 100
      : 0;
  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-6 lg:p-12 space-y-8">

      {/* HEADER - RESPONSIVE SIZING */}
      <header className="space-y-4">
        <Link
          href="/dashboard/fx/intraday"
          className="inline-flex items-center text-[10px] font-bold text-slate-500 hover:text-blue-400 uppercase tracking-widest gap-2 transition-colors"
        >
          <ArrowLeft className="w-3 h-3" /> Back to Terminal
        </Link>

        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-end gap-4">
          <div>
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="bg-blue-600/20 text-blue-400 p-1.5 rounded-lg border border-blue-500/30">
                <Globe className="w-5 h-5 sm:w-6 sm:h-6" />
              </div>
              <h1 className="text-2xl sm:text-5xl lg:text-6xl font-black text-white uppercase italic tracking-tighter leading-none">
                {data.ticker}
              </h1>
            </div>
            <p className="text-[11px] sm:text-lg font-bold text-slate-500 mt-2">
              {data.currency_pair} <span className="text-slate-800 mx-2">|</span> <span className="text-blue-500 uppercase tracking-tighter">Real Time Feed</span>
            </p>
          </div>

          <div className="text-[9px] font-mono font-bold text-slate-400 bg-slate-900 px-3 py-1.5 rounded-full border border-slate-800 flex items-center gap-2">
            <Clock className="w-3 h-3 text-blue-500" />
            {data.timestamp ? new Date(data.timestamp).toLocaleTimeString() : "CONNECTING"}
          </div>
        </div>
      </header>

      {/* PRIMARY METRICS - 2 COLS ON MOBILE */}
      <section className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-5">
        <MetricCard label="Market Price" value={fmtFX(data.close)} flashTrigger={data.close} />
        <MetricCard label="1M Return" value={fmtPct(data.fx_return_1m)} flashTrigger={data.fx_return_1m} />
        <MetricCard label="FX P&L" value={fmtCur(data.fx_pnl)} variant="pnl" flashTrigger={data.fx_pnl} />
        <MetricCard label="Position Size" value={fmtCur(data.position_size)} />
      </section>

      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 sm:gap-8">
        {/* RANGE SECTION */}
        <div className="xl:col-span-2 bg-slate-900/40 border border-slate-800 rounded-3xl p-6 sm:p-8 space-y-8">
          <div className="flex justify-between items-center">
            <h3 className="text-[10px] font-black text-slate-500 uppercase tracking-[0.3em]">Session Range</h3>
            <span className="text-[10px] text-blue-500 font-bold uppercase px-2 py-0.5 bg-blue-500/10 rounded border border-blue-500/20 flex items-center gap-1">
              <TrendingUp className="w-3 h-3" /> Live
            </span>
          </div>

          <div className="space-y-4">
            <div className="flex justify-between text-[10px] sm:text-xs font-mono font-bold text-slate-500">
              <span>LOW: {fmtFX(data.low)}</span>
              <span className="text-white">MID: {fmtFX((data.high + data.low) / 2)}</span>
              <span>HIGH: {fmtFX(data.high)}</span>
            </div>
            <div className="relative h-2 w-full bg-slate-800 rounded-full overflow-hidden">
              <div
                className="absolute h-full bg-gradient-to-r from-blue-600 via-indigo-500 to-emerald-500 transition-all duration-1000 ease-out"
                style={{ width: `${rangePercent}%` }}
              />
            </div>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-4 gap-6 pt-4">
            <MetricBox label="5M Momentum" value={fmtPct(data.fx_return_5m)} />
            <MetricBox label="15M Vol (95%)" value={fmtPct(data.fx_vol_15m)} />
            <MetricBox label="Sess. Open" value={fmtFX(data.open)} />
            <MetricBox label="Prev Close" value={fmtFX(data.prev_close)} color="text-slate-600" />
          </div>
        </div>

        {/* RISK SECTION */}
        <div className="bg-slate-900/80 border-t-4 border-red-500 rounded-3xl p-6 sm:p-8 flex flex-col justify-center space-y-4 relative overflow-hidden">
          <div className="flex items-center gap-2 text-red-500">
            <ShieldAlert className="w-5 h-5" />
            <h3 className="text-[10px] font-black uppercase tracking-[0.2em]">Risk Parameters</h3>
          </div>
          <div className="relative z-10">
            <p className="text-3xl sm:text-5xl font-black text-white tracking-tighter">
              {fmtCur(data.VaR_95_15m)}
            </p>
            <p className="text-[9px] font-bold text-slate-500 uppercase tracking-widest mt-1">
              Estimated 15m Value-at-Risk
            </p>
          </div>
          {/* Subtle background decoration */}
          <div className="absolute -right-4 -bottom-4 opacity-5">
            <ShieldAlert className="w-32 h-32 text-red-500" />
          </div>
        </div>
      </div>

      <style jsx global>{`
        @keyframes flash-green { 0% { background: rgba(16, 185, 129, 0.2); } 100% { background: transparent; } }
        @keyframes flash-red { 0% { background: rgba(239, 68, 68, 0.2); } 100% { background: transparent; } }
        .animate-flash-green { animation: flash-green 0.8s ease-out; }
        .animate-flash-red { animation: flash-red 0.8s ease-out; }
      `}</style>
    </main>
  );
}

/* ---------------- REUSABLE UI ---------------- */
function MetricBox({ label, value, color = "text-white" }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <p className="text-[8px] sm:text-[9px] font-black text-slate-600 uppercase mb-1 tracking-wider">{label}</p>
      <p className={`text-xs sm:text-sm font-mono font-bold ${color}`}>{value}</p>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center">
      <Activity className="w-10 h-10 text-blue-600 animate-spin mb-4" />
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.4em]">Establishing Feed</p>
    </div>
  );
}

function MarketClosedView() {
  return (
    <div className="h-screen bg-[#020617] flex items-center justify-center p-12 text-center">
      <div className="max-w-xs space-y-4">
        <Clock className="w-8 h-8 text-slate-700 mx-auto" />
        <h2 className="text-white font-black uppercase tracking-tight text-xl">Market Closed</h2>
        <p className="text-slate-400 text-xs leading-relaxed">
          The FX Market is currently closed. Trading resumes Sunday 5:00 PM (ET).
        </p>
        <Link href="/dashboard/fx/daily" className="inline-block mt-4 text-blue-500 text-[10px] font-black uppercase tracking-widest border border-blue-500/30 px-6 py-2 rounded-lg hover:bg-blue-500/10 transition">
          ← Back to Daily
        </Link>
      </div>
    </div>
  );
}

function isFXTradingTime() {
  const now = new Date();

  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "short",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
  }).formatToParts(now);

  const weekday = parts.find(p => p.type === "weekday")!.value;
  const hour = Number(parts.find(p => p.type === "hour")!.value);
  const minute = Number(parts.find(p => p.type === "minute")!.value);

  const totalMin = hour * 60 + minute;
  const CLOSE_MIN = 17 * 60 + 3; // 5:03 PM ET

  // FX market: Sun 5:00 PM → Fri 5:03 PM ET
  if (weekday === "Sun" && totalMin >= 17 * 60) return true;
  if (weekday !== "Sat" && weekday !== "Sun" && weekday !== "Fri") return true;
  if (weekday === "Fri" && totalMin <= CLOSE_MIN) return true;

  return false;
}
