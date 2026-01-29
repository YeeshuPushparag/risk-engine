"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import React, { useState, useEffect, useRef } from "react";
import { useWebSocket } from "@/hooks/useWebSocket";
import {
  ArrowLeft, Crosshair, ShieldAlert,
  Layers, Gauge, TrendingUp, Clock
} from "lucide-react";

/* ================= FORMATTERS ================= */
const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", maximumFractionDigits: 2,
});

const fmtUSD = (n?: number | null) => {
  if (n == null || Number.isNaN(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e12) return `${currencyFormatter.format(n / 1e12)}T`;
  if (abs >= 1e9) return `${currencyFormatter.format(n / 1e9)}B`;
  if (abs >= 1e6) return `${currencyFormatter.format(n / 1e6)}M`;
  if (abs >= 1e3) return `${currencyFormatter.format(n / 1e3)}K`;
  return currencyFormatter.format(n);
};

const fmtPct = (v?: number | null) => {
  if (v == null) return "—";
  const val = v * 100;
  return (
    <span className={val >= 0 ? "text-emerald-400" : "text-red-400"}>
      {val >= 0 ? "+" : ""}{val.toFixed(2)}%
    </span>
  );
};

/* ================= UI COMPONENTS ================= */
const getSeverityClass = (severity: string) => {
  const s = severity?.toLowerCase();
  switch (s) {
    case "critical": return "bg-red-500/10 border-red-500/40 text-red-500 shadow-[0_0_10px_rgba(239,68,68,0.1)]";
    case "high": return "bg-orange-500/10 border-orange-500/40 text-orange-500";
    case "medium": return "bg-yellow-500/20 border-yellow-500/40 text-yellow-500";
    case "low": return "bg-blue-500/10 border-blue-500/40 text-blue-400";
    default: return "bg-slate-500/10 border-slate-500/40 text-slate-400";
  }
};

function FlashValue({ value, children, className = "" }: any) {
  const [flash, setFlash] = useState("");
  const prevValue = useRef(value);

  useEffect(() => {
    if (prevValue.current !== value) {
      if (typeof value === "number" && typeof prevValue.current === "number") {
        setFlash(value > prevValue.current ? "animate-flash-green" : "animate-flash-red");
      }
      prevValue.current = value;
      const timer = setTimeout(() => setFlash(""), 1000);
      return () => clearTimeout(timer);
    }
  }, [value]);

  return <span className={`${className} ${flash} transition-all duration-700 rounded px-1`}>{children}</span>;
}

function TacticalMetric({ label, value, numericValue }: any) {
  const [flash, setFlash] = useState("");
  const prevValue = useRef(numericValue);

  useEffect(() => {
    if (numericValue !== prevValue.current) {
      if (numericValue > prevValue.current) setFlash("animate-flash-green");
      else if (numericValue < prevValue.current) setFlash("animate-flash-red");
      prevValue.current = numericValue;
      const timer = setTimeout(() => setFlash(""), 1000);
      return () => clearTimeout(timer);
    }
  }, [numericValue]);

  return (
    <div className={`bg-slate-900/40 border border-slate-800 p-4 rounded-xl transition-all duration-700 ${flash}`}>
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">{label}</p>
      <p className="text-lg font-mono font-bold text-white italic">{value}</p>
    </div>
  );
}

/* ================= MAIN PAGE ================= */
export default function TickerManagerIntradayPage() {
 const params = useParams();
const ticker = String(params.ticker).toUpperCase();
const manager = decodeURIComponent(String(params.manager));

const [timestamp, setTimestamp] = useState("");
const [totals, setTotals] = useState<any>(null);
const [signals, setSignals] = useState<any>(null);
const [alerts, setAlerts] = useState<any[]>([]);

// FX-style change
const [equityEnabled, setEquityEnabled] = useState<boolean | null>(null);
const [wsBaseUrl, setWsBaseUrl] = useState<string | null>(null);

function isMarketTradingTime() {
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
  const CLOSE_MIN = 960 + 2;

  if (weekday === "Sat" || weekday === "Sun") return false;
  return totalMin >= 570 && totalMin <= CLOSE_MIN;
}

useEffect(() => {
  async function fetchConfig() {
    try {
      const res = await fetch("/api/config");
      const config = await res.json();

      const enabled = config.forceStream || isMarketTradingTime();
      setEquityEnabled(enabled);

      if (!enabled) return;

      setWsBaseUrl(config.wsBaseUrl);
    } catch (e) {
      console.error(e);
    }
  }

  fetchConfig();
}, []);

useEffect(() => {
  if (!equityEnabled || !ticker || !manager) return;
  async function load() {
    try {
      const res = await fetch(
        `/api/equity/intraday/ticker_manager?ticker=${ticker}&manager=${manager}`,
        { cache: "no-store" }
      );
      if (!res.ok) return;
      const json = await res.json();
      setTimestamp(json.timestamp);
      setTotals(json.totals);
      setSignals(json.signals);
      setAlerts(json.alerts ?? []);
    } catch (e) {
      console.error(e);
    }
  }
  load();
}, [ticker, manager, equityEnabled]);

useWebSocket(
  equityEnabled && wsBaseUrl && ticker && manager
    ? `${wsBaseUrl}/equity/ticker_manager/${ticker}/${manager}/`
    : null,
  (json) => {
    setTimestamp(json.timestamp);
    setTotals(json.totals);
    setSignals(json.signals);
    setAlerts(json.alerts ?? []);
  }
);

// FX-style return logic
if (equityEnabled === null) {
  return <LoadingState />;
}

if (equityEnabled === false) {
  return (
    <div className="min-h-screen bg-[#020617] flex items-center justify-center p-12 text-center">
      <div className="space-y-4 max-w-md">
        <Clock className="w-12 h-12 text-slate-700 mx-auto" />
        <h2 className="text-xl font-black text-white uppercase tracking-tight">Market Closed</h2>
        <p className="text-slate-400 text-sm leading-relaxed">
          U.S. equity markets operate <strong>Monday through Friday</strong>, 9:30 AM to 4:00 PM (EST).
        </p>
        <Link
          href="/dashboard/equity/daily"
          className="inline-block mt-4 text-blue-500 text-[10px] font-black uppercase tracking-widest border border-blue-500/30 px-6 py-2 rounded-lg hover:bg-blue-500/10 transition"
        >
          ← View Daily Equity Data
        </Link>
      </div>
    </div>
  );
}

if (!totals || !signals) {
 return <LoadingState />;
}


  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-6 lg:p-12 space-y-12">
      {/* HEADER: Fully Responsive Text Scaling */}
      <header className="flex flex-col md:flex-row justify-between items-start gap-6 border-b border-slate-800/60 pb-10">
        <div className="space-y-5 w-full md:w-auto">
          <div className="flex items-center gap-3">
            <Link
              href={`/dashboard/equity/intraday/ticker/${ticker}`}
              className="p-2 bg-slate-900 border border-slate-800 rounded-lg hover:border-blue-500 transition-all group"
            >
              <ArrowLeft className="w-4 h-4 text-slate-400 group-hover:text-blue-500" />
            </Link>
            <div className="px-2 py-1 bg-blue-500/10 border border-blue-500/20 rounded flex items-center gap-2">
              <Crosshair className="w-3 h-3 text-blue-500" />
              <span className="text-[8px] sm:text-[9px] font-black text-blue-500 uppercase tracking-widest">Tactical View</span>
            </div>
          </div>

          <h1 className="text-2xl sm:text-3xl md:text-4xl font-black text-white tracking-tighter uppercase italic leading-none truncate">
            {ticker} <span className="text-blue-500 italic">×</span> {manager}
          </h1>

          <div className="flex items-center gap-4 text-[9px] sm:text-[10px] font-mono font-bold text-slate-500 tracking-widest uppercase">
            <span className="flex items-center gap-1.5">
              <div className="w-1 h-1 bg-slate-700 rounded-full" />
              Pair Analysis
            </span>
            <span className="text-slate-800">|</span>
            <span className="text-slate-400">
              <FlashValue value={timestamp}>
                {timestamp ? new Date(timestamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true }) : "CONNECTING"}
              </FlashValue>
            </span>
          </div>
        </div>

        <div className="w-full md:w-auto">
          <div className="bg-slate-900/80 p-5 border border-slate-800 rounded-2xl text-left md:text-right min-w-[160px]">
            <p className="text-[9px] sm:text-[10px] font-black text-slate-500 uppercase mb-1">Pair P&L</p>
            <div className="text-2xl font-mono font-bold italic">
              <FlashValue value={totals.pnl} className={totals.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}>
                {fmtUSD(totals.pnl)}
              </FlashValue>
            </div>
          </div>
        </div>
      </header>

      {/* EXPOSURE METRICS */}
      <section className="space-y-6">
        <div className="flex items-center gap-3 border-l-4 border-blue-600 pl-4">
          <Layers className="w-4 h-4 text-blue-500" />
          <h2 className="text-xs sm:text-sm font-black text-white uppercase tracking-[0.2em]">Exposure Metrics</h2>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <TacticalMetric label="Market Exposure" value={fmtUSD(totals.exposure)} numericValue={totals.exposure} />
          <TacticalMetric label="Shares Held" value={totals.shares.toLocaleString()} numericValue={totals.shares} />
          <TacticalMetric label="Port. Weight" value={fmtPct(totals.weight)} numericValue={totals.weight} />
          <TacticalMetric label="Live Price" value={fmtUSD(totals.price)} numericValue={totals.price} />
        </div>
      </section>

      {/* SIGNAL ANALYTICS */}
      <section className="space-y-6">
        <div className="flex items-center gap-3 border-l-4 border-emerald-600 pl-4">
          <Gauge className="w-4 h-4 text-emerald-500" />
          <h2 className="text-xs sm:text-sm font-black text-white uppercase tracking-[0.2em]">Intraday Trade Signals</h2>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
          <TacticalMetric label="1m Return" value={fmtPct(signals.return_1m)} numericValue={signals.return_1m} />
          <TacticalMetric label="5m Return" value={fmtPct(signals.return_5m)} numericValue={signals.return_5m} />
          <TacticalMetric label="15m Vol" value={fmtPct(signals.vol_15m)} numericValue={signals.vol_15m} />
          <TacticalMetric label="Range % (1m)" value={fmtPct(signals.range_pct_1m)} numericValue={signals.range_pct_1m} />
          <TacticalMetric label="Trend Slope (5m)" value={signals.trend_slope_5m.toFixed(3)} numericValue={signals.trend_slope_5m} />
          <TacticalMetric label="VWAP 5m" value={signals.vwap_5m.toFixed(2)} numericValue={signals.vwap_5m} />
          <TacticalMetric label="Breakout Str" value={signals.breakout_strength.toFixed(2)} numericValue={signals.breakout_strength} />
          <TacticalMetric label="Volume Burst" value={signals.volume_burst.toFixed(2)} numericValue={signals.volume_burst} />
          <TacticalMetric label="Rolling High" value={fmtUSD(signals.rolling_high)} numericValue={signals.rolling_high} />
          <TacticalMetric label="Rolling Low" value={fmtUSD(signals.rolling_low)} numericValue={signals.rolling_low} />
          <TacticalMetric label="Close Δ" value={fmtUSD(signals.close_diff)} numericValue={signals.close_diff} />
          <div className="bg-emerald-500/5 border border-emerald-500/20 p-4 rounded-xl flex flex-col justify-center items-center">
            <TrendingUp className="w-5 h-5 text-emerald-500 mb-1" />
            <span className="text-[9px] font-black text-emerald-500 uppercase tracking-widest">Signal Ready</span>
          </div>
        </div>
      </section>

      {/* ALERTS */}
      <section className="space-y-6">
        <div className="flex items-center gap-3 border-l-4 border-amber-600 pl-4">
          <ShieldAlert className="w-4 h-4 text-amber-500" />
          <h2 className="text-xs sm:text-sm font-black text-white uppercase tracking-[0.2em]">Pair Violations</h2>
        </div>
        {alerts.length === 0 ? (
          <div className="bg-slate-900/20 border border-slate-800 rounded-2xl p-10 text-center">
            <p className="text-[10px] font-black text-slate-600 uppercase tracking-[0.4em] italic">Operational Status: Nominal</p>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {alerts.map((a, i) => (
              <div key={i} className={`p-5 rounded-2xl border flex justify-between items-center transition-all ${getSeverityClass(a.severity)}`}>
                <div className="space-y-1">
                  <p className="text-[10px] font-black uppercase tracking-widest opacity-60">{a.severity}</p>
                  <p className="text-sm font-bold italic">{a.type}</p>
                </div>
                <div className="text-right">
                  <p className="text-[10px] font-mono opacity-60 uppercase">{a.time}</p>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      <footer className="pt-10 flex justify-center">
        <Link href={`/dashboard/equity/intraday/ticker/${ticker}`} className="px-8 py-3 bg-slate-900 border border-slate-800 rounded-full text-[10px] font-black text-blue-500 uppercase tracking-[0.3em] hover:border-blue-500 hover:text-white transition-all">
          ← Back to {ticker} Central
        </Link>
      </footer>
    </main>
  );
}

function LoadingState() {
  return (
      <div className="p-8 bg-[#020617] min-h-screen flex items-center justify-center">
      <div className="text-blue-500 font-black animate-pulse tracking-widest text-xs uppercase">
        Initialising Tactical Engine...
      </div>
    </div>
  );
}