"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import React, { useEffect, useState, useRef } from "react";
import { useWebSocket } from "@/hooks/useWebSocket";
import {
  ArrowLeft, Activity, TrendingUp, AlertTriangle,
  Globe, BarChart3, Info, Zap, Clock, 
} from "lucide-react";

/* ================== FORMATTERS ================== */

function capitalizeWords(str: string) {
  if (!str) return "";
  return str.split("_").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
}

const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", maximumFractionDigits: 2,
});

const formatCurrency = (v?: number) => {
  if (v == null || Number.isNaN(v)) return "—";

  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";

  const format = (n: number, decimals: number) => {
    const s = n.toFixed(decimals);
    return s.endsWith(".00") ? s.slice(0, -3) : s;
  };

  if (abs >= 1e12) return `${sign}$${format(abs / 1e12, 2)}T`;
  if (abs >= 1e9) return `${sign}$${format(abs / 1e9, 2)}B`;
  if (abs >= 1e6) return `${sign}$${format(abs / 1e6, 1)}M`;
  if (abs >= 1e3) return `${sign}$${format(abs / 1e3, 0)}K`;
  return `${sign}$${abs.toFixed(2)}`;
};

const pctFmt = (v?: number | null) => {
  if (v == null) return "—";
  const val = v * 100;
  return (
    <span className={val >= 0 ? "text-emerald-400" : "text-red-400"}>
      {val >= 0 ? "+" : ""}{val.toFixed(2)}%
    </span>
  );
};

const numberFmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US").format(n);

/* ================== UI ATOMS ================== */

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
      } else {
        setFlash("animate-pulse text-blue-400");
      }
      prevValue.current = value;
      const timer = setTimeout(() => setFlash(""), 1000);
      return () => clearTimeout(timer);
    }
  }, [value]);

  return <span className={`${className} ${flash} transition-all duration-700 rounded px-1`}>{children}</span>;
}

function MetricCard({ label, value, numericValue }: any) {
  const [flash, setFlash] = useState("");
  const prevValue = useRef(numericValue);

  useEffect(() => {
    if (prevValue.current !== undefined && numericValue !== prevValue.current) {
      if (numericValue > prevValue.current) setFlash("animate-flash-green");
      else if (numericValue < prevValue.current) setFlash("animate-flash-red");
      prevValue.current = numericValue;
      const timer = setTimeout(() => setFlash(""), 1000);
      return () => clearTimeout(timer);
    }
  }, [numericValue]);

  return (
    <div className={`bg-slate-900/40 border border-slate-800 p-4 rounded-xl backdrop-blur-md transition-all duration-500 ${flash}`}>
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">{label}</p>
      <div className="text-lg font-mono font-bold text-white italic">
        {value}
      </div>
    </div>
  );
}

/* ================== MAIN PAGE ================== */

export default function TickerIntradayPage() {
const { ticker: raw } = useParams();
const ticker = typeof raw === "string" ? raw.toUpperCase() : "";

const [totals, setTotals] = useState<any>({});
const [market, setMarket] = useState<any>({});
const [fundamentals, setFundamentals] = useState<any>({});
const [alerts, setAlerts] = useState<any[]>([]);
const [managers, setManagers] = useState<any[]>([]);

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
  if (!equityEnabled || !ticker) return;
  async function load() {
    try {
      const res = await fetch(
        `/api/equity/intraday/ticker?ticker=${ticker}`,
        { cache: "no-store" }
      );
      if (!res.ok) return;
      const json = await res.json();
      setTotals(json.totals || {});
      setMarket(json.market || {});
      setFundamentals(json.fundamentals || {});
      setAlerts(json.alerts || []);
      setManagers(json.manager_breakdown || []);
    } catch (e) {
      console.error(e);
    }
  }
  load();
}, [ticker, equityEnabled]);

useWebSocket(
  equityEnabled && wsBaseUrl && ticker
    ? `${wsBaseUrl}/equity/ticker/${ticker}/`
    : null,
  (data) => {
    setTotals(data.totals);
    setMarket(data.market);
    setFundamentals(data.fundamentals);
    setAlerts(data.alerts || []);
    setManagers(data.manager_breakdown || []);
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
          U.S. equity markets operate <strong>Monday through Friday</strong>,
          from <strong>9:30 AM to 4:00 PM (EST)</strong>.
          <br />
          <span className="block mt-1 text-slate-500">
            This corresponds to <strong>8:00 PM – 2:30 AM (IST)</strong>.
          </span>
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

if (!market?.close) {
  return <LoadingState />;
}

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-6 lg:p-12 space-y-10">

      {/* --- HEADER --- */}
      <header className="flex flex-col md:flex-row justify-between items-start gap-6 border-b border-slate-800/60 pb-8">
        <div className="space-y-4 w-full md:w-auto">
          <div className="flex items-center gap-3">
            <Link
              href="/dashboard/equity/intraday"
              className="p-2 bg-slate-900 border border-slate-800 rounded-lg hover:border-blue-500 hover:bg-slate-800 transition-all group"
            >
              <ArrowLeft className="w-4 h-4 text-slate-400 group-hover:text-blue-500" />
            </Link>
            <div className="px-2 py-1 bg-emerald-500/10 border border-emerald-500/20 rounded flex items-center gap-2">
              <div className="w-1.5 h-1.5 bg-emerald-500 rounded-full animate-pulse shadow-[0_0_8px_#10b981]" />
              <span className="text-[9px] font-black text-emerald-500 uppercase tracking-widest">Live Feed</span>
            </div>
          </div>

          <h1 className="text-3xl sm:text-4xl md:text-5xl font-black text-white tracking-tighter italic leading-none">
            {ticker} 
            <span className="text-lg sm:text-xl md:text-2xl not-italic font-light text-slate-500 ml-1">/ USD</span>
          </h1>

          <p className="text-[9px] sm:text-[10px] font-mono font-bold text-slate-500 tracking-widest uppercase flex items-center gap-2">
            <span className="text-slate-700">Last Update:</span>
            <span className="text-slate-400">
              <FlashValue value={market?.timestamp}>
                {market?.timestamp ? new Date(market.timestamp).toLocaleTimeString('en-US', {
                  hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
                }) : "Sourcing Data..."}
              </FlashValue>
            </span>
          </p>
        </div>

        <div className="w-full md:w-auto bg-slate-900/50 p-4 border border-slate-800 rounded-2xl text-left md:text-right">
          <p className="text-[9px] sm:text-[10px] font-black text-slate-500 uppercase mb-1">Last Price</p>
          <div className="text-2xl sm:text-3xl font-mono font-bold text-white">
            <FlashValue value={market.close}>
              ${market.close?.toFixed(2)}
            </FlashValue>
          </div>
          <div className="text-[10px] sm:text-xs font-bold mt-1">
            <FlashValue value={market.return_1m}>
              {pctFmt(market.return_1m)}
            </FlashValue>
          </div>
        </div>
      </header>

      {/* ROW 1: PORTFOLIO & RETURNS */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
        <section className="space-y-4">
          <h2 className="text-[10px] font-black text-white uppercase tracking-[0.3em] flex items-center gap-2">
            <Zap className="w-3 h-3 text-blue-500" /> Portfolio Positioning
          </h2>
          <div className="grid grid-cols-2 gap-3">
            <MetricCard label="Exposure" value={formatCurrency(totals.intraday_exposure)} numericValue={totals.intraday_exposure} />
            <MetricCard label="P&L" value={formatCurrency(totals.intraday_pnl)} numericValue={totals.intraday_pnl} />
            <MetricCard label="Weight" value={pctFmt(totals.portfolio_weight)} numericValue={totals.portfolio_weight} />
            <MetricCard label="Managers" value={totals.managers_count} numericValue={totals.managers_count} />
          </div>
        </section>

        <section className="lg:col-span-2 space-y-4">
          <h2 className="text-[10px] font-black text-white uppercase tracking-[0.3em] flex items-center gap-2">
            <TrendingUp className="w-3 h-3 text-blue-500" /> Execution Metrics
          </h2>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
            <MetricCard label="Return 5m" value={pctFmt(market.return_5m)} numericValue={market.return_5m} />
            <MetricCard label="Vol 15m" value={pctFmt(market.vol_15m)} numericValue={market.vol_15m} />
            <MetricCard label="VWAP 5m" value={market.rolling_vwap_5m?.toFixed(2)} numericValue={market.rolling_vwap_5m} />
            <MetricCard label="Breakout" value={market.breakout_strength?.toFixed(2)} numericValue={market.breakout_strength} />
          </div>
        </section>
      </div>

      {/* MARKET SNAPSHOT TABLES */}
      <section className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div className="bg-slate-900/20 border border-slate-800 rounded-2xl p-6">
          <h3 className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-6">Current Window</h3>
          <div className="grid grid-cols-2 gap-y-4 gap-x-8">
            {[
              { l: "Open", v: market.open?.toFixed(2) }, { l: "High", v: market.high?.toFixed(2) },
              { l: "Low", v: market.low?.toFixed(2) }, { l: "Volume", v: numberFmt(market.volume) }
            ].map((item, idx) => (
              <div key={idx} className="flex justify-between border-b border-slate-800/50 pb-2">
                <span className="text-[10px] font-bold text-slate-500 uppercase">{item.l}</span>
                <FlashValue value={item.v} className="font-mono text-sm text-white font-bold">{item.v}</FlashValue>
              </div>
            ))}
          </div>
        </div>

        <div className="bg-slate-900/20 border border-slate-800 rounded-2xl p-6">
          <h3 className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-6">Previous Window</h3>
          <div className="grid grid-cols-2 gap-y-4 gap-x-8">
            {[
              { l: "Open", v: market.prev_open?.toFixed(2) }, { l: "High", v: market.prev_high?.toFixed(2) },
              { l: "Low", v: market.prev_low?.toFixed(2) }, { l: "Volume", v: numberFmt(market.prev_volume) }
            ].map((item, idx) => (
              <div key={idx} className="flex justify-between border-b border-slate-800/50 pb-2">
                <span className="text-[10px] font-bold text-slate-500 uppercase">{item.l}</span>
                <span className="font-mono text-sm text-slate-400">{item.v}</span>
              </div>
            ))}
          </div>
        </div>
      </section>

      {/* FUNDAMENTALS */}
      <section className="space-y-4">
        <h2 className="text-[10px] font-black text-white uppercase tracking-[0.3em] flex items-center gap-2">
          <Info className="w-3 h-3 text-blue-500" /> Fundamental Profile (Quarterly)
        </h2>
        <div className="bg-slate-900/30 border border-slate-800 rounded-2xl p-8 grid grid-cols-2 md:grid-cols-4 gap-x-12 gap-y-8">
          {[
            { l: "Issuer", v: fundamentals.issuer_name }, { l: "Sector", v: fundamentals.sector },
            { l: "Market Cap", v: formatCurrency(fundamentals.marketCap) }, { l: "Revenue", v: formatCurrency(fundamentals.revenue) },
            { l: "EBITDA Margin", v: pctFmt(fundamentals.ebitda_margin) }, { l: "Debt/EBITDA", v: fundamentals.debt_to_ebitda?.toFixed(2) }
          ].map((f, i) => (
            <div key={i}>
              <p className="text-[9px] font-black text-slate-600 uppercase tracking-widest mb-1">{f.l}</p>
              <p className="text-sm font-bold text-slate-200">{f.v || "—"}</p>
            </div>
          ))}
        </div>
      </section>

      {/* ALERTS & MANAGERS */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
        <section className="space-y-4">
          <h2 className="text-[10px] font-black text-white uppercase tracking-[0.3em] flex items-center gap-2 text-amber-500">
            <AlertTriangle className="w-3 h-3" /> Risk Signals
          </h2>
          <div className="bg-slate-950/50 border border-slate-800 rounded-2xl divide-y divide-slate-800/50 overflow-hidden">
            {alerts.length === 0 ? (
              <p className="p-8 text-center text-[10px] font-bold text-slate-600 uppercase italic tracking-widest">No Active Signals</p>
            ) : (
              alerts.map((a, i) => (
                <div key={i} className="p-4 flex justify-between items-center group hover:bg-slate-800/30 transition-colors">
                  <div className="flex items-center gap-4">
                    <span className={`px-2 py-0.5 rounded text-[8px] font-black border ${getSeverityClass(a.severity)}`}>{a.severity}</span>
                    <span className="text-xs font-bold text-slate-300">{capitalizeWords(a.type)}</span>
                  </div>
                  <span className="text-[10px] font-mono text-slate-600">{a.time}</span>
                </div>
              ))
            )}
          </div>
        </section>

        <section className="space-y-4">
          <h2 className="text-[10px] font-black text-white uppercase tracking-[0.3em] flex items-center gap-2">
            <BarChart3 className="w-3 h-3 text-blue-500" /> Manager Ownership
          </h2>
          <div className="bg-slate-950/50 border border-slate-800 rounded-2xl overflow-hidden">
            <table className="w-full text-left">
              <thead>
                <tr className="bg-slate-900/50">
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Manager</th>
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Exposure</th>
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">P&L</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50 text-xs font-mono">
                {managers.map((m, i) => (
                  <tr key={i} className="hover:bg-slate-800/30 transition-colors">
                    <td className="p-4 font-bold text-slate-300 uppercase tracking-tighter">
                      <Link href={`/dashboard/equity/intraday/ticker/${ticker}/manager/${m.manager}`}>{m.manager}</Link>
                    </td>
                    <td className="p-4">
                      <FlashValue value={m.exposure}>{formatCurrency(m.exposure)}</FlashValue>
                    </td>
                    <td className="p-4 text-right">
                      <FlashValue value={m.pnl} className="font-bold">{formatCurrency(m.pnl)}</FlashValue>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      <style jsx global>{`
        @keyframes flash-green { 
          0% { background-color: rgba(16, 185, 129, 0.2); color: #10b981; } 
          100% { background-color: transparent; } 
        }
        @keyframes flash-red { 
          0% { background-color: rgba(239, 68, 68, 0.2); color: #ef4444; } 
          100% { background-color: transparent; } 
        }
        .animate-flash-green { animation: flash-green 1s ease-out; }
        .animate-flash-red { animation: flash-red 1s ease-out; }
      `}</style>
    </main>
  );
}


function LoadingState() {
  return (
     <div className="p-8 bg-[#020617] min-h-screen flex flex-col gap-6 font-mono">
      <div className="flex justify-between items-center border-b border-slate-800 pb-6">
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <div className="w-3 h-3 bg-blue-500 animate-ping rounded-full" />
            <span className="text-blue-500 font-black uppercase tracking-[0.3em] text-xl">Intraday Engine</span>
          </div>
          <div className="h-4 w-64 bg-slate-900 animate-pulse rounded border border-slate-800" />
        </div>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {[1, 2, 3, 4].map((i) => (
          <div key={i} className="h-32 bg-[#0a0f1d] border border-slate-800 rounded-2xl p-4 animate-pulse" />
        ))}
      </div>
      <div className="flex-1 bg-[#0a0f1d] border border-slate-800 rounded-3xl flex items-center justify-center">
        <div className="text-slate-800 text-[10px] uppercase tracking-[0.5em] font-black animate-pulse">
          Establishing WebSocket Connection...
        </div>
      </div>
    </div>
  );
}