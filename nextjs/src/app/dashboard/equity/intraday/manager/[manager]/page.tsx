"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import React, { useEffect, useState, useRef } from "react";
import { useWebSocket } from "@/hooks/useWebSocket";
import { 
  ArrowLeft, Activity, AlertTriangle, 
  ExternalLink, Clock, MoveHorizontal 
} from "lucide-react";

/* ---------------- FORMATTERS ---------------- */
const currencyFormatter = new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", maximumFractionDigits: 2,
});

const fmtCur = (n?: number | null) => {
  if (n == null || Number.isNaN(n)) return "—";
  const abs = Math.abs(n);
  if (abs >= 1e12) return `${currencyFormatter.format(n / 1e12)}T`;
  if (abs >= 1e9) return `${currencyFormatter.format(n / 1e9)}B`;
  if (abs >= 1e6) return `${currencyFormatter.format(n / 1e6)}M`;
  if (abs >= 1e3) return `${currencyFormatter.format(n / 1e3)}K`;
  return currencyFormatter.format(n);
};

const fmtPct = (n?: number | null) => {
  if (n == null) return "—";
  const val = n * 100;
  return (
    <span className={val >= 0 ? "text-emerald-400" : "text-red-400"}>
      {val >= 0 ? "+" : ""}{val.toFixed(2)}%
    </span>
  );
};

function titleize(raw?: string) {
  if (!raw) return "";
  const s = decodeURIComponent(raw).replace(/[-_]+/g, " ");
  return s.split(" ").map((w) => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
}

/* ---------------- SEVERITY LOGIC ---------------- */
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

/* ---------------- ANIMATED COMPONENTS ---------------- */

function MetricCard({ label, value, numericValue }: any) {
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
    <div className={`bg-slate-900/40 border border-slate-800 p-4 sm:p-5 rounded-2xl backdrop-blur-md transition-all duration-500 ${flash}`}>
      <p className="text-[9px] sm:text-[10px] font-black text-slate-500 uppercase tracking-[0.2em] mb-2">{label}</p>
      <p className="text-lg sm:text-xl font-mono font-bold text-white tracking-tighter italic">{value}</p>
    </div>
  );
}

function FlashCell({ value, children, className = "" }: any) {
  const [flash, setFlash] = useState("");
  const prevValue = useRef(value);

  useEffect(() => {
    if (value !== prevValue.current) {
      setFlash(value > prevValue.current ? "bg-emerald-500/10" : "bg-red-500/10");
      prevValue.current = value;
      const timer = setTimeout(() => setFlash(""), 800);
      return () => clearTimeout(timer);
    }
  }, [value]);

  return <td className={`${className} ${flash} transition-colors duration-500`}>{children}</td>;
}

/* ---------------- MAIN PAGE ---------------- */

export default function ManagerIntradayPage() {
const params = useParams();
const rawManager = params.manager as string;
const managerName = titleize(rawManager);

const [summary, setSummary] = useState<any>(null);
const [holdings, setHoldings] = useState<any[]>([]);
const [alerts, setAlerts] = useState<any[]>([]);
const [timeStamp, setTimestamp] = useState("");

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
  if (!equityEnabled || !rawManager) return;
  async function load() {
    try {
      const res = await fetch(
        `/api/equity/intraday/manager?manager=${rawManager}`,
        { cache: "no-store" }
      );
      if (!res.ok) return;
      const json = await res.json();
      setSummary(json.totals);
      setHoldings(json.holdings);
      setAlerts(json.alerts || []);
      setTimestamp(json.timestamp);
    } catch (e) {
      console.error(e);
    }
  }
  load();
}, [rawManager, equityEnabled]);

useWebSocket(
  equityEnabled && wsBaseUrl && rawManager
    ? `${wsBaseUrl}/equity/manager/${rawManager}/`
    : null,
  (data) => {
    setSummary(data.totals);
    setHoldings(data.holdings);
    setAlerts(data.alerts || []);
    setTimestamp(data.timestamp);
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
          U.S. equity markets operate Mon–Fri, 9:30 AM to 4:00 PM (EST).
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

if (!summary) {
  return <LoadingState />;
}


  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-6 lg:p-12 space-y-12 overflow-x-hidden">
      
      {/* HEADER: RESPONSIVE TEXT */}
      <header className="flex flex-col md:flex-row justify-between items-start md:items-center gap-6 border-b border-slate-800/60 pb-10">
        <div className="space-y-3 w-full">
          <div className="flex flex-wrap items-center gap-3">
            <Link href="/dashboard/equity/intraday" className="p-2 bg-slate-900 border border-slate-800 rounded-lg hover:border-blue-500 transition-colors group">
              <ArrowLeft className="w-4 h-4 text-slate-400 group-hover:text-blue-500" />
            </Link>
            <div className="px-2 py-1 bg-blue-500/10 border border-blue-500/20 rounded flex items-center gap-2">
              <div className="w-1.5 h-1.5 bg-blue-500 rounded-full animate-pulse shadow-[0_0_8px_#3b82f6]" />
              <span className="text-[8px] sm:text-[9px] font-black text-blue-500 uppercase tracking-widest">Asset Manager</span>
            </div>
            <span className="text-[8px] sm:text-[9px] font-mono font-black text-slate-600 uppercase tracking-widest">
              TS: {timeStamp ? new Date(timeStamp).toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true }) : "INITIALIZING"}
            </span>
          </div>
          <h1 className="text-2xl sm:text-3xl md:text-4xl font-black text-white tracking-tighter uppercase italic leading-tight">
            {managerName} <span className="text-blue-500">Intraday</span>
          </h1>
        </div>
      </header>

      {/* SUMMARY METRICS: GRID */}
      <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 sm:gap-4">
        <MetricCard label="Exposure" value={fmtCur(summary.intraday_exposure)} numericValue={summary.intraday_exposure} />
        <MetricCard label="Portfolio P&L" value={fmtCur(summary.intraday_pnl)} numericValue={summary.intraday_pnl} />
        <MetricCard label="1m Return" value={fmtPct(summary.return_1m)} numericValue={summary.return_1m} />
        <MetricCard label="5m Return" value={fmtPct(summary.return_5m)} numericValue={summary.return_5m} />
        <MetricCard label="15m Vol" value={fmtPct(summary.vol_15m)} numericValue={summary.vol_15m} />
        <MetricCard label="Violations" value={alerts.length} numericValue={alerts.length} />
      </section>

      {/* HOLDINGS TABLE: WITH SCROLL INDICATOR */}
      <section className="space-y-6">
        <div className="flex flex-col sm:flex-row sm:items-center justify-between border-l-4 border-blue-600 pl-4 gap-2">
          <div className="flex items-center gap-3">
            <Activity className="w-5 h-5 text-blue-500" />
            <h2 className="text-xs sm:text-sm font-black text-white uppercase tracking-[0.2em]">Intraday Holdings</h2>
          </div>
          <div className="flex items-center gap-4">
             <div className="lg:hidden flex items-center gap-2 text-[8px] font-bold text-blue-400 uppercase animate-pulse">
                <MoveHorizontal className="w-3 h-3" /> Scroll Table
             </div>
             <span className="text-[10px] font-bold text-slate-500 uppercase tracking-widest">{summary.holdings_count} Assets</span>
          </div>
        </div>

        <div className="bg-slate-900/20 border border-slate-800 rounded-3xl overflow-hidden backdrop-blur-md">
          <div className="overflow-x-auto scrollbar-hide">
            <table className="w-full text-left border-collapse min-w-[700px]">
              <thead>
                <tr className="bg-slate-950/50">
                  <th className="p-5 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">Ticker</th>
                  <th className="p-5 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">Exposure</th>
                  <th className="p-5 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">Intraday P&L</th>
                  <th className="p-5 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">1m Ret</th>
                  <th className="p-5 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">5m Ret</th>
                  <th className="p-5 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">15m Vol</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/40">
                {holdings.map((h) => (
                  <tr key={h.ticker} className="hover:bg-slate-800/30 transition-colors group">
                    <td className="p-5 font-mono font-bold text-white tracking-tighter">
                      <Link href={`/dashboard/equity/intraday/ticker/${h.ticker}/manager/${rawManager}`} className="hover:text-blue-500 transition-colors">
                        {h.ticker}
                      </Link>
                    </td>
                    <FlashCell value={h.intraday_exposure} className="p-5 text-sm font-mono text-slate-300">{fmtCur(h.intraday_exposure)}</FlashCell>
                    <FlashCell value={h.portfolio_intraday_pnl} className="p-5 text-sm font-mono text-slate-300">{fmtCur(h.portfolio_intraday_pnl)}</FlashCell>
                    <FlashCell value={h.return_1m} className="p-5 text-xs font-black">{fmtPct(h.return_1m)}</FlashCell>
                    <FlashCell value={h.return_5m} className="p-5 text-xs font-black">{fmtPct(h.return_5m)}</FlashCell>
                    <FlashCell value={h.vol_15m} className="p-5 text-xs font-black text-slate-400 font-mono">{fmtPct(h.vol_15m)}</FlashCell>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* RISK VIOLATIONS */}
      <section className="space-y-6 pt-6">
        <div className="flex items-center gap-3 border-l-4 border-amber-600 pl-4">
          <AlertTriangle className="w-5 h-5 text-amber-500" />
          <h2 className="text-xs sm:text-sm font-black text-white uppercase tracking-[0.2em]">Risk Violations</h2>
        </div>

        <div className="bg-slate-900/30 border border-slate-800 rounded-3xl overflow-hidden backdrop-blur-xl shadow-2xl">
          {alerts.length === 0 ? (
            <div className="p-16 text-center text-[10px] font-black text-slate-600 uppercase tracking-[0.4em] italic">No active violations detected</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left min-w-[800px]">
                <thead>
                  <tr className="bg-slate-950/80">
                    <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">Priority</th>
                    <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">Ticker</th>
                    <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">Violation Detail</th>
                    <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800">Time (EST)</th>
                    <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest border-b border-slate-800 text-right">Review</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/40">
                  {alerts.map((a: any, i: number) => (
                    <tr key={i} className="hover:bg-slate-800/40 transition-all group">
                      <td className="p-6">
                        <span className={`text-[9px] font-black px-3 py-1 rounded-md border uppercase tracking-wider ${getSeverityClass(a.severity)}`}>
                          {a.severity}
                        </span>
                      </td>
                      <td className="p-6 font-mono font-bold text-white tracking-tighter text-base">{a.ticker}</td>
                      <td className="p-6 text-xs font-bold text-slate-300 italic tracking-tight">{a.type}</td>
                      <td className="p-6 text-[10px] font-mono text-slate-500 tracking-widest">{a.time}</td>
                      <td className="p-6 text-right">
                        <Link href={`/dashboard/equity/intraday/ticker/${a.ticker}/manager/${rawManager}`} className="inline-flex items-center gap-2 text-[10px] font-black text-blue-500 hover:text-white transition-all uppercase tracking-[0.2em] group-hover:translate-x-1">
                          Analyze <ExternalLink className="w-3.5 h-3.5" />
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>

      <style jsx global>{`
        @keyframes flash-green { 0% { background: rgba(16, 185, 129, 0.2); } 100% { background: transparent; } }
        @keyframes flash-red { 0% { background: rgba(239, 68, 68, 0.2); } 100% { background: transparent; } }
        .animate-flash-green { animation: flash-green 1s ease-out; }
        .animate-flash-red { animation: flash-red 1s ease-out; }
        .scrollbar-hide::-webkit-scrollbar { display: none; }
        .scrollbar-hide { -ms-overflow-style: none; scrollbar-width: none; }
      `}</style>
    </main>
  );
}


function LoadingState() {
  return (
     <div className="p-8 bg-[#020617] min-h-screen flex items-center justify-center">
      <div className="text-blue-500 font-black animate-pulse tracking-widest text-xs uppercase italic">
        Synchronizing Manager Data...
      </div>
    </div>
  );
}
