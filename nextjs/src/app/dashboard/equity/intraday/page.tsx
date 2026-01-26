"use client";

import React, { useState, useEffect } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import ManagerSearch from "@/components/ManagerSearch";
import { useWebSocket } from "@/hooks/useWebSocket";
import {
  Activity, TrendingUp, AlertTriangle, Clock,
  ArrowUpRight, ArrowDownRight
} from "lucide-react";

/* --- FORMATTERS --- */
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

/* --- MAIN PAGE --- */
export default function IntradayEquityOverview() {
  const [summary, setSummary] = useState<any>(null);
  const [topMovers, setTopMovers] = useState<any[]>([]);
  const [alerts, setAlerts] = useState<any[]>([]);
  const [tickers, setTickers] = useState<any[]>([]);
  const [managers, setManagers] = useState<any[]>([]);
  const [timeStamp, setTimestamp] = useState("");

  const [equityEnabled, setEquityEnabled] = useState(false);
  const [wsBaseUrl, setWsBaseUrl] = useState<string | null>(null);

  const updateState = (data: any) => {
    if (!data) return;
    setSummary(data.totals);
    setTopMovers(data.top_movers);
    setAlerts(data.active_alerts || []);
    setTimestamp(data.timestamp);
    setTickers(data.top_tickers_agg);
    setManagers(data.top_managers_agg);
  };

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
    const CLOSE_MIN = 16 * 60 + 2; // 16:02 ET

    if (weekday === "Sat" || weekday === "Sun") return false;
    return totalMin >= 570 && totalMin <= CLOSE_MIN; // 9:30 → 16:02 ET
  }

  // Fetch config and initial data
  useEffect(() => {
    async function fetchConfigAndData() {
      try {
        const res = await fetch("/api/config");
        const config = await res.json();

        const enabled = config.forceStream || isMarketTradingTime();
        setEquityEnabled(enabled);

        if (!enabled) return;

        setWsBaseUrl(config.wsBaseUrl);

        const dataRes = await fetch("/api/equity/intraday/overview", { cache: "no-store" });
        if (dataRes.ok) updateState(await dataRes.json());
      } catch (e) {
        console.error("Fetch failed", e);
      }
    }

    fetchConfigAndData();
  }, []);

  // --- USE WEBSOCKET HOOK ---
  const wsUrl = wsBaseUrl ? `${wsBaseUrl}/equity/overview/` : null;
  useWebSocket(wsUrl, equityEnabled, updateState);

  if (!equityEnabled) {
    return (
      <div className="min-h-screen bg-[#020617] flex items-center justify-center p-12 text-center">
        <div className="space-y-4 max-w-md">
          <Clock className="w-12 h-12 text-slate-700 mx-auto" />
          <h2 className="text-xl font-black text-white uppercase tracking-tight">Market Closed</h2>
          <p className="text-slate-400 text-sm leading-relaxed">U.S. equity markets operate Mon-Fri, 9:30 AM to 4:00 PM (EST).</p>
          <Link href="/dashboard/equity/daily" className="inline-block mt-4 text-blue-500 text-[10px] font-black uppercase tracking-widest border border-blue-500/30 px-6 py-2 rounded-lg hover:bg-blue-500/10 transition">
            ← View Daily Equity Data
          </Link>
        </div>
      </div>
    );
  }

  if (!summary) return <LoadingState />;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-12 pb-24">
      
      {/* RESTORED SEARCH HEADER - Side-by-side and clean */}
      <header className="flex flex-col lg:flex-row justify-between items-start lg:items-center gap-6 border-b border-slate-800/60 pb-10">
        <div className="space-y-3">
          <div className="flex items-center gap-3">
            <div className="px-2 py-1 bg-emerald-500/10 border border-emerald-500/20 rounded flex items-center gap-2">
              <div className="w-1.5 h-1.5 bg-emerald-500 rounded-full animate-pulse shadow-[0_0_8px_#10b981]" />
              <span className="text-[9px] font-black text-emerald-500 uppercase tracking-widest">Live Feed</span>
            </div>
            <span className="text-[9px] font-mono font-black text-slate-600 uppercase tracking-widest">
              TS: {new Date(timeStamp).toLocaleTimeString()}
            </span>
          </div>
          <h1 className="text-4xl font-black text-white tracking-tighter uppercase italic leading-none">
            Equity <span className="text-blue-500">Intraday</span>
          </h1>
        </div>

     <section className="sticky top-4 lg:relative lg:top-0 z-[999] flex flex-col lg:flex-row gap-4 w-full lg:w-[600px] lg:bg-slate-900/40 lg:p-3 lg:rounded-2xl lg:border lg:border-slate-800 lg:backdrop-blur-md">
    
    {/* Ticker Search Wrapper */}
    <div className="w-full lg:flex-1">
      <label className="lg:hidden text-[9px] font-black text-slate-500 uppercase mb-1 block px-1">Ticker Lookup</label>
      <div className="w-full [&>div]:w-full [&_input]:w-full relative [&_input]:cursor-text">
        <TickerSearch ticker_url="equity/intraday" />
      </div>
    </div>

    {/* Manager Search Wrapper */}
    <div className="w-full lg:flex-1">
      <label className="lg:hidden text-[9px] font-black text-slate-500 uppercase mb-1 block px-1">Manager Profile</label>
      <div className="w-full [&>div]:w-full [&_input]:w-full relative [&_input]:cursor-text">
        <ManagerSearch manager_url="equity/intraday" />
      </div>
    </div>

  </section>
      </header>

      {/* METRICS ROW */}
      <section className="grid grid-cols-2 lg:grid-cols-4 gap-6">
        <MetricCard label="Net Exposure" value={fmtCur(summary.total_exposure)} numericValue={summary.total_exposure} subValue="Portfolio Value" />
        <MetricCard label="Session P&L" value={fmtCur(summary.intraday_pnl)} numericValue={summary.intraday_pnl} subValue="Unrealized Delta" />
        <MetricCard label="Active Streams" value={summary.tickers_streaming} numericValue={summary.tickers_streaming} subValue="Tickers Online" />
        <MetricCard label="Managed Desk" value={summary.active_managers} numericValue={summary.active_managers} subValue="Managers Active" />
      </section>

      {/* VOLATILITY OVERVIEW */}
      <section className="space-y-6">
        <div className="flex items-center gap-3 border-l-4 border-blue-600 pl-4">
          <TrendingUp className="w-5 h-5 text-blue-500" />
          <h2 className="text-sm font-black text-white uppercase tracking-[0.2em]">Volatility Overview</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {topMovers.map((t: any) => (
            <TickerCard key={t.ticker + t.asset_manager} t={t} />
          ))}
        </div>
      </section>

      {/* CONSOLIDATED TICKER EXPOSURE */}
      <section className="space-y-4">
        <div className="flex items-center gap-3 border-l-4 border-emerald-500 pl-4">
          <Activity className="w-5 h-5 text-emerald-500" />
          <h2 className="text-sm font-black text-white uppercase tracking-[0.2em]">Consolidated Ticker Exposure</h2>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-3">
          {tickers.map((t: any) => (
            <Link key={t.ticker} href={`/dashboard/equity/intraday/ticker/${t.ticker}`} className="p-4 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-emerald-500/50 transition-all group">
              <div className="flex justify-between items-start mb-3">
                <span className="text-lg font-black text-white italic tracking-tighter group-hover:text-emerald-400">{t.ticker}</span>
                <span className={`text-[10px] font-mono font-bold ${t.total_pnl >= 0 ? 'text-emerald-500' : 'text-rose-500'}`}>
                  <FlashValue value={t.total_pnl}>{t.total_pnl >= 0 ? '▲' : '▼'}</FlashValue>
                </span>
              </div>
              <p className="text-[8px] font-black text-slate-500 uppercase">Net Position</p>
              <div className="text-sm font-mono font-bold text-slate-200">
                <FlashValue value={t.total_exposure}>{fmtCur(t.total_exposure)}</FlashValue>
              </div>
            </Link>
          ))}
        </div>
      </section>

      {/* MANAGER DISTRIBUTION */}
      <section className="space-y-4">
        <div className="flex items-center gap-3 border-l-4 border-blue-500 pl-4">
          <UserCheck className="w-5 h-5 text-blue-500" />
          <h2 className="text-sm font-black text-white uppercase tracking-[0.2em]">Asset Manager Distribution</h2>
        </div>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-3">
          {managers.map((m: any) => (
            <Link key={m.manager} href={`/dashboard/equity/intraday/manager/${encodeURIComponent(m.manager)}`} className="p-4 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-blue-500/50 transition-all group">
              <div className="flex justify-between items-start mb-2">
                <span className="text-[10px] font-black text-slate-200 uppercase truncate pr-2 group-hover:text-blue-400">{m.manager}</span>
                <span className="px-1.5 py-0.5 bg-blue-500/10 rounded text-[8px] font-black text-blue-400">{m.ticker_count} UNITS</span>
              </div>
              <div className="mt-4">
                <p className="text-[8px] font-black text-slate-500 uppercase">Controlled Allocation</p>
                <div className="text-sm font-mono font-bold text-blue-400">
                  <FlashValue value={m.total_exposure}>{fmtCur(m.total_exposure)}</FlashValue>
                </div>
              </div>
            </Link>
          ))}
        </div>
      </section>

      {/* RISK VIOLATIONS TABLE */}
      <section className="space-y-6">
        <div className="flex items-center justify-between border-l-4 border-amber-600 pl-4">
          <div className="flex items-center gap-3">
            <AlertTriangle className="w-5 h-5 text-amber-500" />
            <h2 className="text-sm font-black text-white uppercase tracking-[0.2em]">Risk Violations</h2>
          </div>
          <div className="flex sm:hidden items-center gap-1.5 px-2 py-0.5 bg-amber-500/10 border border-amber-500/20 rounded-full animate-pulse">
            <ChevronRight className="w-2.5 h-2.5 text-amber-500" />
            <span className="text-[7px] font-black text-amber-500 uppercase">Swipe to Audit</span>
          </div>
        </div>

        <div className="bg-slate-900/30 border border-slate-800 rounded-[2rem] overflow-hidden backdrop-blur-xl shadow-2xl">
          <div className="overflow-x-auto no-scrollbar relative">
            <table className="w-full text-left min-w-[850px] border-separate border-spacing-0">
              <thead>
                <tr className="bg-slate-950/80 border-b border-slate-800">
                  <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest sticky left-0 bg-slate-950 z-30">Priority</th>
                  <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest">Ticker</th>
                  <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest">Manager</th>
                  <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest">Violation Detail</th>
                  <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest">Time</th>
                  <th className="p-6 text-[10px] font-black text-slate-500 uppercase tracking-widest text-right">Review</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/40">
                {alerts.map((a: any, i: number) => (
                  <tr key={i} className="hover:bg-slate-800/40 transition-all group">
                    <td className="p-6 sticky left-0 bg-[#020617] group-hover:bg-slate-900 z-10 border-r border-slate-800/50">
                      <FlashValue value={a.severity}>
                        <span className={`text-[9px] font-black px-3 py-1 rounded-md border uppercase tracking-wider ${getSeverityClass(a.severity)}`}>
                          {a.severity}
                        </span>
                      </FlashValue>
                    </td>
                    <td className="p-6 font-mono font-bold text-white text-base">{a.ticker}</td>
                    <td className="p-6 text-[10px] font-black text-slate-400 uppercase">{a.manager}</td>
                    <td className="p-6 text-xs font-bold text-slate-300 italic">{a.type}</td>
                    <td className="p-6 text-[10px] font-mono text-slate-500">{a.time}</td>
                    <td className="p-6 text-right">
                      <Link href={`/dashboard/equity/intraday/ticker/${a.ticker}`} className="inline-flex items-center gap-2 text-[10px] font-black text-blue-500 uppercase group-hover:translate-x-1 transition-transform">
                        Analyze <ExternalLink className="w-3.5 h-3.5" />
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>
    </main>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex items-center justify-center">
      <div className="text-center space-y-4">
        <Activity className="w-10 h-10 text-blue-500 animate-spin mx-auto" />
        <p className="text-[10px] font-black text-slate-600 uppercase tracking-[0.5em]">Syncing Terminal...</p>
      </div>
    </div>
  );
}