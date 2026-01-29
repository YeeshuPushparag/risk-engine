"use client";

import { useState, useEffect, useRef } from "react";
import Link from "next/link";
import {
  Activity,
  Clock,
  TrendingUp,
  TrendingDown,
  Globe,
  MoveHorizontal,
} from "lucide-react";
import TickerSearch from "@/components/TickerSearch";
import { useWebSocket } from "@/hooks/useWebSocket";

/* --- FORMATTERS --- */
const fmt = (v?: number) => {
  if (v == null) return "-";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
};

const fmtFX = (n?: number) => (n == null ? "—" : n.toFixed(4));

/* --- ANIMATED COMPONENTS --- */

function StatCard({ label, value, numericValue, trend, color }: any) {
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
    <div
      className={`bg-slate-900 border border-slate-800 p-5 rounded-2xl transition-all duration-500 ${flash}`}
    >
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-3">
        {label}
      </p>
      <div className="flex items-end justify-between">
        <h3
          className={`text-xl sm:text-2xl font-black tracking-tighter ${
            color || "text-white"
          }`}
        >
          {value}
        </h3>
        {trend && (
          <span
            className={
              trend === "up" ? "text-emerald-500" : "text-red-500"
            }
          >
            {trend === "up" ? (
              <TrendingUp className="w-5 h-5" />
            ) : (
              <TrendingDown className="w-5 h-5" />
            )}
          </span>
        )}
      </div>
    </div>
  );
}

function FlashCell({ value, children, className = "" }: any) {
  const [flash, setFlash] = useState("");
  const prevValue = useRef(value);

  useEffect(() => {
    if (value !== prevValue.current) {
      setFlash(
        value > prevValue.current
          ? "bg-emerald-500/10"
          : "bg-red-500/10"
      );
      prevValue.current = value;

      const timer = setTimeout(() => setFlash(""), 800);
      return () => clearTimeout(timer);
    }
  }, [value]);

  return (
    <td
      className={`${className} ${flash} transition-colors duration-500`}
    >
      {children}
    </td>
  );
}

/* --- MAIN PAGE --- */

export default function FxIntradayMain() {
  const [data, setData] = useState<any>(null);
  const [wsUrl, setWsUrl] = useState<string | null>(null);
  const [fxEnabled, setFxEnabled] = useState<boolean | null>(null);

  const handleDataUpdate = (update: any) => {
    setData(update);
  };

  useEffect(() => {
    async function fetchConfig() {
      try {
        const res = await fetch("/api/config");
        const config = await res.json();

        const enabled = config.forceStream || isFXTradingTime();
        setFxEnabled(enabled);

        if (!enabled) return;

        setWsUrl(config.wsBaseUrl + "/fx/overview/");

        const initialRes = await fetch(
          "/api/fx/intraday/overview",
          { cache: "no-store" }
        );

        if (initialRes.ok) {
          const initialData = await initialRes.json();
          handleDataUpdate(initialData);
        }
      } catch (e) {
        console.error("Failed to load config or data:", e);
      }
    }

    fetchConfig();
  }, []);

  useWebSocket(wsUrl, handleDataUpdate);

  if (fxEnabled === null) {
    return <LoadingTerminal />;
  }

  if (fxEnabled === false) {
    return <MarketClosedView />;
  }

  if (!data) {
    return <LoadingTerminal />;
  }

    return (
      <main className="min-h-screen bg-[#020617] text-slate-200 p-6 lg:p-10 font-sans overflow-x-hidden">

        {/* HEADER */}
        <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-10 gap-6">
          <div className="space-y-3">
            <div className="flex items-center gap-3">
              <div className="bg-blue-600 p-1.5 rounded shadow-lg shadow-blue-900/20">
                <Globe className="w-5 h-5 text-white" />
              </div>
              <h1 className="text-xl sm:text-2xl font-black tracking-tighter text-white uppercase italic">
                FX Risk <span className="text-blue-500">Intraday</span>
              </h1>
            </div>
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-2 px-2 py-1 bg-slate-900 rounded-md border border-slate-800">
                <div className="w-2 h-2 rounded-full animate-pulse bg-emerald-500" />
                <span className="text-[9px] font-black uppercase tracking-widest text-slate-400">Live Engine</span>
              </div>
              <span className="text-[9px] font-bold text-slate-500 uppercase flex items-center gap-1">
                <Clock className="w-3 h-3 text-blue-500" />
                {data?.timestamp ? new Date(data.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' }) : "SYNCING"}
              </span>
            </div>
          </div>

          <section className="w-full lg:max-w-[500px]">
            <TickerSearch ticker_url="fx/daily" />
          </section>
        </div>

        {/* STATS */}
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4 mb-10">
          <StatCard label="Total Exposure" value={fmt(data.totals.total_exposure)} numericValue={data.totals.total_exposure} />
          <StatCard
            label="Session P&L"
            value={fmt(data.totals.total_fx_pnl)}
            numericValue={data.totals.total_fx_pnl}
            trend={data.totals.total_fx_pnl >= 0 ? "up" : "down"}
          />
          <StatCard label="Worst VaR" value={fmt(data.totals.worst_var_95)} numericValue={data.totals.worst_var_95} color="text-red-400" />
          <StatCard label="Active Pairs" value={data.totals.active_currency_pairs} numericValue={data.totals.active_currency_pairs} />
        </div>

        <div className="grid grid-cols-1 xl:grid-cols-3 gap-8">
          {/* LEFT: POSITION TABLE */}
          <div className="xl:col-span-2 space-y-4">
            <div className="flex items-center justify-between px-2">
              <h2 className="text-[10px] font-black text-slate-500 uppercase tracking-[0.3em]">Watchlist</h2>
              <div className="lg:hidden flex items-center gap-2 text-[8px] font-bold text-blue-400 uppercase animate-pulse">
                <MoveHorizontal className="w-3 h-3" /> Scroll
              </div>
            </div>

            <div className="bg-slate-900/50 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm">
              <div className="overflow-x-auto scrollbar-hide">
                <table className="w-full text-left min-w-[600px]">
                  <thead>
                    <tr className="bg-slate-800/30 border-b border-slate-800">
                      <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Instrument</th>
                      <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">Price</th>
                      <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">P&L</th>
                      <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">95% VaR</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-800/50">
                    {data.top_tickers.map((row: any) => (
                      <tr key={row.ticker} className="group hover:bg-slate-800/40 transition-colors">
                        <td className="p-4">
                          <Link href={`/dashboard/fx/intraday/ticker/${row.ticker}`} className="block">
                            <span className="text-sm font-black text-white group-hover:text-blue-400 transition-colors">{row.ticker}</span>
                            <span className="block text-[10px] font-bold text-slate-600">{row.currency_pair}</span>
                          </Link>
                        </td>
                        <FlashCell value={row.close} className="p-4 text-right font-mono text-sm font-medium text-slate-300">
                          {fmtFX(row.close)}
                        </FlashCell>
                        <FlashCell value={row.fx_pnl} className={`p-4 text-right text-sm font-black ${row.fx_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                          {fmt(row.fx_pnl)}
                        </FlashCell>
                        <td className="p-4 text-right text-sm font-bold text-slate-400">
                          {fmt(row.VaR_95_15m)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          {/* RIGHT: AGGREGATES */}
          <div className="space-y-4">
            <h2 className="text-[10px] font-black text-slate-500 uppercase tracking-[0.3em] px-2">Currency Groups</h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-1 gap-3">
              {Object.entries(data.currency_summary).map(([ccy, vals]: any) => (
                <Link key={ccy} href={`/dashboard/fx/intraday/currency/${ccy}`} className="block p-4 bg-slate-900 border border-slate-800 rounded-xl hover:border-blue-500/50 transition-all group">
                  <div className="flex justify-between items-center mb-2">
                    <span className="text-sm font-black text-white group-hover:text-blue-400">{ccy}</span>
                    <span className={`text-xs font-black ${vals.total_fx_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {fmt(vals.total_fx_pnl)}
                    </span>
                  </div>
                  <div className="flex justify-between items-center text-[9px] font-bold">
                    <span className="text-slate-500 uppercase">{vals.ticker_count} Assets</span>
                    <span className="text-slate-400 uppercase">Exp: {fmt(vals.total_exposure)}</span>
                  </div>
                </Link>
              ))}
            </div>
          </div>
        </div>

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

  /* --- UTILS & SUB-VIEWS --- */

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



  function LoadingTerminal() {
    return (
      <div className="h-screen bg-[#020617] flex flex-col items-center justify-center">
        <Activity className="w-10 h-10 text-blue-500 animate-spin mb-4" />
        <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.4em]">Establishing FX Feed</p>
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
            The FX market operates from Sunday 5:00 PM to Friday 5:00 PM (ET).
          </p>
          <Link href="/dashboard/fx/daily" className="inline-block mt-4 text-blue-500 text-[10px] font-black uppercase tracking-widest border border-blue-500/30 px-6 py-2 rounded-lg hover:bg-blue-500/10 transition">
            ← View Daily Overview
          </Link>
        </div>
      </div>
    );
  }