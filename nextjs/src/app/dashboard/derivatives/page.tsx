"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import { 
  BarChart3, 
  ArrowUpRight, 
  Search, 
  Layers, 
  AlertTriangle, 
  Activity,
  ArrowRight
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type TickerRow = {
  ticker: string;
  notional: number;
  net_exposure: number;
  delta_eq: number;
  pnl: number;
  margin_calls: number;
};

type AssetRow = {
  asset_class: string;
  notional: number;
  net_exposure: number;
  pnl: number;
};

type DerivativesOverviewResponse = {
  date: string;
  summary: {
    total_notional: number;
    total_net_exposure: number;
    delta_equivalent: number;
    margin_call_amount: number;
    margin_call_count: number;
    total_pnl: number;
    margin_stress: boolean;
  };
  top_tickers: TickerRow[];
  by_asset_class: AssetRow[];
};

/* ---------------- FORMATTERS ---------------- */
const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(n);

const cleanID = (id: string) => id?.replace(/[_\s]/g, "").toUpperCase() || "—";

/* ---------------- MAIN COMPONENT ---------------- */
export default function DerivativesMainPage() {
  const [data, setData] = useState<DerivativesOverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/derivatives/overview", { cache: "no-store" });
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Derivatives load failed", e);
      }
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <LoadingScreen />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black">DATA FEED OFFLINE</div>;

  const { date, summary, top_tickers, by_asset_class } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-12">
      
      {/* HEADER */}
      <header className="flex flex-col lg:flex-row lg:items-end justify-between gap-8 border-b border-slate-800/50 pb-10">
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-indigo-500 font-black text-[10px] tracking-[0.4em] uppercase">
            <Activity className="w-4 h-4" /> Global Risk Control
          </div>
          <h1 className="text-4xl sm:text-7xl font-black text-white italic tracking-tighter uppercase leading-none">
            Derivatives <span className="text-slate-800">/</span> Firm
          </h1>
          <div className="flex items-center gap-4">
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              System Date: {new Date(date).toDateString().toUpperCase()}
            </p>
            {summary.margin_stress && (
              <div className="flex items-center gap-1 text-red-500 animate-pulse">
                <AlertTriangle className="w-3 h-3" />
                <span className="text-[9px] font-black uppercase tracking-tighter">Stress Active</span>
              </div>
            )}
          </div>
        </div>

        <div className="w-full lg:w-96 space-y-2">
          <div className="flex items-center gap-2 text-slate-500 px-1">
             <Search className="w-3 h-3" />
             <span className="text-[10px] font-black uppercase tracking-widest">Asset Lookup</span>
          </div>
          <TickerSearch ticker_url="derivatives" />
        </div>
      </header>

      {/* RISK SUMMARY TILES */}
      <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <MetricTile label="Total Notional" value={fmt(summary.total_notional)} />
        <MetricTile label="Net Exposure" value={fmt(summary.total_net_exposure)} />
        <MetricTile label="Delta-Eq" value={fmt(summary.delta_equivalent)} />
        <MetricTile 
            label="Calls Count" 
            value={summary.margin_call_count.toLocaleString()} 
            color="text-indigo-400"
        />
        <MetricTile 
            label="Call Amount" 
            value={fmt(summary.margin_call_amount)} 
            color="text-red-500" 
        />
        <MetricTile 
            label="Total P&L" 
            value={fmt(summary.total_pnl)} 
            color={summary.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"} 
        />
      </section>

      {/* TOP TICKERS TABLE */}
      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <BarChart3 className="w-5 h-5 text-indigo-500" />
            <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">High Concentration Tickers</h2>
          </div>
          <ScrollAlert />
        </div>
        
        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[800px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Ticker", "Notional", "Net Exposure", "Delta Eq", "P&L", "Calls"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {top_tickers.map((t, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      <Link 
                        href={`/dashboard/derivatives/ticker/${encodeURIComponent(t.ticker)}`}
                        className="flex items-center gap-2 font-black text-indigo-400 group-hover:text-white transition-colors italic tracking-tighter"
                      >
                        {cleanID(t.ticker)} <ArrowUpRight className="w-3 h-3 opacity-0 group-hover:opacity-100" />
                      </Link>
                    </td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.notional)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.net_exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.delta_eq)}</td>
                    <td className={`px-6 py-4 text-xs font-mono font-bold ${t.pnl < 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                      {fmt(t.pnl)}
                    </td>
                    <td className="px-6 py-4 text-xs font-mono text-slate-500">{t.margin_calls}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* ASSET CLASS SUMMARY */}
      <section className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <Layers className="w-5 h-5 text-indigo-500" />
            <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Asset Class Distribution</h2>
          </div>
          <ScrollAlert />
        </div>

        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[600px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Asset Class", "Notional", "Net Exposure", "Total P&L"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {by_asset_class.map((a, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors">
                    <td className="px-6 py-4 text-xs font-black text-white uppercase tracking-tighter italic">{a.asset_class}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(a.notional)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(a.net_exposure)}</td>
                    <td className={`px-6 py-4 text-xs font-mono font-bold ${a.pnl < 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                      {fmt(a.pnl)}
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

/* ---------------- UI HELPERS ---------------- */

function MetricTile({ label, value, color = "text-white" }: any) {
  return (
    <div className="p-6 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-slate-600 transition-all">
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-1">{label}</p>
      <p className={`text-xl font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function ScrollAlert() {
  return (
    <div className="lg:hidden flex items-center gap-2 bg-indigo-500/10 px-2 py-1 rounded border border-indigo-500/20">
      <span className="text-[8px] font-black text-indigo-400 uppercase tracking-tighter">Swipe to View</span>
      <div className="w-1 h-1 bg-indigo-500 rounded-full animate-ping" />
    </div>
  );
}

function LoadingScreen() {
    return (
        <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
            <div className="w-16 h-1 bg-slate-800 rounded-full overflow-hidden relative">
                <div className="absolute inset-0 bg-indigo-500 animate-[loading_1.5s_infinite_ease-in-out]" />
            </div>
            <p className="text-[10px] font-black text-slate-600 uppercase tracking-[0.5em]">Syncing Risk Nodes</p>
            <style jsx>{`
                @keyframes loading {
                    0% { transform: translateX(-100%); }
                    100% { transform: translateX(100%); }
                }
            `}</style>
        </div>
    );
}