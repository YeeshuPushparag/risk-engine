"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import { 
  ShieldCheck, 
  AlertCircle, 
  Activity, 
  TrendingUp, 
  Search, 
  Layers, 
  ArrowUpRight 
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type AssetClassRow = {
  asset_class: string;
  exposure: number;
  required: number;
  effective: number;
  net_exposure: number;
  avg_ratio: number;
  margin_stress: number;
};

type TickerRow = {
  ticker: string | null;
  counterparty: string | null;
  exposure: number;
  net_exposure: number;
  margin_call: number;
  stress: number;
};

type CollateralOverview = {
  date: string;
  summary: {
    total_exposure: number;
    total_net_exposure: number;
    total_required: number;
    total_effective: number;
    total_margin_call: number;
    margin_call_count: number;
    margin_stress: boolean;
  };
  asset_class_summary: AssetClassRow[];
  ticker_counterparty: TickerRow[];
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

const formatCP = (cp: string | null) => cp?.replace(/_/g, " ").toUpperCase() || "—";

/* ---------------- MAIN COMPONENT ---------------- */
export default function CollateralOverviewPage() {
  const [data, setData] = useState<CollateralOverview | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/collateral/overview", { cache: "no-store" });
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Collateral overview load failed", e);
      }
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <LoadingState />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-widest">Data Link Offline</div>;

  const { summary, asset_class_summary, ticker_counterparty } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-12">
      
      {/* HEADER */}
      <header className="flex flex-col lg:flex-row lg:items-end justify-between gap-8 border-b border-slate-800/50 pb-10">
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-indigo-500 font-black text-[10px] tracking-[0.4em] uppercase">
            <ShieldCheck className="w-4 h-4" /> Collateral Management
          </div>
          <h1 className="text-4xl sm:text-7xl font-black text-white italic tracking-tighter uppercase leading-none">
            Firm <span className="text-slate-800">/</span> Overview
          </h1>
          <div className="flex items-center gap-4">
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Terminal Date: {new Date(data.date).toDateString().toUpperCase()}
            </p>
            {summary.margin_stress && (
              <div className="flex items-center gap-1 text-red-500 animate-pulse">
                <AlertCircle className="w-3 h-3" />
                <span className="text-[9px] font-black uppercase tracking-tighter">Stress Detected</span>
              </div>
            )}
          </div>
        </div>

        <div className="w-full lg:w-96 space-y-2">
          <div className="flex items-center gap-2 text-slate-500 px-1">
             <Search className="w-3 h-3" />
             <span className="text-[10px] font-black uppercase tracking-widest">Collateral Search</span>
          </div>
          <TickerSearch ticker_url="collateral" />
        </div>
      </header>

      {/* METRICS GRID */}
      <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
        <MetricTile label="Total Exposure" value={fmt(summary.total_exposure)} />
        <MetricTile label="Net Exposure" value={fmt(summary.total_net_exposure)} />
        <MetricTile label="Required" value={fmt(summary.total_required)} color="text-yellow-500" />
        <MetricTile label="Effective" value={fmt(summary.total_effective)} color="text-emerald-500" />
        <MetricTile 
            label="Call Amount" 
            value={fmt(summary.total_margin_call)} 
            color="text-red-500" 
            highlight={summary.margin_stress}
        />
        <MetricTile 
            label="Calls Count" 
            value={summary.margin_call_count.toLocaleString()} 
            color="text-indigo-400"
        />
      </section>

      {/* ASSET CLASS TABLE */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-3">
            <Layers className="w-5 h-5 text-indigo-500" />
            <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Class Summary</h2>
          </div>
          <ScrollPill />
        </div>
        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[800px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Asset Class", "Exposure", "Required", "Effective", "Net Exp", "Ratio", "Stress"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {asset_class_summary.map((a, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors">
                    <td className="px-6 py-4 text-xs font-black text-white italic">{a.asset_class.toUpperCase()}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(a.exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(a.required)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-emerald-400">{fmt(a.effective)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(a.net_exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono font-bold text-indigo-400">{a.avg_ratio.toFixed(3)}</td>
                    <td className="px-6 py-4 text-xs">
                      <span className={`px-2 py-0.5 rounded text-[10px] font-black uppercase ${a.margin_stress > 0 ? 'bg-red-500/20 text-red-500 border border-red-500/20' : 'text-slate-600'}`}>
                        {a.margin_stress > 0 ? 'Warning' : 'Normal'}
                      </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* TICKER / COUNTERPARTY TABLE */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-3">
            <TrendingUp className="w-5 h-5 text-indigo-500" />
            <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Counterparty Exposure</h2>
          </div>
          <ScrollPill />
        </div>
        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[900px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Ticker", "Counterparty", "Exposure", "Net Exp", "Margin Call", "Stress Status"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {ticker_counterparty.map((r, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      {r.ticker ? (
                        <Link 
                          href={`/dashboard/collateral/ticker/${encodeURIComponent(r.ticker)}`}
                          className="flex items-center gap-2 font-black text-indigo-400 group-hover:text-white transition-colors italic tracking-tighter"
                        >
                          {cleanID(r.ticker)} <ArrowUpRight className="w-3 h-3 opacity-0 group-hover:opacity-100" />
                        </Link>
                      ) : "—"}
                    </td>
                    <td className="px-6 py-4 text-[10px] font-black text-slate-400 uppercase tracking-tighter italic">
                      {formatCP(r.counterparty)}
                    </td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(r.exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(r.net_exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono font-bold text-red-400">{fmt(r.margin_call)}</td>
                    <td className="px-6 py-4 text-xs">
                      <div className="flex items-center gap-2">
                        <div className={`w-2 h-2 rounded-full ${
                          r.stress > 0 
                            ? 'bg-red-500 animate-pulse shadow-[0_0_8px_rgba(239,68,68,0.8)]' 
                            : 'bg-emerald-500 opacity-40'
                        }`} />
                        <span className="text-[10px] font-bold text-slate-500 uppercase">
                          {r.stress > 0 ? 'Stressed' : 'Stable'}
                        </span>
                      </div>
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

function MetricTile({ label, value, color = "text-white", highlight = false }: any) {
  return (
    <div className={`p-6 bg-slate-900/50 border ${highlight ? 'border-red-500/50 bg-red-500/5' : 'border-slate-800'} rounded-2xl hover:border-slate-600 transition-all`}>
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-1">{label}</p>
      <p className={`text-xl font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function ScrollPill() {
  return (
    <div className="lg:hidden flex items-center gap-2 bg-indigo-500/10 px-2 py-1 rounded border border-indigo-500/20">
      <span className="text-[8px] font-black text-indigo-400 uppercase tracking-tighter">Swipe to Pan</span>
      <div className="w-1 h-1 bg-indigo-500 rounded-full animate-ping" />
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <Activity className="w-8 h-8 text-indigo-500 animate-pulse" />
      <p className="text-[10px] font-black text-slate-600 uppercase tracking-[0.5em]">Aggregating Collateral Pools</p>
    </div>
  );
}