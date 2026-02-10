"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { 
  ChevronRight, 
  Activity, 
  TrendingDown, 
  Zap, 
  AlertTriangle, 
  BarChart3, 
  ArrowLeft,
  ShieldAlert
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type AssetSummary = {
  asset_class: string;
  notional: number;
  net_exposure: number;
  delta_eq: number;
  margin_stress: number;
};

type DerivativesTickerResponse = {
  date: string;
  ticker: string;
  summary: {
    total_notional: number;
    total_net_exposure: number;
    delta_equivalent: number;
    margin_call_amount: number;
    total_pnl: number;
    margin_call_count: number;
    margin_stress: boolean;
  };
  asset_class_summary: AssetSummary[];
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
export default function DerivativesTickerPage() {
  const { ticker } = useParams();
  const T = decodeURIComponent(String(ticker));

  const [data, setData] = useState<DerivativesTickerResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(
          `/api/derivatives/ticker?ticker=${encodeURIComponent(T)}`,
          { cache: "no-store" }
        );
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Failed to load ticker derivatives", e);
      }
      setLoading(false);
    }
    load();
  }, [T]);

  if (loading) return <LoadingState id={cleanID(T)} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black">NODE OFFLINE</div>;

  const { summary, asset_class_summary, date } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-10">
      
      {/* BREADCRUMB & HEADER */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/derivatives" className="hover:text-indigo-400">Derivatives</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">{cleanID(T)}</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <div className="space-y-1">
            <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
              Asset <span className="text-indigo-500">:</span> {cleanID(T)}
            </h1>
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Data Updated: {new Date(date).toDateString().toUpperCase()}
            </p>
          </div>
          
          {summary.margin_stress && (
            <div className="flex items-center gap-2 px-4 py-2 bg-red-500/10 border border-red-500/20 rounded-xl">
              <ShieldAlert className="w-4 h-4 text-red-500 animate-pulse" />
              <span className="text-[10px] font-black text-red-500 uppercase tracking-widest">Active Margin Stress</span>
            </div>
          )}
        </div>
      </header>

      {/* RISK SUMMARY GRID */}
      <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 sm:gap-4">
        <MetricBox label="Notional" value={fmt(summary.total_notional)} icon={<Activity className="w-3 h-3 text-indigo-500" />} />
        <MetricBox label="Net Exposure" value={fmt(summary.total_net_exposure)} />
        <MetricBox label="Delta-Eq" value={fmt(summary.delta_equivalent)} />
        <MetricBox 
          label="Margin Calls" 
          value={summary.margin_call_count} 
          color="text-indigo-400"
          suffix=" EVENTS"
        />
        <MetricBox 
          label="Call Amount" 
          value={fmt(summary.margin_call_amount)} 
          color="text-red-500" 
        />
        <MetricBox 
          label="Total P&L" 
          value={fmt(summary.total_pnl)} 
          color={summary.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"} 
        />
      </section>

      {/* ASSET CLASS RISK SUMMARY */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <BarChart3 className="w-4 h-4 text-indigo-500" />
            <h2 className="text-[10px] sm:text-xs font-black uppercase tracking-widest text-slate-400">Class Concentration</h2>
          </div>
          <ScrollPill />
        </div>

        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[700px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Asset Class", "Total Notional", "Net Exposure", "Delta-Eq", "Stress Events"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {asset_class_summary.map((r, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4 text-xs font-black text-white uppercase italic"><Link
  href={`/dashboard/derivatives/ticker/${T}/cls/${encodeURIComponent(r.asset_class)}`}
  className="flex items-center gap-2 font-black text-indigo-400 group-hover:text-white transition-colors italic tracking-tighter"
>
  {r.asset_class}
</Link>

</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(r.notional)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(r.net_exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(r.delta_eq)}</td>
                    <td className="px-6 py-4 text-xs">
                      <div className="flex items-center gap-2">
                         <span className={`font-mono font-bold ${r.margin_stress > 0 ? 'text-red-400' : 'text-slate-500'}`}>
                           {r.margin_stress}
                         </span>
                         {r.margin_stress > 0 && <AlertTriangle className="w-3 h-3 text-red-500" />}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <Link 
        href="/dashboard/derivatives"
        className="inline-flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-600 hover:text-white transition-colors"
      >
        <ArrowLeft className="w-3 h-3" /> Return to Risk Overview
      </Link>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricBox({ label, value, icon, color = "text-white", suffix = "" }: any) {
  return (
    <div className="p-4 sm:p-6 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-slate-600 transition-colors">
      <div className="flex items-center gap-2 mb-2 text-slate-500 uppercase tracking-widest text-[8px] sm:text-[9px] font-black">
        {icon} {label}
      </div>
      <p className={`text-lg sm:text-2xl font-black italic tracking-tighter ${color}`}>
        {value}<span className="text-[10px] not-italic opacity-50">{suffix}</span>
      </p>
    </div>
  );
}

function ScrollPill() {
  return (
    <div className="sm:hidden flex items-center gap-2 bg-indigo-500/10 px-2 py-1 rounded border border-indigo-500/20">
      <span className="text-[8px] font-black text-indigo-400 uppercase tracking-tighter">Swipe to Pan</span>
      <div className="w-1 h-1 bg-indigo-500 rounded-full animate-ping" />
    </div>
  );
}

function LoadingState({ id }: { id: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Zap className="w-6 h-6 text-indigo-500 animate-pulse" />
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.4em]">Analyzing {id}</p>
    </div>
  );
}