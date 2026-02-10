"use client";

import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import Link from "next/link";
import { 
  ChevronRight, 
  Activity, 
  ShieldAlert, 
  Layers, 
  ArrowLeft, 
  Zap,
  Globe,
  PieChart,
  Info
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type AssetBreak = {
  asset_class: string;
  exposure: number;
  required: number;
  effective: number;
  net_exposure: number;
  collateral_ratio: number;
  margin_stress: number;
};

type CollateralTicker = {
  date: string;
  ticker: string;
  summary: {
    total_exposure: number;
    total_required: number;
    total_effective: number;
    total_net_exposure: number;
    total_margin_call: number;
    asset_class_count: number;
    margin_call_count: number;
    margin_stress: boolean;
  };
  asset_class_breakdown: AssetBreak[];
  macro: {
    gdp: number;
    unrate: number;
    cpi: number;
    fedfunds: number;
  };
};

/* ---------------- FORMATTERS ---------------- */
function pct(
  value?: number,
  decimals: number = 2
): string {
  if (value === null || value === undefined || isNaN(value)) {
    return "-";
  }

  return `${(value * 100).toFixed(decimals)}%`;
}


const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(n);

const cleanID = (id: string) => id?.replace(/[_\s]/g, "").toUpperCase() || "—";

/* ---------------- MAIN COMPONENT ---------------- */
export default function CollateralTickerPage() {
  const { ticker } = useParams();
  const T = decodeURIComponent(String(ticker));

  const [data, setData] = useState<CollateralTicker | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(`/api/collateral/ticker?ticker=${encodeURIComponent(T)}`, {
          cache: "no-store",
        });
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Collateral ticker failed", e);
      }
      setLoading(false);
    }
    load();
  }, [T]);

  if (loading) return <LoadingState id={cleanID(T)} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-widest text-xs">Node Connection Failed</div>;

  const { summary, asset_class_breakdown, macro, date } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-10">
      
      {/* BREADCRUMB & HEADER */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/collateral" className="hover:text-indigo-400">Collateral</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">{cleanID(T)}</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <div className="space-y-1">
            <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
              Pool <span className="text-indigo-500">:</span> {cleanID(T)}
            </h1>
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Audit Date: {new Date(date).toDateString().toUpperCase()}
            </p>
          </div>
          
          <div className="flex items-center gap-3">
            <div className="text-right hidden sm:block">
              <p className="text-[8px] font-black text-slate-500 uppercase tracking-widest">Active Call Count</p>
              <p className="text-xl font-black text-red-500 italic">{summary.margin_call_count}</p>
            </div>
            {summary.margin_stress && (
              <div className="flex items-center gap-2 px-4 py-2 bg-red-500/10 border border-red-500/20 rounded-xl animate-pulse">
                <ShieldAlert className="w-4 h-4 text-red-500" />
                <span className="text-[10px] font-black text-red-500 uppercase tracking-widest">High Stress</span>
              </div>
            )}
          </div>
        </div>
      </header>

      {/* TOP ROW: SUMMARY & MACRO */}
      <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
        {/* SUMMARY GRID */}
        <section className="lg:col-span-3 grid grid-cols-2 md:grid-cols-4 gap-4">
          <MetricBox label="Total Exposure" value={fmt(summary.total_exposure)} />
          <MetricBox label="Net Exposure" value={fmt(summary.total_net_exposure)} />
          <MetricBox label="Required" value={fmt(summary.total_required)} color="text-yellow-500" />
          <MetricBox label="Effective" value={fmt(summary.total_effective)} color="text-emerald-500" />
          <MetricBox 
            label="Margin Call Value" 
            value={fmt(summary.total_margin_call)} 
            color={summary.total_margin_call > 0 ? "text-red-500" : "text-slate-500"} 
            className="col-span-2"
          />
          <MetricBox label="Asset Classes" value={summary.asset_class_count} />
          <MetricBox label="Call Incidents" value={summary.margin_call_count} color="text-red-400" />
        </section>

        {/* MACRO SIDEBAR */}
        <aside className="bg-slate-900/60 border border-indigo-500/20 rounded-2xl p-6 space-y-4 shadow-[0_0_20px_rgba(79,70,229,0.05)]">
          <div className="flex items-center gap-2 mb-2">
            <Globe className="w-4 h-4 text-indigo-400" />
            <h3 className="text-[10px] font-black uppercase tracking-[0.2em] text-indigo-300">Macro Risk Environment</h3>
          </div>
          <div className="grid grid-cols-2 lg:grid-cols-1 gap-4">
            <MacroItem label="Fed Funds Rate" value={`${macro.fedfunds.toFixed(2)}%`} />
            <MacroItem label="Unemployment" value={`${macro.unrate.toFixed(1)}%`} />
            <MacroItem label="CPI Index" value={macro.cpi.toFixed(2)} />
            <MacroItem label="GDP Scale" value={fmt(macro.gdp*1000000000)} />
          </div>
        </aside>
      </div>

      {/* ASSET CLASS BREAKDOWN */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <Layers className="w-4 h-4 text-indigo-500" />
            <h2 className="text-[10px] sm:text-xs font-black uppercase tracking-widest text-slate-400">Liquidity breakdown by class</h2>
          </div>
          <ScrollPill />
        </div>

        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[900px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Asset Class", "Exposure", "Required", "Effective", "Ratio", "Stress Count", "Status"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {asset_class_breakdown.map((a, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4 text-xs font-black text-white italic uppercase"><Link
  href={`/dashboard/collateral/ticker/${T}/cls/${encodeURIComponent(a.asset_class)}`}
  className="flex items-center gap-2 font-black text-indigo-400 group-hover:text-white transition-colors italic tracking-tighter"
>
  {a.asset_class}
</Link></td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(a.exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(a.required)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-emerald-400">{fmt(a.effective)}</td>
                    <td className="px-6 py-4 text-xs font-mono font-bold text-indigo-400">
                      {pct(a.collateral_ratio, 2)} 
                    </td>
                    <td className="px-6 py-4 text-xs font-mono text-slate-400">
                      {a.margin_stress} events
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-2">
                         <div className={`w-2 h-2 rounded-full ${a.margin_stress > 0 ? 'bg-red-500 animate-pulse' : 'bg-slate-700'}`} />
                         <span className={`text-[10px] font-black uppercase ${a.margin_stress > 0 ? 'text-red-500' : 'text-slate-500'}`}>
                           {a.margin_stress > 0 ? 'Breach' : 'Secure'}
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

      <footer className="pt-10 flex items-center justify-between border-t border-slate-800/50">
        <Link 
          href="/dashboard/collateral"
          className="inline-flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-600 hover:text-white transition-colors"
        >
          <ArrowLeft className="w-3 h-3" /> Back to Firm Overview
        </Link>
        <div className="flex items-center gap-2 text-slate-600">
           <Info className="w-3 h-3" />
           <span className="text-[8px] font-bold uppercase tracking-widest">Calculated via Terminal Logic v2.4</span>
        </div>
      </footer>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricBox({ label, value, color = "text-white", className = "" }: any) {
  return (
    <div className={`p-4 sm:p-6 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-slate-600 transition-colors ${className}`}>
      <p className="text-slate-500 uppercase tracking-widest text-[8px] sm:text-[9px] font-black mb-2">
        {label}
      </p>
      <p className={`text-lg sm:text-2xl font-black italic tracking-tighter ${color}`}>
        {value}
      </p>
    </div>
  );
}

function MacroItem({ label, value }: { label: string, value: string | number }) {
  return (
    <div className="flex justify-between items-center lg:items-start lg:flex-col lg:gap-1 border-b border-slate-800/50 pb-2 lg:border-none">
      <span className="text-[9px] font-bold text-slate-500 uppercase tracking-tighter">{label}</span>
      <span className="text-sm font-black text-white italic tracking-tighter">{value}</span>
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

function LoadingState({ id }: { id: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Zap className="w-6 h-6 text-indigo-500 animate-pulse" />
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.4em]">Decoding {id} Data Lake</p>
    </div>
  );
}