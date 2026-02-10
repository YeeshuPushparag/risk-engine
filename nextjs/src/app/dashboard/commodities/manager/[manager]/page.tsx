"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import { useEffect, useState } from "react";
import { 
  ChevronRight, 
  Users, 
  ShieldAlert, 
  TrendingUp, 
  ArrowLeft, 
  Zap,
  Briefcase,
  PieChart
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type CommodityRow = {
  commodity: string;
  commodity_name: string;
  exposure: number;
  pnl: number;
  var95: number;
  var99: number;
  tickers: number;
};

type ManagerDetail = {
  date: string;
  manager: string;
  summary: {
    total_exposure: number;
    total_pnl: number;
    worst_var95: number;
    worst_var99: number;
    commodities: number;
  };
  commodities: CommodityRow[];
};

/* ---------------- FORMATTERS ---------------- */
const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(n);

/* ---------------- MAIN COMPONENT ---------------- */
export default function CommodityManagerPage() {
  const { manager } = useParams();
  const M = decodeURIComponent(String(manager));
  const [data, setData] = useState<ManagerDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(`/api/commodities/manager?manager=${encodeURIComponent(M)}`, {
          cache: "no-store",
        });
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Manager fetch failed", e);
      }
      setLoading(false);
    }
    load();
  }, [M]);

  if (loading) return <LoadingState id={M} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-widest text-xs">Manager Node Offline</div>;

  const { date, summary, commodities } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-10">
      
      {/* BREADCRUMB & HEADER */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/commodities" className="hover:text-indigo-400 transition-colors">Commodities</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">Manager Analysis</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-indigo-500 font-black text-[10px] tracking-[0.4em] uppercase mb-2">
              <Briefcase className="w-4 h-4" /> Institutional Portfolio
            </div>
            <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
              {M}
            </h1>
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Portfolio Snapshot: {new Date(date).toDateString().toUpperCase()}
            </p>
          </div>
          
          <div className="flex gap-4">
             <div className="bg-slate-900 border border-slate-800 p-4 rounded-2xl">
                <p className="text-[8px] font-black text-slate-500 uppercase tracking-widest">Active Assets</p>
                <p className="text-xl font-black text-white italic">{summary.commodities} <span className="text-xs text-slate-600 not-italic uppercase">Classes</span></p>
             </div>
          </div>
        </div>
      </header>

      {/* SUMMARY GRID */}
      <section className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricBox label="AUM Exposure" value={fmt(summary.total_exposure)} />
        <MetricBox 
          label="Realized/Unrealized P&L" 
          value={fmt(summary.total_pnl)} 
          color={summary.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"} 
        />
        <MetricBox label="Portfolio VaR (95%)" value={fmt(summary.worst_var95)} color="text-orange-400" />
        <MetricBox label="Portfolio VaR (99%)" value={fmt(summary.worst_var99)} color="text-red-500" />
      </section>

      {/* RISK BREAKDOWN TABLE */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <PieChart className="w-4 h-4 text-indigo-500" />
            <h2 className="text-[10px] sm:text-xs font-black uppercase tracking-widest text-slate-400">Commodity Risk Breakdown</h2>
          </div>
          <ScrollPill />
        </div>

        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[900px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Commodity", "MTM Exposure", "P&L", "VaR 95", "VaR 99", "Ticker Count"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {commodities.map((c, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      <Link 
                        href={`/dashboard/commodities/commodity/${encodeURIComponent(c.commodity)}`} 
                        className="text-xs font-black text-white italic hover:text-indigo-400 transition-colors uppercase tracking-tight"
                      >
                        {c.commodity_name}
                      </Link>
                    </td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(c.exposure)}</td>
                    <td className={`px-6 py-4 text-xs font-mono font-bold ${c.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {fmt(c.pnl)}
                    </td>
                    <td className="px-6 py-4 text-xs font-mono text-slate-400">{fmt(c.var95)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-slate-500">{fmt(c.var99)}</td>
                    <td className="px-6 py-4">
                       <span className="text-xs font-black text-slate-300">
                         {c.tickers} <span className="text-[10px] text-slate-600 font-normal uppercase ml-1">Pos.</span>
                       </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* FOOTER ACTION */}
      <footer className="pt-6 border-t border-slate-800/50">
        <Link 
          href="/dashboard/commodities"
          className="inline-flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-600 hover:text-white transition-colors"
        >
          <ArrowLeft className="w-3 h-3" /> Back to Commodities Overview
        </Link>
      </footer>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricBox({ label, value, color = "text-white" }: any) {
  return (
    <div className="p-4 sm:p-6 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-slate-600 transition-all">
      <p className="text-slate-500 uppercase tracking-widest text-[8px] sm:text-[9px] font-black mb-2">
        {label}
      </p>
      <p className={`text-lg sm:text-2xl font-black italic tracking-tighter ${color}`}>
        {value}
      </p>
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
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.4em]">Aggregating {id} Holdings</p>
    </div>
  );
}