"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import { useEffect, useState } from "react";
import { 
  ChevronRight, 
  Droplets, 
  Activity, 
  ShieldAlert, 
  TrendingUp, 
  ArrowLeft, 
  Zap,
  Target,
  BarChart4
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type TickerBreakdown = {
  ticker: string;
  exposure: number;
  pnl: number;
  avg_hedge: number;
  avg_sensitivity: number;
  managers: number;
};

type CommodityDetail = {
  date: string;
  commodity: string;
  commodity_name: string;
  summary: {
    total_exposure: number;
    total_pnl: number;
    worst_var95: number;
    worst_var99: number;
  };
  tickers: TickerBreakdown[];
};

/* ---------------- FORMATTERS ---------------- */
const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(n);

const cleanID = (id: string) => id?.toUpperCase() || "—";

/* ---------------- MAIN COMPONENT ---------------- */
export default function CommodityViewPage() {
  const { commodity } = useParams();
  const C = String(commodity);

  const [data, setData] = useState<CommodityDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(`/api/commodities/commodity?commodity=${C}`, { 
          cache: "no-store" 
        });
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Commodity fetch failed", e);
      }
      setLoading(false);
    }
    load();
  }, [C]);

  if (loading) return <LoadingState id={cleanID(C)} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-widest text-xs">Asset Not Found</div>;

  const { date, commodity: symbol, commodity_name, summary, tickers } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-10">
      
      {/* BREADCRUMB & HEADER */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/commodities" className="hover:text-amber-500 transition-colors">Commodities</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">{symbol}</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <div className="space-y-1">
            <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
              {commodity_name} <span className="text-amber-500">/</span> {symbol}
            </h1>
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Risk Ledger Update: {new Date(date).toDateString().toUpperCase()}
            </p>
          </div>
          
          <div className="flex items-center gap-4 bg-slate-900/80 p-3 rounded-xl border border-slate-800">
             <div className="text-right">
                <p className="text-[8px] font-black text-slate-500 uppercase tracking-widest">Aggregate P&L</p>
                <p className={`text-xl font-black italic ${summary.total_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                  {fmt(summary.total_pnl)}
                </p>
             </div>
             <Droplets className="w-8 h-8 text-amber-600/20" />
          </div>
        </div>
      </header>

      {/* SUMMARY GRID */}
      <section className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricBox label="Gross Exposure" value={fmt(summary.total_exposure)} />
        <MetricBox label="VaR 95% Confidence" value={fmt(summary.worst_var95)} color="text-orange-400" />
        <MetricBox label="VaR 99% Confidence" value={fmt(summary.worst_var99)} color="text-red-500" />
        <MetricBox label="Ticker Count" value={tickers.length} />
      </section>

      {/* TICKER BREAKDOWN TABLE */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <BarChart4 className="w-4 h-4 text-amber-500" />
            <h2 className="text-[10px] sm:text-xs font-black uppercase tracking-widest text-slate-400">Equity Sensitivity Matrix</h2>
          </div>
          <ScrollPill />
        </div>

        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[1000px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Ticker", "Exposure", "P&L Contribution", "Hedge Efficiency", "Price Delta", "Managers"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {tickers.map((t, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      <Link href={`/dashboard/commodities/ticker/${t.ticker}`} className="text-xs font-black text-white italic hover:text-amber-500 transition-colors">
                        {t.ticker}
                      </Link>
                    </td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.exposure)}</td>
                    <td className={`px-6 py-4 text-xs font-mono font-bold ${t.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {fmt(t.pnl)}
                    </td>
                    <td className="px-6 py-4">
                      <div className="flex items-center gap-3">
                         <div className="flex-1 h-1.5 w-16 bg-slate-800 rounded-full overflow-hidden">
                            <div className="bg-indigo-500 h-full" style={{ width: `${t.avg_hedge * 100}%` }} />
                         </div>
                         <span className="text-xs font-mono text-slate-400">{(t.avg_hedge * 100).toFixed(1)}%</span>
                      </div>
                    </td>
                    <td className="px-6 py-4">
                       <span className={`text-xs font-mono px-2 py-1 rounded ${t.avg_sensitivity > 0.5 ? 'bg-amber-500/10 text-amber-500' : 'bg-slate-800 text-slate-400'}`}>
                         β {t.avg_sensitivity.toFixed(2)}
                       </span>
                    </td>
                    <td className="px-6 py-4 text-xs font-black text-slate-300">
                      {t.managers} <span className="text-[10px] text-slate-600 ml-1 font-normal uppercase">Nodes</span>
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
          <ArrowLeft className="w-3 h-3" /> Return to Commodities Desk
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
    <div className="lg:hidden flex items-center gap-2 bg-amber-500/10 px-2 py-1 rounded border border-amber-500/20">
      <span className="text-[8px] font-black text-amber-500 uppercase tracking-tighter">Swipe for Full Ledger</span>
      <div className="w-1 h-1 bg-amber-500 rounded-full animate-ping" />
    </div>
  );
}

function LoadingState({ id }: { id: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Zap className="w-6 h-6 text-amber-500 animate-pulse" />
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.4em]">Querying {id} Risk Matrix</p>
    </div>
  );
}