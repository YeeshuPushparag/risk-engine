"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import { useEffect, useState } from "react";
import { 
  ChevronRight, 
  Activity, 
  ShieldAlert, 
  ArrowLeft, 
  Zap,
  TrendingUp,
  Users,
  Layers,
  BarChart3
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type CommodityBreakdown = {
  commodity: string;
  commodity_name: string;
  exposure: number;
  pnl: number;
  avg_hedge: number;
  avg_sensitivity: number;
  var95: number;
  var99: number;
  avg_pred_vol21: number;
};

type ManagerBreakdown = {
  asset_manager: string;
  exposure: number;
  pnl: number;
  var95: number;
  var99: number;
};

type TickerDetail = {
  date: string;
  ticker: string;
  summary: {
    total_exposure: number;
    total_pnl: number;
    worst_var95: number;
    worst_var99: number;
    commodities: number;
    managers: number;
  };
  by_commodity: CommodityBreakdown[];
  by_manager: ManagerBreakdown[];
};

/* ---------------- FORMATTERS ---------------- */
const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(n);

const vol = (n: number) => n == null ? "—" : `${(n * 100).toFixed(1)}%`;

/* ---------------- MAIN COMPONENT ---------------- */
export default function CommodityTickerPage() {
  const { ticker } = useParams();
  const T = String(ticker).toUpperCase();

  const [data, setData] = useState<TickerDetail | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(`/api/commodities/ticker?ticker=${T}`, { cache: "no-store" });
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Ticker fetch failed", e);
      }
      setLoading(false);
    }
    load();
  }, [T]);

  if (loading) return <LoadingState id={T} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-widest text-xs">Ticker Not Found</div>;

  const { date, summary, by_commodity, by_manager } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-10">
      
      {/* BREADCRUMB & HEADER */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/commodities" className="hover:text-sky-400 transition-colors">Commodities</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">Ticker Analysis</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-sky-500 font-black text-[10px] tracking-[0.4em] uppercase mb-2">
              <Activity className="w-4 h-4" /> Instrument Risk Node
            </div>
            <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
              {T} <span className="text-slate-700">/</span> EXPOSURE
            </h1>
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Valuation Date: {new Date(date).toDateString().toUpperCase()}
            </p>
          </div>
          
          <div className="flex gap-3">
             <StatusPill label="Commodities" value={summary.commodities} icon={<Layers className="w-3 h-3"/>} />
             <StatusPill label="Managers" value={summary.managers} icon={<Users className="w-3 h-3"/>} />
          </div>
        </div>
      </header>

      {/* SUMMARY GRID */}
      <section className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <MetricBox label="Aggregate Exposure" value={fmt(summary.total_exposure)} />
        <MetricBox label="Composite P&L" value={fmt(summary.total_pnl)} color="text-emerald-400" />
        <MetricBox label="Critical VaR (95%)" value={fmt(summary.worst_var95)} color="text-orange-400" />
        <MetricBox label="Tail Risk VaR (99%)" value={fmt(summary.worst_var99)} color="text-red-500" />
      </section>

      {/* COMMODITY BREAKDOWN */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <BarChart3 className="w-4 h-4 text-sky-500" />
            <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-400">Underlying Commodity Correlation</h2>
          </div>
          <ScrollPill color="sky" />
        </div>
        <TableContainer>
          <table className="w-full text-left border-collapse min-w-[1000px]">
            <thead>
              <tr className="bg-slate-800/30 border-b border-slate-800 text-[9px] font-black text-slate-500 uppercase tracking-widest">
                <th className="px-6 py-4">Commodity</th>
                <th className="px-6 py-4">Exposure</th>
                <th className="px-6 py-4">P&L</th>
                <th className="px-6 py-4">Hedge Efficiency</th>
                <th className="px-6 py-4">Beta Sensitivity</th>
                <th className="px-6 py-4">Pred. Vol (21d)</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {by_commodity.map((c, i) => (
                <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                  <td className="px-6 py-4 underline decoration-slate-700 hover:decoration-sky-500 text-xs font-black italic uppercase text-white">
                    <Link href={`/dashboard/commodities/commodity/${encodeURIComponent(c.commodity)}`}>
                      {c.commodity_name}
                    </Link>
                  </td>
                  <td className="px-6 py-4 text-xs font-mono">{fmt(c.exposure)}</td>
                  <td className="px-6 py-4 text-xs font-mono text-emerald-400">{fmt(c.pnl)}</td>
                  <td className="px-6 py-4 text-xs font-mono text-slate-400">{(c.avg_hedge ?? 0).toFixed(2)}</td>
                  <td className="px-6 py-4">
                    <span className="bg-slate-800 px-2 py-1 rounded text-[10px] font-mono text-sky-400 font-bold">
                      β {(c.avg_sensitivity ?? 0).toFixed(2)}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-xs font-mono text-amber-500/80">{vol(c.avg_pred_vol21)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableContainer>
      </section>

      {/* MANAGER BREAKDOWN */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <Users className="w-4 h-4 text-indigo-500" />
            <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-400">Concentration by Asset Manager</h2>
          </div>
          <ScrollPill color="indigo" />
        </div>
        <TableContainer>
          <table className="w-full text-left border-collapse min-w-[900px]">
            <thead>
              <tr className="bg-slate-800/30 border-b border-slate-800 text-[9px] font-black text-slate-500 uppercase tracking-widest">
                <th className="px-6 py-4">Manager</th>
                <th className="px-6 py-4">Net Exposure</th>
                <th className="px-6 py-4">Contribution P&L</th>
                <th className="px-6 py-4">VaR 95</th>
                <th className="px-6 py-4">VaR 99</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {by_manager.map((m, i) => (
                <tr key={i} className="hover:bg-slate-800/20 transition-colors">
                  <td className="px-6 py-4 text-xs font-black text-indigo-400 italic">
                    <Link href={`/dashboard/commodities/manager/${encodeURIComponent(m.asset_manager)}`}>
                      {m.asset_manager}
                    </Link>
                  </td>
                  <td className="px-6 py-4 text-xs font-mono">{fmt(m.exposure)}</td>
                  <td className="px-6 py-4 text-xs font-mono">{fmt(m.pnl)}</td>
                  <td className="px-6 py-4 text-xs font-mono text-slate-400">{fmt(m.var95)}</td>
                  <td className="px-6 py-4 text-xs font-mono text-slate-500">{fmt(m.var99)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableContainer>
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
      <p className="text-slate-500 uppercase tracking-widest text-[8px] sm:text-[9px] font-black mb-2">{label}</p>
      <p className={`text-lg sm:text-2xl font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function StatusPill({ label, value, icon }: any) {
  return (
    <div className="bg-slate-900 border border-slate-800 px-4 py-2 rounded-xl flex flex-col items-center">
      <p className="text-[7px] font-black text-slate-600 uppercase tracking-widest flex items-center gap-1">
        {icon} {label}
      </p>
      <p className="text-sm font-black text-white italic">{value}</p>
    </div>
  );
}

function ScrollPill({ color = "sky" }: { color?: "sky" | "indigo" }) {
  const colorMap = {
    sky: "bg-sky-500/10 border-sky-500/20 text-sky-400 dot-sky-500",
    indigo: "bg-indigo-500/10 border-indigo-500/20 text-indigo-400 dot-indigo-500",
  };

  const currentStyles = colorMap[color];

  return (
    <div className={`lg:hidden flex items-center gap-2 px-2 py-1 rounded border ${currentStyles.split(' ').slice(0,2).join(' ')}`}>
      <span className={`text-[8px] font-black uppercase tracking-tighter ${currentStyles.split(' ')[2]}`}>
        Swipe for Ledger
      </span>
      <div className={`w-1 h-1 rounded-full animate-ping ${color === 'sky' ? 'bg-sky-500' : 'bg-indigo-500'}`} />
    </div>
  );
}

function TableContainer({ children }: { children: React.ReactNode }) {
  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm">
      <div className="overflow-x-auto terminal-scroll">{children}</div>
    </div>
  );
}

function LoadingState({ id }: { id: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Zap className="w-6 h-6 text-sky-500 animate-pulse" />
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.4em]">Deconstructing {id} Commodity Risk</p>
    </div>
  );
}