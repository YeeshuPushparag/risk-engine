"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import ManagerSearch from "@/components/ManagerSearch";
import { 
  BarChart3, 
  Users, 
  Boxes, 
  Activity, 
  Search, 
  TrendingUp, 
  ShieldAlert,
  ArrowUpRight
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type ManagerRow = {
  asset_manager: string;
  exposure: number;
  pnl: number;
  var95: number;
  var99: number;
  commodities: number;
};

type CommodityRow = {
  commodity: string;
  commodity_name: string;
  exposure: number;
  pnl: number;
  var95: number;
  var99: number;
  managers: number;
};

type TickerRow = {
  ticker: string;
  exposure: number;
  pnl: number;
  var95: number;
  var99: number;
  commodities: number;
};

type OverviewResponse = {
  date: string;
  summary: {
    tickers: number;
    managers: number;
    commodities: number;
    total_exposure: number;
    total_pnl: number;
    worst_var95: number;
    worst_var99: number;
  };
  managers: ManagerRow[];
  commodities: CommodityRow[];
  tickers: TickerRow[];
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
export default function CommoditiesOverview() {
  const [data, setData] = useState<OverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/commodities/overview", { cache: "no-store" });
        if (res.ok) setData(await res.json());
      } catch (err) {
        console.error("Commodities overview load failed", err);
      }
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <LoadingState />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-widest text-xs">Link Offline</div>;

  const { summary, managers, commodities, tickers } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-12">
      
      {/* HEADER & SEARCH */}
      <header className="flex flex-col lg:flex-row lg:items-end justify-between gap-8 border-b border-slate-800/50 pb-10">
        <div className="space-y-3">
          <div className="flex items-center gap-2 text-amber-500 font-black text-[10px] tracking-[0.4em] uppercase">
            <Boxes className="w-4 h-4" /> Global Commodities Risk
          </div>
          <h1 className="text-4xl sm:text-7xl font-black text-white italic tracking-tighter uppercase leading-none">
            Firm <span className="text-slate-800">/</span> Assets
          </h1>
          <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
            Reporting Date: {new Date(data.date).toDateString().toUpperCase()}
          </p>
        </div>

        <div className="flex flex-col sm:flex-row gap-4 w-full lg:w-auto">
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-slate-500 px-1 text-[9px] font-black uppercase tracking-widest">
              <Search className="w-3 h-3" /> Ticker Search
            </label>
            <TickerSearch ticker_url="commodities" />
          </div>
          <div className="space-y-2">
            <label className="flex items-center gap-2 text-slate-500 px-1 text-[9px] font-black uppercase tracking-widest">
              <Users className="w-3 h-3" /> Manager Search
            </label>
            <ManagerSearch manager_url="commodities" />
          </div>
        </div>
      </header>

      {/* SUMMARY METRICS */}
      <section className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4">
        <MetricBox label="Tickers" value={summary.tickers} />
        <MetricBox label="Managers" value={summary.managers} />
        <MetricBox label="Classes" value={summary.commodities} />
        <MetricBox label="Total Exposure" value={fmt(summary.total_exposure)} color="text-white" />
        <MetricBox label="Total P&L" value={fmt(summary.total_pnl)} color={summary.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"} />
        <MetricBox label="Worst VaR 95" value={fmt(summary.worst_var95)} color="text-orange-400" />
        <MetricBox label="Worst VaR 99" value={fmt(summary.worst_var99)} color="text-red-500" />
      </section>

      {/* CONCENTRATION TABLES */}
      <div className="grid grid-cols-1 gap-12">
        
        {/* COMMODITY TABLE */}
        <section className="space-y-4">
          <div className="flex justify-between items-center px-2">
            <div className="flex items-center gap-3">
              <TrendingUp className="w-5 h-5 text-amber-500" />
              <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Commodity Concentration</h2>
            </div>
            <ScrollPill />
          </div>
          <TableContainer>
            <table className="w-full text-left border-collapse min-w-[900px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Asset Class", "Exposure", "P&L", "VaR 95", "VaR 99", "Managers"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {commodities.map((c, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      <Link href={`/dashboard/commodities/commodity/${c.commodity}`} className="flex items-center gap-2 font-black text-amber-500 italic uppercase">
                        {c.commodity_name} <ArrowUpRight className="w-3 h-3 opacity-0 group-hover:opacity-100" />
                      </Link>
                    </td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(c.exposure)}</td>
                    <td className={`px-6 py-4 text-xs font-mono font-bold ${c.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>{fmt(c.pnl)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-slate-400">{fmt(c.var95)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-slate-500">{fmt(c.var99)}</td>
                    <td className="px-6 py-4 text-xs font-black text-slate-300">{c.managers}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TableContainer>
        </section>

        {/* MANAGER TABLE */}
        <section className="space-y-4">
          <div className="flex justify-between items-center px-2">
            <div className="flex items-center gap-3">
              <Users className="w-5 h-5 text-indigo-500" />
              <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Top 5 Manager Concentration</h2>
            </div>
            <ScrollPill />
          </div>
          <TableContainer>
            <table className="w-full text-left border-collapse min-w-[900px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Manager", "Exposure", "P&L", "VaR 95", "VaR 99", "Assets"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {managers.map((m, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      <Link href={`/dashboard/commodities/manager/${m.asset_manager}`} className="font-black text-indigo-400 italic uppercase tracking-tighter">
                        {m.asset_manager}
                      </Link>
                    </td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(m.exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-emerald-400">{fmt(m.pnl)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-orange-400">{fmt(m.var95)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-red-500">{fmt(m.var99)}</td>
                    <td className="px-6 py-4 text-xs font-black">{m.commodities}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TableContainer>
        </section>

        {/* TICKER TABLE */}
        <section className="space-y-4">
          <div className="flex justify-between items-center px-2">
            <div className="flex items-center gap-3">
              <Activity className="w-5 h-5 text-sky-500" />
              <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Top 5 Ticker Concentration</h2>
            </div>
            <ScrollPill />
          </div>
          <TableContainer>
            <table className="w-full text-left border-collapse min-w-[900px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  {["Ticker", "Exposure", "P&L", "VaR 95", "VaR 99", "Classes"].map((h) => (
                    <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {tickers.map((t, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4 font-black text-sky-400 italic tracking-widest"> <Link
          href={`/dashboard/commodities/ticker/${t.ticker}`}
          className="font-black text-indigo-400 italic uppercase tracking-tighter"
        >
          {t.ticker}
        </Link></td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.pnl)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.var95)}</td>
                    <td className="px-6 py-4 text-xs font-mono">{fmt(t.var99)}</td>
                    <td className="px-6 py-4 text-xs font-bold">{t.commodities}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </TableContainer>
        </section>

      </div>
    </main>
  );
}

/* ---------------- UI COMPONENTS ---------------- */

function MetricBox({ label, value, color = "text-white" }: any) {
  return (
    <div className="p-4 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-slate-600 transition-all">
      <p className="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-1">{label}</p>
      <p className={`text-lg font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function TableContainer({ children }: { children: React.ReactNode }) {
  return (
    <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
      <div className="overflow-x-auto terminal-scroll">{children}</div>
    </div>
  );
}

function ScrollPill() {
  return (
    <div className="lg:hidden flex items-center gap-2 bg-slate-800/50 px-2 py-1 rounded border border-slate-700">
      <span className="text-[8px] font-black text-slate-500 uppercase tracking-tighter">Swipe to Pan</span>
      <div className="w-1 h-1 bg-amber-500 rounded-full animate-pulse" />
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <BarChart3 className="w-8 h-8 text-amber-500 animate-pulse" />
      <p className="text-[10px] font-black text-slate-600 uppercase tracking-[0.5em]">Analyzing Commodity Risk</p>
    </div>
  );
}