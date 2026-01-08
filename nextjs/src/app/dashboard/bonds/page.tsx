"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import { 
  ShieldAlert, 
  TrendingUp, 
  Activity, 
  Search, 
  BarChart3, 
  ArrowRight,
  MoveHorizontal 
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type BondRowPD = { ticker: string; bond_id: string; credit_rating: string; implied_pd_annual: number; bond_price: number; };
type BondRowSpread = { ticker: string; bond_id: string; credit_rating: string; credit_spread: number; yield_to_maturity: number; };
type BondRowMaturity = { ticker: string; bond_id: string; credit_rating: string; maturity_years: number; bond_price: number; };

type BondsOverviewResponse = {
  date: string;
  summary: {
    bond_count: number; ticker_count: number; weighted_credit_spread: number; weighted_ytm: number; weighted_implied_pd: number;
  };
  top_default_risk: BondRowPD[];
  top_spreads: BondRowSpread[];
  top_maturity: BondRowMaturity[];
};

/* ---------------- FORMATTERS ---------------- */
const pctRaw = (n?: number | null) => n == null ? "—" : `${n.toFixed(2)}%`;
const pctDec = (n?: number | null) => n == null ? "—" : `${(n * 100).toFixed(2)}%`;
const fmt = (n?: number | null) => n == null ? "—" : new Intl.NumberFormat("en-US", {
  style: "currency", currency: "USD", maximumFractionDigits: 0,
}).format(n);

/* ---------------- MAIN PAGE ---------------- */
export default function BondsMasterPage() {
  const [data, setData] = useState<BondsOverviewResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/bonds/overview", { cache: "no-store" });
        if (res.ok) setData(await res.json());
      } catch (e) { console.error(e); }
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <LoadingState />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-widest">No bond data available</div>;

  const { summary, top_default_risk, top_spreads, top_maturity } = data;

  // ---- HUMAN READABLE DATE ----
  const displayDate = data.date 
    ? new Date(data.date).toDateString().toUpperCase()
    : "N/A";

  const cleanID = (id: string) => id.replace(/[_\s]/g, "").toUpperCase();

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-6 lg:p-12 space-y-10 selection:bg-indigo-500/30">
      
     {/* HEADER SECTION */}
<header className="space-y-6">
  <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6">
    <div className="space-y-4">
      {/* Title & Icon Group */}
      <div className="flex items-center gap-4">
        <div className="bg-indigo-600 text-white p-2.5 rounded-xl shadow-lg shadow-indigo-500/20 border border-indigo-400/30">
          <ShieldAlert className="w-6 h-6 md:w-8 md:h-8" />
        </div>
        <h1 className="text-4xl sm:text-6xl font-black text-white uppercase italic tracking-[ -0.05em] leading-none">
          Fixed Income
        </h1>
      </div>

      {/* NEW METADATA STRIP: Aggregates & Date */}
      <div className="flex flex-wrap items-center gap-y-2 gap-x-4">
        {/* Aggregate Status Badge */}
        <div className="flex items-center gap-2 px-3 py-1 bg-slate-800/50 border border-slate-700 rounded-full">
          <span className="relative flex h-2 w-2">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-2 w-2 bg-indigo-500"></span>
          </span>
          <span className="text-[10px] font-black text-slate-300 uppercase tracking-wider">
            Credit Risk & Bond Aggregates
          </span>
        </div>

        {/* Separator Line (Hidden on mobile) */}
        <div className="hidden sm:block h-4 w-px bg-slate-800" />

        {/* Terminal Date Display */}
        <div className="flex items-center gap-2">
          <span className="text-[10px] font-black text-slate-500 uppercase tracking-widest">
            Terminal Date:
          </span>
          <span className="text-xs font-mono font-bold text-indigo-400 bg-indigo-400/10 px-2 py-0.5 rounded">
            {displayDate}
          </span>
        </div>
        
        {/* System Status */}
        <div className="hidden lg:flex items-center gap-1.5">
           <span className="text-[9px] font-black text-slate-600 uppercase tracking-widest">Status:</span>
           <span className="text-[9px] font-black text-emerald-500 uppercase">Live Feed</span>
        </div>
      </div>
    </div>

    {/* Search Box */}
    <div className="w-full md:w-80 space-y-2">
      <div className="flex items-center justify-between px-1">
        <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest flex items-center gap-2">
          <Search className="w-3 h-3" /> Quick Bond Access
        </p>
        <span className="text-[9px] font-mono text-slate-600">ISIN/CUSIP</span>
      </div>
      <TickerSearch ticker_url="bonds" />
    </div>
  </div>
</header>

      {/* CORE METRICS GRID */}
      <section className="grid grid-cols-2 md:grid-cols-5 gap-3 sm:gap-5">
        <MetricCard label="Total Bonds" value={summary.bond_count} />
        <MetricCard label="Tickers" value={summary.ticker_count} />
        <MetricCard label="Avg Spread" value={pctRaw(summary.weighted_credit_spread)} />
        <MetricCard label="Avg YTM" value={pctRaw(summary.weighted_ytm)} />
        <MetricCard label="Avg PD" value={pctDec(summary.weighted_implied_pd)} />
      </section>

      {/* RISK TABLES SECTION */}
      <div className="grid grid-cols-1 gap-12">
        
        {/* TABLE 1: DEFAULT RISK */}
        <TableWrapper title="Top Default Risk (PD)" icon={<ShieldAlert className="w-4 h-4 text-red-500" />}>
          <table className="w-full text-sm text-left min-w-[650px]">
            <thead className="bg-slate-800/40 border-b border-slate-800">
              <tr>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Ticker</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Bond ID</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Rating</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">Implied PD</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">Price</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {top_default_risk.map((r) => (
                <tr key={cleanID(r.bond_id)} className="hover:bg-slate-800/40 transition-colors group">
                  <td className="p-4">
                    <Link href={`/dashboard/bonds/ticker/${r.ticker}`} className="font-black text-white italic hover:text-indigo-400 flex items-center gap-1 transition-colors">
                      {r.ticker} <ArrowRight className="w-3 h-3 opacity-0 group-hover:opacity-100 transition-all -translate-x-2 group-hover:translate-x-0" />
                    </Link>
                  </td>
                  <td className="p-4 text-xs font-mono text-slate-500">{cleanID(r.bond_id)}</td>
                  <td className="p-4">
                    <span className="px-2 py-0.5 rounded text-[10px] font-bold bg-slate-900 border border-slate-700 text-slate-300">{r.credit_rating}</span>
                  </td>
                  <td className="p-4 text-right font-mono font-bold text-red-500">{pctDec(r.implied_pd_annual)}</td>
                  <td className="p-4 text-right font-mono text-slate-300">{fmt(r.bond_price)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableWrapper>

        {/* TABLE 2: SPREADS */}
        <TableWrapper title="Widest Spreads" icon={<Activity className="w-4 h-4 text-indigo-500" />}>
          <table className="w-full text-sm text-left min-w-[600px]">
            <thead className="bg-slate-800/40 border-b border-slate-800">
              <tr>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Ticker</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Bond ID</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">Spread</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">YTM</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {top_spreads.map((r) => (
                <tr key={cleanID(r.bond_id)} className="hover:bg-slate-800/40 transition-colors group">
                  <td className="p-4">
                    <Link href={`/dashboard/bonds/ticker/${r.ticker}`} className="font-black text-white hover:text-indigo-400 flex items-center gap-1 transition-colors">
                      {r.ticker} <ArrowRight className="w-3 h-3 opacity-0 group-hover:opacity-100 transition-all -translate-x-2 group-hover:translate-x-0" />
                    </Link>
                  </td>
                  <td className="p-4 text-xs font-mono text-slate-500">{cleanID(r.bond_id)}</td>
                  <td className="p-4 text-right font-mono font-bold text-indigo-400">{pctRaw(r.credit_spread)}</td>
                  <td className="p-4 text-right font-mono text-slate-300">{pctRaw(r.yield_to_maturity)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableWrapper>

        {/* TABLE 3: MATURITY */}
        <TableWrapper title="Longest Maturity" icon={<BarChart3 className="w-4 h-4 text-emerald-500" />}>
          <table className="w-full text-sm text-left min-w-[600px]">
            <thead className="bg-slate-800/40 border-b border-slate-800">
              <tr>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Ticker</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Bond ID</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Maturity</th>
                <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">Price</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {top_maturity.map((r) => (
                <tr key={cleanID(r.bond_id)} className="hover:bg-slate-800/40 transition-colors group">
                  <td className="p-4">
                    <Link href={`/dashboard/bonds/ticker/${r.ticker}`} className="font-black text-white hover:text-indigo-400 flex items-center gap-1 transition-colors">
                      {r.ticker} <ArrowRight className="w-3 h-3 opacity-0 group-hover:opacity-100 transition-all -translate-x-2 group-hover:translate-x-0" />
                    </Link>
                  </td>
                  <td className="p-4 text-xs font-mono text-slate-500">{cleanID(r.bond_id)}</td>
                  <td className="p-4 font-mono text-slate-300">{r.maturity_years}y</td>
                  <td className="p-4 text-right font-mono text-emerald-500 font-bold">{fmt(r.bond_price)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </TableWrapper>
      </div>

      {/* CUSTOM TERMINAL SCROLLBAR STYLES */}
      <style jsx global>{`
        .terminal-scroll::-webkit-scrollbar {
          height: 6px;
          width: 6px;
        }
        .terminal-scroll::-webkit-scrollbar-track {
          background: rgba(30, 41, 59, 0.3);
          border-radius: 10px;
        }
        .terminal-scroll::-webkit-scrollbar-thumb {
          background: #334155;
          border-radius: 10px;
        }
        .terminal-scroll::-webkit-scrollbar-thumb:hover {
          background: #6366f1;
        }
      `}</style>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricCard({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="p-4 sm:p-5 border border-slate-800 rounded-2xl bg-slate-900/40 backdrop-blur-md">
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1">{label}</p>
      <p className="text-xl sm:text-2xl font-mono font-bold text-white tracking-tighter">{value}</p>
    </div>
  );
}

function TableWrapper({ title, children, icon }: { title: string; children: React.ReactNode; icon: React.ReactNode }) {
  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between px-2">
        <div className="flex items-center gap-2 text-slate-500 font-black uppercase text-[10px] tracking-[0.2em]">
          {icon} {title}
        </div>
        <div className="md:hidden flex items-center gap-2 text-[8px] font-bold text-indigo-400 uppercase animate-pulse">
          <MoveHorizontal className="w-3 h-3" /> Scroll to view
        </div>
      </div>
      <div className="bg-slate-900/50 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
        <div className="overflow-x-auto terminal-scroll">
          {children}
        </div>
      </div>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center">
      <Activity className="w-10 h-10 text-indigo-600 animate-spin mb-4" />
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.4em]">Establishing Credit Terminal</p>
    </div>
  );
}
