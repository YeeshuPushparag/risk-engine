"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import {
  ArrowLeft,
  ShieldCheck,
  TrendingUp,
  Activity,
  Globe,
  BarChart3,
  Calendar,
  Zap
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type Bond = {
  bond_id: string; ticker: string; sector: string; industry: string;
  credit_rating: string; implied_rating: string | null;
  coupon_rate: number; issue_date: string; maturity_date: string; maturity_years: number;
  bond_price: number; yield_to_maturity: number; benchmark_yield: number; credit_spread: number;
  implied_hazard: number; implied_pd_annual: number; implied_pd_multi_year: number; pred_pd_21d: number;
  DGS10: number; DGS10_ma: number; dgs10_anom: number; gdp: number; cpi: number; unrate: number; fedfunds: number;
  pred_spread_5d: number; market_synthetic_score: number;
};

type BondTickerResponse = { date: string; bond: Bond; };

/* ---------------- FORMATTERS ---------------- */
const cleanID = (id: string) => id.replace(/[_\s]/g, "").toUpperCase();
// For values like 6.34 -> "6.34%"
const fmtPctRaw = (n?: number | null) => 
  n == null ? "—" : `${n.toFixed(2)}%`;

// For values like 0.012 -> "1.20%"
const fmtPctDecimal = (n?: number | null) => 
  n == null ? "—" : `${(n * 100).toFixed(2)}%`;

// For spread 2.5 -> "250 bps" (1% = 100bps)
const fmtBpsFromRaw = (n?: number | null) => 
  n == null ? "—" : `${(n * 100).toFixed(0)} bps`;

const fmtPrice = (n?: number | null) => 
  n == null ? "—" : `$${n.toFixed(2)}`;

const fmtCurCompact = (v?: number | null) => {
  if (v == null) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency", currency: "USD",
    maximumFractionDigits: 2,
    notation: "compact",
  }).format(v);
};


/* ---------------- MAIN PAGE ---------------- */
export default function BondTickerPage() {
  const { ticker } = useParams();
  const T = String(ticker).toUpperCase();

  const [data, setData] = useState<BondTickerResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(`/api/bonds/ticker?ticker=${T}`, { cache: "no-store" });
        if (res.ok) setData(await res.json());
      } catch (e) { console.error(e); }
      setLoading(false);
    }
    load();
  }, [T]);

  if (loading) return <LoadingState ticker={T} />;
  if (!data || !data.bond) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-[0.3em]">Ticker Not Found</div>;

  const r = data.bond;
  // ---- HUMAN READABLE DATE ----
  const displayDate = data.date 
  ? new Date(data.date).toDateString().toUpperCase()
  : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-6 lg:p-12 space-y-8 selection:bg-indigo-500/30">
      
<header className="space-y-6">
  {/* 1. TOP NAVIGATION & STATUS PILL - Flex Wrap for Mobile */}
  <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
    <Link 
      href="/dashboard/bonds" 
      className="group inline-flex items-center text-[10px] font-black text-slate-500 hover:text-white uppercase tracking-[0.2em] gap-2 transition-all"
    >
      <ArrowLeft className="w-3.5 h-3.5 group-hover:-translate-x-1 transition-transform" /> 
      <span>Back to Ledger</span>
    </Link>

    <div className="inline-flex items-center self-start sm:self-auto gap-2 px-3 py-1.5 bg-slate-900 border border-slate-800 rounded-lg sm:rounded-full">
      <div className="w-1.5 h-1.5 rounded-full bg-indigo-500 animate-pulse" />
      <span className="text-[9px] font-black text-slate-400 uppercase tracking-widest">
        EOD: <span className="text-slate-100 font-mono">{displayDate}</span>
      </span>
    </div>
  </div>

  {/* 2. MAIN ASSET IDENTIFIER - Optimized for narrow screens */}
  <div className="flex flex-col lg:flex-row justify-between items-start lg:items-end gap-6">
    <div className="w-full space-y-4">
      <div className="flex items-start sm:items-center gap-4">
        <div className="shrink-0 bg-indigo-600 text-white p-2.5 rounded-xl shadow-lg shadow-indigo-500/10 border border-indigo-400/20">
          <BarChart3 className="w-6 h-6 sm:w-8 sm:h-8" />
        </div>
        <div className="min-w-0">
          <h1 className="text-3xl sm:text-5xl lg:text-6xl font-black text-white uppercase italic tracking-tighter leading-none truncate">
            {r.ticker} <span className="text-indigo-500">Risk</span>
          </h1>
          <div className="flex flex-wrap items-center gap-2 mt-2">
            <span className="text-[9px] sm:text-[10px] font-black px-2 py-0.5 bg-slate-800 text-slate-300 rounded uppercase tracking-tighter">
              {r.industry}
            </span>
            <span className="hidden xs:block w-1 h-1 rounded-full bg-slate-700" />
            <span className="text-[10px] sm:text-xs font-mono font-bold text-slate-500 tracking-tight">
              ID: {cleanID(r.bond_id)}
            </span>
          </div>
        </div>
      </div>
    </div>

    {/* 3. CREDIT SCOREBOARD - Grid on Mobile, Flex on Desktop */}
    <div className="w-full lg:w-auto grid grid-cols-2 lg:flex lg:items-stretch bg-[#0a0f1d] border border-slate-800 rounded-2xl overflow-hidden shadow-2xl">
      {/* Current Rating */}
      <div className="px-4 py-4 sm:px-6 sm:py-3 flex flex-col justify-center border-r border-slate-800">
        <p className="text-[8px] sm:text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1">Rating</p>
        <div className="flex items-baseline gap-1">
          <p className="text-2xl sm:text-3xl font-black text-white tracking-tighter">{r.credit_rating}</p>
          <span className="text-[8px] font-bold text-slate-600 italic uppercase">S&P</span>
        </div>
      </div>
      
      {/* Implied Rating */}
      <div className="px-4 py-4 sm:px-6 sm:py-3 flex flex-col justify-center bg-indigo-500/5">
        <div className="flex items-center gap-1.5 mb-1">
           <Activity className="w-3 h-3 text-emerald-500" />
           <p className="text-[8px] sm:text-[9px] font-black text-indigo-400 uppercase tracking-widest">Implied</p>
        </div>
        <p className="text-2xl sm:text-3xl font-black text-emerald-400 tracking-tighter">
          {r.implied_rating || "NR"}
        </p>
      </div>
    </div>
  </div>
</header>


      {/* PRIMARY DATA GRID */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">

        {/* SECTION: PRICING & CREDIT */}
        <DataCard title="Pricing & Credit" icon={<TrendingUp className="w-4 h-4 text-indigo-400" />}>
          <DataRow label="Current Price" value={fmtPrice(r.bond_price)} bold />
          <DataRow label="Yield to Maturity" value={fmtPctRaw(r.yield_to_maturity)} highlight />
          <DataRow label="Benchmark Yield" value={fmtPctRaw(r.benchmark_yield)} />
          <DataRow label="Credit Spread" value={fmtBpsFromRaw(r.credit_spread)} highlight color="text-indigo-400" />
        </DataCard>

        {/* SECTION: DEFAULT RISK */}
        <DataCard title="Default Risk" icon={<ShieldCheck className="w-4 h-4 text-red-500" />}>
          <DataRow label="Implied Hazard" value={fmtPctRaw(r.implied_hazard)} />
          <DataRow label="Annual PD" value={fmtPctRaw(r.implied_pd_annual)} color="text-red-400" />
          <DataRow label="Multi-Year PD" value={fmtPctRaw(r.implied_pd_multi_year)} />
          <DataRow label="Pred. PD (21d)" value={fmtPctRaw(r.pred_pd_21d)} bold />
        </DataCard>

        {/* SECTION: FORWARD SIGNALS */}
        <DataCard title="Predictive Signals" icon={<Zap className="w-4 h-4 text-yellow-500" />}>
          <DataRow label="Pred. Spread (5d)" value={fmtPctRaw(r.pred_spread_5d)} />
          <DataRow label="Synthetic Score" value={r.market_synthetic_score.toFixed(2)} bold color="text-yellow-500" />
          <DataRow label="Sector" value={r.sector} />
          <DataRow label="Maturity" value={`${r.maturity_years} Years`} />
        </DataCard>

      </div>

      {/* MACRO SECTION */}
      <section className="bg-slate-900/40 border border-slate-800 rounded-3xl p-6 sm:p-8 shadow-2xl">
        <div className="flex items-center gap-2 mb-6">
          <Globe className="w-5 h-5 text-blue-400" />
          <h2 className="text-xs font-black uppercase tracking-[0.3em]">Macro & Rates Environment</h2>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-6">
          <MacroMetric label="10Y Treasury" value={fmtPctRaw(r.DGS10)} />
          <MacroMetric label="10Y MA" value={fmtPctRaw(r.DGS10_ma)} />
          <MacroMetric label="National GDP" value={fmtCurCompact(r.gdp * 1e9)} />
          <MacroMetric label="CPI (Inf)" value={`${r.cpi.toFixed(1)}%`} />
          <MacroMetric label="Unemploy." value={`${r.unrate.toFixed(1)}%`} />
          <MacroMetric label="Fed Funds" value={fmtPctRaw(r.fedfunds)} />
          <MacroMetric label="Rate Anom." value={r.dgs10_anom.toFixed(2)} />
        </div>
      </section>

      {/* TERMS SUMMARY */}
      <section className="flex flex-wrap gap-4">
        <div className="bg-slate-900/80 border border-slate-800 px-6 py-4 rounded-2xl flex items-center gap-4 shadow-lg">
          <Calendar className="w-5 h-5 text-slate-500" />
          <div>
            <p className="text-[8px] font-black text-slate-500 uppercase tracking-tighter">Issue Date</p>
            <p className="text-sm font-bold text-white font-mono">{r.issue_date}</p>
          </div>
          <div className="w-px h-6 bg-slate-800" />
          <div>
            <p className="text-[8px] font-black text-slate-500 uppercase tracking-tighter">Maturity Date</p>
            <p className="text-sm font-bold text-white font-mono">{r.maturity_date}</p>
          </div>
          <div className="w-px h-6 bg-slate-800" />
          <div>
            <p className="text-[8px] font-black text-slate-500 uppercase tracking-tighter">Coupon Rate</p>
            <p className="text-sm font-bold text-indigo-400 font-mono">{fmtPctRaw(r.coupon_rate*100)}</p>
          </div>
        </div>
      </section>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function DataCard({ title, icon, children }: { title: string; icon: React.ReactNode; children: React.ReactNode }) {
  return (
    <div className="bg-slate-900/50 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-xl">
      <div className="px-5 py-4 border-b border-slate-800 bg-slate-800/20 flex items-center gap-2">
        {icon}
        <h3 className="text-[10px] font-black uppercase tracking-widest text-slate-400">{title}</h3>
      </div>
      <div className="p-2">{children}</div>
    </div>
  );
}

function DataRow({ label, value, bold = false, highlight = false, color = "text-white" }: any) {
  return (
    <div className={`flex justify-between items-center p-3 rounded-lg hover:bg-slate-800/30 transition-colors`}>
      <span className="text-xs font-bold text-slate-500 uppercase tracking-tight">{label}</span>
      <span className={`text-sm font-mono ${bold ? 'font-black' : 'font-medium'} ${highlight ? 'text-indigo-400' : color}`}>
        {value}
      </span>
    </div>
  );
}

function MacroMetric({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-[8px] font-black text-slate-600 uppercase mb-1 tracking-tight">{label}</p>
      <p className="text-sm font-mono font-bold text-white leading-none">{value}</p>
    </div>
  );
}

function LoadingState({ ticker }: { ticker: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center text-center p-6">
      <Activity className="w-10 h-10 text-indigo-600 animate-spin mb-4" />
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.4em]">Aggregating Risk Matrix: {ticker}</p>
    </div>
  );
}