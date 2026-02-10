"use client";

import { useEffect, useState, useCallback } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import {
  ArrowLeft, Activity, ShieldAlert, BarChart3, Layers,
  Landmark, Target
} from "lucide-react";

/* ---------------- HELPERS ---------------- */
const fmtCur = (v?: number | null, compact = false) => {
  if (v == null) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency", currency: "USD",
    maximumFractionDigits: compact ? 2 : 0,
    notation: compact ? "compact" : "standard",
  }).format(v);
};

const fmtPct = (n?: number | null) => n == null ? "—" : `${(n * 100).toFixed(2)}%`;

/* ---------------- MAIN PAGE ---------------- */
export default function FxTickerDetailPage() {
  const { ticker } = useParams();
  const T = String(ticker).toUpperCase();

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/fx/daily/ticker/?ticker=${T}`, { cache: "no-store" });
      if (res.ok) setData(await res.json());
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  }, [T]);

  useEffect(() => { load(); }, [load]);

  if (loading && !data) return <LoadingState ticker={T} />;
  if (!data) return <ErrorState ticker={T} />;

  const r = data.rows[0];

  const displayDate = data.date
    ? new Date(data.date).toDateString().toUpperCase()
    : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 md:p-8 lg:p-10 space-y-6 md:space-y-8 font-sans">

      {/* HEADER SECTION */}
      <header className="flex flex-col gap-5 border-b border-slate-800 pb-6">
        <div className="space-y-4">
          {/* BACK LINK - Standardized small hit area for mobile */}
          <Link
            href="/dashboard/fx/daily"
            className="inline-flex items-center text-[9px] font-black text-slate-500 hover:text-blue-400 transition-colors uppercase tracking-[0.2em] gap-2"
          >
            <ArrowLeft className="w-3 h-3" /> Dashboard
          </Link>

          <div className="flex items-center gap-3 sm:gap-4">
            {/* ICON - Scales slightly for mobile */}
            <div className="bg-blue-600/10 border border-blue-500/20 p-1.5 sm:p-2 rounded-lg shrink-0">
              <Layers className="w-5 h-5 sm:w-7 sm:h-7 text-blue-500" />
            </div>

            {/* TITLES & METADATA */}
            <div className="min-w-0 flex-1">
              {/* Responsive font scaling: text-2xl for mobile, 4xl for tablet, 5xl+ for desktop */}
              <h1 className="text-2xl sm:text-4xl md:text-5xl font-black text-white tracking-tighter uppercase italic leading-none truncate">
                {data.ticker} <span className="text-blue-500">Risk</span>
              </h1>

              {/* SNAPSHOT LINE - Smart wrapping for small screens */}
              <div className="flex flex-wrap items-center gap-x-2 gap-y-1 mt-2">
                <p className="text-[9px] sm:text-[10px] font-bold text-slate-500 uppercase tracking-[0.15em] whitespace-nowrap">
                  {data.profile.sector}
                </p>
                <span className="text-slate-800 hidden xs:inline">•</span>
                <p className="text-[9px] sm:text-[10px] font-bold text-slate-500 uppercase tracking-[0.15em] whitespace-nowrap">
                  Snapshot EOD: <span className="text-slate-200">{displayDate}</span>
                </p>
              </div>
            </div>
          </div>
        </div>
      </header>

      {/* TOP METRICS */}
      <section className="grid grid-cols-2 lg:grid-cols-4 gap-3 md:gap-4">
        <MetricCard label="Total Exposure" value={fmtCur(data.summary.total_exposure, true)} highlight />
        <MetricCard label="Net P&L" value={fmtCur(data.summary.total_pnl, true)} pnl />
        <MetricCard label="VaR (95)" value={fmtCur(data.summary.worst_var_95, true)} risk />
        <MetricCard label="VaR (99)" value={fmtCur(data.summary.worst_var_99, true)} risk />
      </section>

      {/* DOSSIER */}
      <div className="grid grid-cols-1 xl:grid-cols-3 gap-6 md:gap-8">

        {/* EXPOSURE ATTRIBUTION */}
        <div className="xl:col-span-2 bg-slate-900/40 border border-slate-800 rounded-2xl md:rounded-3xl p-5 md:p-8 space-y-10">
          <div className="flex items-center justify-between border-b border-slate-800 pb-4">
            <div className="flex items-center gap-2">
              <Target className="w-4 h-4 text-emerald-400" />
              <h2 className="text-[10px] font-black text-white uppercase tracking-widest">
                Exposure Attribution
              </h2>
            </div>
            <span className="text-xl md:text-3xl font-black text-white italic tracking-tighter uppercase">
              {r.currency_pair}
            </span>
          </div>

          <div className="grid grid-cols-2 md:grid-cols-3 gap-y-12">
            <BigStat label="Position Size" value={fmtCur(r.position_size, true)} />
            <BigStat label="Exposure Amt" value={fmtCur(r.exposure_amount, true)} />
            <BigStat label="Hedge Eff." value={fmtPct(r.hedge_ratio)} color="text-blue-400" />
            <BigStat label="FX P&L" value={fmtCur(r.fx_pnl, true)} color={r.fx_pnl >= 0 ? "text-emerald-400" : "text-rose-500"} />
            <BigStat label="Carry P&L" value={fmtCur(r.carry_pnl, true)} />
            <BigStat label="Sharpe-Like" value={r.sharpe_like_ratio.toFixed(3)} color="text-indigo-400" />
          </div>
        </div>

        {/* MARKET FEED */}
        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl md:rounded-3xl p-8 space-y-6">
          <div className="flex items-center justify-between border-b border-slate-800 pb-4">
            <div className="flex items-center gap-2">
              <BarChart3 className="w-4 h-4 text-indigo-400" />
              <h2 className="text-[10px] font-black text-white uppercase tracking-widest">
                Market Feed
              </h2>
            </div>

            <div className="text-right">
              <p className="text-[8px] font-black text-slate-600 uppercase">Spot</p>
              <p className="text-lg font-mono font-bold text-blue-400">{r.fx_rate.toFixed(5)}</p>
            </div>
          </div>

          <div className="grid grid-cols-2 gap-y-6 gap-x-4">
            <DetailStat label="Return" value={fmtPct(r.fx_return)} color={r.fx_return >= 0 ? "text-emerald-500" : "text-rose-500"} />
            <DetailStat label="30D Vol" value={fmtPct(r.fx_volatility_30d)} />
            <DetailStat label="Pred 21D Vol" value={fmtPct(r.predicted_volatility_21d)} color="text-indigo-400" />
            <DetailStat label="Int. Diff" value={`${r.interest_diff}%`} color="text-amber-400" />
            <DetailStat label="Daily Carry" value={fmtPct(r.carry_daily)} />
            <DetailStat label="Carry Adj Ret" value={fmtPct(r.return_carry_adj)} />
          </div>
        </div>
      </div>

      {/* MACRO FOOTER */}
      <section className="bg-slate-900/20 border border-slate-800 rounded-2xl p-8 grid grid-cols-1 md:grid-cols-4 gap-6 items-center">
        <div className="md:col-span-1 border-b md:border-b-0 md:border-r border-slate-800 pb-4 md:pb-0">
          <div className="flex items-center gap-2 mb-2">
            <Landmark className="w-4 h-4 text-slate-500" />
            <h2 className="text-[10px] font-black text-white uppercase tracking-widest">
              Macro Overview
            </h2>
          </div>
          <p className="text-[9px] text-slate-500 leading-tight uppercase">
            Economic foundation for {data.ticker}
          </p>
        </div>

        <div className="md:col-span-3 grid grid-cols-2 lg:grid-cols-4 gap-6">
          <MacroItem label="National GDP" value={fmtCur(r.gdp * 1000000000, true)} />
          <MacroItem label="Unrate" value={fmtPct(r.unrate / 100)} />
          <MacroItem label="Fed Funds" value={fmtPct(r.fedfunds / 100)} />
          <MacroItem label="CPI Index" value={r.cpi.toFixed(1)} />
        </div>
      </section>
    </main>
  );
}

/* ---------------- UI ATOMS ---------------- */
function BigStat({ label, value, color = "text-white" }: any) {
  return (
    <div className="space-y-1">
      <p className="text-[8px] md:text-[9px] font-black text-slate-600 uppercase tracking-widest leading-none">{label}</p>
      <p className={`text-xl md:text-2xl lg:text-3xl font-mono font-bold tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function DetailStat({ label, value, color = "text-slate-200" }: any) {
  return (
    <div className="space-y-0.5">
      <p className="text-[8px] font-black text-slate-600 uppercase tracking-widest leading-none">{label}</p>
      <p className={`text-base md:text-lg font-mono font-bold tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function MetricCard({ label, value, highlight, pnl, risk }: any) {
  return (
    <div className="bg-slate-900/60 border border-slate-800 p-4 rounded-xl shadow-lg">
      <p className="text-[8px] md:text-[9px] font-black text-slate-600 uppercase tracking-widest mb-2 leading-none">{label}</p>
      <p className={`text-lg md:text-xl lg:text-2xl font-mono font-bold tracking-tighter 
        ${highlight ? "text-white" : ""} 
        ${pnl ? (String(value).includes("-") ? "text-rose-500" : "text-emerald-400") : ""} 
        ${risk ? "text-rose-500" : "text-slate-200"}`}>
        {value}
      </p>
    </div>
  );
}

function MacroItem({ label, value }: any) {
  return (
    <div>
      <p className="text-[8px] font-black text-slate-500 uppercase tracking-widest leading-none mb-1.5">{label}</p>
      <p className="text-lg md:text-xl font-mono font-bold text-white tracking-tighter italic leading-none">{value}</p>
    </div>
  );
}

function LoadingState({ ticker }: { ticker: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <Activity className="w-6 h-6 text-blue-500 animate-spin opacity-40" />
      <p className="text-[9px] font-black text-slate-600 uppercase tracking-[0.4em]">
        Scaling {ticker} Data
      </p>
    </div>
  );
}

function ErrorState({ ticker }: { ticker: string }) {
  return (
    <div className="h-screen bg-[#020617] flex items-center justify-center p-6 text-center">
      <div className="space-y-4">
        <ShieldAlert className="w-12 h-12 text-rose-500 mx-auto opacity-30" />
        <h2 className="text-lg font-black text-white uppercase italic">
          Access Violation: {ticker}
        </h2>
        <Link href="/dashboard/fx/daily" className="inline-block px-6 py-2 bg-blue-600 text-white text-[9px] font-black uppercase rounded-lg">
          Return to Hub
        </Link>
      </div>
    </div>
  );
}
