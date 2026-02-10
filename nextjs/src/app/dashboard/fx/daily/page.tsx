"use client";

import { useEffect, useState, useCallback } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import { 
  Activity, 
  Globe, 
  ShieldAlert, 
  TrendingUp, 
  ChevronRight,
  PieChart,
  LayoutGrid
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type FxOverview = {
  date: string;
  summary: {
    total_exposure: number; total_pnl: number;
    worst_var_95: number; worst_var_99: number;
    ticker_count: number; currency_count: number;
  };
  by_ticker: {
    ticker: string; exposure: number; pnl: number;
    VaR95: number; VaR99: number; currency_count: number;
  }[];
  by_currency: {
    currency_pair: string; exposure: number; pnl: number;
    VaR95: number; VaR99: number; ticker_count: number;
  }[];
};

/* ---------------- HELPERS ---------------- */
const fmtCur = (v?: number | null, compact = false) => {
  if (v == null) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency", currency: "USD",
    maximumFractionDigits: compact ? 2 : 0,
    notation: compact ? "compact" : "standard",
  }).format(v);
};

/* ---------------- MAIN PAGE ---------------- */
export default function FxMasterPage() {
  const [data, setData] = useState<FxOverview | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch("/api/fx/daily/overview/", { cache: "no-store" });
      if (!res.ok) throw new Error("Failed to load FX overview");
      const json = await res.json();
      setData(json);
    } catch (err) {
      console.error("FX Load Error:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { load(); }, [load]);

  if (loading && !data) return <LoadingState />;
  if (!data) return <ErrorFallback />;

  const { date, summary, by_ticker, by_currency } = data;

  const displayDate = date
    ? new Date(date).toDateString().toUpperCase()
    : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-6 lg:p-10 space-y-8 font-sans">
      
      {/* HEADER SECTION */}
      <header className="flex flex-col gap-6">
        <div className="flex items-center justify-between w-full">
          <div className="space-y-1">
            <div className="flex items-center gap-3">
              <div className="bg-emerald-500/10 border border-emerald-500/20 p-1.5 rounded-lg shrink-0">
                <Globe className="w-5 h-5 sm:w-6 sm:h-6 text-emerald-500" />
              </div>
              <h1 className="text-xl sm:text-2xl md:text-4xl font-black text-white tracking-tighter uppercase italic leading-none">
                FX <span className="text-emerald-500">Firm Risk</span>
              </h1>
            </div>

            {/* ONLY CHANGE IS HERE — SAME DESIGN */}
            <div className="flex items-center gap-3 pl-1">
              <p className="text-[9px] font-bold text-slate-500 uppercase tracking-[0.2em]">
                Snapshot • EOD: {displayDate}
              </p>
            </div>
          </div>
        </div>

        {/* SEARCH COMPONENT */}
        <section className="w-full lg:max-w-[500px]">
          <TickerSearch ticker_url="fx/daily" />
        </section>
      </header>

      {/* SUMMARY METRICS */}
      <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 sm:gap-4">
        <MetricCard label="Pairs" value={summary.currency_count} />
        <MetricCard label="Tickers" value={summary.ticker_count} />
        <MetricCard label="Net Exposure" value={fmtCur(summary.total_exposure, true)} highlight />
        <MetricCard label="Total P&L" value={fmtCur(summary.total_pnl, true)} pnl />
        <MetricCard label="VaR 95" value={fmtCur(summary.worst_var_95, true)} risk />
        <MetricCard label="VaR 99" value={fmtCur(summary.worst_var_99, true)} risk />
      </section>

      {/* TICKER ATTRIBUTION */}
      <section className="space-y-4">
        <div className="flex items-center justify-between border-l-4 border-blue-600 pl-4">
          <div className="flex items-center gap-2">
            <PieChart className="w-4 h-4 text-blue-500" />
            <h2 className="text-[10px] font-black text-white uppercase tracking-widest">
              Ticker Attribution Analysis
            </h2>
          </div>

          <div className="lg:hidden flex items-center gap-2 text-blue-400 animate-pulse">
            <span className="text-[8px] font-black uppercase tracking-tighter">Swipe Table</span>
            <ChevronRight className="w-3 h-3" />
          </div>
        </div>

        <div className="bg-slate-900/20 border border-slate-800 rounded-2xl overflow-hidden shadow-2xl">
          <div className="overflow-x-auto no-scrollbar">
            <table className="w-full text-left border-collapse min-w-[900px]">
              <thead>
                <tr className="bg-slate-950/50 border-b border-slate-800">
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest sticky left-0 bg-[#020617] z-20 border-r border-slate-800/50">Ticker</th>
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">Exposure</th>
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">Net P&L</th>
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">VaR (95)</th>
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">VaR (99)</th>
                  <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-center">Pairs</th>
                </tr>
              </thead>

              <tbody className="divide-y divide-slate-800/40">
                {by_ticker.map((t) => (
                  <tr key={t.ticker} className="hover:bg-slate-900/60 transition-colors group">
                    <td className="p-4 sticky left-0 bg-[#020617] group-hover:bg-slate-900 transition-colors z-20 border-r border-slate-800/50">
                      <Link 
                        href={`/dashboard/fx/daily/ticker/${t.ticker}`} 
                        className="text-xs font-black text-blue-400 italic hover:text-blue-300 transition-all underline decoration-blue-500/20 underline-offset-4"
                      >
                        {t.ticker}
                      </Link>
                    </td>

                    <td className="p-4 text-right font-mono text-xs text-slate-200">
                      {fmtCur(t.exposure, true)}
                    </td>

                    <td className={`p-4 text-right font-mono text-xs font-bold ${t.pnl >= 0 ? "text-emerald-400" : "text-rose-500"}`}>
                      {fmtCur(t.pnl, true)}
                    </td>

                    <td className="p-4 text-right font-mono text-xs text-rose-500/60">
                      {fmtCur(t.VaR95, true)}
                    </td>

                    <td className="p-4 text-right font-mono text-xs text-rose-500/40">
                      {fmtCur(t.VaR99, true)}
                    </td>

                    <td className="p-4 text-center text-[10px] font-black text-slate-500">
                      {t.currency_count}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* CURRENCY GRID */}
      <section className="space-y-4">
        <div className="flex items-center gap-3 border-l-4 border-indigo-600 pl-4">
          <LayoutGrid className="w-4 h-4 text-indigo-500" />
          <h2 className="text-[10px] font-black text-white uppercase tracking-widest">Currency Concentration</h2>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 pb-10">
          {by_currency.map((c) => (
            <Link
              key={c.currency_pair}
              href={`/dashboard/fx/daily/currency/${encodeURIComponent(c.currency_pair)}`}
              className="group p-5 bg-slate-900/30 border border-slate-800 rounded-2xl hover:bg-slate-900 hover:border-indigo-500/50 transition-all shadow-xl"
            >
              <div className="flex justify-between items-start mb-4">
                <span className="text-lg font-black text-white italic group-hover:text-indigo-400 transition-colors">
                  {c.currency_pair}
                </span>
                <span className="text-[9px] font-black px-2 py-1 bg-slate-800 rounded text-slate-500 uppercase">
                  {c.ticker_count} Tickers
                </span>
              </div>

              <div className="space-y-3">
                <div className="flex justify-between items-center">
                  <span className="text-[8px] font-black text-slate-600 uppercase">
                    Exposure
                  </span>
                  <span className="text-xs font-mono font-bold text-slate-300">
                    {fmtCur(c.exposure, true)}
                  </span>
                </div>

                <div className="flex justify-between items-center">
                  <span className="text-[8px] font-black text-slate-600 uppercase">
                    Risk (95)
                  </span>
                  <span className="text-xs font-mono font-bold text-rose-400">
                    {fmtCur(c.VaR95, true)}
                  </span>
                </div>
              </div>
            </Link>
          ))}
        </div>
      </section>
    </main>
  );
}

/* ---------------- UI COMPONENTS ---------------- */

function MetricCard({ label, value, highlight, pnl, risk }: any) {
  return (
    <div className="p-3 sm:p-4 bg-slate-900/60 border border-slate-800 rounded-2xl">
      <p className="text-[8px] sm:text-[9px] font-black text-slate-600 uppercase tracking-widest mb-1 truncate">
        {label}
      </p>
      <p
        className={`text-base sm:text-lg font-mono font-bold tracking-tighter 
        ${highlight ? "text-white" : ""} 
        ${pnl ? (String(value).includes("-") ? "text-rose-500" : "text-emerald-400") : ""} 
        ${risk ? "text-rose-500" : "text-slate-300"}`}
      >
        {value}
      </p>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <Activity className="w-8 h-8 text-emerald-500 animate-spin opacity-50" />
      <p className="text-[9px] font-black text-slate-600 uppercase tracking-[0.4em]">
        Establishing Risk Context
      </p>
    </div>
  );
}

function ErrorFallback() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center p-6 text-center">
      <ShieldAlert className="w-10 h-10 text-rose-500 mb-4 opacity-50" />
      <h2 className="text-lg font-black text-white uppercase italic tracking-tighter">
        Data Stream Interrupted
      </h2>
      <button
        onClick={() => window.location.reload()}
        className="mt-6 px-8 py-2 bg-slate-800 rounded-full text-[9px] font-black uppercase tracking-widest hover:bg-slate-700 transition-all cursor-pointer"
      >
        Retry
      </button>
    </div>
  );
}
