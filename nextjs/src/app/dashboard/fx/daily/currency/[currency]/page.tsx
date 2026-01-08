"use client";

import { useEffect, useState, useCallback } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import {
  ArrowLeft, Globe, ShieldAlert, BarChart3, PieChart,
  ChevronRight
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type FxRow = {
  ticker: string; sector: string; industry: string; currency_pair: string;
  foreign_revenue_ratio: number; date: string; fx_rate: number;
  fx_return: number; volume: number; fx_volatility: number;
  fx_volatility_20d: number; fx_volatility_30d: number;
  predicted_volatility_21d: number; interest_diff: number;
  carry_daily: number; return_carry_adj: number; position_size: number;
  hedge_ratio: number; exposure_amount: number; fx_pnl: number;
  carry_pnl: number; total_pnl: number; VaR_95: number; VaR_99: number;
};

type FxCurrencyResponse = {
  date: string;
  currency_pair: string;
  summary: {
    total_exposure: number;
    total_pnl: number;
    worst_var_95: number;
    worst_var_99: number;
  };
  rows: FxRow[];
};

/* ---------------- FORMATTERS ---------------- */
const fmtCur = (v?: number | null, compact = false) => {
  if (v == null) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency", currency: "USD",
    maximumFractionDigits: compact ? 2 : 0,
    notation: compact ? "compact" : "standard",
  }).format(v);
};

const fmtPct = (v?: number | null) => v == null ? "—" : `${(v * 100).toFixed(2)}%`;

/* ---------------- MAIN PAGE ---------------- */

export default function FxCurrencyDetailPage() {
  const { currency } = useParams();
  const C = decodeURIComponent(String(currency)).toUpperCase();

  const [data, setData] = useState<FxCurrencyResponse | null>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const res = await fetch(`/api/fx/daily/currency/?currency=${C}`, { cache: "no-store" });
      if (res.ok) setData(await res.json());
    } catch (err) {
      console.error("FX Load Failure", err);
    } finally {
      setLoading(false);
    }
  }, [C]);

  useEffect(() => { load(); }, [load]);

  if (loading && !data) return <LoadingState pair={C} />;
  if (!data) return <ErrorState pair={C} />;

  const { summary, rows } = data;

  const displayDate = data.date
    ? new Date(data.date).toDateString().toUpperCase()
    : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 pb-20 font-sans">

      {/* HEADER */}
      <header className="sticky top-0 z-[100] bg-[#020617]/90 backdrop-blur-xl border-b border-slate-800 px-4 sm:px-8 py-3">
        <div className="max-w-[1600px] mx-auto flex justify-between items-center gap-4">
          <div className="flex items-center gap-3 min-w-0">
            <Link href="/dashboard/fx/daily" className="p-2 bg-slate-900 border border-slate-800 rounded-lg hover:bg-slate-800 transition-all shrink-0">
              <ArrowLeft className="w-4 h-4 text-slate-500" />
            </Link>
            <div className="min-w-0">
              <div className="flex items-center gap-1 text-[8px] font-black text-slate-600 uppercase tracking-widest mb-0.5">
                <span>DASHBOARD</span> 
                <ChevronRight size={10} /> 
                <span className="text-blue-500">EOD: {displayDate}</span>
              </div>
              <h1 className="text-xl sm:text-3xl font-black text-white italic tracking-tighter uppercase leading-none">
                {C} <span className="text-slate-700 not-italic"> PORTFOLIO</span>
              </h1>
            </div>
          </div>
        </div>
      </header>

      <div className="max-w-[1600px] mx-auto p-4 sm:p-8 space-y-6">

        {/* AGGREGATE SUMMARY */}
        <section className="grid grid-cols-2 lg:grid-cols-4 gap-3">
          <MetricCard label="Net Currency Exposure" value={fmtCur(summary.total_exposure, true)} highlight />
          <MetricCard label="Aggregate P&L" value={fmtCur(summary.total_pnl, true)} pnl />
          <MetricCard label="Worst VaR (95%)" value={fmtCur(summary.worst_var_95, true)} risk />
          <MetricCard label="Worst VaR (99%)" value={fmtCur(summary.worst_var_99, true)} risk />
        </section>

        {/* TICKER ATTRIBUTION */}
        <section className="space-y-4 pt-4">
          <div className="flex items-center justify-between border-l-4 border-blue-600 pl-4">
            <div className="flex items-center gap-2">
              <PieChart className="w-4 h-4 text-blue-500" />
              <h2 className="text-[10px] font-black text-white uppercase tracking-widest">Ticker Attribution Analysis</h2>
            </div>

            <div className="flex lg:hidden items-center gap-2 px-2 py-1 rounded-full bg-blue-500/10 border border-blue-500/20">
              <span className="text-[8px] font-black text-blue-400 uppercase tracking-tighter animate-pulse">Swipe to explore</span>
              <div className="flex gap-0.5">
                <div className="w-1 h-1 rounded-full bg-blue-500 animate-bounce" style={{ animationDelay: '0ms' }} />
                <div className="w-1 h-1 rounded-full bg-blue-500 animate-bounce" style={{ animationDelay: '150ms' }} />
                <div className="w-1 h-1 rounded-full bg-blue-500 animate-bounce" style={{ animationDelay: '300ms' }} />
              </div>
            </div>

            <span className="text-[9px] font-black text-slate-600 uppercase tracking-tighter hidden lg:block">
              Horizontal Scroll Enabled
            </span>
          </div>

          <div className="relative group bg-slate-900/20 border border-slate-800 rounded-2xl shadow-2xl overflow-hidden">
            <div className="absolute right-0 top-0 bottom-0 w-12 bg-gradient-to-l from-[#020617] to-transparent pointer-events-none z-10 lg:hidden" />

            <div className="overflow-x-auto no-scrollbar scroll-smooth">
              <table className="w-full text-left border-collapse min-w-[900px]">
                <thead>
                  <tr className="bg-slate-950/50 border-b border-slate-800">
                    <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest sticky left-0 bg-[#020617] z-20 border-r border-slate-800/50">Ticker</th>
                    <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">Exposure</th>
                    <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">Hedge %</th>
                    <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">FX P&L</th>
                    <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">Carry P&L</th>
                    <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">Total P&L</th>
                    <th className="p-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">VaR (95)</th>
                  </tr>
                </thead>

                <tbody className="divide-y divide-slate-800/40">
                  {rows.map((r) => (
                    <tr key={r.ticker} className="hover:bg-slate-900/60 transition-colors group">
                      <td className="p-4 sticky left-0 bg-[#020617] group-hover:bg-slate-900 transition-colors z-20 border-r border-slate-800/50 shadow-[4px_0_12px_rgba(0,0,0,0.5)] lg:shadow-none">
                        <Link
                          href={`/dashboard/fx/daily/ticker/${r.ticker}`}
                          className="text-xs font-black text-blue-400 italic hover:text-blue-300 transition-all cursor-pointer underline decoration-blue-500/20 underline-offset-4"
                        >
                          {r.ticker}
                        </Link>
                        <p className="text-[8px] font-bold text-slate-600 uppercase mt-0.5 truncate max-w-[80px]">{r.industry}</p>
                      </td>

                      <td className="p-4 text-right font-mono text-xs text-slate-200">{fmtCur(r.exposure_amount, true)}</td>
                      <td className="p-4 text-right font-mono text-xs text-slate-500">{fmtPct(r.hedge_ratio)}</td>
                      <td className="p-4 text-right font-mono text-xs text-slate-400">{fmtCur(r.fx_pnl, true)}</td>
                      <td className="p-4 text-right font-mono text-xs text-slate-400">{fmtCur(r.carry_pnl, true)}</td>

                      <td className={`p-4 text-right font-mono text-xs font-bold ${r.total_pnl >= 0 ? 'text-emerald-400' : 'text-rose-500'}`}>
                        {fmtCur(r.total_pnl, true)}
                      </td>

                      <td className="p-4 text-right font-mono text-xs text-rose-500/60">{fmtCur(r.VaR_95, true)}</td>
                    </tr>
                  ))}
                </tbody>

              </table>
            </div>
          </div>
        </section>
      </div>
    </main>
  );
}

/* ---------------- ATOMS ---------------- */

function MetricCard({ label, value, highlight, pnl, risk }: any) {
  const isNeg = value.includes('-');
  return (
    <div className="bg-slate-900/40 border border-slate-800 p-4 rounded-xl">
      <p className="text-[9px] font-black text-slate-600 uppercase tracking-widest mb-1.5">{label}</p>
      <p className={`text-lg sm:text-2xl font-mono font-bold tracking-tighter 
        ${highlight ? 'text-white' : ''} 
        ${pnl ? (isNeg ? 'text-rose-500' : 'text-emerald-400') : ''} 
        ${risk ? 'text-rose-500' : 'text-slate-200'}`}>
        {value}
      </p>
    </div>
  );
}

function LoadingState({ pair }: { pair: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center">
      <Globe className="w-8 h-8 text-blue-500 animate-spin opacity-40 mb-4" />
      <p className="text-[9px] font-black text-slate-600 uppercase tracking-[0.4em]">Decoding {pair} Exposure</p>
    </div>
  );
}

function ErrorState({ pair }: { pair: string }) {
  return (
    <div className="h-screen bg-[#020617] flex items-center justify-center p-6 text-center">
      <div className="space-y-4 max-w-sm border border-slate-800 p-10 rounded-3xl bg-slate-900/20">
        <ShieldAlert className="w-12 h-12 text-rose-500 mx-auto opacity-30" />
        <h2 className="text-lg font-black text-white uppercase italic tracking-tighter leading-none">
          Intelligence Breach: {pair}
        </h2>
        <p className="text-slate-500 text-xs">
          The requested currency node returned an empty or invalid risk payload.
        </p>

        <Link
          href="/dashboard/fx/daily"
          className="mt-4 inline-block px-8 py-2 bg-blue-600 rounded-lg text-[9px] font-black text-white uppercase tracking-widest"
        >
          Return to Hub
        </Link>
      </div>
    </div>
  );
}
