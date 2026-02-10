"use client";

import { useEffect, useState } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { 
  ChevronRight, 
  Activity, 
  Database, 
  ShieldAlert, 
  ArrowUpRight,
  Clock,
  CalendarDays
} from "lucide-react";

/* ---------------- FORMATTERS ---------------- */
const fmt = (n?: number | null, compact = false) => {
  if (n == null) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency", currency: "USD",
    maximumFractionDigits: compact ? 2 : 0,
    notation: compact ? "compact" : "standard",
  }).format(n);
};

const cleanID = (id: string) => id.replace(/[_\s]/g, "").toUpperCase();

const stageColor = (stage?: number | null) => {
  if (stage === 3) return "text-red-500 bg-red-500/10 border-red-500/20";
  if (stage === 2) return "text-yellow-500 bg-yellow-500/10 border-yellow-500/20";
  return "text-emerald-500 bg-emerald-500/10 border-emerald-500/20";
};

/* ---------------- MAIN PAGE ---------------- */
export default function LoanTickerPage() {
  const { ticker } = useParams();
  const T = decodeURIComponent(String(ticker)).toUpperCase();

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(`/api/loans/ticker?ticker=${T}`, { cache: "no-store" });
        if (res.ok) setData(await res.json());
      } catch (e) { console.error(e); }
      setLoading(false);
    }
    load();
  }, [T]);

  if (loading) return <LoadingState ticker={T} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-[0.3em]">No Records Found</div>;

  const { summary, loans, ifrs_status, predicted_spread_21d, month } = data;

  const displayMonth = month 
    ? new Date(month).toLocaleDateString("en-US", { year: "numeric", month: "long" }).toUpperCase()
    : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-6 sm:space-y-10">
      
      {/* HEADER SECTION */}
      <header className="space-y-4">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/loans" className="hover:text-indigo-400">Loans</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">{T}</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-4 border-b border-slate-800/50 pb-6">
          <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
            {T} <span className="text-indigo-500">Risk</span>
          </h1>
          <div className="flex gap-2">
            <div className="bg-slate-900 border border-slate-800 px-3 py-1.5 rounded-lg">
              <p className="text-[7px] font-black text-slate-600 uppercase tracking-tighter">IFRS Status</p>
              <p className="text-xs sm:text-sm font-black text-white">{ifrs_status}</p>
            </div>

            {/* MONTH DISPLAY */}
            <div className="bg-slate-900 border border-slate-800 px-3 py-1.5 rounded-lg flex items-center gap-2">
              <CalendarDays className="w-3 h-3 text-slate-500" />
              <p className="text-[9px] font-black text-slate-400 tracking-wider">{displayMonth}</p>
            </div>

          </div>
        </div>
      </header>

      {/* SUMMARY GRID */}
      <section className="grid grid-cols-2 lg:grid-cols-5 gap-3 sm:gap-4">
        <SummaryCard label="Loans" value={summary.loan_count} />
        <SummaryCard label="Notional" value={fmt(summary.total_notional, true)} highlight />
        <SummaryCard label="Exp. Loss" value={fmt(summary.total_expected_loss, true)} color="text-red-500" />
        <SummaryCard label="Net P&L" value={fmt(summary.total_pnl, true)} color={summary.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"} />
        <SummaryCard label="Spread 21D" value={`${predicted_spread_21d?.toFixed(2)}%`} />
      </section>

      {/* LOAN INVENTORY CARD */}
      <section className="bg-slate-900/40 border border-slate-800 rounded-xl overflow-hidden backdrop-blur-sm shadow-2xl">
        
        <div className="px-4 py-3 sm:px-6 sm:py-4 border-b border-slate-800 flex justify-between items-center bg-slate-800/20">
          <div className="flex items-center gap-2">
            <Clock className="w-3 h-3 sm:w-4 h-4 text-indigo-500" />
            <h2 className="text-[10px] sm:text-xs font-black uppercase tracking-widest text-slate-400">Inventory Feed</h2>
          </div>
          
          <div className="sm:hidden flex items-center gap-1.5 bg-indigo-500/10 px-2 py-1 rounded border border-indigo-500/20">
            <span className="text-[8px] font-black text-indigo-400 uppercase tracking-tighter">Scroll Table</span>
            <div className="w-1 h-1 bg-indigo-500 rounded-full animate-pulse" />
          </div>
        </div>

        <div className="overflow-x-auto terminal-scroll">
          <table className="w-full text-left border-collapse min-w-[700px] sm:min-w-0">
            <thead>
              <tr className="bg-slate-900/50">
                {["Loan ID", "Rate Type", "Notional", "Expected Loss", "IFRS Stage", "Maturity"].map((h) => (
                  <th key={h} className="px-4 py-3 sm:px-6 sm:py-4 text-[8px] sm:text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {loans.map((l: any) => (
                <tr key={l.loan_id} className="hover:bg-slate-800/30 transition-colors group">
                  <td className="px-4 py-3 sm:px-6 sm:py-4">
                    <Link href={`/dashboard/loans/ticker/${T}/loan/${l.loan_id}`} className="text-[10px] sm:text-xs font-black text-indigo-400 flex items-center gap-1">
                      {cleanID(l.loan_id)}
                      <ArrowUpRight className="w-2 h-2 opacity-0 group-hover:opacity-100" />
                    </Link>
                  </td>
                  <td className="px-4 py-3 sm:px-6 sm:py-4 text-[10px] sm:text-xs font-mono text-slate-500 uppercase">{l.rate_type || "—"}</td>
                  <td className="px-4 py-3 sm:px-6 sm:py-4 text-[10px] sm:text-xs font-mono font-bold text-white">{fmt(l.notional_usd, true)}</td>
                  <td className="px-4 py-3 sm:px-6 sm:py-4 text-[10px] sm:text-xs font-mono font-bold text-red-500/80">{fmt(l.Expected_Loss, true)}</td>
                  <td className="px-4 py-3 sm:px-6 sm:py-4">
                    <span className={`px-2 py-1 rounded text-[8px] sm:text-[9px] font-black border uppercase tracking-tighter whitespace-nowrap inline-block ${stageColor(l.stage)}`}>
                      Stage {l.stage || 1}
                    </span>
                  </td>
                  <td className="px-4 py-3 sm:px-6 sm:py-4 text-[10px] sm:text-xs font-mono text-slate-500 uppercase">{l.time_to_maturity_months?.toFixed(0)} MO</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function SummaryCard({ label, value, color = "text-white", highlight = false }: any) {
  return (
    <div className={`p-3 sm:p-5 rounded-xl border ${highlight ? 'bg-indigo-600/5 border-indigo-500/30' : 'bg-slate-900/50 border-slate-800'}`}>
      <p className="text-[7px] sm:text-[9px] font-black text-slate-600 uppercase tracking-widest mb-1">{label}</p>
      <p className={`text-lg sm:text-2xl font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function LoadingState({ ticker }: { ticker: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-2">
      <Activity className="w-8 h-8 text-indigo-500 animate-spin" />
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.3em]">{ticker} SYNCING...</p>
    </div>
  );
}
