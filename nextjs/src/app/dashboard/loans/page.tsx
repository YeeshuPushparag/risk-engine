"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import { 
  Briefcase, 
  ShieldAlert, 
  Layers, 
  ArrowUpRight, 
  CalendarDays,
  Search, 
  Activity
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type TopTicker = {
  ticker: string;
  total_notional: number;
  expected_loss: number;
  total_pnl?: number;
  loan_count: number;
};

type LoansOverview = {
  month: string;
  summary: {
    ticker_count: number;
    loan_count: number;
    total_notional: number;
    total_expected_loss: number;
    total_pnl: number;
  };
  top_credit_risk: TopTicker[];
  top_notional: TopTicker[];
  update_frequency: string;
};

/* ---------------- FORMATTERS ---------------- */
const fmt = (n?: number | null, compact = false) => {
  if (n == null) return "—";
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: compact ? 2 : 0,
    notation: compact ? "compact" : "standard",
  }).format(n);
};

/* ---------------- PAGE ---------------- */
export default function LoansMasterPage() {
  const [data, setData] = useState<LoansOverview | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      const res = await fetch("/api/loans/overview", { cache: "no-store" });
      if (res.ok) setData(await res.json());
      setLoading(false);
    }
    load();
  }, []);

  if (loading) return <LoadingState />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black tracking-widest uppercase">No Data Feed Found</div>;

  const { summary, top_credit_risk, top_notional, month } = data;
  const displayMonth = month ? new Date(month).toLocaleDateString("en-US", { year: "numeric", month: "long" }).toUpperCase() : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-12 pb-20">
      
      {/* HEADER SECTION */}
      <header className="flex flex-col lg:flex-row lg:items-end justify-between gap-6 border-b border-slate-800/50 pb-8">
        <div className="space-y-2">
          <div className="flex items-center gap-2 text-indigo-500 font-black text-[10px] tracking-[0.3em] uppercase">
            <Briefcase className="w-3 h-3" /> System Portfolio
          </div>
          <h1 className="text-4xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
            Loans <span className="text-slate-700">/</span> Credit Risk
          </h1>
          <div className="flex items-center gap-4 pt-2">
             <div className="flex items-center gap-2 bg-slate-900 px-3 py-1 rounded-full border border-slate-800">
                <CalendarDays className="w-3 h-3 text-slate-500" />
                <span className="text-[10px] font-bold text-slate-400 tracking-wider">{displayMonth}</span>
             </div>
             <div className="text-[10px] font-bold text-slate-600 tracking-widest uppercase italic">Update Frequency: Monthly</div>
          </div>
        </div>

        <div className="w-full lg:w-96 space-y-2">
          <div className="flex items-center gap-2 text-slate-500 mb-1">
             <Search className="w-3 h-3" />
             <span className="text-[10px] font-black uppercase tracking-widest">Global Ticker Search</span>
          </div>
          <TickerSearch ticker_url="loans" />
        </div>
      </header>

      {/* AGGREGATE SUMMARY GRID */}
      <section className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-4">
        <SummaryCard label="Tickers" value={summary.ticker_count} sub="Active Entities" />
        <SummaryCard label="Total Loans" value={summary.loan_count} sub="System Wide" />
        <SummaryCard label="Notional" value={fmt(summary.total_notional, true)} sub="Gross Exposure" highlight />
        <SummaryCard label="Exp. Loss" value={fmt(summary.total_expected_loss, true)} sub="Risk at Value" color="text-red-500" />
        <SummaryCard label="Net P&L" value={fmt(summary.total_pnl, true)} sub="Portfolio Result" color={summary.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"} />
      </section>

      {/* DATA TABLES GRID */}
      <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">
        
        {/* TOP CREDIT RISK */}
        <div className="space-y-4">
          <div className="flex items-center justify-between px-2">
            <div className="flex items-center gap-3">
              <ShieldAlert className="w-5 h-5 text-red-500" />
              <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Top 5 Credit Risk</h2>
            </div>
            {/* IN-PLACE SCROLL INDICATOR (MOBILE ONLY) */}
            <div className="sm:hidden flex items-center gap-2 bg-indigo-500/10 px-2 py-1 rounded border border-indigo-500/20">
              <span className="text-[8px] font-black text-indigo-400 uppercase tracking-tighter">Scroll Table</span>
              <div className="w-1 h-1 bg-indigo-500 rounded-full animate-ping" />
            </div>
          </div>
          <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm">
            <LoanTable 
              headers={["Ticker", "Notional", "Expected Loss", "P&L", "Loans"]}
              rows={top_credit_risk.map(r => [
                <TickerLink key={r.ticker} ticker={r.ticker} />,
                fmt(r.total_notional, true),
                <span key="loss" className="text-red-400 font-bold">{fmt(r.expected_loss, true)}</span>,
                <span key="pnl" className={r.total_pnl && r.total_pnl < 0 ? 'text-red-400' : 'text-emerald-400'}>{fmt(r.total_pnl, true)}</span>,
                r.loan_count
              ])}
            />
          </div>
        </div>

        {/* NOTIONAL CONCENTRATION */}
        <div className="space-y-4">
          <div className="flex items-center justify-between px-2">
            <div className="flex items-center gap-3">
              <Layers className="w-5 h-5 text-indigo-500" />
              <h2 className="text-xs font-black uppercase tracking-[0.2em] text-slate-400">Top 5 Concentration</h2>
            </div>
            {/* IN-PLACE SCROLL INDICATOR (MOBILE ONLY) */}
            <div className="sm:hidden flex items-center gap-2 bg-indigo-500/10 px-2 py-1 rounded border border-indigo-500/20">
              <span className="text-[8px] font-black text-indigo-400 uppercase tracking-tighter">Scroll Table</span>
              <div className="w-1 h-1 bg-indigo-500 rounded-full animate-ping" />
            </div>
          </div>
          <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm">
            <LoanTable 
              headers={["Ticker", "Total Notional", "Expected Loss", "Loans"]}
              rows={top_notional.map(r => [
                <TickerLink key={r.ticker} ticker={r.ticker} />,
                <span key="notional" className="text-white font-bold">{fmt(r.total_notional, true)}</span>,
                fmt(r.expected_loss, true),
                r.loan_count
              ])}
            />
          </div>
        </div>
      </div>
    </main>
  );
}

/* ---------------- SUB-COMPONENTS ---------------- */

function SummaryCard({ label, value, sub, color = "text-white", highlight = false }: any) {
  return (
    <div className={`p-6 rounded-2xl border ${highlight ? 'bg-indigo-600/5 border-indigo-500/30' : 'bg-slate-900/50 border-slate-800'} transition-all hover:border-slate-600`}>
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-1">{label}</p>
      <p className={`text-3xl font-black italic tracking-tighter ${color}`}>{value}</p>
      <p className="text-[9px] font-bold text-slate-700 uppercase mt-2">{sub}</p>
    </div>
  );
}

function TickerLink({ ticker }: { ticker: string }) {
  return (
    <Link href={`/dashboard/loans/ticker/${ticker}`} className="group flex items-center gap-2 font-black text-indigo-400 hover:text-white transition-colors uppercase italic tracking-tighter">
      {ticker}
      <ArrowUpRight className="w-3 h-3 opacity-0 group-hover:opacity-100 transition-all translate-y-1 group-hover:translate-y-0" />
    </Link>
  );
}

function LoanTable({ headers, rows }: any) {
  return (
    <div className="overflow-x-auto terminal-scroll">
      <table className="w-full text-left border-collapse min-w-[600px] sm:min-w-0">
        <thead>
          <tr className="bg-slate-800/30 border-b border-slate-800">
            {headers.map((h: string) => (
              <th key={h} className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody className="divide-y divide-slate-800/50">
          {rows.map((row: any[], i: number) => (
            <tr key={i} className="hover:bg-slate-800/20 transition-colors">
              {row.map((cell, j) => (
                <td key={j} className="px-6 py-4 text-xs font-mono font-medium text-slate-300">{cell}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <Activity className="w-12 h-12 text-indigo-600 animate-spin" />
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.5em]">Aggregating Monthly Loan Data</p>
    </div>
  );
}