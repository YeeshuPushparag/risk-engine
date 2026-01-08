"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import { useEffect, useState } from "react";
import { 
  ChevronRight, 
  Activity, 
  ShieldCheck, 
  TrendingDown, 
  Zap, 
  Clock,
  ArrowLeft,
  CalendarDays
} from "lucide-react";

/* ---------------- FORMATTERS ---------------- */
const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency", 
    currency: "USD",
    notation: "compact",        
    maximumFractionDigits: 2,   
  }).format(n);

const pct = (n?: number | null) =>
  n == null ? "—" : `${(n * 100).toFixed(2)}%`;

const cleanID = (id: string) => id?.replace(/[_\s]/g, "").toUpperCase() || "—";

const roundVal = (n?: number | null, decimals = 4) => 
  n == null ? "—" : n.toFixed(decimals);

const getStageStyles = (stage?: number | null) => {
  if (stage === 3) return "text-red-500 bg-red-500/10 border-red-500/20";
  if (stage === 2) return "text-yellow-500 bg-yellow-500/10 border-yellow-500/20";
  return "text-emerald-500 bg-emerald-500/10 border-emerald-500/20";
};

/* ---------------- MAIN PAGE ---------------- */
export default function LoanDetailPage() {
  const { ticker, loan_id } = useParams();
  const T = decodeURIComponent(String(ticker)).toUpperCase();
  const L = decodeURIComponent(String(loan_id));

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const load = async () => {
      try {
        const res = await fetch(`/api/loans/loan?loan_id=${encodeURIComponent(L)}`, {
          cache: "no-store",
        });
        if (!res.ok) throw new Error("LOAN FEED OFFLINE");
        const json = await res.json();
        setData(json);
      } catch (err: any) {
        setError(err.message || "ERROR LOADING LOAN");
      } finally {
        setLoading(false);
      }
    };
    load();
  }, [L]);

  if (loading) return <LoadingState id={cleanID(L)} />;
  if (error) return <div className="h-screen bg-[#020617] flex items-center justify-center text-red-500 font-black tracking-widest uppercase">{error}</div>;
  if (!data?.loan) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black tracking-widest uppercase">No loan data found</div>;

  const r = data.loan;
  const month = data.month;

  const displayMonth = month 
    ? new Date(month).toLocaleDateString("en-US", { year: "numeric", month: "long" }).toUpperCase()
    : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-6 sm:space-y-10">
      
      {/* HEADER */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/loans" className="hover:text-indigo-400">Loans</Link>
          <ChevronRight className="w-2 h-2" />
          <Link href={`/dashboard/loans/ticker/${encodeURIComponent(T)}`} className="hover:text-indigo-400">{T}</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">{cleanID(L)}</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
            Loan <span className="text-indigo-500">Ref:</span> {cleanID(L)}
          </h1>

          <div className="flex items-center gap-3">
            <div className={`px-4 py-2 rounded-xl border font-black uppercase text-xs tracking-widest ${getStageStyles(r.stage)}`}>
              Stage {r.stage || 1}
            </div>

            {/* MONTH DISPLAY */}
            <div className="bg-slate-900 border border-slate-800 px-3 py-2 rounded-xl flex items-center gap-2">
              <CalendarDays className="w-3 h-3 text-slate-500" />
              <span className="text-[9px] font-black text-slate-400 tracking-wider">{displayMonth}</span>
            </div>
          </div>

        </div>
      </header>

      {/* METRICS */}
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
        <MetricBox label="Notional" value={fmt(r.notional_usd)} icon={<Activity className="w-3 h-3 text-indigo-500" />} />
        <MetricBox label="Exp. Loss" value={fmt(r.Expected_Loss)} color="text-red-500" icon={<TrendingDown className="w-3 h-3" />} />
        <MetricBox label="PD (%)" value={pct(r.PD)} icon={<ShieldCheck className="w-3 h-3 text-emerald-500" />} />
        <MetricBox label="Maturity" value={`${r.time_to_maturity_months?.toFixed(0)} MO`} icon={<Clock className="w-3 h-3 text-slate-500" />} />
      </div>

      {/* SYSTEM PARAMETERS CARD */}
      <section className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
        <div className="px-4 py-3 sm:px-6 sm:py-4 border-b border-slate-800 flex items-center bg-slate-800/20">
          <Zap className="w-4 h-4 text-indigo-500 mr-2" />
          <h2 className="text-[10px] sm:text-xs font-black uppercase tracking-widest text-slate-400">Asset Data Feed</h2>
        </div>

        <div className="w-full">
          <table className="w-full text-left border-collapse">
            <tbody className="divide-y divide-slate-800/50">
              <DataRow label="Asset Ticker" value={cleanID(r.ticker)} />
              <DataRow label="Rate Type" value={r.rate_type} />
              <DataRow label="Base Currency" value={r.currency} />
              <DataRow label="Credit Rating" value={r.credit_rating} highlight />
              <DataRow label="Exposure (EAD)" value={fmt(r.EAD)} />
              <DataRow label="Loss Given (LGD)" value={pct(r.LGD)} />
              <DataRow 
                label="Total Net P&L" 
                value={fmt(r.total_pnl)} 
                color={r.total_pnl >= 0 ? "text-emerald-400" : "text-red-500"} 
              />
              <DataRow label="Liquidity Score" value={roundVal(r.liquidity_score, 4)} />
              <DataRow label="Macro Stress Score" value={roundVal(r.macro_stress_score, 4)} />
            </tbody>
          </table>
        </div>
      </section>

      <Link 
        href={`/dashboard/loans/ticker/${encodeURIComponent(T)}`}
        className="inline-flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-600 hover:text-white transition-colors pb-10"
      >
        <ArrowLeft className="w-3 h-3" /> Return to Ticker
      </Link>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricBox({ label, value, icon, color = "text-white" }: any) {
  return (
    <div className="p-4 sm:p-6 bg-slate-900/50 border border-slate-800 rounded-2xl">
      <div className="flex items-center gap-2 mb-2 text-slate-500 uppercase tracking-widest text-[8px] sm:text-[10px] font-black">
        {icon} {label}
      </div>
      <p className={`text-lg sm:text-2xl font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function DataRow({ label, value, color = "text-slate-300", highlight = false }: any) {
  return (
    <tr className="hover:bg-slate-800/20 transition-colors">
      <td className="px-4 sm:px-6 py-4 text-[9px] sm:text-[10px] font-black text-slate-500 uppercase tracking-widest leading-tight">{label}</td>
      <td className={`px-4 sm:px-6 py-4 text-xs sm:text-sm font-mono font-bold ${color} ${highlight ? 'text-indigo-400' : ''}`}>
        {String(value ?? "—")}
      </td>
    </tr>
  );
}

function LoadingState({ id }: { id: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Activity className="w-8 h-8 text-indigo-500 animate-pulse" />
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.4em]">Feeding {id}</p>
    </div>
  );
}
