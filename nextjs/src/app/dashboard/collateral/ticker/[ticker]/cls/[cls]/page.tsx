"use client";

import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import Link from "next/link";
import { 
  ChevronRight, 
  Terminal, 
  ArrowLeft, 
  Zap, 
  History,
  AlertTriangle,
  Globe,
  ShieldCheck,
  Activity
} from "lucide-react";

/* ---------------- FORMATTERS ---------------- */
const cleanID = (id: string) => id?.replace(/[_\s]/g, "").toUpperCase() || "—";
const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(n);

const ratioFmt = (n: number) =>
  n == null ? "—" : `${(n * 100).toFixed(2)}%`;

/**
 * INSTITUTIONAL TRADE ID FORMATTER
 */
function FormattedTradeID({ rawId }: { rawId: string }) {
  const parts = rawId.replace(/^#/, "").split("_");
  if (parts.length < 3) return <span className="font-mono text-[10px] text-slate-500">{rawId}</span>;

  const [type, ticker, dateStr] = parts;
  const isoDate = dateStr.length === 8 
    ? `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`
    : dateStr;

  return (
    <div className="flex items-center gap-2 group cursor-default whitespace-nowrap">
      <span className="px-1.5 py-0.5 rounded border border-slate-700 bg-slate-800 text-slate-400 text-[9px] font-black uppercase tracking-tight">
        {type}
      </span>
      <div className="flex items-center gap-1.5 font-mono text-[10px]">
        <span className="text-white font-bold tracking-tight group-hover:text-emerald-400 transition-colors">
          {ticker}
        </span>
        <span className="text-slate-700">/</span>
        <span className="text-slate-500 tabular-nums">{isoDate}</span>
      </div>
    </div>
  );
}

/* ---------------- MAIN COMPONENT ---------------- */
export default function CollateralTickerAssetClassPage() {
  const { ticker, cls } = useParams();
  const T = String(ticker).toUpperCase();
  const A = String(cls).toUpperCase();

  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(
          `/api/collateral/ticker-asset?ticker=${ticker}&asset_class=${cls}`,
          { cache: "no-store" }
        );
        if (res.ok) setData(await res.json());
      } catch (e) { console.error(e); }
      setLoading(false);
    }
    load();
  }, [T, A]);

  if (loading) return <LoadingState ticker={T} cls={A} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-700 font-black uppercase tracking-[0.5em]">Sync Lost</div>;

  const { date, summary, stress_events } = data;

  // ----------- HUMAN READABLE DATE -----------
  const displayDate = date 
    ? new Date(date).toDateString().toUpperCase()
    : "N/A";

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-10">
      
      {/* HEADER */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/collateral" className="hover:text-emerald-400 italic">Collateral</Link>
          <ChevronRight className="w-2 h-2" />
          <Link href={`/dashboard/collateral/ticker/${T}`} className="hover:text-emerald-400 italic">{T}</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">{A} Ledger</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-emerald-500 font-black text-[10px] tracking-[0.4em] uppercase mb-2">
              <ShieldCheck className="w-4 h-4" /> Margin & Guarantee Node
            </div>
            <h1 className="text-4xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
              {T} <span className="text-slate-800">/</span> <span className="text-sky-500/80">{A}</span>
            </h1>

            {/* UPDATED DATE HERE */}
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Node Status: Operational // {displayDate}
            </p>
          </div>
          
          <div className="flex gap-3">
             <StatusBadge 
                active={summary.margin_stress} 
                label="Stress Mode" 
                sub={summary.margin_stress ? "Breach Detected" : "Compliant"} 
             />
          </div>
        </div>
      </header>

      {/* SUMMARY GRID */}
      <section className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
        <MetricBox label="Gross Exposure" value={fmt(summary.exposure)} />
        <MetricBox label="Required Collateral" value={fmt(summary.required)} />
        <MetricBox label="Effective Held" value={fmt(summary.effective)} color="text-emerald-400" />
        <MetricBox label="Net Residual Exposure" value={fmt(summary.net_exposure)} color="text-red-400" />
        <MetricBox label="Active Margin Calls" value={summary.margin_call_count.toString()} color="text-amber-500" />
        <MetricBox label="Utilization" value={ratioFmt(summary.effective / summary.required)} />
      </section>

      {/* STRESS EVENTS TABLE */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-emerald-500" />
            <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-400 italic">Stress Event Ledger</h2>
          </div>
        </div>

        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[1200px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800 text-[9px] font-black text-slate-500 uppercase tracking-widest">
                  <th className="px-6 py-4">Trade Identifier</th>
                  <th className="px-6 py-4">Counterparty</th>
                  <th className="px-6 py-4"><div className="flex items-center gap-1"><Globe className="w-3 h-3"/> Juris.</div></th>
                  <th className="px-6 py-4">Agreement</th>
                  <th className="px-6 py-4 text-right">Required</th>
                  <th className="px-6 py-4 text-right">Effective</th>
                  <th className="px-6 py-4 text-right text-amber-500">Margin Call</th>
                  <th className="px-6 py-4 text-right">Ratio</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {stress_events.map((e: any, i: number) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      <FormattedTradeID rawId={e.trade_id} />
                    </td>
                    <td className="px-6 py-4">
                      <div className="text-xs font-black text-slate-300 uppercase italic tracking-tight">{cleanID(e.counterparty)}</div>
                    </td>
                    <td className="px-6 py-4">
                      <span className="text-[10px] font-bold text-slate-500 uppercase">{e.jurisdiction}</span>
                    </td>
                    <td className="px-6 py-4">
                      <span className="text-[10px] px-2 py-0.5 rounded bg-slate-800 text-slate-400 border border-slate-700 uppercase font-bold">
                        {e.agreement_type}
                      </span>
                    </td>
                    <td className="px-6 py-4 text-right text-xs font-mono">{fmt(e.required_collateral)}</td>
                    <td className="px-6 py-4 text-right text-xs font-mono text-emerald-400/80">{fmt(e.effective_collateral)}</td>
                    <td className="px-6 py-4 text-right text-xs font-mono font-bold text-amber-500">
                      {e.margin_call_amount > 0 ? fmt(e.margin_call_amount) : "—"}
                    </td>
                    <td className="px-6 py-4 text-right">
                       <span className={`text-[10px] font-black tabular-nums ${e.collateral_ratio < 1 ? 'text-red-500' : 'text-slate-400'}`}>
                         {ratioFmt(e.collateral_ratio)}
                       </span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* FOOTER */}
      <footer className="pt-6 border-t border-slate-800/50">
        <Link 
          href={`/dashboard/collateral/ticker/${T}`}
          className="inline-flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-600 hover:text-white transition-all"
        >
          <ArrowLeft className="w-3 h-3" /> Exit Ledger
        </Link>
      </footer>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricBox({ label, value, color = "text-white" }: any) {
  return (
    <div className="p-4 bg-slate-900/50 border border-slate-800 rounded-2xl">
      <p className="text-slate-500 uppercase tracking-widest text-[8px] font-black mb-1.5">{label}</p>
      <p className={`text-xl font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function StatusBadge({ active, label, sub }: any) {
  return (
    <div className={`px-5 py-3 rounded-2xl border transition-all flex flex-col items-center ${
      active ? 'bg-red-500/10 border-red-500/30 animate-pulse' : 'bg-slate-900 border-slate-800'
    }`}>
      <span className="text-[7px] font-black uppercase tracking-[0.2em] text-slate-500 mb-1">{label}</span>
      <div className="flex items-center gap-2">
        <div className={`w-1.5 h-1.5 rounded-full ${active ? 'bg-red-500 animate-ping' : 'bg-emerald-500'}`} />
        <span className={`text-xs font-black italic uppercase ${active ? 'text-red-500' : 'text-emerald-500'}`}>{sub}</span>
      </div>
    </div>
  );
}

function LoadingState({ ticker, cls }: { ticker: string, cls: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Zap className="w-6 h-6 text-emerald-500 animate-pulse" />
      <p className="text-center text-[9px] font-black text-slate-600 uppercase tracking-[0.4em]">
        Querying {ticker}::{cls} Margin Ledger
      </p>
    </div>
  );
}
