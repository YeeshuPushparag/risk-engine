"use client";

import { useParams } from "next/navigation";
import { useEffect, useState } from "react";
import Link from "next/link";
import {
  ChevronRight,
  Terminal,
  ArrowLeft,
  Zap,
  Activity,
  History,
  AlertTriangle, ShieldCheck
} from "lucide-react";

/* ---------------- TYPES ---------------- */
type Trade = {
  trade_id: string;
  derivative_type: string;
  notional: number;
  net_exposure: number;
  delta_equivalent_exposure: number;
  tenor_years: number;
  margin_call_flag: boolean;
  margin_call_amount: number;
  pnl: number;
};

type AssetClassData = {
  date: string;
  summary: {
    total_notional: number;
    total_net_exposure: number;
    delta_equivalent: number;
    margin_stress: boolean;
    margin_call_amount: number;
    total_pnl: number;
  };
  trades: Trade[];
};

/* ---------------- FORMATTERS & UI LOGIC ---------------- */
const fmt = (n?: number | null) =>
  n == null ? "—" : new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    notation: "compact",
    maximumFractionDigits: 2,
  }).format(n);

/**
 * INSTITUTIONAL TRADE ID FORMATTER
 * Converts "#FXFwd_AMZN_20251219" -> [TYPE] TICKER | YYYY-MM-DD
 */
function FormattedTradeID({ rawId }: { rawId: string }) {
  // 1. Clean and split the ID
  const parts = rawId.replace(/^#/, "").split("_");

  if (parts.length < 3) {
    return <span className="font-mono text-[10px] text-slate-500">{rawId}</span>;
  }

  const [type, ticker, dateStr] = parts;

  // 2. Format Date (YYYYMMDD to YYYY-MM-DD)
  const isoDate = dateStr.length === 8
    ? `${dateStr.slice(0, 4)}-${dateStr.slice(4, 6)}-${dateStr.slice(6, 8)}`
    : dateStr;

  // 3. Asset-specific color scheme (Matching Bloomberg/Terminal standards)
  const getPrefixStyles = (t: string) => {
    const p = t.toLowerCase();
    if (p.includes('fx')) return "bg-emerald-500/10 text-emerald-400 border-emerald-500/30";
    if (p.includes('swp') || p.includes('irs')) return "bg-purple-500/10 text-purple-400 border-purple-500/30";
    if (p.includes('opt')) return "bg-sky-500/10 text-sky-400 border-sky-500/30";
    return "bg-slate-800 text-slate-400 border-slate-700";
  };

  return (
    <div className="flex items-center gap-2 group cursor-default whitespace-nowrap">
      {/* Product Tag */}
      <span className={`px-1.5 py-0.5 rounded border text-[9px] font-black uppercase tracking-tight ${getPrefixStyles(type)}`}>
        {type}
      </span>
      {/* Ticker & ISO Date Group */}
      <div className="flex items-center gap-1.5 font-mono text-[10px]">
        <span className="text-white font-bold tracking-tight group-hover:text-emerald-400 transition-colors">
          {ticker}
        </span>
        <span className="text-slate-700">/</span>
        <span className="text-slate-500 tabular-nums">
          {isoDate}
        </span>
      </div>
    </div>
  );
}

/* ---------------- MAIN COMPONENT ---------------- */
export default function DerivativesTickerAssetClassPage() {
  const params = useParams();
  const ticker = String(params.ticker).toUpperCase();
  const cls = String(params["cls"]);
  const assetClass = String(params["cls"]).toUpperCase();

  const [data, setData] = useState<AssetClassData | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch(
          `/api/derivatives/ticker-asset?ticker=${ticker}&asset_class=${cls}`,
          { cache: "no-store" }
        );
        if (res.ok) setData(await res.json());
      } catch (e) {
        console.error("Fetch failed", e);
      }
      setLoading(false);
    }
    load();
  }, [ticker, assetClass]);

  if (loading) return <LoadingState ticker={ticker} cls={assetClass} />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-500 font-black uppercase tracking-[0.4em] text-xs italic">Node Desynchronized</div>;

  const { date, summary, trades } = data;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-8 lg:p-12 space-y-10">

      {/* HEADER & NAV */}
      <header className="space-y-6">
        <nav className="flex items-center gap-1 text-[9px] font-black uppercase tracking-widest text-slate-500">
          <Link href="/dashboard/derivatives" className="hover:text-emerald-400 transition-colors">Derivatives</Link>
          <ChevronRight className="w-2 h-2" />
          <Link href={`/dashboard/derivatives/ticker/${ticker}`} className="hover:text-emerald-400 transition-colors">{ticker}</Link>
          <ChevronRight className="w-2 h-2" />
          <span className="text-slate-300">{assetClass} Inventory</span>
        </nav>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8">
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-emerald-500 font-black text-[10px] tracking-[0.4em] uppercase mb-2">
              <ShieldCheck className="w-4 h-4" strokeWidth={3} /> Asset Class Ledger
            </div>
            <h1 className="text-3xl sm:text-6xl font-black text-white italic tracking-tighter uppercase leading-none">
              {ticker} <span className="text-slate-700">/</span> {assetClass}
            </h1>
            <p className="text-[10px] font-bold text-slate-500 tracking-widest uppercase italic">
              Snapshot Date: {new Date(date).toDateString().toUpperCase()}
            </p>
          </div>

          <div className="flex gap-3">
            <StatusBadge
              active={summary.margin_stress}
              label="Margin Risk"
              sub={summary.margin_stress ? "Critical" : "Nominal"}
            />
          </div>
        </div>
      </header>

      {/* SUMMARY DASHBOARD */}
      <section className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
        <MetricBox label="Notional Volume" value={fmt(summary.total_notional)} />
        <MetricBox label="Net Exposure" value={fmt(summary.total_net_exposure)} />
        <MetricBox label="Delta Equivalent" value={fmt(summary.delta_equivalent)} color="text-sky-400" />
        <MetricBox label="Margin Required" value={fmt(summary.margin_call_amount)} color={summary.margin_call_amount > 0 ? "text-red-500" : "text-slate-500"} />
        <MetricBox label="Inventory P&L" value={fmt(summary.total_pnl)} color={summary.total_pnl >= 0 ? "text-emerald-400" : "text-red-400"} />
        <MetricBox label="Trade Count" value={trades.length.toString()} />
      </section>

      {/* TRADE INVENTORY LEDGER */}
      <section className="space-y-4">
        <div className="flex justify-between items-center px-2">
          <div className="flex items-center gap-2">
            <History className="w-4 h-4 text-emerald-500" />
            <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-400">Inventory Records</h2>
          </div>
          <ScrollPill />
        </div>

        <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm shadow-2xl">
          <div className="overflow-x-auto terminal-scroll">
            <table className="w-full text-left border-collapse min-w-[1100px]">
              <thead>
                <tr className="bg-slate-800/30 border-b border-slate-800">
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Structured Trade ID</th>
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Instrument</th>
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Notional</th>
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Net Exp.</th>
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Delta Eq.</th>
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest">Tenor</th>
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-center">Margin Status</th>
                  <th className="px-6 py-4 text-[9px] font-black text-slate-500 uppercase tracking-widest text-right">P&L</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {trades.map((t, i) => (
                  <tr key={i} className="hover:bg-slate-800/20 transition-colors group">
                    <td className="px-6 py-4">
                      {/* Institutional ID Deconstruction */}
                      <FormattedTradeID rawId={t.trade_id} />
                    </td>
                    <td className="px-6 py-4">
                      <div className="text-xs font-black text-white italic uppercase">{t.derivative_type}</div>
                    </td>
                    <td className="px-6 py-4 text-xs font-mono whitespace-nowrap">{fmt(t.notional)}</td>
                    <td className="px-6 py-4 text-xs font-mono whitespace-nowrap">{fmt(t.net_exposure)}</td>
                    <td className="px-6 py-4 text-xs font-mono text-sky-400/80 whitespace-nowrap">{fmt(t.delta_equivalent_exposure)}</td>
                    <td className="px-6 py-4">
                      <span className="text-xs font-black text-slate-400 italic">
                        {t.tenor_years?.toFixed(1)} <span className="text-[8px] font-normal not-italic text-slate-600">Y</span>
                      </span>
                    </td>
                    <td className="px-6 py-4">
                      {t.margin_call_flag ? (
                        <div className="flex items-center justify-center gap-2 text-red-500 animate-pulse">
                          <AlertTriangle className="w-3 h-3" />
                          <span className="text-[10px] font-black uppercase italic">{fmt(t.margin_call_amount)}</span>
                        </div>
                      ) : (
                        <div className="text-center">
                          <span className="text-[9px] font-bold text-slate-600 uppercase tracking-tighter">Compliant</span>
                        </div>
                      )}
                    </td>
                    <td className={`px-6 py-4 text-right text-xs font-mono font-bold ${t.pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                      {fmt(t.pnl)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      {/* FOOTER ACTION */}
      <footer className="pt-6 border-t border-slate-800/50">
        <Link
          href={`/dashboard/derivatives/ticker/${ticker}`}
          className="inline-flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.2em] text-slate-600 hover:text-white transition-colors"
        >
          <ArrowLeft className="w-3 h-3" /> Terminate Ledger View
        </Link>
      </footer>
    </main>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricBox({ label, value, color = "text-white" }: any) {
  return (
    <div className="p-4 bg-slate-900/50 border border-slate-800 rounded-2xl hover:border-slate-700 transition-all">
      <p className="text-slate-500 uppercase tracking-widest text-[8px] font-black mb-1.5">{label}</p>
      <p className={`text-xl font-black italic tracking-tighter ${color}`}>{value}</p>
    </div>
  );
}

function StatusBadge({ active, label, sub }: any) {
  return (
    <div className={`px-5 py-3 rounded-2xl border transition-all flex flex-col items-center ${active ? 'bg-red-500/10 border-red-500/30' : 'bg-slate-900 border-slate-800'
      }`}>
      <span className="text-[7px] font-black uppercase tracking-[0.2em] text-slate-500 mb-1">{label}</span>
      <div className="flex items-center gap-2">
        <div className={`w-1.5 h-1.5 rounded-full ${active ? 'bg-red-500 animate-ping' : 'bg-emerald-500'}`} />
        <span className={`text-xs font-black italic uppercase ${active ? 'text-red-500' : 'text-emerald-500'}`}>{sub}</span>
      </div>
    </div>
  );
}

function ScrollPill() {
  return (
    <div className="lg:hidden flex items-center gap-2 bg-emerald-500/10 px-2 py-1 rounded border border-emerald-500/20">
      <span className="text-[8px] font-black text-emerald-400 uppercase tracking-tighter">Swipe Inventory</span>
      <div className="w-1 h-1 bg-emerald-500 rounded-full animate-ping" />
    </div>
  );
}

function LoadingState({ ticker, cls }: { ticker: string, cls: string }) {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Zap className="w-6 h-6 text-emerald-500 animate-pulse" />
      <p className="text-[9px] font-black text-slate-600 uppercase tracking-[0.4em]">Decoupling {ticker}::{cls} Trade Tree</p>
    </div>
  );
}