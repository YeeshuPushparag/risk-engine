"use client";

import React, { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import {
  ShieldAlert,
  Download,
  ArrowUpRight,
  LayoutGrid,
  Zap,
  Calendar,
  MousePointer2,
  Globe,
  AlertTriangle,
  History,
  FileText
} from "lucide-react";

/* ---------------- HELPERS ---------------- */
const getStatusStyles = (status: string) => {
  switch (status) {
    case "Hard Breach": return "bg-red-500/10 text-red-400 border-red-500/30";
    case "Warning": return "bg-amber-500/10 text-amber-400 border-amber-500/30";
    default: return "bg-emerald-500/10 text-emerald-400 border-emerald-500/30";
  }
};

const SEGMENT_LINKS: Record<string, string> = {
  Equities: "/dashboard/equity",
  FX: "/dashboard/fx",
  Bonds: "/dashboard/bonds",
  Commodities: "/dashboard/commodities",
  Derivatives: "/dashboard/derivatives",
  Collateral: "/dashboard/collateral",
  Loans: "/dashboard/loans",
};

const SEGMENT_CODES: Record<string, string> = {
  Equities: "EQ",
  FX: "FX",
  Bonds: "FI",
  Commodities: "CM",
  Derivatives: "DX",
  Collateral: "CL",
  Loans: "LN",
};

const formatDateFull = (dateStr: string) => {
  if (!dateStr) return "---";
  const d = new Date(dateStr);
  return d.toLocaleDateString('en-US', {
    weekday: 'long',
    year: 'numeric',
    month: 'short',
    day: 'numeric'
  });
};

export default function LimitsPage() {
  const router = useRouter();
  const [data, setData] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  // --- NEW STATE FOR TOGGLE ---
  const [isArchive, setIsArchive] = useState(false);

  // --- UPDATED FETCH LOGIC ---
  const fetchLimits = useCallback(async () => {
    setLoading(true);
    try {
      // Passes the archive state as a query parameter to your Route Handler
      const response = await fetch(`/api/limits?archive=${isArchive}`, {
        cache: "no-store"
      });
      const result = await response.json();
      setData(result);
    } catch (error) {
      console.error("Fetch error:", error);
    } finally {
      setLoading(false);
    }
  }, [isArchive]);

  useEffect(() => {
    fetchLimits();
  }, [fetchLimits]);

  const handleExportPDF = () => {
    window.print();
  };

  const formatNum = (val: number) => {
    if (val === null || val === undefined) return "---";
    if (Math.abs(val) < 1 && Math.abs(val) > 0) return (val * 100).toFixed(2) + "%";
    if (Math.abs(val) >= 1_000_000_000_000) return (val / 1_000_000_000_000).toFixed(1) + "T";
    if (Math.abs(val) >= 1_000_000_000) return (val / 1_000_000_000).toFixed(1) + "B";
    if (Math.abs(val) >= 1_000_000) return (val / 1_000_000).toFixed(1) + "M";
    return val.toLocaleString("en-US");
  };

  if (loading) return <LoadingState />;
  if (!data) return <div className="h-screen bg-[#020617] flex items-center justify-center text-slate-700 font-black uppercase tracking-[0.5em]">Sync Lost</div>;

  const { date, summary, limits = [], violations = [], governance } = data;

  return (
    <div className="min-h-screen bg-[#020617] text-slate-300 pb-20 p-4 md:p-8 lg:p-12 print:bg-white print:text-black">

      {/* HEADER SECTION */}
      <header className="max-w-[1600px] mx-auto mb-12 space-y-6">
        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-6 border-b border-slate-800/50 pb-8 print:border-black">
          <div className="space-y-1">
            <div className="flex items-center gap-2 text-red-500 font-black text-[10px] tracking-[0.4em] uppercase mb-2 print:text-red-600">
              <ShieldAlert className="w-4 h-4" /> Policy Enforcement Terminal
            </div>
            <h1 className="text-4xl sm:text-7xl font-black text-white italic tracking-tighter uppercase leading-none print:text-black">
              Risk <span className="text-slate-800 print:text-slate-400">/</span> Limits
            </h1>
            <div className="flex flex-wrap items-center gap-4 mt-4 print:mt-2">
              <div className="flex items-center gap-2 px-3 py-1 bg-slate-900 border border-slate-800 rounded-full print:bg-white print:border-black">
                <Calendar className="w-3 h-3 text-blue-500" />
                <span className="text-[10px] font-black uppercase text-slate-300 tracking-wider print:text-black">
                  {formatDateFull(date)}
                </span>
              </div>
              <div className="flex items-center gap-2 px-3 py-1 bg-slate-900 border border-slate-800 rounded-full print:bg-white print:border-black">
                <Globe
                  className={
                    governance?.regime_color === "red"
                      ? "w-3 h-3 text-red-500 print:text-red-700"
                      : governance?.regime_color === "amber"
                        ? "w-3 h-3 text-amber-400 print:text-amber-700"
                        : "w-3 h-3 text-emerald-400 print:text-emerald-700"
                  }
                />
                <span className="text-[10px] font-black uppercase text-slate-300 tracking-wider print:text-black">
                  Regime:{" "}
                  <span
                    className={
                      governance?.regime_color === "red"
                        ? "text-red-500 print:text-red-700"
                        : governance?.regime_color === "amber"
                          ? "text-amber-400 print:text-amber-700"
                          : "text-emerald-400 print:text-emerald-700"
                    }
                  >
                    {governance?.regime_mode}
                  </span>
                </span>
              </div>

              {/* VISUAL INDICATOR FOR ARCHIVE MODE */}
              {isArchive && (
                <div className="flex items-center gap-2 px-3 py-1 bg-amber-500/10 border border-amber-500/30 rounded-full animate-pulse">
                  <History className="w-3 h-3 text-amber-500" />
                  <span className="text-[10px] font-black uppercase text-amber-500 tracking-wider">Historical Archive</span>
                </div>
              )}
            </div>
          </div>

          <div className="flex gap-3 shrink-0 print:hidden">
            {/* TOGGLE BUTTON WITH DYNAMIC TEXT AND STYLE */}
            <button
              onClick={() => setIsArchive(!isArchive)}
              className={`cursor-pointer px-5 py-3 border rounded-xl text-[9px] font-black uppercase tracking-widest transition-all ${isArchive
                  ? "bg-amber-600 border-amber-500 text-white shadow-[0_0_20px_rgba(217,119,6,0.3)] hover:bg-amber-500"
                  : "bg-slate-900 border-slate-800 text-slate-300 hover:border-slate-600"
                }`}
            >
              {isArchive ? "Switch to Latest" : "Archive History"}
            </button>
            <button
              onClick={handleExportPDF}
              className="cursor-pointer px-5 py-3 bg-blue-600 text-white rounded-xl text-[9px] font-black uppercase tracking-widest shadow-[0_0_20px_rgba(37,99,235,0.25)] hover:bg-blue-500 transition-all"
            >
              <Download className="w-3 h-3 inline mr-2" /> Export PDF
            </button>
          </div>
        </div>
      </header>

      <main className="max-w-[1600px] mx-auto space-y-12">

        {/* SUMMARY METRICS */}
        <section className="grid grid-cols-1 md:grid-cols-3 gap-6 print:grid-cols-3">
          <MetricBox
            label="Critical Breaches"
            value={summary.critical_breaches}
            sub="Hard Limit Violations"
            color={summary.critical_breaches > 0 ? "text-red-500 animate-pulse print:text-red-600" : "text-white print:text-black"}
          />
          <MetricBox
            label="Compliance Score"
            value={`${summary.compliance_score}%`}
            sub="Aggregate Health Index"
            color={summary.compliance_score < 70 ? "text-amber-500 print:text-amber-700" : "text-emerald-400 print:text-emerald-700"}
          />
          <MetricBox
            label="Gov. Framework"
            value="Internal"
            sub={governance.framework}
            color="text-blue-400 print:text-blue-700"
          />
        </section>

        {/* POLICY LEDGER TABLE */}
        <section className="space-y-4">
          <div className="flex justify-between items-center px-2">
            <div className="flex items-center gap-2">
              <LayoutGrid className="w-4 h-4 text-blue-500" />
              <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-400 italic print:text-black">EOD Exposure Thresholds</h2>
            </div>
            <div className="lg:hidden flex items-center gap-2 text-[9px] font-black uppercase text-slate-600 animate-pulse print:hidden">
              <MousePointer2 className="w-3 h-3" /> Scroll to Inspect
            </div>
          </div>

          <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-sm print:bg-white print:border-black">
            <div className="overflow-x-auto terminal-scroll">
              <table className="w-full text-left border-collapse min-w-[1000px]">
                <thead>
                  <tr className="bg-slate-800/30 border-b border-slate-800/50 print:bg-slate-100 print:border-black">
                    <th className="px-6 py-5 text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] print:text-black">Asset Segment</th>
                    <th className="px-6 py-5 text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] print:text-black">Risk Category</th>
                    <th className="px-6 py-5 text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] text-right print:text-black">EOD Limit</th>
                    <th className="px-6 py-5 text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] text-right print:text-black">EOD Actual</th>
                    <th className="px-6 py-5 text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] print:text-black">Utilization</th>
                    <th className="px-6 py-5 text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] text-right print:text-black">Policy Status</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-slate-800/30 print:divide-slate-200">
                  {limits.map((item: any, i: number) => {
                    const util = (item.actual / item.limit) * 100;
                    const link = SEGMENT_LINKS[item.segment];
                    const isFirm = item.segment === "Firm";

                    return (
                      <tr
                        key={i}
                        onClick={() => !isFirm && link && router.push(link)}
                        className={`transition-colors group ${!isFirm && link ? 'cursor-pointer hover:bg-blue-500/5' : ''}`}
                      >
                        <td className="px-6 py-5">
                          <div className="flex items-center gap-3">
                            <span className="text-xs font-black text-white italic uppercase tracking-tight print:text-black">{item.segment}</span>
                            {!isFirm && link && <ArrowUpRight className="w-3 h-3 text-slate-700 group-hover:text-blue-400 transition-colors print:hidden" />}
                          </div>
                        </td>
                        <td className="px-6 py-5 text-[10px] font-bold text-slate-500 uppercase tracking-tighter print:text-slate-600">{item.type}</td>
                        <td className="px-6 py-5 text-right text-xs font-mono tabular-nums print:text-black">{formatNum(item.limit)}</td>
                        <td className="px-6 py-5 text-right text-xs font-mono font-bold text-white tabular-nums print:text-black">{formatNum(item.actual)}</td>
                        <td className="px-6 py-5">
                          <div className="flex items-center gap-3 w-32">
                            <div className="flex-1 h-1 bg-slate-800/50 rounded-full overflow-hidden print:bg-slate-200">
                              <div
                                className={`h-full transition-all duration-700 ${util >= 100 ? 'bg-red-500' : util > 85 ? 'bg-amber-500' : 'bg-emerald-500'
                                  }`}
                                style={{ width: `${Math.min(util, 100)}%` }}
                              />
                            </div>
                            <span className={`text-[9px] font-mono font-bold ${util >= 100 ? 'text-red-400 print:text-red-600' : 'text-slate-500'}`}>
                              {util > 999 ? '>1k' : util.toFixed(0)}%
                            </span>
                          </div>
                        </td>
                        <td className="px-6 py-5 text-right">
                          <span className={`px-2 py-1 rounded text-[8px] font-black uppercase border tracking-tighter ${getStatusStyles(item.status)} print:border-black print:text-black`}>
                            {item.status}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        </section>

        {/* VIOLATION STREAM */}
        <section className="grid grid-cols-1 lg:grid-cols-3 gap-8 print:block">
          <div className="lg:col-span-2 space-y-4">
            <div className="flex items-center gap-2 px-2">
              <AlertTriangle className="w-4 h-4 text-red-500" />
              <h2 className="text-[10px] font-black uppercase tracking-widest text-slate-400 italic print:text-black">
                {isArchive ? "Historical Breach Archive" : "Active Breach Ledger"}
              </h2>
            </div>
            <div className="bg-red-500/5 border border-red-500/10 rounded-2xl divide-y divide-red-500/10 overflow-hidden print:bg-white print:border-black print:divide-slate-200">
              {violations.length > 0 ? violations.map((v: any, i: number) => {
                const vLink = SEGMENT_LINKS[v.segment];
                const sCode = SEGMENT_CODES[v.segment] || "XX";
                return (
                  <div
                    key={i}
                    onClick={() => vLink && router.push(vLink)}
                    className={`p-5 flex justify-between items-center group transition-all ${vLink ? 'cursor-pointer hover:bg-red-500/10' : ''}`}
                  >
                    <div className="space-y-1">
                      <p className="text-sm font-black text-white italic uppercase tracking-tight leading-none mb-1 group-hover:text-red-400 transition-colors print:text-black">
                        {v.message}
                      </p>
                      <div className="flex items-center gap-2 text-[9px] uppercase font-bold text-slate-500 tracking-widest print:text-slate-700">
                        <span className="text-red-500/80 print:text-red-600">Segment: {v.segment}</span>
                        <span className="text-slate-800">|</span>
                        <span className="text-blue-400/70 font-mono print:text-blue-800">
                          ID: EOD-{date.replace(/-/g, '')} / {sCode}-BREACH
                        </span>
                      </div>
                    </div>
                    <div className="flex items-center gap-4 print:hidden">
                      <div className="hidden sm:block px-3 py-1 bg-red-500/10 border border-red-500/20 text-red-500 text-[8px] font-black uppercase rounded tracking-tighter">
                        Action Required
                      </div>
                      <ArrowUpRight className="w-4 h-4 text-slate-700 group-hover:text-red-500 transition-colors" />
                    </div>
                  </div>
                );
              }) : (
                <div className="p-10 text-center text-[10px] font-black uppercase tracking-widest text-slate-600">
                  No violations found in this period
                </div>
              )}
            </div>
          </div>

          <div className="bg-slate-900/60 border border-slate-800 rounded-2xl p-6 space-y-6 self-start print:mt-8 print:bg-white print:border-black">
            <div className="flex items-center gap-2 text-blue-400 print:text-black">
              <History className="w-4 h-4" />
              <h3 className="text-[10px] font-black uppercase tracking-widest">Archival Meta</h3>
            </div>
            <p className="text-[10px] text-slate-500 leading-relaxed uppercase font-bold italic print:text-black">
              {isArchive
                ? `Viewing historical snapshot for ${formatDateFull(date)}. These records are frozen.`
                : `Finalized figures as of ${formatDateFull(date)}. All historical breaches are logged in the immutable firm-wide compliance chain.`
              }
            </p>
            <button
              onClick={() => setIsArchive(!isArchive)}
              className="cursor-pointer w-full py-3 bg-slate-800 text-white rounded-xl text-[9px] font-black uppercase tracking-[0.2em] border border-slate-700 hover:bg-slate-700 transition-all shadow-lg print:hidden"
            >
              {isArchive ? "Switch to Latest View" : "View Full Archive"}
            </button>
            <div className="hidden print:block text-[8px] uppercase font-black pt-4 border-t border-black">
              Auth Signature: ___________________________
            </div>
          </div>
        </section>
      </main>
    </div>
  );
}

/* ---------------- UI SUB-COMPONENTS ---------------- */

function MetricBox({ label, value, sub, color }: any) {
  return (
    <div className="bg-slate-900/40 border border-slate-800 p-6 rounded-2xl flex flex-row items-center gap-6 group hover:border-slate-700 transition-all print:border-black print:bg-white">
      <div className="shrink-0">
        <span className={`text-4xl sm:text-5xl font-black italic tracking-tighter tabular-nums ${color}`}>
          {typeof value === 'number' ? String(value).padStart(2, "0") : value}
        </span>
      </div>
      <div className="flex flex-col min-w-0">
        <p className="text-[9px] font-black uppercase tracking-[0.2em] text-slate-500 mb-1 print:text-black">{label}</p>
        <p className="text-[9px] font-bold uppercase text-slate-600 italic tracking-widest leading-tight print:text-slate-800">
          {sub}
        </p>
      </div>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-3">
      <Zap className="w-6 h-6 text-blue-500 animate-pulse" />
      <p className="text-[9px] font-black text-slate-600 uppercase tracking-[0.4em] italic">Finalizing EOD Compliance Ledger</p>
    </div>
  );
}