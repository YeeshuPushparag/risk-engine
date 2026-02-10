"use client";

import React, { useEffect, useState, useCallback } from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { 
  ArrowLeft, Cpu, Globe, TrendingUp, Layers,
  Activity, AlertCircle, Zap, ChevronRight, RefreshCcw
} from "lucide-react";
import StressTestingControls from "@/components/StressTesting";

/* --------------------------- Specialized Formatters --------------------------- */


const formatCurrency = (v?: number | null) => {
  if (v == null || Number.isNaN(v)) return "—";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(2)}`;
};

const pctFmt = (v?: number | null, d = 2) => v == null ? "—" : `${(v * 100).toFixed(d)}%`;

/* --------------------------- UI Atomic Components --------------------------- */

const ForecastCard = ({ label, value, trend }: any) => (
  <div className="text-center p-3 bg-slate-900/40 border border-slate-800 rounded-xl">
    <p className="text-[8px] font-black text-slate-500 uppercase mb-1 tracking-wider">{label}</p>
    <p className={`text-xs sm:text-sm font-mono font-bold ${trend === 'up' ? 'text-emerald-400' : 'text-red-400'}`}>{value}</p>
  </div>
);

const TrajectoryBox = ({ label, value, highlight }: any) => {
  const isPos = !String(value).includes('-');
  return (
    <div className={`relative p-5 sm:p-6 rounded-2xl border transition-all duration-500 ${
      highlight 
        ? 'bg-blue-600/10 border-blue-500/50 shadow-[0_0_40px_-15px_rgba(59,130,246,0.3)]' 
        : 'bg-slate-900/40 border-slate-800'
    }`}>
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-2">{label}</p>
      <p className={`text-2xl sm:text-4xl font-mono font-bold tracking-tighter ${isPos ? 'text-emerald-400' : 'text-red-400'}`}>
        {value}
      </p>
      {highlight && (
        <div className="absolute top-3 right-3 flex items-center gap-1.5">
          <div className="w-1.5 h-1.5 rounded-full bg-blue-500 animate-ping" />
          <span className="text-[7px] font-black bg-blue-500 text-white px-1.5 py-0.5 rounded uppercase">Primary Horizon</span>
        </div>
      )}
    </div>
  );
};

/* --------------------------- Main Intelligence Page --------------------------- */

export default function PositionPage() {
  const params = useParams();
  const ticker = String(params.ticker).toUpperCase();
  const manager = decodeURIComponent(String(params.manager));

  const [stressParams, setStressParams] = useState<Record<string, any>>({});
  const [data, setData] = useState<any>(null);
  const [date, setDate] = useState<any>(null);
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const dateRes = await fetch("/api/equity/daily/main/latest-date");
      const { latest_date } = await dateRes.json();
      const qs = new URLSearchParams({ date: latest_date, ticker, manager, ...stressParams }).toString();
      setDate(latest_date);
      const [summary, intel, alerts] = await Promise.all([
        fetch(`/api/equity/daily/ticker/manager/position-summary?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/manager/position-intelligence?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/manager/position-alerts?${qs}`).then(r => r.json()),
      ]);
      setData({ summary, intel, alerts: Array.isArray(alerts) ? alerts : [] });
    } catch (e) { console.error(e); } finally { setLoading(false); }
  }, [ticker, manager, stressParams]);

  useEffect(() => { load(); }, [load]);

  if (!data) return <LoadingTerminal />;
  const { summary, intel, alerts } = data;
  const displayDate = new Date(date).toDateString().toUpperCase();
  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 pb-20 font-sans selection:bg-blue-500/30">
      
      {/* HEADER - Consistent with Security Page */}
      <header className="sticky top-0 z-[100] bg-[#020617]/90 backdrop-blur-xl border-b border-slate-800 px-4 sm:px-8 py-3">
        <div className="max-w-[1600px] mx-auto flex justify-between items-center gap-4">
          <div className="flex items-center gap-3 min-w-0">
            <Link 
              href={`/dashboard/equity/daily/ticker/${ticker}`} 
              className="p-2 bg-slate-900 rounded-lg border border-slate-800 hover:bg-slate-800 transition-all cursor-pointer group"
              aria-label="Back to Ticker"
            >
              <ArrowLeft className="w-4 h-4 text-slate-500 group-hover:text-white" />
            </Link>
            <div className="min-w-0">
              <div className="flex items-center gap-1 text-[7px] sm:text-[9px] font-black text-slate-600 uppercase tracking-widest mb-0.5">
                <span>{ticker}</span> <ChevronRight size={10} /> <span className="text-blue-500">Position Monitor</span>
              </div>
              
              <div className="flex flex-wrap items-center gap-x-2 sm:gap-x-3 gap-y-0.5">
                <h1 className="text-base sm:text-2xl font-black text-white tracking-tighter uppercase italic leading-none shrink-0">
                  Intellicense
                </h1>
                <span className="hidden sm:inline text-slate-800 font-thin text-xl">|</span>
                <span className="text-[10px] sm:text-lg font-bold text-slate-400 tracking-tight truncate max-w-[120px] sm:max-w-none">
                  {manager}
                </span>
                {summary.is_stressed && (
                  <span className="bg-red-500 text-white text-[7px] font-black px-1.5 py-0.5 rounded animate-pulse shrink-0 uppercase">
                    Stressed
                  </span>
                )}
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2 sm:gap-4 shrink-0">
            <div className="text-right">
              <p className="text-[7px] font-black text-slate-600 uppercase tracking-widest">EOD Snapshot</p>
              <p className="text-[10px] sm:text-xs font-mono font-bold text-slate-400">{displayDate}</p>
            </div>
            <button 
              onClick={load} 
              className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-500 transition-all cursor-pointer shadow-lg shadow-blue-900/20 active:scale-95"
              aria-label="Refresh Data"
            >
              <RefreshCcw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
            </button>
          </div>
        </div>
      </header>

      {/* PERFORMANCE RIBBON */}
      <section className="bg-slate-900/30 border-b border-slate-800/60 backdrop-blur-sm">
        <div className="max-w-[1600px] mx-auto px-4 sm:px-8 py-4">
          <div className="flex flex-wrap items-center gap-6 sm:gap-16">
            <div className="flex flex-col gap-0.5">
              <span className="text-[9px] font-black text-slate-500 uppercase tracking-widest">Net Exposure</span>
              <span className="text-xl sm:text-2xl font-mono font-black text-white tracking-tighter">{formatCurrency(summary.exposure_usd)}</span>
            </div>
            <div className="hidden sm:block h-10 w-px bg-slate-800" />
            <div className="flex flex-col gap-0.5">
              <span className="text-[9px] font-black text-slate-500 uppercase tracking-widest">{summary.is_stressed ? "Proj. P&L" : "Daily P&L"}</span>
              <span className={`text-xl sm:text-2xl font-mono font-black tracking-tighter ${summary.daily_pnl_usd >= 0 ? "text-emerald-400" : "text-rose-500"}`}>{formatCurrency(summary.daily_pnl_usd)}</span>
            </div>
            <div className="hidden sm:block h-10 w-px bg-slate-800" />
            <div className="flex flex-col gap-0.5">
              <span className="text-[9px] font-black text-slate-500 uppercase tracking-widest">Portf. Weight</span>
              <span className="text-xl sm:text-2xl font-mono font-black text-blue-500 tracking-tighter">{pctFmt(summary.portfolio_weight)}</span>
            </div>
          </div>
        </div>
      </section>

      <div className="max-w-[1600px] mx-auto p-4 sm:p-8 space-y-6">
        
        {/* SIMULATION & REGIME */}
        <div className="grid grid-cols-1 lg:grid-cols-4 gap-6 items-start">
          <div className="lg:col-span-3 bg-slate-900/40 border border-slate-800 rounded-2xl p-5 sm:p-6 backdrop-blur-md">
            <div className="flex items-center gap-2 mb-4 text-slate-500">
              <Zap size={14} className="text-blue-500" />
              <span className="text-[9px] font-black uppercase tracking-widest">Simulation Parameters</span>
            </div>
            <div className="[&_button]:cursor-pointer [&_select]:cursor-pointer">
              <StressTestingControls onChange={setStressParams} />
            </div>
          </div>
          
          <div className={`rounded-2xl p-6 flex flex-col justify-between border min-h-[160px] backdrop-blur-md transition-all ${
            intel.pred_macro_regime === 0 ? "bg-red-500/5 border-red-500/20" : 
            intel.pred_macro_regime === 2 ? "bg-emerald-500/5 border-emerald-500/20" : "bg-blue-500/5 border-blue-500/20"
          }`}>
            <div className="flex justify-between items-start">
              <Globe className={intel.pred_macro_regime === 0 ? "text-red-500" : "text-emerald-500"} size={20} />
              <span className="text-[9px] font-black text-slate-600 uppercase text-right">Market<br/>Regime</span>
            </div>
            <div className="mt-4">
              <p className="text-2xl font-black text-white leading-tight uppercase italic">
                {intel.pred_macro_regime === 0 ? "Risk-Off" : intel.pred_macro_regime === 1 ? "Neutral" : "Risk-On"}
              </p>
              <p className="text-[8px] font-black text-slate-500 mt-1 uppercase">
                {intel.pred_macro_regime === 0 ? "Structural Defensive" : "Risk Accrual Mode"}
              </p>
            </div>
          </div>
        </div>

        {/* DATA GRID */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 items-start">
          
          {/* Predictive */}
          <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden">
            <div className="px-6 py-4 border-b border-slate-800 bg-slate-800/10 flex justify-between items-center">
              <h3 className="text-[9px] font-black text-slate-500 uppercase tracking-widest">Predictive Intelligence</h3>
              <Cpu className="w-3.5 h-3.5 text-blue-500" />
            </div>
            <div className="p-6 space-y-6">
              <div className="grid grid-cols-3 gap-3">
                <ForecastCard label="1D Forecast" value={pctFmt(intel.pred_ret_1d)} trend={intel.pred_ret_1d >= 0 ? 'up' : 'down'} />
                <ForecastCard label="5D Forecast" value={pctFmt(intel.pred_ret_5d)} trend={intel.pred_ret_5d >= 0 ? 'up' : 'down'} />
                <ForecastCard label="21D Forecast" value={pctFmt(intel.pred_ret_21d)} trend={intel.pred_ret_21d >= 0 ? 'up' : 'down'} />
              </div>
              <div className="space-y-4 pt-4 border-t border-slate-800/50">
                <ForecastItem label="Alpha Drift Factor" value={intel.pred_factor_21d?.toFixed(4)} subtext={intel.pred_factor_21d > 0 ? "Positive Tailwind" : "Factor Drag"} />
                <ForecastItem label="Forward Volatility" value={pctFmt(intel.pred_vol_21d)} />
                <ForecastItem label="Downside Tail Risk" value={pctFmt(intel.pred_downside_21d)} color="text-red-400" />
              </div>
            </div>
          </div>

          {/* Synergy */}
          <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden">
            <div className="px-6 py-4 border-b border-slate-800 bg-slate-800/10 flex justify-between items-center">
              <h3 className="text-[9px] font-black text-slate-500 uppercase tracking-widest">Systemic Synergy</h3>
              <Layers className="w-3.5 h-3.5 text-purple-500" />
            </div>
            <div className="p-6 space-y-6">
              <div className="grid grid-cols-2 gap-4">
                <div className="p-3 bg-slate-800/20 rounded-xl border border-slate-800">
                  <p className="text-[8px] font-black text-slate-600 uppercase mb-1">Port. VaR Contribution</p>
                  <p className="text-base font-mono font-bold text-white">{pctFmt(intel.var_contribution_pct)}</p>
                </div>
                <div className="p-3 bg-slate-800/20 rounded-xl border border-slate-800">
                  <p className="text-[8px] font-black text-slate-600 uppercase mb-1">Expected VaR Δ</p>
                  <p className="text-base font-mono font-bold text-white">{formatCurrency(intel.expected_var_contribution_21d)}</p>
                </div>
              </div>
              <div className="space-y-4">
                <ForecastItem label="Portfolio Alpha (21D)" value={pctFmt(intel.pred_port_ret_21d)} />
                <ForecastItem label="Beta-Weighted Alignment" value={intel.beta_weighted?.toFixed(4)} />
                <ForecastItem label="Core Correlation" value="0.84" subtext="Strong Positive Link" />
              </div>
            </div>
          </div>

          {/* Flows & Compliance */}
          <div className="space-y-6">
            <div className="bg-slate-900/40 border border-slate-800 rounded-2xl p-6">
              <h3 className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-4">Strategic Flows</h3>
              <div className="space-y-4">
                <ForecastItem label="Sector Rotation Signal" value={intel.pred_sector_rotation?.toFixed(4)} subtext={intel.pred_sector_rotation > 0 ? "Institutional Buy" : "Capital Flight"} />
                <ForecastItem label="Asset Concentration" value={pctFmt(intel.sector_exposure_pct)} subtext={intel.sector_exposure_pct > 0.3 ? "Overweight" : "Neutral"} />
              </div>
            </div>
            <div className="bg-slate-900/40 border border-slate-800 rounded-2xl p-6">
               <h3 className="text-[9px] font-black text-slate-500 uppercase mb-4 tracking-widest">Compliance Engine</h3>
               {alerts.length === 0 ? (
                 <div className="flex items-center gap-2 text-emerald-400 font-bold text-[10px] uppercase">
                    <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 animate-pulse" /> Zero Breaches Found
                 </div>
               ) : (
                 <div className="space-y-3">
                    {alerts.slice(0, 2).map((a: any, i: number) => (
                      <div key={i} className="p-2.5 bg-red-500/5 border border-red-500/20 rounded flex gap-3">
                        <AlertCircle className="w-3.5 h-3.5 text-red-500 shrink-0" />
                        <div className="min-w-0">
                          <p className="text-[8px] font-black text-red-500 uppercase truncate">{a.type}</p>
                          <p className="text-[10px] font-bold text-white leading-tight">{a.message}</p>
                        </div>
                      </div>
                    ))}
                 </div>
               )}
            </div>
          </div>
        </div>

        {/* FORWARD PROJECTION FOOTER */}
        <div className="bg-slate-950 border border-slate-800 rounded-2xl p-6 sm:p-8 shadow-2xl relative overflow-hidden">
           <div className="absolute top-0 right-0 w-1/2 h-full bg-blue-600/5 blur-[100px] pointer-events-none" />
           <div className="flex justify-between items-end mb-8 relative z-10">
             <div>
                <p className="text-blue-500 text-[9px] font-black uppercase tracking-[0.4em] mb-2">Simulation Matrix</p>
                <h2 className="text-xl sm:text-3xl font-black text-white uppercase italic tracking-tighter leading-none">Expected P&L Trajectory</h2>
             </div>
             <Activity className="w-6 h-6 text-slate-800" />
           </div>
           <div className="grid grid-cols-1 md:grid-cols-3 gap-4 sm:gap-8 relative z-10">
              <TrajectoryBox label="1-Day Projection" value={formatCurrency(intel.expected_pnl_1d)} />
              <TrajectoryBox label="5-Day Projection" value={formatCurrency(intel.expected_pnl_5d)} />
              <TrajectoryBox label="21-Day Projection" value={formatCurrency(intel.expected_pnl_21d)} highlight />
           </div>
        </div>
      </div>

      <style jsx global>{`
        button, a, .cursor-pointer { cursor: pointer !important; }
        .no-scrollbar::-webkit-scrollbar { display: none; }
      `}</style>
    </main>
  );
}

function ForecastItem({ label, value, color, subtext }: any) {
  return (
    <div className="flex flex-col">
      <span className="text-[8px] font-black text-slate-600 uppercase tracking-tight mb-0.5">{label}</span>
      <span className={`text-xs sm:text-sm font-mono font-bold ${color || 'text-white'}`}>{value}</span>
      {subtext && <span className="text-[7px] font-black text-slate-600 uppercase mt-0.5 truncate">{subtext}</span>}
    </div>
  );
}

function LoadingTerminal() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <Cpu className="w-8 h-8 text-blue-600 animate-spin opacity-50" />
      <p className="text-[9px] font-black text-slate-600 uppercase tracking-[0.5em]">Syncing Intelligence...</p>
    </div>
  );
}