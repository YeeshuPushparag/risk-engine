"use client";

import { useParams } from "next/navigation";
import { useEffect, useState, useCallback, useMemo } from "react";
import Link from "next/link";
import {
  ArrowLeft, ShieldAlert, TrendingUp, PieChart,
  AlertTriangle, FileText, Activity, Globe, RefreshCcw, 
  ChevronRight, MoveRight
} from "lucide-react";
import StressTestingControls from "@/components/StressTesting";

/* ---------------- Unified Formatters ---------------- */
const fmtUsd = (v?: number) => {
  if (v == null || Number.isNaN(v)) return "—";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
};

const fmtPct = (v?: number) => v == null ? "—" : `${(v * 100).toFixed(2)}%`;



/* ---------------- Dark Terminal Components ---------------- */
const MetricCard = ({ label, value, icon, trend, status, version }: any) => (
  <div className="bg-slate-900/40 border border-slate-800 p-4 sm:p-5 rounded-2xl backdrop-blur-md relative overflow-hidden group">
    <div key={version} className="absolute inset-0 bg-blue-500/5 pointer-events-none animate-data-flash" />
    <div className="flex items-center gap-2 text-slate-500 mb-3 relative z-10">
      {icon}
      <span className="text-[8px] sm:text-[9px] font-black uppercase tracking-[0.2em]">{label}</span>
    </div>
    <div className={`text-lg sm:text-2xl font-mono font-bold tracking-tighter relative z-10 ${
        trend === 'up' ? 'text-emerald-400' :
        trend === 'down' ? 'text-red-400' :
        status === 'danger' ? 'text-red-400' : 'text-white'
      }`}>
      {value}
    </div>
  </div>
);

export default function ManagerPage() {
  const params = useParams();
  const [loading, setLoading] = useState(true);
  const [version, setVersion] = useState(0);
  const [date, setDate] = useState<string | null>(null);
  const [stressParams, setStressParams] = useState<Record<string, any>>({});
  const [data, setData] = useState<any>(null);

  const slug = decodeURIComponent(params.manager as string);
  const managerName = slug.replace(/-/g, " ").split(" ").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");

  const activeScenarioLabel = useMemo(() => {
    if (!stressParams.scenario || stressParams.scenario === "baseline") {
        const isManualShift = Object.entries(stressParams).some(([k, v]) => k !== "scenario" && parseFloat(v) !== (k === "ret_shift" ? 0 : 1));
        return isManualShift ? "Custom Stress" : null;
    }
    return stressParams.scenario.replace(/_/g, ' ').toUpperCase();
  }, [stressParams]);

  const loadData = useCallback(async (forcedParams?: Record<string, any>) => {
    setLoading(true);
    const activeParams = forcedParams !== undefined ? forcedParams : stressParams;
    try {
      const dateRes = await fetch("/api/equity/daily/main/latest-date");
      const { latest_date } = await dateRes.json();
      setDate(latest_date);

      const qs = new URLSearchParams({ date: latest_date, manager: managerName, ...activeParams }).toString();
      const [overview, holdings, risk, alerts] = await Promise.all([
        fetch(`/api/equity/daily/manager/overview?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/manager/holdings?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/manager/risk?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/manager/alerts?${qs}`).then(r => r.json()),
      ]);

      setData({ overview, holdings, risk, alerts });
      setVersion(v => v + 1);
    } catch (err) { console.error(err); } finally { setLoading(false); }
  }, [managerName, stressParams]);

  useEffect(() => { loadData(); }, [loadData]);

  const handleGlobalReset = () => {
    setStressParams({});
    loadData({});
  };

  if (!data || !date) return <LoadingTerminal />;
  const { overview, holdings, risk, alerts } = data;
  const displayDate = new Date(date).toDateString().toUpperCase();
  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 pb-20 font-sans selection:bg-blue-500/30">
      
      {/* RESPONSIVE HEADER */}
      <div className="sticky top-0 z-[100] bg-[#020617]/80 backdrop-blur-xl border-b border-slate-800 px-4 sm:px-8 py-3 sm:py-4 flex justify-between items-center">
        <div className="flex items-center gap-3 sm:gap-4 min-w-0">
          <Link href="/dashboard/equity/daily" className="p-1.5 sm:p-2 hover:bg-slate-800 rounded-lg shrink-0 border border-slate-800/50 cursor-pointer">
            <ArrowLeft className="w-4 h-4 sm:w-5 sm:h-5 text-slate-500 hover:text-white" />
          </Link>
          <div className="min-w-0">
            <div className="flex items-center gap-1 sm:gap-2 text-[7px] sm:text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-0.5">
              <span>Equity</span> <ChevronRight className="w-2.5 h-2.5" /> <span className="text-blue-500">Manager Profile</span>
            </div>
            
            <div className="flex flex-col sm:flex-row sm:items-center gap-1 sm:gap-3 min-w-0">
              <h1 className="text-lg sm:text-2xl font-black text-white tracking-tighter uppercase italic shrink-0">
                {managerName}
              </h1>
              
              {activeScenarioLabel && (
                <div className="flex items-center gap-1.5 shrink-0 sm:mt-0.5">
                  <span className="bg-amber-500 text-black text-[7px] sm:text-[8px] font-black px-1.5 py-0.5 rounded animate-pulse shadow-[0_0_10px_rgba(245,158,11,0.2)]">
                    STRESSED
                  </span>
                  <span className="text-[8px] sm:text-[10px] text-amber-400 font-mono border border-amber-500/20 px-1.5 py-0.5 rounded bg-amber-500/5 whitespace-nowrap">
                    {activeScenarioLabel}
                  </span>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* EOD & Refresh */}
        <div className="flex items-center gap-3 sm:gap-4 shrink-0">
          <div className="text-right">
              <p className="text-[7px] font-black text-slate-600 uppercase tracking-widest">EOD Snapshot</p>
              <p className="text-[10px] sm:text-xs font-mono font-bold text-slate-400">{displayDate}</p>
            </div>
          <button 
            onClick={handleGlobalReset} 
            className="p-2 sm:p-2.5 bg-blue-600 text-white rounded-lg sm:rounded-xl hover:bg-blue-500 transition-all active:scale-90 shadow-lg cursor-pointer"
          >
            <RefreshCcw className={`w-3.5 h-3.5 sm:w-4 sm:h-4 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      <div className="max-w-[1600px] mx-auto p-4 sm:p-8 space-y-6 sm:space-y-8 animate-in fade-in duration-700">

        {/* SIMULATION ENGINE */}
        <section className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-md">
          <div className="bg-slate-800/20 px-6 py-3 border-b border-slate-800 flex items-center gap-2">
            <Activity className="w-4 h-4 text-blue-500" />
            <h2 className="text-[10px] font-black uppercase tracking-[0.2em] text-slate-400">Risk Control Panel</h2>
          </div>
          <div className="p-4 sm:p-6 [&_button]:cursor-pointer [&_select]:cursor-pointer [&_input]:cursor-pointer">
            <StressTestingControls onChange={setStressParams} />
          </div>
        </section>

        {/* METRICS */}
        <section className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 sm:gap-4">
          <MetricCard version={version} label="Exposure" value={fmtUsd(overview.exposure)} icon={<PieChart size={14} />} />
          <MetricCard version={version} label="Daily P&L" value={fmtUsd(overview.portfolio_daily_pnl)} icon={<TrendingUp size={14} />} trend={overview.portfolio_daily_pnl >= 0 ? 'up' : 'down'} />
          <MetricCard version={version} label="VaR 95%" value={fmtUsd(overview.daily_portfolio_var_95)} icon={<ShieldAlert size={14} />} />
          <MetricCard version={version} label="VaR 99%" value={fmtUsd(overview.daily_portfolio_var_99)} icon={<ShieldAlert size={14} />} status="danger" />
          <MetricCard version={version} label="Portfolio Wt" value={fmtPct(overview.portfolio_weight)} icon={<Activity size={14} />} />
          <MetricCard version={version} label="Alerts" value={overview.alerts} icon={<AlertTriangle size={14} />} status={overview.alerts > 0 ? 'danger' : 'neutral'} />
        </section>

        {/* CONTENT */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8">
          
          <div className="lg:col-span-2 space-y-6">
            <div className="bg-slate-900/40 border border-slate-800 rounded-2xl overflow-hidden backdrop-blur-md">
              <div className="px-6 py-5 border-b border-slate-800/50 flex justify-between items-center bg-slate-800/20">
                <div className="flex items-center gap-2">
                  <FileText className="w-4 h-4 text-slate-500" />
                  <h3 className="font-black text-slate-400 uppercase text-[10px] tracking-[0.2em]">Positions Ledger</h3>
                </div>
                {/* Scroll Indicator for Mobile */}
                <div className="flex items-center gap-1.5 px-2 py-0.5 rounded border border-blue-500/30 bg-blue-500/10 lg:hidden">
                  <span className="text-[8px] font-black text-blue-400 uppercase">Scroll</span>
                  <MoveRight className="w-2.5 h-2.5 text-blue-400 animate-pulse" />
                </div>
              </div>
              
              <div className="overflow-x-auto no-scrollbar">
                <div className="min-w-[800px] p-6">
                  <div className="grid grid-cols-[120px_1fr_120px_120px_120px_140px] gap-4 border-b border-slate-800 pb-4 text-[10px] font-black text-slate-600 uppercase tracking-widest">
                    <div>Ticker</div>
                    <div>Sector</div>
                    <div className="text-right">Exposure</div>
                    <div className="text-right">Weight</div>
                    <div className="text-right">Daily P&L</div>
                    <div className="text-right">VaR Contrib.</div>
                  </div>

                  <div key={version} className="divide-y divide-slate-800/50">
                    {holdings.map((h: any) => (
                      <div key={h.ticker} className="grid grid-cols-[120px_1fr_120px_120px_120px_140px] gap-4 py-4 items-center hover:bg-slate-800/30 transition-colors group">
                        <div className="font-bold text-blue-400">
                           <Link href={`/dashboard/equity/daily/ticker/${h.ticker}/manager/${slug}`} className="hover:text-blue-300 cursor-pointer">
                            {h.ticker}
                           </Link>
                        </div>
                        <div className="text-[10px] font-black text-slate-500 uppercase italic truncate">{h.sector}</div>
                        <div className="text-right font-mono text-white">{fmtUsd(h.exposure)}</div>
                        <div className="text-right font-mono text-slate-500">{fmtPct(h.weight)}</div>
                        <div className={`text-right font-mono font-bold ${h.daily_pnl < 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                          {fmtUsd(h.daily_pnl)}
                        </div>
                        <div className="text-right font-mono text-slate-500">{fmtPct(h.var_pct)}</div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-6">
            {/* CONCENTRATION - Fixed with Links and Ticker Names */}
            <div className="bg-slate-900/40 border border-slate-800 rounded-2xl p-6 backdrop-blur-md">
              <h3 className="font-black text-slate-400 uppercase text-[10px] tracking-[0.2em] mb-6 flex items-center gap-2">
                <ShieldAlert className="w-4 h-4 text-red-500" /> Concentration
              </h3>
              <div className="space-y-4">
                {risk.map((r: any) => (
                  <Link 
                    key={r.ticker} 
                    href={`/dashboard/equity/daily/ticker/${r.ticker}/manager/${slug}`} 
                    className="block p-3 rounded-xl border border-slate-800 bg-slate-950/40 hover:border-blue-500/30 transition-all group cursor-pointer"
                  >
                    <div className="flex justify-between items-center mb-2">
                      <div className="flex items-center gap-2">
                        <span className="font-black text-white group-hover:text-blue-400 tracking-tight transition-colors">{r.ticker}</span>
                        <span className="text-[8px] font-black text-slate-600 uppercase tracking-tighter italic truncate max-w-[80px]">
                          {r.sector}
                        </span>
                      </div>
                      <span className="text-[10px] font-mono font-bold text-red-400">{fmtUsd(r.var_usd)}</span>
                    </div>
                    <div className="w-full bg-slate-800 h-1 rounded-full overflow-hidden">
                      <div 
                        className="bg-red-500 h-full transition-all duration-1000" 
                        style={{ width: `${Math.min((r.var_usd / (overview.daily_portfolio_var_99 || 1)) * 100, 100)}%` }} 
                      />
                    </div>
                  </Link>
                ))}
              </div>
            </div>

            {/* COMPLIANCE */}
            <div className="bg-slate-900/40 border border-slate-800 rounded-2xl p-6 backdrop-blur-md">
                <h3 className="font-black text-slate-400 uppercase text-[10px] tracking-[0.2em] mb-6 flex items-center gap-2">
                  <AlertTriangle className="w-4 h-4 text-amber-500" /> Compliance
                </h3>
                <div className="space-y-4">
                    {alerts.length === 0 ? (
                        <p className="text-[10px] text-slate-600 uppercase text-center py-4 italic">No Active Breaches</p>
                    ) : (
                        alerts.map((a: any, i: number) => (
                            <Link 
                              key={i} 
                              href={`/dashboard/equity/daily/ticker/${a.ticker}/manager/${slug}`} 
                              className="block p-3 rounded-xl border border-red-500/20 bg-red-500/5 hover:border-red-500/40 transition-all cursor-pointer group"
                            >
                                <div className="flex justify-between items-start mb-1">
                                  <p className="text-xs font-bold text-white group-hover:text-blue-400 transition-colors leading-tight">{a.alert}</p>
                                  <span className="text-[8px] font-black text-red-500 uppercase bg-red-500/10 px-1 rounded ml-2 shrink-0">{a.severity}</span>
                                </div>
                                <p className="text-[9px] font-black text-slate-600 uppercase tracking-widest">{a.ticker}</p>
                            </Link>
                        ))
                    )}
                </div>
            </div>
          </div>
        </div>
      </div>

      <style jsx global>{`
        button, a, Link, .cursor-pointer { cursor: pointer !important; }
        @keyframes data-flash {
          0% { background-color: rgba(59, 130, 246, 0.4); opacity: 1; }
          100% { background-color: transparent; opacity: 0; }
        }
        .animate-data-flash { animation: data-flash 1s ease-out forwards; }
        .no-scrollbar::-webkit-scrollbar { display: none; }
        .no-scrollbar { -ms-overflow-style: none; scrollbar-width: none; }
      `}</style>
    </main>
  );
}

function LoadingTerminal() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <Globe className="w-10 h-10 text-blue-600 animate-pulse opacity-50" />
      <p className="text-[10px] font-black text-slate-600 uppercase tracking-[0.5em]">Syncing Intelligence...</p>
    </div>
  );
}