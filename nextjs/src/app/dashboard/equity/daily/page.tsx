"use client";

import React, { useEffect, useState, useCallback, useMemo } from "react";
import Link from "next/link";
import TickerSearch from "@/components/TickerSearch";
import ManagerSearch from "@/components/ManagerSearch";
import StressTestingControls from "@/components/StressTesting";
import {
  TrendingUp, Users, Layers, ArrowLeft, RefreshCcw, Search,
  ChevronRight, ShieldAlert, BarChart3, Globe, Activity, MoveRight
} from "lucide-react";

/* -------------------------
    FORMATTERS (Locked Schema)
------------------------- */
const formatCurrency = (v?: number) => {
  if (v == null || Number.isNaN(v)) return "—";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
};

const formatPct = (n?: number) => n == null || Number.isNaN(n) ? "—" : `${(n * 100).toFixed(2)}%`;
const slugify = (s: string) => s ? s.toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "") : "";

/* -------------------------
    REUSABLE UI COMPONENTS
------------------------- */
const Card = ({ children, title, icon: Icon, className = "", headerExtra }: any) => (
  <div className={`bg-slate-900/40 border border-slate-800 rounded-2xl flex flex-col backdrop-blur-md overflow-hidden transition-all duration-500 ${className}`}>
    {(title || Icon) && (
      <div className="px-5 py-4 border-b border-slate-800/50 flex items-center bg-slate-800/20">
        <h3 className="flex-1 text-[10px] font-black text-slate-500 uppercase tracking-[0.2em] truncate">
          {title}
        </h3>
        <div className="flex items-center gap-2 shrink-0">
          {headerExtra}
          {Icon && <Icon className="w-4 h-4 text-slate-600" />}
        </div>
      </div>
    )}
    <div className="p-5 flex-1 w-full">{children}</div>
  </div>
);

const StatCard = ({ label, value, isWarning, isPositive, version }: any) => (
  <div className="p-5 rounded-2xl border border-slate-800 bg-slate-900/60 relative overflow-hidden group">
    <div key={version} className="absolute inset-0 bg-blue-500/10 pointer-events-none animate-data-flash" />
    <p className="text-[9px] font-black text-slate-500 uppercase mb-2 tracking-widest relative z-10">{label}</p>
    <p className={`text-xl font-mono font-bold tracking-tighter relative z-10 ${
      isWarning ? 'text-red-400' : isPositive ? 'text-emerald-400' : 'text-white'
    }`}>
      {value}
    </p>
  </div>
);

function RiskSection({ title, items, version }: { title: string; items: any[]; version: number }) {
  return (
    <Card title={title} icon={Layers}>
      <div key={version} className="space-y-3 pt-1 animate-in fade-in slide-in-from-bottom-2 duration-700">
        {items.map((i, idx) => (
          <div key={idx} className="group p-3 rounded-xl border border-slate-800 bg-slate-900/40 hover:bg-slate-800/40 transition-all cursor-pointer">
            <div className="flex justify-between items-center gap-4">
              <div className="min-w-0">
                <Link href={`/dashboard/equity/daily/ticker/${i.ticker}`} className="font-black text-sm text-white group-hover:text-blue-400 transition-colors truncate block cursor-pointer">
                  {i.ticker}
                </Link>
                <p className="text-[9px] text-slate-600 font-black uppercase tracking-tight truncate">{i.sector ?? "General"}</p>
              </div>
              <div className="text-right shrink-0">
                <p className={`text-xs font-mono font-bold ${i.aggregatedDailyPnlUsd < 0 ? 'text-red-400' : 'text-emerald-400'}`}>
                  {formatCurrency(i.aggregatedDailyPnlUsd)}
                </p>
                <p className="text-[9px] font-black text-slate-600 uppercase">VaR: {formatCurrency(i.varContributionUsd)}</p>
              </div>
            </div>
          </div>
        ))}
      </div>
    </Card>
  );
}

export default function EquityPage() {
  const [date, setDate] = useState<string | null>(null);
  const [stressParams, setStressParams] = useState<Record<string, any>>({});
  const [loading, setLoading] = useState(true);
  const [version, setVersion] = useState(0);

  const [ps, setPs] = useState<any>(null);
  const [managers, setManagers] = useState<any[]>([]);
  const [risk, setRisk] = useState<any>({ topVaR: [], topLoss: [], topExposure: [] });
  const [alerts, setAlerts] = useState<any[]>([]);

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

      const qs = new URLSearchParams({ date: latest_date, ...activeParams }).toString();
      
      const [overview, mgrs, var_, loss, exposure, alertData] = await Promise.all([
        fetch(`/api/equity/daily/main/overview?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/main/managers?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/main/risk-drivers/?${qs}&type=var`).then(r => r.json()),
        fetch(`/api/equity/daily/main/risk-drivers/?${qs}&type=loss`).then(r => r.json()),
        fetch(`/api/equity/daily/main/risk-drivers/?${qs}&type=exposure`).then(r => r.json()),
        fetch(`/api/equity/daily/main/alerts?${qs}`).then(r => r.json()),
      ]);

      setPs({ ...overview, alerts_count: alertData.length });
      setManagers(mgrs);
      setRisk({ topVaR: var_, topLoss: loss, topExposure: exposure });
      setAlerts(alertData);
      setVersion(v => v + 1);
    } catch (err) { console.error(err); } 
    finally { setLoading(false); }
  }, [stressParams]);

  const handleGlobalReset = () => {
    setStressParams({}); 
    loadData({});
  };

  useEffect(() => { loadData(); }, [loadData]);

  if (!ps || !date) return <LoadingTerminal />;

  const displayDate = new Date(date).toDateString().toUpperCase();

  return (
    <div className="min-h-screen bg-[#020617] text-slate-300 pb-20 font-sans selection:bg-blue-500/30 overflow-x-hidden transition-colors duration-700">
      
{/* HEADER */}
<div className="sticky top-0 z-[100] bg-[#020617]/80 backdrop-blur-xl border-b border-slate-800 px-4 sm:px-8 py-3 sm:py-4 flex justify-between items-center">
  <div className="flex items-center gap-3 sm:gap-4 min-w-0">
    <Link href="/dashboard/equity" className="p-1.5 sm:p-2 hover:bg-slate-800 rounded-lg shrink-0 cursor-pointer">
      <ArrowLeft className="w-4 h-4 sm:w-5 sm:h-5 text-slate-500 hover:text-white" />
    </Link>

    <div className="min-w-0">
      <div className="flex items-center gap-1 sm:gap-2 text-[7px] sm:text-[9px] font-black text-slate-500 uppercase tracking-[0.2em] mb-0.5">
        <span>Risk</span> 
        <ChevronRight className="w-2.5 h-2.5" /> 
        <span className="text-blue-500">Equity Daily</span>
      </div>

      <div className="flex flex-col sm:flex-row sm:items-center gap-1.5 sm:gap-3 min-w-0">
        <h1 className="text-lg sm:text-2xl font-black text-white tracking-tighter uppercase italic shrink-0">
          Risk Oversight
        </h1>

        {activeScenarioLabel && (
          <div className="flex items-center gap-1.5 shrink-0 sm:mt-0.5">
            <span className="bg-red-500 text-white text-[7px] sm:text-[8px] font-black px-1.5 py-0.5 rounded animate-pulse shadow-[0_0_10px_rgba(239,68,68,0.3)]">
              STRESSED
            </span>
            <span className="text-[8px] sm:text-[10px] text-red-400 font-mono border border-red-500/20 px-1.5 py-0.5 rounded bg-red-500/5 whitespace-nowrap">
              {activeScenarioLabel}
            </span>
          </div>
        )}
      </div>
    </div>
  </div>

  <div className="flex items-center gap-3 sm:gap-4 shrink-0 ml-4">
    <div className="text-right">
      <p className="text-[7px] font-black text-slate-600 uppercase tracking-widest">EOD Snapshot</p>
      <p className="text-[10px] sm:text-xs font-mono font-bold text-slate-400">{displayDate}</p>
    </div>

    <button 
      onClick={handleGlobalReset} 
      className="p-2 sm:p-2.5 bg-blue-600 text-white rounded-lg sm:rounded-xl hover:bg-blue-500 transition-all cursor-pointer active:scale-90 shadow-lg"
    >
      <RefreshCcw className={`w-3.5 h-3.5 sm:w-4 sm:h-4 ${loading ? 'animate-spin' : ''}`} />
    </button>
  </div>
</div>

    <main className="max-w-[1600px] mx-auto p-4 sm:p-8 space-y-8 animate-in fade-in duration-700">
        
        {/* SEARCH & STRESS ENGINE */}
        <div className="grid grid-cols-1 lg:grid-cols-12 gap-6 relative">
          <Card title="Direct Access" icon={Search} className="lg:col-span-4 overflow-visible relative z-50">
            <div className="space-y-6">
              <div className="w-full">
                <label className="text-[9px] font-black text-slate-500 uppercase mb-2 block px-1">Ticker Lookup</label>
                <div className="w-full [&>div]:w-full [&_input]:w-full relative [&_input]:cursor-text">
                  <TickerSearch ticker_url="equity/daily" />
                </div>
              </div>
              <div className="w-full">
                <label className="text-[9px] font-black text-slate-500 uppercase mb-2 block px-1">Manager Profile</label>
                <div className="w-full [&>div]:w-full [&_input]:w-full relative [&_input]:cursor-text">
                  <ManagerSearch manager_url="equity/daily" />
                </div>
              </div>
            </div>
          </Card>
          <Card title="Risk Exposure Engine" icon={TrendingUp} className="lg:col-span-8 overflow-hidden relative z-0 [&_button]:cursor-pointer">
            <StressTestingControls onChange={setStressParams} />
          </Card>
        </div>

        {/* STATS */}
        <div className="grid grid-cols-2 lg:grid-cols-6 gap-4">
          <StatCard version={version} label="Total Exposure" value={formatCurrency(ps.total_exposure)} />
          <StatCard version={version} label="Daily P&L" value={formatCurrency(ps.daily_pnl)} isWarning={ps.daily_pnl < 0} isPositive={ps.daily_pnl > 0} />
          <StatCard version={version} label="VaR 99%" value={formatCurrency(ps.var_99)} />
          <StatCard version={version} label="Ex-Ante Vol" value={formatPct(ps.ex_ante_volatility)} />
          <StatCard version={version} label="HHI Sector" value={ps.hhi_sector?.toFixed(4)} />
          <StatCard version={version} label="Alerts" value={ps.alerts_count} isWarning={ps.alerts_count > 0} />
        </div>

        {/* LEDGER & BREACHES */}
        <div className="grid grid-cols-1 xl:grid-cols-4 gap-8">
       <Card 
  title="Manager Ledger" 
  icon={Users} 
  className="xl:col-span-3"
  headerExtra={
    <div className="flex items-center gap-1.5 px-2 py-0.5 rounded border border-blue-500/30 bg-blue-500/10 lg:hidden">
      <span className="text-[8px] font-black text-blue-400 uppercase">Scroll</span>
      <MoveRight className="w-2.5 h-2.5 text-blue-400 animate-pulse" />
    </div>
  }
>
  <div className="overflow-x-auto pb-4 no-scrollbar">
    {/* Using a Grid instead of a Table for absolute control over gaps */}
    <div className="min-w-[700px]">
      {/* Header Row */}
      <div className="grid grid-cols-[180px_120px_120px_140px_80px] gap-4 border-b border-slate-800 pb-4 text-[10px] font-black text-slate-600 uppercase tracking-widest">
        <div>Entity</div>
        <div className="text-right">Exposure</div>
        <div className="text-right">Day P&L</div>
        <div className="text-right">VaR Usage</div>
        <div className="text-right">Weight</div>
      </div>

      {/* Data Rows */}
      <div key={version} className="divide-y divide-slate-800/50">
        {managers.map((m) => (
          <div 
            key={m.asset_manager} 
            className="grid grid-cols-[180px_120px_120px_140px_80px] gap-4 py-4 items-center hover:bg-slate-800/30 transition-colors group px-0"
          >
            <div className="font-bold text-white truncate">
              <Link href={`/dashboard/equity/daily/manager/${slugify(m.asset_manager)}`} className="hover:text-blue-400 cursor-pointer">
                {m.asset_manager}
              </Link>
            </div>
            <div className="text-right font-mono text-slate-300">{formatCurrency(m.exposure)}</div>
            <div className={`text-right font-black ${m.daily_pnl < 0 ? 'text-red-400' : 'text-emerald-400'}`}>
              {formatCurrency(m.daily_pnl)}
            </div>
            <div className="flex flex-col items-end gap-1">
              <span className="text-[10px] font-mono text-white">{formatPct(m.var_usage_pct)}</span>
              <div className="w-full h-1 bg-slate-800 rounded-full overflow-hidden">
                <div 
                  className="h-full bg-blue-500 transition-all duration-1000" 
                  style={{ width: `${Math.min(m.var_usage_pct * 100, 100)}%` }} 
                />
              </div>
            </div>
            <div className="text-right text-slate-500 font-mono text-[11px]">{formatPct(m.book_pct)}</div>
          </div>
        ))}
      </div>
    </div>
  </div>
</Card>

          <Card title="Systemic Breaches" icon={ShieldAlert} className="xl:col-span-1 border-t-2 border-t-red-500/50">
            <div className="space-y-4 max-h-[600px] overflow-y-auto no-scrollbar">
              {alerts.length === 0 ? (
                <div className="py-12 text-center opacity-30">
                  <Globe className="w-8 h-8 mx-auto mb-2" />
                  <p className="text-[9px] font-black uppercase">No Breaches</p>
                </div>
              ) : (
                alerts.map((a, i) => (
                  <div key={i} className="p-4 rounded-xl bg-slate-800/20 border border-slate-800 transition-all hover:border-red-500/30">
                    <div className="flex justify-between mb-2">
                      <span className="text-[8px] font-black uppercase px-2 py-0.5 rounded bg-red-500/20 text-red-500">{a.severity}</span>
                      <span className="text-[9px] font-mono text-slate-600">{a.eod}</span>
                    </div>
                    <p className="text-xs font-bold text-white mb-2 leading-tight">{a.alert}</p>
                    <Link href={`/dashboard/equity/daily/ticker/${a.ticker}`} className="text-[10px] text-blue-500 font-black cursor-pointer hover:underline">{a.ticker}</Link>
                  </div>
                ))
              )}
            </div>
          </Card>
        </div>

        {/* RISK ATTRIBUTION */}
        <section className="space-y-6">
          <div className="flex items-center gap-2 px-2">
            <BarChart3 className="w-4 h-4 text-slate-600" />
            <h2 className="text-xs font-black text-slate-500 uppercase tracking-[0.3em]">Systemic Risk Attribution</h2>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            <RiskSection title="Tail Risk (VaR)" items={risk.topVaR} version={version} />
            <RiskSection title="Unrealized Drawdown" items={risk.topLoss} version={version} />
            <RiskSection title="Concentration (Gross)" items={risk.topExposure} version={version} />
          </div>
        </section>

      </main>

<style jsx global>{`
  @keyframes data-flash {
    0% { background-color: rgba(59, 130, 246, 0.4); opacity: 1; }
    100% { background-color: transparent; opacity: 0; }
  }
  .animate-data-flash { animation: data-flash 1s ease-out forwards; }
  button, a, .cursor-pointer { cursor: pointer !important; }
`}</style>

</div>
  );
}

function LoadingTerminal() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center">
      <Activity className="w-10 h-10 text-blue-600 animate-spin opacity-40 mb-4" />
      <p className="text-[9px] font-black text-slate-700 uppercase tracking-[0.4em]">
        Syncing Intelligence...
      </p>
    </div>
  );
}
