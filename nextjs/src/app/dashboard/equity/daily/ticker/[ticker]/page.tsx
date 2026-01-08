"use client";

import { useParams } from "next/navigation";
import Link from "next/link";
import React, { useEffect, useState, useCallback, useMemo } from "react";
import {
  ArrowLeft, BarChart3, ShieldCheck, Activity, Zap,
  Database, UserCircle, TrendingUp, Globe, RefreshCcw,
  ChevronRight, MoveRight
} from "lucide-react";
import StressTestingControls from "@/components/StressTesting";

/* --------------------------- Unified Formatters --------------------------- */
const formatCurrency = (v?: number) => {
  if (v == null || Number.isNaN(v)) return "—";

  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";

  const format = (n: number, decimals: number) => {
    // Remove trailing zeros and decimal point if not needed
    const s = n.toFixed(decimals);
    return s.endsWith(".00") ? s.slice(0, -3) : s;
  };

  if (abs >= 1e12) return `${sign}$${format(abs / 1e12, 2)}T`;
  if (abs >= 1e9) return `${sign}$${format(abs / 1e9, 2)}B`;
  if (abs >= 1e6) return `${sign}$${format(abs / 1e6, 1)}M`;
  if (abs >= 1e3) return `${sign}$${format(abs / 1e3, 0)}K`;
  return `${sign}$${abs.toFixed(2)}`;
};


const pctFmt = (v?: number | null, digits = 2) =>
  v === null || v === undefined ? "—" : `${(v * 100).toFixed(digits)}%`;

const numberFmt = (n?: number | null) =>
  n === null || n === undefined ? "—" : new Intl.NumberFormat("en-US").format(n);


/* --------------------------- Dark Terminal Components --------------------------- */

const MetricCard = ({ label, value, icon, trend, status, version }: any) => (
  <div className="bg-slate-900/40 border border-slate-800 p-4 rounded-xl backdrop-blur-md relative overflow-hidden group">
    <div key={version} className="absolute inset-0 bg-blue-500/5 pointer-events-none animate-data-flash" />
    <div className="flex items-center gap-2 text-slate-500 mb-2 relative z-10">
      {React.cloneElement(icon, { size: 12 })}
      <span className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.2em]">{label}</span>
    </div>
    <div className={`text-lg sm:text-xl font-mono font-bold tracking-tighter relative z-10 ${
        trend === 'up' ? 'text-emerald-400' :
        trend === 'down' ? 'text-red-400' :
        status === 'danger' ? 'text-red-400' : 'text-white'
      }`}>
      {value}
    </div>
  </div>
);

const DataBlock = ({ title, icon, children, scrollable }: { title: string; icon: React.ReactNode; children: React.ReactNode; scrollable?: boolean }) => (
  <div className="bg-slate-900/40 border border-slate-800 rounded-xl overflow-hidden backdrop-blur-md">
    <div className="px-4 py-2.5 border-b border-slate-800/50 flex items-center justify-between bg-slate-800/10">
      <div className="flex items-center gap-2">
        {icon}
        <h3 className="text-[9px] font-black text-slate-500 uppercase tracking-[0.15em]">{title}</h3>
      </div>
      {scrollable && (
        <div className="flex items-center gap-1 px-1.5 py-0.5 rounded border border-blue-500/30 bg-blue-500/10 lg:hidden">
            <span className="text-[7px] font-black text-blue-400 uppercase">Scroll</span>
            <MoveRight className="w-2.5 h-2.5 text-blue-400 animate-pulse" />
        </div>
      )}
    </div>
    <div className="p-4">{children}</div>
  </div>
);

const DataRow = ({ label, value, trend, highlight }: { label: string; value: any; trend?: 'up' | 'down'; highlight?: boolean }) => (
  <div className="flex flex-col">
    <span className="text-[8px] font-black text-slate-600 uppercase tracking-tight mb-0.5">{label}</span>
    <span className={`text-xs font-mono font-bold truncate ${
        trend === 'up' ? 'text-emerald-400' :
        trend === 'down' ? 'text-red-400' :
        highlight ? 'text-white' : 'text-slate-400'
      }`}>
      {value}
    </span>
  </div>
);

/* --------------------------- Main Page --------------------------- */

export default function TickerDetailPage() {
  const params = useParams();
  const ticker = typeof params?.ticker === "string" ? params.ticker.toUpperCase() : "";
  const [loading, setLoading] = useState(true);
  const [version, setVersion] = useState(0);
  const [date, setDate] = useState<string | null>(null);
  const [stressParams, setStressParams] = useState<Record<string, any>>({});
  const [data, setData] = useState<any>(null);

  const activeScenarioLabel = useMemo(() => {
    if (!stressParams.scenario || stressParams.scenario === "baseline") return null;
    return stressParams.scenario.replace(/_/g, ' ').toUpperCase();
  }, [stressParams]);

  const loadData = useCallback(async (forcedParams?: Record<string, any>) => {
    setLoading(true);
    const activeParams = forcedParams !== undefined ? forcedParams : stressParams;
    try {
      const dateRes = await fetch("/api/equity/daily/main/latest-date");
      const { latest_date } = await dateRes.json();
      setDate(latest_date);

      const qs = new URLSearchParams({ date: latest_date, ticker, ...activeParams }).toString();
      const [market, vol, risk, liq, fund, agg, managers] = await Promise.all([
        fetch(`/api/equity/daily/ticker/market/?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/volatility/?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/risk/?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/liquidity/?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/fundamentals/?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/aggregate/?${qs}`).then(r => r.json()),
        fetch(`/api/equity/daily/ticker/managers/?${qs}`).then(r => r.json()),
      ]);
      setData({ market, vol, risk, liq, fund, agg, managers });
      setVersion(v => v + 1);
    } catch (e) { console.error(e); } finally { setLoading(false); }
  }, [ticker, stressParams]);

  useEffect(() => { if (ticker) loadData(); }, [loadData, ticker]);

  if (!date || !data) return <LoadingTerminal />;
  const { market, vol, risk, liq, fund, agg, managers } = data;
  const displayDate = new Date(date).toDateString().toUpperCase();
  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 pb-10 font-sans selection:bg-blue-500/30">
      
      {/* RESPONSIVE HEADER - EOD & WRAP FIX */}
      <div className="sticky top-0 z-[100] bg-[#020617]/90 backdrop-blur-xl border-b border-slate-800 px-4 sm:px-8 py-3 flex justify-between items-center gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <Link href="/dashboard/equity/daily" className="p-2 hover:bg-slate-800 rounded-lg shrink-0 border border-slate-800/50 cursor-pointer">
            <ArrowLeft className="w-4 h-4 text-slate-500" />
          </Link>
          <div className="min-w-0">
            <div className="flex items-center gap-1 text-[7px] sm:text-[9px] font-black text-slate-600 uppercase tracking-widest mb-0.5">
              <span>Equity</span> <ChevronRight size={10} /> <span className="text-blue-500">Security</span>
            </div>
            
            <div className="flex flex-wrap items-center gap-x-2 sm:gap-x-3 gap-y-0.5">
              <h1 className="text-base sm:text-2xl font-black text-white tracking-tighter uppercase italic leading-none shrink-0">
                {ticker}
              </h1>
              <span className="hidden sm:inline text-slate-800 font-thin text-xl">|</span>
              <span className="text-[10px] sm:text-lg font-bold text-slate-400 tracking-tight truncate max-w-[120px] sm:max-w-none">
                {fund?.issuer_name}
              </span>
              {activeScenarioLabel && (
                <span className="bg-red-500 text-white text-[7px] font-black px-1.5 py-0.5 rounded animate-pulse shrink-0">
                  STRESSED
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
          <button onClick={() => loadData({})} className="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-500 transition-all cursor-pointer shadow-lg shadow-blue-900/20 active:scale-95">
            <RefreshCcw className={`w-3.5 h-3.5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
      </div>

      <div className="max-w-[1600px] mx-auto p-4 sm:p-6 space-y-6">
        
        {/* SIMULATION */}
        <section className="bg-slate-900/40 border border-slate-800 rounded-xl overflow-hidden backdrop-blur-md">
          <div className="p-4 sm:p-6 [&_button]:cursor-pointer [&_select]:cursor-pointer">
            <StressTestingControls onChange={setStressParams} />
          </div>
        </section>

        {/* METRICS GRID */}
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 sm:gap-4">
          <MetricCard version={version} label="Net Exposure" value={formatCurrency(agg.exposureUsd)} icon={<BarChart3 />} />
          <MetricCard version={version} label="Daily P&L" value={formatCurrency(agg.dailyPnlUsd)} icon={<TrendingUp />} trend={agg.dailyPnlUsd >= 0 ? 'up' : 'down'} />
          <MetricCard version={version} label="Portf. Weight" value={pctFmt(agg.portfolio_weight_pct)} icon={<Zap />} />
          <MetricCard version={version} label="Weighted Beta" value={agg.beta_weighted?.toFixed(3)} icon={<Activity />} />
          <MetricCard version={version} label="Managers" value={managers.length} icon={<UserCircle />} />
          <MetricCard version={version} label="Daily VaR 99" value={formatCurrency(risk?.daily_var_99)} icon={<ShieldCheck />} status="danger" />
        </div>

        {/* DATA GRID - items-start prevents card stretching */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6 items-start">
          
          <div className="space-y-6">
            <DataBlock title="Market Execution" icon={<TrendingUp size={12} className="text-blue-500" />}>
              <div className="grid grid-cols-2 gap-4">
                <DataRow label="Current Close" value={formatCurrency(market.close)} highlight />
                <DataRow label="Return" value={pctFmt(market.daily_return)} trend={market.daily_return >= 0 ? 'up' : 'down'} />
                <DataRow label="Open" value={formatCurrency(market.open)} />
                <DataRow label="High" value={formatCurrency(market.high)} />
                <DataRow label="Low" value={formatCurrency(market.low)} />
                <DataRow label="Volume" value={numberFmt(market.volume)} />
                <DataRow label="Prev Close" value={formatCurrency(market.prev_close)} />
                <DataRow label="Prev Open" value={formatCurrency(market.prev_open)} />
                <DataRow label="Prev High" value={formatCurrency(market.prev_high)} />
                <DataRow label="Prev Low" value={formatCurrency(market.prev_low)} />
              </div>
            </DataBlock>

            <DataBlock title="Risk Metrics" icon={<Zap size={12} className="text-amber-500" />}>
              <div className="grid grid-cols-2 gap-4">
                <DataRow label="Sharpe Ratio" value={vol?.sharpe_ratio?.toFixed(3)} />
                <DataRow label="Ann. Vol" value={pctFmt(vol?.vol_20d)} />
                <DataRow label="Momentum" value={pctFmt(vol?.momentum_20d)} />
                <DataRow label="MA (20)" value={formatCurrency(vol?.ma_20)} />
              </div>
            </DataBlock>
          </div>

          <div className="space-y-6">
            <DataBlock title="Issuer Profile" icon={<Database size={12} className="text-emerald-500" />}>
              <div className="space-y-4">
                <div className="pb-4 border-b border-slate-800/50">
                  <DataRow label="Sector & Industry" value={`${fund?.sector} / ${fund?.industry}`} highlight />
                </div>
                <div className="grid grid-cols-2 gap-4">
                 <div className="grid grid-cols-2 gap-4">
  <DataRow label="Market Cap" value={formatCurrency(fund?.market_cap)} />
  <DataRow label="Revenue" value={formatCurrency(fund?.revenue)} />
  <DataRow label="EBITDA" value={formatCurrency(fund?.ebitda)} />
  <DataRow label="Net Income" value={formatCurrency(fund?.net_income)} />
  <DataRow label="Total Assets" value={formatCurrency(fund?.total_assets)} />
  <DataRow label="Altman Z" value={fund?.altman_z?.toFixed(2)} />
  <DataRow label="D / EBITDA" value={fund?.debt_to_ebitda?.toFixed(2)} />
  <DataRow label="Rating" value={fund?.credit_rating} highlight />
</div>

                </div>
              </div>
            </DataBlock>
            
            <DataBlock title="Liquidity" icon={<ShieldCheck size={12} className="text-red-500" />}>
              <div className="grid grid-cols-2 gap-4">
                <DataRow label="Turnover" value={liq?.turnover_ratio?.toFixed(4)} />
                <DataRow label="Risk Score" value={liq?.liquidity_risk_score?.toExponential(1)} />
              </div>
            </DataBlock>
          </div>

         {/* ALLOCATIONS COLUMN */}
          <div className="lg:col-span-1">
            <DataBlock 
              title="Manager Allocations" 
              icon={<UserCircle size={12} className="text-blue-500" />} 
            >
              {/* Removed all max-height and overflow-y-auto to prevent "scroll-trapping" */}
              <div className="space-y-2 pr-1">
                {managers.map((m: any) => (
                  <Link
                    key={m.asset_manager}
                    href={`/dashboard/equity/daily/ticker/${ticker}/manager/${encodeURIComponent(m.asset_manager)}`}
                    className="flex justify-between items-center p-3 rounded-lg bg-slate-800/20 border border-slate-800 hover:border-blue-500/30 transition-all cursor-pointer group active:bg-blue-500/5"
                  >
                    <div className="flex flex-col min-w-0 pr-4">
                        <span className="text-[10px] font-black text-slate-400 group-hover:text-blue-400 uppercase tracking-tight truncate transition-colors">
                          {m.asset_manager}
                        </span>
                        <span className="text-[7px] font-black text-slate-600 uppercase tracking-widest mt-0.5">
                          Direct Allocation
                        </span>
                    </div>
                    <div className="flex flex-col items-end shrink-0">
                        <span className="text-xs font-mono font-bold text-white">
                          {pctFmt(m.weightPct)}
                        </span>
                        <div className="w-12 bg-slate-800 h-0.5 mt-1 rounded-full overflow-hidden">
                           <div 
                             className="bg-blue-500 h-full" 
                             style={{ width: `${(m.weightPct || 0) * 100}%` }} 
                           />
                        </div>
                    </div>
                  </Link>
                ))}
                
                {managers.length === 0 && (
                  <p className="text-[8px] text-slate-600 uppercase text-center py-4 italic tracking-widest">
                    No active manager records
                  </p>
                )}
              </div>
            </DataBlock>
          </div>

        </div>
      </div>

      <style jsx global>{`
        button, a, .cursor-pointer { cursor: pointer !important; }
        .no-scrollbar::-webkit-scrollbar { display: none; }
        .no-scrollbar { -ms-overflow-style: none; scrollbar-width: none; }
        @keyframes data-flash {
          0% { background-color: rgba(59, 130, 246, 0.2); }
          100% { background-color: transparent; }
        }
        .animate-data-flash { animation: data-flash 0.8s ease-out forwards; }
      `}</style>
    </main>
  );
}

function LoadingTerminal() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center gap-4">
      <Globe className="w-8 h-8 text-blue-600 animate-pulse" />
      <p className="text-[8px] font-black text-slate-600 uppercase tracking-[0.4em]">Pulling Security Records...</p>
    </div>
  );
}