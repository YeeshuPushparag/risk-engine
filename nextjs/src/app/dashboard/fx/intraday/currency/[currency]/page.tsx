"use client";

import { useState, useEffect, useRef, useCallback} from "react";
import { useParams } from "next/navigation";
import Link from "next/link";
import { useWebSocket } from "@/hooks/useWebSocket";
import { Clock, Activity, Globe, ArrowLeft, TrendingUp, MoveHorizontal } from "lucide-react";

/* ---------------- UPDATED FORMATTERS ---------------- */
const fmtCur = (v?: number) => {
  if (v == null) return "-";
  const abs = Math.abs(v);
  const sign = v < 0 ? "-" : "";
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(2)}T`;
  if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(2)}B`;
  if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
};

const fmtNum = (n?: number | null) => (n == null ? "—" : new Intl.NumberFormat("en-US").format(n));
const fmtFX = (n?: number | null) => (n == null ? "—" : n.toFixed(4));

/* ---------------- ANIMATED METRIC COMPONENT ---------------- */
function MetricCard({ label, value, trigger }: { label: string; value: React.ReactNode; trigger?: number | null }) {
  const [flash, setFlash] = useState<"up" | "down" | null>(null);
  const prevValue = useRef<number | null>(null);

  useEffect(() => {
    if (prevValue.current !== null && trigger !== null && trigger !== undefined) {
      if (trigger > prevValue.current) setFlash("up");
      else if (trigger < prevValue.current) setFlash("down");
      const timer = setTimeout(() => setFlash(null), 800);
      return () => clearTimeout(timer);
    }
    if (trigger !== null && trigger !== undefined) {
      prevValue.current = trigger;
    }
  }, [trigger]);

  const flashClass = flash === "up" ? "animate-flash-green" : flash === "down" ? "animate-flash-red" : "";

  return (
    <div className={`p-4 sm:p-5 border border-slate-800 rounded-2xl bg-slate-900/40 transition-all duration-500 ${flashClass}`}>
      <p className="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1">{label}</p>
      <p className="text-xl sm:text-2xl font-mono font-bold text-white tracking-tighter">{value}</p>
    </div>
  );
}

/* ---------------- MAIN PAGE ---------------- */
export default function CurrencyPage() {
  const { currency } = useParams();
  const [data, setData] = useState<any>(null);
  const [wsUrl, setWsUrl] = useState<string | null>(null);
  const [fxEnabled, setFxEnabled] = useState<boolean | null>(null); 
  const prevTickersRef = useRef<any>(null);

  const handleUpdate = (update: any) => {
    prevTickersRef.current = data?.tickers;
    setData(update);
  };

  useEffect(() => {
    if (!currency) return;

    async function fetchConfigAndData() {
      try {
        const res = await fetch("/api/config");
        const config = await res.json();

        const enabled = config.forceStream || isFXTradingTime();
        setFxEnabled(enabled);

        if (!enabled) return;

        setWsUrl(`${config.wsBaseUrl}/fx/currency/${currency}/`);

        const initialRes = await fetch(
          `/api/fx/intraday/currency/?currency=${currency}`,
          { cache: "no-store" }
        );

        if (initialRes.ok) {
          handleUpdate(await initialRes.json());
        }
      } catch (e) {
        console.error(e);
      }
    }

    fetchConfigAndData();
  }, [currency]);

  useWebSocket(wsUrl, handleUpdate);

  // ---- RETURN LOGIC (CONSISTENT) ----
  if (fxEnabled === null) return <LoadingState />;
  if (fxEnabled === false) return <MarketClosedView />;
  if (!data) return <LoadingState />;

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 p-4 sm:p-6 lg:p-12 space-y-8 sm:space-y-10">

      {/* HEADER - RESPONSIVE */}
      <header className="space-y-4 sm:space-y-6">
        <Link href="/dashboard/fx/intraday" className="inline-flex items-center text-[10px] font-bold text-slate-500 hover:text-blue-400 transition-colors uppercase tracking-widest gap-2">
          <ArrowLeft className="w-3 h-3" /> Back
        </Link>

        <div className="flex flex-col md:flex-row justify-between items-start md:items-end gap-4 sm:gap-6">
          <div className="space-y-1 sm:space-y-2">
            <div className="flex items-center gap-2 sm:gap-3">
              <div className="bg-blue-600/20 text-blue-400 p-1.5 rounded-lg border border-blue-500/30">
                <Globe className="w-5 h-5 sm:w-6 sm:h-6" />
              </div>
              <h1 className="text-2xl sm:text-4xl lg:text-5xl font-black text-white tracking-tighter uppercase italic leading-none">
                {currency} <span className="text-blue-500">Aggregates</span>
              </h1>
            </div>
            <p className="text-[11px] sm:text-lg font-bold text-slate-500 tracking-tight">Currency-level risk and performance monitoring</p>
          </div>

          <div className="flex flex-row md:flex-col items-center md:items-end gap-3 sm:gap-2">
            <div className="flex items-center gap-2 px-2 py-0.5 sm:px-3 sm:py-1 bg-emerald-500/10 border border-emerald-500/20 rounded-full">
              <div className="relative flex h-1.5 w-1.5 sm:h-2 sm:w-2">
                <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75"></span>
                <span className="relative inline-flex rounded-full h-1.5 w-1.5 sm:h-2 sm:w-2 bg-emerald-500"></span>
              </div>
              <span className="text-[8px] sm:text-[9px] font-black text-emerald-500 uppercase tracking-[0.15em]">Live Stream</span>
            </div>

            <div className="flex items-center gap-1.5 text-slate-500 font-mono text-[9px] sm:text-[10px] font-bold uppercase tracking-tighter">
              <Clock className="w-3 h-3 text-slate-600" />
              <span>{data?.market?.timestamp ? new Date(data.market.timestamp).toLocaleTimeString() : "Pending Sync..."}</span>
            </div>
          </div>
        </div>
      </header>

      {/* TOP METRICS - 2 COL ON MOBILE */}
      <section className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-5">
        <MetricCard label="Tickers" value={fmtNum(data.totals.ticker_count)} />
        <MetricCard label="Exposure" value={fmtCur(data.totals.total_exposure)} />
        <MetricCard label="Total P&L" value={fmtCur(data.totals.total_fx_pnl)} trigger={data.totals.total_fx_pnl} />
        <MetricCard label="Worst VaR" value={fmtCur(data.totals.worst_var_95)} />
      </section>

      {/* MARKET SNAPSHOTS - RESPONSIVE GRID */}
      {data.market && (
        <section className="grid grid-cols-1 xl:grid-cols-2 gap-6 sm:gap-8">
          <div className="bg-slate-900/40 border border-slate-800 rounded-2xl sm:rounded-3xl p-5 sm:p-8">
            <h2 className="text-[10px] font-black text-slate-500 uppercase tracking-[0.3em] mb-4 sm:mb-6">Market Snapshot</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-4 sm:gap-6">
              <MetricBox label="Open" value={fmtFX(data.market.open)} />
              <MetricBox label="High" value={fmtFX(data.market.high)} />
              <MetricBox label="Low" value={fmtFX(data.market.low)} />
              <MetricBox label="Close" value={fmtFX(data.market.close)} color="text-blue-400" />
            </div>
          </div>
          <div className="hidden md:block bg-slate-900/40 border border-slate-800 rounded-3xl p-8">
            <h2 className="text-[10px] font-black text-slate-500 uppercase tracking-[0.3em] mb-6">Previous Session</h2>
            <div className="grid grid-cols-2 md:grid-cols-4 gap-6 opacity-60">
              <MetricBox label="Prev Open" value={fmtFX(data.market.prev_open)} />
              <MetricBox label="Prev High" value={fmtFX(data.market.prev_high)} />
              <MetricBox label="Prev Low" value={fmtFX(data.market.prev_low)} />
              <MetricBox label="Prev Close" value={fmtFX(data.market.prev_close)} />
            </div>
          </div>
        </section>
      )}

      {/* TICKERS TABLE */}
      <section>
        <div className="flex items-center justify-between mb-4 px-2">
          <div className="flex items-center gap-2 text-slate-500">
            <TrendingUp className="w-4 h-4" />
            <h2 className="text-[10px] font-black uppercase tracking-[0.3em]">Associated Tickers</h2>
          </div>
          {/* Scroll Indicator for mobile */}
          <div className="lg:hidden flex items-center gap-2 text-[8px] font-bold text-blue-500 uppercase animate-pulse">
            <MoveHorizontal className="w-3 h-3" /> Scroll to view
          </div>
        </div>

        <div className="bg-slate-900/50 border border-slate-800 rounded-2xl sm:rounded-3xl overflow-hidden backdrop-blur-sm">
          <div className="overflow-x-auto scrollbar-hide">
            <table className="w-full text-sm text-left min-w-[700px]">
              <thead>
                <tr className="bg-slate-800/40 border-b border-slate-800">
                  <th className="p-4 text-[10px] font-black text-slate-500 uppercase">Ticker</th>
                  <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">Spot Price</th>
                  <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">Position</th>
                  <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">P&L</th>
                  <th className="p-4 text-[10px] font-black text-slate-500 uppercase text-right">95% VaR</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-slate-800/50">
                {data.tickers.map((r: any) => {
                  const prevTicker = prevTickersRef.current?.find((t: any) => t.ticker === r.ticker);
                  const priceUp = prevTicker ? r.close > prevTicker.close : null;
                  const flashClass = priceUp === true ? "animate-flash-green" : priceUp === false ? "animate-flash-red" : "";

                  return (
                    <tr key={r.ticker} className={`hover:bg-slate-800/40 transition-all duration-700 ${flashClass}`}>
                      <td className="p-4">
                        <Link href={`/dashboard/fx/intraday/ticker/${r.ticker}`} className="font-black text-white hover:text-blue-400 transition-colors">
                          {r.ticker}
                        </Link>
                        <div className="text-[8px] font-bold text-slate-600 uppercase mt-0.5 font-mono">
                          {new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })}
                        </div>
                      </td>
                      <td className="p-4 text-right font-mono font-medium">{fmtFX(r.close)}</td>
                      <td className="p-4 text-right font-mono">{fmtCur(r.position_size)}</td>
                      <td className={`p-4 text-right font-black ${r.fx_pnl >= 0 ? 'text-emerald-400' : 'text-red-400'}`}>
                        {fmtCur(r.fx_pnl)}
                      </td>
                      <td className="p-4 text-right text-slate-400 font-bold">{fmtCur(r.VaR_95_15m)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <style jsx global>{`
        @keyframes flash-green { 0% { background: rgba(16, 185, 129, 0.15); } 100% { background: transparent; } }
        @keyframes flash-red { 0% { background: rgba(239, 68, 68, 0.15); } 100% { background: transparent; } }
        .animate-flash-green { animation: flash-green 0.8s ease-out; }
        .animate-flash-red { animation: flash-red 0.8s ease-out; }
        .scrollbar-hide::-webkit-scrollbar { display: none; }
        .scrollbar-hide { -ms-overflow-style: none; scrollbar-width: none; }
      `}</style>
    </main>
  );
}

/* ---------------- MINI COMPONENTS ---------------- */

function MetricBox({ label, value, color = "text-white" }: { label: string, value: string, color?: string }) {
  return (
    <div>
      <p className="text-[8px] font-black text-slate-600 uppercase mb-1 tracking-wider">{label}</p>
      <p className={`text-sm sm:text-md font-mono font-bold ${color}`}>{value}</p>
    </div>
  );
}

function LoadingState() {
  return (
    <div className="h-screen bg-[#020617] flex flex-col items-center justify-center">
      <Activity className="w-10 h-10 text-blue-600 animate-spin mb-4" />
      <p className="text-[10px] font-black text-slate-500 uppercase tracking-[0.4em]">Establishing Feed...</p>
    </div>
  );
}

function MarketClosedView() {
  return (
    <div className="h-screen bg-[#020617] flex items-center justify-center p-8 text-center">
      <div className="max-w-xs space-y-4">
        <Clock className="w-8 h-8 text-slate-700 mx-auto" />
        <h2 className="text-white font-black uppercase tracking-tight text-xl">FX Market Closed</h2>
        <p className="text-slate-400 text-xs leading-relaxed">
          Sunday 5:00 PM – Friday 5:00 PM (ET).
        </p>
        <Link href="/dashboard/fx/daily" className="inline-block mt-4 text-blue-500 text-[10px] font-black uppercase tracking-widest border border-blue-500/30 px-6 py-2 rounded-lg hover:bg-blue-500/10 transition">
          ← Back to Daily
        </Link>
      </div>
    </div>
  );
}

function isFXTradingTime() {
  const now = new Date();

  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    weekday: "short",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
  }).formatToParts(now);

  const weekday = parts.find(p => p.type === "weekday")!.value;
  const hour = Number(parts.find(p => p.type === "hour")!.value);
  const minute = Number(parts.find(p => p.type === "minute")!.value);

  const totalMin = hour * 60 + minute;
  const CLOSE_MIN = 17 * 60 + 3; // 5:03 PM ET

  // FX market: Sun 5:00 PM → Fri 5:03 PM ET
  if (weekday === "Sun" && totalMin >= 17 * 60) return true;
  if (weekday !== "Sat" && weekday !== "Sun" && weekday !== "Fri") return true;
  if (weekday === "Fri" && totalMin <= CLOSE_MIN) return true;

  return false;
}