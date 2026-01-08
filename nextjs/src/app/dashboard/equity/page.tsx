"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import {
  Database,
  Zap,
  Clock,
  ChevronRight,
  ShieldCheck,
  Activity,
  Lock,
  ArrowUpRight,
  LineChart,
  BarChart3
} from "lucide-react";

/* ---------------- TYPES & UI HELPERS ---------------- */

interface MarketStatus {
  isOpen: boolean;
  day: string;
  time: string;
}

const Feature = ({ text }: { text: string }) => (
  <div className="flex items-center gap-2 text-[10px] text-slate-500 font-black uppercase tracking-tight">
    <div className="w-1 h-1 rounded-full bg-blue-500" />
    {text}
  </div>
);

/* ---------------- MARKET LOGIC ---------------- */

const checkMarketOpen = (): MarketStatus => {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
    weekday: "long",
  });

  const parts = formatter.formatToParts(new Date());
  const getValue = (type: string) => parts.find((p) => p.type === type)?.value;
  
  const day = getValue("weekday") ?? "Unknown";
  const hour = parseInt(getValue("hour") ?? "0");
  const minute = parseInt(getValue("minute") ?? "0");

  const isWeekend = day === "Saturday" || day === "Sunday";
  const currentTimeInMinutes = hour * 60 + minute;
  const openTime = 9 * 60 + 30; // 09:30 AM
  const closeTime = 16 * 60;    // 04:00 PM

  const isOpen = !isWeekend && currentTimeInMinutes >= openTime && currentTimeInMinutes < closeTime;
  
  return {
    isOpen,
    day,
    time: `${hour}:${minute < 10 ? "0" + minute : minute} EST`
  };
};

/* ---------------- MAIN COMPONENT ---------------- */

export default function EquityLandingPage() {
  const [date, setDate] = useState<string | null>(null);
  const [marketStatus, setMarketStatus] = useState<MarketStatus>({ 
    isOpen: false, 
    day: "Detecting...", 
    time: "00:00 EST" 
  });

  useEffect(() => {
    async function loadLatestDate() {
      try {
        const dateRes = await fetch("/api/equity/daily/main/latest-date", { cache: "no-store" });
        const data = await dateRes.json();
        setDate(data.latest_date);
      } catch (err) {
        setDate(null);
      }
    }

    const updateStatus = () => setMarketStatus(checkMarketOpen());
    updateStatus();
    const interval = setInterval(updateStatus, 30000);

    loadLatestDate();
    return () => clearInterval(interval);
  }, []);

  return (
    <div className="min-h-screen bg-[#020617] text-slate-300 pb-20 relative overflow-hidden font-sans">
      <div className="absolute inset-0 bg-[linear-gradient(to_right,#1e293b_1px,transparent_1px),linear-gradient(to_bottom,#1e293b_1px,transparent_1px)] bg-[size:4rem_4rem] [mask-image:radial-gradient(ellipse_60%_50%_at_50%_0%,#000_70%,transparent_100%)] opacity-20 pointer-events-none" />

      <main className="relative max-w-[1400px] mx-auto p-8 lg:p-12 space-y-10">

        {/* --- HEADER --- */}
        <header className="flex flex-col md:flex-row md:items-end justify-between gap-6 border-b border-slate-800/60 pb-10">
          <div className="space-y-3">
            <div className="flex items-center gap-3 text-blue-500 font-black text-[10px] uppercase tracking-[0.3em]">
              <div className="w-6 h-[2px] bg-blue-600 shadow-[0_0_8px_#2563eb]" />
              Asset Class: Public Equities
            </div>
            <h1 className="text-4xl font-black text-white tracking-tighter uppercase italic">
              Portfolio{" "}
              <span className="text-slate-600 not-italic font-light">Navigator</span>
            </h1>
            <p className="text-slate-500 max-w-xl text-sm font-medium leading-relaxed">
              Consolidated access to{" "}
              <span className="text-white">Batch Reporting</span> and{" "}
              <span className="text-emerald-400">Real-Time WebSocket Streams</span>{" "}
              for US-listed tickers.
            </p>
          </div>

          <div className="bg-slate-900/50 border border-slate-800 rounded-2xl px-6 py-4 backdrop-blur-md flex items-center gap-5 shadow-2xl">
            <div className="p-2.5 bg-slate-950 rounded-xl border border-slate-800">
              <Clock className="w-5 h-5 text-blue-500" />
            </div>
            <div>
              <p className="text-[9px] font-black text-slate-500 uppercase tracking-widest">
                NY Market Time ({marketStatus.day})
              </p>
              <p className="text-lg font-mono font-bold text-white tracking-tighter italic">
                {marketStatus.time}
              </p>
            </div>
          </div>
        </header>

        {/* --- SYSTEM STATUS & AVAILABILITY --- */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-8 items-stretch">
          <div className="lg:col-span-2 bg-slate-900/30 border border-slate-800 rounded-3xl p-8">
            <h2 className="text-[10px] font-black text-blue-500 uppercase tracking-[0.3em] mb-8 flex items-center gap-2">
              <Activity className="w-4 h-4" /> Market Data Availability
            </h2>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
              <div className="space-y-4">
                <h3 className="text-sm font-black text-white uppercase tracking-tight flex items-center gap-2">
                  <Zap className="w-4 h-4 text-emerald-500" /> Intraday Session
                </h3>
                <p className="text-xs text-slate-500 leading-relaxed font-medium">
                  Streaming 1-minute OHLCV data directly from market data providers. 
                  Real-time updates active during US exchange hours.
                </p>
                <div className="inline-flex items-center gap-2 px-3 py-1.5 bg-blue-500/10 text-blue-400 rounded-lg text-[10px] font-black border border-blue-500/20 uppercase tracking-widest">
                  Active: 09:30 – 16:00 EST
                </div>
              </div>

              <div className="space-y-4">
                <h3 className="text-sm font-black text-white uppercase tracking-tight flex items-center gap-2">
                  <LineChart className="w-4 h-4 text-indigo-500" /> Regional Monitoring
                </h3>
                <p className="text-xs text-slate-500 leading-relaxed font-medium">
                  Automatic currency and timezone conversion for offshore desks. 
                  Real-time session monitoring mapped to local IST.
                </p>
                <div className="inline-flex items-center gap-2 px-3 py-1.5 bg-indigo-500/10 text-indigo-400 rounded-lg text-[10px] font-black border border-indigo-500/20 uppercase tracking-widest">
                  Live: 20:00 – 02:30 IST
                </div>
              </div>
            </div>
          </div>

          <div className="bg-slate-900 border border-slate-800 rounded-3xl p-8 flex flex-col justify-between shadow-2xl relative overflow-hidden">
            <div className="absolute top-0 right-0 p-8 opacity-10">
               <ShieldCheck className="w-24 h-24 text-blue-500" />
            </div>
            <div>
              <div className="w-10 h-10 bg-slate-950 rounded-xl flex items-center justify-center border border-slate-800 mb-6">
                <ShieldCheck className="w-5 h-5 text-blue-500" />
              </div>
              <h3 className="text-sm font-black text-white uppercase tracking-widest mb-3">System Access</h3>
              <p className="text-slate-500 text-[11px] leading-relaxed uppercase font-bold tracking-tight">
                Data routing is <span className="text-emerald-500 underline decoration-emerald-500/30 underline-offset-4">Direct</span>. 
                WebSocket connectivity and API endpoints are currently operational.
              </p>
            </div>
            <div className="mt-8 pt-6 border-t border-slate-800">
              <p className="text-[9px] text-slate-600 uppercase font-black tracking-[0.3em]">Connectivity Status</p>
              <div className="flex items-center gap-2 mt-2">
                <div className={`w-2 h-2 rounded-full animate-pulse ${marketStatus.isOpen ? "bg-emerald-500 shadow-[0_0_8px_#10b981]" : "bg-red-500"}`} />
                <p className={`text-[10px] font-black uppercase ${marketStatus.isOpen ? "text-emerald-500" : "text-red-500"}`}>
                  {marketStatus.isOpen ? "Live Stream Connected" : "Stream Standby (Market Closed)"}
                </p>
              </div>
            </div>
          </div>
        </div>

        {/* --- NAVIGATION CARDS --- */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8">
          
          {/* EOD CARD */}
          <Link href="/dashboard/equity/daily" className="group relative">
            <div className="h-full bg-slate-900/40 border border-slate-800 hover:border-blue-500/50 rounded-3xl p-8 transition-all duration-500 hover:bg-slate-900/60 flex flex-col justify-between">
              <div>
                <div className="flex justify-between items-start mb-8">
                  <div className="p-4 bg-blue-500/10 rounded-2xl border border-blue-500/20 group-hover:scale-110 transition-transform">
                    <Database className="w-8 h-8 text-blue-500" />
                  </div>
                  <ArrowUpRight className="text-slate-700 group-hover:text-blue-500 transition-colors" />
                </div>
                <h3 className="text-2xl font-black text-white uppercase italic tracking-tighter mb-4">
                  Batch Reporting <span className="text-slate-500 font-light not-italic text-sm ml-2">EOD</span>
                </h3>
                <div className="grid grid-cols-2 gap-4 mb-6">
                  <Feature text="Risk Ledger" />
                  <Feature text="VaR 99%" />
                  <Feature text="PnL Attribution" />
                  <Feature text="Historical Archive" />
                </div>
                <p className="text-slate-500 text-xs font-medium mb-6">
                  Review finalized risk metrics for horizon: <span className="text-blue-400 font-mono">{date ?? "---"}</span>.
                </p>
              </div>
              <div className="flex items-center gap-2 text-[10px] font-black text-blue-500 uppercase tracking-widest border-t border-slate-800 pt-6">
                Enter EOD Terminal <ChevronRight className="w-3 h-3" />
              </div>
            </div>
          </Link>

          {/* INTRADAY CARD */}
          <div className="relative group">
            {!marketStatus.isOpen && (
              <div className="absolute inset-0 z-20 bg-slate-950/80 backdrop-blur-[3px] rounded-3xl flex flex-col items-center justify-center border border-dashed border-slate-700 text-center px-6">
                <div className="p-4 bg-slate-900 rounded-full mb-4 border border-slate-800 shadow-2xl">
                  <Lock className="w-8 h-8 text-red-500" />
                </div>
                <h4 className="text-sm font-black text-white uppercase tracking-widest mb-2">Market is Offline</h4>
                <p className="text-[10px] text-slate-500 font-bold uppercase leading-relaxed max-w-[200px]">
                  US Markets are open Mon–Fri <br/> 
                  <span className="text-white">09:30 – 16:00 EST</span>
                </p>
              </div>
            )}

            <Link 
              href={marketStatus.isOpen ? "/dashboard/equity/intraday" : "#"} 
              onClick={(e) => !marketStatus.isOpen && e.preventDefault()}
              className={`block h-full bg-slate-900/40 border border-slate-800 rounded-3xl p-8 transition-all duration-500 flex flex-col justify-between
                ${marketStatus.isOpen ? "hover:border-emerald-500/50 hover:bg-slate-900/60 shadow-[0_0_40px_-20px_rgba(16,185,129,0.3)]" : "cursor-not-allowed opacity-40"}`}
            >
              <div>
                <div className="flex justify-between items-start mb-8">
                  <div className={`p-4 rounded-2xl border transition-transform group-hover:scale-110 
                    ${marketStatus.isOpen ? "bg-emerald-500/10 border-emerald-500/20" : "bg-slate-800 border-slate-700"}`}>
                    <Zap className={`w-8 h-8 ${marketStatus.isOpen ? "text-emerald-500" : "text-slate-600"}`} />
                  </div>
                  {marketStatus.isOpen && (
                    <div className="flex items-center gap-2 px-2 py-1 bg-emerald-500/20 rounded text-[8px] font-black text-emerald-400 uppercase animate-pulse">
                      Live Stream
                    </div>
                  )}
                </div>
                <h3 className="text-2xl font-black text-white uppercase italic tracking-tighter mb-4">
                  Live Terminal <span className="text-slate-500 font-light not-italic text-sm ml-2">Intraday</span>
                </h3>
                <div className="grid grid-cols-2 gap-4 mb-6">
                  <Feature text="WSS Feed" />
                  <Feature text="Price Vol" />
                  <Feature text="Limit Alerts" />
                  <Feature text="Session High/Low" />
                </div>
                <p className="text-slate-500 text-xs font-medium mb-6">
                  Real-time price action and streaming risk signals for active US sessions.
                </p>
              </div>
              <div className={`flex items-center gap-2 text-[10px] font-black uppercase tracking-widest border-t pt-6
                ${marketStatus.isOpen ? "text-emerald-500 border-emerald-500/10" : "text-slate-600 border-slate-800"}`}>
                {marketStatus.isOpen ? "Connect to Stream" : "Stream Suspended"} <ChevronRight className="w-3 h-3" />
              </div>
            </Link>
          </div>
        </div>

      </main>
    </div>
  );
}