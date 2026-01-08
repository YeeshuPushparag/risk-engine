"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { 
  BarChart3, 
  Activity, 
  Clock, 
  Globe, 
  ChevronRight, 
  ShieldCheck,
  Zap,
  Lock,
  Info
} from "lucide-react";

/* ---------------- TYPES ---------------- */

interface MarketStatus {
  isOpen: boolean;
  dayEST: string;
  timeEST: string;
  timeIST: string;
}

/* ---------------- FX MARKET LOGIC (STRICTLY EST) ---------------- */

const checkFxMarketOpen = (): MarketStatus => {
  const now = new Date();

  // 1. Get New York Time (EST/EDT) for Logic
  const estFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
    weekday: "long",
  });

  // 2. Get India Time (IST) for User Display
  const istFormatter = new Intl.DateTimeFormat("en-US", {
    timeZone: "Asia/Kolkata",
    hour: "numeric",
    minute: "numeric",
    hour12: false,
    weekday: "long",
  });

  const estParts = estFormatter.formatToParts(now);
  const getValue = (parts: Intl.DateTimeFormatPart[], type: string) => parts.find((p) => p.type === type)?.value;
  
  const dayEST = getValue(estParts, "weekday") ?? "Unknown";
  const hourEST = parseInt(getValue(estParts, "hour") ?? "0");
  const minuteEST = parseInt(getValue(estParts, "minute") ?? "0");
  
  const istTime = istFormatter.format(now);

  const currentTimeInMinutesEST = hourEST * 60 + minuteEST;

  // LOGIC IS STRICTLY ON EST: Opens Sun 17:00, Closes Fri 17:00
  let isOpen = true;

  if (dayEST === "Saturday") {
    isOpen = false;
  } else if (dayEST === "Sunday") {
    // Closed until 5:00 PM EST
    if (currentTimeInMinutesEST < 17 * 60) isOpen = false;
  } else if (dayEST === "Friday") {
    // Closes at 5:00 PM EST
    if (currentTimeInMinutesEST >= 17 * 60) isOpen = false;
  }

  return {
    isOpen,
    dayEST,
    timeEST: `${hourEST}:${minuteEST < 10 ? "0" + minuteEST : minuteEST} EST`,
    timeIST: `${istTime} IST`
  };
};

/* ---------------- MAIN COMPONENT ---------------- */

export default function FxMainPage() {
  const [status, setStatus] = useState<MarketStatus>({ 
    isOpen: false, 
    dayEST: "Syncing...", 
    timeEST: "00:00 EST",
    timeIST: "00:00 IST"
  });

  useEffect(() => {
    const update = () => setStatus(checkFxMarketOpen());
    update();
    const interval = setInterval(update, 30000);
    return () => clearInterval(interval);
  }, []);

  return (
    <main className="min-h-screen bg-[#020617] text-slate-300 relative overflow-hidden">
      <div className="absolute inset-0 bg-[linear-gradient(to_right,#1e293b_1px,transparent_1px),linear-gradient(to_bottom,#1e293b_1px,transparent_1px)] bg-[size:4rem_4rem] [mask-image:radial-gradient(ellipse_60%_50%_at_50%_0%,#000_70%,transparent_100%)] opacity-20 pointer-events-none" />

      {/* 1. TOP TERMINAL HEADER WITH DUAL CLOCKS */}
      <div className="relative bg-slate-950/80 backdrop-blur-xl border-b border-slate-800/60 px-8 py-10">
        <div className="max-w-6xl mx-auto flex flex-col md:flex-row justify-between items-start md:items-end gap-8">
          <div>
            <div className="flex items-center gap-5 mb-6">
              <div className="bg-blue-600 p-2.5 rounded-xl shadow-[0_0_20px_rgba(37,99,235,0.4)]">
                <Globe className="w-7 h-7 text-white" />
              </div>
              <div>
                <h1 className="text-3xl font-black text-white tracking-tighter uppercase italic">
                  FX Risk Intelligence
                </h1>
                <div className="flex gap-4 mt-1">
                  <span className="text-[10px] font-black text-blue-400 uppercase tracking-widest">Logic: NY Session (EST)</span>
                </div>
              </div>
            </div>
            <p className="text-slate-400 max-w-2xl text-lg font-medium leading-relaxed italic">
              Analyze currency exposure across <span className="text-white">EOD reporting</span> and <span className="text-emerald-400">live sessions</span>.
            </p>
          </div>

          {/* DUAL CLOCK DISPLAY */}
          <div className="flex flex-col sm:flex-row gap-4 w-full md:w-auto">
            <div className="flex-1 bg-slate-900 border border-slate-800 rounded-2xl px-5 py-3">
              <p className="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-1">New York Time</p>
              <p className="text-sm font-mono font-bold text-white italic">{status.timeEST}</p>
            </div>
            <div className="flex-1 bg-slate-900 border border-slate-800 rounded-2xl px-5 py-3">
              <p className="text-[8px] font-black text-slate-500 uppercase tracking-widest mb-1">Local India Time</p>
              <p className="text-sm font-mono font-bold text-emerald-500 italic">{status.timeIST}</p>
            </div>
          </div>
        </div>
      </div>

      {/* 2. NAVIGATION SELECTION */}
      <div className="relative max-w-6xl mx-auto p-8 lg:p-12">
        <section className="grid grid-cols-1 md:grid-cols-2 gap-10">
          <NavCard
            title="Daily FX Risk"
            description="Official end-of-day risk snapshots including carry analysis and portfolio-wide VaR."
            href="/dashboard/fx/daily"
            tag="EOD Snapshot"
            icon={<BarChart3 className="w-7 h-7 text-blue-500" />}
            metrics={["Full Portfolio VaR", "Carry Attribution"]}
            isOpen={true}
          />

          <NavCard
            title="Intraday FX Risk"
            description="High-frequency monitoring. Track real-time exposure shifts and session P&L during active hours."
            href="/dashboard/fx/intraday"
            tag="Live Engine"
            icon={<Activity className="w-7 h-7 text-emerald-500" />}
            metrics={["Real-time P&L", "1-Min Spot"]}
            isLive
            isOpen={status.isOpen}
          />
        </section>

        {/* 3. CONVERSION TABLE FOR USER (EST to IST) */}
        <section className="mt-12 bg-slate-900/40 border border-slate-800 rounded-3xl p-8">
          <div className="flex items-center gap-3 mb-6">
            <Info className="w-5 h-5 text-blue-500" />
            <h3 className="text-xs font-black text-white uppercase tracking-[0.2em]">Market Schedule Conversion</h3>
          </div>
          
          <div className="overflow-x-auto">
            <table className="w-full text-left">
              <thead>
                <tr className="border-b border-slate-800">
                  <th className="pb-4 text-[10px] font-black text-slate-500 uppercase tracking-widest">Event</th>
                  <th className="pb-4 text-[10px] font-black text-slate-500 uppercase tracking-widest text-blue-400">NY Logic (EST)</th>
                  <th className="pb-4 text-[10px] font-black text-slate-500 uppercase tracking-widest text-orange-400">Local Time (IST)</th>
                </tr>
              </thead>
              <tbody className="text-xs font-bold uppercase tracking-tighter">
                <tr className="border-b border-slate-800/50">
                  <td className="py-4 text-slate-300">Market Opens</td>
                  <td className="py-4 text-white">Sunday 17:00</td>
                  <td className="py-4 text-slate-400">Monday 03:30</td>
                </tr>
                <tr>
                  <td className="py-4 text-slate-300">Market Closes</td>
                  <td className="py-4 text-white">Friday 17:00</td>
                  <td className="py-4 text-slate-400">Saturday 03:30</td>
                </tr>
              </tbody>
            </table>
          </div>
        </section>

        {/* 4. SYSTEM FOOTER */}
        <section className="mt-12 pt-10 border-t border-slate-900 grid grid-cols-1 md:grid-cols-2 gap-12">
          <div className="flex gap-5">
            <ShieldCheck className="w-5 h-5 text-slate-600 shrink-0" />
            <p className="text-[10px] text-slate-500 leading-relaxed font-black uppercase tracking-widest">
              Daily views are synchronized with the central risk engine for regulatory reporting.
            </p>
          </div>
          <div className="flex gap-5">
            <Clock className="w-5 h-5 text-slate-600 shrink-0" />
            <p className="text-[10px] text-slate-500 leading-relaxed font-black uppercase tracking-widest">
              Intraday analytics are tuned for active session management during global hours.
            </p>
          </div>
        </section>
      </div>
    </main>
  );
}

/* ---------------- NAV CARD ---------------- */

function NavCard({ title, description, href, tag, icon, metrics, isLive = false, isOpen = true }: any) {
  return (
    <div className="relative group">
      {!isOpen && (
        <div className="absolute inset-0 z-20 bg-slate-950/85 backdrop-blur-[4px] rounded-2xl flex flex-col items-center justify-center border border-dashed border-slate-700 text-center px-6">
          <Lock className="w-8 h-8 text-red-500 mb-4" />
          <h4 className="text-sm font-black text-white uppercase tracking-widest mb-1">Market is Offline</h4>
          <p className="text-[10px] text-slate-400 font-bold uppercase tracking-widest italic">Logic Gate: Friday 17:00 EST</p>
          <p className="text-[9px] text-blue-500 font-black uppercase mt-2">Reopens Mon 03:30 IST</p>
        </div>
      )}

      <Link
        href={isOpen ? href : "#"}
        onClick={(e) => !isOpen && e.preventDefault()}
        className={`relative block bg-slate-900/40 border border-slate-800 rounded-2xl p-10 transition-all duration-500 flex flex-col justify-between min-h-[360px] overflow-hidden
          ${isOpen ? "hover:border-blue-500/50 hover:bg-slate-900/60 hover:-translate-y-2 shadow-2xl" : "cursor-not-allowed opacity-40"}`}
      >
        <div>
          <div className="flex items-center justify-between mb-8">
            <div className={`p-4 rounded-2xl ${isOpen ? (isLive ? 'bg-emerald-500/10' : 'bg-blue-500/10') : 'bg-slate-800'}`}>
              {icon}
            </div>
            <span className={`text-[10px] font-black px-3 py-1.5 rounded-md tracking-[0.2em] uppercase border ${
              isOpen ? (isLive ? 'bg-emerald-500/10 border-emerald-500/20 text-emerald-400' : 'bg-blue-500/10 border-blue-200 text-blue-400') : 'bg-slate-800 border-slate-700 text-slate-600'
            }`}>
              {tag}
            </span>
          </div>

          <h2 className={`text-3xl font-black mb-4 uppercase italic tracking-tighter ${isOpen ? 'text-white group-hover:text-blue-500' : 'text-slate-600'}`}>
            {title}
          </h2>
          <p className="text-slate-500 text-sm font-medium leading-relaxed mb-6">{description}</p>
          
          <div className="flex flex-wrap gap-2">
            {metrics.map((m: string) => (
              <span key={m} className="text-[9px] font-black text-slate-500 border border-slate-800 px-2.5 py-1 rounded-md uppercase bg-slate-950/50">
                {m}
              </span>
            ))}
          </div>
        </div>

        <div className={`flex items-center text-xs font-black uppercase tracking-[0.3em] mt-8 ${isOpen ? 'text-blue-500' : 'text-slate-700'}`}>
          {isOpen ? "Launch System" : "Standby"} <ChevronRight className="w-4 h-4 ml-2" />
        </div>
      </Link>
    </div>
  );
}