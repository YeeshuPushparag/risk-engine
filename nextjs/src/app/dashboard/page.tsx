"use client";

import React, { useEffect, useState } from "react";
import SegmentTile from "@/components/SegmentTile";
import { Globe, Loader2, Calendar, Clock } from "lucide-react";

export default function DashboardPage() {
  const [data, setData] = useState<any>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    fetch("/api/dashboard")
      .then((res) => res.json())
      .then((json) => {
        setData(json);
        setIsLoading(false);
      });
  }, []);

  if (isLoading) {
    return (
      <div className="min-h-screen bg-[#020617] flex flex-col items-center justify-center">
        <Loader2 className="w-10 h-10 text-blue-500 animate-spin" />
        <p className="mt-4 text-slate-500 font-mono text-xs tracking-widest">LOADING RISK DATA...</p>
      </div>
    );
  }

  return (
    <main className="min-h-screen bg-[#020617] pb-10">
      {/* HEADER - RESPONSIVE & NON-FIXED */}
      <header className="bg-slate-900/40 border-b border-slate-800">
        <div className="max-w-[1600px] mx-auto px-4 sm:px-8 py-6">
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-6">

            {/* Branding & Date */}
            <div className="flex items-center gap-4">
              <div className="bg-blue-600 p-2.5 rounded-lg shrink-0">
                <Globe className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-xl sm:text-2xl font-black text-white tracking-tight uppercase italic">
                  Risk Monitor
                </h1>
                <div className="flex flex-wrap gap-2 mt-1">
                  <span className="flex items-center gap-1 text-[10px] font-bold text-amber-500 bg-amber-500/10 px-2 py-0.5 rounded border border-amber-500/20">
                    <Clock className="w-3 h-3" /> EOD
                  </span>
                  <span className="flex items-center gap-1 text-[10px] font-bold text-blue-400 bg-blue-400/10 px-2 py-0.5 rounded border border-blue-400/20">
                    <Calendar className="w-3 h-3" />
                    {data?.metadata?.market_date ? new Date(data.metadata.market_date).toLocaleDateString('en-US', {
                      weekday: 'long',
                      month: 'short',
                      day: 'numeric',
                      year: 'numeric'
                    }) : "N/A"}
                  </span>
                </div>
              </div>
            </div>

            {/* Total Exposure */}
            <div className="w-full md:w-auto pt-4 md:pt-0 border-t md:border-0 border-slate-800">
              <p className="text-[10px] font-bold text-slate-500 uppercase tracking-widest mb-1">
                Total Gross Exposure
              </p>
              <p className="text-3xl sm:text-4xl font-mono font-bold text-white tracking-tighter">
                {data?.total_gross_exposure}
              </p>
            </div>
          </div>
        </div>
      </header>

      {/* GRID */}
      <div className="max-w-[1600px] mx-auto p-4 sm:p-8">
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4 sm:gap-6">
          {data?.segments.map((segment: any) => (
            <SegmentTile
              key={segment.id}
              {...segment}
              // Force 0 Alerts to be Green
              alerts={Number(segment.alerts) === 0 ? "0 ALERTS" : segment.alerts}
              riskColor={Number(segment.alerts) === 0 ? "green" : segment.riskColor}
            />
          ))}
        </div>
      </div>
    </main>
  );
}