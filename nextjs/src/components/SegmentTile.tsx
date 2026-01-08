"use client";

import React from "react";
import Link from "next/link";
import {
  ArrowUpRight,
  ArrowDownRight,
  AlertCircle,
  ChevronRight,
  Globe
} from "lucide-react";

interface SegmentProps {
  name: string;
  exposure: string;
  link: string;
  pnl: string;
  riskColor: "green" | "yellow" | "red";
  alerts: number;
}

const SEGMENT_LABELS: Record<string, string> = {
  equity: "Daily · Market Risk",
  fx: "Daily · Market Risk",
  commodities: "Daily · Market Risk",
  bonds: "Daily · Market Risk",
  derivatives: "Daily · Notional Overlay",
  collateral: "Daily · Coverage Asset",
  loans: "Monthly · Credit Exposure",
};

export default function SegmentTile({
  name,
  link,
  exposure,
  pnl,
  riskColor,
  alerts
}: SegmentProps) {
  const isLoss = pnl.includes("-");
  const isLoans = link === "loans";

  const effectiveRiskColor = alerts === 0 ? "green" : riskColor;

  const statusLabel = {
    green: "Stable",
    yellow: "Caution",
    red: "Breach"
  }[effectiveRiskColor];

  const pnlLabel = isLoans ? "Monthly P&L" : "Daily P&L";
  const segmentLabel = SEGMENT_LABELS[link] ?? "";

  return (
    <Link
      href={`/dashboard/${link}`}
      className="group bg-white border border-slate-200 rounded-2xl p-6 transition-all duration-300 shadow-[0_8px_30px_rgb(0,0,0,0.12)] hover:shadow-[0_20px_40px_rgba(0,0,0,0.2)] hover:border-blue-500 hover:-translate-y-1 relative overflow-hidden flex flex-col justify-between min-h-[230px]"
    >
      {/* IDENTITY SECTION */}
      <div>
        <div className="flex justify-between items-start mb-3">
          <div>
            <h3 className="text-xl font-black text-slate-900 tracking-tighter uppercase group-hover:text-blue-600 transition-colors">
              {name}
            </h3>

            {/* NEW LABEL */}
            {segmentLabel && (
              <p className="mt-1 text-[9px] font-black uppercase tracking-[0.25em] text-slate-400">
                {segmentLabel}
              </p>
            )}

            <div className="flex items-center gap-1.5 mt-2">
              <div
                className={`w-1.5 h-1.5 rounded-full ${
                  effectiveRiskColor === "green"
                    ? "bg-emerald-500"
                    : effectiveRiskColor === "yellow"
                    ? "bg-amber-500"
                    : "bg-red-600"
                }`}
              />
              <span className="text-[9px] font-black text-slate-400 uppercase tracking-[0.2em]">
                {statusLabel}
              </span>
            </div>
          </div>

          {/* ALERT BADGE — always shown */}
          <div
            className={`px-2 py-1 rounded flex items-center gap-1.5 shadow-lg ${
              alerts > 0
                ? "bg-red-600 text-white shadow-red-100"
                : "bg-emerald-500/10 text-emerald-600 shadow-emerald-100"
            }`}
          >
            <AlertCircle size={12} className="shrink-0" />
            <span className="text-[10px] font-black">{alerts}</span>
          </div>
        </div>
      </div>

      {/* DATA SECTION */}
      <div className="space-y-4">
        <div className="flex justify-between items-end border-b border-slate-50 pb-2">
          <p className="text-[9px] font-black text-slate-400 uppercase tracking-widest">
            Exposure
          </p>
          <p className="text-lg font-black text-slate-800 tracking-tight">
            {exposure}
          </p>
        </div>

        <div className="flex justify-between items-end">
          <p className="text-[9px] font-black text-slate-400 uppercase tracking-widest">
            {pnlLabel}
          </p>

          <div
            className={`flex items-center gap-0.5 font-black ${
              isLoss ? "text-red-600" : "text-emerald-500"
            }`}
          >
            <span className="text-sm font-mono">{pnl}</span>
            {isLoss ? (
              <ArrowDownRight size={16} />
            ) : (
              <ArrowUpRight size={16} />
            )}
          </div>
        </div>
      </div>

      {/* HOVER ACTION */}
      <div className="mt-6 pt-4 border-t border-slate-50 flex items-center justify-between text-blue-600 opacity-0 group-hover:opacity-100 transition-all duration-300">
        <span className="text-[9px] font-black uppercase tracking-[0.3em]">
          Open Terminal
        </span>
        <div className="bg-blue-50 p-1.5 rounded-full">
          <ChevronRight size={14} />
        </div>
      </div>

      {/* DECORATIVE WATERMARK */}
      <div className="absolute -right-4 -bottom-4 opacity-[0.03] group-hover:opacity-[0.07] transition-opacity pointer-events-none">
        <Globe size={120} className="text-slate-900" />
      </div>
    </Link>
  );
}
