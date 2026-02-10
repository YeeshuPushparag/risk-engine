"use client";

import React, { useEffect, useState, useRef } from "react";
import { useRouter } from "next/navigation";
import { Search, Command, X } from "lucide-react";

export default function TickerSearch({ ticker_url }: { ticker_url: string }) {
  const [query, setQuery] = useState("");
  const [tickers, setTickers] = useState<string[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const router = useRouter();
  const dropdownRef = useRef<HTMLDivElement>(null);

 useEffect(() => {
  async function loadTickers() {
    try {
      const type = ticker_url?.includes("intraday") ? "intraday" : "daily";

      const res = await fetch(
        `/api/equity/daily/main/tickers-list?type=${type}`
      );

      const data: string[] = await res.json();
      setTickers(data);
    } catch (err) {
      console.error("Failed to load tickers:", err);
    }
  }

  loadTickers();
}, [ticker_url]);

  // Close dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const filtered = tickers.filter((t) =>
    t.toLowerCase().includes(query.toLowerCase())
  ).slice(0, 10); // Limit visible results for cleaner UI

  const go = (ticker: string) => {
    if (!ticker_url) return;
    setQuery("");
    setIsOpen(false);
    router.push(`/dashboard/${ticker_url}/ticker/${encodeURIComponent(ticker)}`);
  };

  return (
    <>
    <div className="relative w-72" ref={dropdownRef}>
      {/* Search Input Container */}
      <div className="relative group">
        <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none z-[9900]">
          <Search className="h-4 w-4 text-slate-400 group-focus-within:text-blue-500 transition-colors" />
        </div>
        
        <input
          type="text"
          placeholder="Jump to Ticker..."
          className="w-full bg-slate-100 border border-slate-200 rounded-xl pl-10 pr-10 py-2.5 text-sm font-bold text-slate-700 placeholder:text-slate-400 focus:bg-white focus:ring-4 focus:ring-blue-500/10 focus:border-blue-500 outline-none transition-all"
          value={query}
          onFocus={() => setIsOpen(true)}
          onChange={(e) => {
            setQuery(e.target.value);
            setIsOpen(true);
          }}
        />

        <div className="absolute inset-y-0 right-3 flex items-center gap-1">
          {query ? (
            <button onClick={() => setQuery("")}>
              <X className="h-3.5 w-3.5 text-slate-400 hover:text-slate-600" />
            </button>
          ) : (
            <kbd className="hidden sm:inline-flex items-center gap-1 px-1.5 font-mono text-[10px] font-medium text-slate-400 bg-white border border-slate-200 rounded">
              <Command className="w-2.5 h-2.5" /> K
            </kbd>
          )}
        </div>
      </div>

      {/* Professional Dropdown Result List */}
      {isOpen && query && (
        <div className="absolute w-full mt-2 bg-white border border-slate-200 rounded-xl shadow-2xl z-[100] overflow-hidden animate-in fade-in slide-in-from-top-2 duration-200">
          <div className="p-2 border-b border-slate-50">
            <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest px-2">
              Results for "{query}"
            </p>
          </div>

          <div className="max-h-64 overflow-y-auto">
            {filtered.length === 0 ? (
              <div className="p-4 text-center">
                <p className="text-xs text-slate-500 font-medium">No instruments found</p>
              </div>
            ) : (
              filtered.map((t) => (
                <div
                  key={t}
                  onClick={() => go(t)}
                  className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-blue-50 transition-colors group"
                >
                  <div className="flex items-center gap-3">
                    <div className="w-8 h-8 rounded bg-slate-100 flex items-center justify-center text-[10px] font-black text-slate-500 group-hover:bg-blue-600 group-hover:text-white transition-colors">
                      {t.substring(0, 2)}
                    </div>
                    <span className="text-sm font-bold text-slate-700">{t}</span>
                  </div>
                  <ChevronRight className="w-4 h-4 text-slate-300 group-hover:text-blue-500 transition-colors" />
                </div>
              ))
            )}
          </div>

          <div className="p-2 bg-slate-50 border-t border-slate-100">
             <p className="text-[9px] text-center text-slate-400 font-medium italic">
                Press Enter to select
             </p>
          </div>
        </div>
      )}
    </div>
    </>
  );
}

// Sub-component for the arrow icon used in the list
function ChevronRight(props: any) {
  return (
    <svg 
      {...props} 
      xmlns="http://www.w3.org/2000/svg" 
      fill="none" 
      viewBox="0 0 24 24" 
      stroke="currentColor" 
      strokeWidth={3}
    >
      <path strokeLinecap="round" strokeLinejoin="round" d="M9 5l7 7-7 7" />
    </svg>
  );
}