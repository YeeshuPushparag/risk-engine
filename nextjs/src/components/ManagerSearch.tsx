"use client";

import React, { useEffect, useState, useRef } from "react";
import { useRouter } from "next/navigation";
import { Users, ChevronRight, X, UserCheck } from "lucide-react";

export default function ManagerSearch({ manager_url }: { manager_url: string }) {
  const [query, setQuery] = useState("");
  const [managers, setManagers] = useState<string[]>([]);
  const [isOpen, setIsOpen] = useState(false);
  const router = useRouter();
  const dropdownRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    async function loadManagers() {
      try {
        const res = await fetch("/api/equity/daily/main/managers-list");
        const data: string[] = await res.json();
        setManagers(data);
      } catch (err) {
        console.error("Failed to load managers:", err);
      }
    }
    loadManagers();
  }, []);

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  const filtered = managers.filter((m) =>
    m.toLowerCase().includes(query.toLowerCase())
  ).slice(0, 8);

  const go = (manager: string) => {
    if (!manager_url) return;
    setQuery("");
    setIsOpen(false);
    router.push(`/dashboard/${manager_url}/manager/${encodeURIComponent(manager)}`);
  };

  return (
    <div className="relative w-72" ref={dropdownRef}>
      <div className="relative group">
        <div className="absolute inset-y-0 left-3 flex items-center pointer-events-none">
          <Users className="h-4 w-4 text-slate-400 group-focus-within:text-blue-500 transition-colors z-[9900]" />
        </div>
        
        <input
          type="text"
          placeholder="Filter by Manager..."
          className="w-full bg-slate-100 border border-slate-200 rounded-xl pl-10 pr-10 py-2.5 text-sm font-bold text-slate-700 placeholder:text-slate-400 focus:bg-white focus:ring-4 focus:ring-blue-500/10 focus:border-blue-500 outline-none transition-all"
          value={query}
          onFocus={() => setIsOpen(true)}
          onChange={(e) => {
            setQuery(e.target.value);
            setIsOpen(true);
          }}
        />

        {query && (
          <button 
            onClick={() => setQuery("")}
            className="absolute inset-y-0 right-3 flex items-center"
          >
            <X className="h-3.5 w-3.5 text-slate-400 hover:text-slate-600" />
          </button>
        )}
      </div>

      {/* DROPDOWN  */}
      {isOpen && query && (
        <div className="absolute w-full mt-2 bg-white border border-slate-200 rounded-xl shadow-2xl overflow-hidden animate-in fade-in slide-in-from-top-2 duration-200">
          <div className="p-2 bg-slate-50/50 border-b border-slate-100">
            <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest px-2">
              Equity Portfolios
            </p>
          </div>

          <div className="max-h-60 overflow-y-auto">
            {filtered.length === 0 ? (
              <div className="p-4 text-center">
                <p className="text-xs text-slate-500 font-medium italic">No matches found</p>
              </div>
            ) : (
              filtered.map((m) => (
                <div
                  key={m}
                  onClick={() => go(m)}
                  className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-blue-50 transition-colors group"
                >
                  <div className="flex items-center gap-3">
                    <div className="w-8 h-8 rounded-full bg-blue-100 flex items-center justify-center text-[10px] font-black text-blue-600 group-hover:bg-blue-600 group-hover:text-white transition-all shadow-sm">
                      {m.split(' ').map(n => n[0]).join('').substring(0, 2).toUpperCase()}
                    </div>
                    <div>
                      <span className="text-sm font-bold text-slate-700 block line-clamp-1">{m}</span>
                      <span className="text-[9px] font-bold text-slate-400 uppercase tracking-tight">Active Allocation</span>
                    </div>
                  </div>
                  <ChevronRight className="w-4 h-4 text-slate-300 group-hover:text-blue-500 transition-colors" />
                </div>
              ))
            )}
          </div>

          <div className="p-2 bg-slate-50 border-t border-slate-100 flex items-center justify-center gap-2">
             <UserCheck className="w-3 h-3 text-slate-300" />
             <p className="text-[9px] text-slate-400 font-bold uppercase tracking-tighter">
                Manager Directory
             </p>
          </div>
        </div>
      )}
    </div>
  );
}