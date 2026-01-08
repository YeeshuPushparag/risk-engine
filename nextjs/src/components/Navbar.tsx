"use client";

import React, { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { 
  ChevronDown, 
  BellRing,
  Menu,
  X,
  ArrowRight
} from "lucide-react";

const SEGMENTS = [
  { name: "Equity", path: "equity", icon: "EQ", hasIntraday: true },
  { name: "FX", path: "fx", icon: "FX", hasIntraday: true },
  { name: "Commodity", path: "commodities", icon: "commodity", hasIntraday: false },
  { name: "Bonds", path: "bonds", icon: "bonds", hasIntraday: false },
  { name: "Derivatives", path: "derivatives", icon: "derivatives", hasIntraday: false },
  { name: "Collateral", path: "collateral", icon: "collateral", hasIntraday: false },
  { name: "Loans", path: "loans", icon: "loans", hasIntraday: false },
];

export default function Navbar() {
  const pathname = usePathname();
  const [openDropdown, setOpenDropdown] = useState<string | null>(null);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const isActive = (path: string) => pathname.includes(`/dashboard/${path}`);

  return (
    <nav className="w-full bg-[#020617] text-white border-b border-slate-800 sticky top-0 z-[9999] backdrop-blur-md">
      <div className="max-w-[1800px] mx-auto px-6">
        <div className="flex items-center justify-between h-20">
          
          {/* LOGO SECTION */}
          <div className="flex items-center gap-8">
            <Link href="/" className="flex items-center gap-3 group shrink-0">
              <div className="w-10 h-10 bg-blue-600 rounded-lg flex items-center justify-center font-black text-base tracking-tighter shadow-lg shadow-blue-500/20 group-hover:bg-blue-500 transition-colors">
                REX
              </div>
              <span className="font-bold tracking-tighter text-xl uppercase">
                Risk<span className="text-blue-500 font-black">Engine</span>
              </span>
            </Link>

            {/* DESKTOP LINKS */}
            <div className="hidden lg:flex items-center gap-2">
              <Link href="/dashboard" className={`px-4 py-2 text-[10px] font-black tracking-widest hover:text-white transition-colors ${pathname === '/dashboard' ? 'text-white' : 'text-slate-500'}`}>
                DASHBOARD
              </Link>

              <div className="relative" onMouseLeave={() => setOpenDropdown(null)}>
                <button 
                  className={`px-4 py-2 text-[10px] font-black tracking-widest transition-colors flex items-center gap-1 ${openDropdown === 'segments' ? 'text-white' : 'text-slate-500 hover:text-white'}`}
                  onMouseEnter={() => setOpenDropdown('segments')}
                >
                  SEGMENTS <ChevronDown className={`w-3 h-3 transition-transform ${openDropdown === 'segments' ? 'rotate-180' : ''}`} />
                </button>

                {/* Dropdown Menu */}
                <div className={`absolute top-full left-0 w-[520px] bg-slate-900 border border-slate-800 shadow-2xl rounded-xl p-4 mt-2 grid grid-cols-2 gap-3 transition-all ${openDropdown === 'segments' ? 'opacity-100 translate-y-0' : 'opacity-0 -translate-y-2 pointer-events-none'}`}>
                  {SEGMENTS.map((seg) => (
                    <div key={seg.name} className={`group rounded-xl border transition-all ${isActive(seg.path) ? 'bg-blue-600/10 border-blue-500/40' : 'bg-slate-950/50 border-slate-800/50 hover:border-slate-600'}`}>
                      {seg.hasIntraday ? (
                        /* MULTI-LINK LAYOUT (Equity, FX) */
                        <div className="p-3">
                          <div className="flex items-center justify-between mb-2 px-1">
                            <span className="text-[9px] font-black text-blue-500 uppercase">{seg.icon}</span>
                            <span className="text-[11px] font-bold text-slate-200 uppercase">{seg.name}</span>
                          </div>
                          <div className="flex gap-2">
                            <Link href={`/dashboard/${seg.path}/daily`} className="flex-1 text-center text-[8px] bg-slate-800 py-2 rounded font-black hover:bg-blue-600 transition-colors uppercase">Daily</Link>
                            <Link href={`/dashboard/${seg.path}/intraday`} className="flex-1 text-center text-[8px] bg-slate-800 py-2 rounded font-black hover:bg-emerald-600 text-emerald-400 hover:text-white transition-colors uppercase">Live</Link>
                          </div>
                        </div>
                      ) : (
                        /* SINGLE-LINK LAYOUT (Bonds, Loans, etc.) */
                        <Link href={`/dashboard/${seg.path}`} className="flex items-center justify-between p-4 w-full group/item">
                           <div className="flex flex-col">
                             <span className="text-[9px] font-black text-slate-500 uppercase group-hover/item:text-blue-500 transition-colors">{seg.icon}</span>
                             <span className="text-[11px] font-bold text-slate-200 uppercase">{seg.name}</span>
                           </div>
                           <ArrowRight className="w-4 h-4 text-slate-700 group-hover/item:text-white group-hover/item:translate-x-1 transition-all" />
                        </Link>
                      )}
                    </div>
                  ))}
                </div>
              </div>

              <Link href="/dashboard/limits" className={`px-4 py-2 text-[10px] font-black tracking-widest hover:text-white flex items-center gap-2 ${pathname.includes('limits') ? 'text-white' : 'text-slate-500'}`}>
                LIMITS
                <span className="h-1.5 w-1.5 rounded-full bg-red-500 animate-pulse" />
              </Link>
            </div>
          </div>

          {/* RIGHT SIDE */}
          <div className="flex items-center gap-4">
            <button className="hidden sm:block p-2 text-slate-500 hover:text-white relative cursor-pointer outline-none">
              <BellRing className="w-5 h-5" />
              <span className="absolute top-1.5 right-1.5 bg-blue-600 text-[8px] px-1 rounded-full ring-2 ring-[#020617]">3</span>
            </button>

            <div className="hidden sm:flex items-center gap-3 pl-4 border-l border-slate-800">
              <div className="text-right">
                <p className="text-[9px] font-black text-white leading-none uppercase">J. Doe</p>
                <p className="text-[8px] font-bold text-slate-500 uppercase mt-1">Risk Admin</p>
              </div>
              <div className="w-9 h-9 rounded-lg bg-slate-800 border border-slate-700 flex items-center justify-center font-black text-[10px] text-blue-400">JD</div>
            </div>

            <button 
              className="lg:hidden p-2 bg-slate-900 border border-slate-800 rounded-lg text-slate-400 hover:text-white"
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            >
              {mobileMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
            </button>
          </div>
        </div>
      </div>

      {/* MOBILE MENU OVERLAY */}
      <div className={`lg:hidden fixed inset-x-0 bg-slate-950 border-b border-slate-800 transition-all duration-300 ease-in-out ${mobileMenuOpen ? 'top-20 opacity-100' : '-top-full opacity-0 pointer-events-none'}`}>
        <div className="p-6 space-y-8 max-h-[80vh] overflow-y-auto terminal-scroll">
          <div className="grid grid-cols-2 gap-4">
            <Link onClick={() => setMobileMenuOpen(false)} href="/dashboard" className="p-4 bg-slate-900 border border-slate-800 rounded-xl text-center text-[9px] font-black tracking-widest uppercase">Dashboard</Link>
            <Link onClick={() => setMobileMenuOpen(false)} href="/dashboard/limits" className="p-4 bg-red-500/10 border border-red-500/20 rounded-xl text-center text-[9px] font-black tracking-widest text-red-500 uppercase">Limits</Link>
          </div>
          
          <div>
            <p className="text-[10px] font-black text-slate-600 mb-4 tracking-[0.2em] uppercase">Market Segments</p>
            <div className="grid grid-cols-1 gap-2">
              {SEGMENTS.map(seg => (
                <div key={seg.name} className="bg-slate-900/50 border border-slate-800 rounded-xl overflow-hidden">
                  {seg.hasIntraday ? (
                    <div className="p-4 flex items-center justify-between">
                      <span className="text-xs font-bold uppercase">{seg.name}</span>
                      <div className="flex gap-2">
                        <Link onClick={() => setMobileMenuOpen(false)} href={`/dashboard/${seg.path}/daily`} className="px-3 py-1.5 bg-slate-800 rounded text-[9px] font-black uppercase">Daily</Link>
                        <Link onClick={() => setMobileMenuOpen(false)} href={`/dashboard/${seg.path}/intraday`} className="px-3 py-1.5 bg-emerald-600 rounded text-[9px] font-black uppercase">Live</Link>
                      </div>
                    </div>
                  ) : (
                    <Link onClick={() => setMobileMenuOpen(false)} href={`/dashboard/${seg.path}`} className="p-4 flex items-center justify-between hover:bg-slate-800 transition-colors">
                      <span className="text-xs font-bold uppercase">{seg.name}</span>
                      <ArrowRight className="w-4 h-4 text-slate-600" />
                    </Link>
                  )}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </nav>
  );
}