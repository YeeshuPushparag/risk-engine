import React from 'react'

export default function Footer() {
  return (
    <>
    {/* 1. THE GHOST SPACER 
          This is NOT fixed. It sits at the bottom of your page content 
          and creates a "hole" so your content doesn't get hidden.
      */}
      <div className="h-16 w-full bg-[#020617]" aria-hidden="true" />

      {/* 2. THE ACTUAL FIXED FOOTER 
      */}
    {/* --- STATUS FOOTER --- */}
      <footer className="fixed bottom-0 left-0 right-0 bg-[#020617] backdrop-blur-md border-t border-slate-900 py-3 px-8 flex justify-between items-center z-50">
        <div className="text-[8px] font-black text-slate-600 uppercase tracking-[0.4em]">
          Portfolio Risk Engine // Institutional Asset Oversight
        </div>
        <div className="flex items-center gap-4">
          <div className="flex items-center gap-2">
            <div className="w-1.5 h-1.5 rounded-full bg-emerald-500 shadow-[0_0_8px_#10b981]" />
            <span className="text-[9px] font-black text-slate-500 uppercase">Gateway Online</span>
          </div>
        </div>
      </footer>
    </>
  )
}
