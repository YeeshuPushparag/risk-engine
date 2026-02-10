"use client";

import React, { useState, useRef } from "react";
import { RotateCcw, Plus, Minus } from "lucide-react";

const SCENARIO_PRESETS: Record<string, any> = {
  baseline: {
    label: "Baseline (Market Mid)",
    group: "Core",
    ret: 0,
    vol: 1.0,
    var: 1.0,
  },

  risk_on: {
    label: "Risk On",
    group: "Market Sentiment",
    ret: 5,
    vol: 0.85,
    var: 0.80,
  },

  risk_off: {
    label: "Risk Off",
    group: "Market Sentiment",
    ret: -8,
    vol: 1.30,
    var: 1.40,
  },

  equity_rally: {
    label: "Equity Rally (+10%)",
    group: "Shocks",
    ret: 10,
    vol: 0.75,
    var: 0.70,
  },

  equity_crash: {
    label: "Equity Crash (-20%)",
    group: "Shocks",
    ret: -20,
    vol: 1.80,
    var: 1.60,
  },

  volatility_spike: {
    label: "Volatility Spike",
    group: "Shocks",
    ret: -5,
    vol: 2.00,
    var: 1.80,
  },

  rates_up: {
    label: "Rates Up",
    group: "Macro",
    ret: -6,
    vol: 1.25,
    var: 1.30,
  },

  rates_down: {
    label: "Rates Down",
    group: "Macro",
    ret: 4,
    vol: 0.90,
    var: 0.85,
  },

  inflation_shock: {
    label: "Inflation Shock",
    group: "Macro",
    ret: -7,
    vol: 1.35,
    var: 1.40,
  },

  stagflation: {
    label: "Stagflation",
    group: "Macro",
    ret: -10,
    vol: 1.40,
    var: 1.30,
  },

  gfc_2008: {
    label: "GFC 2008",
    group: "Historical",
    ret: -30,
    vol: 2.20,
    var: 2.00,
  },

  covid_2020: {
    label: "COVID-19 Crash (2020)",
    group: "Historical",
    ret: -25,
    vol: 2.00,
    var: 1.80,
  },
};


export default function StressTestingControls({ onChange }: { onChange: (params: any) => void }) {
  const [scenario, setScenario] = useState("baseline");
  const [retShift, setRetShift] = useState(0);
  const [volMult, setVolMult] = useState(1.0);
  const [varMult, setVarMult] = useState(1.0);

  const handleScenarioChange = (key: string) => {
    setScenario(key);
    const preset = SCENARIO_PRESETS[key];
    if (preset) {
      setRetShift(preset.ret);
      setVolMult(preset.vol);
      setVarMult(preset.var);
    }
  };

  const handleApply = () => {
    onChange({
      scenario,
      ret_shift: retShift,
      vol_multiplier: volMult,
      var_multiplier: varMult,
    });
  };

  const handleResetUI = () => {
    // Only resets local UI state. Does NOT call onChange/API.
    setScenario("baseline");
    setRetShift(0);
    setVolMult(1.0);
    setVarMult(1.0);
  };

  const grouped = Object.entries(SCENARIO_PRESETS).reduce((acc: any, [key, cfg]: any) => {
    acc[cfg.group] = acc[cfg.group] || [];
    acc[cfg.group].push({ key, ...cfg });
    return acc;
  }, {});

  return (
    <div className="p-4 sm:p-6 bg-[#0a0f1d]/60 border border-slate-800 rounded-2xl shadow-2xl backdrop-blur-sm">
      <div className="flex flex-col lg:flex-row lg:items-end gap-6">
        <div className="flex-grow min-w-0 w-full lg:w-auto">
          <label className="text-[10px] font-black text-slate-500 uppercase mb-2 tracking-widest block">Institutional Scenarios</label>
          <select
            className="w-full bg-slate-950 border border-slate-800 rounded-xl px-4 py-2.5 text-sm font-mono text-white outline-none cursor-pointer focus:border-blue-500/50 transition-colors"
            value={scenario}
            onChange={(e) => handleScenarioChange(e.target.value)}
          >
            {Object.entries(grouped).map(([group, items]: any) => (
              <optgroup key={group} label={group}>
                {items.map((s: any) => <option key={s.key} value={s.key}>{s.label}</option>)}
              </optgroup>
            ))}
          </select>
        </div>

        <div className="grid grid-cols-3 sm:flex gap-3 sm:gap-4 shrink-0">
          <HoldStepper label="Shift %" value={retShift} setValue={setRetShift} step={1} min={-30} max={30} decimals={0} />
          <HoldStepper label="Vol x" value={volMult} setValue={setVolMult} step={0.1} min={0.3} max={3.0} decimals={1} />
          <HoldStepper label="VaR x" value={varMult} setValue={setVarMult} step={0.1} min={0.5} max={5.0} decimals={1} />
        </div>

        <div className="flex items-center gap-2 w-full lg:w-auto shrink-0">
          <button onClick={handleApply} className="flex-grow lg:flex-none h-11 px-8 bg-blue-600 hover:bg-blue-500 text-white rounded-xl text-[10px] font-black uppercase tracking-widest transition-all shadow-lg active:scale-95">
            Execute
          </button>
          <button onClick={handleResetUI} title="Reset UI" className="h-11 px-4 bg-slate-800 hover:bg-slate-700 text-slate-400 rounded-xl border border-slate-700 active:scale-90 transition-all">
            <RotateCcw className="w-4 h-4" />
          </button>
        </div>
      </div>
    </div>
  );
}

function HoldStepper({ label, value, setValue, step, min, max, decimals }: any) {
  const timerRef = useRef<any>(null);
  const intervalRef = useRef<any>(null);
  
  const update = (dir: number) => {
    setValue((prev: number) => {
      const next = parseFloat((prev + dir * step).toFixed(decimals));
      return next >= min && next <= max ? next : prev;
    });
  };
  
  const stop = () => { 
    clearTimeout(timerRef.current); 
    clearInterval(intervalRef.current); 
  };
  
  const start = (dir: number) => { 
    stop(); 
    update(dir); 
    timerRef.current = setTimeout(() => { 
      intervalRef.current = setInterval(() => update(dir), 70); 
    }, 300); 
  };

  return (
    <div className="flex flex-col min-w-0 flex-1 sm:flex-none">
      <label className="text-[8px] sm:text-[9px] font-black text-slate-600 uppercase mb-1.5 sm:mb-2 tracking-widest truncate">
        {label}
      </label>
      <div className="flex items-center bg-slate-950 border border-slate-800 rounded-lg sm:rounded-xl p-0.5 sm:p-1">
        {/* Responsive padding and icon size */}
        <button 
          onMouseDown={() => start(-1)} 
          onMouseUp={stop} 
          onMouseLeave={stop} 
          className="p-1 sm:p-1.5 hover:bg-slate-800 rounded-md sm:rounded-lg text-slate-400"
        >
          <Minus className="w-3 h-3 sm:w-3.5 sm:h-3.5" />
        </button>
        
        {/* Responsive text size */}
        <div className="flex-1 text-center font-mono text-xs sm:text-sm font-bold text-white px-0.5">
          {value.toFixed(decimals)}
        </div>
        
        <button 
          onMouseDown={() => start(1)} 
          onMouseUp={stop} 
          onMouseLeave={stop} 
          className="p-1 sm:p-1.5 hover:bg-slate-800 rounded-md sm:rounded-lg text-slate-400"
        >
          <Plus className="w-3 h-3 sm:w-3.5 sm:h-3.5" />
        </button>
      </div>
    </div>
  );
}