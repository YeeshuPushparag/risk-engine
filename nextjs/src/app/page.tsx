import React from 'react';
import Link from 'next/link';
import { 
  Database, 
  Cpu, 
  Zap, 
  Layers, 
  Activity, 
  Globe,
  LayoutDashboard,
  ArrowRight,
  ShieldCheck,
  Github,
  ShieldAlert,
  Server
} from 'lucide-react';

export default function ProjectPage() {
  return (
    <div className="min-h-screen bg-[#020617] text-slate-300 font-mono selection:bg-blue-500/30">
      
      {/* HEADER / HERO */}
      <header className="max-w-6xl mx-auto px-6 pt-20 pb-16 border-b border-slate-800">
        <h1 className="text-4xl md:text-6xl font-black text-white uppercase italic tracking-tighter leading-none mb-8">
          Cross Asset <br />
          <span className="text-blue-600 font-black">Risk Infrastructure</span>
        </h1>
        
        <p className="max-w-3xl text-lg text-slate-400 font-medium leading-relaxed italic border-l-4 border-blue-600 pl-6 mb-10">
          A technical implementation of a multi-asset risk engine, designed to handle 
          high-concurrency data streams and batch-processed historical analytics. 
          Built to simulate the architectural requirements of institutional portfolio monitoring.
        </p>

       {/* ACTION BUTTONS */}
<div className="flex flex-col md:flex-row gap-3 w-fit">
  <Link 
    href="https://github.com/YeeshuPushparag/risk-engine" 
    target="_blank"
    className="flex items-center justify-center gap-3 bg-white text-black px-6 py-3 text-[10px] font-black uppercase tracking-widest hover:bg-blue-500 hover:text-white transition-all shadow-lg shadow-blue-500/10 min-w-[200px]"
  >
    <Github className="w-4 h-4" /> GitHub Repository
  </Link>
  <Link 
    href="/dashboard" 
    className="flex items-center justify-center gap-3 bg-slate-900 border border-slate-800 text-white px-6 py-3 text-[10px] font-black uppercase tracking-widest hover:border-blue-500 transition-all min-w-[200px]"
  >
    <LayoutDashboard className="w-4 h-4 text-blue-500" /> View Dashboard
  </Link>
</div>
      </header>

      <main className="max-w-6xl mx-auto px-6 py-20">
        
        {/* KPI GRID */}
        <section className="grid grid-cols-2 md:grid-cols-4 gap-px bg-slate-800 border border-slate-800 mb-24">
          {[
            { label: "Asset Universe", val: "3,217", sub: "Equity Tickers" },
            { label: "Entity Coverage", val: "20", sub: "Institutional Managers" },
            { label: "Data Segments", val: "07", sub: "Cross Asset Classes" },
            { label: "Model Stack", val: "20", sub: "XGBoost & Statistical" },
          ].map((stat) => (
            <div key={stat.label} className="bg-[#020617] p-8">
              <p className="text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2">{stat.label}</p>
              <p className="text-4xl font-black text-white italic">{stat.val}</p>
              <p className="text-[9px] font-black text-blue-500 uppercase mt-1">{stat.sub}</p>
            </div>
          ))}
        </section>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-16">
          <div className="lg:col-span-2 space-y-24">
            
            {/* INGESTION */}
            <section>
              <h2 className="text-xs font-black text-blue-500 uppercase tracking-[0.3em] mb-8 flex items-center gap-3">
                <Database className="w-4 h-4" /> 01. Ingestion Architecture
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                {[
                  { title: "yFinance Gateway", desc: "Equity & FX daily and intraday stream", tag: "Market Data" },
                  { title: "FRED Connector", desc: "Macro factors & interest rate data", tag: "Exogenous" },
                 { title: "13F Flat-File Ingest", desc: "Batch processed institutional CSV data", tag: "Holdings" },
                  { title: "Training Store", desc: "1.5Y historical market context", tag: "History" }
                ].map((item) => (
                  <div key={item.title} className="p-6 bg-slate-900/40 border border-slate-800 group hover:border-blue-500/50 transition-all">
                    <p className="text-[9px] font-black text-slate-600 uppercase mb-2 tracking-widest">{item.tag}</p>
                    <h3 className="text-white font-black italic uppercase tracking-tighter mb-1">{item.title}</h3>
                    <p className="text-xs text-slate-500 font-bold">{item.desc}</p>
                  </div>
                ))}
              </div>
            </section>

            {/* PIPELINES */}
            <section className="space-y-8">
              <h2 className="text-xs font-black text-blue-500 uppercase tracking-[0.3em] mb-8 flex items-center gap-3">
                <Layers className="w-4 h-4" /> 02. Engineering Workflows
              </h2>
              <div className="space-y-4 font-mono">
                <div className="flex items-start gap-4 p-6 bg-slate-900/20 border-l-2 border-slate-800">
                  <div className="text-blue-500 font-black pt-1 px-2 uppercase text-[10px]">Batch</div>
                  <div className="text-xs leading-loose text-slate-400">
                    EOD processing via <span className="text-white">Apache Airflow</span>. Feature engineering (Returns, Volatility, Momentum) persists in <span className="text-white">Snowflake</span>.
                  </div>
                </div>
                <div className="flex items-start gap-4 p-6 bg-slate-900/20 border-l-2 border-blue-600">
                  <div className="text-blue-500 font-black pt-1 px-2 uppercase text-[10px]">Stream</div>
                  <div className="text-xs leading-loose text-slate-400">
                    Minute-level ingestion via <span className="text-white">Kafka + Spark</span>. Real-time risk states are cached in <span className="text-white">Redis</span> for low-latency serving.
                  </div>
                </div>
              </div>
            </section>

            {/* SCENARIO ANALYSIS */}
            <section className="p-8 bg-blue-600/5 border border-blue-500/20 relative overflow-hidden">
              <ShieldAlert className="absolute -right-4 -bottom-4 w-24 h-24 text-blue-500/10" />
              <h2 className="text-xs font-black text-blue-500 uppercase tracking-[0.3em] mb-6 flex items-center gap-3">
                <Activity className="w-4 h-4" /> 03. Scenario Analysis
              </h2>
              <p className="text-xs text-slate-400 mb-6 font-bold uppercase italic">Simulated Historical Shock Calibration:</p>
              <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-[10px] font-black text-slate-500 uppercase">
                <div className="flex items-center gap-2"><ArrowRight className="w-3 h-3 text-blue-500" /> 2008 Financial Contagion</div>
                <div className="flex items-center gap-2"><ArrowRight className="w-3 h-3 text-blue-500" /> COVID-19 Liquidity Shock</div>
                <div className="flex items-center gap-2"><ArrowRight className="w-3 h-3 text-blue-500" /> Interest Rate Hike Volatility</div>
                <div className="flex items-center gap-2"><ArrowRight className="w-3 h-3 text-blue-500" /> Custom Asset-Level Stress</div>
              </div>
            </section>
          </div>

          {/* RIGHT COLUMN - TECH STACK RESTORED */}
          <aside className="space-y-12">
            <div className="p-8 bg-slate-950 border border-slate-800 rounded-lg">
  <h3 className="text-[10px] font-black text-white uppercase tracking-[0.2em] mb-6 border-b border-slate-800 pb-4 flex items-center gap-2">
    <Server className="w-3 h-3 text-blue-500" /> Stack Composition
  </h3>
  <div className="space-y-5 text-[10px] font-bold text-slate-500 uppercase italic">
   <p><span className="text-blue-500">Streaming:</span> Apache Kafka & Redis</p>
  <p><span className="text-blue-500">Processing:</span> Spark & Airflow DAGs</p>
  <p><span className="text-blue-500">Backend:</span> Django & Channels (WebSockets)</p>
  <p><span className="text-blue-500">Frontend:</span> Next.js & Tailwind CSS</p>
  <p><span className="text-blue-500">Database:</span> Snowflake & PostgreSQL</p>
  <p><span className="text-blue-500">Monitoring:</span> Prometheus & Grafana</p>
  <p><span className="text-blue-500">CI/CD:</span> Jenkins Automation & Argo CD</p>
  <p><span className="text-blue-500">Infrastructure:</span> Terraform & AWS EKS</p>
  </div>
</div>
            <div className="p-8 bg-slate-950 border border-slate-800 rounded-lg">
              <h3 className="text-[10px] font-black text-white uppercase tracking-[0.2em] mb-6 border-b border-slate-800 pb-4 flex items-center gap-2">
                <Globe className="w-3 h-3 text-emerald-500" /> Coverage Scope
              </h3>
              <div className="flex flex-wrap gap-2">
                {['Equities', 'FX', 'Commodities', 'Bonds', 'Derivatives', 'Collateral', 'Loans'].map(asset => (
                  <span key={asset} className="px-3 py-1 bg-slate-900 border border-slate-800 text-[9px] font-black text-slate-400 uppercase tracking-widest">
                    {asset}
                  </span>
                ))}
              </div>
            </div>

          {/* ================= MACHINE LEARNING ================= */}
<div className="p-8 bg-[#020617] border-l-2 border-purple-600 mb-10">
  <h3 className="text-[10px] font-black text-white uppercase tracking-[0.2em] mb-6 flex items-center gap-2">
    <Cpu className="w-3 h-3 text-purple-500" /> Machine Learning & Risk Models
  </h3>
  
  <div className="space-y-6 text-[11px] font-bold text-slate-500 uppercase italic">
    <p className="flex justify-between items-center border-b border-slate-900 pb-2">
      <span className="text-purple-500">Training:</span> 
      <span className="text-white">~1.5Y Supervised Dataset</span>
    </p>
    
    <p className="flex justify-between items-center border-b border-slate-900 pb-2">
      <span className="text-purple-500">Core Logic:</span> 
      <span className="text-white">Equity Risk & Factor Signals</span>
    </p>
    
    <p className="flex justify-between items-center border-b border-slate-800 pb-2">
      <span className="text-purple-500">Versioning:</span> 
      <span className="text-white">Amazon S3 Artifact Store</span>
    </p>
    
   <p className="flex justify-between items-center">
  <span className="text-purple-500">Execution:</span> 
  <span className="text-white">Airflow Orchestrated Batch</span>
</p>
  </div>
</div>
          </aside>
        </div>

        {/* OBJECTIVE */}
        <footer className="mt-32 pt-16 border-t border-slate-800">
           <div className="flex flex-col md:flex-row gap-12 items-start">
             <div className="bg-emerald-600/10 p-4 rounded border border-emerald-500/20 shrink-0">
               <ShieldCheck className="w-8 h-8 text-emerald-500" />
             </div>
             <div className="space-y-4">
                <h2 className="text-xs font-black text-white uppercase tracking-[0.3em]">Project Focus and Rationale</h2>
                <p className="text-sm font-medium text-slate-500 max-w-4xl leading-relaxed italic">
                  This project serves as a practical exploration of modern data engineering 
                  challenges within the financial domain. The focus remains on establishing 
                  <span className="text-slate-300"> consistent and verifiable data flows</span> across a multi-asset universe 
                  using a tiered storage architecture—integrating Redis for real-time state and Snowflake 
                  for deep analytics to address institutional scalability requirements.
                </p>
             </div>
           </div>
        </footer>
      </main>
    </div>
  );
}