import React, { useEffect, useState, useContext, useMemo, useCallback, useRef } from "react";
import axios from "axios";
import { AuthContext } from "../context/AuthContext";
import { MapContainer, TileLayer, Marker, Tooltip, useMap } from "react-leaflet";
import {
  Phone, Mail, MapPin, X, RefreshCw, Filter, Eye, Zap, Database,
  Globe, Search, TrendingUp, Clock, Loader, Layers, Target,
  Users, MessageSquare, ThumbsUp, Share2, ExternalLink,
  CheckCircle, AlertTriangle, AlertCircle, Info, ChevronDown, ChevronUp,
  Menu, LayoutGrid, List,
} from "lucide-react";

const API = "http://127.0.0.1:8000/api";
const PAGE_SIZE = 50; // matches backend default; user sees all 50 per page

const SERVICE_TAXONOMY = {
  "Cleaning": {
    services: [
      { label: "Carpet Cleaning", phrases: ["carpet clean","carpet steam","rug clean","carpet shampoo"], words: ["carpet","upholstery"] },
      { label: "House / Home Cleaning", phrases: ["house clean","home clean","housekeeping","deep clean","maid service","residential clean","apartment clean","cleaning service"], words: ["maid","housekeeper"] },
      { label: "Window Cleaning", phrases: ["window clean","window wash","glass clean","window washing"], words: [] },
      { label: "Pressure / Power Washing", phrases: ["pressure wash","power wash","soft wash","pressure clean","driveway wash"], words: [] },
      { label: "Janitorial / Commercial", phrases: ["janitorial","commercial clean","office clean","building clean","floor buffer","strip and wax"], words: ["janitor","custodial"] },
      { label: "Move-Out Cleaning", phrases: ["move out clean","move-out clean","end of tenancy","vacancy clean","rental clean"], words: [] },
    ],
  },
  "Maintenance": {
    services: [
      { label: "Plumbing", phrases: ["plumbing service","pipe repair","drain clean","water heater","leak repair","toilet repair","faucet repair"], words: ["plumber","plumbing"] },
      { label: "Electrical", phrases: ["electrical service","wiring install","panel upgrade","circuit breaker","outlet install","electrical work"], words: ["electrician","electrical"] },
      { label: "HVAC", phrases: ["hvac service","ac repair","air conditioning","furnace repair","heat pump","duct clean"], words: ["hvac","heating","cooling","furnace"] },
      { label: "Roofing", phrases: ["roof repair","roof install","shingle repair","gutter clean","gutter repair","roof leak"], words: ["roofer","roofing"] },
      { label: "Painting", phrases: ["interior paint","exterior paint","house paint","drywall repair","wall paint","deck stain"], words: ["painter","painting"] },
      { label: "Lawn & Landscaping", phrases: ["lawn care","lawn mow","landscape service","tree trim","hedge trim","yard clean","mow lawn"], words: ["landscaping","landscaper","lawn"] },
      { label: "Handyman", phrases: ["handyman service","general repair","home repair","odd jobs","furniture assembl","tv mount"], words: ["handyman"] },
      { label: "Pest Control", phrases: ["pest control","bed bug","termite control","rodent control","mosquito control","bug control"], words: ["exterminator","pest"] },
    ],
  },
  "Waste Management": {
    services: [
      { label: "Junk Removal", phrases: ["junk removal","junk hauling","trash removal","debris removal","haul away","estate cleanout","garage cleanout"], words: ["junk","hauling"] },
      { label: "Dumpster Rental", phrases: ["dumpster rental","roll off","bin rental","container rental","dumpster service"], words: ["dumpster"] },
      { label: "Appliance Removal", phrases: ["appliance removal","old appliance","refrigerator removal","washer removal","appliance disposal"], words: [] },
      { label: "Yard Waste Removal", phrases: ["yard waste","green waste","brush removal","leaf hauling","lawn debris"], words: [] },
      { label: "Construction Debris", phrases: ["construction debris","construction waste","demo debris","demolition debris","renovation waste","concrete removal"], words: [] },
    ],
  },
};

const ALL_SERVICES_FLAT = Object.entries(SERVICE_TAXONOMY).flatMap(([group, g]) =>
  g.services.map(s => ({ ...s, group }))
);
const CAT_KEY_TO_TAXONOMY = {
  cleaning: "Cleaning",
  maintenance: "Maintenance",
  waste_management: "Waste Management",
};

function matchesService(lead, serviceLabel) {
  if (!serviceLabel) return true;
  const entry = ALL_SERVICES_FLAT.find(s => s.label === serviceLabel);
  if (!entry) return false;
  const hay = `${lead.title || ""} ${lead.post || ""}`.toLowerCase();
  if (entry.phrases.some(p => hay.includes(p.toLowerCase()))) return true;
  if (entry.words.some(w => new RegExp(`\\b${w.toLowerCase()}\\b`).test(hay))) return true;
  return false;
}

const LOG_CFG = {
  info:    { icon: Info,          text: "text-blue-400",    bg: "bg-blue-500/5" },
  success: { icon: CheckCircle,   text: "text-emerald-400", bg: "bg-emerald-500/5" },
  warning: { icon: AlertTriangle, text: "text-amber-400",   bg: "bg-amber-500/5" },
  error:   { icon: AlertCircle,   text: "text-red-400",     bg: "bg-red-500/5" },
};

function stageSource(stage = "") {
  const s = stage.toLowerCase();
  if (s.includes("facebook")) return "facebook";
  if (s.includes("craigslist")) return "craigslist";
  return "system";
}

// ── UI atoms ──────────────────────────────────────────────────────────────────

const Badge = ({ children, color = "slate" }) => {
  const colors = {
    slate:  "bg-slate-100 text-slate-700 border-slate-200",
    blue:   "bg-blue-50 text-blue-700 border-blue-200",
    green:  "bg-emerald-50 text-emerald-700 border-emerald-200",
    yellow: "bg-amber-50 text-amber-700 border-amber-200",
    red:    "bg-red-50 text-red-700 border-red-200",
  };
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold border ${colors[color]}`}>
      {children}
    </span>
  );
};

const ScoreDot = ({ score }) => {
  const color = score >= 70 ? "#10b981" : score >= 40 ? "#f59e0b" : "#94a3b8";
  return (
    <div className="relative w-8 h-8 flex-shrink-0">
      <svg viewBox="0 0 32 32" className="w-8 h-8 -rotate-90">
        <circle cx="16" cy="16" r="12" fill="none" stroke="#e2e8f0" strokeWidth="3" />
        <circle cx="16" cy="16" r="12" fill="none" stroke={color} strokeWidth="3"
          strokeDasharray={`${(score / 100) * 75.4} 75.4`} strokeLinecap="round" />
      </svg>
      <span className="absolute inset-0 flex items-center justify-center text-[9px] font-bold text-slate-700">{score}</span>
    </div>
  );
};

const StatusSelect = ({ value, onChange }) => {
  const cfg = {
    NEW:       { color: "text-blue-700 bg-blue-50 border-blue-200",         label: "New" },
    CONTACTED: { color: "text-amber-700 bg-amber-50 border-amber-200",       label: "Contacted" },
    QUALIFIED: { color: "text-emerald-700 bg-emerald-50 border-emerald-200", label: "Qualified" },
    WON:       { color: "text-violet-700 bg-violet-50 border-violet-200",    label: "Won" },
    LOST:      { color: "text-red-700 bg-red-50 border-red-200",             label: "Lost" },
  };
  const c = cfg[value] || cfg.NEW;
  return (
    <select value={value} onChange={e => onChange(e.target.value)} onClick={e => e.stopPropagation()}
      className={`text-[10px] font-bold border rounded-full px-2.5 py-1 focus:outline-none cursor-pointer ${c.color}`}>
      {Object.entries(cfg).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
    </select>
  );
};

const SourceTag = ({ source }) => (
  source === "FACEBOOK"
    ? <span className="inline-flex items-center gap-1 text-[9px] font-bold uppercase px-2 py-0.5 rounded border bg-indigo-500/10 text-indigo-400 border-indigo-500/20">FB</span>
    : <span className="inline-flex items-center gap-1 text-[9px] font-bold uppercase px-2 py-0.5 rounded border bg-orange-500/10 text-orange-400 border-orange-500/20">CL</span>
);

// ── Stat card ─────────────────────────────────────────────────────────────────

const StatCard = ({ label, value, icon: Icon, color, loading }) => (
  <div className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-3 relative overflow-hidden">
    <div className={`${color} mb-1.5`}><Icon className="w-3.5 h-3.5" /></div>
    <div className="text-lg font-bold tabular-nums">
      {loading ? <span className="inline-block w-8 h-4 bg-white/10 rounded animate-pulse" /> : value}
    </div>
    <div className="text-[10px] text-white/30 mt-0.5">{label}</div>
  </div>
);

// ── Map auto-fit ───────────────────────────────────────────────────────────────

function MapAutoFit({ leads }) {
  const map = useMap();
  const prevLen = useRef(0);
  useEffect(() => {
    if (!leads || leads.length === 0) return;
    if (leads.length === prevLen.current) return;
    prevLen.current = leads.length;
    const valid = leads.filter(l => l.latitude && l.longitude);
    if (valid.length === 0) return;
    if (valid.length === 1) { map.setView([parseFloat(valid[0].latitude), parseFloat(valid[0].longitude)], 10); return; }
    const lats = valid.map(l => parseFloat(l.latitude));
    const lngs = valid.map(l => parseFloat(l.longitude));
    map.fitBounds([[Math.min(...lats), Math.min(...lngs)], [Math.max(...lats), Math.max(...lngs)]], { padding: [40, 40] });
  }, [leads.length]);
  return null;
}

// ── Post-run activity panel ────────────────────────────────────────────────────

function ActivityPanel({ scrapeStatus, visible }) {
  const logRef = useRef(null);
  const [collapsed, setCollapsed] = useState(false);
  const log = scrapeStatus?.activity_log || [];
  const isRunning = scrapeStatus?.status === "RUNNING";
  useEffect(() => {
    if (logRef.current && !collapsed) logRef.current.scrollTop = logRef.current.scrollHeight;
  }, [log.length, collapsed]);
  if (!visible || isRunning) return null;
  return (
    <div className="bg-[#0d0f18] border border-white/[0.07] rounded-2xl overflow-hidden">
      <div className="px-4 py-3 border-b border-white/[0.05] flex items-center justify-between">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-white/20" />
          <span className="text-xs font-semibold text-white/35">Last run log</span>
          {log.length > 0 && <span className="text-[10px] text-white/20 font-mono">{log.length} events</span>}
        </div>
        <button onClick={() => setCollapsed(p => !p)} className="text-white/20 hover:text-white/50 transition-colors">
          {collapsed ? <ChevronDown className="w-4 h-4" /> : <ChevronUp className="w-4 h-4" />}
        </button>
      </div>
      {!collapsed && (
        <div ref={logRef} className="overflow-y-auto max-h-56 divide-y divide-white/[0.03]">
          {log.length === 0 ? (
            <div className="px-4 py-5 text-center text-white/20 text-xs">No events recorded</div>
          ) : (
            [...log].reverse().map((entry, i) => {
              const cfg = LOG_CFG[entry.level] || LOG_CFG.info;
              const Icon = cfg.icon;
              const src = stageSource(entry.stage);
              const ts = entry.ts ? new Date(entry.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";
              return (
                <div key={i} className="px-3 py-2.5 flex gap-2.5 items-start">
                  <Icon className={`w-3 h-3 mt-0.5 flex-shrink-0 ${cfg.text}`} />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      {src === "facebook" && <span className="text-[8px] font-bold uppercase px-1 py-px rounded bg-indigo-500/15 text-indigo-400 border border-indigo-500/20">FB</span>}
                      {src === "craigslist" && <span className="text-[8px] font-bold uppercase px-1 py-px rounded bg-orange-500/15 text-orange-400 border border-orange-500/20">CL</span>}
                      <span className={`text-[10px] font-semibold ${cfg.text}`}>{entry.stage}</span>
                    </div>
                    <p className="text-[10px] text-white/35 leading-relaxed mt-px">{entry.detail}</p>
                  </div>
                  <span className="text-[9px] text-white/15 font-mono flex-shrink-0">{ts}</span>
                </div>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}

// ── Live scrape dashboard ──────────────────────────────────────────────────────

function ScrapeLiveDashboard({ scrapeStatus, onStop, isAborting }) {
  const logRef = useRef(null);
  const log = scrapeStatus?.activity_log || [];
  const currentStage = scrapeStatus?.current_stage || "Initialising...";
  const stageDetail = scrapeStatus?.stage_detail || "";
  const saved = scrapeStatus?.leads_collected || 0;
  const skipped = scrapeStatus?.leads_skipped || 0;
  const sources = scrapeStatus?.sources || [];
  const hasCL = sources.includes("craigslist");
  const hasFB = sources.includes("facebook");
  const activeSrc = stageSource(currentStage);
  const clDone = log.some(e => e.stage?.toLowerCase().includes("craigslist --- complete") || e.stage?.toLowerCase().includes("craigslist --- skipped"));
  const fbDone = log.some(e => e.stage?.toLowerCase().includes("facebook --- complete") || e.stage?.toLowerCase().includes("facebook --- no groups") || e.stage?.toLowerCase().includes("facebook --- skipped"));
  const clStarted = log.some(e => e.stage?.toLowerCase().includes("craigslist"));
  const fbStarted = log.some(e => e.stage?.toLowerCase().includes("facebook"));
  const clBatchCount = log.filter(e => e.stage?.toLowerCase().includes("craigslist --- batch")).length;
  const fbChunkCount = log.filter(e => e.stage?.toLowerCase().includes("facebook --- scraping chunk") || e.stage?.toLowerCase().includes("facebook --- chunk")).length;
  const fbLogs = log.filter(e => e.stage?.toLowerCase().startsWith("facebook"));
  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [log.length]);
  const [elapsed, setElapsed] = useState(0);
  const startTs = useRef(scrapeStatus?.started_at ? new Date(scrapeStatus.started_at) : new Date());
  useEffect(() => {
    const iv = setInterval(() => setElapsed(Math.floor((Date.now() - startTs.current) / 1000)), 1000);
    return () => clearInterval(iv);
  }, [scrapeStatus?.started_at]);
  const fmtElapsed = `${String(Math.floor(elapsed / 60)).padStart(2, "0")}:${String(elapsed % 60).padStart(2, "0")}`;
  return (
    <div className="space-y-3">
      <div className={`relative overflow-hidden rounded-2xl border transition-all duration-500 ${
        isAborting ? "border-red-500/30 bg-gradient-to-br from-red-950/30 to-[#0f1117]" :
        activeSrc === "craigslist" ? "border-orange-500/30 bg-gradient-to-br from-orange-950/30 to-[#0f1117]" :
        activeSrc === "facebook" ? "border-indigo-500/30 bg-gradient-to-br from-indigo-950/30 to-[#0f1117]" :
        "border-blue-500/20 bg-gradient-to-br from-slate-900/60 to-[#0f1117]"
      }`}>
        <div className={`h-[2px] w-full transition-all duration-500 ${
          isAborting ? "bg-red-500/50" :
          activeSrc === "craigslist" ? "bg-gradient-to-r from-transparent via-orange-400/80 to-transparent animate-pulse" :
          activeSrc === "facebook"   ? "bg-gradient-to-r from-transparent via-indigo-400/80 to-transparent animate-pulse" :
          "bg-gradient-to-r from-transparent via-blue-400/60 to-transparent animate-pulse"
        }`} />
        <div className="px-4 py-4 lg:px-6 lg:py-5">
          <div className="flex items-start justify-between gap-3 mb-4">
            <div className="flex items-start gap-3 min-w-0">
              <div className="flex-shrink-0">
                {isAborting ? (
                  <div className="w-9 h-9 rounded-full bg-red-500/15 border border-red-500/30 flex items-center justify-center"><X className="w-4 h-4 text-red-400" /></div>
                ) : activeSrc === "craigslist" ? (
                  <div className="relative w-9 h-9">
                    <div className="absolute inset-0 rounded-full bg-orange-500/20 animate-ping" />
                    <div className="relative w-9 h-9 rounded-full bg-orange-500/20 border border-orange-500/40 flex items-center justify-center"><Globe className="w-4 h-4 text-orange-400" /></div>
                  </div>
                ) : activeSrc === "facebook" ? (
                  <div className="relative w-9 h-9">
                    <div className="absolute inset-0 rounded-full bg-indigo-500/20 animate-ping" />
                    <div className="relative w-9 h-9 rounded-full bg-indigo-500/20 border border-indigo-500/40 flex items-center justify-center"><Users className="w-4 h-4 text-indigo-400" /></div>
                  </div>
                ) : (
                  <div className="w-9 h-9 rounded-full bg-blue-500/15 border border-blue-500/25 flex items-center justify-center"><Loader className="w-4 h-4 text-blue-400 animate-spin" /></div>
                )}
              </div>
              <div className="min-w-0">
                <div className={`text-[10px] font-bold uppercase tracking-widest mb-0.5 ${
                  isAborting ? "text-red-400" : activeSrc === "craigslist" ? "text-orange-400" : activeSrc === "facebook" ? "text-indigo-400" : "text-blue-400"
                }`}>
                  {isAborting ? "⛔ Stopping run" : activeSrc === "craigslist" ? "🔶 Scraping Craigslist" : activeSrc === "facebook" ? "🔷 Scraping Facebook Groups" : "⚙️ Initialising pipeline"}
                </div>
                <div className="text-sm font-semibold text-white/85 leading-snug">{currentStage}</div>
                {stageDetail && <p className="text-[11px] text-white/45 leading-relaxed mt-1">{stageDetail}</p>}
              </div>
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              <div className="text-right hidden sm:block">
                <div className="text-[9px] text-white/25 uppercase tracking-widest">Elapsed</div>
                <div className="font-mono text-sm font-bold text-white/50 tabular-nums">{fmtElapsed}</div>
              </div>
              <button onClick={onStop} disabled={isAborting}
                className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-red-500/10 border border-red-500/20 text-red-400 text-xs font-semibold hover:bg-red-500/20 transition-all disabled:opacity-40">
                <X className="w-3 h-3" /><span className="hidden sm:inline">{isAborting ? "Stopping..." : "Stop"}</span>
              </button>
            </div>
          </div>
          {(hasCL || hasFB) && (
            <div className="flex gap-2 mb-4 flex-wrap">
              {hasCL && (
                <div className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-xs font-semibold transition-all duration-300 ${
                  clDone ? "bg-emerald-500/10 border-emerald-500/25 text-emerald-400" :
                  activeSrc === "craigslist" ? "bg-orange-500/15 border-orange-500/40 text-orange-300" :
                  clStarted ? "bg-orange-500/8 border-orange-500/20 text-orange-400/60" :
                  "bg-white/[0.02] border-white/[0.05] text-white/20"
                }`}>
                  {clDone ? <CheckCircle className="w-3.5 h-3.5" /> : activeSrc === "craigslist" ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Globe className="w-3.5 h-3.5" />}
                  Craigslist
                  {clDone && <span className="text-[10px] text-emerald-400/60 font-normal">✓ done</span>}
                  {!clDone && !clStarted && <span className="text-[10px] text-white/20 font-normal">queued</span>}
                  {activeSrc === "craigslist" && !clDone && (
                    <span className="flex gap-0.5">{[0,150,300].map(d => <span key={d} className="w-1 h-1 rounded-full bg-orange-400 animate-bounce" style={{ animationDelay: `${d}ms` }} />)}</span>
                  )}
                  {clBatchCount > 0 && <span className="text-[10px] font-mono text-white/30">{clBatchCount} batches</span>}
                </div>
              )}
              {hasFB && (
                <div className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-xs font-semibold transition-all duration-300 ${
                  fbDone ? "bg-emerald-500/10 border-emerald-500/25 text-emerald-400" :
                  activeSrc === "facebook" ? "bg-indigo-500/15 border-indigo-500/40 text-indigo-300" :
                  fbStarted ? "bg-indigo-500/8 border-indigo-500/20 text-indigo-400/60" :
                  "bg-white/[0.02] border-white/[0.05] text-white/20"
                }`}>
                  {fbDone ? <CheckCircle className="w-3.5 h-3.5" /> : activeSrc === "facebook" ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Users className="w-3.5 h-3.5" />}
                  Facebook Groups
                  {fbDone && <span className="text-[10px] text-emerald-400/60 font-normal">✓ done</span>}
                  {!fbDone && !fbStarted && <span className="text-[10px] text-white/20 font-normal">queued</span>}
                  {activeSrc === "facebook" && !fbDone && (
                    <span className="flex gap-0.5">{[0,150,300].map(d => <span key={d} className="w-1 h-1 rounded-full bg-indigo-400 animate-bounce" style={{ animationDelay: `${d}ms` }} />)}</span>
                  )}
                  {fbChunkCount > 0 && <span className="text-[10px] font-mono text-white/30">{fbChunkCount} chunks</span>}
                </div>
              )}
            </div>
          )}
          <div className="grid grid-cols-3 gap-2">
            <div className="bg-white/[0.03] border border-white/[0.05] rounded-xl px-3 py-2.5">
              <div className="text-[9px] text-white/25 uppercase tracking-widest mb-1">Leads saved</div>
              <div className="text-xl font-bold tabular-nums text-emerald-400">{saved}</div>
              {saved > 0 && <div className="text-[9px] text-emerald-400/40 mt-0.5">↑ live</div>}
            </div>
            <div className="bg-white/[0.03] border border-white/[0.05] rounded-xl px-3 py-2.5">
              <div className="text-[9px] text-white/25 uppercase tracking-widest mb-1">Duplicates</div>
              <div className="text-xl font-bold tabular-nums text-white/35">{skipped}</div>
            </div>
            <div className="bg-white/[0.03] border border-white/[0.05] rounded-xl px-3 py-2.5">
              <div className="text-[9px] text-white/25 uppercase tracking-widest mb-1">Events</div>
              <div className="text-xl font-bold tabular-nums text-white/35">{log.length}</div>
            </div>
          </div>
        </div>
      </div>
      <div className={`grid grid-cols-1 gap-3 ${hasFB ? "lg:grid-cols-2" : ""}`}>
        {hasFB && (
          <div className="bg-[#07090f] border border-white/[0.06] rounded-2xl overflow-hidden">
            <div className="px-4 py-3 border-b border-white/[0.05] flex items-center gap-2">
              <Users className="w-3.5 h-3.5 text-indigo-400/60" />
              <span className="text-xs font-semibold text-white/40">Facebook Groups Detail</span>
              {fbChunkCount > 0 && <span className="ml-auto text-[10px] font-mono text-white/20">{fbChunkCount} chunks processed</span>}
            </div>
            <div className="p-2 max-h-56 overflow-y-auto divide-y divide-white/[0.03]">
              {fbLogs.length === 0 ? (
                <div className="py-8 text-center text-white/15 text-xs">{fbStarted ? "Collecting group data..." : "Waiting for Facebook phase..."}</div>
              ) : (
                fbLogs.map((entry, i) => {
                  const cfg = LOG_CFG[entry.level] || LOG_CFG.info;
                  const Icon = cfg.icon;
                  const isNewest = i === fbLogs.length - 1;
                  return (
                    <div key={i} className={`flex items-start gap-2 px-3 py-2 ${isNewest ? cfg.bg : ""}`}>
                      <Icon className={`w-3 h-3 mt-0.5 flex-shrink-0 ${isNewest ? cfg.text : "text-white/20"}`} />
                      <div className="flex-1 min-w-0">
                        <p className={`text-[10px] font-semibold ${isNewest ? cfg.text : "text-white/35"}`}>{entry.stage}</p>
                        <p className={`text-[10px] leading-relaxed mt-px ${isNewest ? "text-white/55" : "text-white/22"}`}>{entry.detail}</p>
                      </div>
                      <span className="text-[9px] font-mono text-white/15 flex-shrink-0">
                        {entry.ts ? new Date(entry.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : ""}
                      </span>
                    </div>
                  );
                })
              )}
            </div>
          </div>
        )}
        <div className={`bg-[#07090f] border border-white/[0.06] rounded-2xl overflow-hidden ${!hasFB ? "lg:col-span-1" : ""}`}>
          <div className="px-4 py-3 border-b border-white/[0.05] flex items-center gap-2">
            <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
            <span className="text-xs font-semibold text-white/50">Live event stream</span>
            <span className="font-mono text-[10px] text-white/20 ml-auto">{log.length} events</span>
          </div>
          <div ref={logRef} className="overflow-y-auto h-56 divide-y divide-white/[0.03]">
            {log.length === 0 ? (
              <div className="h-full flex flex-col items-center justify-center gap-3 text-white/20">
                <Loader className="w-5 h-5 animate-spin" />
                <span className="text-xs">Waiting for first pipeline event...</span>
              </div>
            ) : (
              log.map((entry, i) => {
                const cfg = LOG_CFG[entry.level] || LOG_CFG.info;
                const Icon = cfg.icon;
                const src = stageSource(entry.stage);
                const ts = entry.ts ? new Date(entry.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";
                const isNewest = i === log.length - 1;
                return (
                  <div key={i} className={`px-4 py-2.5 flex gap-2.5 items-start transition-colors ${isNewest ? cfg.bg : ""}`}
                    style={isNewest ? { borderLeft: "2px solid" } : { borderLeft: "2px solid transparent" }}>
                    <div className="flex-shrink-0 pt-0.5">
                      {src === "facebook" ? <span className="text-[8px] font-bold uppercase px-1 py-px rounded bg-indigo-500/15 text-indigo-400 border border-indigo-500/20">FB</span>
                        : src === "craigslist" ? <span className="text-[8px] font-bold uppercase px-1 py-px rounded bg-orange-500/15 text-orange-400 border border-orange-500/20">CL</span>
                        : <span className="text-[8px] font-bold uppercase px-1 py-px rounded bg-white/5 text-white/25 border border-white/10">SYS</span>}
                    </div>
                    <Icon className={`w-3 h-3 mt-0.5 flex-shrink-0 ${isNewest ? cfg.text : "text-white/20"}`} />
                    <div className="flex-1 min-w-0">
                      <span className={`text-[10px] font-semibold leading-tight ${isNewest ? cfg.text : "text-white/35"}`}>{entry.stage}</span>
                      <p className={`text-[10px] leading-relaxed mt-px ${isNewest ? "text-white/60" : "text-white/22"}`}>{entry.detail}</p>
                    </div>
                    <span className="text-[9px] text-white/15 font-mono flex-shrink-0 mt-0.5 tabular-nums">{ts}</span>
                  </div>
                );
              })
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

// ── Facebook lead card ─────────────────────────────────────────────────────────

const FacebookLeadCard = ({ lead, onSelect, updateStatus }) => {
  const authorInitial = (lead.raw_json?.authorName || lead.title || "?").charAt(0).toUpperCase();
  const groupName = lead.raw_json?.groupName || lead.raw_json?.group || "";
  const likes = lead.raw_json?.likesCount || 0;
  const comments = lead.raw_json?.commentsCount || 0;
  const shares = lead.raw_json?.sharesCount || 0;
  const authorName = lead.raw_json?.authorName || lead.raw_json?.author || "";
  const snippet = (lead.post || "").slice(0, 200) + ((lead.post || "").length > 200 ? "..." : "");
  return (
    <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl p-3 lg:p-4 hover:bg-white/[0.04] hover:border-indigo-500/20 transition-all group cursor-pointer" onClick={() => onSelect(lead)}>
      <div className="flex items-start gap-3 mb-3">
        <div className="w-8 h-8 rounded-full bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white text-sm font-bold flex-shrink-0">{authorInitial}</div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap mb-0.5">
            {authorName && <span className="text-xs font-semibold text-white/80 truncate max-w-[140px]">{authorName}</span>}
            <SourceTag source="FACEBOOK" />
            <ScoreDot score={lead.score} />
          </div>
          {groupName && <div className="flex items-center gap-1 text-[10px] text-indigo-400/70"><Users className="w-2.5 h-2.5 flex-shrink-0" /><span className="truncate">{groupName}</span></div>}
          {lead.datetime && <div className="text-[10px] text-white/25 font-mono mt-0.5">{new Date(lead.datetime).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}</div>}
        </div>
        <div onClick={e => e.stopPropagation()}><StatusSelect value={lead.status} onChange={v => updateStatus(lead.post_id, v)} /></div>
      </div>
      {snippet && <p className="text-xs text-white/55 leading-relaxed mb-3 whitespace-pre-wrap">{snippet}</p>}
      {(lead.phone || lead.email) && (
        <div className="flex flex-wrap gap-2 mb-3">
          {lead.phone && <a href={`tel:${lead.phone}`} onClick={e => e.stopPropagation()} className="inline-flex items-center gap-1.5 bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-[11px] font-semibold px-2.5 py-1 rounded-full hover:bg-emerald-500/20 transition-colors"><Phone className="w-3 h-3" />{lead.phone}</a>}
          {lead.email && <a href={`mailto:${lead.email}`} onClick={e => e.stopPropagation()} className="inline-flex items-center gap-1.5 bg-violet-500/10 border border-violet-500/20 text-violet-400 text-[11px] font-semibold px-2.5 py-1 rounded-full hover:bg-violet-500/20 transition-colors"><Mail className="w-3 h-3" />{lead.email}</a>}
        </div>
      )}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3 text-[11px] text-white/25 flex-wrap">
          {likes > 0 && <span className="flex items-center gap-1"><ThumbsUp className="w-3 h-3" />{likes}</span>}
          {comments > 0 && <span className="flex items-center gap-1"><MessageSquare className="w-3 h-3" />{comments}</span>}
          {shares > 0 && <span className="flex items-center gap-1"><Share2 className="w-3 h-3" />{shares}</span>}
          {lead.location && <span className="flex items-center gap-1"><MapPin className="w-3 h-3" />{lead.location}</span>}
        </div>
        <div className="flex items-center gap-2">
          {lead.url && <a href={lead.url} target="_blank" rel="noreferrer" onClick={e => e.stopPropagation()} className="text-indigo-400/50 hover:text-indigo-400 transition-colors"><ExternalLink className="w-3.5 h-3.5" /></a>}
          <button className="opacity-0 group-hover:opacity-100 transition-opacity text-white/40 hover:text-white flex items-center gap-1 text-[11px]"><Eye className="w-3.5 h-3.5" /> View</button>
        </div>
      </div>
    </div>
  );
};

// ── Leads table ───────────────────────────────────────────────────────────────

function LeadsTable({ leads, onSelect, updateStatus }) {
  return (
    <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden overflow-x-auto">
      <table className="w-full min-w-[600px]">
        <thead>
          <tr className="border-b border-white/[0.06]">
            {["Title", "Src", "Category", "Location", "Contact", "Status", "Score", "Date", ""].map(h => (
              <th key={h} className="px-3 py-3 text-left text-[10px] font-semibold uppercase tracking-widest text-white/25 whitespace-nowrap">{h}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {leads.map((lead, i) => (
            <tr key={lead.post_id} className={`border-b border-white/[0.04] hover:bg-white/[0.02] transition-colors group cursor-pointer ${i % 2 === 0 ? "" : "bg-white/[0.01]"}`} onClick={() => onSelect(lead)}>
              <td className="px-3 py-3 max-w-[180px]"><p className="text-xs font-medium text-white/80 truncate">{lead.title}</p></td>
              <td className="px-3 py-3 whitespace-nowrap"><SourceTag source={lead.source} /></td>
              <td className="px-3 py-3 whitespace-nowrap"><span className="text-[10px] text-white/40 font-mono">{lead.service_category || lead.category || "---"}</span></td>
              <td className="px-3 py-3"><span className="text-xs text-white/40 truncate max-w-[80px] block">{lead.location || lead.state || "---"}</span></td>
              <td className="px-3 py-3 whitespace-nowrap">
                <div className="flex gap-1.5">
                  {lead.phone && <span title={lead.phone}><Phone className="w-3.5 h-3.5 text-emerald-400" /></span>}
                  {lead.email && <span title={lead.email}><Mail className="w-3.5 h-3.5 text-violet-400" /></span>}
                  {!lead.phone && !lead.email && <span className="text-white/20 text-[10px]">---</span>}
                </div>
              </td>
              <td className="px-3 py-3" onClick={e => e.stopPropagation()}><StatusSelect value={lead.status} onChange={v => updateStatus(lead.post_id, v)} /></td>
              <td className="px-3 py-3"><ScoreDot score={lead.score} /></td>
              <td className="px-3 py-3 whitespace-nowrap"><span className="text-[10px] text-white/25 font-mono">{lead.datetime ? new Date(lead.datetime).toLocaleDateString("en-US", { month: "short", day: "numeric" }) : "---"}</span></td>
              <td className="px-3 py-3"><button className="opacity-0 group-hover:opacity-100 transition-opacity text-white/40 hover:text-white"><Eye className="w-4 h-4" /></button></td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Improved Pagination ────────────────────────────────────────────────────────

function Pagination({ page, totalPages, total, pageSize, onPage, loading }) {
  if (totalPages <= 1) return null;
  const from = (page - 1) * pageSize + 1;
  const to   = Math.min(page * pageSize, total);

  const buildPages = () => {
    const delta = 2;
    const range = [];
    const dots  = "…";
    for (let i = Math.max(2, page - delta); i <= Math.min(totalPages - 1, page + delta); i++) range.push(i);
    const pages = [];
    pages.push(1);
    if (range[0] > 2) pages.push(dots);
    pages.push(...range);
    if (range[range.length - 1] < totalPages - 1) pages.push(dots);
    if (totalPages > 1) pages.push(totalPages);
    return pages;
  };

  const pages = buildPages();

  return (
    <div className="mt-4 flex flex-col sm:flex-row items-center justify-between gap-3 px-1">
      <div className="flex items-center gap-3 text-[11px] text-white/30 order-2 sm:order-1">
        <span>Showing <span className="text-white/60 font-semibold tabular-nums">{from}–{to}</span> of <span className="text-white/60 font-semibold tabular-nums">{total.toLocaleString()}</span> leads</span>
        {loading && <span className="flex items-center gap-1 text-white/20"><Loader className="w-3 h-3 animate-spin" /> loading</span>}
      </div>

      <div className="flex items-center gap-1 order-1 sm:order-2">
        <button onClick={() => onPage(page - 1)} disabled={page === 1}
          className="flex items-center gap-1 px-3 py-1.5 rounded-xl bg-white/[0.04] border border-white/[0.06] text-xs text-white/40 hover:text-white hover:bg-white/[0.08] disabled:opacity-20 disabled:cursor-not-allowed transition-all">
          <svg className="w-3 h-3" fill="none" viewBox="0 0 16 16"><path d="M10 12L6 8l4-4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
          <span className="hidden sm:inline">Prev</span>
        </button>

        <div className="flex items-center gap-0.5">
          {pages.map((p, i) =>
            p === "…" ? (
              <span key={`dot-${i}`} className="px-2 text-white/20 text-xs select-none">…</span>
            ) : (
              <button key={p} onClick={() => onPage(p)}
                className={`min-w-[32px] h-8 rounded-lg text-xs font-semibold transition-all tabular-nums ${
                  page === p
                    ? "bg-blue-600 text-white shadow-lg shadow-blue-500/25"
                    : "text-white/40 hover:text-white hover:bg-white/[0.07]"
                }`}>
                {p}
              </button>
            )
          )}
        </div>

        <button onClick={() => onPage(page + 1)} disabled={page === totalPages}
          className="flex items-center gap-1 px-3 py-1.5 rounded-xl bg-white/[0.04] border border-white/[0.06] text-xs text-white/40 hover:text-white hover:bg-white/[0.08] disabled:opacity-20 disabled:cursor-not-allowed transition-all">
          <span className="hidden sm:inline">Next</span>
          <svg className="w-3 h-3" fill="none" viewBox="0 0 16 16"><path d="M6 4l4 4-4 4" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/></svg>
        </button>
      </div>

      {totalPages > 10 && (
        <div className="flex items-center gap-2 text-[11px] text-white/30 order-3">
          <span className="hidden sm:inline">Go to</span>
          <input
            type="number" min={1} max={totalPages}
            defaultValue={page}
            key={page}
            onKeyDown={e => {
              if (e.key === "Enter") {
                const v = parseInt(e.target.value);
                if (!isNaN(v) && v >= 1 && v <= totalPages) onPage(v);
              }
            }}
            className="w-14 bg-white/5 border border-white/10 rounded-lg px-2 py-1 text-xs text-white text-center focus:outline-none focus:border-blue-500/60 transition-all [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none"
          />
        </div>
      )}
    </div>
  );
}

// ── Source controls panel ─────────────────────────────────────────────────────

function SliderRow({ label, hint, value, min, max, step, onChange, format, accent = "blue" }) {
  const a = {
    blue:   { track: "accent-blue-500",   val: "text-blue-300",   bg: "bg-blue-600/10",   border: "border-blue-500/20" },
    orange: { track: "accent-orange-500", val: "text-orange-300", bg: "bg-orange-600/10", border: "border-orange-500/20" },
    indigo: { track: "accent-indigo-500", val: "text-indigo-300", bg: "bg-indigo-600/10", border: "border-indigo-500/20" },
  }[accent];
  return (
    <div>
      <div className="flex items-center justify-between mb-1.5">
        <span className="text-[11px] font-semibold uppercase tracking-widest text-white/30">{label}</span>
        <span className={`text-xs font-bold tabular-nums px-2 py-0.5 rounded-lg ${a.bg} ${a.border} border ${a.val}`}>{format ? format(value) : value}</span>
      </div>
      <input type="range" min={min} max={max} step={step} value={value} onChange={e => onChange(Number(e.target.value))} className={`w-full cursor-pointer ${a.track}`} />
      {hint && <p className="text-[10px] text-white/20 mt-1">{hint}</p>}
    </div>
  );
}

function SourceControls({ selectedSources, toggleSource, clMaxPostsPerCity, setClMaxPostsPerCity, clDaysBack, setClDaysBack, fbMaxGroups, setFbMaxGroups, fbPostsPerGroup, setFbPostsPerGroup }) {
  const [clOpen, setClOpen] = useState(true);
  const [fbOpen, setFbOpen] = useState(true);
  const clActive = selectedSources.includes("craigslist");
  const fbActive = selectedSources.includes("facebook");
  return (
    <div className="space-y-3">
      <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block">Sources &amp; Settings</label>
      <div className={`rounded-xl border transition-all ${clActive ? "border-orange-500/30 bg-orange-500/5" : "border-white/[0.06] bg-white/[0.02]"}`}>
        <div className="flex items-center gap-2 px-3 py-2.5">
          <button onClick={() => toggleSource("craigslist")} className="flex items-center gap-2 flex-1 text-left">
            <div className={`w-4 h-4 rounded border-2 flex items-center justify-center flex-shrink-0 transition-all ${clActive ? "bg-orange-500 border-orange-500" : "border-white/20"}`}>
              {clActive && <svg viewBox="0 0 10 8" className="w-2.5 h-2.5"><path d="M1 4l3 3 5-6" stroke="white" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/></svg>}
            </div>
            <Globe className={`w-3.5 h-3.5 flex-shrink-0 ${clActive ? "text-orange-400" : "text-white/30"}`} />
            <span className={`text-xs font-semibold ${clActive ? "text-white" : "text-white/40"}`}>🔶 Craigslist</span>
          </button>
          {clActive && <button onClick={() => setClOpen(p => !p)} className="text-white/20 hover:text-white/50 transition-colors">{clOpen ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}</button>}
        </div>
        {clActive && clOpen && (
          <div className="px-3 pb-3 space-y-3 border-t border-white/[0.05] pt-3">
            <SliderRow label="Max posts per city" hint="Higher = more leads but slower. 40 is a good balance." value={clMaxPostsPerCity} min={10} max={200} step={10} onChange={setClMaxPostsPerCity} accent="orange" />
            <SliderRow label="Days back" hint="Only pull posts newer than this." value={clDaysBack} min={1} max={30} step={1} onChange={setClDaysBack} format={v => `${v}d`} accent="orange" />
          </div>
        )}
      </div>
      <div className={`rounded-xl border transition-all ${fbActive ? "border-indigo-500/30 bg-indigo-500/5" : "border-white/[0.06] bg-white/[0.02]"}`}>
        <div className="flex items-center gap-2 px-3 py-2.5">
          <button onClick={() => toggleSource("facebook")} className="flex items-center gap-2 flex-1 text-left">
            <div className={`w-4 h-4 rounded border-2 flex items-center justify-center flex-shrink-0 transition-all ${fbActive ? "bg-indigo-500 border-indigo-500" : "border-white/20"}`}>
              {fbActive && <svg viewBox="0 0 10 8" className="w-2.5 h-2.5"><path d="M1 4l3 3 5-6" stroke="white" strokeWidth="1.5" fill="none" strokeLinecap="round" strokeLinejoin="round"/></svg>}
            </div>
            <Users className={`w-3.5 h-3.5 flex-shrink-0 ${fbActive ? "text-indigo-400" : "text-white/30"}`} />
            <span className={`text-xs font-semibold ${fbActive ? "text-white" : "text-white/40"}`}>🔷 Facebook Groups</span>
          </button>
          {fbActive && <button onClick={() => setFbOpen(p => !p)} className="text-white/20 hover:text-white/50 transition-colors">{fbOpen ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}</button>}
        </div>
        {fbActive && fbOpen && (
          <div className="px-3 pb-3 space-y-3 border-t border-white/[0.05] pt-3">
            <SliderRow label="Groups to search" hint="More groups = wider reach but longer run." value={fbMaxGroups} min={5} max={100} step={5} onChange={setFbMaxGroups} accent="indigo" />
            <SliderRow label="Posts per group" hint="How many recent posts to pull per group." value={fbPostsPerGroup} min={10} max={100} step={10} onChange={setFbPostsPerGroup} accent="indigo" />
          </div>
        )}
      </div>
      {!clActive && !fbActive && <p className="text-[10px] text-red-400/70 mt-1">⚠ Select at least one source to run a scrape.</p>}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function Services() {
  const { logout, user } = useContext(AuthContext);
  const token = localStorage.getItem("access");
  const headers = useMemo(() => ({ Authorization: `Bearer ${token}` }), [token]);

  // ── Leads + server-side pagination state ────────────────────────────────────
  const [leads, setLeads] = useState([]);
  const seenLeadIds = useRef(new Set());

  const [serverTotal, setServerTotal]         = useState(0);
  const [serverTotalPages, setServerTotalPages] = useState(1);
  const [serverPage, setServerPage]           = useState(1);
  const [serverPageSize, setServerPageSize]   = useState(PAGE_SIZE);

  const [globalStats, setGlobalStats] = useState({ total: 0, cl: 0, fb: 0, withPhone: 0, withEmail: 0, avgScore: 0 });
  const [statsLoading, setStatsLoading] = useState(false);

  const [history, setHistory] = useState([]);
  const [categories, setCategories] = useState([]);
  const [cities, setCities] = useState([]);
  const [locationType, setLocationType]   = useState("city");
  const [locationValue, setLocationValue] = useState("");
  const selectedCityCode = useRef(null);
  const [selectedCategories, setSelectedCategories] = useState([]);
  const [selectedSubServices, setSelectedSubServices] = useState([]);
  const [expandedCategories, setExpandedCategories] = useState({});

  const [selectedSources, setSelectedSources]       = useState(["craigslist"]);
  const [clMaxPostsPerCity, setClMaxPostsPerCity]   = useState(40);
  const [clDaysBack, setClDaysBack]                 = useState(7);
  const [fbMaxGroups, setFbMaxGroups]               = useState(20);
  const [fbPostsPerGroup, setFbPostsPerGroup]       = useState(30);

  const [scraping, setScraping]         = useState(false);
  const [runId, setRunId]               = useState(null);
  const [scrapeStatus, setScrapeStatus] = useState(null);
  const prevStatusRef                   = useRef(null);
  const [isAborting, setIsAborting]     = useState(false);

  const [fSource, setFSource]           = useState("");
  const [fServiceCat, setFServiceCat]   = useState("");
  const [fServiceLabel, setFServiceLabel] = useState("");
  const [fStatus, setFStatus]           = useState("");
  const [fMinScore, setFMinScore]       = useState("");
  const [fSearch, setFSearch]           = useState("");
  const [fHasPhone, setFHasPhone]       = useState(false);
  const [fHasEmail, setFHasEmail]       = useState(false);

  const [selectedLead, setSelectedLead] = useState(null);
  const [loading, setLoading]           = useState(false);
  const [page, setPage]                 = useState(1);
  const [historyPage, setHistoryPage]   = useState(1);
  const [activeTab, setActiveTab]       = useState("leads");
  const [citySuggestions, setCitySuggestions] = useState([]);
  const [showSuggestions, setShowSuggestions] = useState(false);
  const [fbViewMode, setFbViewMode]     = useState("cards");
  const [sidebarOpen, setSidebarOpen]   = useState(false);

  // ── fetchLeads: server-side pagination ─────────────────────────
  const fetchLeads = useCallback(async (pageNum = 1, mode = "replace") => {
    if (mode === "replace") setLoading(true);
    try {
      const params = { page: pageNum, page_size: PAGE_SIZE };
      if (fSource)     params.source            = fSource;
      if (fServiceCat) params.service_category  = fServiceCat;
      if (fStatus)     params.status            = fStatus;
      if (fMinScore)   params.min_score         = fMinScore;
      if (fSearch)     params.search            = fSearch;

      const res  = await axios.get(`${API}/leads/`, { headers, params });
      const data = res.data;

      const incoming   = Array.isArray(data) ? data : (data.results ?? []);
      const backendTotal      = data.total      ?? incoming.length;
      const backendTotalPages = data.total_pages ?? 1;
      const backendPage       = data.page        ?? pageNum;
      const backendPageSize   = data.page_size   ?? PAGE_SIZE;

      setServerTotal(backendTotal);
      setServerTotalPages(backendTotalPages);
      setServerPage(backendPage);
      setServerPageSize(backendPageSize);

      if (mode === "append") {
        const fresh = incoming.filter(l => !seenLeadIds.current.has(l.post_id));
        fresh.forEach(l => seenLeadIds.current.add(l.post_id));
        if (fresh.length > 0) setLeads(prev => [...fresh, ...prev]);
      } else {
        seenLeadIds.current = new Set(incoming.map(l => l.post_id));
        setLeads(incoming);
      }
    } catch (e) { console.error(e); }
    if (mode === "replace") setLoading(false);
  }, [headers, fSource, fServiceCat, fStatus, fMinScore, fSearch]);

  // ── fetchStats: get global totals from backend ─────────────────
  const fetchStats = useCallback(async () => {
    setStatsLoading(true);
    try {
      const baseParams = { page: 1, page_size: 1 };
      if (fServiceCat) baseParams.service_category = fServiceCat;
      if (fStatus)     baseParams.status           = fStatus;
      if (fMinScore)   baseParams.min_score        = fMinScore;
      if (fSearch)     baseParams.search           = fSearch;

      const [allRes, clRes, fbRes, phoneRes, emailRes] = await Promise.all([
        axios.get(`${API}/leads/`, { headers, params: { ...baseParams } }),
        axios.get(`${API}/leads/`, { headers, params: { ...baseParams, source: "CRAIGSLIST" } }),
        axios.get(`${API}/leads/`, { headers, params: { ...baseParams, source: "FACEBOOK" } }),
        axios.get(`${API}/leads/`, { headers, params: { ...baseParams, has_phone: true } }),
        axios.get(`${API}/leads/`, { headers, params: { ...baseParams, has_email: true } }),
      ]);

      const sampleRes = await axios.get(`${API}/leads/`, { headers, params: { ...baseParams, page_size: 100 } });
      const sample    = sampleRes.data.results ?? [];
      const avgScore  = sample.length
        ? Math.round(sample.reduce((a, l) => a + (l.score || 0), 0) / sample.length)
        : 0;

      setGlobalStats({
        total:     allRes.data.total   ?? 0,
        cl:        clRes.data.total    ?? 0,
        fb:        fbRes.data.total    ?? 0,
        withPhone: phoneRes.data.total ?? 0,
        withEmail: emailRes.data.total ?? 0,
        avgScore,
      });
    } catch (e) { console.error("fetchStats error", e); }
    setStatsLoading(false);
  }, [headers, fServiceCat, fStatus, fMinScore, fSearch]);

  const fetchCategories = useCallback(async () => {
    try { const res = await axios.get(`${API}/meta/categories/`); setCategories(res.data.categories); } catch (e) {}
  }, []);

  const fetchCities = useCallback(async () => {
    try { const res = await axios.get(`${API}/meta/cities/`, { headers }); setCities(res.data.cities); } catch (e) {}
  }, [headers]);

  const fetchHistory = useCallback(async () => {
    try { const res = await axios.get(`${API}/scrape/history/`, { headers }); setHistory(res.data.runs ?? res.data ?? []); } catch (e) {}
  }, [headers]);

  // ── checkScrapeStatus: reliable polling ────────────────────────
  const checkScrapeStatus = useCallback(async () => {
    try {
      const res  = await axios.get(`${API}/scrape/status/`, { headers });
      const data = res.data;
      setScrapeStatus(data);

      const wasRunning = prevStatusRef.current === "RUNNING";
      const nowDone    = data.status !== "RUNNING" && data.status !== "IDLE";
      prevStatusRef.current = data.status;

      if (data.status === "RUNNING") {
        setScraping(true);
        setRunId(data.run_id);
        fetchLeads(1, "append");
      } else {
        setScraping(false);
        setIsAborting(false);
        if (wasRunning && nowDone) {
          seenLeadIds.current = new Set();
          fetchLeads(1, "replace");
          fetchStats();
          fetchHistory();
        }
      }
    } catch (e) { console.error(e); }
  }, [headers, fetchLeads, fetchStats]);

  // ── Master polling interval ────────────────────────────────────
  useEffect(() => {
    fetchLeads();
    fetchStats();
    fetchCategories();
    fetchCities();
    fetchHistory();
    checkScrapeStatus();
    const iv = setInterval(checkScrapeStatus, 3000);
    return () => clearInterval(iv);
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // Re-fetch on filter change
  const isFirstRender = useRef(true);
  useEffect(() => {
    if (isFirstRender.current) { isFirstRender.current = false; return; }
    seenLeadIds.current = new Set();
    setPage(1);
    fetchLeads(1, "replace");
    fetchStats();
  }, [fSource, fServiceCat, fStatus, fMinScore, fSearch]);

  // City suggestions
  useEffect(() => {
    if (locationType !== "city" || !locationValue.trim()) { setCitySuggestions([]); setShowSuggestions(false); return; }
    const q = locationValue.toLowerCase();
    const matches = cities.filter(c => c.name.toLowerCase().includes(q) || c.display?.toLowerCase().includes(q)).slice(0, 8);
    setCitySuggestions(matches);
    setShowSuggestions(matches.length > 0);
  }, [locationValue, locationType, cities]);

  const handleLocationChange = e => { setLocationValue(e.target.value); selectedCityCode.current = null; };
  const handleCitySelect = c => { setLocationValue(c.name); selectedCityCode.current = c.code; setShowSuggestions(false); };
  const getLocationPayload = () => {
    if (locationType === "city")
      return { type: "city", value: selectedCityCode.current ?? locationValue.trim(), display: locationValue.trim() };
    return { type: locationType, value: locationValue.trim() };
  };

  const startScrape = async () => {
    if (!locationValue.trim())            return alert("Enter a location.");
    if (selectedCategories.length === 0)  return alert("Select at least one category.");
    if (selectedSources.length === 0)     return alert("Select at least one source.");
    setScraping(true); setScrapeStatus(null); setSidebarOpen(false);
    try {
      const res = await axios.post(`${API}/scrape/start/`, {
        location:           getLocationPayload(),
        categories:         selectedCategories,
        sub_services:       selectedSubServices.length > 0 ? selectedSubServices : undefined,
        sources:            selectedSources,
        cl_max_per_city:    clMaxPostsPerCity,
        cl_days_back:       clDaysBack,
        max_groups:         fbMaxGroups,
        fb_posts_per_group: fbPostsPerGroup,
      }, { headers });
      setRunId(res.data.run_id);
      fetchHistory();
      setTimeout(checkScrapeStatus, 800);
    } catch (e) { alert(e.response?.data?.error || "Failed to start scrape."); setScraping(false); }
  };

  const cancelScrape = async () => {
    if (!runId) return; setIsAborting(true);
    try {
      await axios.post(`${API}/scrape/cancel/`, { run_id: runId }, { headers });
      setTimeout(() => { fetchLeads(1, "replace"); fetchHistory(); checkScrapeStatus(); }, 1000);
    } catch (e) { setIsAborting(false); }
  };

  const updateLeadStatus = async (postId, status) => {
    try {
      await axios.patch(`${API}/leads/${postId}/status/`, { status }, { headers });
      setLeads(prev => prev.map(l => l.post_id === postId ? { ...l, status } : l));
      if (selectedLead?.post_id === postId) setSelectedLead(prev => ({ ...prev, status }));
    } catch (e) { console.error(e); }
  };

  const handlePageChange = (newPage) => {
    setPage(newPage);
    fetchLeads(newPage, "replace");
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const filtered = useMemo(() => {
    let out = [...(Array.isArray(leads) ? leads : [])];
    if (fHasPhone)    out = out.filter(l => l.phone);
    if (fHasEmail)    out = out.filter(l => l.email);
    if (fServiceLabel) out = out.filter(l => matchesService(l, fServiceLabel));
    return out;
  }, [leads, fHasPhone, fHasEmail, fServiceLabel]);

  const mapLeads  = filtered.filter(l => l.latitude && l.longitude);
  const fbLeads   = filtered.filter(l => l.source === "FACEBOOK");
  const clLeads   = filtered.filter(l => l.source === "CRAIGSLIST");
  const hasMixed  = fbLeads.length > 0 && clLeads.length > 0;

  const toggleCategory = k => {
    setSelectedCategories(p => {
      const next = p.includes(k) ? p.filter(x => x !== k) : [...p, k];
      if (p.includes(k)) {
        const taxKey = CAT_KEY_TO_TAXONOMY[k];
        if (taxKey) { const subLabels = SERVICE_TAXONOMY[taxKey]?.services.map(s => s.label) || []; setSelectedSubServices(prev => prev.filter(s => !subLabels.includes(s))); }
      }
      return next;
    });
  };
  const toggleSubService    = label => setSelectedSubServices(p => p.includes(label) ? p.filter(x => x !== label) : [...p, label]);
  const toggleCategoryExpand = k   => setExpandedCategories(p => ({ ...p, [k]: !p[k] }));
  const toggleSource         = s   => setSelectedSources(p => p.includes(s) ? p.filter(x => x !== s) : [...p, s]);
  const resetFilters = () => {
    setFSource(""); setFServiceCat(""); setFServiceLabel(""); setFStatus("");
    setFMinScore(""); setFSearch(""); setFHasPhone(false); setFHasEmail(false);
    setPage(1);
  };
  const hasActiveFilters = fSource || fServiceCat || fServiceLabel || fStatus || fMinScore || fSearch || fHasPhone || fHasEmail;
  const showActivityPanel = scrapeStatus && scrapeStatus.status !== undefined && scrapeStatus.status !== "IDLE";

  // ── Sidebar ───────────────────────────────────────────────────────────────────
  const SidebarContent = () => (
    <>
      <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
        <div className="px-4 py-4 border-b border-white/[0.06] flex items-center justify-between">
          <div className="flex items-center gap-2"><Zap className="w-4 h-4 text-blue-400" /><span className="text-sm font-semibold">New Scrape</span></div>
          {scrapeStatus && !scraping && (
            <Badge color={scrapeStatus.status === "SUCCEEDED" ? "green" : scrapeStatus.status === "PARTIAL" ? "yellow" : "slate"}>{scrapeStatus.status}</Badge>
          )}
        </div>
        <div className="p-4 space-y-4">
          <div>
            <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block mb-2">Location Type</label>
            <div className="grid grid-cols-3 gap-1.5 bg-white/5 p-1 rounded-xl">
              {["city", "state", "zip"].map(t => (
                <button key={t} onClick={() => { setLocationType(t); setLocationValue(""); selectedCityCode.current = null; }}
                  className={`py-1.5 rounded-lg text-xs font-semibold transition-all capitalize ${locationType === t ? "bg-blue-600 text-white shadow-lg shadow-blue-500/20" : "text-white/40 hover:text-white/70"}`}>{t}</button>
              ))}
            </div>
          </div>
          <div className="relative">
            <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block mb-2">
              {locationType === "city" ? "City Name" : locationType === "state" ? "State Code (e.g. TX)" : "ZIP Code"}
            </label>
            <div className="relative">
              <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-white/30" />
              <input type="text" value={locationValue} onChange={handleLocationChange}
                onBlur={() => setTimeout(() => setShowSuggestions(false), 150)}
                placeholder={locationType === "city" ? "Houston" : locationType === "state" ? "TX" : "77001"}
                className="w-full bg-white/5 border border-white/10 rounded-xl pl-9 pr-4 py-2.5 text-sm text-white placeholder-white/20 focus:outline-none focus:border-blue-500/60 transition-all" />
            </div>
            {showSuggestions && (
              <div className="absolute z-50 left-0 right-0 mt-1 bg-[#1a1d27] border border-white/10 rounded-xl shadow-2xl overflow-hidden">
                {citySuggestions.map(c => (
                  <button key={c.code} onClick={() => handleCitySelect(c)}
                    className="w-full text-left px-4 py-2.5 text-sm text-white/70 hover:bg-white/5 hover:text-white flex justify-between items-center transition-colors">
                    <span>{c.name}</span><span className="text-white/30 text-xs">{c.state}</span>
                  </button>
                ))}
              </div>
            )}
          </div>
          <div>
            <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block mb-2">Categories</label>
            <div className="space-y-2">
              {categories.map(cat => {
                const taxKey      = CAT_KEY_TO_TAXONOMY[cat.key];
                const subServices = taxKey ? (SERVICE_TAXONOMY[taxKey]?.services || []) : [];
                const isSelected  = selectedCategories.includes(cat.key);
                const isExpanded  = expandedCategories[cat.key];
                const activeSubCount = subServices.filter(s => selectedSubServices.includes(s.label)).length;
                return (
                  <div key={cat.key} className={`rounded-xl border overflow-hidden transition-all ${isSelected ? "border-blue-500/40" : "border-white/[0.06]"}`}>
                    <div className={`flex items-center gap-3 p-3 cursor-pointer transition-all ${isSelected ? "bg-blue-600/10" : "bg-white/[0.02] hover:bg-white/[0.04]"}`}>
                      <div onClick={() => toggleCategory(cat.key)} className={`w-4 h-4 rounded-md border-2 flex items-center justify-center flex-shrink-0 transition-all ${isSelected ? "border-blue-500 bg-blue-500" : "border-white/20"}`}>
                        {isSelected && <svg className="w-2.5 h-2.5 text-white" fill="currentColor" viewBox="0 0 12 12"><path d="M10 3L5 8.5 2 5.5l-1 1L5 10.5l6-7-1-0.5z"/></svg>}
                      </div>
                      <span onClick={() => { toggleCategory(cat.key); if (!isSelected) setExpandedCategories(p => ({ ...p, [cat.key]: true })); }} className={`flex-1 text-xs font-medium ${isSelected ? "text-white" : "text-white/50"}`}>{cat.label}</span>
                      {isSelected && activeSubCount > 0 && <span className="text-[9px] font-bold bg-blue-500/20 text-blue-300 border border-blue-500/30 px-1.5 py-0.5 rounded-full">{activeSubCount}</span>}
                      {isSelected && subServices.length > 0 && (
                        <button onClick={e => { e.stopPropagation(); toggleCategoryExpand(cat.key); }} className="text-white/25 hover:text-white/60 transition-colors ml-1 flex-shrink-0">
                          {isExpanded ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
                        </button>
                      )}
                    </div>
                    {isSelected && isExpanded && subServices.length > 0 && (
                      <div className="border-t border-white/[0.05] bg-white/[0.01] px-3 py-2 space-y-1">
                        <p className="text-[9px] text-white/20 uppercase tracking-widest font-semibold mb-2 px-1">Filter by service type (optional)</p>
                        {subServices.map(svc => {
                          const isSvcSelected = selectedSubServices.includes(svc.label);
                          return (
                            <button key={svc.label} onClick={() => toggleSubService(svc.label)} className={`w-full flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-left transition-all ${isSvcSelected ? "bg-blue-500/15 border border-blue-500/30 text-blue-200" : "text-white/40 hover:bg-white/[0.04] hover:text-white/70 border border-transparent"}`}>
                              <div className={`w-3 h-3 rounded border flex items-center justify-center flex-shrink-0 transition-all ${isSvcSelected ? "border-blue-400 bg-blue-500" : "border-white/20"}`}>
                                {isSvcSelected && <svg className="w-2 h-2 text-white" fill="currentColor" viewBox="0 0 12 12"><path d="M10 3L5 8.5 2 5.5l-1 1L5 10.5l6-7-1-0.5z"/></svg>}
                              </div>
                              <span className="text-[11px] font-medium leading-tight">{svc.label}</span>
                            </button>
                          );
                        })}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          </div>
          <SourceControls
            selectedSources={selectedSources} toggleSource={toggleSource}
            clMaxPostsPerCity={clMaxPostsPerCity} setClMaxPostsPerCity={setClMaxPostsPerCity}
            clDaysBack={clDaysBack} setClDaysBack={setClDaysBack}
            fbMaxGroups={fbMaxGroups} setFbMaxGroups={setFbMaxGroups}
            fbPostsPerGroup={fbPostsPerGroup} setFbPostsPerGroup={setFbPostsPerGroup}
          />
          {!scraping ? (
            <button onClick={startScrape} disabled={!locationValue.trim() || selectedCategories.length === 0 || selectedSources.length === 0}
              className="w-full py-3 rounded-xl bg-gradient-to-r from-blue-600 to-violet-600 text-white text-sm font-semibold hover:from-blue-500 hover:to-violet-500 transition-all shadow-lg shadow-blue-500/20 disabled:opacity-30 disabled:cursor-not-allowed">
              Launch Scrape
            </button>
          ) : (
            <div className="space-y-2">
              <div className="w-full bg-white/5 rounded-xl h-1.5 overflow-hidden">
                <div className="h-full bg-gradient-to-r from-blue-500 to-violet-500 animate-pulse rounded-full w-2/3" />
              </div>
              <div className="flex gap-2">
                <div className="flex-1 py-2.5 rounded-xl bg-amber-500/10 border border-amber-500/20 text-amber-400 text-xs font-semibold text-center">{isAborting ? "Stopping..." : "Running..."}</div>
                <button onClick={cancelScrape} disabled={isAborting} className="px-4 py-2.5 rounded-xl bg-red-500/10 border border-red-500/20 text-red-400 text-xs font-semibold hover:bg-red-500/20 transition-all disabled:opacity-50">Stop</button>
              </div>
              <p className="text-[10px] text-emerald-400/50 text-center">✓ Results save as they arrive</p>
            </div>
          )}
        </div>
      </div>
      <ActivityPanel scrapeStatus={scrapeStatus} visible={showActivityPanel} />
      <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
        <div className="px-4 py-4 border-b border-white/[0.06] flex items-center justify-between">
          <div className="flex items-center gap-2"><Filter className="w-4 h-4 text-white/40" /><span className="text-sm font-semibold">Filters</span>{hasActiveFilters && <span className="w-2 h-2 rounded-full bg-blue-400 animate-pulse" />}</div>
          {hasActiveFilters && <button onClick={resetFilters} className="text-[11px] text-white/30 hover:text-white/60 transition-colors">Clear all</button>}
        </div>
        <div className="p-4 space-y-4">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-white/30" />
            <input type="text" placeholder="Search title or location..." value={fSearch} onChange={e => setFSearch(e.target.value)}
              className="w-full bg-white/5 border border-white/10 rounded-xl pl-9 pr-4 py-2 text-xs text-white placeholder-white/20 focus:outline-none focus:border-blue-500/60 transition-all" />
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Source</label>
            <div className="grid grid-cols-3 gap-1 bg-white/5 p-1 rounded-lg">
              {[["", "All"], ["CRAIGSLIST", "CL"], ["FACEBOOK", "FB"]].map(([v, l]) => (
                <button key={v} onClick={() => setFSource(v)} className={`py-1.5 rounded-md text-[11px] font-semibold transition-all ${fSource === v ? "bg-white/10 text-white" : "text-white/30 hover:text-white/60"}`}>{l}</button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Category</label>
            <select value={fServiceCat} onChange={e => setFServiceCat(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-blue-500/60 transition-all">
              <option value="">All categories</option>
              {categories.map(c => <option key={c.key} value={c.key}>{c.label}</option>)}
            </select>
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Service Type</label>
            <select value={fServiceLabel} onChange={e => setFServiceLabel(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-blue-500/60 transition-all">
              <option value="">All services</option>
              {Object.entries(SERVICE_TAXONOMY).map(([group, g]) => (
                <optgroup key={group} label={`── ${group} ──`}>{g.services.map(s => <option key={s.label} value={s.label}>{s.label}</option>)}</optgroup>
              ))}
            </select>
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Status</label>
            <select value={fStatus} onChange={e => setFStatus(e.target.value)} className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-xs text-white focus:outline-none focus:border-blue-500/60 transition-all">
              <option value="">All statuses</option>
              {["NEW","CONTACTED","QUALIFIED","WON","LOST"].map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Min Score: <span className="text-white/60">{fMinScore || "0"}</span></label>
            <input type="range" min="0" max="100" value={fMinScore || 0} onChange={e => setFMinScore(e.target.value === "0" ? "" : e.target.value)} className="w-full accent-blue-500" />
          </div>
          <div className="flex gap-3">
            {[{ label: "Has Phone", icon: Phone, val: fHasPhone, set: setFHasPhone }, { label: "Has Email", icon: Mail, val: fHasEmail, set: setFHasEmail }].map(({ label, icon: Icon, val, set }) => (
              <button key={label} onClick={() => set(p => !p)}
                className={`flex-1 flex items-center justify-center gap-1.5 py-2 rounded-xl border text-[11px] font-semibold transition-all ${val ? "bg-emerald-600/10 border-emerald-500/40 text-emerald-400" : "bg-white/[0.02] border-white/[0.06] text-white/30 hover:border-white/20"}`}>
                <Icon className="w-3 h-3" />{label}
              </button>
            ))}
          </div>
        </div>
      </div>
    </>
  );

  return (
    <div style={{ fontFamily: "'DM Sans', 'Helvetica Neue', sans-serif" }} className="min-h-screen bg-[#0f1117] text-slate-100">
      <style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');`}</style>

      {sidebarOpen && (
        <div className="fixed inset-0 z-40 lg:hidden" onClick={() => setSidebarOpen(false)}>
          <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
          <div className="absolute left-0 top-0 bottom-0 w-[320px] bg-[#0f1117] border-r border-white/[0.06] overflow-y-auto p-4 space-y-4" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between pb-2 border-b border-white/[0.06]">
              <span className="text-sm font-semibold">Controls</span>
              <button onClick={() => setSidebarOpen(false)} className="text-white/40 hover:text-white transition-colors"><X className="w-5 h-5" /></button>
            </div>
            <SidebarContent />
          </div>
        </div>
      )}

      <header className="border-b border-white/5 bg-[#0f1117]/80 backdrop-blur-xl sticky top-0 z-50">
        <div className="max-w-[1400px] mx-auto px-4 lg:px-6 h-14 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <button onClick={() => setSidebarOpen(true)} className="lg:hidden text-white/40 hover:text-white transition-colors"><Menu className="w-5 h-5" /></button>
            <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center flex-shrink-0"><Target className="w-4 h-4 text-white" /></div>
            <span className="font-semibold text-sm tracking-tight">LeadPipe</span>
            <span className="text-white/20 text-xs hidden sm:block">|</span>
            <span className="text-white/40 text-xs hidden sm:block">contractor intelligence</span>
          </div>
          <div className="flex items-center gap-2 lg:gap-4">
            {scraping && (() => {
              const navSrc = (() => { const s = (scrapeStatus?.current_stage || "").toLowerCase(); return s.includes("facebook") ? "facebook" : s.includes("craigslist") ? "craigslist" : "system"; })();
              return (
                <div className="flex items-center gap-2">
                  <div className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-full border text-xs font-semibold ${isAborting ? "bg-red-500/10 border-red-500/20 text-red-400" : navSrc === "craigslist" ? "bg-orange-500/10 border-orange-500/25 text-orange-400" : navSrc === "facebook" ? "bg-indigo-500/10 border-indigo-500/25 text-indigo-400" : "bg-amber-500/10 border-amber-500/20 text-amber-400"}`}>
                    <Loader className="w-3 h-3 animate-spin" />
                    <span className="hidden sm:inline">{isAborting ? "Stopping..." : navSrc === "craigslist" ? "🔶 Craigslist" : navSrc === "facebook" ? "🔷 Facebook" : "Running..."}</span>
                  </div>
                  {(scrapeStatus?.leads_collected > 0) && (
                    <span className="hidden sm:flex items-center gap-1.5 bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-bold px-2.5 py-1 rounded-full">
                      <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />{scrapeStatus.leads_collected}
                    </span>
                  )}
                </div>
              );
            })()}
            <div className="text-white/40 text-xs font-medium hidden sm:block">{user?.username}</div>
            <button onClick={logout} className="text-xs text-white/40 hover:text-white/80 transition-colors">Sign out</button>
          </div>
        </div>
      </header>

      <div className="max-w-[1400px] mx-auto px-4 lg:px-6 py-6 lg:grid lg:grid-cols-[300px_1fr] xl:grid-cols-[330px_1fr] gap-6 lg:gap-8 items-start">
        <aside className="hidden lg:block space-y-4 sticky top-[72px] max-h-[calc(100vh-88px)] overflow-y-auto pr-1">
          <SidebarContent />
        </aside>

        <main className="space-y-4 min-w-0">
          {scraping && scrapeStatus && (
            <ScrapeLiveDashboard scrapeStatus={scrapeStatus} onStop={cancelScrape} isAborting={isAborting} />
          )}

          {!scraping && (<>
            {/* Stats – from globalStats, not page-limited */}
            <div className="grid grid-cols-3 sm:grid-cols-6 gap-2">
              <StatCard label="Total"      value={globalStats.total.toLocaleString()}     icon={Database}   color="text-blue-400"    loading={statsLoading} />
              <StatCard label="Craigslist" value={globalStats.cl.toLocaleString()}        icon={Globe}      color="text-orange-400"  loading={statsLoading} />
              <StatCard label="Facebook"   value={globalStats.fb.toLocaleString()}        icon={Layers}     color="text-indigo-400"  loading={statsLoading} />
              <StatCard label="Phones"     value={globalStats.withPhone.toLocaleString()} icon={Phone}      color="text-emerald-400" loading={statsLoading} />
              <StatCard label="Emails"     value={globalStats.withEmail.toLocaleString()} icon={Mail}       color="text-violet-400"  loading={statsLoading} />
              <StatCard label="Avg Score"  value={globalStats.avgScore}                   icon={TrendingUp} color="text-amber-400"   loading={statsLoading} />
            </div>

            <div className="flex items-center justify-between gap-3 flex-wrap">
              <div className="flex gap-1 bg-white/[0.03] border border-white/[0.06] p-1 rounded-xl">
                {[{ id: "leads", label: "Leads", icon: Database }, { id: "map", label: "Map", icon: MapPin }, { id: "history", label: "History", icon: Clock }].map(t => (
                  <button key={t.id} onClick={() => setActiveTab(t.id)}
                    className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-semibold transition-all ${activeTab === t.id ? "bg-white/10 text-white" : "text-white/30 hover:text-white/60"}`}>
                    <t.icon className="w-3.5 h-3.5" />
                    <span className="hidden sm:inline">{t.label}</span>
                    {t.id === "leads" && serverTotal > 0 && (
                      <span className="bg-white/10 text-white/60 px-1.5 rounded-full text-[10px] tabular-nums">{serverTotal.toLocaleString()}</span>
                    )}
                  </button>
                ))}
              </div>
              <div className="flex items-center gap-2">
                {activeTab === "leads" && (
                  <div className="flex gap-1 bg-white/[0.03] border border-white/[0.06] p-1 rounded-lg">
                    {[{ id: "cards", icon: LayoutGrid }, { id: "table", icon: List }].map(v => (
                      <button key={v.id} onClick={() => setFbViewMode(v.id)}
                        className={`px-2.5 py-1.5 rounded-md flex items-center transition-all ${fbViewMode === v.id ? "bg-white/10 text-white" : "text-white/30 hover:text-white/60"}`}>
                        <v.icon className="w-3.5 h-3.5" />
                      </button>
                    ))}
                  </div>
                )}
                <button
                  onClick={() => { seenLeadIds.current = new Set(); fetchLeads(serverPage, "replace"); fetchStats(); }}
                  className="flex items-center gap-1.5 text-xs text-white/30 hover:text-white/60 transition-colors">
                  <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} />
                  <span className="hidden sm:inline">Refresh</span>
                </button>
              </div>
            </div>

            {activeTab === "leads" && (
              <div>
                {loading ? (
                  <div className="py-24 flex flex-col items-center gap-3 bg-white/[0.03] border border-white/[0.06] rounded-2xl">
                    <Loader className="w-6 h-6 text-white/20 animate-spin" />
                    <span className="text-white/20 text-sm">Loading leads...</span>
                  </div>
                ) : filtered.length === 0 ? (
                  <div className="py-24 flex flex-col items-center gap-3 bg-white/[0.03] border border-white/[0.06] rounded-2xl">
                    <Database className="w-8 h-8 text-white/10" />
                    <span className="text-white/20 text-sm">No leads match your filters</span>
                  </div>
                ) : (
                  <>
                    {fbViewMode === "cards" && fbLeads.length > 0 ? (
                      <div className="space-y-6">
                        {fbLeads.length > 0 && (
                          <div>
                            {hasMixed && (<div className="flex items-center gap-2 mb-3"><span className="text-[10px] font-bold uppercase tracking-widest text-indigo-400/60">Facebook — {fbLeads.length} posts</span><div className="flex-1 h-px bg-white/[0.05]" /></div>)}
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
                              {fbLeads.map(lead => <FacebookLeadCard key={lead.post_id} lead={lead} onSelect={setSelectedLead} updateStatus={updateLeadStatus} />)}
                            </div>
                          </div>
                        )}
                        {clLeads.length > 0 && (
                          <div>
                            {hasMixed && (<div className="flex items-center gap-2 mb-3"><span className="text-[10px] font-bold uppercase tracking-widest text-orange-400/60">Craigslist — {clLeads.length} listings</span><div className="flex-1 h-px bg-white/[0.05]" /></div>)}
                            <LeadsTable leads={clLeads} onSelect={setSelectedLead} updateStatus={updateLeadStatus} />
                          </div>
                        )}
                      </div>
                    ) : (
                      <LeadsTable leads={filtered} onSelect={setSelectedLead} updateStatus={updateLeadStatus} />
                    )}

                    <Pagination
                      page={serverPage}
                      totalPages={serverTotalPages}
                      total={serverTotal}
                      pageSize={serverPageSize}
                      onPage={handlePageChange}
                      loading={loading}
                    />
                  </>
                )}
              </div>
            )}

            {activeTab === "map" && (
              <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
                <div className="px-4 py-3 border-b border-white/[0.06] flex flex-wrap items-center justify-between gap-2">
                  <div className="flex items-center gap-3"><span className="text-sm font-semibold">Lead Locations</span><span className="text-[11px] text-white/30">{mapLeads.length} mapped this page</span></div>
                  <div className="flex items-center gap-4 text-[11px] text-white/30">
                    {mapLeads.filter(l => l.source === "CRAIGSLIST").length > 0 && <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-orange-400" />CL: {mapLeads.filter(l => l.source === "CRAIGSLIST").length}</span>}
                    {mapLeads.filter(l => l.source === "FACEBOOK").length > 0 && <span className="flex items-center gap-1.5"><span className="w-2 h-2 rounded-full bg-indigo-400" />FB: {mapLeads.filter(l => l.source === "FACEBOOK").length}</span>}
                  </div>
                </div>
                <div className="h-[380px] lg:h-[540px]">
                  {mapLeads.length === 0 ? (
                    <div className="h-full flex flex-col items-center justify-center text-white/20 gap-3"><MapPin className="w-8 h-8 text-white/10" /><span className="text-sm">No leads with location data on this page</span></div>
                  ) : (
                    <MapContainer center={[37.8, -96]} zoom={4} minZoom={3} maxZoom={16} scrollWheelZoom style={{ height: "100%", width: "100%" }}>
                      <TileLayer attribution="&copy; OpenStreetMap" url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
                      <MapAutoFit leads={mapLeads} />
                      {mapLeads.map(lead => {
                        const isFB = lead.source === "FACEBOOK";
                        return (
                          <Marker key={lead.post_id} position={[parseFloat(lead.latitude), parseFloat(lead.longitude)]} eventHandlers={{ click: () => setSelectedLead(lead) }}>
                            <Tooltip>
                              <div className="text-xs min-w-[160px]">
                                <div className={`font-bold mb-1 ${isFB ? "text-indigo-600" : "text-orange-600"}`}>{isFB ? "🔷 Facebook" : "🔶 Craigslist"}</div>
                                <div className="font-semibold text-gray-800">{lead.title?.slice(0, 55)}</div>
                                <div className="text-gray-500 text-[11px] mt-0.5">{lead.location}</div>
                                {lead.phone && <div className="text-emerald-600 text-[11px] mt-0.5">📞 {lead.phone}</div>}
                                {lead.score && <div className="text-gray-400 text-[10px] mt-0.5">Score: {lead.score}</div>}
                              </div>
                            </Tooltip>
                          </Marker>
                        );
                      })}
                    </MapContainer>
                  )}
                </div>
              </div>
            )}

            {activeTab === "history" && (
              <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
                <div className="px-5 py-4 border-b border-white/[0.06]"><span className="text-sm font-semibold">Scrape History</span></div>
                {history.length === 0 ? (
                  <div className="py-16 text-center text-white/20 text-sm">No scrape runs yet</div>
                ) : (
                  <div className="divide-y divide-white/[0.04]">
                    {history.slice((historyPage - 1) * 10, historyPage * 10).map(run => (
                      <div key={run.run_id} className="px-4 py-4 flex items-start justify-between gap-4 hover:bg-white/[0.02] transition-colors">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap mb-1">
                            <span className={`inline-flex items-center gap-1 text-[10px] font-bold uppercase px-2 py-0.5 rounded border ${run.status === "SUCCEEDED" ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-400" : run.status === "RUNNING" ? "bg-amber-500/10 border-amber-500/20 text-amber-400" : run.status === "PARTIAL" ? "bg-yellow-500/10 border-yellow-500/20 text-yellow-400" : "bg-red-500/10 border-red-500/20 text-red-400"}`}>
                              {run.status === "RUNNING" && <Loader className="w-2.5 h-2.5 animate-spin" />}{run.status}
                            </span>
                            <span className="text-xs text-white/60 font-medium">{run.location || run.location_display || "---"}</span>
                          </div>
                          <div className="flex items-center gap-2 text-[11px] text-white/25 flex-wrap">
                            <span>{run.categories?.join(", ") || "---"}</span><span>·</span>
                            <span>{run.sources?.join(", ") || "---"}</span><span>·</span>
                            <span>{new Date(run.started_at || run.created_at).toLocaleString()}</span>
                          </div>
                        </div>
                        <div className="text-right flex-shrink-0">
                          <div className="text-lg font-bold tabular-nums">{run.leads_collected}</div>
                          <div className="text-[10px] text-white/25">leads</div>
                          {run.leads_skipped > 0 && <div className="text-[10px] text-white/20">{run.leads_skipped} skipped</div>}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
                {history.length > 10 && (
                  <div className="px-5 py-3 border-t border-white/[0.06] flex justify-center gap-2 flex-wrap">
                    {Array.from({ length: Math.ceil(history.length / 10) }, (_, i) => (
                      <button key={i} onClick={() => setHistoryPage(i + 1)}
                        className={`px-3 py-1.5 rounded-lg text-xs transition-colors ${historyPage === i + 1 ? "bg-blue-600 text-white" : "bg-white/5 text-white/30 hover:text-white"}`}>{i + 1}</button>
                    ))}
                  </div>
                )}
              </div>
            )}
          </>)}
        </main>
      </div>

      {/* Lead detail modal */}
      {selectedLead && (
        <div className="fixed inset-0 bg-black/70 backdrop-blur-sm z-[2000] flex items-end sm:items-center justify-center p-0 sm:p-4" onClick={() => setSelectedLead(null)}>
          <div className="bg-[#13151f] border border-white/10 rounded-t-2xl sm:rounded-2xl w-full sm:max-w-2xl max-h-[92vh] sm:max-h-[88vh] overflow-hidden shadow-2xl" onClick={e => e.stopPropagation()}>
            <div className="px-5 py-4 border-b border-white/[0.06] flex items-start justify-between gap-4">
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1 flex-wrap">
                  <SourceTag source={selectedLead.source} />
                  <Badge color={selectedLead.score >= 70 ? "green" : selectedLead.score >= 40 ? "yellow" : "slate"}>Score {selectedLead.score}</Badge>
                  {selectedLead.source === "FACEBOOK" && selectedLead.raw_json?.groupName && (
                    <span className="text-[10px] text-indigo-400/70 flex items-center gap-1"><Users className="w-3 h-3" />{selectedLead.raw_json.groupName}</span>
                  )}
                </div>
                <h2 className="text-sm font-semibold text-white leading-snug">{selectedLead.title}</h2>
              </div>
              <button onClick={() => setSelectedLead(null)} className="text-white/30 hover:text-white transition-colors flex-shrink-0"><X className="w-5 h-5" /></button>
            </div>
            <div className="p-5 overflow-y-auto max-h-[calc(92vh-130px)] sm:max-h-[calc(88vh-130px)] space-y-4">
              {selectedLead.source === "FACEBOOK" && (
                <div className="bg-indigo-500/5 border border-indigo-500/20 rounded-xl p-4 flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white font-bold flex-shrink-0">
                    {(selectedLead.raw_json?.authorName || "?").charAt(0).toUpperCase()}
                  </div>
                  <div>
                    <div className="text-xs font-semibold text-white/80">{selectedLead.raw_json?.authorName || "Unknown author"}</div>
                    {selectedLead.raw_json?.authorUrl && <a href={selectedLead.raw_json.authorUrl} target="_blank" rel="noreferrer" className="text-[10px] text-indigo-400 hover:text-indigo-300">View Facebook profile →</a>}
                    <div className="flex items-center gap-3 mt-1 text-[10px] text-white/30">
                      {selectedLead.raw_json?.likesCount > 0 && <span className="flex items-center gap-1"><ThumbsUp className="w-3 h-3" />{selectedLead.raw_json.likesCount}</span>}
                      {selectedLead.raw_json?.commentsCount > 0 && <span className="flex items-center gap-1"><MessageSquare className="w-3 h-3" />{selectedLead.raw_json.commentsCount}</span>}
                    </div>
                  </div>
                </div>
              )}
              <div className="grid grid-cols-2 gap-2">
                {[
                  { label: "Source",   value: selectedLead.source },
                  { label: "Category", value: selectedLead.service_category || selectedLead.category },
                  { label: "Location", value: selectedLead.location || "---" },
                  { label: "State",    value: selectedLead.state || "---" },
                  { label: "Phone",    value: selectedLead.phone || "---", highlight: !!selectedLead.phone },
                  { label: "Email",    value: selectedLead.email || "---", highlight: !!selectedLead.email },
                  { label: "Posted",   value: selectedLead.datetime ? new Date(selectedLead.datetime).toLocaleDateString() : "---" },
                  { label: "Post ID",  value: (selectedLead.post_id || "").slice(0, 16) + "..." },
                ].map(({ label, value, highlight }) => (
                  <div key={label} className="bg-white/[0.03] rounded-xl p-3">
                    <div className="text-[10px] text-white/25 uppercase tracking-widest mb-0.5">{label}</div>
                    <div className={`text-xs font-medium truncate ${highlight ? "text-emerald-400" : "text-white/70"}`}>{value}</div>
                  </div>
                ))}
              </div>
              {(selectedLead.phone || selectedLead.email) && (
                <div className="flex gap-2 flex-wrap">
                  {selectedLead.phone && <a href={`tel:${selectedLead.phone}`} className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-semibold hover:bg-emerald-500/20 transition-colors min-w-[120px]"><Phone className="w-3.5 h-3.5" />{selectedLead.phone}</a>}
                  {selectedLead.email && <a href={`mailto:${selectedLead.email}`} className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-xl bg-violet-500/10 border border-violet-500/20 text-violet-400 text-xs font-semibold hover:bg-violet-500/20 transition-colors min-w-[120px]"><Mail className="w-3.5 h-3.5" />Email</a>}
                </div>
              )}
              <div className="bg-white/[0.03] rounded-xl p-4">
                <div className="text-[10px] text-white/25 uppercase tracking-widest mb-2">Update Status</div>
                <div className="flex gap-2 flex-wrap">
                  {["NEW","CONTACTED","QUALIFIED","WON","LOST"].map(s => (
                    <button key={s} onClick={() => updateLeadStatus(selectedLead.post_id, s)}
                      className={`px-3 py-1.5 rounded-full text-xs font-semibold border transition-all ${selectedLead.status === s ? "bg-blue-600 border-blue-500 text-white" : "bg-white/5 border-white/10 text-white/40 hover:border-white/30 hover:text-white/70"}`}>{s}</button>
                  ))}
                </div>
              </div>
              {selectedLead.post && (
                <div className="bg-white/[0.03] rounded-xl p-4">
                  <div className="text-[10px] text-white/25 uppercase tracking-widest mb-2">Post Content</div>
                  <p className="text-xs text-white/50 leading-relaxed whitespace-pre-wrap max-h-48 overflow-y-auto">{selectedLead.post}</p>
                </div>
              )}
              {selectedLead.url && (
                <a href={selectedLead.url} target="_blank" rel="noreferrer" className="flex items-center gap-2 text-xs text-blue-400 hover:text-blue-300 transition-colors">
                  <Globe className="w-3.5 h-3.5" />View original post
                </a>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}