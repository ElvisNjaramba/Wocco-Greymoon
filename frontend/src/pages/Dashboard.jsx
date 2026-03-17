import React, { useEffect, useState, useContext, useMemo, useCallback, useRef } from "react";
import axios from "axios";
import { AuthContext } from "../context/AuthContext";
import { MapContainer, TileLayer, Marker, Tooltip, useMap } from "react-leaflet";
import {
  Phone, Mail, MapPin, X, RefreshCw, Filter, Eye, Zap, Database,
  Globe, Search, TrendingUp, Clock, Loader, Target,
  Users, MessageSquare, ThumbsUp, Share2, ExternalLink,
  CheckCircle, AlertTriangle, AlertCircle, Info, ChevronDown, ChevronUp,
  Menu, LayoutGrid, List, Plus, Play, Trash2,
} from "lucide-react";

const API = "http://127.0.0.1:8000/api";

// ── Nominatim geocoder ────────────────────────────────────────
const _geocodeCache = {};
let _lastGeoReq = 0;
async function geocodeLocation(locationStr) {
  if (!locationStr) return null;
  const key = locationStr.toLowerCase().trim();
  if (_geocodeCache[key] !== undefined) return _geocodeCache[key];
  const now = Date.now();
  const wait = Math.max(0, _lastGeoReq + 1100 - now);
  if (wait > 0) await new Promise(r => setTimeout(r, wait));
  _lastGeoReq = Date.now();
  try {
    const resp = await fetch(
      `https://nominatim.openstreetmap.org/search?q=${encodeURIComponent(locationStr)}&format=json&limit=1`,
      { headers: { "Accept-Language": "en" } }
    );
    const data = await resp.json();
    if (data && data[0]) {
      const result = { lat: parseFloat(data[0].lat), lng: parseFloat(data[0].lon) };
      _geocodeCache[key] = result;
      return result;
    }
  } catch (_) {}
  _geocodeCache[key] = null;
  return null;
}

// ── Service taxonomy ──────────────────────────────────────────
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
const CAT_KEY_TO_TAXONOMY = { cleaning: "Cleaning", maintenance: "Maintenance", waste_management: "Waste Management" };

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
  if (s.includes("facebook posts")) return "facebook_posts";
  if (s.includes("facebook"))        return "facebook";
  if (s.includes("craigslist"))      return "craigslist";
  if (s.includes("google"))          return "google";
  return "system";
}

function sourceFamily(src) {
  if (src === "facebook_posts") return "facebook";
  return src;
}

const SOURCE_CFG = {
  CRAIGSLIST: { label: "CL",  badge: "bg-orange-500/10 text-orange-400 border-orange-500/20", pill: "bg-orange-500/15 border-orange-500/40 text-orange-300", pillIdle: "bg-orange-500/8 border-orange-500/20 text-orange-400/60", hero: "border-orange-500/30 bg-gradient-to-br from-orange-950/30 to-[#0f1117]", line: "bg-gradient-to-r from-transparent via-orange-400/80 to-transparent animate-pulse", nav: "bg-orange-500/10 border-orange-500/25 text-orange-400", icon: Globe,  mapColor: "text-orange-600", emoji: "🔶", name: "Craigslist" },
  FACEBOOK:   { label: "FB",  badge: "bg-indigo-500/10 text-indigo-400 border-indigo-500/20", pill: "bg-indigo-500/15 border-indigo-500/40 text-indigo-300", pillIdle: "bg-indigo-500/8 border-indigo-500/20 text-indigo-400/60", hero: "border-indigo-500/30 bg-gradient-to-br from-indigo-950/30 to-[#0f1117]", line: "bg-gradient-to-r from-transparent via-indigo-400/80 to-transparent animate-pulse", nav: "bg-indigo-500/10 border-indigo-500/25 text-indigo-400", icon: Users,  mapColor: "text-indigo-600", emoji: "🔷", name: "Facebook Groups" },
  GOOGLE:     { label: "GG",  badge: "bg-sky-500/10 text-sky-400 border-sky-500/20",           pill: "bg-sky-500/15 border-sky-500/40 text-sky-300",           pillIdle: "bg-sky-500/8 border-sky-500/20 text-sky-400/60",           hero: "border-sky-500/30 bg-gradient-to-br from-sky-950/30 to-[#0f1117]",         line: "bg-gradient-to-r from-transparent via-sky-400/80 to-transparent animate-pulse",     nav: "bg-sky-500/10 border-sky-500/25 text-sky-400",         icon: Search, mapColor: "text-sky-600",     emoji: "🔍", name: "Google Search" },
};

// ── UI atoms ──────────────────────────────────────────────────
const Badge = ({ children, color = "slate" }) => {
  const colors = { slate: "bg-slate-100 text-slate-700 border-slate-200", blue: "bg-blue-50 text-blue-700 border-blue-200", green: "bg-emerald-50 text-emerald-700 border-emerald-200", yellow: "bg-amber-50 text-amber-700 border-amber-200", red: "bg-red-50 text-red-700 border-red-200" };
  return <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-semibold border ${colors[color]}`}>{children}</span>;
};

const ScoreDot = ({ score }) => {
  const color = score >= 70 ? "#10b981" : score >= 40 ? "#f59e0b" : "#94a3b8";
  return (
    <div className="relative w-8 h-8 flex-shrink-0">
      <svg viewBox="0 0 32 32" className="w-8 h-8 -rotate-90">
        <circle cx="16" cy="16" r="12" fill="none" stroke="#e2e8f0" strokeWidth="3" />
        <circle cx="16" cy="16" r="12" fill="none" stroke={color} strokeWidth="3" strokeDasharray={`${(score / 100) * 75.4} 75.4`} strokeLinecap="round" />
      </svg>
      <span className="absolute inset-0 flex items-center justify-center text-[9px] font-bold text-slate-700">{score}</span>
    </div>
  );
};

const StatusSelect = ({ value, onChange }) => {
  const cfg = { NEW: { color: "text-blue-700 bg-blue-50 border-blue-200", label: "New" }, CONTACTED: { color: "text-amber-700 bg-amber-50 border-amber-200", label: "Contacted" }, QUALIFIED: { color: "text-emerald-700 bg-emerald-50 border-emerald-200", label: "Qualified" }, WON: { color: "text-violet-700 bg-violet-50 border-violet-200", label: "Won" }, LOST: { color: "text-red-700 bg-red-50 border-red-200", label: "Lost" } };
  const c = cfg[value] || cfg.NEW;
  return (
    <select value={value} onChange={e => onChange(e.target.value)} onClick={e => e.stopPropagation()}
      className={`text-[10px] font-bold border rounded-full px-2.5 py-1 focus:outline-none cursor-pointer ${c.color}`}>
      {Object.entries(cfg).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}
    </select>
  );
};

const SourceTag = ({ source }) => {
  const cfg = SOURCE_CFG[source];
  if (!cfg) return null;
  return <span className={`inline-flex items-center gap-1 text-[9px] font-bold uppercase px-2 py-0.5 rounded border ${cfg.badge}`}>{cfg.label}</span>;
};

const Toggle = ({ value, onChange, label, hint }) => (
  <div className="flex items-center justify-between gap-3">
    <div>
      <div className="text-xs font-semibold text-white/60">{label}</div>
      {hint && <div className="text-[10px] text-white/25 mt-0.5">{hint}</div>}
    </div>
    <button onClick={() => onChange(!value)}
      className={`flex-shrink-0 w-10 h-5 rounded-full transition-colors relative ${value ? "bg-sky-500" : "bg-white/10"}`}>
      <span className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-all ${value ? "left-5" : "left-0.5"}`} />
    </button>
  </div>
);

function NewLeadsBanner({ count, onView }) {
  if (count === 0) return null;
  return (
    <div className="fixed top-[72px] left-1/2 -translate-x-1/2 z-[1000]">
      <button onClick={onView} className="flex items-center gap-2 px-4 py-2.5 rounded-full bg-emerald-500 text-white text-xs font-bold shadow-xl shadow-emerald-500/30 hover:bg-emerald-400 transition-colors">
        <span className="w-2 h-2 rounded-full bg-white animate-pulse" />
        {count} new lead{count !== 1 ? "s" : ""} arrived — click to view
      </button>
    </div>
  );
}

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

function ActivityPanel({ scrapeStatus, visible }) {
  const logRef = useRef(null);
  const [collapsed, setCollapsed] = useState(false);
  const log = scrapeStatus?.activity_log || [];
  const isRunning = scrapeStatus?.status === "RUNNING";
  useEffect(() => { if (logRef.current && !collapsed) logRef.current.scrollTop = logRef.current.scrollHeight; }, [log.length, collapsed]);
  if (!visible || isRunning) return null;
  return (
    <div className="bg-[#0d0f18] border border-white/[0.07] rounded-2xl overflow-hidden">
      <div className="px-4 py-3 border-b border-white/[0.05] flex items-center justify-between">
        <div className="flex items-center gap-2"><span className="w-2 h-2 rounded-full bg-white/20" /><span className="text-xs font-semibold text-white/35">Last run log</span>{log.length > 0 && <span className="text-[10px] text-white/20 font-mono">{log.length} events</span>}</div>
        <button onClick={() => setCollapsed(p => !p)} className="text-white/20 hover:text-white/50 transition-colors">{collapsed ? <ChevronDown className="w-4 h-4" /> : <ChevronUp className="w-4 h-4" />}</button>
      </div>
      {!collapsed && (
        <div ref={logRef} className="overflow-y-auto max-h-56 divide-y divide-white/[0.03]">
          {log.length === 0 ? <div className="px-4 py-5 text-center text-white/20 text-xs">No events recorded</div> : (
            [...log].reverse().map((entry, i) => {
              const cfg = LOG_CFG[entry.level] || LOG_CFG.info;
              const Icon = cfg.icon;
              const src = stageSource(entry.stage);
              const family = sourceFamily(src);
              const srcCfg = SOURCE_CFG[family?.toUpperCase()];
              const ts = entry.ts ? new Date(entry.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";
              return (
                <div key={i} className="px-3 py-2.5 flex gap-2.5 items-start">
                  <Icon className={`w-3 h-3 mt-0.5 flex-shrink-0 ${cfg.text}`} />
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-1.5 flex-wrap">
                      {srcCfg && family !== "system" && <span className={`text-[8px] font-bold uppercase px-1 py-px rounded border ${srcCfg.badge}`}>{srcCfg.label}</span>}
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

function SourcePill({ sourceKey, activeSrc, log }) {
  const cfg = SOURCE_CFG[sourceKey];
  if (!cfg) return null;
  const key = sourceKey.toLowerCase();
  const isDone    = log.some(e => e.stage?.toLowerCase().includes(`${key} — complete`) || e.stage?.toLowerCase().includes(`${key} — skipped`));
  const isStarted = log.some(e => e.stage?.toLowerCase().includes(key));
  const isActive  = activeSrc === key || (key === "facebook" && activeSrc === "facebook_posts");
  const Icon = cfg.icon;
  return (
    <div className={`flex items-center gap-2 px-3 py-1.5 rounded-xl border text-xs font-semibold transition-all duration-300 ${
      isDone    ? "bg-emerald-500/10 border-emerald-500/25 text-emerald-400" :
      isActive  ? cfg.pill :
      isStarted ? cfg.pillIdle :
      "bg-white/[0.02] border-white/[0.05] text-white/20"
    }`}>
      {isDone ? <CheckCircle className="w-3.5 h-3.5" /> : isActive ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Icon className="w-3.5 h-3.5" />}
      {cfg.name}
      {isDone    && <span className="text-[10px] text-emerald-400/60 font-normal">✓ done</span>}
      {!isDone && !isStarted && <span className="text-[10px] text-white/20 font-normal">queued</span>}
      {isActive && !isDone && <span className="flex gap-0.5">{[0,150,300].map(d => <span key={d} className="w-1 h-1 rounded-full animate-bounce" style={{ animationDelay: `${d}ms`, backgroundColor: "currentColor" }} />)}</span>}
    </div>
  );
}

function ScrapeLiveDashboard({ scrapeStatus, onStop, isAborting, liveLeadCount, newLeadsBuffer }) {
  const logRef = useRef(null);
  const log          = scrapeStatus?.activity_log || [];
  const currentStage = scrapeStatus?.current_stage || "Initialising...";
  const stageDetail  = scrapeStatus?.stage_detail  || "";
  const serverSaved  = scrapeStatus?.leads_collected || 0;
  const skipped      = scrapeStatus?.leads_skipped   || 0;
  const sources      = scrapeStatus?.sources || [];
  const activeSrc    = stageSource(currentStage);
  const activeFamily = sourceFamily(activeSrc);
  const activeCfg    = SOURCE_CFG[activeFamily?.toUpperCase()];

  const displayCount = Math.max(liveLeadCount ?? 0, serverSaved);
  useEffect(() => { if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight; }, [log.length]);
  const [elapsed, setElapsed] = useState(0);
  const startTs = scrapeStatus?.started_at ? new Date(scrapeStatus.started_at) : new Date();
  useEffect(() => { const iv = setInterval(() => setElapsed(Math.floor((Date.now() - startTs) / 1000)), 1000); return () => clearInterval(iv); }, [scrapeStatus?.started_at]);
  const fmtElapsed = `${String(Math.floor(elapsed / 60)).padStart(2, "0")}:${String(elapsed % 60).padStart(2, "0")}`;

  const heroBorder = isAborting ? "border-red-500/30 bg-gradient-to-br from-red-950/30 to-[#0f1117]" : activeCfg ? activeCfg.hero : "border-blue-500/20 bg-gradient-to-br from-slate-900/60 to-[#0f1117]";
  const heroLine   = isAborting ? "bg-red-500/50" : activeCfg ? activeCfg.line : "bg-gradient-to-r from-transparent via-blue-400/60 to-transparent animate-pulse";

  return (
    <div className="space-y-3">
      <div className={`relative overflow-hidden rounded-2xl border transition-all duration-500 ${heroBorder}`}>
        <div className={`h-[2px] w-full transition-all duration-500 ${heroLine}`} />
        <div className="px-4 py-4 lg:px-6 lg:py-5">
          <div className="flex items-start justify-between gap-3 mb-4">
            <div className="flex items-start gap-3 min-w-0">
              <div className="flex-shrink-0">
                {isAborting ? (
                  <div className="w-9 h-9 rounded-full bg-red-500/15 border border-red-500/30 flex items-center justify-center"><X className="w-4 h-4 text-red-400" /></div>
                ) : activeCfg ? (
                  <div className="relative w-9 h-9">
                    <div className={`absolute inset-0 rounded-full animate-ping opacity-30 ${activeCfg.pillIdle}`} />
                    <div className={`relative w-9 h-9 rounded-full flex items-center justify-center ${activeCfg.pillIdle}`}><activeCfg.icon className="w-4 h-4" /></div>
                  </div>
                ) : (
                  <div className="w-9 h-9 rounded-full bg-blue-500/15 border border-blue-500/25 flex items-center justify-center"><Loader className="w-4 h-4 text-blue-400 animate-spin" /></div>
                )}
              </div>
              <div className="min-w-0">
                <div className={`text-[10px] font-bold uppercase tracking-widest mb-0.5 ${isAborting ? "text-red-400" : activeCfg ? activeCfg.nav.split(" ").pop() : "text-blue-400"}`}>
                  {isAborting ? "⛔ Stopping run" : activeCfg ? `${activeCfg.emoji} Scraping ${activeCfg.name}` : "⚙️ Initialising pipeline"}
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
              <button onClick={onStop} disabled={isAborting} className="flex items-center gap-1.5 px-3 py-1.5 rounded-xl bg-red-500/10 border border-red-500/20 text-red-400 text-xs font-semibold hover:bg-red-500/20 transition-all disabled:opacity-40">
                <X className="w-3 h-3" /><span className="hidden sm:inline">{isAborting ? "Stopping..." : "Stop"}</span>
              </button>
            </div>
          </div>
          {sources.length > 0 && (
            <div className="flex gap-2 mb-4 flex-wrap">
              {sources.includes("craigslist") && <SourcePill sourceKey="CRAIGSLIST" activeSrc={activeSrc} log={log} />}
              {sources.includes("facebook")   && <SourcePill sourceKey="FACEBOOK"   activeSrc={activeSrc} log={log} />}
              {sources.includes("google")     && <SourcePill sourceKey="GOOGLE"     activeSrc={activeSrc} log={log} />}
            </div>
          )}
          <div className="grid grid-cols-3 gap-2">
            <div className="bg-white/[0.03] border border-white/[0.05] rounded-xl px-3 py-2.5">
              <div className="text-[9px] text-white/25 uppercase tracking-widest mb-1">Leads saved</div>
              <div className="text-xl font-bold tabular-nums text-emerald-400">{displayCount}</div>
              {displayCount > 0 && <div className="text-[9px] text-emerald-400/40 mt-0.5 flex items-center gap-1"><span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />live</div>}
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
          {newLeadsBuffer && newLeadsBuffer.length > 0 && (
            <div className="mt-3 border border-emerald-500/15 rounded-xl overflow-hidden">
              <div className="px-3 py-2 bg-emerald-500/5 border-b border-emerald-500/10 flex items-center gap-2">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                <span className="text-[10px] font-semibold text-emerald-400 uppercase tracking-widest">Latest incoming leads</span>
              </div>
              <div className="divide-y divide-white/[0.04] max-h-36 overflow-y-auto">
                {newLeadsBuffer.slice(0, 5).map(lead => (
                  <div key={lead.post_id} className="px-3 py-2 flex items-start gap-2">
                    <SourceTag source={lead.source} />
                    <div className="flex-1 min-w-0">
                      <p className="text-[11px] text-white/70 font-medium truncate">{lead.title}</p>
                      {lead.location && <p className="text-[10px] text-white/30 mt-0.5 flex items-center gap-1"><MapPin className="w-2.5 h-2.5" />{lead.location}</p>}
                    </div>
                    {lead.phone && <Phone className="w-3 h-3 text-emerald-400/60 flex-shrink-0 mt-0.5" />}
                  </div>
                ))}
                {newLeadsBuffer.length > 5 && <div className="px-3 py-1.5 text-[10px] text-white/25 text-center">+{newLeadsBuffer.length - 5} more</div>}
              </div>
            </div>
          )}
        </div>
      </div>
      <div className="bg-[#07090f] border border-white/[0.06] rounded-2xl overflow-hidden">
        <div className="px-4 py-3 border-b border-white/[0.05] flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
          <span className="text-xs font-semibold text-white/50">Live event stream</span>
          <span className="font-mono text-[10px] text-white/20 ml-auto">{log.length} events</span>
        </div>
        <div ref={logRef} className="overflow-y-auto h-56 divide-y divide-white/[0.03]">
          {log.length === 0 ? (
            <div className="h-full flex flex-col items-center justify-center gap-3 text-white/20"><Loader className="w-5 h-5 animate-spin" /><span className="text-xs">Waiting for first pipeline event...</span></div>
          ) : log.map((entry, i) => {
            const cfg = LOG_CFG[entry.level] || LOG_CFG.info;
            const Icon = cfg.icon;
            const src = stageSource(entry.stage);
            const family = sourceFamily(src);
            const srcCfg = SOURCE_CFG[family?.toUpperCase()];
            const ts = entry.ts ? new Date(entry.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";
            const isNewest = i === log.length - 1;
            return (
              <div key={i} className={`px-4 py-2.5 flex gap-2.5 items-start transition-colors ${isNewest ? cfg.bg : ""}`} style={isNewest ? { borderLeft: "2px solid" } : { borderLeft: "2px solid transparent" }}>
                <div className="flex-shrink-0 pt-0.5">
                  {srcCfg && family !== "system" ? (
                    <span className={`text-[8px] font-bold uppercase px-1 py-px rounded border ${srcCfg.badge}`}>{srcCfg.label}</span>
                  ) : (
                    <span className="text-[8px] font-bold uppercase px-1 py-px rounded bg-white/5 text-white/25 border border-white/10">SYS</span>
                  )}
                </div>
                <Icon className={`w-3 h-3 mt-0.5 flex-shrink-0 ${isNewest ? cfg.text : "text-white/20"}`} />
                <div className="flex-1 min-w-0">
                  <span className={`text-[10px] font-semibold leading-tight ${isNewest ? cfg.text : "text-white/35"}`}>{entry.stage}</span>
                  <p className={`text-[10px] leading-relaxed mt-px ${isNewest ? "text-white/60" : "text-white/22"}`}>{entry.detail}</p>
                </div>
                <span className="text-[9px] text-white/15 font-mono flex-shrink-0 mt-0.5 tabular-nums">{ts}</span>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

const FacebookLeadCard = ({ lead, onSelect, updateStatus }) => {
  const authorInitial = (lead.raw_json?.authorName || lead.title || "?").charAt(0).toUpperCase();
  const groupName  = lead.fb_group_name || lead.raw_json?.groupName || lead.raw_json?.group || "";
  const groupUrl   = lead.fb_group_url  || lead.raw_json?.groupUrl  || "";
  const likes      = lead.raw_json?.likesCount    || 0;
  const comments   = lead.raw_json?.commentsCount || 0;
  const shares     = lead.raw_json?.sharesCount   || 0;
  const authorName = lead.raw_json?.authorName    || lead.raw_json?.author || "";
  const snippet    = (lead.post || "").slice(0, 200) + ((lead.post || "").length > 200 ? "..." : "");
  return (
    <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl p-3 lg:p-4 hover:bg-white/[0.04] hover:border-indigo-500/20 transition-all group cursor-pointer" onClick={() => onSelect(lead)}>
      <div className="flex items-start gap-3 mb-3">
        <div className="w-8 h-8 rounded-full bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white text-sm font-bold flex-shrink-0">{authorInitial}</div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap mb-0.5">
            {authorName && <span className="text-xs font-semibold text-white/80 truncate max-w-[140px]">{authorName}</span>}
            <SourceTag source="FACEBOOK" /><ScoreDot score={lead.score} />
          </div>
          {groupName && (
            <div className="flex items-center gap-1 text-[10px] text-indigo-400/70">
              <Users className="w-2.5 h-2.5 flex-shrink-0" />
              {groupUrl ? <a href={groupUrl} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()} className="truncate hover:text-indigo-300 underline underline-offset-2">{groupName}</a> : <span className="truncate">{groupName}</span>}
            </div>
          )}
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
          {likes    > 0 && <span className="flex items-center gap-1"><ThumbsUp className="w-3 h-3" />{likes}</span>}
          {comments > 0 && <span className="flex items-center gap-1"><MessageSquare className="w-3 h-3" />{comments}</span>}
          {shares   > 0 && <span className="flex items-center gap-1"><Share2 className="w-3 h-3" />{shares}</span>}
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

function LeadsTable({ leads, onSelect, updateStatus }) {
  return (
    <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden overflow-x-auto">
      <table className="w-full min-w-[600px]">
        <thead>
          <tr className="border-b border-white/[0.06]">
            {["Title","Src","Category","Location","Contact","Status","Score","Date",""].map(h => (
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

function Pagination({ page, totalPages, total, perPage, onPage }) {
  if (totalPages <= 1) return null;
  const from = (page - 1) * perPage + 1;
  const to   = Math.min(page * perPage, total);
  const delta = 2;
  const pages = [];
  for (let i = Math.max(1, page - delta); i <= Math.min(totalPages, page + delta); i++) pages.push(i);
  return (
    <div className="mt-3 px-4 py-3 bg-white/[0.03] border border-white/[0.06] rounded-2xl flex flex-col sm:flex-row items-center justify-between gap-2">
      <span className="text-[11px] text-white/25 order-2 sm:order-1">{from}–{to} of {total} leads</span>
      <div className="flex items-center gap-1 flex-wrap justify-center order-1 sm:order-2">
        <button onClick={() => onPage(1)} disabled={page === 1} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">«</button>
        <button onClick={() => onPage(page - 1)} disabled={page === 1} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">‹</button>
        {pages[0] > 1 && <span className="px-1.5 text-white/20 text-xs">...</span>}
        {pages.map(p => <button key={p} onClick={() => onPage(p)} className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${page === p ? "bg-blue-600 text-white" : "bg-white/5 text-white/40 hover:text-white"}`}>{p}</button>)}
        {pages[pages.length - 1] < totalPages && <span className="px-1.5 text-white/20 text-xs">...</span>}
        <button onClick={() => onPage(page + 1)} disabled={page === totalPages} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">›</button>
        <button onClick={() => onPage(totalPages)} disabled={page === totalPages} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">»</button>
      </div>
    </div>
  );
}

function isRecentlyScraped(lastScraped) {
  if (!lastScraped) return false;
  return (Date.now() - new Date(lastScraped).getTime()) < 24 * 60 * 60 * 1000;
}

const US_STATES = ["Alabama","Alaska","Arizona","Arkansas","California","Colorado","Connecticut","Delaware","Florida","Georgia","Hawaii","Idaho","Illinois","Indiana","Iowa","Kansas","Kentucky","Louisiana","Maine","Maryland","Massachusetts","Michigan","Minnesota","Mississippi","Missouri","Montana","Nebraska","Nevada","New Hampshire","New Jersey","New Mexico","New York","North Carolina","North Dakota","Ohio","Oklahoma","Oregon","Pennsylvania","Rhode Island","South Carolina","South Dakota","Tennessee","Texas","Utah","Vermont","Virginia","Washington","West Virginia","Wisconsin","Wyoming"];
const ALL_SOURCES = [
  { key: "craigslist", label: "Craigslist",      emoji: "🔶" },
  { key: "facebook",   label: "Facebook Groups", emoji: "🔷" },
  { key: "google",     label: "Google",          emoji: "🔍" },
];

// ── Helper: format city display name with state ───────────────
function cityDisplayName(city) {
  if (!city) return "";
  if (city.state) return `${city.name}, ${city.state}`;
  return city.name;
}

// ── Facebook Groups Management Panel ─────────────────────────
function FbGroupsPanel({ headers, scrapeStatus, scraping, onScrapeDone }) {
  const [groups, setGroups]               = useState([]);
  const [loading, setLoading]             = useState(false);
  const [addText, setAddText]             = useState("");
  const [adding, setAdding]               = useState(false);
  const [addError, setAddError]           = useState("");
  const [selectedUrls, setSelectedUrls]   = useState(new Set());
  const [maxPosts, setMaxPosts]           = useState(50);
  const [scrapeLoading, setScrapeLoading] = useState(false);
  const [selectedGroup, setSelectedGroup]             = useState(null);
  const [groupLeads, setGroupLeads]                   = useState([]);
  const [groupLeadsTotal, setGroupLeadsTotal]         = useState(0);
  const [groupLeadsPage, setGroupLeadsPage]           = useState(1);
  const [groupLeadsTotalPages, setGroupLeadsTotalPages] = useState(1);
  const [groupLeadsLoading, setGroupLeadsLoading]     = useState(false);
  const [selectedLead, setSelectedLead]   = useState(null);

  const fetchGroups = useCallback(async () => {
    setLoading(true);
    try { const r = await axios.get(`${API}/fb-groups/`, { headers }); setGroups(r.data.groups || []); }
    catch (_) {}
    setLoading(false);
  }, [headers]);

  useEffect(() => { fetchGroups(); }, [fetchGroups]);

  useEffect(() => {
    if (scrapeStatus?.status && scrapeStatus.status !== "RUNNING") fetchGroups();
  }, [scrapeStatus?.status]);

  const handleAdd = async () => {
    const lines = addText.split(/[\n,]+/).map(l => l.trim()).filter(l => l.startsWith("http"));
    if (!lines.length) { setAddError("Enter at least one valid Facebook group URL (must start with http)."); return; }
    setAddError(""); setAdding(true);
    try {
      const r = await axios.post(`${API}/fb-groups/add/`, { group_urls: lines }, { headers });
      setAddText("");
      await fetchGroups();
      setAddError(`✓ Added ${r.data.added} group(s)${r.data.already_exists ? `, ${r.data.already_exists} already existed` : ""}.`);
    } catch (e) {
      setAddError(e.response?.data?.error || "Failed to add groups.");
    }
    setAdding(false);
  };

  const handleScrapeSelected = async () => {
    if (selectedUrls.size === 0) return;
    setScrapeLoading(true);
    try {
      await axios.post(`${API}/fb-groups/scrape/`, {
        group_urls: [...selectedUrls],
        max_posts_per_group: maxPosts,
      }, { headers });
      setSelectedUrls(new Set());
      onScrapeDone && onScrapeDone();
    } catch (e) {
      alert(e.response?.data?.error || "Failed to start scrape.");
    }
    setScrapeLoading(false);
  };

  const handleDelete = async (groupUrl) => {
    if (!window.confirm("Remove this group? Its leads remain, but it won't appear in the group registry.")) return;
    try {
      await axios.delete(`${API}/fb-groups/delete/`, { headers, data: { group_url: groupUrl } });
      if (selectedGroup?.group_url === groupUrl) { setSelectedGroup(null); setGroupLeads([]); }
      setSelectedUrls(s => { const n = new Set(s); n.delete(groupUrl); return n; });
      fetchGroups();
    } catch (_) {}
  };

  const fetchGroupLeads = useCallback(async (groupUrl, p = 1) => {
    setGroupLeadsLoading(true);
    try {
      const r = await axios.get(`${API}/fb-groups/leads/`, { headers, params: { group_url: groupUrl, page: p, page_size: 50 } });
      setGroupLeads(r.data.results || []); setGroupLeadsTotal(r.data.total || 0);
      setGroupLeadsPage(p); setGroupLeadsTotalPages(r.data.total_pages || 1);
    } catch (_) {}
    setGroupLeadsLoading(false);
  }, [headers]);

  const updateLeadStatus = useCallback(async (postId, status) => {
    try {
      await axios.patch(`${API}/leads/${postId}/status/`, { status }, { headers });
      setGroupLeads(prev => prev.map(l => l.post_id === postId ? { ...l, status } : l));
    } catch (_) {}
  }, [headers]);

  const toggleSelect = (url) => {
    setSelectedUrls(s => {
      const n = new Set(s);
      n.has(url) ? n.delete(url) : n.add(url);
      return n;
    });
  };

  const selectAll = () => setSelectedUrls(new Set(groups.map(g => g.group_url)));
  const clearAll  = () => setSelectedUrls(new Set());

  return (
    <div className="space-y-4">
      {/* Add Groups */}
      <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
        <div className="px-4 py-3.5 border-b border-white/[0.06] flex items-center gap-2">
          <Plus className="w-4 h-4 text-indigo-400" />
          <span className="text-sm font-semibold">Add Facebook Groups</span>
        </div>
        <div className="p-4 space-y-3">
          <textarea
            value={addText}
            onChange={e => setAddText(e.target.value)}
            placeholder={"https://www.facebook.com/groups/123456789\nhttps://www.facebook.com/groups/anothergroup\nOne URL per line"}
            rows={4}
            spellCheck="false"
            autoComplete="off"
            className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-2.5 text-xs text-white placeholder-white/20 focus:outline-none focus:border-indigo-500/60 transition-all resize-none leading-relaxed font-mono"
          />
          {addError && (
            <p className={`text-[11px] leading-relaxed ${addError.startsWith("✓") ? "text-emerald-400" : "text-red-400"}`}>{addError}</p>
          )}
          <button
            onClick={handleAdd}
            disabled={adding || !addText.trim()}
            className="w-full py-2.5 rounded-xl bg-indigo-600/80 border border-indigo-500/40 text-white text-xs font-semibold hover:bg-indigo-500 transition-all disabled:opacity-30 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            {adding ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Plus className="w-3.5 h-3.5" />}
            {adding ? "Adding..." : "Add Groups to Registry"}
          </button>
        </div>
      </div>

      {/* Group list + scrape controls */}
      <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
        <div className="px-4 py-3.5 border-b border-white/[0.06] flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-2">
            <Users className="w-4 h-4 text-indigo-400" />
            <span className="text-sm font-semibold">Group Registry</span>
            <span className="text-[11px] text-white/30 bg-white/5 px-2 py-0.5 rounded-full">{groups.length}</span>
            {selectedUrls.size > 0 && <span className="text-[11px] text-indigo-300 bg-indigo-500/15 border border-indigo-500/25 px-2 py-0.5 rounded-full">{selectedUrls.size} selected</span>}
          </div>
          <div className="flex items-center gap-2">
            {groups.length > 0 && (
              <button onClick={selectedUrls.size === groups.length ? clearAll : selectAll} className="text-[11px] text-white/30 hover:text-white/60 transition-colors">
                {selectedUrls.size === groups.length ? "Deselect all" : "Select all"}
              </button>
            )}
            <button onClick={fetchGroups} className="text-white/30 hover:text-white/60 transition-colors"><RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} /></button>
          </div>
        </div>

        {selectedUrls.size > 0 && (
          <div className="px-4 py-3 bg-indigo-500/5 border-b border-indigo-500/15 flex flex-col sm:flex-row items-start sm:items-center gap-3">
            <div className="flex-1 space-y-1">
              <div className="flex items-center justify-between">
                <label className="text-[11px] font-semibold uppercase tracking-widest text-indigo-300/70">Posts per group</label>
                <span className="text-xs font-bold text-indigo-300 tabular-nums">{maxPosts}</span>
              </div>
              <input
                type="number"
                min={5}
                max={500}
                value={maxPosts}
                onChange={e => setMaxPosts(Math.max(5, Math.min(500, parseInt(e.target.value) || 50)))}
                className="w-full bg-white/5 border border-white/10 rounded-lg px-3 py-1.5 text-xs text-white focus:outline-none focus:border-indigo-500/60 tabular-nums"
              />
              <p className="text-[10px] text-white/20">Already-scraped posts are always skipped</p>
            </div>
            <button
              onClick={handleScrapeSelected}
              disabled={scrapeLoading || scraping}
              className="flex-shrink-0 flex items-center gap-2 px-5 py-2.5 rounded-xl bg-indigo-600 border border-indigo-500/50 text-white text-xs font-bold hover:bg-indigo-500 transition-all disabled:opacity-40 disabled:cursor-not-allowed shadow-lg shadow-indigo-500/20"
            >
              {scrapeLoading ? <Loader className="w-3.5 h-3.5 animate-spin" /> : <Play className="w-3.5 h-3.5" />}
              {scrapeLoading ? "Starting..." : scraping ? "Scrape running..." : `Scrape ${selectedUrls.size} group${selectedUrls.size !== 1 ? "s" : ""}`}
            </button>
          </div>
        )}

        {groups.length === 0 ? (
          <div className="py-12 flex flex-col items-center gap-3 text-white/20">
            <Users className="w-8 h-8 text-white/10" />
            <span className="text-sm">No groups added yet</span>
            <p className="text-[11px] text-white/15 text-center max-w-xs">Add Facebook group URLs above and they'll appear here for scraping</p>
          </div>
        ) : (
          <div className="divide-y divide-white/[0.04]">
            {groups.map(g => {
              const isRecent   = isRecentlyScraped(g.last_scraped);
              const isSelected = selectedUrls.has(g.group_url);
              return (
                <div
                  key={g.group_url}
                  className={`px-4 py-3 flex items-center gap-3 hover:bg-white/[0.02] transition-colors ${
                    selectedGroup?.group_url === g.group_url ? "bg-indigo-500/5 border-l-2 border-indigo-500" : "border-l-2 border-transparent"
                  }`}
                >
                  <button
                    onClick={() => toggleSelect(g.group_url)}
                    className={`w-4 h-4 rounded border-2 flex-shrink-0 flex items-center justify-center transition-all ${
                      isSelected ? "border-indigo-500 bg-indigo-500" : "border-white/20 hover:border-white/40"
                    }`}
                  >
                    {isSelected && <svg className="w-2.5 h-2.5 text-white" fill="currentColor" viewBox="0 0 12 12"><path d="M10 3L5 8.5 2 5.5l-1 1L5 10.5l6-7-1-0.5z"/></svg>}
                  </button>

                  <div
                    className="flex-1 min-w-0 flex items-center gap-3 cursor-pointer"
                    onClick={() => {
                      if (selectedGroup?.group_url === g.group_url) { setSelectedGroup(null); setGroupLeads([]); }
                      else { setSelectedGroup(g); fetchGroupLeads(g.group_url, 1); }
                    }}
                  >
                    <div className="relative w-8 h-8 flex-shrink-0">
                      <div className="w-8 h-8 rounded-full bg-gradient-to-br from-indigo-500/30 to-purple-600/30 border border-indigo-500/20 flex items-center justify-center text-indigo-300 text-xs font-bold">
                        {(g.group_name || "?").charAt(0).toUpperCase()}
                      </div>
                      {isRecent && <span className="absolute -top-0.5 -right-0.5 w-2.5 h-2.5 rounded-full bg-emerald-400 border-2 border-[#0f1117]" title="Scraped in last 24h" />}
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span className="text-sm font-medium text-white/80 truncate">{g.group_name || g.group_url}</span>
                        {isRecent && <span className="text-[9px] font-bold uppercase px-1.5 py-px rounded bg-emerald-500/10 border border-emerald-500/20 text-emerald-400">new</span>}
                      </div>
                      <div className="flex items-center gap-3 mt-0.5 flex-wrap">
                        <span className="text-[11px] text-white/30">{g.post_count} posts scraped</span>
                        {g.last_scraped && <span className="text-[11px] text-white/20">{new Date(g.last_scraped).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}</span>}
                        {!g.last_scraped && <span className="text-[11px] text-white/15 italic">never scraped</span>}
                      </div>
                    </div>
                  </div>

                  <div className="flex items-center gap-1 flex-shrink-0">
                    <a href={g.group_url} target="_blank" rel="noopener noreferrer" onClick={e => e.stopPropagation()} className="text-white/20 hover:text-indigo-400 transition-colors p-1.5 rounded-lg hover:bg-indigo-500/10">
                      <ExternalLink className="w-3.5 h-3.5" />
                    </a>
                    <button onClick={() => handleDelete(g.group_url)} className="text-white/20 hover:text-red-400 transition-colors p-1.5 rounded-lg hover:bg-red-500/10">
                      <Trash2 className="w-3.5 h-3.5" />
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Group drill-down */}
      {selectedGroup && (
        <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
          <div className="px-4 py-3.5 border-b border-white/[0.06] flex items-center justify-between gap-3">
            <div className="flex items-center gap-3 min-w-0">
              <div className="w-8 h-8 rounded-full bg-gradient-to-br from-indigo-500/30 to-purple-600/30 border border-indigo-500/20 flex items-center justify-center text-indigo-300 text-xs font-bold flex-shrink-0">
                {(selectedGroup.group_name || "?").charAt(0).toUpperCase()}
              </div>
              <div className="min-w-0">
                <div className="text-sm font-semibold text-white/90 truncate">{selectedGroup.group_name || selectedGroup.group_url}</div>
                <div className="text-[11px] text-white/35 mt-0.5">
                  {groupLeadsLoading ? "Loading…" : `${groupLeadsTotal} post${groupLeadsTotal !== 1 ? "s" : ""} stored`}
                  {groupLeadsTotalPages > 1 && !groupLeadsLoading && ` · page ${groupLeadsPage} of ${groupLeadsTotalPages}`}
                </div>
              </div>
            </div>
            <div className="flex items-center gap-2 flex-shrink-0">
              <a href={selectedGroup.group_url} target="_blank" rel="noopener noreferrer" className="text-white/20 hover:text-indigo-400 transition-colors p-1.5 rounded-lg hover:bg-indigo-500/10">
                <ExternalLink className="w-3.5 h-3.5" />
              </a>
              <button onClick={() => { setSelectedGroup(null); setGroupLeads([]); setGroupLeadsPage(1); }} className="text-white/30 hover:text-white/60 p-1.5 rounded-lg hover:bg-white/5 transition-colors">
                <X className="w-4 h-4" />
              </button>
            </div>
          </div>

          {groupLeadsLoading ? (
            <div className="py-12 flex flex-col items-center justify-center gap-3">
              <Loader className="w-6 h-6 text-indigo-400 animate-spin" />
              <span className="text-[11px] text-white/30">Loading posts…</span>
            </div>
          ) : groupLeads.length === 0 ? (
            <div className="py-12 flex flex-col items-center gap-3 text-white/25">
              <MessageSquare className="w-8 h-8 text-white/10" />
              <span className="text-sm">No posts scraped from this group yet</span>
              <p className="text-[11px] text-white/15 text-center max-w-xs">Select this group and click Scrape to pull posts from it</p>
            </div>
          ) : (
            <>
              <div className="divide-y divide-white/[0.04]">
                {groupLeads.map(lead => {
                  const authorName = lead.raw_json?.authorName || lead.raw_json?.author || "";
                  const authorInitial = (authorName || lead.title || "?").charAt(0).toUpperCase();
                  const snippet = (lead.post || "").slice(0, 220) + ((lead.post || "").length > 220 ? "…" : "");
                  const postDate = lead.datetime ? new Date(lead.datetime).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" }) : null;
                  const likes    = lead.raw_json?.likesCount    || 0;
                  const comments = lead.raw_json?.commentsCount || 0;
                  return (
                    <div
                      key={lead.post_id}
                      className="px-4 py-3.5 hover:bg-white/[0.02] transition-colors cursor-pointer group"
                      onClick={() => setSelectedLead(lead)}
                    >
                      <div className="flex items-start gap-3 mb-2">
                        <div className="w-7 h-7 rounded-full bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white text-xs font-bold flex-shrink-0 mt-0.5">
                          {authorInitial}
                        </div>
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap">
                            {authorName && <span className="text-xs font-semibold text-white/75 truncate max-w-[160px]">{authorName}</span>}
                            {postDate && <span className="text-[10px] text-white/25 font-mono">{postDate}</span>}
                            <div className="ml-auto flex items-center gap-2 flex-shrink-0">
                              <ScoreDot score={lead.score} />
                            </div>
                          </div>
                        </div>
                        <div onClick={e => e.stopPropagation()} className="flex-shrink-0">
                          <StatusSelect value={lead.status} onChange={v => updateLeadStatus(lead.post_id, v)} />
                        </div>
                      </div>

                      {snippet && (
                        <p className="text-xs text-white/50 leading-relaxed mb-2.5 whitespace-pre-wrap ml-10">{snippet}</p>
                      )}

                      <div className="flex items-center gap-3 ml-10 flex-wrap">
                        {lead.phone && (
                          <a href={`tel:${lead.phone}`} onClick={e => e.stopPropagation()} className="inline-flex items-center gap-1 text-[11px] text-emerald-400 font-semibold hover:text-emerald-300 transition-colors">
                            <Phone className="w-3 h-3" />{lead.phone}
                          </a>
                        )}
                        {lead.email && (
                          <a href={`mailto:${lead.email}`} onClick={e => e.stopPropagation()} className="inline-flex items-center gap-1 text-[11px] text-violet-400 font-semibold hover:text-violet-300 transition-colors">
                            <Mail className="w-3 h-3" />{lead.email}
                          </a>
                        )}
                        {lead.location && (
                          <span className="text-[10px] text-white/25 flex items-center gap-1">
                            <MapPin className="w-2.5 h-2.5" />{lead.location}
                          </span>
                        )}
                        {likes > 0 && <span className="text-[10px] text-white/20 flex items-center gap-1"><ThumbsUp className="w-2.5 h-2.5" />{likes}</span>}
                        {comments > 0 && <span className="text-[10px] text-white/20 flex items-center gap-1"><MessageSquare className="w-2.5 h-2.5" />{comments}</span>}
                        <span className="ml-auto text-[10px] text-indigo-400/40 opacity-0 group-hover:opacity-100 transition-opacity">View full post →</span>
                      </div>
                    </div>
                  );
                })}
              </div>

              {groupLeadsTotalPages > 1 && (
                <div className="px-4 py-3 border-t border-white/[0.06] flex flex-col sm:flex-row items-center justify-between gap-2">
                  <span className="text-[11px] text-white/25 order-2 sm:order-1">
                    {((groupLeadsPage - 1) * 50) + 1}–{Math.min(groupLeadsPage * 50, groupLeadsTotal)} of {groupLeadsTotal} posts
                  </span>
                  <div className="flex items-center gap-1 order-1 sm:order-2">
                    <button onClick={() => fetchGroupLeads(selectedGroup.group_url, 1)} disabled={groupLeadsPage === 1} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">«</button>
                    <button onClick={() => fetchGroupLeads(selectedGroup.group_url, groupLeadsPage - 1)} disabled={groupLeadsPage <= 1} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">‹</button>
                    {Array.from({ length: groupLeadsTotalPages }, (_, i) => i + 1)
                      .filter(p => p === 1 || p === groupLeadsTotalPages || Math.abs(p - groupLeadsPage) <= 2)
                      .reduce((acc, p, idx, arr) => {
                        if (idx > 0 && p - arr[idx - 1] > 1) acc.push("...");
                        acc.push(p);
                        return acc;
                      }, [])
                      .map((p, i) => p === "..." ? (
                        <span key={`ellipsis-${i}`} className="px-1.5 text-white/20 text-xs">…</span>
                      ) : (
                        <button key={p} onClick={() => fetchGroupLeads(selectedGroup.group_url, p)} className={`px-3 py-1.5 rounded-lg text-xs font-medium transition-colors ${groupLeadsPage === p ? "bg-indigo-600 text-white" : "bg-white/5 text-white/40 hover:text-white"}`}>{p}</button>
                      ))
                    }
                    <button onClick={() => fetchGroupLeads(selectedGroup.group_url, groupLeadsPage + 1)} disabled={groupLeadsPage >= groupLeadsTotalPages} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">›</button>
                    <button onClick={() => fetchGroupLeads(selectedGroup.group_url, groupLeadsTotalPages)} disabled={groupLeadsPage === groupLeadsTotalPages} className="px-2.5 py-1.5 rounded-lg bg-white/5 text-xs text-white/40 hover:text-white disabled:opacity-20 transition-colors">»</button>
                  </div>
                </div>
              )}
            </>
          )}
        </div>
      )}

      {selectedLead && (
        <LeadDetailModal lead={selectedLead} onClose={() => setSelectedLead(null)} updateStatus={updateLeadStatus} />
      )}
    </div>
  );
}

// ── Lead Detail Modal ─────────────────────────────────────────
function LeadDetailModal({ lead, onClose, updateStatus }) {
  return (
    <div className="fixed inset-0 bg-black/70 backdrop-blur-sm z-[2000] flex items-end sm:items-center justify-center p-0 sm:p-4" onClick={onClose}>
      <div className="bg-[#13151f] border border-white/10 rounded-t-2xl sm:rounded-2xl w-full sm:max-w-2xl max-h-[92vh] sm:max-h-[88vh] overflow-hidden shadow-2xl" onClick={e => e.stopPropagation()}>
        <div className="px-5 py-4 border-b border-white/[0.06] flex items-start justify-between gap-4">
          <div className="flex-1 min-w-0">
            <div className="flex items-center gap-2 mb-1 flex-wrap">
              <SourceTag source={lead.source} />
              <Badge color={lead.score >= 70 ? "green" : lead.score >= 40 ? "yellow" : "slate"}>Score {lead.score}</Badge>
              {lead.source === "FACEBOOK" && (lead.fb_group_name || lead.raw_json?.groupName) && (
                <span className="text-[10px] text-indigo-400/70 flex items-center gap-1">
                  <Users className="w-3 h-3" />
                  {lead.fb_group_url
                    ? <a href={lead.fb_group_url} target="_blank" rel="noreferrer" className="hover:text-indigo-300 underline underline-offset-2">{lead.fb_group_name || lead.raw_json?.groupName}</a>
                    : (lead.fb_group_name || lead.raw_json?.groupName)}
                </span>
              )}
            </div>
            <h2 className="text-sm font-semibold text-white leading-snug">{lead.title}</h2>
          </div>
          <button onClick={onClose} className="text-white/30 hover:text-white transition-colors flex-shrink-0"><X className="w-5 h-5" /></button>
        </div>
        <div className="p-5 overflow-y-auto max-h-[calc(92vh-130px)] sm:max-h-[calc(88vh-130px)] space-y-4">
          {lead.source === "FACEBOOK" && (
            <div className="bg-indigo-500/5 border border-indigo-500/20 rounded-xl p-4 flex items-center gap-3">
              <div className="w-10 h-10 rounded-full bg-gradient-to-br from-indigo-500 to-purple-600 flex items-center justify-center text-white font-bold flex-shrink-0">
                {(lead.raw_json?.authorName || "?").charAt(0).toUpperCase()}
              </div>
              <div>
                <div className="text-xs font-semibold text-white/80">{lead.raw_json?.authorName || "Unknown author"}</div>
                {lead.raw_json?.authorUrl && <a href={lead.raw_json.authorUrl} target="_blank" rel="noreferrer" className="text-[10px] text-indigo-400 hover:text-indigo-300">View Facebook profile →</a>}
              </div>
            </div>
          )}
          <div className="grid grid-cols-2 gap-2">
            {[
              { label: "Source",   value: lead.source },
              { label: "Category", value: lead.service_category || lead.category },
              { label: "Location", value: lead.location || "---" },
              { label: "State",    value: lead.state    || "---" },
              { label: "Phone",    value: lead.phone    || "---", highlight: !!lead.phone },
              { label: "Email",    value: lead.email    || "---", highlight: !!lead.email },
              { label: "Posted",   value: lead.datetime ? new Date(lead.datetime).toLocaleDateString() : "---" },
              { label: "Post ID",  value: (lead.post_id || "").slice(0, 16) + "..." },
            ].map(({ label, value, highlight }) => (
              <div key={label} className="bg-white/[0.03] rounded-xl p-3">
                <div className="text-[10px] text-white/25 uppercase tracking-widest mb-0.5">{label}</div>
                <div className={`text-xs font-medium truncate ${highlight ? "text-emerald-400" : "text-white/70"}`}>{value}</div>
              </div>
            ))}
          </div>
          {(lead.phone || lead.email) && (
            <div className="flex gap-2 flex-wrap">
              {lead.phone && <a href={`tel:${lead.phone}`} className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-xl bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-semibold hover:bg-emerald-500/20 transition-colors min-w-[120px]"><Phone className="w-3.5 h-3.5" />{lead.phone}</a>}
              {lead.email && <a href={`mailto:${lead.email}`} className="flex-1 flex items-center justify-center gap-2 py-2.5 rounded-xl bg-violet-500/10 border border-violet-500/20 text-violet-400 text-xs font-semibold hover:bg-violet-500/20 transition-colors min-w-[120px]"><Mail className="w-3.5 h-3.5" />Email</a>}
            </div>
          )}
          <div className="bg-white/[0.03] rounded-xl p-4">
            <div className="text-[10px] text-white/25 uppercase tracking-widest mb-2">Update Status</div>
            <div className="flex gap-2 flex-wrap">
              {["NEW","CONTACTED","QUALIFIED","WON","LOST"].map(s => <button key={s} onClick={() => updateStatus(lead.post_id, s)} className={`px-3 py-1.5 rounded-full text-xs font-semibold border transition-all ${lead.status === s ? "bg-blue-600 border-blue-500 text-white" : "bg-white/5 border-white/10 text-white/40 hover:border-white/30 hover:text-white/70"}`}>{s}</button>)}
            </div>
          </div>
          {lead.post && (
            <div className="bg-white/[0.03] rounded-xl p-4">
              <div className="text-[10px] text-white/25 uppercase tracking-widest mb-2">Post Content</div>
              <p className="text-xs text-white/50 leading-relaxed whitespace-pre-wrap max-h-48 overflow-y-auto">{lead.post}</p>
            </div>
          )}
          {lead.url && <a href={lead.url} target="_blank" rel="noreferrer" className="flex items-center gap-2 text-xs text-blue-400 hover:text-blue-300 transition-colors"><Globe className="w-3.5 h-3.5" />View original post</a>}
        </div>
      </div>
    </div>
  );
}

export default function Services() {
  const { logout, user } = useContext(AuthContext);
  const token   = localStorage.getItem("access");
  const headers = useMemo(() => ({ Authorization: `Bearer ${token}` }), [token]);

  const [leads, setLeads]               = useState([]);
  const [history, setHistory]           = useState([]);
  const [categories, setCategories]     = useState([]);
  const [cities, setCities]             = useState([]);
  const [locationType, setLocationType] = useState("city");
  const [locationValue, setLocationValue] = useState("");
  const [selectedCategories, setSelectedCategories]   = useState([]);
  const [selectedSubServices, setSelectedSubServices] = useState([]);
  const [expandedCategories, setExpandedCategories]   = useState({});
  const [maxPostsPerGroup, setMaxPostsPerGroup]       = useState(50);
  const [fbManualGroupUrls, setFbManualGroupUrls]     = useState("");
  const [googleMaxPages, setGoogleMaxPages]   = useState(3);
  const [googleDeepScrape, setGoogleDeepScrape] = useState(true);
  const [selectedSources, setSelectedSources] = useState(["craigslist"]);
  const [scraping, setScraping]         = useState(false);
  const [runId, setRunId]               = useState(null);
  const [scrapeStatus, setScrapeStatus] = useState(null);
  const [isAborting, setIsAborting]     = useState(false);
  const [fSource, setFSource]           = useState("");
  const [fServiceCat, setFServiceCat]   = useState("");
  const [fServiceLabel, setFServiceLabel] = useState("");
  const [fStatus, setFStatus]           = useState("");
  const [fMinScore, setFMinScore]       = useState("");
  const [fSearch, setFSearch]           = useState("");
  const [fSearchDebounced, setFSearchDebounced] = useState("");
  const [fFbGroup, setFFbGroup]         = useState("");
  const [fFbGroupDebounced, setFFbGroupDebounced] = useState("");
  const [fHasPhone, setFHasPhone]       = useState(false);
  const [fHasEmail, setFHasEmail]       = useState(false);
  const [selectedLead, setSelectedLead] = useState(null);
  const [loading, setLoading]           = useState(false);
  const [page, setPage]                 = useState(1);
  const [historyPage, setHistoryPage]   = useState(1);
  const [activeTab, setActiveTab]       = useState("leads");
  const [suggestions, setSuggestions]   = useState({ list: [], show: false });
  const locationInputRef = useRef(null);
  const lastTypedRef     = useRef(0);
  const [fbViewMode, setFbViewMode]     = useState("cards");
  const [sidebarOpen, setSidebarOpen]   = useState(false);
  const [totalLeads, setTotalLeads]     = useState(0);
  const [totalPages, setTotalPages]     = useState(1);
  const newestLeadTs     = useRef(null);
  const [newLeadsBuffer, setNewLeadsBuffer]   = useState([]);
  const [pendingNewCount, setPendingNewCount] = useState(0);
  const [fbGeoCoords, setFbGeoCoords]         = useState({});
  const geocodingQueue = useRef(new Set());
  const [zipHint, setZipHint] = useState(null);
  const [fbGroupCount, setFbGroupCount] = useState(0);

  useEffect(() => { const t = setTimeout(() => setFSearchDebounced(fSearch), 400); return () => clearTimeout(t); }, [fSearch]);
  useEffect(() => { const t = setTimeout(() => setFFbGroupDebounced(fFbGroup), 400); return () => clearTimeout(t); }, [fFbGroup]);
  useEffect(() => {
    if (locationType === "zip" && /^\d{5}$/.test(locationValue.trim())) {
      setZipHint({ note: "Leads stored with ZIP" });
    } else {
      setZipHint(null);
    }
  }, [locationType, locationValue]);

  const fetchLeads = useCallback(async (pageNum = 1) => {
    setLoading(true);
    try {
      const params = { page: pageNum, page_size: 50 };
      if (fSource)           params.source           = fSource;
      if (fServiceCat)       params.service_category = fServiceCat;
      if (fStatus)           params.status           = fStatus;
      if (fMinScore)         params.min_score        = fMinScore;
      if (fSearchDebounced)  params.search           = fSearchDebounced;
      if (fHasPhone)         params.has_phone        = "true";
      if (fHasEmail)         params.has_email        = "true";
      if (fFbGroupDebounced) params.fb_group         = fFbGroupDebounced;
      const res     = await axios.get(`${API}/leads/`, { headers, params });
      const data    = res.data;
      const results = Array.isArray(data) ? data : (data.results ?? []);
      setLeads(results);
      setTotalLeads(Array.isArray(data) ? data.length : (data.total ?? 0));
      setTotalPages(Array.isArray(data) ? 1 : (data.total_pages ?? 1));
      if (results.length > 0) {
        const newest = results.reduce((a, b) => new Date(a.created_at) > new Date(b.created_at) ? a : b);
        newestLeadTs.current = newest.created_at;
      }
      setNewLeadsBuffer([]); setPendingNewCount(0);
    } catch (e) { console.error(e); }
    setLoading(false);
  }, [headers, fSource, fServiceCat, fStatus, fMinScore, fSearchDebounced, fHasPhone, fHasEmail, fFbGroupDebounced]);

  const fetchNewLeadsOnly = useCallback(async () => {
    try {
      const floor = newestLeadTs.current
        || (scrapeStatus?.started_at ? new Date(new Date(scrapeStatus.started_at).getTime() - 5000).toISOString() : null)
        || new Date(Date.now() - 30_000).toISOString();

      const params = {
        page: 1,
        page_size: 100,
        date_after: floor,
        ordering: "-created_at",
      };
      const res     = await axios.get(`${API}/leads/`, { headers, params });
      const data    = res.data;
      const results = Array.isArray(data) ? data : (data.results ?? []);
      if (results.length > 0) {
        const newest = results.reduce((a, b) => new Date(a.created_at) > new Date(b.created_at) ? a : b);
        newestLeadTs.current = newest.created_at;
        setLeads(prev => {
          const existingIds = new Set(prev.map(l => l.post_id));
          const truly_new   = results.filter(l => !existingIds.has(l.post_id));
          if (truly_new.length === 0) return prev;
          setTotalLeads(t => t + truly_new.length);
          setNewLeadsBuffer(buf => [...truly_new, ...buf].slice(0, 30));
          setPendingNewCount(c => c + truly_new.length);
          return [...truly_new, ...prev];
        });
      }
    } catch (e) { console.error(e); }
  }, [headers, scrapeStatus?.started_at]);

  const geocodeFbLeads = useCallback(async (leadsToGeocode) => {
    const need = leadsToGeocode.filter(l => l.source === "FACEBOOK" && !l.latitude && !l.longitude && l.location && !geocodingQueue.current.has(l.post_id) && fbGeoCoords[l.post_id] === undefined);
    if (need.length === 0) return;
    need.forEach(l => geocodingQueue.current.add(l.post_id));
    for (const lead of need.slice(0, 30)) {
      const coords = await geocodeLocation(lead.location);
      geocodingQueue.current.delete(lead.post_id);
      if (coords) setFbGeoCoords(prev => ({ ...prev, [lead.post_id]: coords }));
    }
  }, [fbGeoCoords]);

  const fetchCategories = useCallback(async () => { try { const r = await axios.get(`${API}/meta/categories/`); setCategories(r.data.categories); } catch (_) {} }, []);
  const fetchCities     = useCallback(async () => { try { const r = await axios.get(`${API}/meta/cities/`, { headers }); setCities(r.data.cities); } catch (_) {} }, [headers]);
  const fetchHistory    = useCallback(async () => { try { const r = await axios.get(`${API}/scrape/history/`, { headers }); setHistory(r.data); } catch (_) {} }, [headers]);
  const fetchFbGroupCount = useCallback(async () => { try { const r = await axios.get(`${API}/fb-groups/`, { headers }); setFbGroupCount((r.data.groups || []).length); } catch (_) {} }, [headers]);
  const checkScrapeStatus = useCallback(async () => {
    try {
      const res = await axios.get(`${API}/scrape/status/`, { headers });
      setScrapeStatus(res.data);
      if (res.data.status === "RUNNING") { setScraping(true); setRunId(res.data.run_id); }
      else { setScraping(false); setIsAborting(false); }
    } catch (e) { console.error(e); }
  }, [headers]);

  useEffect(() => {
    fetchLeads(); fetchCategories(); fetchCities(); fetchHistory(); checkScrapeStatus(); fetchFbGroupCount();
  }, []);

  const isFirstRender = useRef(true);
  useEffect(() => {
    if (isFirstRender.current) { isFirstRender.current = false; return; }
    setPage(1); fetchLeads(1);
  }, [fSource, fServiceCat, fStatus, fMinScore, fSearchDebounced, fHasPhone, fHasEmail, fFbGroupDebounced]);

  useEffect(() => {
    if (!scraping) return;
    checkScrapeStatus();
    fetchNewLeadsOnly();
    let tick = 0;
    const iv = setInterval(() => {
      tick++;
      checkScrapeStatus();
      fetchNewLeadsOnly();
      if (tick % 8 === 0) fetchHistory();
    }, 2000);
    return () => clearInterval(iv);
  }, [scraping, checkScrapeStatus, fetchNewLeadsOnly, fetchHistory]);

  useEffect(() => {
    if (scrapeStatus?.status && scrapeStatus.status !== "RUNNING") fetchFbGroupCount();
  }, [scrapeStatus?.status]);

  useEffect(() => { if (activeTab === "map" && leads.length > 0) geocodeFbLeads(leads); }, [activeTab, leads, geocodeFbLeads]);

  // ── UPDATED: City suggestions now show "City, State" format ──
  useEffect(() => {
    if (!locationValue.trim()) { setSuggestions({ list: [], show: false }); return; }
    const q = locationValue.toLowerCase();
    if (locationType === "city") {
      const matches = cities
        .filter(c =>
          c.name.toLowerCase().includes(q) ||
          (c.display && c.display.toLowerCase().includes(q)) ||
          (c.state && c.state.toLowerCase().includes(q))
        )
        .slice(0, 12)
        .map(c => ({
          code: c.code,
          name: cityDisplayName(c),   // "Birmingham, Alabama"
          rawName: c.name,             // "Birmingham" — sent to scraper
        }));
      setSuggestions({ list: matches, show: matches.length > 0 });
    } else if (locationType === "state") {
      const matches = US_STATES
        .filter(st => st.toLowerCase().startsWith(q) || st.toLowerCase().includes(q))
        .slice(0, 12)
        .map(st => ({ code: st, name: st, rawName: st }));
      setSuggestions({ list: matches, show: matches.length > 0 });
    } else {
      setSuggestions({ list: [], show: false });
    }
  }, [locationValue, locationType, cities]);

  const startScrape = async () => {
    const fbOnly = selectedSources.length === 1 && selectedSources[0] === "facebook";
    const hasFbGroups = fbManualGroupUrls.trim();
    const nonFbSources = selectedSources.filter(s => s !== "facebook");

    if (!locationValue.trim() && !(fbOnly && hasFbGroups)) {
      return alert("Enter a location, or provide Facebook group URLs for a Facebook-only run.");
    }
    if (selectedSources.length === 0) return alert("Select at least one source.");
    if (nonFbSources.length > 0 && selectedCategories.length === 0) {
      return alert("Select at least one category for Craigslist / Google.");
    }

    setScraping(true); setScrapeStatus(null); setSidebarOpen(false);
    newestLeadTs.current = new Date().toISOString();
    setNewLeadsBuffer([]); setPendingNewCount(0);
    try {
      const fbGroupUrlsClean = fbManualGroupUrls.trim()
        ? fbManualGroupUrls.split(/[\n,]+/).map(u => u.trim()).filter(u => u.startsWith("http"))
        : [];

      // Strip the ", State" suffix before sending to the scraper —
      // the backend only needs the raw city name or code.
      const rawLocation = locationValue.trim().split(",")[0].trim();

      const res = await axios.post(`${API}/scrape/start/`, {
        location: { type: locationType, value: rawLocation },
        ...(selectedCategories.length > 0 ? { categories: selectedCategories } : {}),
        max_posts_per_group: maxPostsPerGroup,
        sources: selectedSources,
        fb_group_urls: fbGroupUrlsClean,
        google_max_pages:   googleMaxPages,
        google_deep_scrape: googleDeepScrape,
      }, { headers });
      setRunId(res.data.run_id);
      fetchHistory();
      setTimeout(checkScrapeStatus, 1000);
      setTimeout(fetchNewLeadsOnly, 2000);
    } catch (e) { alert(e.response?.data?.error || "Failed to start scrape."); setScraping(false); }
  };

  const cancelScrape = async () => {
    if (!runId) return; setIsAborting(true);
    try {
      await axios.post(`${API}/scrape/cancel/`, { run_id: runId }, { headers });
      setTimeout(() => { fetchLeads(); fetchHistory(); checkScrapeStatus(); fetchFbGroupCount(); }, 1000);
    } catch (_) { setIsAborting(false); }
  };

  const updateLeadStatus = async (postId, status) => {
    try {
      await axios.patch(`${API}/leads/${postId}/status/`, { status }, { headers });
      setLeads(prev => prev.map(l => l.post_id === postId ? { ...l, status } : l));
      if (selectedLead?.post_id === postId) setSelectedLead(prev => ({ ...prev, status }));
    } catch (_) {}
  };

  const filtered = useMemo(() => {
    let out = [...(Array.isArray(leads) ? leads : [])];
    if (fServiceLabel) out = out.filter(l => matchesService(l, fServiceLabel));
    return out;
  }, [leads, fServiceLabel]);

  const mapLeads = useMemo(() => filtered.map(l => {
    if (l.latitude && l.longitude) return l;
    if (l.source === "FACEBOOK" && fbGeoCoords[l.post_id]) return { ...l, latitude: fbGeoCoords[l.post_id].lat.toString(), longitude: fbGeoCoords[l.post_id].lng.toString(), _geocoded: true };
    return null;
  }).filter(Boolean), [filtered, fbGeoCoords]);

  const fbLeads = filtered.filter(l => l.source === "FACEBOOK");
  const clLeads = filtered.filter(l => l.source === "CRAIGSLIST");
  const ggLeads = filtered.filter(l => l.source === "GOOGLE");
  const fbPendingGeoCount = leads.filter(l => l.source === "FACEBOOK" && !l.latitude && !fbGeoCoords[l.post_id] && l.location).length;

  const stats = useMemo(() => ({
    total:     totalLeads,
    cl:        filtered.filter(l => l.source === "CRAIGSLIST").length,
    fb:        filtered.filter(l => l.source === "FACEBOOK").length,
    google:    filtered.filter(l => l.source === "GOOGLE").length,
    withPhone: filtered.filter(l => l.phone).length,
    withEmail: filtered.filter(l => l.email).length,
    avgScore:  filtered.length ? Math.round(filtered.reduce((a, l) => a + l.score, 0) / filtered.length) : 0,
  }), [filtered, totalLeads]);

  const toggleCategory = k => {
    setSelectedCategories(p => {
      const next = p.includes(k) ? p.filter(x => x !== k) : [...p, k];
      if (p.includes(k)) { const taxKey = CAT_KEY_TO_TAXONOMY[k]; if (taxKey) { const subLabels = SERVICE_TAXONOMY[taxKey]?.services.map(s => s.label) || []; setSelectedSubServices(prev => prev.filter(s => !subLabels.includes(s))); } }
      return next;
    });
  };
  const toggleSubService     = label => setSelectedSubServices(p => p.includes(label) ? p.filter(x => x !== label) : [...p, label]);
  const toggleCategoryExpand = k => setExpandedCategories(p => ({ ...p, [k]: !p[k] }));
  const toggleSource = s => setSelectedSources(p => p.includes(s) ? p.filter(x => x !== s) : [...p, s]);
  const resetFilters = () => { setFSource(""); setFServiceCat(""); setFServiceLabel(""); setFStatus(""); setFMinScore(""); setFSearch(""); setFHasPhone(false); setFHasEmail(false); setFFbGroup(""); setPage(1); };
  const hasActiveFilters  = fSource || fServiceCat || fServiceLabel || fStatus || fMinScore || fSearch || fHasPhone || fHasEmail || fFbGroup;
  const showActivityPanel = scrapeStatus && scrapeStatus.status !== undefined && scrapeStatus.status !== "IDLE";

  const showGoogleSettings = selectedSources.includes("google");
  const showFbSettings     = selectedSources.includes("facebook");

  const sidebarContent = (
    <>
      <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
        <div className="px-4 py-3.5 border-b border-white/[0.06] flex items-center justify-between">
          <div className="flex items-center gap-2"><Zap className="w-4 h-4 text-blue-400" /><span className="text-sm font-semibold">New Scrape</span></div>
          {scrapeStatus && !scraping && <Badge color={scrapeStatus.status === "SUCCEEDED" ? "green" : scrapeStatus.status === "PARTIAL" ? "yellow" : "slate"}>{scrapeStatus.status}</Badge>}
        </div>
        <div className="p-4 space-y-4">

          {/* Location type */}
          <div>
            <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block mb-2">Location Type</label>
            <div className="grid grid-cols-3 gap-1.5 bg-white/5 p-1 rounded-xl">
              {["city","state","zip"].map(t => (
                <button key={t} onClick={() => { setLocationType(t); setLocationValue(""); setSuggestions({ list: [], show: false }); setZipHint(null); setTimeout(() => locationInputRef.current?.focus(), 50); }}
                  className={`py-1.5 rounded-lg text-xs font-semibold transition-all capitalize ${locationType === t ? "bg-blue-600 text-white shadow-lg shadow-blue-500/20" : "text-white/40 hover:text-white/70"}`}>{t}</button>
              ))}
            </div>
          </div>

          {/* Location input */}
          <div className="relative">
            <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block mb-2">
              {locationType === "city" ? "City" : locationType === "state" ? "State" : "ZIP Code"}
            </label>
            <div className="relative">
              <MapPin className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-white/30 pointer-events-none" />
              <input
                ref={locationInputRef}
                type="text"
                value={locationValue}
                autoComplete="off"
                autoCorrect="off"
                autoCapitalize="off"
                spellCheck="false"
                onFocus={() => {
                  if (locationType !== "zip" && !locationValue.trim()) {
                    if (locationType === "city") {
                      // ── UPDATED: show all cities with "City, State" on focus ──
                      setSuggestions({
                        list: cities.slice(0, 50).map(c => ({
                          code: c.code,
                          name: cityDisplayName(c),
                          rawName: c.name,
                        })),
                        show: true,
                      });
                    } else {
                      setSuggestions({
                        list: US_STATES.map(st => ({ code: st, name: st, rawName: st })),
                        show: true,
                      });
                    }
                  }
                }}
                onChange={e => { lastTypedRef.current = Date.now(); setLocationValue(e.target.value); }}
                onBlur={() => setTimeout(() => setSuggestions(s => ({ ...s, show: false })), 180)}
                placeholder={
                  locationType === "city"  ? "Search cities..." :
                  locationType === "state" ? "Search or select a state..." :
                  "e.g. 77001"
                }
                className="w-full bg-white/5 border border-white/10 rounded-xl pl-9 pr-4 py-2.5 text-sm text-white placeholder-white/20 focus:outline-none focus:border-blue-500/60 transition-all"
              />
            </div>

            {/* ── UPDATED: suggestions dropdown — state baked into name, no separate sub ── */}
            {suggestions.show && suggestions.list.length > 0 && (
              <div className="absolute z-50 left-0 right-0 mt-1 bg-[#16192a] border border-white/10 rounded-xl shadow-2xl overflow-hidden">
                <div className="max-h-52 overflow-y-auto">
                  {suggestions.list.map(c => (
                    <button
                      key={c.code}
                      onMouseDown={e => e.preventDefault()}
                      onClick={() => {
                        setLocationValue(c.name);       // shows "Birmingham, Alabama"
                        setSuggestions({ list: [], show: false });
                        setTimeout(() => locationInputRef.current?.focus(), 0);
                      }}
                      className="w-full text-left px-4 py-2.5 text-sm text-white/70 hover:bg-white/[0.07] hover:text-white transition-colors"
                    >
                      {c.name}
                    </button>
                  ))}
                </div>
              </div>
            )}

            {zipHint && (
              <div className="mt-2 flex items-start gap-2 bg-indigo-500/5 border border-indigo-500/15 rounded-xl px-3 py-2">
                <Info className="w-3 h-3 text-indigo-400/60 flex-shrink-0 mt-0.5" />
                <p className="text-[10px] text-indigo-300/60 leading-relaxed">{zipHint.note}</p>
              </div>
            )}
          </div>

          {/* Categories */}
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
                            <button key={svc.label} onClick={() => toggleSubService(svc.label)}
                              className={`w-full flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-left transition-all ${isSvcSelected ? "bg-blue-500/15 border border-blue-500/30 text-blue-200" : "text-white/40 hover:bg-white/[0.04] hover:text-white/70 border border-transparent"}`}>
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

          {/* Sources */}
          <div>
            <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block mb-2">Sources</label>
            <div className="grid grid-cols-2 gap-2">
              {ALL_SOURCES.map(src => (
                <button key={src.key} onClick={() => toggleSource(src.key)}
                  className={`flex items-center gap-2 p-3 rounded-xl border text-xs font-semibold transition-all ${selectedSources.includes(src.key) ? "bg-blue-600/10 border-blue-500/40 text-white" : "bg-white/[0.02] border-white/[0.06] text-white/40 hover:border-white/20"}`}>
                  <span>{src.emoji}</span><span>{src.label}</span>
                </button>
              ))}
            </div>
          </div>

          {/* Facebook settings */}
          {showFbSettings && (
            <div className="space-y-3 pt-1 border-t border-white/[0.06]">
              <div className="flex items-center gap-2 pt-1">
                <Users className="w-3.5 h-3.5 text-indigo-400" />
                <span className="text-[11px] font-semibold uppercase tracking-widest text-indigo-400/80">Facebook Settings</span>
              </div>
              <div className="bg-indigo-500/5 border border-indigo-500/15 rounded-xl px-3 py-2.5">
                <p className="text-[10px] text-indigo-300/60 leading-relaxed">
                  Paste group URLs to scrape. Already-scraped posts are always skipped automatically.
                  Use the <span className="font-semibold text-indigo-300/80">Groups tab</span> to manage your full group library.
                </p>
              </div>
              <div>
                <div className="flex items-center justify-between mb-1.5">
                  <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30">Posts per Group</label>
                  <span className="text-sm font-bold text-indigo-300 tabular-nums">{maxPostsPerGroup}</span>
                </div>
                <input
                  type="number" min={5} max={500} value={maxPostsPerGroup}
                  onChange={e => setMaxPostsPerGroup(Math.max(5, Math.min(500, parseInt(e.target.value) || 50)))}
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-sm text-white focus:outline-none focus:border-indigo-500/60 transition-all tabular-nums"
                />
              </div>
              <div>
                <label className="text-[11px] font-semibold uppercase tracking-widest text-white/30 block mb-1">Group URLs</label>
                <textarea value={fbManualGroupUrls} autoComplete="off" spellCheck="false"
                  onChange={e => { lastTypedRef.current = Date.now(); setFbManualGroupUrls(e.target.value); }}
                  placeholder={"https://www.facebook.com/groups/...\nOne URL per line"}
                  rows={3}
                  className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-2.5 text-xs text-white placeholder-white/20 focus:outline-none focus:border-indigo-500/60 transition-all resize-none leading-relaxed font-mono" />
                <p className="mt-1 text-[10px] text-white/20">For managing all groups, use the Groups tab</p>
              </div>
            </div>
          )}

          {/* Google settings */}
          {showGoogleSettings && (
            <div className="space-y-4 pt-1 border-t border-white/[0.06]">
              <div className="flex items-center gap-2 pt-1">
                <Search className="w-3.5 h-3.5 text-sky-400" />
                <span className="text-[11px] font-semibold uppercase tracking-widest text-sky-400/80">Google Settings</span>
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <div>
                    <label className="text-xs font-semibold text-white/70">How far to search</label>
                    <p className="text-[10px] text-white/30 mt-0.5 leading-relaxed">More pages = more businesses found, but takes longer.</p>
                  </div>
                  <div className="text-right flex-shrink-0 ml-3">
                    <span className="text-sm font-bold text-sky-300 tabular-nums">{googleMaxPages}</span>
                    <div className="text-[10px] text-white/25">{googleMaxPages === 1 ? "page" : "pages"}</div>
                  </div>
                </div>
                <input type="range" min={1} max={10} step={1} value={googleMaxPages} onChange={e => setGoogleMaxPages(Number(e.target.value))} className="w-full accent-sky-500 cursor-pointer" />
                <div className="grid grid-cols-3 gap-1 text-[10px]">
                  <button onClick={() => setGoogleMaxPages(1)} className={`py-1.5 px-2 rounded-lg border text-left transition-all ${googleMaxPages <= 2 ? "border-sky-500/40 bg-sky-500/10 text-sky-300" : "border-white/[0.06] text-white/25 hover:text-white/50"}`}>
                    <div className="font-semibold">Quick</div><div className="text-[9px] opacity-70">~10 results</div>
                  </button>
                  <button onClick={() => setGoogleMaxPages(5)} className={`py-1.5 px-2 rounded-lg border text-left transition-all ${googleMaxPages >= 3 && googleMaxPages <= 7 ? "border-sky-500/40 bg-sky-500/10 text-sky-300" : "border-white/[0.06] text-white/25 hover:text-white/50"}`}>
                    <div className="font-semibold">Normal</div><div className="text-[9px] opacity-70">~50 results</div>
                  </button>
                  <button onClick={() => setGoogleMaxPages(10)} className={`py-1.5 px-2 rounded-lg border text-left transition-all ${googleMaxPages >= 8 ? "border-sky-500/40 bg-sky-500/10 text-sky-300" : "border-white/[0.06] text-white/25 hover:text-white/50"}`}>
                    <div className="font-semibold">Deep</div><div className="text-[9px] opacity-70">~100 results</div>
                  </button>
                </div>
              </div>
              <div className="bg-white/[0.02] border border-white/[0.06] rounded-xl p-3 space-y-2">
                <Toggle value={googleDeepScrape} onChange={setGoogleDeepScrape} label="Visit each website for contact info" hint={null} />
                <p className="text-[10px] text-white/25 leading-relaxed">
                  {googleDeepScrape
                    ? "✓ We'll open each business's website and look for their phone number and email — like a person would. Slower but finds way more contacts."
                    : "We'll only use what's visible in Google search results. Faster, but fewer phone numbers and emails."}
                </p>
              </div>
            </div>
          )}

          {/* Launch */}
          {!scraping ? (
            <button onClick={startScrape}
              disabled={(() => {
                if (selectedSources.length === 0) return true;
                const fbOnly = selectedSources.length === 1 && selectedSources[0] === "facebook";
                const hasFbGroups = fbManualGroupUrls.trim();
                if (fbOnly && hasFbGroups) return false;
                if (!locationValue.trim()) return true;
                return false;
              })()}
              className="w-full py-3 rounded-xl bg-gradient-to-r from-blue-600 to-violet-600 text-white text-sm font-semibold hover:from-blue-500 hover:to-violet-500 transition-all shadow-lg shadow-blue-500/20 disabled:opacity-30 disabled:cursor-not-allowed">
              Launch Scrape
            </button>
          ) : (
            <div className="space-y-2">
              <div className="w-full bg-white/5 rounded-xl h-1.5 overflow-hidden"><div className="h-full bg-gradient-to-r from-blue-500 to-violet-500 animate-pulse rounded-full w-2/3" /></div>
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

      {/* Filters */}
      <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
        <div className="px-4 py-3.5 border-b border-white/[0.06] flex items-center justify-between">
          <div className="flex items-center gap-2"><Filter className="w-4 h-4 text-white/40" /><span className="text-sm font-semibold">Filters</span>{hasActiveFilters && <span className="w-2 h-2 rounded-full bg-blue-400 animate-pulse" />}</div>
          {hasActiveFilters && <button onClick={resetFilters} className="text-[11px] text-white/30 hover:text-white/60 transition-colors">Clear all</button>}
        </div>
        <div className="p-4 space-y-3">
          <div className="relative">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-white/30 pointer-events-none" />
            <input type="text" placeholder="Search title or location..." value={fSearch} autoComplete="off" autoCorrect="off" spellCheck="false" onChange={e => { lastTypedRef.current = Date.now(); setFSearch(e.target.value); }} className="w-full bg-white/5 border border-white/10 rounded-xl pl-9 pr-4 py-2.5 text-xs text-white placeholder-white/20 focus:outline-none focus:border-blue-500/60 transition-all" />
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Source</label>
            <div className="grid grid-cols-2 gap-1 bg-white/5 p-1 rounded-lg">
              {[["", "All"], ["CRAIGSLIST", "CL 🔶"], ["FACEBOOK", "FB Groups 🔷"], ["GOOGLE", "GG 🔍"]].map(([v, l]) => (
                <button key={v} onClick={() => setFSource(v)} className={`py-1.5 rounded-md text-[10px] font-semibold transition-all ${fSource === v ? "bg-white/10 text-white" : "text-white/30 hover:text-white/60"}`}>{l}</button>
              ))}
            </div>
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Category</label>
            <select value={fServiceCat} onChange={e => setFServiceCat(e.target.value)} className="w-full bg-[#1a1d2e] border border-white/10 rounded-xl px-3 py-2.5 text-xs text-white focus:outline-none focus:border-blue-500/60 transition-all cursor-pointer">
              <option value="">All categories</option>{categories.map(c => <option key={c.key} value={c.key}>{c.label}</option>)}
            </select>
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Service Type</label>
            <select value={fServiceLabel} onChange={e => setFServiceLabel(e.target.value)} className="w-full bg-[#1a1d2e] border border-white/10 rounded-xl px-3 py-2.5 text-xs text-white focus:outline-none focus:border-blue-500/60 transition-all cursor-pointer">
              <option value="">All services</option>
              {Object.entries(SERVICE_TAXONOMY).map(([group, g]) => <optgroup key={group} label={`── ${group} ──`}>{g.services.map(sv => <option key={sv.label} value={sv.label}>{sv.label}</option>)}</optgroup>)}
            </select>
          </div>
          <div>
            <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Status</label>
            <select value={fStatus} onChange={e => setFStatus(e.target.value)} className="w-full bg-[#1a1d2e] border border-white/10 rounded-xl px-3 py-2.5 text-xs text-white focus:outline-none focus:border-blue-500/60 transition-all cursor-pointer">
              <option value="">All statuses</option>{["NEW","CONTACTED","QUALIFIED","WON","LOST"].map(st => <option key={st} value={st}>{st}</option>)}
            </select>
          </div>
          <div>
            <div className="flex items-center justify-between mb-1.5"><label className="text-[10px] font-semibold uppercase tracking-widest text-white/25">Min Score</label><span className="text-[11px] font-bold text-white/50 tabular-nums">{fMinScore || "0"}</span></div>
            <input type="range" min="0" max="100" value={fMinScore || 0} onChange={e => setFMinScore(e.target.value === "0" ? "" : e.target.value)} className="w-full accent-blue-500 cursor-pointer" />
          </div>
          <div className="grid grid-cols-2 gap-2">
            {[{ label: "Has Phone", icon: Phone, val: fHasPhone, set: setFHasPhone }, { label: "Has Email", icon: Mail, val: fHasEmail, set: setFHasEmail }].map(({ label, icon: Icon, val, set }) => (
              <button key={label} onClick={() => set(p => !p)} className={`flex items-center justify-center gap-1.5 py-2.5 rounded-xl border text-[11px] font-semibold transition-all ${val ? "bg-emerald-600/10 border-emerald-500/40 text-emerald-400" : "bg-white/[0.02] border-white/[0.06] text-white/30 hover:border-white/20 hover:text-white/50"}`}>
                <Icon className="w-3 h-3" />{label}
              </button>
            ))}
          </div>
          {(fSource === "FACEBOOK" || fSource === "") && (
            <div>
              <label className="text-[10px] font-semibold uppercase tracking-widest text-white/25 block mb-1.5">Filter by Group</label>
              <input type="text" value={fFbGroup} onChange={e => { lastTypedRef.current = Date.now(); setFFbGroup(e.target.value); }} placeholder="Group name..." autoComplete="off" autoCorrect="off" spellCheck="false" className="w-full bg-white/5 border border-white/10 rounded-xl px-3 py-2 text-xs text-white placeholder-white/20 focus:outline-none focus:border-blue-500/50 transition-all" />
            </div>
          )}
        </div>
      </div>
    </>
  );

  return (
    <div style={{ fontFamily: "'DM Sans', 'Helvetica Neue', sans-serif" }} className="min-h-screen bg-[#0f1117] text-slate-100">
      <style>{`@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');`}</style>

      {scraping && pendingNewCount > 0 && <NewLeadsBanner count={pendingNewCount} onView={() => { setPendingNewCount(0); setActiveTab("leads"); window.scrollTo({ top: 0, behavior: "smooth" }); }} />}

      {/* Mobile sidebar overlay */}
      {sidebarOpen && (
        <div className="fixed inset-0 z-40 lg:hidden" onClick={() => setSidebarOpen(false)}>
          <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />
          <div className="absolute left-0 top-0 bottom-0 w-[320px] bg-[#0f1117] border-r border-white/[0.06] overflow-y-auto p-4 space-y-4" onClick={e => e.stopPropagation()}>
            <div className="flex items-center justify-between pb-2 border-b border-white/[0.06]"><span className="text-sm font-semibold">Controls</span><button onClick={() => setSidebarOpen(false)} className="text-white/40 hover:text-white transition-colors"><X className="w-5 h-5" /></button></div>
            {sidebarContent}
          </div>
        </div>
      )}

      <header className="border-b border-white/5 bg-[#0f1117]/80 backdrop-blur-xl sticky top-0 z-50">
        <div className="max-w-[1400px] mx-auto px-4 lg:px-6 h-14 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <button onClick={() => setSidebarOpen(true)} className="lg:hidden text-white/40 hover:text-white transition-colors"><Menu className="w-5 h-5" /></button>
            <div className="w-7 h-7 rounded-lg bg-gradient-to-br from-blue-500 to-violet-600 flex items-center justify-center flex-shrink-0"><Target className="w-4 h-4 text-white" /></div>
            <span className="font-semibold text-sm tracking-tight">GreyMoon Scraper</span>
            <span className="text-white/20 text-xs hidden sm:block">|</span>
            <span className="text-white/40 text-xs hidden sm:block">contractor intelligence</span>
          </div>
          <div className="flex items-center gap-2 lg:gap-4">
            {scraping && (() => {
              const navSrc = stageSource(scrapeStatus?.current_stage || "");
              const navFamily = sourceFamily(navSrc);
              const navCfg = SOURCE_CFG[navFamily?.toUpperCase()];
              return (
                <div className="flex items-center gap-2">
                  <div className={`flex items-center gap-1.5 px-2.5 py-1.5 rounded-full border text-xs font-semibold ${isAborting ? "bg-red-500/10 border-red-500/20 text-red-400" : navCfg ? navCfg.nav : "bg-amber-500/10 border-amber-500/20 text-amber-400"}`}>
                    <Loader className="w-3 h-3 animate-spin" />
                    <span className="hidden sm:inline">{isAborting ? "Stopping..." : navCfg ? `${navCfg.emoji} ${navCfg.name}` : "Running..."}</span>
                  </div>
                  {Math.max(totalLeads, scrapeStatus?.leads_collected ?? 0) > 0 && <span className="hidden sm:flex items-center gap-1.5 bg-emerald-500/10 border border-emerald-500/20 text-emerald-400 text-xs font-bold px-2.5 py-1 rounded-full"><span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />{Math.max(totalLeads, scrapeStatus?.leads_collected ?? 0)}</span>}
                </div>
              );
            })()}
            <div className="text-white/40 text-xs font-medium hidden sm:block">{user?.username}</div>
            <button onClick={logout} className="text-xs text-white/40 hover:text-white/80 transition-colors">Sign out</button>
          </div>
        </div>
      </header>

      <div className="max-w-[1400px] mx-auto px-4 lg:px-6 py-6 lg:grid lg:grid-cols-[300px_1fr] xl:grid-cols-[330px_1fr] gap-6 lg:gap-8 items-start">
        <aside className="hidden lg:block space-y-4 sticky top-[72px] max-h-[calc(100vh-88px)] overflow-y-auto pr-1">{sidebarContent}</aside>

        <main className="space-y-4 min-w-0">
          {scraping && scrapeStatus && <ScrapeLiveDashboard scrapeStatus={scrapeStatus} onStop={cancelScrape} isAborting={isAborting} liveLeadCount={totalLeads} newLeadsBuffer={newLeadsBuffer} />}

          {scraping && (
            <div className="bg-white/[0.02] border border-white/[0.06] rounded-2xl overflow-hidden">
              <div className="px-4 py-3 border-b border-white/[0.06] flex items-center gap-2">
                <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse" />
                <span className="text-xs font-semibold text-white/60">
                  {leads.length === 0 ? "Waiting for first leads…" : "Leads — updating live"}
                </span>
                <span className="text-[11px] text-white/25 ml-auto">
                  {Math.max(totalLeads, scrapeStatus?.leads_collected ?? 0) > 0
                    ? `${Math.max(totalLeads, scrapeStatus?.leads_collected ?? 0)} saved`
                    : scrapeStatus?.current_stage || "Starting…"}
                </span>
              </div>

              {leads.length === 0 ? (
                <div className="px-4 py-6 flex flex-col items-center gap-4 text-center">
                  <div className="relative">
                    <div className="w-12 h-12 rounded-full bg-blue-500/10 border border-blue-500/20 flex items-center justify-center">
                      <Loader className="w-5 h-5 text-blue-400 animate-spin" />
                    </div>
                    <div className="absolute inset-0 rounded-full bg-blue-500/5 animate-ping" />
                  </div>
                  <div>
                    <p className="text-sm font-semibold text-white/70 mb-1">
                      {scrapeStatus?.current_stage || "Initialising pipeline…"}
                    </p>
                    {scrapeStatus?.stage_detail && (
                      <p className="text-[11px] text-white/35 leading-relaxed max-w-sm">{scrapeStatus.stage_detail}</p>
                    )}
                    {!scrapeStatus?.current_stage && (
                      <p className="text-[11px] text-white/30 leading-relaxed max-w-xs">
                        The scraper is launching. First leads usually appear within 1–3 minutes depending on the source.
                      </p>
                    )}
                  </div>
                  {(scrapeStatus?.activity_log?.length ?? 0) > 0 && (
                    <div className="w-full max-w-md space-y-1">
                      {[...(scrapeStatus.activity_log)].reverse().slice(0, 3).map((entry, i) => {
                        const cfg = LOG_CFG[entry.level] || LOG_CFG.info;
                        const Icon = cfg.icon;
                        return (
                          <div key={i} className={`flex items-start gap-2 px-3 py-2 rounded-xl text-left ${i === 0 ? cfg.bg + " border border-white/[0.04]" : "opacity-40"}`}>
                            <Icon className={`w-3 h-3 mt-0.5 flex-shrink-0 ${cfg.text}`} />
                            <div className="flex-1 min-w-0">
                              <span className={`text-[10px] font-semibold ${i === 0 ? cfg.text : "text-white/30"}`}>{entry.stage}</span>
                              {entry.detail && <p className="text-[10px] text-white/30 leading-relaxed mt-px truncate">{entry.detail}</p>}
                            </div>
                            <span className="text-[9px] text-white/15 font-mono flex-shrink-0">
                              {entry.ts ? new Date(entry.ts).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : ""}
                            </span>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              ) : (
                <div className="divide-y divide-white/[0.04] max-h-[420px] overflow-y-auto">
                  {leads.slice(0, 25).map((lead, i) => (
                    <div
                      key={lead.post_id}
                      className={`px-4 py-3 flex items-start gap-3 hover:bg-white/[0.02] cursor-pointer transition-all ${i === 0 ? "bg-emerald-500/[0.03]" : ""}`}
                      onClick={() => setSelectedLead(lead)}
                    >
                      <SourceTag source={lead.source} />
                      <div className="flex-1 min-w-0">
                        <p className="text-xs font-medium text-white/75 truncate">{lead.title}</p>
                        <div className="flex items-center gap-2 mt-0.5 flex-wrap">
                          {lead.location && <span className="text-[10px] text-white/30 flex items-center gap-0.5"><MapPin className="w-2.5 h-2.5" />{lead.location}</span>}
                          {lead.phone    && <span className="text-[10px] text-emerald-400/80 flex items-center gap-0.5"><Phone className="w-2.5 h-2.5" />{lead.phone}</span>}
                          {lead.email    && <span className="text-[10px] text-violet-400/70 flex items-center gap-0.5"><Mail className="w-2.5 h-2.5" />{lead.email}</span>}
                        </div>
                      </div>
                      <div className="flex items-center gap-2 flex-shrink-0">
                        {i === 0 && <span className="text-[9px] font-bold uppercase bg-emerald-500/15 text-emerald-400 border border-emerald-500/25 px-1.5 py-px rounded-full">new</span>}
                        <ScoreDot score={lead.score} />
                      </div>
                    </div>
                  ))}
                  {leads.length > 25 && (
                    <div className="px-4 py-2.5 text-center text-[10px] text-white/20">
                      +{leads.length - 25} more — view all in the Leads tab after scrape finishes
                    </div>
                  )}
                </div>
              )}
            </div>
          )}

          {!scraping && (<>
            {/* Stats row */}
            <div className="grid grid-cols-3 sm:grid-cols-7 gap-2">
              {[
                { label: "Total",      value: stats.total,     icon: Database,   color: "text-blue-400" },
                { label: "Craigslist", value: stats.cl,        icon: Globe,      color: "text-orange-400" },
                { label: "FB Groups",  value: stats.fb,        icon: Users,      color: "text-indigo-400" },
                { label: "Google",     value: stats.google,    icon: Search,     color: "text-sky-400" },
                { label: "Phones",     value: stats.withPhone, icon: Phone,      color: "text-emerald-400" },
                { label: "Emails",     value: stats.withEmail, icon: Mail,       color: "text-violet-400" },
                { label: "Avg Score",  value: stats.avgScore,  icon: TrendingUp, color: "text-amber-400" },
              ].map(({ label, value, icon: Icon, color }) => (
                <div key={label} className="bg-white/[0.03] border border-white/[0.06] rounded-xl p-3">
                  <div className={`${color} mb-1.5`}><Icon className="w-3.5 h-3.5" /></div>
                  <div className="text-lg font-bold tabular-nums">{value}</div>
                  <div className="text-[10px] text-white/30 mt-0.5">{label}</div>
                </div>
              ))}
            </div>

            {/* Tab bar */}
            <div className="flex items-center justify-between gap-3 flex-wrap">
              <div className="flex gap-1 bg-white/[0.03] border border-white/[0.06] p-1 rounded-xl">
                {[
                  { id: "leads",   label: "Leads",   icon: Database },
                  { id: "groups",  label: "Groups",  icon: Users },
                  { id: "map",     label: "Map",     icon: MapPin },
                  { id: "history", label: "History", icon: Clock },
                ].map(t => (
                  <button key={t.id} onClick={() => setActiveTab(t.id)} className={`flex items-center gap-1.5 px-3 py-2 rounded-lg text-xs font-semibold transition-all ${activeTab === t.id ? "bg-white/10 text-white" : "text-white/30 hover:text-white/60"}`}>
                    <t.icon className="w-3.5 h-3.5" />
                    <span className="hidden sm:inline">{t.label}</span>
                    {t.id === "leads"  && filtered.length > 0 && <span className="bg-white/10 text-white/60 px-1.5 rounded-full text-[10px]">{filtered.length}</span>}
                    {t.id === "groups" && fbGroupCount > 0   && <span className="bg-indigo-500/20 text-indigo-400 px-1.5 rounded-full text-[10px]">{fbGroupCount}</span>}
                  </button>
                ))}
              </div>
              <div className="flex items-center gap-2">
                {activeTab === "leads" && (
                  <div className="flex gap-1 bg-white/[0.03] border border-white/[0.06] p-1 rounded-lg">
                    {[{ id: "cards", icon: LayoutGrid }, { id: "table", icon: List }].map(v => (
                      <button key={v.id} onClick={() => setFbViewMode(v.id)} className={`px-2.5 py-1.5 rounded-md flex items-center transition-all ${fbViewMode === v.id ? "bg-white/10 text-white" : "text-white/30 hover:text-white/60"}`}><v.icon className="w-3.5 h-3.5" /></button>
                    ))}
                  </div>
                )}
                <button onClick={fetchLeads} className="flex items-center gap-1.5 text-xs text-white/30 hover:text-white/60 transition-colors">
                  <RefreshCw className={`w-3.5 h-3.5 ${loading ? "animate-spin" : ""}`} /><span className="hidden sm:inline">Refresh</span>
                </button>
              </div>
            </div>

            {/* Leads tab */}
            {activeTab === "leads" && (
              <div>
                {loading ? (
                  <div className="py-24 flex flex-col items-center gap-3 bg-white/[0.03] border border-white/[0.06] rounded-2xl"><Loader className="w-6 h-6 text-white/20 animate-spin" /><span className="text-white/20 text-sm">Loading leads...</span></div>
                ) : filtered.length === 0 ? (
                  <div className="py-24 flex flex-col items-center gap-3 bg-white/[0.03] border border-white/[0.06] rounded-2xl"><Database className="w-8 h-8 text-white/10" /><span className="text-white/20 text-sm">No leads match your filters</span></div>
                ) : (
                  <>
                    {fbViewMode === "cards" && fbLeads.length > 0 ? (
                      <div className="space-y-6">
                        {fbLeads.length > 0 && (
                          <div>
                            {(clLeads.length > 0 || ggLeads.length > 0) && <div className="flex items-center gap-2 mb-3"><span className="text-[10px] font-bold uppercase tracking-widest text-indigo-400/60">Facebook Groups — {fbLeads.length} posts</span><div className="flex-1 h-px bg-white/[0.05]" /></div>}
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">{fbLeads.map(lead => <FacebookLeadCard key={lead.post_id} lead={lead} onSelect={setSelectedLead} updateStatus={updateLeadStatus} />)}</div>
                          </div>
                        )}
                        {ggLeads.length > 0 && (<div><div className="flex items-center gap-2 mb-3"><span className="text-[10px] font-bold uppercase tracking-widest text-sky-400/60">Google Search — {ggLeads.length} businesses</span><div className="flex-1 h-px bg-white/[0.05]" /></div><LeadsTable leads={ggLeads} onSelect={setSelectedLead} updateStatus={updateLeadStatus} /></div>)}
                        {clLeads.length > 0 && (<div>{(fbLeads.length > 0 || ggLeads.length > 0) && <div className="flex items-center gap-2 mb-3"><span className="text-[10px] font-bold uppercase tracking-widest text-orange-400/60">Craigslist — {clLeads.length} listings</span><div className="flex-1 h-px bg-white/[0.05]" /></div>}<LeadsTable leads={clLeads} onSelect={setSelectedLead} updateStatus={updateLeadStatus} /></div>)}
                      </div>
                    ) : <LeadsTable leads={filtered} onSelect={setSelectedLead} updateStatus={updateLeadStatus} />}
                    <Pagination page={page} totalPages={totalPages} total={totalLeads} perPage={50} onPage={p => { setPage(p); fetchLeads(p); window.scrollTo({ top: 0, behavior: "smooth" }); }} />
                  </>
                )}
              </div>
            )}

            {/* Groups tab */}
            {activeTab === "groups" && (
              <FbGroupsPanel
                headers={headers}
                scrapeStatus={scrapeStatus}
                scraping={scraping}
                onScrapeDone={() => {
                  setTimeout(checkScrapeStatus, 800);
                  setScraping(true);
                }}
              />
            )}

            {/* Map tab */}
            {activeTab === "map" && (
              <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
                <div className="px-4 py-3 border-b border-white/[0.06] flex flex-wrap items-center justify-between gap-2">
                  <div className="flex items-center gap-3">
                    <span className="text-sm font-semibold">Lead Locations</span>
                    <span className="text-[11px] text-white/30">{mapLeads.length} mapped</span>
                    {fbPendingGeoCount > 0 && <span className="flex items-center gap-1.5 text-[10px] text-indigo-400/60"><Loader className="w-3 h-3 animate-spin" />Geocoding {fbPendingGeoCount} FB leads...</span>}
                  </div>
                </div>
                <div className="h-[380px] lg:h-[540px]">
                  {mapLeads.length === 0 ? (
                    <div className="h-full flex flex-col items-center justify-center text-white/20 gap-3"><MapPin className="w-8 h-8 text-white/10" /><span className="text-sm">No leads with location data yet</span></div>
                  ) : (
                    <MapContainer center={[37.8, -96]} zoom={4} minZoom={3} maxZoom={16} scrollWheelZoom style={{ height: "100%", width: "100%" }}>
                      <TileLayer attribution="&copy; OpenStreetMap" url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
                      <MapAutoFit leads={mapLeads} />
                      {mapLeads.map(lead => {
                        const srcCfg = SOURCE_CFG[lead.source];
                        return (
                          <Marker key={lead.post_id} position={[parseFloat(lead.latitude), parseFloat(lead.longitude)]} eventHandlers={{ click: () => setSelectedLead(lead) }}>
                            <Tooltip>
                              <div className="text-xs min-w-[160px]">
                                <div className={`font-bold mb-1 ${srcCfg?.mapColor || "text-gray-600"}`}>{srcCfg ? `${srcCfg.emoji} ${srcCfg.name}` : lead.source}</div>
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

            {/* History tab */}
            {activeTab === "history" && (
              <div className="bg-white/[0.03] border border-white/[0.06] rounded-2xl overflow-hidden">
                <div className="px-5 py-4 border-b border-white/[0.06]"><span className="text-sm font-semibold">Scrape History</span></div>
                {history.length === 0 ? <div className="py-16 text-center text-white/20 text-sm">No scrape runs yet</div> : (
                  <div className="divide-y divide-white/[0.04]">
                    {history.slice((historyPage - 1) * 10, historyPage * 10).map(run => (
                      <div key={run.run_id} className="px-4 py-4 flex items-start justify-between gap-4 hover:bg-white/[0.02] transition-colors">
                        <div className="flex-1 min-w-0">
                          <div className="flex items-center gap-2 flex-wrap mb-1">
                            <span className={`inline-flex items-center gap-1 text-[10px] font-bold uppercase px-2 py-0.5 rounded border ${run.status === "SUCCEEDED" ? "bg-emerald-500/10 border-emerald-500/20 text-emerald-400" : run.status === "RUNNING" ? "bg-amber-500/10 border-amber-500/20 text-amber-400" : run.status === "PARTIAL" ? "bg-yellow-500/10 border-yellow-500/20 text-yellow-400" : "bg-red-500/10 border-red-500/20 text-red-400"}`}>
                              {run.status === "RUNNING" && <Loader className="w-2.5 h-2.5 animate-spin" />}{run.status}
                            </span>
                            <span className="text-xs text-white/60 font-medium">{run.location || "---"}</span>
                          </div>
                          <div className="flex items-center gap-2 text-[11px] text-white/25 flex-wrap">
                            <span>{run.categories?.join(", ") || "---"}</span><span>·</span>
                            <span>{run.sources?.join(", ") || "---"}</span><span>·</span>
                            <span>{new Date(run.started_at).toLocaleString()}</span>
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
                    {Array.from({ length: Math.ceil(history.length / 10) }, (_, i) => <button key={i} onClick={() => setHistoryPage(i + 1)} className={`px-3 py-1.5 rounded-lg text-xs transition-colors ${historyPage === i + 1 ? "bg-blue-600 text-white" : "bg-white/5 text-white/30 hover:text-white"}`}>{i + 1}</button>)}
                  </div>
                )}
              </div>
            )}
          </>)}
        </main>
      </div>

      {selectedLead && <LeadDetailModal lead={selectedLead} onClose={() => setSelectedLead(null)} updateStatus={updateLeadStatus} />}
    </div>
  );
}