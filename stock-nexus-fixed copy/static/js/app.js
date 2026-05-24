// ═══════════════════════════════════════════════════════════
//  STOCK NEXUS — app.js
// ═══════════════════════════════════════════════════════════

let euStocks = [], asiaStocks = [], usStocks = [];
let marketEu = {}, marketAs = {}, marketUs = {};
let currentPage = "dashboard";
let sseSource = null;
let selectedStock = null;
let predMarket = "eu";
let scanMarket = "both";
let euSectorFilter = "ALL";
let usSectorFilter = "ALL";
const CURR_SYMS = {"USD":"$","EUR":"€","GBP":"p","JPY":"¥","HKD":"HK$","INR":"₹","KRW":"₩","CHF":"Fr","DKK":"kr","SEK":"kr","NGN":"₦"};
let trades = JSON.parse(localStorage.getItem("sn_trades") || "[]");

// ── FORMAT HELPERS ─────────────────────────────────────────────
function fp(v, currency = "USD") {
  if (v == null || isNaN(v)) return "—";
  const sym = CURR_SYMS[currency] || "$";
  if (v >= 1e12) return sym + (v/1e12).toFixed(2) + "T";
  if (v >= 1e9)  return sym + (v/1e9).toFixed(2) + "B";
  if (v >= 1e6)  return sym + (v/1e6).toFixed(2) + "M";
  if (v >= 1000) return sym + v.toLocaleString("en", {minimumFractionDigits:2,maximumFractionDigits:2});
  return sym + v.toFixed(2);
}
function currSym(currency) { return CURR_SYMS[currency] || "$"; }
function fpRaw(v) {
  if (v == null || isNaN(v)) return "—";
  if (v >= 1000) return v.toLocaleString("en", {minimumFractionDigits:2,maximumFractionDigits:2});
  return v.toFixed(2);
}
function fPct(v) { return (v >= 0 ? "+" : "") + v.toFixed(2) + "%"; }
function chColor(v) { return v > 0 ? "var(--green)" : v < 0 ? "var(--red)" : "var(--text3)"; }
function chClass(v) { return v > 0 ? "price-up" : v < 0 ? "price-down" : "price-neutral"; }
function dirColor(d) { return d==="BULLISH"?"var(--green)":d==="BEARISH"?"var(--red)":"var(--amber)"; }
function sigBadge(dir, conf) {
  const cls = dir==="BULLISH"?"sig-bull":dir==="BEARISH"?"sig-bear":"sig-neut";
  return `<span class="sig-badge ${cls}">${dir} ${conf?conf+'%':''}</span>`;
}
function quickSignal(change) {
  if (change > 2) return sigBadge("BULLISH", "");
  if (change < -2) return sigBadge("BEARISH", "");
  return sigBadge("NEUTRAL", "");
}

// ── CLOCK ──────────────────────────────────────────────────────
function updateClock() {
  const now = new Date();
  document.getElementById("clock").textContent = now.toUTCString().slice(17,25) + " UTC";
}
setInterval(updateClock, 1000);
updateClock();

// ── INIT ───────────────────────────────────────────────────────
window.addEventListener("DOMContentLoaded", async () => {
  try {
    const res  = await fetch("/api/seed");
    const data = await res.json();
    euStocks   = data.eu_stocks   || [];
    asiaStocks = data.asia_stocks || [];
    usStocks   = data.us_stocks   || [];
    marketEu   = data.market_eu   || {};
    marketAs   = data.market_as   || {};
    marketUs   = data.market_us   || {};

    renderDashboard();
    renderEuAsiaPage();
    renderUsPage();
    renderPredSidebar();
    renderJournal();
    startSSE();

    // Auto-scan: run 2s after load, then every 5 minutes
    setTimeout(() => runScan(true), 2000);
    setInterval(() => runScan(true), 5 * 60 * 1000);
  } catch(e) {
    console.error("Init failed:", e);
    document.getElementById("kpi-grid").innerHTML =
      `<div style="color:var(--red);font-family:var(--font-mono);font-size:10px;padding:20px">
        ⚠ Cannot connect. Make sure app.py is running on port 5002.
      </div>`;
  }

  // Nav
  document.querySelectorAll(".nav-item").forEach(btn => {
    btn.addEventListener("click", () => {
      const page = btn.dataset.page;
      document.querySelectorAll(".nav-item").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
      const el = document.getElementById("page-" + page);
      if (el) { el.classList.add("active"); }
      currentPage = page;
    });
  });

  document.getElementById("analyze-btn").addEventListener("click", runAnalysis);
  document.getElementById("scan-btn").addEventListener("click", runScan);
  document.getElementById("add-trade-btn").addEventListener("click", () => {
    document.getElementById("add-trade-form").classList.toggle("hidden");
  });
  document.getElementById("export-csv-btn").addEventListener("click", exportTrades);

  // Drag-drop on upload zone
  const zone = document.getElementById("upload-zone");
  zone.addEventListener("dragover", e => { e.preventDefault(); zone.classList.add("dragover"); });
  zone.addEventListener("dragleave", () => zone.classList.remove("dragover"));
  zone.addEventListener("drop", e => {
    e.preventDefault(); zone.classList.remove("dragover");
    const file = e.dataTransfer.files[0];
    if (file) handleChartFile(file);
  });
});

// ── SSE ────────────────────────────────────────────────────────
function startSSE() {
  if (sseSource) sseSource.close();
  sseSource = new EventSource("/api/stream");
  sseSource.onopen = () => setWsStatus("connecting");
  sseSource.onmessage = e => {
    const msg = JSON.parse(e.data);
    if (msg.type === "snapshot") {
      marketEu = msg.market_eu || marketEu;
      marketAs = msg.market_as || marketAs;
      marketUs = msg.market_us || marketUs;
      refreshAll();
      setWsStatus("simulated");
    }
    if (msg.type === "tick") handleTick(msg);
    if (msg.type === "status") setWsStatus(msg.status);
  };
  sseSource.onerror = () => setWsStatus("error");
}

function setWsStatus(s) {
  const labels = { connecting:"CONNECTING", live:"LIVE · 15s", simulated:"SIMULATED", error:"RETRYING" };
  document.querySelectorAll(".ws-badge").forEach(el => {
    el.className = "ws-badge " + s;
    el.innerHTML = `<span class="ws-dot"></span> ${labels[s] || s}`;
  });
}

function handleTick(msg) {
  const {id, market, price, change} = msg;
  if (market === "eu" && marketEu[id]) {
    const prev = marketEu[id].price;
    marketEu[id].price  = price;
    marketEu[id].change = change;
    updateTopBarEu(id, price);
    flashCell(id, price > prev ? "up" : price < prev ? "down" : "");
  } else if (market === "as" && marketAs[id]) {
    const prev = marketAs[id].price;
    marketAs[id].price  = price;
    marketAs[id].change = change;
    flashCell(id, price > prev ? "up" : price < prev ? "down" : "");
  } else if (market === "us" && marketUs[id]) {
    const prev = marketUs[id].price;
    marketUs[id].price  = price;
    marketUs[id].change = change;
    updateTopBarUs(id, price);
    flashCell(id, price > prev ? "up" : price < prev ? "down" : "");
  }
  updatePredHeaderPrice(id, price, change);
  if (currentPage === "dashboard") updateDashKPIs();
  updateHeatCell(id, change);
}

function flashCell(id, dir) {
  const el = document.getElementById("price-" + id);
  if (!el || !dir) return;
  el.classList.remove("tick-up","tick-down");
  void el.offsetWidth;
  el.classList.add(dir === "up" ? "tick-up" : "tick-down");
}

function updateTopBarEu(id, price) {
  if (id === "ASML.AS") {
    const el = document.getElementById("hdr-dangcem");
    if (el) el.textContent = "€" + fpRaw(price);
  }
}
function updateTopBarUs(id, price) {
  if (id === "NVDA") {
    const el = document.getElementById("hdr-nvda");
    if (el) el.textContent = "$" + fpRaw(price);
  }
}

function refreshAll() {
  renderDashboard();
  if (currentPage === "ngx") renderEuAsiaPage();
  if (currentPage === "us")  renderUsPage();
}

// ── DASHBOARD ─────────────────────────────────────────────────
function renderDashboard() {
  updateDashKPIs();
  renderDashMovers();
  renderHeatmap();
}

function updateDashKPIs() {
  const allEu   = euStocks.map(s => ({...s, ...marketEu[s.id]}));
  const allAs   = asiaStocks.map(s => ({...s, ...marketAs[s.id]}));
  const allUs   = usStocks.map(s => ({...s, ...marketUs[s.id]}));
  const all = [...allEu, ...allAs, ...allUs];
  const total = all.length;
  const bulls = all.filter(s => (s.change||0) > 0).length;
  const bears = all.filter(s => (s.change||0) < 0).length;
  const flat  = total - bulls - bears;
  const topGainer = [...all].sort((a,b)=>(b.change||0)-(a.change||0))[0];
  const topLoser  = [...all].sort((a,b)=>(a.change||0)-(b.change||0))[0];
  const spyData  = marketUs["SPY"];
  const asmlData = marketEu["ASML.AS"];

  document.getElementById("kpi-grid").innerHTML = `
    <div class="kpi-card" style="--accent:var(--green)">
      <div class="kpi-label">BULLISH STOCKS</div>
      <div class="kpi-val" style="color:var(--green)">${bulls}</div>
      <div class="kpi-sub">${bears} bearish · ${flat} flat</div>
    </div>
    <div class="kpi-card" style="--accent:var(--ng-green)">
      <div class="kpi-label">ASML (EU)</div>
      <div class="kpi-val" style="color:var(--ng-green)">€${fpRaw(asmlData?.price||0)}</div>
      <div class="kpi-sub" style="color:${chColor(asmlData?.change||0)}">${fPct(asmlData?.change||0)}</div>
    </div>
    <div class="kpi-card" style="--accent:var(--us-blue)">
      <div class="kpi-label">S&P 500 ETF (SPY)</div>
      <div class="kpi-val" style="color:var(--us-blue)">$${fpRaw(spyData?.price||0)}</div>
      <div class="kpi-sub" style="color:${chColor(spyData?.change||0)}">${fPct(spyData?.change||0)}</div>
    </div>
    <div class="kpi-card" style="--accent:var(--green)">
      <div class="kpi-label">TOP GAINER</div>
      <div class="kpi-val" style="color:var(--green)">${fPct(topGainer?.change||0)}</div>
      <div class="kpi-sub">${topGainer?.id||"—"}</div>
    </div>
    <div class="kpi-card" style="--accent:var(--red)">
      <div class="kpi-label">TOP LOSER</div>
      <div class="kpi-val" style="color:var(--red)">${fPct(topLoser?.change||0)}</div>
      <div class="kpi-sub">${topLoser?.id||"—"}</div>
    </div>`;
}

function renderDashMovers() {
  const allEuAs = [...euStocks, ...asiaStocks];
  const euAsMkt = id => marketEu[id] || marketAs[id] || {};
  const euAsSorted = [...allEuAs].sort((a,b)=>Math.abs(euAsMkt(b.id)?.change||0)-Math.abs(euAsMkt(a.id)?.change||0));
  const usSorted   = [...usStocks].sort((a,b)=>Math.abs(marketUs[b.id]?.change||0)-Math.abs(marketUs[a.id]?.change||0));

  const row = (s, mkt) => {
    const d = mkt==="us" ? marketUs[s.id]||{} : euAsMkt(s.id);
    const ch = d.change || 0;
    const sym = currSym(s.currency || "USD");
    return `<div class="stock-row" onclick="quickSelect('${s.id}','${mkt}')">
      <div style="display:flex;align-items:center;gap:8px">
        <div class="stock-icon" style="background:${s.color}22;color:${s.color}">${s.id.slice(0,4)}</div>
        <div>
          <div class="stock-name">${s.id}</div>
          <div class="stock-full">${s.name}</div>
        </div>
      </div>
      <div>
        <div class="stock-price" style="color:${chColor(ch)}">${sym}${fpRaw(d.price||0)}</div>
        <div class="stock-change" style="color:${chColor(ch)}">${fPct(ch)}</div>
      </div>
    </div>`;
  };

  const euMkt = s => euStocks.find(x=>x.id===s.id) ? "eu" : "as";
  document.getElementById("dash-ng-movers").innerHTML = euAsSorted.slice(0,5).map(s=>row(s, euMkt(s))).join("");
  document.getElementById("dash-us-movers").innerHTML = usSorted.slice(0,5).map(s=>row(s,"us")).join("");

  // leaders / laggards
  const allWithMkt = [
    ...euStocks.map(s=>({...s, mkt:"eu", change: marketEu[s.id]?.change||0, price: marketEu[s.id]?.price||0})),
    ...asiaStocks.map(s=>({...s, mkt:"as", change: marketAs[s.id]?.change||0, price: marketAs[s.id]?.price||0})),
    ...usStocks.map(s=>({...s, mkt:"us", change: marketUs[s.id]?.change||0, price: marketUs[s.id]?.price||0}))
  ];
  const leaders  = [...allWithMkt].sort((a,b)=>b.change-a.change).slice(0,5);
  const laggards = [...allWithMkt].sort((a,b)=>a.change-b.change).slice(0,5);

  const mktFlag = m => m==="eu"?"🌍":m==="as"?"🌏":"🇺🇸";
  const glRow = s => {
    const sym = currSym(s.currency || "USD");
    return `<div class="stock-row" onclick="quickSelect('${s.id}','${s.mkt}')">
      <div style="display:flex;align-items:center;gap:8px">
        <div class="stock-icon" style="background:${s.color}22;color:${s.color};width:24px;height:24px;font-size:7px">${s.id.slice(0,3)}</div>
        <div>
          <div style="font-size:11px;font-weight:700">${s.id}</div>
          <div style="font-size:8px;color:var(--text3)">${mktFlag(s.mkt)} ${sym}${fpRaw(s.price)}</div>
        </div>
      </div>
      <div style="font-family:var(--font-mono);font-size:11px;font-weight:700;color:${chColor(s.change)}">${fPct(s.change)}</div>
    </div>`;
  };
  document.getElementById("dash-leaders").innerHTML  = leaders.map(glRow).join("");
  document.getElementById("dash-laggards").innerHTML = laggards.map(glRow).join("");
}

function renderHeatmap() {
  const all = [
    ...euStocks.map(s=>({...s, mkt:"eu", change: marketEu[s.id]?.change||0})),
    ...asiaStocks.map(s=>({...s, mkt:"as", change: marketAs[s.id]?.change||0})),
    ...usStocks.map(s=>({...s, mkt:"us", change: marketUs[s.id]?.change||0}))
  ];
  document.getElementById("heatmap").innerHTML = all.map(s => {
    const ch = s.change;
    const intensity = Math.min(1, Math.abs(ch) / 5);
    const bg = ch > 0
      ? `rgba(0,200,83,${0.1+intensity*0.5})`
      : ch < 0
        ? `rgba(255,61,87,${0.1+intensity*0.5})`
        : "var(--bg3)";
    const tc = ch > 0 ? "#00e676" : ch < 0 ? "#ff3d57" : "var(--text3)";
    return `<div class="heat-cell" style="background:${bg};color:${tc};border-color:${tc}33"
      id="heat-${s.id}" onclick="quickSelect('${s.id}','${s.mkt}')">
      <div style="font-weight:700;font-size:10px">${s.id}</div>
      <div style="font-size:9px">${fPct(ch)}</div>
    </div>`;
  }).join("");
}

function updateHeatCell(id, change) {
  const el = document.getElementById("heat-" + id);
  if (!el) return;
  const ch = change;
  const intensity = Math.min(1, Math.abs(ch)/5);
  const bg = ch>0?`rgba(0,200,83,${0.1+intensity*0.5})`:ch<0?`rgba(255,61,87,${0.1+intensity*0.5})`:"var(--bg3)";
  const tc = ch>0?"#00e676":ch<0?"#ff3d57":"var(--text3)";
  el.style.background = bg;
  el.style.color = tc;
  el.children[1].textContent = fPct(ch);
}

// ── EU/ASIA PAGE ──────────────────────────────────────────────
function renderEuAsiaPage() {
  const allEuAs = [...euStocks, ...asiaStocks];
  const sectors = ["ALL", ...new Set(allEuAs.map(s=>s.sector))];
  document.getElementById("ngx-sector-filters").innerHTML = sectors.map(s =>
    `<button class="cat-btn${s===euSectorFilter?" active":""}" onclick="setEuSector('${s}')">${s}</button>`
  ).join("");

  const filtered = euSectorFilter === "ALL" ? allEuAs : allEuAs.filter(s=>s.sector===euSectorFilter);
  document.getElementById("ngx-list").innerHTML = filtered.map((s,i) => {
    const isEu = !!euStocks.find(x=>x.id===s.id);
    const d = isEu ? (marketEu[s.id]||{}) : (marketAs[s.id]||{});
    const mktType = isEu ? "eu" : "as";
    const ch = d.change || 0;
    const sym = currSym(s.currency || "EUR");
    const flag = isEu ? "🌍" : "🌏";
    return `<div class="table-row-ng" onclick="quickSelect('${s.id}','${mktType}')">
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3)">${i+1}</div>
      <div style="display:flex;align-items:center;gap:8px">
        <div class="stock-icon" style="background:${s.color}22;color:${s.color};width:28px;height:28px;font-size:7px">${s.id.slice(0,4)}</div>
        <div>
          <div style="font-family:var(--font-main);font-size:13px;font-weight:600">${flag} ${s.id}</div>
          <div style="font-family:var(--font-mono);font-size:7px;color:var(--text3)">${s.name}</div>
        </div>
      </div>
      <div id="price-${s.id}" style="font-family:var(--font-mono);font-size:12px;color:${chColor(ch)}">${sym}${fpRaw(d.price||0)}</div>
      <div style="font-family:var(--font-mono);font-size:11px;color:${chColor(ch)}">${fPct(ch)}</div>
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3)">${sym}${fpRaw(d.high||0)} / ${sym}${fpRaw(d.low||0)}</div>
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3)">${fmtVol(d.vol||0)}</div>
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text2)">${s.sector}</div>
      <div>${quickSignal(ch)}</div>
    </div>`;
  }).join("");
}
function setEuSector(s) { euSectorFilter = s; renderEuAsiaPage(); }

// ── US PAGE ────────────────────────────────────────────────────
function renderUsPage() {
  const sectors = ["ALL", ...new Set(usStocks.map(s=>s.sector))];
  document.getElementById("us-sector-filters").innerHTML = sectors.map(s =>
    `<button class="cat-btn${s===usSectorFilter?" active":""}" onclick="setUsSector('${s}')">${s}</button>`
  ).join("");

  const filtered = usSectorFilter === "ALL" ? usStocks : usStocks.filter(s=>s.sector===usSectorFilter);
  document.getElementById("us-list").innerHTML = filtered.map((s,i) => {
    const d = marketUs[s.id] || {};
    const ch = d.change || 0;
    return `<div class="table-row-ng" onclick="quickSelect('${s.id}','us')">
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3)">${i+1}</div>
      <div style="display:flex;align-items:center;gap:8px">
        <div class="stock-icon" style="background:${s.color}22;color:${s.color};width:28px;height:28px;font-size:7px">${s.id.slice(0,4)}</div>
        <div>
          <div style="font-family:var(--font-main);font-size:13px;font-weight:600">${s.id}</div>
          <div style="font-family:var(--font-mono);font-size:7px;color:var(--text3)">${s.name}</div>
        </div>
      </div>
      <div id="price-${s.id}" style="font-family:var(--font-mono);font-size:12px;color:${chColor(ch)}">$${fpRaw(d.price||0)}</div>
      <div style="font-family:var(--font-mono);font-size:11px;color:${chColor(ch)}">${fPct(ch)}</div>
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3)">${d.mktcap||"—"}</div>
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3)">${fmtVol(d.vol||0)}</div>
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text2)">${s.sector}</div>
      <div>${quickSignal(ch)}</div>
    </div>`;
  }).join("");
}
function setUsSector(s) { usSectorFilter = s; renderUsPage(); }

function fmtVol(v) {
  if (!v) return "—";
  if (v >= 1e9) return (v/1e9).toFixed(1)+"B";
  if (v >= 1e6) return (v/1e6).toFixed(1)+"M";
  if (v >= 1e3) return (v/1e3).toFixed(1)+"K";
  return v;
}

// ── PREDICTOR ─────────────────────────────────────────────────
function setPredMarket(mkt) {
  predMarket = mkt;
  document.querySelectorAll(".mkt-btn").forEach(b => {
    b.classList.toggle("active", b.dataset.mkt === mkt);
  });
  selectedStock = null;
  renderPredSidebar();
  document.getElementById("pred-header").innerHTML = `<div class="empty-pred-header">SELECT A STOCK TO ANALYZE</div>`;
  document.getElementById("pred-content").innerHTML = `<div class="empty-state"><div class="empty-title">STOCK NEXUS</div><div class="empty-sub">SELECT A STOCK AND CLICK ANALYZE<br/>FOR AI-POWERED EQUITY PREDICTIONS</div></div>`;
}

function getMktList(mkt)  { return mkt==="eu"?euStocks:mkt==="as"?asiaStocks:usStocks; }
function getMktData(mkt)  { return mkt==="eu"?marketEu:mkt==="as"?marketAs:marketUs; }
function getMktFlag(mkt)  { return mkt==="eu"?"🌍 EU":mkt==="as"?"🌏 ASIA":"🇺🇸 US"; }

function renderPredSidebar() {
  const list = getMktList(predMarket);
  const mkt  = getMktData(predMarket);
  document.getElementById("pred-stocklist").innerHTML = list.map(s => {
    const d   = mkt[s.id] || {};
    const ch  = d.change || 0;
    const sym = currSym(s.currency || "USD");
    return `<div class="pred-coin-item${selectedStock===s.id?" active":""}"
      id="pred-item-${s.id}" onclick="selectStock('${s.id}','${predMarket}')">
      <div class="pred-coin-icon" style="background:${s.color}22;color:${s.color}">${s.id.slice(0,4)}</div>
      <div>
        <div class="pred-coin-name">${s.id}</div>
        <div class="pred-coin-full">${s.name.slice(0,22)}</div>
      </div>
      <div style="text-align:right;margin-left:auto">
        <div class="pred-coin-price" style="color:${chColor(ch)}">${sym}${fpRaw(d.price||0)}</div>
        <div class="pred-coin-ch" style="color:${chColor(ch)}">${fPct(ch)}</div>
      </div>
    </div>`;
  }).join("");
}

function filterPredStocks(q) {
  const list = getMktList(predMarket);
  const mkt  = getMktData(predMarket);
  const filtered = q ? list.filter(s => s.id.toLowerCase().includes(q.toLowerCase()) || s.name.toLowerCase().includes(q.toLowerCase())) : list;
  document.getElementById("pred-stocklist").innerHTML = filtered.map(s => {
    const d   = mkt[s.id] || {};
    const ch  = d.change || 0;
    const sym = currSym(s.currency || "USD");
    return `<div class="pred-coin-item${selectedStock===s.id?" active":""}"
      onclick="selectStock('${s.id}','${predMarket}')">
      <div class="pred-coin-icon" style="background:${s.color}22;color:${s.color}">${s.id.slice(0,4)}</div>
      <div>
        <div class="pred-coin-name">${s.id}</div>
        <div class="pred-coin-full">${s.name.slice(0,22)}</div>
      </div>
      <div style="text-align:right;margin-left:auto">
        <div class="pred-coin-price" style="color:${chColor(ch)}">${sym}${fpRaw(d.price||0)}</div>
        <div class="pred-coin-ch" style="color:${chColor(ch)}">${fPct(ch)}</div>
      </div>
    </div>`;
  }).join("");
}

function selectStock(id, mkt) {
  selectedStock = id;
  predMarket = mkt;
  const list = getMktList(mkt);
  const mkd  = getMktData(mkt);
  const info = list.find(s => s.id === id);
  const d    = mkd[id] || {};
  const ch   = d.change || 0;
  const sym  = currSym(info?.currency || "USD");

  document.querySelectorAll(".pred-coin-item").forEach(el => el.classList.remove("active"));
  const el = document.getElementById("pred-item-" + id);
  if (el) el.classList.add("active");

  document.getElementById("pred-header").innerHTML = `
    <div class="pred-stock-header">
      <div class="pred-stock-icon" style="background:${info?.color||"#333"}22;color:${info?.color||"#fff"}">${id.slice(0,4)}</div>
      <div>
        <div class="pred-stock-id">${id}</div>
        <div class="pred-stock-name">${info?.name||""} · ${getMktFlag(mkt)}</div>
      </div>
      <div style="margin-left:auto;text-align:right">
        <div class="pred-stock-price" style="color:${chColor(ch)}">${sym}${fpRaw(d.price||0)}</div>
        <div class="pred-stock-ch" style="color:${chColor(ch)}">${fPct(ch)}</div>
      </div>
    </div>`;

  document.getElementById("analyze-btn").textContent = `ANALYZE ${id}`;

  // Navigate to predictor if not already
  if (currentPage !== "predictor") {
    document.querySelectorAll(".nav-item").forEach(b => {
      b.classList.toggle("active", b.dataset.page === "predictor");
    });
    document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
    document.getElementById("page-predictor").classList.add("active");
    currentPage = "predictor";
  }
}

function quickSelect(id, mkt) {
  setPredMarket(mkt);
  selectStock(id, mkt);
}

function updatePredHeaderPrice(id, price, change) {
  if (id !== selectedStock) return;
  const list = getMktList(predMarket);
  const info = list.find(s => s.id === id);
  const sym = currSym(info?.currency || "USD");
  const prEl = document.querySelector(".pred-stock-price");
  const chEl = document.querySelector(".pred-stock-ch");
  if (prEl) { prEl.textContent = sym + fpRaw(price); prEl.style.color = chColor(change); }
  if (chEl) { chEl.textContent = fPct(change); chEl.style.color = chColor(change); }
}

// ── ANALYSIS ──────────────────────────────────────────────────
async function runAnalysis() {
  if (!selectedStock) {
    alert("Select a stock first.");
    return;
  }
  const btn = document.getElementById("analyze-btn");
  btn.disabled = true; btn.textContent = "ANALYZING...";

  const balance  = parseFloat(document.getElementById("pred-balance").value) || 0;
  const riskPct  = parseFloat(document.getElementById("pred-risk-pct").value) || 1;
  const tradeSize = balance > 0 ? balance * riskPct / 100 : 0;

  document.getElementById("pred-content").innerHTML =
    `<div class="loading"><div class="spin"></div> FETCHING REAL-TIME DATA & COMPUTING INDICATORS...</div>`;

  try {
    const res = await fetch("/api/analyze", {
      method: "POST",
      headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ stockId: selectedStock, market: predMarket, tradeSize })
    });
    const data = await res.json();
    renderAnalysisResult(data, balance, riskPct);
  } catch(e) {
    document.getElementById("pred-content").innerHTML =
      `<div style="color:var(--red);font-family:var(--font-mono);font-size:10px;padding:20px">ERROR: ${e.message}</div>`;
  }
  btn.disabled = false; btn.textContent = "ANALYZE " + selectedStock;
}

function renderAnalysisResult(d, balance, riskPct) {
  if (d.error) {
    document.getElementById("pred-content").innerHTML =
      `<div style="color:var(--red);font-family:var(--font-mono);font-size:10px;padding:20px">ERROR: ${d.error}</div>`;
    return;
  }

  const dir   = d.prediction?.dir || "NEUTRAL";
  const conf  = d.prediction?.conf || 0;
  const bp    = d.prediction?.bullPct || 50;
  const t1    = d.prediction?.targets?.t1;
  const t2    = d.prediction?.targets?.t2;
  const stop  = d.prediction?.targets?.stop;
  const rr    = d.prediction?.rr;
  const ind   = d.indicators || {};
  const sym   = currSym(d.stockInfo?.currency || "USD");
  const price = d.price || 0;
  const change= d.change || 0;
  const color = d.stockInfo?.color || "#4A9EFF";
  const sigCls = dir==="BULLISH"?"bull":dir==="BEARISH"?"bear":"neut";
  const sigColor = dirColor(dir);

  let posHtml = "";
  if (d.posSizing) {
    const ps = d.posSizing;
    posHtml = `
    <div style="margin-bottom:14px">
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px;margin-bottom:8px">POSITION SIZING</div>
      <div class="pos-sizing-grid">
        <div class="analysis-card"><div class="analysis-label">RISK AMOUNT</div><div class="analysis-val" style="color:var(--red);font-size:18px">${sym}${fpRaw(ps.tradeSize)}</div></div>
        <div class="analysis-card"><div class="analysis-label">UNITS</div><div class="analysis-val" style="font-size:18px">${ps.units.toLocaleString("en",{maximumFractionDigits:0})}</div></div>
        <div class="analysis-card"><div class="analysis-label">TP1 PROFIT</div><div class="analysis-val" style="color:var(--green);font-size:18px">${sym}${fpRaw(ps.t1Profit)}</div></div>
        <div class="analysis-card"><div class="analysis-label">TP2 PROFIT</div><div class="analysis-val" style="color:var(--green);font-size:18px">${sym}${fpRaw(ps.t2Profit)}</div></div>
        <div class="analysis-card"><div class="analysis-label">R/R (TP1)</div><div class="analysis-val" style="color:var(--cyan);font-size:18px">${ps.rr1}:1</div></div>
        <div class="analysis-card"><div class="analysis-label">R/R (TP2)</div><div class="analysis-val" style="color:var(--cyan);font-size:18px">${ps.rr2}:1</div></div>
      </div>
    </div>`;
  }

  let newsHtml = "";
  if (d.news && d.news.length > 0) {
    newsHtml = `
    <div style="margin-bottom:14px">
      <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px;margin-bottom:8px">
        MARKET NEWS · SENTIMENT: 
        <span style="color:${d.newsBull>0.55?"var(--green)":d.newsBull<0.45?"var(--red)":"var(--amber)"}">${d.newsBull>0.55?"BULLISH":d.newsBull<0.45?"BEARISH":"NEUTRAL"}</span>
      </div>
      <div class="news-list">
        ${d.news.slice(0,4).map(n=>`
          <div class="news-item" onclick="window.open('${n.url||"#"}','_blank')">
            <div class="news-title">${n.title||"—"}</div>
            <div class="news-meta">${n.source||""} · ${n.score!=null?`SCORE: ${n.score>0?"+":""}${n.score}`:""}</div>
          </div>`).join("")}
      </div>
    </div>`;
  }

  document.getElementById("pred-content").innerHTML = `
  <div class="fadeIn">
    <!-- SIGNAL BOX -->
    <div class="signal-box ${sigCls}" style="margin-bottom:16px">
      <div class="signal-dir" style="color:${sigColor}">${dir}</div>
      <div class="signal-conf" style="color:${sigColor}">${conf}% CONFIDENCE · ${d.marketPhase||""}</div>
      <div class="conf-bar-wrap" style="max-width:300px;margin:8px auto 0">
        <div class="conf-bar" style="width:${conf}%;background:${sigColor}"></div>
      </div>
      <div style="font-family:var(--font-mono);font-size:8px;color:var(--text3);margin-top:6px">
        ${d.dataSource==="real_candles"?"✓ REAL CANDLE DATA":"⚡ APPROXIMATION"} · 
        BULL ${bp}% / BEAR ${100-bp}%
      </div>
    </div>

    <!-- CONSENSUS SIGNAL -->
    ${(()=>{
      const cs = d.consensus;
      if (!cs) return "";
      const isFull      = cs.agreement === "FULL";
      const isRuleOnly  = cs.agreement === "RULE_ONLY";
      const noConsensus = cs.signal === "NO_CONSENSUS";
      const isBuy  = cs.signal && cs.signal.includes("BUY");
      const isSell = cs.signal && cs.signal.includes("SELL");

      const csColor  = isFull && isBuy  ? "var(--green)"
                     : isFull && isSell ? "var(--red)"
                     : isRuleOnly       ? "var(--amber)"
                     : "var(--text3)";
      const csBg     = isFull && isBuy  ? "rgba(0,255,136,0.06)"
                     : isFull && isSell ? "rgba(255,61,87,0.06)"
                     : isRuleOnly       ? "rgba(255,170,0,0.06)"
                     : "rgba(255,255,255,0.02)";
      const csBorder = isFull && isBuy  ? "var(--green)"
                     : isFull && isSell ? "var(--red)"
                     : isRuleOnly       ? "var(--amber)"
                     : "var(--border)";
      const icon = isFull && isBuy ? "\u25b2" : isFull && isSell ? "\u25bc" : noConsensus ? "\u2014" : "\u25c6";

      const sys  = cs.systems || {};
      function mkDot(val, label) {
        const agree = val === true || (isBuy && val === "BULL") || (isSell && val === "BEAR");
        const clash = val === false || (isBuy && val === "BEAR") || (isSell && val === "BULL");
        const c2    = agree ? "var(--green)" : clash ? "var(--red)" : "var(--text3)";
        const mark  = val === null || val === undefined ? "—" : agree ? "\u2713" : clash ? "\u2717" : val;
        const suffix = val === null || val === undefined ? " (no data for this stock)" : "";
        return '<span style="font-family:var(--font-mono);font-size:9px;color:' + c2 + ';margin-right:12px">' + mark + ' ' + label + suffix + '</span>';
      }

      let confBar = "";
      if (isFull) {
        confBar = '<div style="margin:8px 0 0">'
          + '<div style="display:flex;justify-content:space-between;font-family:var(--font-mono);font-size:8px;color:var(--text3);margin-bottom:4px">'
          + '<span>COMBINED CONFIDENCE</span><span>' + cs.confidence + '%</span></div>'
          + '<div style="height:3px;background:var(--border);border-radius:2px">'
          + '<div style="height:3px;width:' + cs.confidence + '%;background:' + csColor + ';border-radius:2px;transition:width .4s"></div>'
          + '</div></div>';
      }

      let probLine = "";
      if (cs.ml_prob_up != null) {
        probLine = '<div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);margin-top:6px">'
          + 'ML PROB UP: <span style="color:' + csColor + '">' + (cs.ml_prob_up * 100).toFixed(1) + '%</span></div>';
      }

      let badges = "";
      if (isFull)       badges = '<span style="font-family:var(--font-mono);font-size:9px;color:var(--green);margin-left:10px;background:rgba(0,255,136,0.1);padding:2px 8px;border-radius:3px">ALL SYSTEMS AGREE</span>';
      else if (isRuleOnly)   badges = '<span style="font-family:var(--font-mono);font-size:9px;color:var(--amber);margin-left:10px;background:rgba(255,170,0,0.1);padding:2px 8px;border-radius:3px">RULE-BASED ONLY</span>';
      else if (noConsensus)  badges = '<span style="font-family:var(--font-mono);font-size:9px;color:var(--text3);margin-left:10px;background:rgba(255,255,255,0.05);padding:2px 8px;border-radius:3px">WAIT \u2014 MIXED SIGNALS</span>';

      const tradeColor = cs.tradeable ? "var(--green)" : "var(--text3)";
      const tradeBg    = cs.tradeable ? "rgba(0,255,136,0.1)" : "rgba(255,255,255,0.04)";
      const tradeBorder= cs.tradeable ? "var(--green)" : "var(--border)";
      const tradeLabel = cs.tradeable ? "\u2713 TRADEABLE" : "\u2717 NOT TRADEABLE";

      return '<div style="border:1px solid ' + csBorder + '44;background:' + csBg + ';border-radius:8px;padding:14px 16px;margin-bottom:16px">'
        + '<div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px;margin-bottom:10px">CONSENSUS SIGNAL</div>'
        + '<div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px">'
        + '<div>'
        + '<span style="font-family:var(--font-mono);font-size:22px;font-weight:700;color:' + csColor + ';letter-spacing:1px">' + icon + ' ' + cs.signal + '</span>'
        + '<br>'
        + badges
        + '</div>'
        + '<div style="font-family:var(--font-mono);font-size:9px;color:' + tradeColor + ';background:' + tradeBg + ';padding:4px 10px;border-radius:4px;border:1px solid ' + tradeBorder + '">' + tradeLabel + '</div>'
        + '</div>'
        + '<div style="margin-top:10px">' + mkDot(sys.rule,"RULE-BASED") + mkDot(sys.xgb,"XGBOOST") + mkDot(sys.lstm,"LSTM") + '</div>'
        + '<div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);margin-top:8px;line-height:1.5">' + (cs.reason||"") + '</div>'
        + confBar + probLine
        + '</div>';
    })()}
    <!-- OVERVIEW CARDS -->
    <div class="analysis-top" style="margin-bottom:14px">
      <div class="analysis-card">
        <div class="analysis-label">CURRENT PRICE</div>
        <div class="analysis-val" style="color:${color}">${sym}${fpRaw(price)}</div>
        <div class="analysis-sub" style="color:${chColor(change)}">${fPct(change)} today</div>
      </div>
      <div class="analysis-card">
        <div class="analysis-label">MARKET CAP</div>
        <div class="analysis-val" style="font-size:18px">${getMktData(predMarket)[selectedStock]?.mktcap||"—"}</div>
        <div class="analysis-sub">${d.stockInfo?.sector||""}</div>
      </div>
      <div class="analysis-card">
        <div class="analysis-label">TREND STRENGTH</div>
        <div class="analysis-val" style="font-size:18px">${d.trendStr||"—"}</div>
        <div class="analysis-sub">ADX ${ind.adx||"—"}</div>
      </div>
    </div>

    <!-- KEY LEVELS -->
    <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px;margin-bottom:8px">KEY LEVELS · R/R ${rr||"—"}:1</div>
    <div class="levels-grid" style="margin-bottom:14px">
      <div class="level-card" style="border-color:var(--green)44">
        <div class="level-label" style="color:var(--green)">TARGET 1</div>
        <div class="level-price" style="color:var(--green)">${sym}${fpRaw(t1)}</div>
        <div class="level-dist">+${fpRaw(Math.abs((t1||price)-price))} (${(Math.abs(((t1||price)-price)/price)*100).toFixed(2)}%)</div>
      </div>
      <div class="level-card" style="border-color:var(--cyan)44">
        <div class="level-label" style="color:var(--cyan)">TARGET 2</div>
        <div class="level-price" style="color:var(--cyan)">${sym}${fpRaw(t2)}</div>
        <div class="level-dist">+${fpRaw(Math.abs((t2||price)-price))} (${(Math.abs(((t2||price)-price)/price)*100).toFixed(2)}%)</div>
      </div>
      <div class="level-card" style="border-color:var(--red)44">
        <div class="level-label" style="color:var(--red)">STOP LOSS</div>
        <div class="level-price" style="color:var(--red)">${sym}${fpRaw(stop)}</div>
        <div class="level-dist">-${fpRaw(Math.abs((stop||price)-price))} (${(Math.abs(((stop||price)-price)/price)*100).toFixed(2)}%)</div>
      </div>
    </div>

    <!-- INDICATORS -->
    <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px;margin-bottom:8px">TECHNICAL INDICATORS</div>
    <div class="ind-grid" style="margin-bottom:14px">
      <div class="ind-card">
        <div class="ind-label">RSI 14</div>
        <div class="ind-val" style="color:${ind.rsi>70?"var(--red)":ind.rsi<30?"var(--green)":"var(--text)"}">${ind.rsi||"—"}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">RSI 7</div>
        <div class="ind-val" style="color:${ind.rsi_7>70?"var(--red)":ind.rsi_7<30?"var(--green)":"var(--text)"}">${ind.rsi_7||"—"}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">MACD</div>
        <div class="ind-val" style="color:${ind.macd==="BULLISH"?"var(--green)":"var(--red)"}">${ind.macd||"—"}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">BB POSITION</div>
        <div class="ind-val" style="color:${ind.bb_pct>80?"var(--red)":ind.bb_pct<20?"var(--green)":"var(--text)"}">${ind.bb_pct?.toFixed(1)||"—"}%</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">STOCH K/D</div>
        <div class="ind-val">${ind.stoch_k?.toFixed(0)||"—"}/${ind.stoch_d?.toFixed(0)||"—"}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">ADX</div>
        <div class="ind-val" style="color:${ind.adx>50?"var(--green)":ind.adx>25?"var(--amber)":"var(--text3)"}">${ind.adx?.toFixed(1)||"—"}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">EMA 9</div>
        <div class="ind-val">${sym}${fpRaw(ind.ema9)}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">EMA 50</div>
        <div class="ind-val">${sym}${fpRaw(ind.ema50)}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">EMA 200</div>
        <div class="ind-val">${sym}${fpRaw(ind.ema200)}</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">ATR</div>
        <div class="ind-val">${ind.atr?.toFixed(3)||"—"}%</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">VOLUME RATIO</div>
        <div class="ind-val" style="color:${ind.vol_ratio>2?"var(--amber)":"var(--text)"}">${ind.vol_ratio?.toFixed(2)||"—"}x</div>
      </div>
      <div class="ind-card">
        <div class="ind-label">CANDLE</div>
        <div class="ind-val" style="font-size:10px">${ind.candle||"—"}</div>
      </div>
    </div>

    ${posHtml}

    <!-- AI TEXT -->
    <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px;margin-bottom:8px">AI ANALYSIS SUMMARY</div>
    <div class="ai-text-box">${d.aiText||""}</div>

    ${newsHtml}

    <!-- QUICK ADD TO JOURNAL -->
    <div style="display:flex;gap:8px;margin-top:8px">
      <button class="btn-secondary" onclick="prefillJournal('${d.stockId}','${predMarket}','LONG','${price}','${stop}','${t1}','${t2}')">+ ADD LONG TO JOURNAL</button>
      <button class="btn-secondary" onclick="prefillJournal('${d.stockId}','${predMarket}','SHORT','${price}','${stop}','${t1}','${t2}')">+ ADD SHORT TO JOURNAL</button>
    </div>
  </div>`;
}

// ── SCANNER ────────────────────────────────────────────────────
function setScanMarket(mkt, btn) {
  scanMarket = mkt;
  document.querySelectorAll(".cat-filters .cat-btn").forEach(b => b.classList.remove("active"));
  if (btn) btn.classList.add("active");
}

async function runScan(silent = false) {
  const btn = document.getElementById("scan-btn");
  btn.disabled = true;
  if (!silent) {
    btn.textContent = "SCANNING...";
    document.getElementById("scanner-content").innerHTML =
      `<div class="loading"><div class="spin"></div> SCANNING ALL STOCKS...</div>`;
  }
  try {
    const res  = await fetch("/api/scan", {
      method: "POST", headers: {"Content-Type":"application/json"},
      body: JSON.stringify({ market: silent ? "both" : scanMarket })
    });
    const data = await res.json();
    renderScanResults(data, silent);
  } catch(e) {
    if (!silent) document.getElementById("scanner-content").innerHTML =
      `<div style="color:var(--red);font-family:var(--font-mono);font-size:10px;padding:20px">ERROR: ${e.message}</div>`;
  }
  btn.disabled = false; btn.textContent = "⚡ RUN SCAN";
}

function renderScanResults(data, silent = false) {
  const results = data.results || [];

  // Update scan nav badge always
  const scanNav = document.querySelector('.nav-item[data-page="scanner"]');
  if (scanNav && results.length) {
    const bulls = results.filter(r=>r.direction==="BULLISH").length;
    const bears = results.filter(r=>r.direction==="BEARISH").length;
    scanNav.querySelector("span:last-child").innerHTML =
      `SCAN <span style="font-size:7px;color:var(--green)">${bulls}▲</span><span style="font-size:7px;color:var(--red)">${bears}▼</span>`;
  }

  // Only update scanner page content if not silent (auto-scan doesn't overwrite what user sees)
  if (silent && currentPage !== "scanner") return;

  if (!results.length) {
    document.getElementById("scanner-content").innerHTML =
      `<div class="empty-state"><div class="empty-title">NO SIGNALS</div><div class="empty-sub">NO HIGH-CONFIDENCE SETUPS FOUND RIGHT NOW</div></div>`;
    return;
  }
  const bulls = results.filter(r=>r.direction==="BULLISH");
  const bears = results.filter(r=>r.direction==="BEARISH");
  const lastScan = new Date().toUTCString().slice(17,25);

  document.getElementById("scanner-content").innerHTML = `
    <div style="font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px;margin-bottom:16px;display:flex;justify-content:space-between;align-items:center">
      <span>FOUND ${results.length} SIGNALS ·
      <span style="color:var(--green)">${bulls.length} BULLISH</span> ·
      <span style="color:var(--red)">${bears.length} BEARISH</span></span>
      <span style="font-size:8px;color:var(--text4)">AUTO-UPDATED ${lastScan} UTC · refreshes every 5 min</span>
    </div>
    </div>
    <div class="scanner-grid">
      ${results.map(r => {
        const sigColor = dirColor(r.direction);
        const sym = currSym(r.currency || "USD");
        const mktLabel = r.market==="eu"?"🌍 EU":r.market==="as"?"🌏 ASIA":"🇺🇸 US";
        return `<div class="scan-card ${r.direction==="BULLISH"?"bull":"bear"}" onclick="quickSelect('${r.id}','${r.market}')">
          <div class="scan-header">
            <div>
              <div class="scan-name" style="color:${r.color}">${r.id}</div>
              <div class="scan-mkt">${mktLabel} · ${r.sector}</div>
            </div>
            <div>
              <div class="scan-conf" style="color:${sigColor}">${r.conf}%</div>
              <div style="font-family:var(--font-mono);font-size:8px;color:${sigColor}">${r.direction}</div>
            </div>
          </div>
          <div style="display:flex;justify-content:space-between;margin-bottom:8px">
            <div style="font-family:var(--font-title);font-size:18px;color:${chColor(r.change)}">${sym}${fpRaw(r.price)}</div>
            <div style="font-family:var(--font-mono);font-size:11px;color:${chColor(r.change)}">${fPct(r.change)}</div>
          </div>
          <div class="conf-bar-wrap"><div class="conf-bar" style="width:${r.conf}%;background:${sigColor}"></div></div>
          <div class="scan-metrics" style="margin-top:6px">
            <div class="scan-metric">RSI <span>${r.rsi}</span></div>
            <div class="scan-metric">ADX <span>${r.adx}</span></div>
            <div class="scan-metric">BULL <span>${r.bullPct}%</span></div>
          </div>
          <div style="font-family:var(--font-mono);font-size:8px;color:var(--text4);margin-top:6px">Click to analyze →</div>
        </div>`;
      }).join("")}
    </div>`;
}

// ── CHART VISION AI ────────────────────────────────────────────
let chartFile = null;

function handleChartUpload(input) {
  if (input.files && input.files[0]) handleChartFile(input.files[0]);
}

function handleChartFile(file) {
  chartFile = file;
  const reader = new FileReader();
  reader.onload = e => {
    document.getElementById("upload-zone").style.display = "none";
    document.getElementById("chart-preview-wrap").style.display = "block";
    document.getElementById("chart-preview").src = e.target.result;
  };
  reader.readAsDataURL(file);
}

function clearChartUpload() {
  chartFile = null;
  document.getElementById("upload-zone").style.display = "block";
  document.getElementById("chart-preview-wrap").style.display = "none";
  document.getElementById("chart-preview").src = "";
  document.getElementById("chart-file-input").value = "";
  document.getElementById("chart-analyze-status").textContent = "";
}

async function runChartAnalysis() {
  if (!chartFile) {
    document.getElementById("chart-analyze-status").textContent = "⚠ Please upload a chart image first.";
    return;
  }

  const btn = document.getElementById("chart-analyze-btn");
  btn.disabled = true; btn.textContent = "🔍 SEARCHING...";
  document.getElementById("chart-analyze-status").textContent = "Searching database for similar chart...";
  document.getElementById("chartai-empty").style.display = "none";
  document.getElementById("chartai-result").style.display = "none";
  document.getElementById("chartai-result-panel").innerHTML = `<div class="loading"><div class="spin"></div> MATCHING YOUR CHART AGAINST HISTORICAL DATABASE...</div>`;

  const formData = new FormData();
  formData.append("image", chartFile);

  // Pass current indicators if a stock is selected in the analysis panel
  if (selectedStock) {
    const mkt = getMktData(predMarket);
    const state = mkt[selectedStock] || {};
    // Send whatever indicator data we have for better matching
    const indProxy = {
      rsi: Math.min(98, Math.max(2, 50 + (state.change||0) * 4.2)),
      bb_pct: 50,
      stoch_k: 50,
      adx: Math.min(80, Math.max(10, Math.abs(state.change||0)*8+20)),
      vol_ratio: 1.0,
      atr: 0.5,
    };
    formData.append("indicators", JSON.stringify(indProxy));
  }

  try {
    const res  = await fetch("/api/analyze_chart", { method: "POST", body: formData });
    const data = await res.json();
    renderChartResult(data);
    document.getElementById("chart-analyze-status").textContent = data.success ? "✓ Match found" : "⚠ No match";
  } catch(e) {
    document.getElementById("chartai-result-panel").innerHTML =
      `<div style="color:var(--red);font-family:var(--font-mono);font-size:10px;padding:20px">ERROR: ${e.message}</div>`;
    document.getElementById("chart-analyze-status").textContent = "⚠ Error during analysis";
  } finally {
    btn.disabled = false; btn.textContent = "👁 ANALYZE CHART WITH AI";
  }
}

function renderChartResult(data) {
  if (!data.success) {
    const isNotBuilt = (data.error||"").includes("not built") || (data.error||"").includes("build_db");
    document.getElementById("chartai-result-panel").innerHTML = `
      <div style="padding:20px">
        <div style="color:var(--red);font-family:var(--font-mono);font-size:10px;margin-bottom:12px">⚠ ANALYSIS FAILED</div>
        <div style="color:var(--text2);font-family:var(--font-mono);font-size:10px;white-space:pre-wrap">${data.error||"Unknown error"}</div>
        ${isNotBuilt ? `
        <div style="color:var(--amber);font-family:var(--font-mono);font-size:9px;margin-top:16px;line-height:2;border:1px solid var(--amber);padding:10px;border-radius:4px">
          ▶ TO BUILD THE DATABASE, run this once in your terminal:<br/>
          <code style="color:var(--cyan)">cd stock-nexus && python model/build_db.py</code><br/>
          Takes ~5 minutes. After that, chart analysis works fully offline.
        </div>` : ""}
      </div>`;
    return;
  }

  const sig  = data.signal || "NEUTRAL";
  const conf = data.confidence || 50;
  const sigColor = dirColor(sig);
  const sigCls = sig==="BULLISH"?"bull":sig==="BEARISH"?"bear":"neut";

  // Convert markdown-like text to HTML
  let html = data.analysis || "";
  html = html.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>');
  html = html.replace(/^### (.+)$/gm, '<h3>$1</h3>');
  html = html.replace(/^## (.+)$/gm, '<h2>$1</h2>');
  html = html.replace(/^# (.+)$/gm, '<h1>$1</h1>');
  html = html.replace(/^- (.+)$/gm, '<li>$1</li>');
  html = html.replace(/(<li>.*<\/li>)/gs, m => `<ul>${m}</ul>`);
  html = html.replace(/\n\n/g, '</p><p>');
  html = `<p>${html}</p>`;

  // Match metadata
  const m = data.matched || {};
  const matchBadge = m.stock_id ? `
    <div style="font-family:var(--font-mono);font-size:8px;color:var(--text3);margin-top:6px;display:flex;gap:12px;flex-wrap:wrap">
      <span>📊 MATCHED: <span style="color:var(--cyan)">${m.stock_name} (${m.stock_id})</span></span>
      <span>📅 ${m.date_end}</span>
      <span>🎯 SIM: ${((m.combined_sim||0)*100).toFixed(1)}%</span>
      <span style="color:var(--text4)">MODEL: local-retrieval</span>
    </div>` : "";

  document.getElementById("chartai-result-panel").innerHTML = `
    <div class="fadeIn">
      <div class="chart-signal-header ${sigCls}">
        <div>
          <div style="font-family:var(--font-title);font-size:28px;color:${sigColor}">${sig}</div>
          <div style="font-family:var(--font-mono);font-size:9px;color:${sigColor}">${conf}% CONFIDENCE</div>
        </div>
        <div class="conf-bar-wrap" style="flex:1;margin-left:16px">
          <div class="conf-bar" style="width:${conf}%;background:${sigColor}"></div>
        </div>
      </div>
      ${matchBadge}
      <div class="chart-analysis-content" style="padding:4px">${html}</div>
    </div>`;
}

// ── JOURNAL ────────────────────────────────────────────────────
function prefillJournal(stockId, mkt, dir, entry, stop, tp1, tp2) {
  document.getElementById("add-trade-form").classList.remove("hidden");
  document.getElementById("jnl-market").value = mkt;
  document.getElementById("jnl-stock").value  = stockId;
  document.getElementById("jnl-dir").value    = dir;
  document.getElementById("jnl-entry").value  = parseFloat(entry).toFixed(2);
  document.getElementById("jnl-stop").value   = parseFloat(stop).toFixed(2);
  document.getElementById("jnl-tp1").value    = parseFloat(tp1).toFixed(2);
  document.getElementById("jnl-tp2").value    = parseFloat(tp2).toFixed(2);
  // scroll to journal
  document.querySelectorAll(".nav-item").forEach(b => b.classList.toggle("active", b.dataset.page==="journal"));
  document.querySelectorAll(".page").forEach(p => p.classList.remove("active"));
  document.getElementById("page-journal").classList.add("active");
  currentPage = "journal";
}

function addJournalTrade() {
  const mkt   = document.getElementById("jnl-market").value;
  const stock = document.getElementById("jnl-stock").value.trim().toUpperCase();
  const dir   = document.getElementById("jnl-dir").value;
  const entry = parseFloat(document.getElementById("jnl-entry").value);
  const qty   = parseFloat(document.getElementById("jnl-qty").value);
  const stop  = parseFloat(document.getElementById("jnl-stop").value);
  const tp1   = parseFloat(document.getElementById("jnl-tp1").value);
  const tp2   = parseFloat(document.getElementById("jnl-tp2").value);
  const notes = document.getElementById("jnl-notes").value;

  if (!stock || isNaN(entry) || isNaN(qty)) { alert("Fill in stock, entry and quantity."); return; }

  trades.push({
    id: Date.now(), mkt, stock, dir, entry, qty, stop, tp1, tp2, notes,
    status: "OPEN", date: new Date().toISOString().slice(0,10), exitPrice: null, exitDate: null
  });
  saveTrades();
  renderJournal();
  document.getElementById("add-trade-form").classList.add("hidden");
}

function closeTrade(id) {
  const trade = trades.find(t => t.id === id);
  if (!trade) return;
  const mkt = getMktData(trade.mkt);
  const currentPrice = mkt[trade.stock]?.price || trade.entry;
  trade.exitPrice = currentPrice;
  trade.exitDate  = new Date().toISOString().slice(0,10);
  trade.status    = "CLOSED";
  saveTrades();
  renderJournal();
}

function deleteTrade(id) {
  trades = trades.filter(t => t.id !== id);
  saveTrades();
  renderJournal();
}

function saveTrades() { localStorage.setItem("sn_trades", JSON.stringify(trades)); }

function calcPnL(t) {
  const exitP = t.exitPrice || getMktData(t.mkt)[t.stock]?.price || t.entry;
  const mult  = t.dir === "LONG" ? 1 : -1;
  return round2((exitP - t.entry) * t.qty * mult);
}
function round2(v) { return Math.round(v * 100) / 100; }

function renderJournal() {
  const open   = trades.filter(t => t.status === "OPEN");
  const closed = trades.filter(t => t.status === "CLOSED");

  // Group P&L by market/currency
  const usClosed  = closed.filter(t => t.mkt === "us");
  const euClosed  = closed.filter(t => t.mkt === "eu");
  const asClosed  = closed.filter(t => t.mkt === "as");
  const usPnL  = usClosed.reduce((a,t) => a + calcPnL(t), 0);
  const euPnL  = euClosed.reduce((a,t) => a + calcPnL(t), 0);
  const asPnL  = asClosed.reduce((a,t) => a + calcPnL(t), 0);
  const wins    = closed.filter(t => calcPnL(t) > 0).length;
  const losses  = closed.filter(t => calcPnL(t) <= 0).length;
  const winRate = closed.length ? round2(wins/closed.length*100) : 0;

  const pnlDisplay = [
    euClosed.length ? `<span style="color:${euPnL>=0?"var(--green)":"var(--red)"}">€${euPnL>=0?"+":""}${fpRaw(euPnL)} EU</span>` : "",
    asClosed.length ? `<span style="color:${asPnL>=0?"var(--green)":"var(--red)"}">¥${asPnL>=0?"+":""}${fpRaw(asPnL)} ASIA</span>` : "",
    usClosed.length ? `<span style="color:${usPnL>=0?"var(--green)":"var(--red)"}">$${usPnL>=0?"+":""}${fpRaw(usPnL)} US</span>` : "",
  ].filter(Boolean).join(" · ") || "—";

  document.getElementById("journal-stats").innerHTML = `
    <div style="margin-bottom:4px;font-family:var(--font-mono);font-size:9px;color:var(--text3);letter-spacing:2px">PORTFOLIO SUMMARY</div>
    <div class="jnl-stats-grid">
      <div class="jnl-stat">
        <div class="jnl-stat-label">REALISED P&L</div>
        <div class="jnl-stat-val" style="font-size:11px">${pnlDisplay}</div>
      </div>
      <div class="jnl-stat">
        <div class="jnl-stat-label">OPEN TRADES</div>
        <div class="jnl-stat-val">${open.length}</div>
      </div>
      <div class="jnl-stat">
        <div class="jnl-stat-label">CLOSED</div>
        <div class="jnl-stat-val">${closed.length}</div>
      </div>
      <div class="jnl-stat">
        <div class="jnl-stat-label">WIN RATE</div>
        <div class="jnl-stat-val" style="color:${winRate>=50?"var(--green)":"var(--red)"}">${winRate}%</div>
      </div>
      <div class="jnl-stat">
        <div class="jnl-stat-label">WINS</div>
        <div class="jnl-stat-val" style="color:var(--green)">${wins}</div>
      </div>
      <div class="jnl-stat">
        <div class="jnl-stat-label">LOSSES</div>
        <div class="jnl-stat-val" style="color:var(--red)">${losses}</div>
      </div>
    </div>`;

  const headerHtml = `
    <div class="jnl-header">
      <div>MKT</div><div>STOCK</div><div>DIR</div><div>ENTRY</div>
      <div>CURRENT</div><div>P&L</div><div>QTY</div>
      <div>SL</div><div>TP1</div><div>STATUS</div><div>ACTION</div>
    </div>`;

  const tradeRow = t => {
    const mkt  = getMktData(t.mkt);
    const cur  = t.exitPrice || mkt[t.stock]?.price || t.entry;
    const pnl  = calcPnL(t);
    const sym  = t.mkt==="eu"?"€":t.mkt==="as"?"¥":"$";
    const flag = t.mkt==="eu"?"🌍":t.mkt==="as"?"🌏":"🇺🇸";
    return `<div class="jnl-row">
      <div style="font-size:9px">${flag}</div>
      <div style="font-weight:700;color:var(--text)">${t.stock}</div>
      <div style="color:${t.dir==="LONG"?"var(--green)":"var(--red)"}">${t.dir}</div>
      <div>${sym}${fpRaw(t.entry)}</div>
      <div style="color:${chColor(cur-t.entry)}">${sym}${fpRaw(cur)}</div>
      <div style="color:${pnl>=0?"var(--green)":"var(--red);"}font-weight:700">${pnl>=0?"+":""}${fpRaw(pnl)}</div>
      <div>${t.qty}</div>
      <div style="color:var(--red)">${sym}${fpRaw(t.stop)}</div>
      <div style="color:var(--green)">${sym}${fpRaw(t.tp1)}</div>
      <div>${t.status==="OPEN"?`<span class="sig-badge sig-bull">OPEN</span>`:`<span class="sig-badge sig-neut">CLOSED</span>`}</div>
      <div style="display:flex;gap:4px">
        ${t.status==="OPEN"?`<button class="btn-secondary" style="padding:3px 7px;font-size:8px" onclick="closeTrade(${t.id})">CLOSE</button>`:""}
        <button class="btn-secondary" style="padding:3px 7px;font-size:8px;color:var(--red)" onclick="deleteTrade(${t.id})">DEL</button>
      </div>
    </div>`;
  };

  document.getElementById("journal-open").innerHTML = open.length
    ? headerHtml + open.map(tradeRow).join("")
    : `<div style="font-family:var(--font-mono);font-size:10px;color:var(--text4);padding:20px;text-align:center">NO OPEN POSITIONS</div>`;

  document.getElementById("journal-closed").innerHTML = closed.length
    ? headerHtml + closed.map(tradeRow).join("")
    : `<div style="font-family:var(--font-mono);font-size:10px;color:var(--text4);padding:20px;text-align:center">NO CLOSED TRADES</div>`;
}

function exportTrades() {
  if (!trades.length) { alert("No trades to export."); return; }
  const headers = ["Market","Stock","Direction","Entry","Exit","Qty","SL","TP1","Status","P&L","Date","Notes"];
  const rows = trades.map(t => {
    const pnl = calcPnL(t);
    return [t.mkt,t.stock,t.dir,t.entry,t.exitPrice||"",t.qty,t.stop,t.tp1,t.status,pnl,t.date,t.notes||""];
  });
  const csv = [headers,...rows].map(r=>r.join(",")).join("\n");
  const blob = new Blob([csv], {type:"text/csv"});
  const url  = URL.createObjectURL(blob);
  const a    = Object.assign(document.createElement("a"), {href:url,download:"stock_nexus_trades.csv"});
  a.click(); URL.revokeObjectURL(url);
}