"""
STOCK NEXUS — Nigerian & US Stock Intelligence Terminal
=======================================================
Mirrors the Forex Nexus architecture but built for:
  - Nigerian Exchange Group (NGX) stocks
  - US stocks (NYSE / NASDAQ)
  - Chart image analysis via local retrieval model (no external API needed)
"""

# ── Async server monkey-patch — MUST be first, before all other imports ───────
# gevent/eventlet must patch here at module load time. Patching inside
# if __name__ == "__main__" is too late (Flask/requests already imported)
# and causes "Cannot switch to different thread" / recursion errors on macOS.
_ASYNC_SERVER = None
try:
    import gevent.monkey as _gm
    _gm.patch_all()
    _ASYNC_SERVER = "gevent"
except ImportError:
    try:
        # Force eventlet to use 'poll' hub — kqueue hub is broken on macOS/Python 3.9
        import os as _os
        _os.environ.setdefault("EVENTLET_HUB", "poll")
        import eventlet as _ev
        _ev.monkey_patch()
        _ASYNC_SERVER = "eventlet"
    except ImportError:
        _ASYNC_SERVER = "werkzeug"

import json, os, queue, random, threading, time, base64, sys
import requests
from flask import Flask, Response, render_template, jsonify, request, stream_with_context
from flask_cors import CORS
import numpy as np
import pandas as pd

# ── Load .env file if present (must be before any os.environ.get calls) ───────
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv optional — env vars still work via OS / Render dashboard

# ── Persistent storage (Supabase) ─────────────────────────────────────────────
try:
    from database import (
        save_ngx_prices   as _db_save,
        load_all_ngx_history as _db_load_history,
        get_data_summary  as _db_summary,
        is_connected      as _db_connected,
    )
    _db_ok = True
except ImportError:
    _db_ok = False
    def _db_save(prices):         return False
    def _db_load_history(days=200): return {}
    def _db_summary():            return {"connected": False, "message": "database.py not found"}
    def _db_connected():          return False

# ── Local retrieval model ─────────────────────────────────────────────────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "model"))
try:
    from retrieval import retrieve_top, db_ready, db_stats
    _retrieval_ok = True
except ImportError:
    _retrieval_ok = False

try:
    from predictor import predict as ml_predict, predictor_status, models_ready
    _predictor_ok = True
except ImportError:
    _predictor_ok = False
    def ml_predict(df, price): return {"ml_signal": "UNAVAILABLE", "ml_source": "unavailable", "ml_confidence": 0, "ml_direction": None, "ml_prob_up": None, "ml_change_pct": None, "ml_target_low": None, "ml_target_mid": None, "ml_target_high": None}
    def predictor_status(): return {"xgboost_ready": False, "lstm_ready": False}
    def models_ready(): return False

app = Flask(__name__)
CORS(app)

# ── Numpy JSON serialiser ─────────────────────────────────────────────────────
def _sanitize(obj):
    if isinstance(obj, dict):   return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)): return [_sanitize(v) for v in obj]
    if isinstance(obj, np.bool_):    return bool(obj)
    if isinstance(obj, np.integer):  return int(obj)
    if isinstance(obj, np.floating): return float(obj)
    if isinstance(obj, np.ndarray):  return obj.tolist()
    return obj

try:
    import yfinance as yf
    _yf_ok = True
except ImportError:
    _yf_ok = False

# ══════════════════════════════════════════════════════════════════════════════
# STOCK UNIVERSE
# ══════════════════════════════════════════════════════════════════════════════

_NGX_SECTOR_COLORS = {
    "Finance": "#4A9EFF", "Telecom": "#FFD700", "Materials": "#FF6B4A",
    "Consumer": "#F472B6", "Energy": "#34D399", "Agriculture": "#86EFAC",
    "Technology": "#38BDF8", "Healthcare": "#E63946", "Construction": "#FB923C",
    "Transport": "#2DD4BF", "Hospitality": "#C084FC", "Real Estate": "#FCD34D",
    "Conglomerate": "#A3E635",
}
# Full 100+ NGX stock list — single source of truth for fallback
# Format: (id, name, sector, seed_price, seed_vol)
_NGX_FULL = [
    ("AIRTELAFRI","Airtel Africa Plc","Telecom",2497.00,320_000),
    ("MTNN","MTN Nigeria Communications PLC","Telecom",760.00,2_100_000),
    ("BUAFOODS","BUA Foods PLC","Consumer",798.00,490_000),
    ("DANGCEM","Dangote Cement Plc","Materials",810.00,1_240_000),
    ("BUACEMENT","BUA Cement Plc","Materials",326.70,550_000),
    ("ARADEL","Aradel Holdings Plc","Energy",1260.00,150_000),
    ("SEPLAT","Seplat Energy Plc","Energy",9099.90,180_000),
    ("GTCO","Guaranty Trust Holding Co Plc","Finance",120.95,8_800_000),
    ("ZENITHBANK","Zenith Bank Plc","Finance",103.00,12_000_000),
    ("WAPCO","Lafarge Africa Plc","Materials",220.00,400_000),
    ("GEREGU","Geregu Power Plc","Energy",1141.50,80_000),
    ("NESTLE","Nestlé Nigeria Plc","Consumer",3055.50,85_000),
    ("INTBREW","International Breweries Plc","Consumer",14.00,10_000_000),
    ("PRESCO","Presco Plc","Agriculture",1980.00,67_000),
    ("TRANSPOWER","Transcorp Power Plc","Energy",306.90,300_000),
    ("NB","Nigerian Breweries Plc","Consumer",72.00,2_500_000),
    ("FIRSTHOLDCO","First HoldCo Plc","Finance",50.00,16_000_000),
    ("STANBIC","Stanbic IBTC Holdings PLC","Finance",133.10,620_000),
    ("TRANSCOHOT","Transcorp Hotels Plc","Hospitality",203.00,200_000),
    ("UBA","United Bank for Africa Plc","Finance",46.15,18_000_000),
    ("OKOMUOIL","The Okomu Oil Palm Co Plc","Agriculture",1765.00,95_000),
    ("ACCESSCORP","Access Holdings Plc","Finance",26.00,25_000_000),
    ("ETI","Ecobank Transnational Inc","Finance",46.00,5_000_000),
    ("WEMABANK","Wema Bank PLC","Finance",26.10,8_000_000),
    ("FIDELITYBK","Fidelity Bank Plc","Finance",19.25,22_000_000),
    ("GUINNESS","Guinness Nigeria Plc","Consumer",423.20,180_000),
    ("DANGSUGAR","Dangote Sugar Refinery Plc","Consumer",65.00,3_000_000),
    ("OANDO","Oando PLC","Energy",49.60,4_000_000),
    ("UNILEVER","Unilever Nigeria Plc","Consumer",94.00,500_000),
    ("FCMB","FCMB Group Plc","Finance",12.10,12_000_000),
    ("TRANSCORP","Transnational Corporation Plc","Conglomerate",48.00,5_000_000),
    ("JBERGER","Julius Berger Nigeria Plc","Construction",288.00,120_000),
    ("JAIZBANK","Jaiz Bank Plc","Finance",9.30,6_000_000),
    ("CUSTODIAN","Custodian Investment Plc","Finance",73.50,400_000),
    ("NASCON","Nascon Allied Industries Plc","Consumer",152.00,250_000),
    ("STERLINGNG","Sterling Financial Holdings Plc","Finance",8.00,10_000_000),
    ("NAHCO","Nigerian Aviation Handling Co Plc","Transport",189.95,300_000),
    ("NGXGROUP","The Nigerian Exchange Group Plc","Finance",165.00,350_000),
    ("UCAP","United Capital Plc","Finance",18.10,2_000_000),
    ("PZ","PZ Cussons Nigeria Plc","Consumer",83.00,600_000),
    ("BETAGLAS","Beta Glass Plc","Materials",498.50,50_000),
    ("UACN","UAC of Nigeria PLC","Conglomerate",99.00,400_000),
    ("FIDSON","Fidson Healthcare Plc","Healthcare",100.00,350_000),
    ("TOTAL","TotalEnergies Marketing Nigeria Plc","Energy",640.00,120_000),
    ("SKYAVN","Skyway Aviation Handling Co Plc","Transport",143.10,200_000),
    ("ETRANZACT","eTranzact International Plc","Technology",20.15,1_500_000),
    ("VITAFOAM","Vitafoam Nigeria Plc","Consumer",118.00,300_000),
    ("HONYFLOUR","Honeywell Flour Mills Plc","Consumer",20.95,2_000_000),
    ("NEM","NEM Insurance Plc","Finance",31.90,800_000),
    ("CADBURY","Cadbury Nigeria Plc","Consumer",67.20,400_000),
    ("AIICO","AIICO Insurance Plc","Finance",4.10,3_000_000),
    ("CONOIL","Conoil Plc","Energy",204.40,380_000),
    ("MANSARD","AXA Mansard Insurance Plc","Finance",15.21,1_500_000),
    ("CHAMPION","Champion Breweries Plc","Consumer",15.05,1_200_000),
    ("CORNERST","Cornerstone Insurance Plc","Finance",5.60,2_000_000),
    ("ABBEYBDS","Abbey Mortgage Bank Plc","Finance",9.90,800_000),
    ("UPDC","UPDC Plc","Real Estate",4.65,1_500_000),
    ("VFDGROUP","VFD Group PLC","Finance",11.45,500_000),
    ("IKEJAHOTEL","Ikeja Hotel Plc","Hospitality",39.00,300_000),
    ("MBENEFIT","Mutual Benefits Assurance Plc","Finance",4.40,2_500_000),
    ("CAP","Chemical and Allied Products Plc","Materials",99.80,200_000),
    ("INFINITY","Infinity Trust Mortgage Bank Plc","Finance",19.00,600_000),
    ("WAPIC","Coronation Insurance Plc","Finance",3.00,2_000_000),
    ("MAYBAKER","May & Baker Nigeria Plc","Healthcare",34.30,500_000),
    ("AFRIPRUD","Africa Prudential Plc","Finance",14.00,1_000_000),
    ("CWG","CWG Plc","Technology",20.70,800_000),
    ("CONHALLPLC","Consolidated Hallmark Holdings Plc","Finance",4.70,1_500_000),
    ("JAPAULGOLD","Japaul Gold & Ventures Plc","Materials",3.42,2_000_000),
    ("ELLAHLAKES","Ellah Lakes Plc","Agriculture",11.95,400_000),
    ("ETERNA","Eterna Plc","Energy",34.90,800_000),
    ("NEIMETH","Neimeth International Pharma Plc","Healthcare",10.00,700_000),
    ("EUNISELL","Eunisell Interlinked Plc","Materials",169.95,80_000),
    ("CHAMS","Chams Holding Company Plc","Technology",3.96,2_000_000),
    ("NPFMCRFBK","NPF Microfinance Bank Plc","Finance",6.19,800_000),
    ("SOVRENINS","Sovereign Trust Insurance Plc","Finance",1.97,2_000_000),
    ("VERITASKAP","Veritas Kapital Assurance Plc","Finance",2.00,1_500_000),
    ("LINKASSURE","Linkage Assurance Plc","Finance",1.50,1_800_000),
    ("SUNUASSUR","Sunu Assurances Nigeria Plc","Finance",4.65,600_000),
    ("REDSTAREX","Red Star Express Plc","Transport",28.15,300_000),
    ("IMG","Industrial and Medical Gases Nigeria","Materials",36.00,200_000),
    ("LIVINGTRUST","Livingtrust Mortgage Bank PLC","Finance",4.80,600_000),
    ("FTNCOCOA","FTN Cocoa Processors Plc","Agriculture",5.33,800_000),
    ("LASACO","LASACO Assurance Plc","Finance",2.05,1_500_000),
    ("CUTIX","Cutix Plc","Materials",3.17,1_200_000),
    ("BERGER","Berger Paints Nigeria Plc","Materials",75.90,150_000),
    ("TANTALIZER","Tantalizers PLC","Consumer",4.25,800_000),
    ("NCR","NCR (Nigeria) Plc","Technology",199.00,60_000),
    ("CAVERTON","Caverton Offshore Support Group Plc","Transport",6.40,1_000_000),
    ("PRESTIGE","Prestige Assurance Plc","Finance",1.53,1_200_000),
    ("LIVESTOCK","Livestock Feeds Plc","Agriculture",6.70,800_000),
    ("UNIVINSURE","Universal Insurance Plc","Finance",1.22,1_500_000),
    ("CILEASING","C & I Leasing Plc","Finance",6.95,600_000),
    ("UPDCREIT","UPDC Real Estate Investment Trust","Real Estate",7.70,400_000),
    ("REGALINS","Regency Alliance Insurance Plc","Finance",1.14,1_500_000),
    ("UNITYBNK","Unity Bank Plc","Finance",1.51,3_000_000),
    ("RTBRISCOE","R.T Briscoe (Nigeria) Plc","Consumer",10.50,600_000),
    ("CHELLARAM","Chellarams Plc","Consumer",13.20,300_000),
    ("GUINEAINS","Guinea Insurance Plc","Finance",1.13,1_000_000),
    ("ABCTRANS","ABC Transport Plc","Transport",6.24,500_000),
    ("MORISON","Morison Industries Plc","Healthcare",11.80,250_000),
    ("NNFM","Northern Nigeria Flour Mills Plc","Consumer",79.40,200_000),
    ("FLOURMILL","Flour Mills of Nigeria Plc","Consumer",54.00,1_200_000),
    ("MECURE","Mecure Industries PLC","Materials",61.50,200_000),
]
# Deduplicate
_seen_ngx = set()
_NGX_FULL = [r for r in _NGX_FULL if not (r[0] in _seen_ngx or _seen_ngx.add(r[0]))]


NIGERIAN_STOCKS = [
    {"id": sid, "name": name, "sector": sector, "currency": "NGN",
     "yf": None,  # NGX prices come from scraper, not Yahoo
     "color": _NGX_SECTOR_COLORS.get(sector, "#CBD5E1")}
    for sid, name, sector, price, vol in _NGX_FULL
]

# ── Load full NGX seed prices from scraper if available ─────────────────────
try:
    from ngx_scraper import build_stock_list as _build_ngx, build_seed_prices as _build_ng_seeds
    _scraper_stocks = _build_ngx()
    # Merge any extra stocks the scraper finds that aren't in our list
    _existing_ids = {s["id"] for s in NIGERIAN_STOCKS}
    for _s in _scraper_stocks:
        if _s["id"] not in _existing_ids:
            NIGERIAN_STOCKS.append(_s)
            _existing_ids.add(_s["id"])
    _NG_SEEDS_FULL = _build_ng_seeds()
except Exception as _e:
    print(f"[NGX] scraper not available, using built-in 100-stock list: {_e}")
    _NG_SEEDS_FULL = {}

US_STOCKS = [
    # ── Mega-cap Technology ───────────────────────────────────────────────────
    {"id":"AAPL",  "name":"Apple Inc",             "sector":"Technology",    "currency":"USD","yf":"AAPL",  "color":"#4A9EFF"},
    {"id":"MSFT",  "name":"Microsoft Corp",         "sector":"Technology",    "currency":"USD","yf":"MSFT",  "color":"#00D4AA"},
    {"id":"NVDA",  "name":"NVIDIA Corp",             "sector":"Technology",    "currency":"USD","yf":"NVDA",  "color":"#76C442"},
    {"id":"GOOGL", "name":"Alphabet Inc",            "sector":"Technology",    "currency":"USD","yf":"GOOGL", "color":"#FF6B4A"},
    {"id":"META",  "name":"Meta Platforms",          "sector":"Technology",    "currency":"USD","yf":"META",  "color":"#4267B2"},
    # ── Technology ────────────────────────────────────────────────────────────
    {"id":"AMD",   "name":"Advanced Micro Devices",  "sector":"Technology",    "currency":"USD","yf":"AMD",   "color":"#ED1C24"},
    {"id":"INTC",  "name":"Intel Corp",              "sector":"Technology",    "currency":"USD","yf":"INTC",  "color":"#0071C5"},
    {"id":"AVGO",  "name":"Broadcom Inc",            "sector":"Technology",    "currency":"USD","yf":"AVGO",  "color":"#CF0A2C"},
    {"id":"QCOM",  "name":"Qualcomm Inc",            "sector":"Technology",    "currency":"USD","yf":"QCOM",  "color":"#3253DC"},
    {"id":"CRM",   "name":"Salesforce Inc",          "sector":"Technology",    "currency":"USD","yf":"CRM",   "color":"#00A1E0"},
    {"id":"ORCL",  "name":"Oracle Corp",             "sector":"Technology",    "currency":"USD","yf":"ORCL",  "color":"#F80000"},
    {"id":"ADBE",  "name":"Adobe Inc",               "sector":"Technology",    "currency":"USD","yf":"ADBE",  "color":"#FF0000"},
    {"id":"IBM",   "name":"IBM Corp",                "sector":"Technology",    "currency":"USD","yf":"IBM",   "color":"#1F70C1"},
    {"id":"UBER",  "name":"Uber Technologies",       "sector":"Technology",    "currency":"USD","yf":"UBER",  "color":"#000000"},
    {"id":"PLTR",  "name":"Palantir Technologies",   "sector":"Technology",    "currency":"USD","yf":"PLTR",  "color":"#8B5CF6"},
    {"id":"SNOW",  "name":"Snowflake Inc",           "sector":"Technology",    "currency":"USD","yf":"SNOW",  "color":"#29B5E8"},
    {"id":"NFLX",  "name":"Netflix Inc",             "sector":"Media",         "currency":"USD","yf":"NFLX",  "color":"#E50914"},
    # ── Consumer (Discretionary & Staples) ───────────────────────────────────
    {"id":"AMZN",  "name":"Amazon.com Inc",          "sector":"Consumer",      "currency":"USD","yf":"AMZN",  "color":"#FFB84A"},
    {"id":"TSLA",  "name":"Tesla Inc",               "sector":"EV/Auto",       "currency":"USD","yf":"TSLA",  "color":"#CC0000"},
    {"id":"WMT",   "name":"Walmart Inc",             "sector":"Consumer",      "currency":"USD","yf":"WMT",   "color":"#FCD34D"},
    {"id":"DIS",   "name":"Walt Disney Co",          "sector":"Media",         "currency":"USD","yf":"DIS",   "color":"#0072CE"},
    {"id":"NKE",   "name":"Nike Inc",                "sector":"Consumer",      "currency":"USD","yf":"NKE",   "color":"#F05A28"},
    {"id":"MCD",   "name":"McDonald's Corp",         "sector":"Consumer",      "currency":"USD","yf":"MCD",   "color":"#FFC72C"},
    {"id":"SBUX",  "name":"Starbucks Corp",          "sector":"Consumer",      "currency":"USD","yf":"SBUX",  "color":"#00704A"},
    {"id":"HD",    "name":"Home Depot Inc",          "sector":"Consumer",      "currency":"USD","yf":"HD",    "color":"#F96302"},
    {"id":"COST",  "name":"Costco Wholesale",        "sector":"Consumer",      "currency":"USD","yf":"COST",  "color":"#005DAA"},
    {"id":"CMG",   "name":"Chipotle Mexican Grill",  "sector":"Consumer",      "currency":"USD","yf":"CMG",   "color":"#A81612"},
    {"id":"GM",    "name":"General Motors Co",       "sector":"EV/Auto",       "currency":"USD","yf":"GM",    "color":"#0170CE"},
    {"id":"F",     "name":"Ford Motor Co",           "sector":"EV/Auto",       "currency":"USD","yf":"F",     "color":"#003478"},
    # ── Finance ───────────────────────────────────────────────────────────────
    {"id":"BRK-B", "name":"Berkshire Hathaway B",    "sector":"Finance",       "currency":"USD","yf":"BRK-B", "color":"#C084FC"},
    {"id":"JPM",   "name":"JPMorgan Chase",          "sector":"Finance",       "currency":"USD","yf":"JPM",   "color":"#38BDF8"},
    {"id":"V",     "name":"Visa Inc",                "sector":"Finance",       "currency":"USD","yf":"V",     "color":"#1A56DB"},
    {"id":"MA",    "name":"Mastercard Inc",          "sector":"Finance",       "currency":"USD","yf":"MA",    "color":"#EB001B"},
    {"id":"BAC",   "name":"Bank of America",         "sector":"Finance",       "currency":"USD","yf":"BAC",   "color":"#E31837"},
    {"id":"GS",    "name":"Goldman Sachs Group",     "sector":"Finance",       "currency":"USD","yf":"GS",    "color":"#7399C6"},
    {"id":"MS",    "name":"Morgan Stanley",          "sector":"Finance",       "currency":"USD","yf":"MS",    "color":"#215091"},
    {"id":"WFC",   "name":"Wells Fargo & Co",        "sector":"Finance",       "currency":"USD","yf":"WFC",   "color":"#D71E28"},
    {"id":"C",     "name":"Citigroup Inc",           "sector":"Finance",       "currency":"USD","yf":"C",     "color":"#003B70"},
    {"id":"AXP",   "name":"American Express Co",     "sector":"Finance",       "currency":"USD","yf":"AXP",   "color":"#007BC1"},
    {"id":"PYPL",  "name":"PayPal Holdings",         "sector":"Finance",       "currency":"USD","yf":"PYPL",  "color":"#003087"},
    {"id":"SCHW",  "name":"Charles Schwab Corp",     "sector":"Finance",       "currency":"USD","yf":"SCHW",  "color":"#00A0DF"},
    {"id":"COIN",  "name":"Coinbase Global",         "sector":"Crypto/Finance","currency":"USD","yf":"COIN",  "color":"#1652F0"},
    # ── Healthcare ────────────────────────────────────────────────────────────
    {"id":"JNJ",   "name":"Johnson & Johnson",       "sector":"Healthcare",    "currency":"USD","yf":"JNJ",   "color":"#E63946"},
    {"id":"UNH",   "name":"UnitedHealth Group",      "sector":"Healthcare",    "currency":"USD","yf":"UNH",   "color":"#005EB8"},
    {"id":"LLY",   "name":"Eli Lilly & Co",          "sector":"Healthcare",    "currency":"USD","yf":"LLY",   "color":"#E11B22"},
    {"id":"ABBV",  "name":"AbbVie Inc",              "sector":"Healthcare",    "currency":"USD","yf":"ABBV",  "color":"#071D49"},
    {"id":"MRK",   "name":"Merck & Co",              "sector":"Healthcare",    "currency":"USD","yf":"MRK",   "color":"#00857C"},
    {"id":"PFE",   "name":"Pfizer Inc",              "sector":"Healthcare",    "currency":"USD","yf":"PFE",   "color":"#0074C8"},
    {"id":"TMO",   "name":"Thermo Fisher Scientific","sector":"Healthcare",    "currency":"USD","yf":"TMO",   "color":"#005C8E"},
    {"id":"ABT",   "name":"Abbott Laboratories",     "sector":"Healthcare",    "currency":"USD","yf":"ABT",   "color":"#007AC2"},
    {"id":"AMGN",  "name":"Amgen Inc",               "sector":"Healthcare",    "currency":"USD","yf":"AMGN",  "color":"#00579B"},
    {"id":"ISRG",  "name":"Intuitive Surgical",      "sector":"Healthcare",    "currency":"USD","yf":"ISRG",  "color":"#00A99D"},
    # ── Energy ────────────────────────────────────────────────────────────────
    {"id":"XOM",   "name":"Exxon Mobil",             "sector":"Energy",        "currency":"USD","yf":"XOM",   "color":"#34D399"},
    {"id":"CVX",   "name":"Chevron Corp",            "sector":"Energy",        "currency":"USD","yf":"CVX",   "color":"#009BDE"},
    {"id":"COP",   "name":"ConocoPhillips",          "sector":"Energy",        "currency":"USD","yf":"COP",   "color":"#E31837"},
    {"id":"SLB",   "name":"SLB (Schlumberger)",      "sector":"Energy",        "currency":"USD","yf":"SLB",   "color":"#00A3E0"},
    {"id":"OXY",   "name":"Occidental Petroleum",    "sector":"Energy",        "currency":"USD","yf":"OXY",   "color":"#B31F2B"},
    # ── Industrials ──────────────────────────────────────────────────────────
    {"id":"CAT",   "name":"Caterpillar Inc",         "sector":"Industrial",    "currency":"USD","yf":"CAT",   "color":"#FFCD11"},
    {"id":"BA",    "name":"Boeing Co",               "sector":"Industrial",    "currency":"USD","yf":"BA",    "color":"#1D428A"},
    {"id":"GE",    "name":"GE Aerospace",            "sector":"Industrial",    "currency":"USD","yf":"GE",    "color":"#003057"},
    {"id":"HON",   "name":"Honeywell International", "sector":"Industrial",    "currency":"USD","yf":"HON",   "color":"#E1001A"},
    {"id":"UPS",   "name":"United Parcel Service",   "sector":"Industrial",    "currency":"USD","yf":"UPS",   "color":"#4B1C12"},
    {"id":"RTX",   "name":"RTX Corp (Raytheon)",     "sector":"Industrial",    "currency":"USD","yf":"RTX",   "color":"#005EB8"},
    {"id":"LMT",   "name":"Lockheed Martin Corp",    "sector":"Industrial",    "currency":"USD","yf":"LMT",   "color":"#003B70"},
    # ── Materials ─────────────────────────────────────────────────────────────
    {"id":"LIN",   "name":"Linde PLC",               "sector":"Materials",     "currency":"USD","yf":"LIN",   "color":"#009BDE"},
    {"id":"NEM",   "name":"Newmont Corp (Gold)",      "sector":"Materials",     "currency":"USD","yf":"NEM",   "color":"#FFD700"},
    {"id":"FCX",   "name":"Freeport-McMoRan (Copper)","sector":"Materials",    "currency":"USD","yf":"FCX",   "color":"#B87333"},
    # ── ETFs ──────────────────────────────────────────────────────────────────
    {"id":"SPY",   "name":"S&P 500 ETF (SPDR)",      "sector":"ETF",           "currency":"USD","yf":"SPY",   "color":"#FB923C"},
    {"id":"QQQ",   "name":"Nasdaq 100 ETF",          "sector":"ETF",           "currency":"USD","yf":"QQQ",   "color":"#F472B6"},
    {"id":"IWM",   "name":"Russell 2000 ETF",        "sector":"ETF",           "currency":"USD","yf":"IWM",   "color":"#A78BFA"},
    {"id":"DIA",   "name":"Dow Jones ETF (SPDR)",    "sector":"ETF",           "currency":"USD","yf":"DIA",   "color":"#60A5FA"},
    {"id":"GLD",   "name":"Gold ETF (SPDR)",         "sector":"ETF",           "currency":"USD","yf":"GLD",   "color":"#FFD700"},
    {"id":"TLT",   "name":"20+ Year Treasury Bond ETF","sector":"ETF",         "currency":"USD","yf":"TLT",   "color":"#6EE7B7"},
    {"id":"XLF",   "name":"Financial Sector ETF",    "sector":"ETF",           "currency":"USD","yf":"XLF",   "color":"#93C5FD"},
    {"id":"XLE",   "name":"Energy Sector ETF",       "sector":"ETF",           "currency":"USD","yf":"XLE",   "color":"#6EE7B7"},
    {"id":"XLK",   "name":"Technology Sector ETF",   "sector":"ETF",           "currency":"USD","yf":"XLK",   "color":"#A5F3FC"},
]

ALL_STOCKS = NIGERIAN_STOCKS + US_STOCKS

# ── Seed prices ───────────────────────────────────────────────────────────────
# NGX: sourced from scraper (current real prices from stockanalysis.com)
NG_SEEDS = _NG_SEEDS_FULL if _NG_SEEDS_FULL else {
    sid: {"price": price, "change": 0.0,
          "high": round(price * 1.015, 2), "low": round(price * 0.985, 2),
          "vol": vol, "mktcap": "N/A"}
    for sid, name, sector, price, vol in _NGX_FULL
}
US_SEEDS = {
    # Technology
    "AAPL":  {"price":198.5, "change":0.84, "high":200.1,"low":197.0,"vol":52000000, "mktcap":"3.02T"},
    "MSFT":  {"price":452.3, "change":0.62, "high":454.1,"low":450.5,"vol":18000000, "mktcap":"3.36T"},
    "NVDA":  {"price":135.2, "change":2.31, "high":137.0,"low":133.5,"vol":42000000, "mktcap":"3.31T"},
    "GOOGL": {"price":175.8, "change":0.55, "high":177.2,"low":174.3,"vol":21000000, "mktcap":"2.17T"},
    "META":  {"price":605.7, "change":1.44, "high":609.0,"low":602.1,"vol":15000000, "mktcap":"1.54T"},
    "AMD":   {"price":108.4, "change":1.21, "high":110.0,"low":107.0,"vol":35000000, "mktcap":"176B"},
    "INTC":  {"price":21.3,  "change":-0.85,"high":21.8, "low":21.0, "vol":50000000, "mktcap":"91B"},
    "AVGO":  {"price":233.5, "change":0.92, "high":235.0,"low":232.0,"vol":8000000,  "mktcap":"1.09T"},
    "QCOM":  {"price":155.2, "change":0.43, "high":156.5,"low":154.0,"vol":9000000,  "mktcap":"172B"},
    "CRM":   {"price":272.8, "change":0.65, "high":274.5,"low":271.0,"vol":5000000,  "mktcap":"263B"},
    "ORCL":  {"price":168.3, "change":0.82, "high":169.8,"low":167.0,"vol":6000000,  "mktcap":"462B"},
    "ADBE":  {"price":368.5, "change":0.55, "high":370.5,"low":367.0,"vol":3000000,  "mktcap":"162B"},
    "IBM":   {"price":238.4, "change":0.31, "high":239.5,"low":237.5,"vol":4000000,  "mktcap":"218B"},
    "UBER":  {"price":72.5,  "change":1.15, "high":73.2, "low":71.8, "vol":18000000, "mktcap":"152B"},
    "PLTR":  {"price":125.8, "change":2.30, "high":127.5,"low":124.0,"vol":55000000, "mktcap":"272B"},
    "SNOW":  {"price":171.2, "change":1.05, "high":172.8,"low":170.0,"vol":4000000,  "mktcap":"57B"},
    "NFLX":  {"price":1148.4,"change":1.27, "high":1155.0,"low":1142.0,"vol":5200000,"mktcap":"488B"},
    # Consumer
    "AMZN":  {"price":200.4, "change":1.12, "high":202.1,"low":198.8,"vol":35000000, "mktcap":"2.14T"},
    "TSLA":  {"price":339.3, "change":-1.82,"high":345.5,"low":337.0,"vol":88000000, "mktcap":"1.09T"},
    "WMT":   {"price":97.3,  "change":0.33, "high":97.9, "low":96.7, "vol":14000000, "mktcap":"262B"},
    "DIS":   {"price":99.3,  "change":-0.45,"high":100.2,"low":98.5, "vol":12000000, "mktcap":"181B"},
    "NKE":   {"price":62.4,  "change":-0.31,"high":63.0, "low":61.8, "vol":9000000,  "mktcap":"93B"},
    "MCD":   {"price":310.5, "change":0.45, "high":312.0,"low":309.0,"vol":3000000,  "mktcap":"222B"},
    "SBUX":  {"price":87.2,  "change":-0.22,"high":88.0, "low":86.5, "vol":7000000,  "mktcap":"97B"},
    "HD":    {"price":395.8, "change":0.67, "high":397.5,"low":394.0,"vol":4000000,  "mktcap":"389B"},
    "COST":  {"price":1018.5,"change":0.88, "high":1023.0,"low":1015.0,"vol":1800000,"mktcap":"452B"},
    "CMG":   {"price":51.4,  "change":0.55, "high":52.0, "low":51.0, "vol":2500000,  "mktcap":"143B"},
    "GM":    {"price":52.3,  "change":0.82, "high":52.8, "low":51.8, "vol":14000000, "mktcap":"45B"},
    "F":     {"price":11.2,  "change":-0.45,"high":11.5, "low":11.0, "vol":40000000, "mktcap":"43B"},
    # Finance
    "BRK-B": {"price":548.6, "change":0.28, "high":550.0,"low":547.0,"vol":3400000,  "mktcap":"1.21T"},
    "JPM":   {"price":271.4, "change":0.61, "high":272.6,"low":270.1,"vol":9200000,  "mktcap":"775B"},
    "V":     {"price":368.9, "change":0.43, "high":370.1,"low":367.5,"vol":7100000,  "mktcap":"756B"},
    "MA":    {"price":556.8, "change":0.55, "high":558.5,"low":555.0,"vol":2500000,  "mktcap":"501B"},
    "BAC":   {"price":46.2,  "change":0.68, "high":46.6, "low":45.8, "vol":41000000, "mktcap":"354B"},
    "GS":    {"price":617.5, "change":0.82, "high":619.5,"low":615.5,"vol":2000000,  "mktcap":"199B"},
    "MS":    {"price":128.4, "change":0.55, "high":129.5,"low":127.5,"vol":8000000,  "mktcap":"211B"},
    "WFC":   {"price":76.3,  "change":0.43, "high":76.8, "low":75.8, "vol":14000000, "mktcap":"256B"},
    "C":     {"price":76.5,  "change":0.61, "high":77.0, "low":76.0, "vol":16000000, "mktcap":"143B"},
    "AXP":   {"price":302.8, "change":0.44, "high":304.0,"low":301.5,"vol":3500000,  "mktcap":"211B"},
    "PYPL":  {"price":72.5,  "change":-0.32,"high":73.0, "low":72.0, "vol":10000000, "mktcap":"72B"},
    "SCHW":  {"price":77.8,  "change":0.52, "high":78.3, "low":77.3, "vol":8000000,  "mktcap":"138B"},
    "COIN":  {"price":235.8, "change":3.41, "high":239.5,"low":228.3,"vol":14000000, "mktcap":"59B"},
    # Healthcare
    "JNJ":   {"price":154.2, "change":-0.19,"high":155.0,"low":153.5,"vol":8800000,  "mktcap":"369B"},
    "UNH":   {"price":288.5, "change":0.72, "high":290.0,"low":287.0,"vol":5000000,  "mktcap":"265B"},
    "LLY":   {"price":745.2, "change":1.12, "high":749.0,"low":742.0,"vol":4000000,  "mktcap":"709B"},
    "ABBV":  {"price":201.3, "change":0.45, "high":202.5,"low":200.0,"vol":5500000,  "mktcap":"355B"},
    "MRK":   {"price":82.5,  "change":-0.25,"high":83.0, "low":82.0, "vol":9000000,  "mktcap":"208B"},
    "PFE":   {"price":26.8,  "change":-0.45,"high":27.2, "low":26.5, "vol":30000000, "mktcap":"151B"},
    "TMO":   {"price":455.8, "change":0.62, "high":457.5,"low":454.0,"vol":2500000,  "mktcap":"173B"},
    "ABT":   {"price":128.5, "change":0.38, "high":129.5,"low":127.5,"vol":5000000,  "mktcap":"222B"},
    "AMGN":  {"price":282.5, "change":0.42, "high":284.0,"low":281.0,"vol":3500000,  "mktcap":"149B"},
    "ISRG":  {"price":552.8, "change":0.85, "high":555.0,"low":550.5,"vol":1200000,  "mktcap":"195B"},
    # Energy
    "XOM":   {"price":110.7, "change":0.77, "high":111.5,"low":109.8,"vol":16000000, "mktcap":"446B"},
    "CVX":   {"price":155.3, "change":0.55, "high":156.0,"low":154.5,"vol":10000000, "mktcap":"288B"},
    "COP":   {"price":94.5,  "change":0.42, "high":95.2, "low":93.8, "vol":8000000,  "mktcap":"120B"},
    "SLB":   {"price":40.2,  "change":0.35, "high":40.8, "low":39.8, "vol":12000000, "mktcap":"57B"},
    "OXY":   {"price":44.8,  "change":0.62, "high":45.2, "low":44.4, "vol":9000000,  "mktcap":"41B"},
    # Industrial
    "CAT":   {"price":358.5, "change":0.72, "high":360.5,"low":357.0,"vol":3000000,  "mktcap":"172B"},
    "BA":    {"price":175.8, "change":-0.85,"high":177.0,"low":174.5,"vol":8000000,  "mktcap":"133B"},
    "GE":    {"price":208.5, "change":0.62, "high":209.8,"low":207.5,"vol":5500000,  "mktcap":"226B"},
    "HON":   {"price":224.8, "change":0.35, "high":225.8,"low":223.8,"vol":3000000,  "mktcap":"143B"},
    "UPS":   {"price":101.5, "change":-0.22,"high":102.0,"low":101.0,"vol":4000000,  "mktcap":"87B"},
    "RTX":   {"price":128.5, "change":0.45, "high":129.0,"low":128.0,"vol":6000000,  "mktcap":"172B"},
    "LMT":   {"price":468.5, "change":0.35, "high":470.0,"low":467.0,"vol":1500000,  "mktcap":"108B"},
    # Materials
    "LIN":   {"price":488.5, "change":0.42, "high":490.0,"low":487.0,"vol":1500000,  "mktcap":"234B"},
    "NEM":   {"price":55.2,  "change":0.88, "high":55.8, "low":54.7, "vol":12000000, "mktcap":"43B"},
    "FCX":   {"price":41.5,  "change":1.12, "high":42.0, "low":41.0, "vol":18000000, "mktcap":"59B"},
    # ETFs
    "SPY":   {"price":592.1, "change":0.51, "high":593.8,"low":590.4,"vol":65000000, "mktcap":"590B"},
    "QQQ":   {"price":522.2, "change":0.72, "high":524.0,"low":520.5,"vol":38000000, "mktcap":"312B"},
    "IWM":   {"price":205.8, "change":0.45, "high":206.5,"low":205.0,"vol":25000000, "mktcap":"62B"},
    "DIA":   {"price":432.5, "change":0.38, "high":433.5,"low":431.5,"vol":5000000,  "mktcap":"34B"},
    "GLD":   {"price":306.5, "change":0.55, "high":307.4,"low":305.6,"vol":9800000,  "mktcap":"108B"},
    "TLT":   {"price":85.2,  "change":-0.22,"high":85.8, "low":84.8, "vol":25000000, "mktcap":"58B"},
    "XLF":   {"price":51.5,  "change":0.35, "high":51.8, "low":51.2, "vol":30000000, "mktcap":"46B"},
    "XLE":   {"price":88.5,  "change":0.42, "high":89.0, "low":88.0, "vol":15000000, "mktcap":"36B"},
    "XLK":   {"price":242.5, "change":0.68, "high":243.5,"low":241.5,"vol":8000000,  "mktcap":"68B"},
}

# Rolling price history for NGX stocks — stores up to 200 daily closes
# Populated from doclib scraper on each poll cycle (every 30s)
# Used to build synthetic OHLCV for indicator computation
import collections
_ngx_price_history = collections.defaultdict(list)  # {ticker: [{"date":str,"close":float,"vol":int}, ...]}
_NGX_HISTORY_MAX = 200  # keep 200 days

def _record_ngx_prices(prices: dict):
    """
    Called after each scraper poll — appends today's price to in-memory history
    AND saves to Supabase for persistent accumulation across server restarts.
    """
    import datetime
    today = datetime.date.today().isoformat()
    for ticker, data in prices.items():
        p = data.get("price", 0)
        v = data.get("volume", 0)
        if p <= 0:
            continue
        hist = _ngx_price_history[ticker]
        if hist and hist[-1]["date"] == today:
            hist[-1]["close"] = p
            hist[-1]["vol"]   = v
        else:
            hist.append({"date": today, "close": p, "vol": v})
        if len(hist) > _NGX_HISTORY_MAX:
            _ngx_price_history[ticker] = hist[-_NGX_HISTORY_MAX:]
    # Persist to Supabase in a daemon thread (non-blocking, fire-and-forget)
    if _db_ok:
        threading.Thread(target=_db_save, args=(prices,), daemon=True,
                         name="db-save").start()

def _bootstrap_ngx_history(prices: dict):
    """
    Bootstrap 120 days of synthetic history from today's price + PrevClosingPrice.
    Uses realistic NGX daily volatility (~1.2%) to simulate past closes via
    reverse random walk seeded from today's price and prev close.
    This gives enough data for RSI(14), MACD(26), BB(20) to compute immediately.
    """
    import datetime, numpy as np
    today = datetime.date.today()
    rng = np.random.default_rng(42)  # fixed seed = deterministic history

    for ticker, data in prices.items():
        if _ngx_price_history[ticker]:
            continue  # already have history
        price = data.get("price", 0)
        prev  = data.get("prev_close", price)  # from PrevClosingPrice field
        vol   = data.get("volume", 1_000_000) or 1_000_000
        if price <= 0:
            continue

        # Daily vol estimate from today's move; floor at 0.8%, cap at 3%
        day_ret = abs((price - prev) / (prev + 1e-9)) if prev > 0 else 0.012
        daily_vol = max(0.008, min(0.03, day_ret or 0.012))

        # Walk backwards 120 days from today's price
        closes = [price]
        for _ in range(119):
            ret = rng.normal(0, daily_vol)
            closes.append(closes[-1] / (1 + ret))
        closes.reverse()  # oldest first

        # Store as dated history
        hist = []
        for i, c in enumerate(closes):
            d = (today - datetime.timedelta(days=119 - i)).isoformat()
            hist.append({"date": d, "close": round(c, 2), "vol": int(vol)})
        _ngx_price_history[ticker] = hist
    print(f"[NGX HISTORY] Bootstrapped {len(prices)} stocks with 120-day synthetic history")

def _build_ngx_df(ticker: str):
    """Build a pandas OHLCV DataFrame from stored NGX price history."""
    hist = _ngx_price_history.get(ticker, [])
    if len(hist) < 5:
        return None
    import pandas as pd, numpy as np
    closes = [h["close"] for h in hist]
    dates  = pd.to_datetime([h["date"] for h in hist])
    vols   = [h.get("vol", 1_000_000) for h in hist]
    # Synthesise OHLC — daily range based on rolling std of returns
    rets = np.diff(closes) / (np.array(closes[:-1]) + 1e-9)
    daily_std = float(np.std(rets)) if len(rets) > 1 else 0.012
    highs, lows, opens = [], [], []
    for i, c in enumerate(closes):
        rng_amt = c * daily_std * 1.5
        highs.append(round(c + rng_amt, 2))
        lows.append(round(max(c - rng_amt, c * 0.5), 2))
        opens.append(round(closes[i-1] if i > 0 else c * 0.999, 2))
    df = pd.DataFrame({
        "Open": opens, "High": highs, "Low": lows,
        "Close": closes, "Volume": vols,
    }, index=dates)
    return df

def _fetch_live_ng_seeds():
    """
    Fetch current NGX prices at startup. Priority order:
      1. Supabase persistent storage (real accumulated history)
      2. Live scraper prices (to update today's close + bootstrap synthetic history)
    """
    # ── Step 1: Load real history from Supabase ───────────────────────────────
    if _db_ok:
        try:
            supabase_hist = _db_load_history(days=200)
            if supabase_hist:
                real_count = 0
                for ticker, hist in supabase_hist.items():
                    if hist:
                        _ngx_price_history[ticker] = hist
                        real_count += 1
                print(f"[STARTUP] ✅ Loaded REAL price history for {real_count} NGX tickers from Supabase")
            else:
                print("[STARTUP] Supabase connected but no NGX history stored yet — will start accumulating")
        except Exception as e:
            print(f"[STARTUP] Supabase history load failed ({e})")

    # ── Step 2: Fetch live prices from scraper ────────────────────────────────
    try:
        from ngx_scraper import fetch_ngx_prices
        print(f"[STARTUP] Fetching live NGX prices from scraper...")
        prices = fetch_ngx_prices(force=True)
        if not prices:
            print("[STARTUP] NGX scraper returned no data, using hardcoded seeds.")
            return
        # Sanity check — reject if prices look like USD not NGN
        dangcem = prices.get("DANGCEM", {}).get("price", 0)
        gtco    = prices.get("GTCO",    {}).get("price", 0)
        if (dangcem > 0 and dangcem < 10) or (gtco > 0 and gtco < 1):
            print(f"[STARTUP] NGX prices look like USD (DANGCEM={dangcem}), skipping live seed.")
            return
        updated = 0
        for stock_id, data in prices.items():
            if stock_id in NG_SEEDS:
                NG_SEEDS[stock_id]["price"]  = data["price"]
                NG_SEEDS[stock_id]["change"] = data.get("change", 0.0)
                NG_SEEDS[stock_id]["high"]   = round(data["price"] * 1.005, 2)
                NG_SEEDS[stock_id]["low"]    = round(data["price"] * 0.995, 2)
                if data.get("mktcap") and data["mktcap"] != "N/A":
                    NG_SEEDS[stock_id]["mktcap"] = data["mktcap"]
                updated += 1
        # Only bootstrap synthetic history for tickers that have no real history yet
        tickers_needing_bootstrap = {
            sid: data for sid, data in prices.items()
            if not _ngx_price_history.get(sid)
        }
        if tickers_needing_bootstrap:
            _bootstrap_ngx_history(tickers_needing_bootstrap)
        _record_ngx_prices(prices)  # record today's actual price + save to Supabase
        print(f"[STARTUP] Live NGX prices loaded for {updated}/{len(NG_SEEDS)} stocks.")
    except Exception as e:
        print(f"[STARTUP] NGX startup fetch failed ({e}), using hardcoded seeds.")

def _fetch_live_us_seeds():
    """Fetch current prices from yfinance at startup to replace stale hardcoded seeds."""
    try:
        import yfinance as _yf
        ticker_map = {s["yf"]: s["id"] for s in US_STOCKS if s.get("yf")}
        print(f"[STARTUP] Fetching live US prices for {len(ticker_map)} tickers...")
        raw = _yf.download(
            tickers=list(ticker_map.keys()),
            period="5d", interval="1d",
            auto_adjust=True, progress=False,
            threads=False, group_by="ticker",
        )
        updated = 0
        for yf_tick, stock_id in ticker_map.items():
            try:
                if len(ticker_map) == 1:
                    col = raw["Close"]
                else:
                    if yf_tick not in raw.columns.get_level_values(0):
                        continue
                    col = raw[yf_tick]["Close"]
                col = col.dropna()
                if len(col) >= 2:
                    prev  = float(col.iloc[-2])
                    price = float(col.iloc[-1])
                    if price > 0 and stock_id in US_SEEDS:
                        ch = round((price - prev) / (prev + 1e-9) * 100, 4)
                        high = price * 1.005
                        low  = price * 0.995
                        US_SEEDS[stock_id]["price"]  = round(price, 2)
                        US_SEEDS[stock_id]["change"] = ch
                        US_SEEDS[stock_id]["high"]   = round(high, 2)
                        US_SEEDS[stock_id]["low"]    = round(low, 2)
                        updated += 1
            except Exception:
                pass
        print(f"[STARTUP] Live prices loaded for {updated}/{len(ticker_map)} US stocks.")
    except Exception as e:
        print(f"[STARTUP] yfinance startup fetch failed ({e}), using hardcoded seeds.")

_fetch_live_ng_seeds()
_fetch_live_us_seeds()

market_ng = {s["id"]: {**NG_SEEDS.get(s["id"], {"price":100,"change":0,"high":101,"low":99,"vol":1000000,"mktcap":"N/A"})} for s in NIGERIAN_STOCKS}
market_us = {s["id"]: {**US_SEEDS.get(s["id"], {"price":100,"change":0,"high":101,"low":99,"vol":1000000,"mktcap":"N/A"})} for s in US_STOCKS}

# ── Broadcast SSE ─────────────────────────────────────────────────────────────
subscribers = []
subscribers_lock = threading.Lock()
ws_status = "connecting"

def broadcast(data):
    msg = "data: " + json.dumps(data) + "\n\n"
    with subscribers_lock:
        dead = []
        for q in subscribers:
            try: q.put_nowait(msg)
            except: dead.append(q)
        for q in dead: subscribers.remove(q)

# ── US price fetcher — yfinance isolated in a real OS thread ─────────────────
# yfinance uses curl_cffi (native libcurl) which cannot be patched by eventlet.
# We run it in a stdlib threading.Thread (not a greenthread) via a daemon
# worker that loops independently, writing results into a shared dict.
# The eventlet greenthread only reads from that dict — no blocking.

import threading as _threading
import queue as _queue

_price_result_queue = _queue.Queue(maxsize=1)

def _yf_worker_loop():
    """Runs in a real OS thread — curl_cffi/libcurl safe. Fetches prices
    for all US tickers using yfinance and pushes results to the queue."""
    ticker_map = {s["yf"]: s["id"] for s in US_STOCKS if s["yf"]}  # NGX via scraper only
    import yfinance as _yf
    import time as _t
    while True:
        results = {}
        try:
            raw = _yf.download(
                tickers=list(ticker_map.keys()),
                period="1d", interval="5m",
                auto_adjust=True, progress=False,
                threads=False, group_by="ticker",
            )
            for yf_tick, stock_id in ticker_map.items():
                try:
                    if len(ticker_map) == 1:
                        col = raw["Close"]
                    else:
                        if yf_tick not in raw.columns.get_level_values(0):
                            continue
                        col = raw[yf_tick]["Close"]
                    col = col.dropna()
                    if len(col) > 0:
                        p = float(col.iloc[-1])
                        if p > 0:
                            results[stock_id] = round(p, 2)
                except Exception:
                    pass
        except Exception as e:
            print(f"[YF WORKER] {e}")
        # Drain old result then push fresh one (non-blocking)
        try: _price_result_queue.get_nowait()
        except _queue.Empty: pass
        try: _price_result_queue.put_nowait(results)
        except _queue.Full: pass
        _t.sleep(30)   # fetch every 30 s from the OS thread

# Start the real-thread worker once at import time
_yf_thread = _threading.Thread(target=_yf_worker_loop, daemon=True, name="yf-worker")
_yf_thread.start()

def fetch_stock_prices():
    """Read the latest yfinance results from the OS-thread worker (non-blocking)."""
    try:
        return _price_result_queue.get_nowait()
    except _queue.Empty:
        return {}

def simulate_tick_stock(stock_id, cur, currency):
    p = cur.get("price", 100)
    if currency == "NGN":
        tick_size = p * random.uniform(0.0005, 0.003)
    else:
        tick_size = p * random.uniform(0.0002, 0.0015)
    d = 1 if random.random() > 0.47 else -1
    dec = 2
    np_ = round(p + d * tick_size, dec)
    seed_price = (market_us if stock_id in market_us else market_ng).get(stock_id, {}).get("price", p)
    ch = round((np_ - seed_price) / (seed_price + 1e-9) * 100, 4)
    return np_, ch

def price_poll_thread():
    """Poll US stocks via Yahoo Finance REST API every 15s; fallback to simulation."""
    _fail_count = 0
    _backoff_until = 0
    while True:
        try:
            if time.time() > _backoff_until:
                prices = fetch_stock_prices()
                if prices:
                    _fail_count = 0
                    for stock_id, price in prices.items():
                        seed_price = market_us.get(stock_id, {}).get("price") or price
                        ch = round((price - seed_price) / (seed_price + 1e-9) * 100, 4)
                        if stock_id in market_us:
                            market_us[stock_id]["price"] = price
                            market_us[stock_id]["change"] = ch
                            broadcast({"type": "tick", "market": "us", "id": stock_id, "price": price, "change": ch})
                    broadcast({"type": "status", "status": "live"})
                    time.sleep(15)
                    continue
                else:
                    _fail_count += 1
        except Exception as e:
            _fail_count += 1
            print(f"[US PRICE POLL] {e}")

        # After 3 consecutive failures, back off for 5 minutes
        if _fail_count >= 3:
            print(f"[US PRICE POLL] {_fail_count} failures — backing off 5 min, using simulation")
            _backoff_until = time.time() + 300
            _fail_count = 0

        # Simulate US stocks while live feed is unavailable
        for s in US_STOCKS:
            p, ch = simulate_tick_stock(s["id"], market_us[s["id"]], "USD")
            market_us[s["id"]]["price"] = p
            market_us[s["id"]]["change"] = ch
            broadcast({"type": "tick", "market": "us", "id": s["id"], "price": p, "change": ch})
        broadcast({"type": "status", "status": "simulated"})
        time.sleep(15)

def ngx_price_poll_thread():
    """Poll NGX stocks every 30s via scraper; only simulate after 3 consecutive failures."""
    try:
        from ngx_scraper import fetch_ngx_prices
        _scraper_ok = True
    except ImportError:
        _scraper_ok = False

    _fail_streak = 0
    _MAX_FAILS_BEFORE_SIM = 3  # tolerate 3 misses (e.g. slow startup) before simulating

    while True:
        updated = 0
        if _scraper_ok:
            try:
                prices = fetch_ngx_prices()
                if prices:
                    _fail_streak = 0
                    for stock_id, data in prices.items():
                        if stock_id in market_ng:
                            market_ng[stock_id]["price"]  = data["price"]
                            market_ng[stock_id]["change"] = data["change"]
                            if data.get("mktcap") and data["mktcap"] != "N/A":
                                market_ng[stock_id]["mktcap"] = data["mktcap"]
                            broadcast({"type": "tick", "market": "ng", "id": stock_id,
                                       "price": data["price"], "change": data["change"]})
                            updated += 1
                    _record_ngx_prices(prices)  # store closes for indicator history
                    if updated:
                        broadcast({"type": "status", "status": "live"})
                        print(f"[NGX] Updated {updated} stocks (live)")
                else:
                    _fail_streak += 1
            except Exception as e:
                print(f"[NGX PRICE POLL] {e}")
                _fail_streak += 1

        # Only simulate after repeated failures — prevents "simulated" flash on startup
        if updated == 0 and (not _scraper_ok or _fail_streak >= _MAX_FAILS_BEFORE_SIM):
            for s in NIGERIAN_STOCKS:
                if s["id"] not in market_ng:
                    continue
                p, ch = simulate_tick_stock(s["id"], market_ng[s["id"]], "NGN")
                market_ng[s["id"]]["price"]  = p
                market_ng[s["id"]]["change"] = ch
                broadcast({"type": "tick", "market": "ng", "id": s["id"], "price": p, "change": ch})
            broadcast({"type": "status", "status": "simulated"})

        time.sleep(30)  # NGX prices update every few minutes, 30s is sufficient

threading.Thread(target=price_poll_thread,     daemon=True).start()
threading.Thread(target=ngx_price_poll_thread, daemon=True).start()

# ── Technical indicator helpers ───────────────────────────────────────────────
def _rsi(s, p=14):
    d = s.diff()
    g = d.clip(lower=0).rolling(p).mean()
    l = (-d.clip(upper=0)).rolling(p).mean()
    return 100 - 100 / (1 + g / (l + 1e-9))

def _ema(s, p):
    return s.ewm(span=p, adjust=False).mean()

def _atr(h, l, c, p=14):
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(p).mean()

def _macd(c, f=12, sl=26, sig=9):
    ml = _ema(c, f) - _ema(c, sl)
    sig_ = _ema(ml, sig)
    return ml, sig_, ml - sig_

def _bollinger(c, p=20, k=2):
    m = c.rolling(p).mean(); s = c.rolling(p).std()
    up, lo = m + k*s, m - k*s
    return up, m, lo, (c - lo)/(up - lo + 1e-9), (up - lo)/(m + 1e-9)

def _stoch(h, l, c, k=14, d=3):
    K = 100*(c - l.rolling(k).min())/(h.rolling(k).max() - l.rolling(k).min() + 1e-9)
    return K, K.rolling(d).mean()

def _adx(h, l, c, p=14):
    up = h.diff(); dn = -l.diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr_s = _atr(h, l, c, p)
    pdi = 100 * pd.Series(pdm, index=h.index).rolling(p).mean() / (tr_s + 1e-9)
    mdi = 100 * pd.Series(mdm, index=h.index).rolling(p).mean() / (tr_s + 1e-9)
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi + 1e-9)
    return dx.rolling(p).mean(), pdi, mdi

_ohlcv_cache = {}
_ohlcv_cache_ts = {}

def fetch_ohlcv_stock(ticker, period="120d", interval="1d"):
    if not _yf_ok or not ticker:
        return None
    cache_key = f"{ticker}_{period}_{interval}"
    now = time.time()
    if cache_key in _ohlcv_cache and now - _ohlcv_cache_ts.get(cache_key, 0) < 3600:
        return _ohlcv_cache[cache_key]
    try:
        df = yf.download(ticker, period=period, interval=interval,
                         auto_adjust=True, progress=False, threads=False)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [str(c).strip().title() for c in df.columns]
        if df.empty or "Close" not in df.columns:
            return None
        df = df[["Open","High","Low","Close","Volume"]].dropna()
        if len(df) < 20:
            return None
        _ohlcv_cache[cache_key] = df
        _ohlcv_cache_ts[cache_key] = now
        return df
    except Exception as e:
        print(f"[OHLCV] {ticker}: {e}")
        return None

def compute_indicators(df):
    c, h, l, v, o = df["Close"], df["High"], df["Low"], df["Volume"], df["Open"]

    rsi14  = _rsi(c, 14)
    rsi7   = _rsi(c, 7)
    ml, sl_, hist = _macd(c)
    bb_up, bb_mid, bb_lo, pct_b, bw = _bollinger(c)
    atr14  = _atr(h, l, c, 14)
    ema9   = _ema(c, 9)
    ema21  = _ema(c, 21)
    ema50  = _ema(c, 50)
    ema200 = _ema(c, 200)
    k_, d_ = _stoch(h, l, c)
    adx_, pdi, mdi = _adx(h, l, c)
    v_ma20 = v.rolling(20).mean()

    def last(s):
        v2 = s.dropna()
        return float(v2.iloc[-1]) if len(v2) > 0 else 0.0

    rsi_v    = last(rsi14)
    rsi7_v   = last(rsi7)
    macd_l   = last(ml)
    macd_s   = last(sl_)
    macd_h   = last(hist)
    bb_pct   = last(pct_b) * 100
    atr_v    = last(atr14)
    ema9_v   = last(ema9)
    ema21_v  = last(ema21)
    ema50_v  = last(ema50)
    ema200_v = last(ema200)
    stoch_k  = last(k_)
    stoch_d  = last(d_)
    adx_v    = last(adx_)
    pdi_v    = last(pdi)
    mdi_v    = last(mdi)
    price    = last(c)
    bb_u     = last(bb_up)
    bb_lo_v  = last(bb_lo)
    vol_r    = last(v) / (last(v_ma20) + 1e-9)

    macd_dir = "BULLISH" if macd_l > macd_s else "BEARISH"
    atr_pct  = atr_v / price * 100 if price > 0 else 0

    # Candle pattern
    body = abs(float(c.iloc[-1]) - float(o.iloc[-1]))
    rng  = max(float(h.iloc[-1]) - float(l.iloc[-1]), 1e-9)
    uw   = (float(h.iloc[-1]) - max(float(c.iloc[-1]), float(o.iloc[-1]))) / rng
    lw   = (min(float(c.iloc[-1]), float(o.iloc[-1])) - float(l.iloc[-1])) / rng
    br   = body / rng
    if br < 0.1: candle = "DOJI"
    elif lw > 0.6 and uw < 0.15: candle = "HAMMER"
    elif uw > 0.6 and lw < 0.15: candle = "SHOOTING STAR"
    else: candle = "BULLISH BAR" if float(c.iloc[-1]) > float(o.iloc[-1]) else "BEARISH BAR"

    return {
        "rsi": round(rsi_v, 1), "rsi_7": round(rsi7_v, 1),
        "macd": macd_dir, "macd_line": round(macd_l, 4), "macd_hist": round(macd_h, 4),
        "bb_pct": round(bb_pct, 1), "bb_upper": round(bb_u, 2), "bb_lower": round(bb_lo_v, 2),
        "atr": round(atr_pct, 4), "atr_raw": round(atr_v, 2),
        "ema9": round(ema9_v, 2), "ema21": round(ema21_v, 2),
        "ema50": round(ema50_v, 2), "ema200": round(ema200_v, 2),
        "stoch_k": round(stoch_k, 1), "stoch_d": round(stoch_d, 1),
        "adx": round(adx_v, 1), "plus_di": round(pdi_v, 1), "minus_di": round(mdi_v, 1),
        "vol_ratio": round(vol_r, 2), "candle": candle, "price": round(price, 2),
    }

def rule_based_signal(ind, ch):
    b = br = 0
    rsi = ind["rsi"]; macd = ind["macd"]; bb = ind["bb_pct"]
    stoch = ind["stoch_k"]; adx = ind["adx"]
    if rsi < 30: b += 3
    elif rsi > 70: br += 3
    elif rsi < 50: b += 1
    else: br += 1
    if macd == "BULLISH": b += 3
    else: br += 3
    if bb < 20: b += 2
    elif bb > 80: br += 2
    if stoch < 20: b += 2
    elif stoch > 80: br += 2
    if ch > 1: b += 2
    elif ch < -1: br += 2
    T = b + br
    bp = round(b / T * 100) if T else 50
    direction = "BULLISH" if bp >= 60 else "BEARISH" if bp <= 40 else "NEUTRAL"
    conf = round(min(88, max(20, abs(bp - 50) * 2.2)))
    if adx < 18 and direction != "NEUTRAL" and conf < 55:
        direction = "NEUTRAL"; conf = round(conf * 0.6)
    return direction, conf, bp

def swing_levels(df):
    """Extract recent swing highs and lows for TP/SL."""
    h = df["High"].values
    l = df["Low"].values
    sw = 5
    highs, lows = [], []
    for i in range(sw, len(h) - sw):
        if h[i] == max(h[i-sw:i+sw+1]): highs.append(float(h[i]))
        if l[i] == min(l[i-sw:i+sw+1]): lows.append(float(l[i]))
    return sorted(set(highs)), sorted(set(lows), reverse=True)

# ══════════════════════════════════════════════════════════════════════════════
# NEWS
# ══════════════════════════════════════════════════════════════════════════════

_news_cache = {}
_news_cache_lock = threading.Lock()
NEWS_TTL = 600

STOCK_KEYWORDS = {
    "beat":+2.5, "record":+2.0, "profit":+1.8, "revenue growth":+2.0, "upgrade":+2.0,
    "buy rating":+2.0, "strong earnings":+2.5, "dividend":+1.5, "buyback":+1.8,
    "expansion":+1.5, "acquisition":+1.2, "partnership":+1.2, "growth":+1.5,
    "miss":-2.5, "loss":-2.0, "downgrade":-2.0, "sell rating":-2.0, "weak earnings":-2.5,
    "layoff":-1.8, "recall":-1.5, "investigation":-2.0, "fraud":-2.5, "debt":-1.5,
    "lawsuit":-1.8, "decline":-1.5, "cut":-1.5, "warning":-2.0,
}

def fetch_stock_news(stock_id, stock_name):
    key = stock_id
    now = time.time()
    with _news_cache_lock:
        cached = _news_cache.get(key)
        if cached and (now - cached["ts"]) < NEWS_TTL:
            return cached["items"]
    items = []
    # Use GNews RSS feed filtered by stock name/id — free, no key required
    queries = [stock_id, stock_name.split(" ")[0]]  # e.g. "DANGCEM" and "Dangote"
    for query in queries:
        if len(items) >= 6:
            break
        try:
            encoded = requests.utils.quote(query)
            r = requests.get(
                f"https://news.google.com/rss/search?q={encoded}+stock&hl=en&gl=US&ceid=US:en",
                headers={"User-Agent": "StockNexus/1.0"}, timeout=8
            )
            if r.status_code == 200:
                import xml.etree.ElementTree as ET
                root = ET.fromstring(r.text)
                for item in root.findall(".//item")[:4]:
                    title = item.findtext("title", "")
                    link  = item.findtext("link", "")
                    src_el = item.find("source")
                    source = src_el.text if src_el is not None else "Google News"
                    pub    = item.findtext("pubDate", "")
                    # Deduplicate by title
                    if title and not any(x["title"] == title for x in items):
                        items.append({"title": title, "source": source,
                                      "url": link, "published": pub})
        except Exception as e:
            print(f"[NEWS] {stock_id} query={query}: {e}")
    with _news_cache_lock:
        _news_cache[key] = {"items": items[:6], "ts": now}
    return items[:6]

def score_news(items):
    if not items: return 0.0, 0.5
    total = 0.0
    for item in items:
        text = item.get("title","").lower()
        score = sum(v for kw,v in STOCK_KEYWORDS.items() if kw in text)
        item["score"] = round(score, 2)
        total += score
    avg = total / len(items)
    bull = min(1.0, max(0.0, 0.5 + avg / 10))
    return round(avg, 2), round(bull, 3)

# ══════════════════════════════════════════════════════════════════════════════
# CHART IMAGE ANALYSIS  (Local Retrieval Model — no external API needed)
# ══════════════════════════════════════════════════════════════════════════════
# Run  python model/build_db.py  once to populate the database, then
# chart analysis works fully offline using similarity search.
# ══════════════════════════════════════════════════════════════════════════════

# ══════════════════════════════════════════════════════════════════════════════
# API ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index(): return render_template("index.html")

@app.route("/favicon.ico")
def favicon(): return "", 204

@app.route("/api/seed")
def seed():
    return jsonify({
        "ng_stocks": NIGERIAN_STOCKS,
        "us_stocks": US_STOCKS,
        "market_ng": market_ng,
        "market_us": market_us,
    })

@app.route("/api/stream")
def stream():
    q = queue.Queue(maxsize=500)
    with subscribers_lock: subscribers.append(q)
    def generate():
        yield "data: " + json.dumps({"type":"snapshot","market_ng":market_ng,"market_us":market_us}) + "\n\n"
        try:
            while True:
                try:
                    msg = q.get(timeout=20)
                    if msg is None:  # shutdown sentinel
                        return
                    yield msg
                except:
                    yield ": keepalive\n\n"
        except GeneratorExit: pass
        finally:
            with subscribers_lock:
                try: subscribers.remove(q)
                except: pass
    return Response(stream_with_context(generate()), mimetype="text/event-stream",
                    headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"})

# ── Consensus engine ─────────────────────────────────────────────────────────
def _build_consensus(rule_dir, rule_conf, bp, ml_pred):
    """
    Returns a consensus signal only when rule-based and ML agree on direction.
    Inputs:
        rule_dir  — "BULLISH" | "BEARISH" | "NEUTRAL"
        rule_conf — int 0-100
        bp        — bullish % from rule_based_signal
        ml_pred   — dict from ml_predict()
    Output:
        signal    — "STRONG BUY"|"BUY"|"STRONG SELL"|"SELL"|"NO_CONSENSUS"
        agreement — "FULL"|"RULE_ONLY"|"NONE"
        tradeable — bool, True only when all agree with sufficient confidence
    """
    ml_src  = ml_pred.get("ml_source", "unavailable")
    ml_dir  = ml_pred.get("ml_direction")
    ml_prob = ml_pred.get("ml_prob_up")
    ml_conf = ml_pred.get("ml_confidence", 0)
    ml_sig  = ml_pred.get("ml_signal", "UNAVAILABLE")
    ml_avail = ml_src not in ("unavailable", None) and ml_dir is not None

    rule_up   = rule_dir == "BULLISH" and rule_conf >= 45
    rule_down = rule_dir == "BEARISH" and rule_conf >= 45

    # No ML available — rule only
    ml_reason = ("ML models not trained — run model/train_model.py"
                 if not models_ready()
                 else "NGX stock — ML ran on synthetic history (no YF data)")

    if not ml_avail:
        if rule_up:
            sig = "STRONG BUY" if rule_conf >= 70 else "BUY"
            return {"signal": sig, "agreement": "RULE_ONLY",
                    "systems": {"rule": True, "xgb": None, "lstm": None},
                    "confidence": rule_conf, "tradeable": rule_conf >= 60,
                    "reason": f"Rule-based only — {ml_reason}"}
        if rule_down:
            sig = "STRONG SELL" if rule_conf >= 70 else "SELL"
            return {"signal": sig, "agreement": "RULE_ONLY",
                    "systems": {"rule": True, "xgb": None, "lstm": None},
                    "confidence": rule_conf, "tradeable": rule_conf >= 60,
                    "reason": f"Rule-based only — {ml_reason}"}
        return {"signal": "NO_CONSENSUS", "agreement": "NONE",
                "systems": {"rule": False, "xgb": None, "lstm": None},
                "confidence": 0, "tradeable": False,
                "reason": f"No clear rule-based signal — {ml_reason}"}

    ml_up   = ml_dir == "UP"   and ml_prob is not None and ml_prob >= 0.54
    ml_down = ml_dir == "DOWN" and ml_prob is not None and ml_prob <= 0.46

    # Full consensus — all three agree
    if rule_up and ml_up:
        combined = int(rule_conf * 0.5 + ml_conf * 0.5)
        sig = "STRONG BUY" if combined >= 70 and ml_prob >= 0.64 else "BUY"
        return {"signal": sig, "agreement": "FULL",
                "systems": {"rule": True, "xgb": True, "lstm": True},
                "confidence": combined,
                "ml_prob_up": round(ml_prob, 3),
                "tradeable": combined >= 55,
                "reason": (f"All systems bullish — Rule {rule_conf}% conf, "
                           f"ML {ml_sig} ({ml_prob:.0%} prob up)")}

    if rule_down and ml_down:
        combined = int(rule_conf * 0.5 + ml_conf * 0.5)
        sig = "STRONG SELL" if combined >= 70 and ml_prob <= 0.36 else "SELL"
        return {"signal": sig, "agreement": "FULL",
                "systems": {"rule": True, "xgb": True, "lstm": True},
                "confidence": combined,
                "ml_prob_up": round(ml_prob, 3),
                "tradeable": combined >= 55,
                "reason": (f"All systems bearish — Rule {rule_conf}% conf, "
                           f"ML {ml_sig} ({ml_prob:.0%} prob up)")}

    # Disagreement
    rule_vote = "BULL" if rule_up else ("BEAR" if rule_down else "NEUTRAL")
    ml_vote   = "BULL" if ml_dir == "UP" else ("BEAR" if ml_dir == "DOWN" else "NEUTRAL")
    return {"signal": "NO_CONSENSUS", "agreement": "NONE",
            "systems": {"rule": rule_vote, "xgb": ml_vote, "lstm": ml_vote},
            "confidence": 0, "tradeable": False,
            "reason": f"Systems disagree — Rule={rule_vote}, ML={ml_vote} ({ml_prob:.0%} prob up)" if ml_prob else "Systems disagree"}


@app.route("/api/analyze", methods=["POST"])
def analyze():
    try:
        data = request.get_json()
        stock_id = data.get("stockId")
        market_type = data.get("market", "us")  # "ng" or "us"
        stock_list = NIGERIAN_STOCKS if market_type == "ng" else US_STOCKS
        stock_info = next((s for s in stock_list if s["id"] == stock_id), None)
        if not stock_info:
            return jsonify({"error": "Unknown stock"}), 400

        mkt = market_ng if market_type == "ng" else market_us
        state = mkt.get(stock_id, {})
        price  = float(state.get("price", 100))
        change = float(state.get("change", 0))
        high   = float(state.get("high", price * 1.02))
        low    = float(state.get("low", price * 0.98))
        trade_size = float(data.get("tradeSize", 0))

        # Fetch real OHLCV
        # NGX stocks have yf=None — try Yahoo Finance .LG suffix as fallback
        _yf = stock_info.get("yf") or (stock_id + ".LG")
        df = fetch_ohlcv_stock(_yf)
        if df is None and market_type == "ng":
            # Some NGX stocks use different YF symbols — try without suffix
            df = fetch_ohlcv_stock(stock_id)
        if df is None and market_type == "ng":
            # Build synthetic OHLCV from our rolling NGX price history
            df = _build_ngx_df(stock_id)
            if df is not None:
                data_src = "ngx_history"
        indicators = None
        if "data_src" not in dir():
            data_src = "approximation"
        if df is not None and len(df) >= 20:
            try:
                indicators = compute_indicators(df)
                data_src = "real_candles"
                price = indicators["price"] or price
            except Exception as e:
                print(f"[INDICATORS] {stock_id}: {e}")

        if indicators is None:
            # Fallback approximation
            rng = high - low or price * 0.02
            indicators = {
                "rsi": min(98, max(2, 50 + change * 4.2)),
                "rsi_7": min(98, max(2, 50 + change * 6)),
                "macd": "BULLISH" if change > 0 else "BEARISH",
                "macd_line": change * 0.01, "macd_hist": change * 0.001,
                "bb_pct": ((price - low) / rng) * 100,
                "bb_upper": high, "bb_lower": low,
                "atr": rng / price * 100, "atr_raw": rng,
                "ema9": price, "ema21": price, "ema50": price, "ema200": price,
                "stoch_k": ((price - low) / rng) * 100,
                "stoch_d": ((price - low) / rng) * 90,
                "adx": min(80, max(10, abs(change)*8+20)),
                "plus_di": 25.0, "minus_di": 25.0,
                "vol_ratio": 1.0, "candle": "BULLISH BAR" if change > 0 else "BEARISH BAR",
                "price": price,
            }

        direction, conf, bp = rule_based_signal(indicators, change)

        # TP/SL from swing levels or ATR
        atr_raw = indicators["atr_raw"]
        if df is not None and len(df) >= 20:
            swing_h, swing_l = swing_levels(df)
        else:
            swing_h, swing_l = [], []

        def nearest_above(min_atr=1.0):
            thr = price + atr_raw * min_atr
            cands = [x for x in swing_h if x >= thr]
            return cands[0] if cands else None
        def nearest_below(min_atr=0.8):
            thr = price - atr_raw * min_atr
            cands = [x for x in swing_l if x <= thr]
            return cands[0] if cands else None

        m = atr_raw / price
        if direction == "BULLISH":
            t1   = round(nearest_above(1.0) or price*(1+m*1.5), 2)
            t2   = round(nearest_above(2.0) or price*(1+m*3.0), 2)
            # Ensure T2 is always further above price than T1
            if t2 <= t1:
                t2 = round(t1 + atr_raw * 1.5, 2)
            stop = round((nearest_below(0.8) or price*(1-m*1.0)) * 0.9985, 2)
            # Ensure SL is actually below price
            if stop >= price:
                stop = round(price * (1 - m * 1.0), 2)
        elif direction == "BEARISH":
            t1   = round(nearest_below(1.0) or price*(1-m*1.5), 2)
            t2   = round(nearest_below(2.0) or price*(1-m*3.0), 2)
            # Ensure T2 is always further below price than T1
            if t2 >= t1:
                t2 = round(t1 - atr_raw * 1.5, 2)
            stop = round((nearest_above(0.8) or price*(1+m*1.0)) * 1.0015, 2)
            # Ensure SL is actually above price
            if stop <= price:
                stop = round(price * (1 + m * 1.0), 2)
        else:
            t1   = round(price*(1+m*0.8), 2)
            t2   = round(price*(1+m*1.5), 2)
            stop = round(price*(1-m*1.2), 2)

        rr = round(abs(t1 - price) / (abs(price - stop) + 1e-6), 2)

        # News
        news_items = []
        try: news_items = fetch_stock_news(stock_id, stock_info["name"])
        except: pass
        news_score, news_bull = score_news(news_items)

        # Trend label
        adx = indicators["adx"]
        trend_str = "STRONG" if adx > 50 else "MODERATE" if adx > 25 else "WEAK"
        mkt_phase = "TRENDING" if adx > 35 else "RANGING"
        currency = stock_info["currency"]
        curr_sym = "₦" if currency == "NGN" else "$"

        # Position sizing
        pos_sizing = None
        if trade_size > 0:
            sl_dist = abs(price - stop)
            if sl_dist > 0:
                units = trade_size / sl_dist
                pos_sizing = {
                    "tradeSize": trade_size, "units": round(units, 2),
                    "slRisk": round(trade_size, 2),
                    "t1Profit": round(abs(t1-price)*units, 2),
                    "t2Profit": round(abs(t2-price)*units, 2),
                    "slDist": round(sl_dist, 2),
                    "rr1": round(abs(t1-price)/sl_dist, 2),
                    "rr2": round(abs(t2-price)/sl_dist, 2),
                }

        ai_text = (
            f"STOCK NEXUS [{stock_id}] ({direction}, {conf}% confidence) — "
            f"{stock_info['name']} at {curr_sym}{price:,.2f} ({change:+.2f}%). "
            f"[{data_src.replace('_',' ').upper()}] "
            f"RSI(14) {indicators['rsi']:.1f} · RSI(7) {indicators['rsi_7']:.1f} — "
            f"{'overbought' if indicators['rsi']>70 else 'oversold' if indicators['rsi']<30 else 'neutral'}. "
            f"MACD {indicators['macd']} (hist {indicators['macd_hist']:+.4f}). "
            f"Stoch K:{indicators['stoch_k']:.0f}/D:{indicators['stoch_d']:.0f}. "
            f"ADX {adx:.0f} (+DI {indicators['plus_di']:.0f}/-DI {indicators['minus_di']:.0f}) — {trend_str} trend. "
            f"BB {indicators['bb_pct']:.0f}%. Candle: {indicators['candle']}. Vol:{indicators['vol_ratio']:.2f}x. "
            f"EMA9:{curr_sym}{indicators['ema9']:,.2f} EMA50:{curr_sym}{indicators['ema50']:,.2f}. "
            f"Levels: TP1 {curr_sym}{t1:,.2f}, TP2 {curr_sym}{t2:,.2f}, SL {curr_sym}{stop:,.2f}. R/R: {rr}:1."
        )

        # ── ML Ensemble prediction ────────────────────────────────────────
        ml_pred = ml_predict(df, price) if df is not None else ml_predict(None, price)

        # ── Consensus engine — only fire when ALL systems agree ───────────
        consensus = _build_consensus(direction, conf, bp, ml_pred)

        return jsonify(_sanitize({
            "stockId": stock_id,
            "stockInfo": stock_info,
            "price": price, "change": change,
            "indicators": indicators,
            "prediction": {
                "dir": direction, "bullPct": bp, "bearPct": 100-bp,
                "conf": conf, "targets": {"t1":t1,"t2":t2,"stop":stop}, "rr": rr,
            },
            "mlPrediction": ml_pred,
            "consensus": consensus,
            "posSizing": pos_sizing,
            "news": news_items,
            "newsScore": news_score,
            "newsBull": news_bull,
            "aiText": ai_text,
            "trendStr": trend_str,
            "marketPhase": mkt_phase,
            "dataSource": data_src,
            "market": market_type,
        }))

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/predict", methods=["POST"])
def predict():
    """
    Standalone ML prediction endpoint.
    POST { "stockId": "DANGCEM" }
    Returns all 4 ML outputs: direction, change%, signal, 5-day targets.
    """
    data     = request.get_json() or {}
    stock_id = data.get("stockId", "").upper().strip()

    stock_info = next((s for s in ALL_STOCKS if s["id"] == stock_id), None)
    if not stock_info:
        return jsonify({"error": f"Unknown stock: {stock_id}"}), 404

    if not models_ready():
        return jsonify({
            "error": "ML models not trained yet. Run: python3 model/train_model.py",
            "ml_source": "unavailable",
        }), 503

    # Fetch OHLCV
    yf_ticker = stock_info.get("yf") or (stock_id + ".NG" if stock_info.get("currency") == "NGN" else stock_id)
    df = fetch_ohlcv_stock(yf_ticker)

    state = next((s for s in _stock_state if s["id"] == stock_id), {})
    current_price = float(state.get("price", 0)) or (df["Close"].iloc[-1] if df is not None else 0)

    result = ml_predict(df, current_price)
    result["stockId"]    = stock_id
    result["stockName"]  = stock_info["name"]
    result["price"]      = current_price
    result["currency"]   = stock_info["currency"]
    return jsonify(_sanitize(result))


@app.route("/api/predictor_status")
def predictor_status_route():
    """Return ML model readiness and accuracy stats."""
    return jsonify(predictor_status())


@app.route("/api/model_status")
def model_status():
    """Return the status of the local retrieval database."""
    if not _retrieval_ok:
        return jsonify({"ready": False, "reason": "retrieval module not loaded"})
    ready = db_ready()
    stats = db_stats() if ready else {"total": 0}
    return jsonify({"ready": ready, "stats": stats})


@app.route("/api/analyze_chart", methods=["POST"])
def analyze_chart():
    """Analyze an uploaded chart image using the local retrieval model."""
    try:
        if not _retrieval_ok:
            return jsonify({
                "success": False,
                "error": "Retrieval module not loaded. Check model/retrieval.py."
            }), 500

        if not db_ready():
            return jsonify({
                "success": False,
                "error": (
                    "Chart database not built yet.\n\n"
                    "Run this command once to generate it:\n"
                    "  python model/build_db.py\n\n"
                    "This pulls 120 days of history for all 40 stocks and takes ~5 minutes."
                )
            })

        if "image" not in request.files:
            return jsonify({"error": "No image file provided"}), 400

        file        = request.files["image"]
        image_bytes = file.read()

        # Optionally pass the selected stock's current indicators for better matching
        ind_json = request.form.get("indicators", "")
        query_indicators = None
        if ind_json:
            try:
                query_indicators = json.loads(ind_json)
            except Exception:
                pass

        match = retrieve_top(image_bytes, query_indicators)
        if not match:
            return jsonify({
                "success": False,
                "error": "No similar chart found in database. Try rebuilding: python model/build_db.py"
            })

        if "error" in match:
            return jsonify({"success": False, "error": match["error"]})

        # Parse direction/confidence from the analysis text (already stored)
        analysis  = match["analysis"]
        direction = match["direction"]
        conf      = match["confidence"]

        return jsonify({
            "success":    True,
            "analysis":   analysis,
            "signal":     direction,
            "confidence": conf,
            "model":      "local-retrieval",
            "matched": {
                "stock_id":   match["stock_id"],
                "stock_name": match["stock_name"],
                "date_end":   match["date_end"],
                "img_sim":    match["img_sim"],
                "combined_sim": match["combined_sim"],
            },
        })

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e), "success": False}), 500


@app.route("/api/db_stats")
def db_stats():
    """Return Supabase database statistics and real NGX data accumulation progress."""
    summary = _db_summary()
    # Add guidance on what comes next
    days = summary.get("days_stored", 0)
    if not summary.get("connected"):
        summary["next_milestone"] = "Configure Supabase env vars to enable persistence"
    elif days < 30:
        summary["next_milestone"] = f"30-day milestone: {30 - days} days to go — first retrain triggers then"
    elif days < 60:
        summary["next_milestone"] = f"60-day milestone: {60 - days} days to go — good prediction accuracy"
    elif days < 90:
        summary["next_milestone"] = f"90-day milestone: {90 - days} days to go — excellent accuracy"
    else:
        summary["next_milestone"] = "Full maturity reached — AI is continuously improving ✅"
    return jsonify(summary)


@app.route("/api/retrain", methods=["POST"])
def retrain_now():
    """
    Manually trigger a model retrain. Runs in background — returns immediately.
    Requires ≥30 days of real NGX data in Supabase.
    POST body: {} or {"reason": "manual test"}
    """
    try:
        from retrain_scheduler import run_retrain_background, get_status
        status = get_status()
        if status.get("running"):
            return jsonify({
                "success": False,
                "reason": "Retraining already in progress",
                "status": status,
            })
        data   = request.get_json() or {}
        reason = data.get("reason", "manual_api")
        run_retrain_background(trigger=reason)
        return jsonify({
            "success": True,
            "message": "Retraining started in background — check /api/training_status for results",
            "status": get_status(),
        })
    except ImportError:
        return jsonify({"success": False, "reason": "retrain_scheduler not available"}), 503
    except Exception as e:
        return jsonify({"success": False, "reason": str(e)}), 500


@app.route("/api/training_status")
def training_status():
    """Return retraining scheduler state, last results, and AI improvement history."""
    sched = {}
    try:
        from retrain_scheduler import get_status
        sched = get_status()
    except ImportError:
        sched = {"error": "retrain_scheduler not available"}
    return jsonify(_sanitize({
        "scheduler":  sched,
        "database":   _db_summary(),
        "predictor":  predictor_status(),
    }))


@app.route("/api/scan", methods=["POST"])
def scan():
    """Quick scan returning rule-based signals for all stocks."""
    data = request.get_json() or {}
    market_type = data.get("market", "both")
    results = []

    def scan_list(stock_list, mkt_data, mkt_type):
        for s in stock_list:
            state = mkt_data.get(s["id"], {})
            price  = float(state.get("price", 100))
            change = float(state.get("change", 0))
            high   = float(state.get("high", price*1.02))
            low    = float(state.get("low", price*0.98))
            rng = high - low or price * 0.02
            ind = {
                "rsi": min(98, max(2, 50+change*4.2)),
                "macd": "BULLISH" if change > 0 else "BEARISH",
                "bb_pct": ((price-low)/rng)*100,
                "stoch_k": ((price-low)/rng)*100,
                "adx": min(80, max(10, abs(change)*8+20)),
            }
            direction, conf, bp = rule_based_signal(ind, change)
            # Consensus filter — only include if rule-based signal is clear
            # (ML not run here for speed; full ML consensus available via /api/analyze)
            if direction != "NEUTRAL" and conf >= 55:
                consensus = _build_consensus(direction, conf, bp,
                                             {"ml_source": "unavailable", "ml_direction": None})
                results.append({
                    "id": s["id"], "name": s["name"], "sector": s["sector"],
                    "market": mkt_type, "currency": s["currency"], "color": s["color"],
                    "price": price, "change": change,
                    "direction": direction, "conf": conf, "bullPct": bp,
                    "rsi": round(ind["rsi"], 1), "adx": round(ind["adx"], 1),
                    "consensusSignal": consensus["signal"],
                })

    if market_type in ("ng","both"): scan_list(NIGERIAN_STOCKS, market_ng, "ng")
    if market_type in ("us","both"): scan_list(US_STOCKS,       market_us, "us")
    results.sort(key=lambda x: x["conf"], reverse=True)
    return jsonify({"results": results, "count": len(results)})


def _shutdown(signum=None, frame=None):
    print("\n[NEXUS] Shutting down cleanly...")
    with subscribers_lock:
        for q in list(subscribers):
            try:
                q.put(None)
            except Exception:
                pass
        subscribers.clear()
    print("[NEXUS] All SSE connections closed. Goodbye.")
    import os
    os._exit(0)

import signal
signal.signal(signal.SIGINT,  _shutdown)
signal.signal(signal.SIGTERM, _shutdown)

# ── Retraining scheduler — start once at module load time ────────────────────
_scheduler = None
try:
    from retrain_scheduler import start_scheduler
    _scheduler = start_scheduler()
except ImportError:
    print("[NEXUS] retrain_scheduler not found — auto-retraining disabled")
except Exception as _se:
    print(f"[NEXUS] Scheduler startup error: {_se}")

if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5002))
    print(f"STOCK NEXUS starting on http://0.0.0.0:{PORT}  [{_ASYNC_SERVER} server]")
    print(f"[NEXUS] Database: {'Supabase connected ✅' if _db_connected() else 'In-memory only ⚠️  (set SUPABASE_URL + SUPABASE_KEY to persist data)'}")
    print("[NEXUS] Press Ctrl+C to stop.")
    try:
        if _ASYNC_SERVER == "gevent":
            from gevent.pywsgi import WSGIServer
            from gevent import signal as gsignal
            print("[SERVER] gevent WSGIServer — SSE-safe, no kqueue issues")
            server = WSGIServer(("0.0.0.0", PORT), app)
            gsignal.signal(signal.SIGINT,  _shutdown)
            gsignal.signal(signal.SIGTERM, _shutdown)
            server.serve_forever()
        elif _ASYNC_SERVER == "eventlet":
            import eventlet
            import eventlet.wsgi
            print("[SERVER] eventlet WSGIServer — SSE-safe, no kqueue issues")
            sock = eventlet.listen(("0.0.0.0", PORT))
            eventlet.wsgi.server(sock, app, log_output=False)
        else:
            print("[SERVER] Werkzeug dev server. Install gevent for better SSE support.")
            app.run(debug=False, port=PORT, threaded=True, use_reloader=False)
    except (KeyboardInterrupt, SystemExit):
        _shutdown()