"""
STOCK NEXUS — Global & US Stock Intelligence Terminal
======================================================
Mirrors the Forex Nexus architecture but built for:
  - European stocks (LSE, Euronext, XETRA, SIX, etc.)
  - Asian stocks (TSE, HKEX, NSE, KRX, plus US-listed ADRs)
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

# ── Persistent storage (optional — graceful fallback) ─────────────────────────
_db_ok = False
def _db_connected(): return False

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

# ── Currency symbol map ───────────────────────────────────────────────────────
CURR_SYMBOLS = {
    "USD": "$", "EUR": "€", "GBP": "p", "JPY": "¥",
    "HKD": "HK$", "INR": "₹", "KRW": "₩", "CHF": "Fr",
    "DKK": "kr", "SEK": "kr",
}

EU_STOCKS = [
    # UK
    {"id":"SHEL.L",    "name":"Shell Plc",             "sector":"Energy",     "currency":"GBP","yf":"SHEL.L",    "color":"#34D399"},
    {"id":"BP.L",      "name":"BP Plc",                "sector":"Energy",     "currency":"GBP","yf":"BP.L",      "color":"#22C55E"},
    {"id":"AZN.L",     "name":"AstraZeneca Plc",       "sector":"Healthcare", "currency":"GBP","yf":"AZN.L",     "color":"#E63946"},
    {"id":"HSBA.L",    "name":"HSBC Holdings Plc",     "sector":"Finance",    "currency":"GBP","yf":"HSBA.L",    "color":"#4A9EFF"},
    {"id":"ULVR.L",    "name":"Unilever Plc",          "sector":"Consumer",   "currency":"GBP","yf":"ULVR.L",    "color":"#F472B6"},
    {"id":"GSK.L",     "name":"GSK Plc",               "sector":"Healthcare", "currency":"GBP","yf":"GSK.L",     "color":"#FB923C"},
    {"id":"RIO.L",     "name":"Rio Tinto Plc",         "sector":"Materials",  "currency":"GBP","yf":"RIO.L",     "color":"#FF6B4A"},
    # France
    {"id":"MC.PA",     "name":"LVMH",                  "sector":"Luxury",     "currency":"EUR","yf":"MC.PA",     "color":"#FFD700"},
    {"id":"OR.PA",     "name":"L'Oreal SA",            "sector":"Consumer",   "currency":"EUR","yf":"OR.PA",     "color":"#F472B6"},
    {"id":"TTE.PA",    "name":"TotalEnergies SE",      "sector":"Energy",     "currency":"EUR","yf":"TTE.PA",    "color":"#34D399"},
    {"id":"AIR.PA",    "name":"Airbus SE",             "sector":"Industrial", "currency":"EUR","yf":"AIR.PA",    "color":"#FB923C"},
    {"id":"BNP.PA",    "name":"BNP Paribas SA",        "sector":"Finance",    "currency":"EUR","yf":"BNP.PA",    "color":"#4A9EFF"},
    {"id":"RMS.PA",    "name":"Hermes International",  "sector":"Luxury",     "currency":"EUR","yf":"RMS.PA",    "color":"#C084FC"},
    {"id":"SAN.PA",    "name":"Sanofi SA",             "sector":"Healthcare", "currency":"EUR","yf":"SAN.PA",    "color":"#E63946"},
    # Germany
    {"id":"SAP.DE",    "name":"SAP SE",                "sector":"Technology", "currency":"EUR","yf":"SAP.DE",    "color":"#38BDF8"},
    {"id":"SIE.DE",    "name":"Siemens AG",            "sector":"Industrial", "currency":"EUR","yf":"SIE.DE",    "color":"#60A5FA"},
    {"id":"BMW.DE",    "name":"BMW AG",                "sector":"Auto",       "currency":"EUR","yf":"BMW.DE",    "color":"#A78BFA"},
    {"id":"MBG.DE",    "name":"Mercedes-Benz Group",   "sector":"Auto",       "currency":"EUR","yf":"MBG.DE",    "color":"#7C3AED"},
    {"id":"VOW3.DE",   "name":"Volkswagen AG",         "sector":"Auto",       "currency":"EUR","yf":"VOW3.DE",   "color":"#0071C5"},
    {"id":"ALV.DE",    "name":"Allianz SE",            "sector":"Finance",    "currency":"EUR","yf":"ALV.DE",    "color":"#38BDF8"},
    {"id":"ADS.DE",    "name":"Adidas AG",             "sector":"Consumer",   "currency":"EUR","yf":"ADS.DE",    "color":"#1A1A1A"},
    {"id":"DTE.DE",    "name":"Deutsche Telekom AG",   "sector":"Telecom",    "currency":"EUR","yf":"DTE.DE",    "color":"#E2007A"},
    # Switzerland
    {"id":"NESN.SW",   "name":"Nestle SA",             "sector":"Consumer",   "currency":"CHF","yf":"NESN.SW",   "color":"#F472B6"},
    {"id":"ROG.SW",    "name":"Roche Holding AG",      "sector":"Healthcare", "currency":"CHF","yf":"ROG.SW",    "color":"#E63946"},
    {"id":"NOVN.SW",   "name":"Novartis AG",           "sector":"Healthcare", "currency":"CHF","yf":"NOVN.SW",   "color":"#FB923C"},
    # Netherlands
    {"id":"ASML.AS",   "name":"ASML Holding NV",       "sector":"Technology", "currency":"EUR","yf":"ASML.AS",   "color":"#00A3E0"},
    # Denmark
    {"id":"NOVO-B.CO", "name":"Novo Nordisk A/S",      "sector":"Healthcare", "currency":"DKK","yf":"NOVO-B.CO", "color":"#E11B22"},
    # Italy
    {"id":"RACE.MI",   "name":"Ferrari NV",            "sector":"Auto",       "currency":"EUR","yf":"RACE.MI",   "color":"#CC0000"},
    {"id":"ENI.MI",    "name":"Eni SpA",               "sector":"Energy",     "currency":"EUR","yf":"ENI.MI",    "color":"#FFD700"},
]

ASIA_STOCKS = [
    # Japan
    {"id":"7203.T",    "name":"Toyota Motor Corp",     "sector":"Auto",       "currency":"JPY","yf":"7203.T",    "color":"#CC0000"},
    {"id":"6758.T",    "name":"Sony Group Corp",       "sector":"Technology", "currency":"JPY","yf":"6758.T",    "color":"#000000"},
    {"id":"9984.T",    "name":"SoftBank Group Corp",   "sector":"Technology", "currency":"JPY","yf":"9984.T",    "color":"#FF6B00"},
    {"id":"7974.T",    "name":"Nintendo Co Ltd",       "sector":"Gaming",     "currency":"JPY","yf":"7974.T",    "color":"#E4000F"},
    {"id":"6501.T",    "name":"Hitachi Ltd",           "sector":"Industrial", "currency":"JPY","yf":"6501.T",    "color":"#CF0A2C"},
    {"id":"8306.T",    "name":"Mitsubishi UFJ Fin.",   "sector":"Finance",    "currency":"JPY","yf":"8306.T",    "color":"#4A9EFF"},
    # Hong Kong / China
    {"id":"0700.HK",   "name":"Tencent Holdings",      "sector":"Technology", "currency":"HKD","yf":"0700.HK",   "color":"#38BDF8"},
    {"id":"9988.HK",   "name":"Alibaba Group (HK)",    "sector":"eCommerce",  "currency":"HKD","yf":"9988.HK",   "color":"#FF6A00"},
    {"id":"3690.HK",   "name":"Meituan",               "sector":"eCommerce",  "currency":"HKD","yf":"3690.HK",   "color":"#FFD700"},
    {"id":"9618.HK",   "name":"JD.com Inc (HK)",       "sector":"eCommerce",  "currency":"HKD","yf":"9618.HK",   "color":"#CC0000"},
    {"id":"2318.HK",   "name":"Ping An Insurance",     "sector":"Finance",    "currency":"HKD","yf":"2318.HK",   "color":"#E63946"},
    # India
    {"id":"RELIANCE.NS","name":"Reliance Industries",  "sector":"Conglomerate","currency":"INR","yf":"RELIANCE.NS","color":"#1D4ED8"},
    {"id":"HDFCBANK.NS","name":"HDFC Bank Ltd",        "sector":"Finance",    "currency":"INR","yf":"HDFCBANK.NS","color":"#004C8F"},
    {"id":"TCS.NS",    "name":"Tata Consultancy Svcs", "sector":"Technology", "currency":"INR","yf":"TCS.NS",    "color":"#4A9EFF"},
    {"id":"INFY.NS",   "name":"Infosys Ltd",           "sector":"Technology", "currency":"INR","yf":"INFY.NS",   "color":"#007CC3"},
    {"id":"ICICIBANK.NS","name":"ICICI Bank Ltd",      "sector":"Finance",    "currency":"INR","yf":"ICICIBANK.NS","color":"#F97316"},
    # South Korea
    {"id":"005930.KS", "name":"Samsung Electronics",   "sector":"Technology", "currency":"KRW","yf":"005930.KS", "color":"#1428A0"},
    # Taiwan / SE Asia (US-listed ADRs — most reliable via yfinance)
    {"id":"TSM",       "name":"TSMC (Taiwan Semi)",    "sector":"Technology", "currency":"USD","yf":"TSM",       "color":"#0070AD"},
    {"id":"SE",        "name":"Sea Limited",           "sector":"Technology", "currency":"USD","yf":"SE",        "color":"#EE3024"},
    {"id":"GRAB",      "name":"Grab Holdings",         "sector":"Transport",  "currency":"USD","yf":"GRAB",      "color":"#00B14F"},
]

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

ALL_STOCKS = EU_STOCKS + ASIA_STOCKS + US_STOCKS

# ── Seed prices (approximate current prices — overwritten by live yfinance fetch at startup) ─
EU_SEEDS = {
    "SHEL.L":{"price":2721,"change":0.42,"high":2745,"low":2698,"vol":15000000,"mktcap":"189B"},
    "BP.L":{"price":452,"change":-0.31,"high":458,"low":448,"vol":35000000,"mktcap":"76B"},
    "AZN.L":{"price":10520,"change":0.65,"high":10610,"low":10440,"vol":3500000,"mktcap":"198B"},
    "HSBA.L":{"price":748,"change":0.28,"high":755,"low":743,"vol":25000000,"mktcap":"143B"},
    "ULVR.L":{"price":2398,"change":0.15,"high":2415,"low":2382,"vol":4500000,"mktcap":"58B"},
    "GSK.L":{"price":1385,"change":-0.22,"high":1398,"low":1375,"vol":8000000,"mktcap":"55B"},
    "RIO.L":{"price":4820,"change":0.88,"high":4865,"low":4780,"vol":4000000,"mktcap":"72B"},
    "MC.PA":{"price":695,"change":0.55,"high":701,"low":689,"vol":500000,"mktcap":"347B"},
    "OR.PA":{"price":348,"change":0.33,"high":351,"low":345,"vol":600000,"mktcap":"190B"},
    "TTE.PA":{"price":56.2,"change":0.45,"high":56.8,"low":55.8,"vol":5000000,"mktcap":"135B"},
    "AIR.PA":{"price":168,"change":0.72,"high":169.5,"low":166.5,"vol":1200000,"mktcap":"131B"},
    "BNP.PA":{"price":69.5,"change":0.38,"high":70.2,"low":69.0,"vol":3000000,"mktcap":"84B"},
    "RMS.PA":{"price":2145,"change":0.82,"high":2165,"low":2128,"vol":120000,"mktcap":"227B"},
    "SAN.PA":{"price":91.2,"change":-0.15,"high":92.0,"low":90.8,"vol":2500000,"mktcap":"116B"},
    "SAP.DE":{"price":242,"change":0.68,"high":244,"low":240,"vol":1800000,"mktcap":"295B"},
    "SIE.DE":{"price":197,"change":0.42,"high":198.5,"low":195.5,"vol":1500000,"mktcap":"157B"},
    "BMW.DE":{"price":76.5,"change":-0.28,"high":77.2,"low":76.0,"vol":2800000,"mktcap":"46B"},
    "MBG.DE":{"price":61.2,"change":-0.42,"high":62.0,"low":60.8,"vol":3500000,"mktcap":"62B"},
    "VOW3.DE":{"price":102,"change":-0.55,"high":103.5,"low":101.5,"vol":2000000,"mktcap":"51B"},
    "ALV.DE":{"price":292,"change":0.52,"high":294,"low":290,"vol":900000,"mktcap":"118B"},
    "ADS.DE":{"price":218,"change":0.65,"high":220,"low":216,"vol":800000,"mktcap":"37B"},
    "DTE.DE":{"price":29.2,"change":0.21,"high":29.5,"low":29.0,"vol":8000000,"mktcap":"135B"},
    "NESN.SW":{"price":86.5,"change":-0.12,"high":87.2,"low":86.0,"vol":5000000,"mktcap":"228B"},
    "ROG.SW":{"price":268,"change":0.35,"high":270,"low":266,"vol":1800000,"mktcap":"148B"},
    "NOVN.SW":{"price":91.5,"change":0.22,"high":92.2,"low":91.0,"vol":4000000,"mktcap":"195B"},
    "ASML.AS":{"price":712,"change":1.12,"high":718,"low":706,"vol":800000,"mktcap":"280B"},
    "NOVO-B.CO":{"price":645,"change":0.88,"high":651,"low":639,"vol":5000000,"mktcap":"288B"},
    "RACE.MI":{"price":392,"change":0.55,"high":395,"low":389,"vol":300000,"mktcap":"71B"},
    "ENI.MI":{"price":13.8,"change":0.22,"high":14.0,"low":13.7,"vol":12000000,"mktcap":"45B"},
}

ASIA_SEEDS = {
    "7203.T":{"price":3620,"change":0.55,"high":3650,"low":3590,"vol":8000000,"mktcap":"235B"},
    "6758.T":{"price":2715,"change":0.82,"high":2740,"low":2690,"vol":6000000,"mktcap":"168B"},
    "9984.T":{"price":9120,"change":1.25,"high":9200,"low":9050,"vol":3500000,"mktcap":"151B"},
    "7974.T":{"price":8520,"change":-0.35,"high":8580,"low":8460,"vol":1200000,"mktcap":"110B"},
    "6501.T":{"price":3415,"change":0.68,"high":3440,"low":3390,"vol":2000000,"mktcap":"74B"},
    "8306.T":{"price":1712,"change":0.42,"high":1725,"low":1698,"vol":15000000,"mktcap":"118B"},
    "0700.HK":{"price":398,"change":0.75,"high":402,"low":394,"vol":18000000,"mktcap":"384B"},
    "9988.HK":{"price":92.5,"change":1.05,"high":93.8,"low":91.5,"vol":25000000,"mktcap":"188B"},
    "3690.HK":{"price":152,"change":0.88,"high":154,"low":150,"vol":12000000,"mktcap":"97B"},
    "9618.HK":{"price":147,"change":0.62,"high":149,"low":145,"vol":8000000,"mktcap":"91B"},
    "2318.HK":{"price":38.5,"change":-0.22,"high":39.0,"low":38.2,"vol":20000000,"mktcap":"68B"},
    "RELIANCE.NS":{"price":1458,"change":0.35,"high":1468,"low":1445,"vol":5000000,"mktcap":"193B"},
    "HDFCBANK.NS":{"price":1712,"change":0.28,"high":1722,"low":1700,"vol":8000000,"mktcap":"130B"},
    "TCS.NS":{"price":3521,"change":0.42,"high":3545,"low":3498,"vol":2500000,"mktcap":"128B"},
    "INFY.NS":{"price":1598,"change":0.55,"high":1610,"low":1585,"vol":6000000,"mktcap":"66B"},
    "ICICIBANK.NS":{"price":1312,"change":0.68,"high":1322,"low":1300,"vol":10000000,"mktcap":"92B"},
    "005930.KS":{"price":74800,"change":0.95,"high":75200,"low":74400,"vol":12000000,"mktcap":"446B"},
    "TSM":{"price":186.5,"change":1.12,"high":188.0,"low":185.0,"vol":15000000,"mktcap":"968B"},
    "SE":{"price":76.2,"change":1.35,"high":77.0,"low":75.5,"vol":5000000,"mktcap":"43B"},
    "GRAB":{"price":3.52,"change":0.85,"high":3.56,"low":3.48,"vol":18000000,"mktcap":"13B"},
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

def _fetch_yf_seeds(stock_list, seeds_dict, label):
    """Generic yfinance seed fetcher for any stock list."""
    try:
        import yfinance as _yf
        ticker_map = {s["yf"]: s["id"] for s in stock_list if s.get("yf")}
        print(f"[STARTUP] Fetching live {label} prices for {len(ticker_map)} tickers...")
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
                    prev = float(col.iloc[-2])
                    price = float(col.iloc[-1])
                    if price > 0 and stock_id in seeds_dict:
                        ch = round((price - prev) / (prev + 1e-9) * 100, 4)
                        seeds_dict[stock_id]["price"] = round(price, 2)
                        seeds_dict[stock_id]["change"] = ch
                        seeds_dict[stock_id]["high"] = round(price * 1.005, 2)
                        seeds_dict[stock_id]["low"] = round(price * 0.995, 2)
                        updated += 1
            except Exception:
                pass
        print(f"[STARTUP] Live prices loaded for {updated}/{len(ticker_map)} {label} stocks.")
    except Exception as e:
        print(f"[STARTUP] {label} yfinance fetch failed ({e}), using hardcoded seeds.")

def _fetch_live_eu_seeds():
    """Fetch live EU stock prices from yfinance at startup."""
    _fetch_yf_seeds(EU_STOCKS, EU_SEEDS, "EU")

def _fetch_live_asia_seeds():
    """Fetch live Asian stock prices from yfinance at startup."""
    _fetch_yf_seeds(ASIA_STOCKS, ASIA_SEEDS, "ASIA")

def _fetch_live_us_seeds():
    """Fetch current US prices from yfinance at startup to replace stale hardcoded seeds."""
    _fetch_yf_seeds(US_STOCKS, US_SEEDS, "US")

_fetch_yf_seeds(EU_STOCKS, EU_SEEDS, "EU")
_fetch_yf_seeds(ASIA_STOCKS, ASIA_SEEDS, "ASIA")
_fetch_live_us_seeds()

def _init_market(seeds, stocks, defaults):
    """Build market dict with open price stored for change% accumulation."""
    result = {}
    for s in stocks:
        d = {**seeds.get(s["id"], defaults)}
        d.setdefault("open", d.get("price", 100))   # session open reference
        result[s["id"]] = d
    return result

market_eu = _init_market(EU_SEEDS,   EU_STOCKS,   {"price":100,"change":0,"high":101,"low":99,"vol":1000000,"mktcap":"N/A"})
market_as = _init_market(ASIA_SEEDS, ASIA_STOCKS, {"price":100,"change":0,"high":101,"low":99,"vol":1000000,"mktcap":"N/A"})
market_us = _init_market(US_SEEDS,   US_STOCKS,   {"price":100,"change":0,"high":101,"low":99,"vol":1000000,"mktcap":"N/A"})

# ── Signal history — in-memory, last 100 tradeable signals ────────────────────
_signal_history      = []
_signal_history_lock = threading.Lock()

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
    for all EU, Asian, and US tickers using yfinance and pushes results to the queue."""
    ticker_map = {s["yf"]: s["id"] for s in (EU_STOCKS + ASIA_STOCKS + US_STOCKS) if s.get("yf")}
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
    tick_size = p * random.uniform(0.0002, 0.0015)
    d = 1 if random.random() > 0.47 else -1
    np_ = round(p + d * tick_size, 2)
    # Accumulate change from the session open price (stored as "open")
    open_price = cur.get("open", p)
    ch = round((np_ - open_price) / (open_price + 1e-9) * 100, 4)
    return np_, ch

def price_poll_thread():
    """Poll EU, Asian, and US stocks via Yahoo Finance every 15s; fallback to simulation."""
    _fail_count = 0
    _backoff_until = 0
    while True:
        try:
            if time.time() > _backoff_until:
                prices = fetch_stock_prices()
                if prices:
                    _fail_count = 0
                    for stock_id, price in prices.items():
                        if stock_id in market_eu:
                            seed_price = market_eu[stock_id].get("price") or price
                            ch = round((price - seed_price) / (seed_price + 1e-9) * 100, 4)
                            market_eu[stock_id]["price"] = price
                            market_eu[stock_id]["change"] = ch
                            broadcast({"type": "tick", "market": "eu", "id": stock_id, "price": price, "change": ch})
                        elif stock_id in market_as:
                            seed_price = market_as[stock_id].get("price") or price
                            ch = round((price - seed_price) / (seed_price + 1e-9) * 100, 4)
                            market_as[stock_id]["price"] = price
                            market_as[stock_id]["change"] = ch
                            broadcast({"type": "tick", "market": "as", "id": stock_id, "price": price, "change": ch})
                        elif stock_id in market_us:
                            seed_price = market_us[stock_id].get("price") or price
                            ch = round((price - seed_price) / (seed_price + 1e-9) * 100, 4)
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
            print(f"[PRICE POLL] {e}")

        # After 3 consecutive failures, back off for 5 minutes
        if _fail_count >= 3:
            print(f"[PRICE POLL] {_fail_count} failures — backing off 5 min, using simulation")
            _backoff_until = time.time() + 300
            _fail_count = 0

        # Simulate all stocks while live feed is unavailable
        for s in EU_STOCKS:
            p, ch = simulate_tick_stock(s["id"], market_eu[s["id"]], s["currency"])
            market_eu[s["id"]]["price"] = p
            market_eu[s["id"]]["change"] = ch
            broadcast({"type": "tick", "market": "eu", "id": s["id"], "price": p, "change": ch})
        for s in ASIA_STOCKS:
            p, ch = simulate_tick_stock(s["id"], market_as[s["id"]], s["currency"])
            market_as[s["id"]]["price"] = p
            market_as[s["id"]]["change"] = ch
            broadcast({"type": "tick", "market": "as", "id": s["id"], "price": p, "change": ch})
        for s in US_STOCKS:
            p, ch = simulate_tick_stock(s["id"], market_us[s["id"]], "USD")
            market_us[s["id"]]["price"] = p
            market_us[s["id"]]["change"] = ch
            broadcast({"type": "tick", "market": "us", "id": s["id"], "price": p, "change": ch})
        broadcast({"type": "status", "status": "simulated"})
        time.sleep(15)

threading.Thread(target=price_poll_thread, daemon=True).start()

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
        "eu_stocks": EU_STOCKS,
        "asia_stocks": ASIA_STOCKS,
        "us_stocks": US_STOCKS,
        "market_eu": market_eu,
        "market_as": market_as,
        "market_us": market_us,
    })

@app.route("/api/stream")
def stream():
    q = queue.Queue(maxsize=500)
    with subscribers_lock: subscribers.append(q)
    def generate():
        yield "data: " + json.dumps({"type":"snapshot","market_eu":market_eu,"market_as":market_as,"market_us":market_us}) + "\n\n"
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
def _build_why_bullets(indicators, direction, news_score, news_items, data_src):
    """Return up to 5 human-readable bullets explaining a signal."""
    bullets = []
    rsi      = indicators.get("rsi", 50)
    macd     = indicators.get("macd", "")
    macd_h   = indicators.get("macd_hist", 0)
    bb_pct   = indicators.get("bb_pct", 50)
    stoch_k  = indicators.get("stoch_k", 50)
    adx      = indicators.get("adx", 20)
    vol_r    = indicators.get("vol_ratio", 1.0)
    candle   = indicators.get("candle", "")
    ema9     = indicators.get("ema9", 0)
    ema50    = indicators.get("ema50", 0)

    if direction == "BULLISH":
        if rsi < 35:   bullets.append(f"RSI oversold at {rsi:.0f} — recovery expected")
        elif rsi < 50: bullets.append(f"RSI at {rsi:.0f} — room to move higher")
        if macd == "BULLISH":
            bullets.append(f"MACD bullish cross (hist {'+' if macd_h>=0 else ''}{macd_h:.4f})")
        if bb_pct < 25:
            bullets.append(f"Price near lower Bollinger Band ({bb_pct:.0f}%) — mean-reversion setup")
        if stoch_k < 25:
            bullets.append(f"Stochastic oversold at {stoch_k:.0f} — bounce likely")
        if ema9 > ema50 > 0:
            bullets.append("EMA 9 above EMA 50 — short-term uptrend confirmed")
        if vol_r > 1.5:
            bullets.append(f"Volume {vol_r:.1f}× avg — institutional buying detected")
        if candle in ("HAMMER", "BULLISH BAR"):
            bullets.append(f"Candle pattern: {candle}")
    elif direction == "BEARISH":
        if rsi > 65:   bullets.append(f"RSI overbought at {rsi:.0f} — pullback expected")
        elif rsi > 50: bullets.append(f"RSI at {rsi:.0f} — momentum fading")
        if macd == "BEARISH":
            bullets.append(f"MACD bearish cross (hist {'+' if macd_h>=0 else ''}{macd_h:.4f})")
        if bb_pct > 75:
            bullets.append(f"Price near upper Bollinger Band ({bb_pct:.0f}%) — reversal zone")
        if stoch_k > 75:
            bullets.append(f"Stochastic overbought at {stoch_k:.0f} — sell pressure building")
        if ema9 < ema50 and ema50 > 0:
            bullets.append("EMA 9 below EMA 50 — downtrend in force")
        if vol_r > 1.5:
            bullets.append(f"Volume {vol_r:.1f}× avg — distribution selling")
        if candle in ("SHOOTING STAR", "BEARISH BAR", "DOJI"):
            bullets.append(f"Candle pattern: {candle}")

    if adx > 40:   bullets.append(f"ADX {adx:.0f} — strong trending environment")
    elif adx < 18: bullets.append(f"ADX {adx:.0f} — weak trend, trade with caution")

    n_bull = sum(1 for it in news_items if it.get("score", 0) > 0.5)
    n_bear = sum(1 for it in news_items if it.get("score", 0) < -0.5)
    if n_bull > 0 and direction == "BULLISH":
        bullets.append(f"News: {n_bull} bullish headline{'s' if n_bull>1 else ''} (avg score {news_score:+.1f})")
    elif n_bear > 0 and direction == "BEARISH":
        bullets.append(f"News: {n_bear} bearish headline{'s' if n_bear>1 else ''} (avg score {news_score:+.1f})")

    if data_src == "real_candles":
        bullets.append("Analysis based on real OHLCV candle data")
    else:
        bullets.append("Approximated from current price/change (real candle data unavailable)")

    return bullets[:5]


def _build_trade_brief(direction, conf, price, t1, t2, stop, rr,
                       indicators, news_score, news_items, data_src):
    action   = "LONG" if direction == "BULLISH" else ("SHORT" if direction == "BEARISH" else "WAIT")
    why      = _build_why_bullets(indicators, direction, news_score, news_items, data_src)
    sl_dist  = abs(price - stop)
    tp1_dist = abs(t1 - price)
    tp2_dist = abs(t2 - price)
    return {
        "action":    action,
        "entry":     price,
        "stop":      stop,
        "tp1":       t1,
        "tp2":       t2,
        "slPct":     round(sl_dist  / price * 100, 2) if price else 0,
        "tp1Pct":    round(tp1_dist / price * 100, 2) if price else 0,
        "tp2Pct":    round(tp2_dist / price * 100, 2) if price else 0,
        "rr1":       rr,
        "rr2":       round(tp2_dist / (sl_dist + 1e-9), 2),
        "confidence": conf,
        "why":       why,
        "dataSource": data_src,
    }


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
                 else "ML ran on available data")

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
        market_type = data.get("market", "us")  # "eu", "as", or "us"
        stock_list = EU_STOCKS if market_type == "eu" else (ASIA_STOCKS if market_type == "as" else US_STOCKS)
        stock_info = next((s for s in stock_list if s["id"] == stock_id), None)
        if not stock_info:
            return jsonify({"error": "Unknown stock"}), 400

        mkt = market_eu if market_type == "eu" else (market_as if market_type == "as" else market_us)
        state = mkt.get(stock_id, {})
        price  = float(state.get("price", 100))
        change = float(state.get("change", 0))
        high   = float(state.get("high", price * 1.02))
        low    = float(state.get("low", price * 0.98))
        trade_size = float(data.get("tradeSize", 0))

        # Fetch real OHLCV
        _yf = stock_info.get("yf") or stock_id
        df = fetch_ohlcv_stock(_yf)
        indicators = None
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
        curr_sym = CURR_SYMBOLS.get(currency, "$")

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

        # ── Trade brief — human-readable signal card ──────────────────────
        trade_brief = _build_trade_brief(direction, conf, price, t1, t2, stop, rr,
                                         indicators, news_score, news_items, data_src)

        # ── Record to signal history when tradeable ───────────────────────
        if consensus.get("tradeable"):
            with _signal_history_lock:
                _signal_history.append({
                    "id": stock_id, "name": stock_info["name"],
                    "market": market_type, "direction": direction, "conf": conf,
                    "entry": price, "stop": stop, "tp1": t1, "tp2": t2, "rr": rr,
                    "time": time.strftime("%d %b %H:%M", time.gmtime()),
                })
                if len(_signal_history) > 100:
                    _signal_history.pop(0)

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
            "tradeBrief": trade_brief,
        }))

    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/api/predict", methods=["POST"])
def predict():
    """
    Standalone ML prediction endpoint.
    POST { "stockId": "ASML.AS" }
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

    if market_type in ("eu","both"): scan_list(EU_STOCKS,   market_eu, "eu")
    if market_type in ("as","both"): scan_list(ASIA_STOCKS, market_as, "as")
    if market_type in ("us","both"): scan_list(US_STOCKS,   market_us, "us")
    results.sort(key=lambda x: x["conf"], reverse=True)
    return jsonify({"results": results, "count": len(results)})


@app.route("/api/brief")
def brief():
    """Daily signal briefing — top longs and shorts across all markets."""
    longs, shorts = [], []

    def _scan_for_brief(stock_list, mkt_data, mkt_type):
        for s in stock_list:
            state  = mkt_data.get(s["id"], {})
            price  = float(state.get("price", 100))
            change = float(state.get("change", 0))
            high   = float(state.get("high", price * 1.02))
            low    = float(state.get("low",  price * 0.98))
            rng    = high - low or price * 0.02
            ind = {
                "rsi":    min(98, max(2, 50 + change * 4.2)),
                "rsi_7":  min(98, max(2, 50 + change * 6)),
                "macd":   "BULLISH" if change > 0 else "BEARISH",
                "macd_hist": change * 0.001,
                "bb_pct": ((price - low) / rng) * 100,
                "stoch_k": ((price - low) / rng) * 100,
                "adx":    min(80, max(10, abs(change) * 8 + 20)),
                "vol_ratio": 1.0,
                "candle": "BULLISH BAR" if change > 0 else "BEARISH BAR",
                "ema9": price, "ema50": price,
            }
            direction, conf, bp = rule_based_signal(ind, change)
            if direction == "NEUTRAL" or conf < 58:
                continue
            atr_m = rng * 0.015 / (price + 1e-9)
            if direction == "BULLISH":
                t1 = round(price * (1 + atr_m * 1.5), 2)
                t2 = round(price * (1 + atr_m * 3.0), 2)
                stop = round(price * (1 - atr_m * 1.0), 2)
            else:
                t1 = round(price * (1 - atr_m * 1.5), 2)
                t2 = round(price * (1 - atr_m * 3.0), 2)
                stop = round(price * (1 + atr_m * 1.0), 2)
            rr = round(abs(t1 - price) / (abs(price - stop) + 1e-9), 2)
            entry = {
                "id": s["id"], "name": s["name"], "market": mkt_type,
                "sector": s["sector"], "currency": s["currency"], "color": s["color"],
                "price": price, "change": change, "direction": direction,
                "conf": conf, "t1": t1, "t2": t2, "stop": stop, "rr": rr,
            }
            if direction == "BULLISH": longs.append(entry)
            else: shorts.append(entry)

    _scan_for_brief(EU_STOCKS,   market_eu, "eu")
    _scan_for_brief(ASIA_STOCKS, market_as, "as")
    _scan_for_brief(US_STOCKS,   market_us, "us")

    longs.sort(key=lambda x: x["conf"],  reverse=True)
    shorts.sort(key=lambda x: x["conf"], reverse=True)

    total    = len(longs) + len(shorts)
    bull_pct = round(len(longs) / total * 100) if total else 50
    if bull_pct >= 70:   mood = "STRONGLY BULLISH"
    elif bull_pct >= 55: mood = "CAUTIOUS BULLISH"
    elif bull_pct <= 30: mood = "STRONGLY BEARISH"
    elif bull_pct <= 45: mood = "CAUTIOUS BEARISH"
    else:                mood = "NEUTRAL"

    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    return jsonify(_sanitize({
        "date": now.strftime("%d %b %Y"),
        "time": now.strftime("%H:%M UTC"),
        "longs":        longs[:4],
        "shorts":       shorts[:4],
        "totalSignals": total,
        "marketMood":   mood,
        "bullPct":      bull_pct,
    }))


@app.route("/api/signal_history")
def signal_history_route():
    with _signal_history_lock:
        return jsonify({"signals": list(reversed(_signal_history[-50:]))})


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
    print(f"[NEXUS] Markets: EU ({len(EU_STOCKS)} stocks), Asia ({len(ASIA_STOCKS)} stocks), US ({len(US_STOCKS)} stocks)")
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