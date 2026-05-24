"""
database.py — Persistent NGX Price History Storage (Supabase)
=============================================================
Saves real NGX price data to Supabase PostgreSQL so history survives
server restarts and accumulates over time — enabling the AI to improve
automatically as real data replaces synthetic history.

Falls back gracefully to in-memory-only mode if Supabase is not configured.

Supabase SQL schema (run once in your Supabase SQL editor):
  → See supabase_schema.sql in this directory
"""

import os
import logging
from datetime import date, datetime
from typing import Dict, List

log = logging.getLogger(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "").strip()

_sb = None   # supabase.Client instance
_ok = False  # True only when successfully connected


def init_db() -> bool:
    """Initialize Supabase connection. Called once at import time."""
    global _sb, _ok
    if not SUPABASE_URL or not SUPABASE_KEY:
        log.warning(
            "[DB] SUPABASE_URL/KEY not set — price history in-memory only "
            "(data resets on every server restart). Set env vars to enable persistence."
        )
        return False
    try:
        from supabase import create_client  # type: ignore
        _sb = create_client(SUPABASE_URL, SUPABASE_KEY)
        # Lightweight connectivity check
        _sb.table("ngx_price_history").select("ticker").limit(1).execute()
        _ok = True
        log.info("[DB] ✅ Supabase connected — NGX history will persist across restarts")
        return True
    except ImportError:
        log.error("[DB] ❌ supabase package not installed. Run: pip install supabase")
        return False
    except Exception as e:
        log.error(f"[DB] ❌ Supabase connection failed ({e}) — falling back to in-memory")
        return False


def is_connected() -> bool:
    return _ok


def save_ngx_prices(prices: Dict[str, dict]) -> bool:
    """
    Upsert today's NGX prices into Supabase.
    Called after every successful scraper poll. The UNIQUE(ticker, date)
    constraint deduplicates — only the final price of the day is stored.
    """
    if not _ok or not _sb:
        return False
    today = date.today().isoformat()
    rows = []
    for ticker, data in prices.items():
        p = float(data.get("price", 0))
        if p <= 0:
            continue
        rows.append({
            "ticker":     ticker,
            "date":       today,
            "close":      p,
            "volume":     int(data.get("volume", 0) or 0),
            "updated_at": datetime.utcnow().isoformat(),
        })
    if not rows:
        return False
    try:
        _sb.table("ngx_price_history").upsert(rows, on_conflict="ticker,date").execute()
        return True
    except Exception as e:
        log.error(f"[DB] save_ngx_prices failed: {e}")
        return False


def load_all_ngx_history(days: int = 200) -> Dict[str, List[dict]]:
    """
    Load stored price history for ALL NGX tickers in a single query.
    Returns: {ticker: [{"date": str, "close": float, "vol": int}, ...]}
    Entries are sorted oldest-first, trimmed to `days` per ticker.
    """
    if not _ok or not _sb:
        return {}
    try:
        result = (
            _sb.table("ngx_price_history")
            .select("ticker,date,close,volume")
            .order("date", desc=False)
            .limit(days * 150)   # 200 days × ~100 tickers = 20,000 rows max
            .execute()
        )
        history: Dict[str, List[dict]] = {}
        for r in (result.data or []):
            t = r["ticker"]
            history.setdefault(t, []).append({
                "date":  r["date"],
                "close": float(r["close"]),
                "vol":   int(r.get("volume") or 0),
            })
        # Trim each ticker to the most recent `days`
        for t in history:
            history[t] = history[t][-days:]
        log.info(f"[DB] Loaded history for {len(history)} NGX tickers from Supabase")
        return history
    except Exception as e:
        log.error(f"[DB] load_all_ngx_history failed: {e}")
        return {}


def get_data_summary() -> dict:
    """Return a summary of how much real NGX data has been accumulated."""
    if not _ok or not _sb:
        return {
            "connected": False,
            "message": "Supabase not configured — set SUPABASE_URL and SUPABASE_KEY",
        }
    try:
        result  = _sb.table("ngx_price_history").select("ticker,date").execute()
        rows    = result.data or []
        tickers = {r["ticker"] for r in rows}
        dates   = sorted({r["date"] for r in rows})
        days_stored = (
            (datetime.fromisoformat(dates[-1]) - datetime.fromisoformat(dates[0])).days + 1
            if len(dates) >= 2 else (1 if dates else 0)
        )
        return {
            "connected":   True,
            "tickers":     len(tickers),
            "records":     len(rows),
            "oldest_date": dates[0]  if dates else None,
            "newest_date": dates[-1] if dates else None,
            "days_stored": days_stored,
            "data_quality": (
                "excellent" if days_stored >= 90 else
                "good"      if days_stored >= 60 else
                "growing"   if days_stored >= 30 else
                "early"
            ),
        }
    except Exception as e:
        return {"connected": False, "error": str(e)}


# Initialise on import
init_db()
