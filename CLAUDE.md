# CLAUDE.md — Stock Nexus Project Context

## What This Project Is
**Stock Nexus** — a Flask-based global equity intelligence terminal for EU, Asian, and US stocks.
Live at: https://nexus-stock-ty2t.onrender.com/
GitHub (deployment repo): https://github.com/Xomie1/Nexus-Stock.git

## Git Author Config (run at session start, every session)
```bash
git config --local user.name "Oloruntobiloba"
git config --local user.email "tobiakindele30@gmail.com"
```
The repo's local `.git/config` is wiped on each fresh clone. Always run these two lines before the first commit so contributions count toward the GitHub profile.

## Critical: Two Repos, One Codebase

| Repo | Purpose | Push access from this session |
|------|---------|-------------------------------|
| `Xomie1/Nexus-Stock` | **Render deployment** — what the live site runs | ✅ Use force push via HTTPS |
| `Xomie1/Nexus-Stock-AI` | Development/backup | ✅ Works via `git push origin` |

**Deployment command (run after every change):**
```bash
git push --force https://github.com/Xomie1/Nexus-Stock.git HEAD:main
```
The local proxy (port 41729) is unreliable — always use the direct HTTPS URL above.
Render auto-redeploys ~3 min after push to Nexus-Stock main.

## Source Code Location
All editable source lives here (on this machine):
```
/home/user/Nexus-Stock-AI/stock-nexus-fixed copy/
├── app.py                  # Flask backend (1500+ lines)
├── static/css/style.css    # Dark terminal UI
├── static/js/app.js        # Frontend JS (1400+ lines)
├── templates/index.html    # Single-page app HTML
├── requirements.txt
├── runtime.txt             # python-3.11.9 (pins Render build)
├── Procfile                # web: python app.py
├── render.yaml
└── model/                  # XGBoost/LSTM + chart retrieval
```

## Stack
- **Backend:** Flask + gevent SSE, yfinance (real OHLCV), Google News RSS
- **Frontend:** Vanilla JS, dark terminal CSS (Rajdhani/JetBrains Mono/Bebas Neue fonts)
- **Stocks:** 29 EU stocks (.L .PA .DE .AS .SW .CO .MI), 20 Asian stocks (.T .HK .NS .KS), 76 US stocks
- **Multi-currency:** GBP(p), EUR(€), JPY(¥), HKD(HK$), INR(₹), KRW(₩), CHF(Fr), DKK(kr), USD($)
- **No Supabase** — removed entirely, yfinance has years of real OHLCV history

## Current Feature State (as of v3)

### Working
- 7-page SPA: Dashboard, EU/Asia, US Markets, Signal/Predictor, Scanner, Chart Vision AI, Trade Journal
- Real-time SSE price streaming (30s yfinance + simulated ticks)
- `/api/analyze` — full technical analysis: RSI/MACD/BB/Stoch/ADX/ATR/EMAs + news sentiment
- `/api/scan` — fast scan of all 125 stocks, returns high-confidence setups
- `/api/brief` — daily briefing: top longs + shorts with entry/SL/TP/R:R
- `/api/signal_history` — in-memory log of tradeable signals fired this session
- **Trade Brief Card** — appears at top of predictor panel: LONG/SHORT/WAIT + entry/SL/TP1/TP2/R:R + why bullets
- **Morning Brief** — auto-loads on dashboard, mini cards for each setup, refreshes every 10 min
- **Auto-scan** — runs silently on page load (2s delay) + every 5 min, updates nav badge
- **Signal History** — appended below scanner results after each scan
- Trade Journal with localStorage persistence and CSV export

### Known Limitations
- ML models (XGBoost/LSTM) not trained — signals use rule-based engine only (still useful)
- Chart Vision AI requires `python model/build_db.py` to be run once first
- No alerts/notifications when price hits SL or TP

## User Goals
User wants to:
1. Get clear LONG/SHORT signals with entry, stop, TP1, TP2, confidence %
2. Trade European + Asian + US stocks starting with ~$100-$200
3. Use Trading 212 (fractional shares) for execution — no auto-trading yet
4. Eventually scale up once signals prove reliable

## Deployment Pipeline
1. Edit files in `/home/user/Nexus-Stock-AI/stock-nexus-fixed copy/`
2. Commit to `claude/festive-cori-vRHmi` branch: `git push origin claude/festive-cori-vRHmi`
3. Build zip: `cd /tmp && zip -r nexus-stock-vN.zip nexus-export/`
4. Send zip to user: `SendUserFile(["/tmp/nexus-stock-vN.zip"])`
5. User runs from `nexus-clean/`:
   ```
   git add -A && git commit -m "..." && git push origin main
   ```
6. Render auto-redeploys (~3 min)

## Git State
- Working branch: `claude/festive-cori-vRHmi`
- Latest commit: `c2d2e57` — "Add signal engine: trade brief cards, morning briefing, signal history, why bullets"
- PR: https://github.com/Xomie1/Nexus-Stock-AI/pull/1

## Key API Routes
| Route | Method | Purpose |
|-------|--------|---------|
| `/api/seed` | GET | Bootstrap all stock lists + initial prices |
| `/api/stream` | GET | SSE — real-time price ticks |
| `/api/analyze` | POST | Full analysis: indicators + trade brief + news |
| `/api/scan` | POST | Quick scan all stocks, filter by market |
| `/api/brief` | GET | Daily top longs/shorts briefing |
| `/api/signal_history` | GET | In-memory tradeable signal log |
| `/api/analyze_chart` | POST | Chart image similarity search |
| `/api/predictor_status` | GET | ML model readiness |

## Next Improvements (User's Priority)
- News API upgrade (currently Google News RSS keyword scoring — works but basic)
- SL/TP alert notifications (toast or sound when price approaches)
- Backtest tab to validate signal accuracy historically
- Paper trading mode that auto-records signals to journal
