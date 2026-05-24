# 🚀 Nexus Stock AI — Deployment Guide

Complete step-by-step guide to deploy for free with a self-improving AI pipeline.

---

## Architecture at a Glance

```
UptimeRobot (free) ──pings every 5 min──→ Render Free Tier (Flask app)
                                                │
                                     Scraper polls NGX live prices (30s)
                                                │
                                          Supabase (free)
                                      PostgreSQL persistent storage
                                      [Real NGX price history grows daily]
                                                │
                                     Every Sunday 02:00 UTC
                                     XGBoost auto-retrains on real data
                                     Accuracy improves over time
```

---

## Step 1 — Set Up Supabase (Persistent Database)

**Time: ~5 minutes | Cost: Free**

1. Go to [supabase.com](https://supabase.com) → Sign up → New Project
2. Name it `nexus-stock-ai`, choose a region close to your users
3. Wait ~2 min for project to spin up
4. Go to **SQL Editor** → **New Query** → paste contents of `supabase_schema.sql` → **Run**
5. Go to **Project Settings** → **API**:
   - Copy **Project URL** → this is your `SUPABASE_URL`
   - Copy **anon / public** key → this is your `SUPABASE_KEY`

---

## Step 2 — Deploy to Render

**Time: ~5 minutes | Cost: Free**

1. Go to [render.com](https://render.com) → Sign up → **New** → **Web Service**
2. Connect your GitHub repository
3. Render auto-detects `render.yaml` — it will configure everything
4. In **Environment Variables** section, add:
   ```
   SUPABASE_URL = https://your-project-id.supabase.co
   SUPABASE_KEY = your-anon-public-key
   ```
5. Click **Deploy** → wait ~3 minutes for build

Your app will be live at: `https://nexus-stock-ai.onrender.com` (or similar)

---

## Step 3 — Set Up UptimeRobot (Keep-Alive)

**Time: ~2 minutes | Cost: Free**

Without this, Render free tier shuts down after 15 min of inactivity.
UptimeRobot pings the site every 5 minutes to keep it awake 24/7.

1. Go to [uptimerobot.com](https://uptimerobot.com) → Sign up (free)
2. Click **Add New Monitor**:
   - Monitor Type: **HTTP(s)**
   - Friendly Name: `Nexus Stock AI`
   - URL: `https://your-app-name.onrender.com`
   - Monitoring Interval: **5 minutes**
3. Click **Create Monitor**

That's it! The app now runs 24/7, collecting data automatically.

---

## What Happens Automatically After Deployment

```
Day 1:    App deploys → Synthetic NGX history (120 days, deterministic)
          Real prices start streaming every 30 seconds
          Data saved to Supabase automatically

Day 2+:   Real price history accumulates in Supabase daily
          Each day: 1 row per NGX stock stored permanently

Day 30:   First auto-retrain triggers (Sunday 02:00 UTC)
          XGBoost trains on 30 days of REAL NGX data
          Accuracy improves from ~53% baseline

Day 60:   Second retrain — 60 days of real data
          NGX-specific patterns being learned

Day 90:   Third retrain — meaningful accuracy improvement
          Predictions increasingly reliable for NGX stocks

Day 180+: Self-improving pipeline fully mature
          Real NGX market microstructure understood by the model
```

---

## New API Endpoints (Added in This Update)

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/db_stats` | GET | Real data accumulation progress |
| `/api/retrain` | POST | Manually trigger model retrain |
| `/api/training_status` | GET | Scheduler state + last retrain results |

### Check data progress:
```bash
curl https://your-app.onrender.com/api/db_stats
```
```json
{
  "connected": true,
  "tickers": 87,
  "records": 8700,
  "days_stored": 100,
  "data_quality": "good",
  "next_milestone": "90-day milestone: 10 days to go"
}
```

### Manually trigger retraining (after 30+ days):
```bash
curl -X POST https://your-app.onrender.com/api/retrain \
  -H "Content-Type: application/json" \
  -d '{"reason": "manual_test"}'
```

---

## Local Development

```bash
# Clone and enter the project
cd "stock-nexus-fixed copy"

# Install dependencies
pip install -r requirements.txt

# Create .env from template
cp .env.example .env
# Edit .env with your Supabase credentials

# Run the app
python app.py
# → http://localhost:5002
```

---

## US Stock Universe (Expanded: 20 → 79 stocks)

The app now tracks stocks across all major S&P 500 sectors:

| Sector | Count | Examples |
|--------|-------|---------|
| Technology | 17 | AAPL, MSFT, NVDA, AMD, PLTR |
| Consumer | 10 | AMZN, TSLA, NKE, MCD, COST |
| Finance | 13 | JPM, V, MA, GS, COIN |
| Healthcare | 10 | LLY, UNH, ABBV, PFE |
| Energy | 5 | XOM, CVX, COP, SLB |
| Industrial | 7 | CAT, BA, GE, HON |
| Materials | 3 | LIN, NEM, FCX |
| ETFs | 9 | SPY, QQQ, GLD, TLT |

All powered by **yfinance** (free, no API key needed).

---

## Semi-Auto Trading (Phase 2 — Future)

Once the model achieves consistent >60% accuracy:

1. Sign up at [alpaca.markets](https://alpaca.markets) (free paper trading)
2. Get API key + secret from Alpaca dashboard
3. Add to Render env vars: `ALPACA_KEY` + `ALPACA_SECRET`
4. Paper trade for 30 days to validate real-world performance
5. If results are good → enable live trading with capital controls

**Current Status:** Data collection phase. Semi-auto trading unlocks when:
- ✅ 90+ days of real NGX data accumulated
- ✅ XGBoost accuracy consistently above 60%
- ✅ Backtesting shows positive expectancy

---

## Monitoring & Maintenance

### Check if everything is working:
```
/api/db_stats          → Is Supabase receiving data?
/api/training_status   → When did last retrain run?
/api/predictor_status  → Is ML ready?
/api/model_status      → Is chart DB ready?
```

### Common issues:
| Problem | Fix |
|---------|-----|
| `connected: false` in db_stats | Check SUPABASE_URL and SUPABASE_KEY env vars |
| App sleeping (slow first load) | UptimeRobot not set up or wrong URL |
| NGX prices showing as "simulated" | NGX scraper rate-limited — auto-recovers in 5 min |
| Retrain fails: "not enough data" | Need 30+ days of data — keep the site running |
