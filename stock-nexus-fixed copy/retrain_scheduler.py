"""
retrain_scheduler.py — Automatic Weekly AI Retraining
======================================================
Runs every Sunday at 02:00 UTC to retrain XGBoost models on accumulated
real NGX price data from Supabase, replacing synthetic history with real
market patterns — gradually improving prediction accuracy over time.

Design decisions:
  - XGBoost-only retrain (LSTM skipped — too slow/memory-heavy for free tier)
  - Requires ≥30 days of real NGX data before triggering
  - Hot-reloads predictor after training (zero-downtime, no server restart)
  - Manual trigger available via POST /api/retrain
  - Scheduler state exposed via GET /api/training_status
"""

import os, sys, json, logging, threading, time
from datetime import datetime
from pathlib import Path

log = logging.getLogger(__name__)

# Paths
_BASE  = Path(__file__).parent
_MODEL = _BASE / "model"
_OUT   = _MODEL / "trained"

# Retraining state (thread-safe reads, lock-protected writes)
_lock   = threading.Lock()
_status = {
    "running":     False,
    "last_run":    None,
    "last_result": None,
    "next_run":    None,
    "history":     [],      # last 10 retrain results
}


def _build_ngx_features(history: dict) -> tuple:
    """
    Convert Supabase NGX price history → XGBoost training arrays.
    Synthesises OHLC from close prices (OHLCV needed by make_features).
    Returns (X, y_direction, y_change) or (None, None, None) on failure.
    """
    import numpy as np
    import pandas as pd
    sys.path.insert(0, str(_MODEL))
    from train_model import make_features, FEATURE_COLS

    X_list, yd_list, yc_list = [], [], []
    ok = skip = 0

    for ticker, hist in history.items():
        if len(hist) < 60:   # need ≥60 days for most indicators to stabilise
            skip += 1
            continue
        try:
            df = (
                pd.DataFrame(hist)
                .assign(date=lambda d: pd.to_datetime(d["date"]))
                .set_index("date")
                .sort_index()
                .rename(columns={"close": "Close", "vol": "Volume"})
            )
            # Synthesise realistic OHLC from close
            df["High"]  = df["Close"] * 1.012
            df["Low"]   = df["Close"] * 0.988
            df["Open"]  = df["Close"].shift(1).fillna(df["Close"])

            df = make_features(df)
            df["target_chg"] = df["Close"].pct_change(1).shift(-1)
            df["target_dir"] = (df["target_chg"] > 0).astype(int)
            df = df.dropna(subset=FEATURE_COLS + ["target_chg", "target_dir"])

            if len(df) < 30:
                skip += 1
                continue

            X_list.append(df[FEATURE_COLS].values.astype("float32"))
            yd_list.append(df["target_dir"].values.astype(int))
            yc_list.append(df["target_chg"].values.astype("float32"))
            ok += 1
        except Exception as e:
            log.debug(f"[RETRAIN] Feature build skipped for {ticker}: {e}")
            skip += 1

    log.info(f"[RETRAIN] NGX features: {ok} tickers built, {skip} skipped")
    if not X_list:
        return None, None, None

    import numpy as np
    return (
        np.vstack(X_list),
        np.concatenate(yd_list),
        np.concatenate(yc_list),
    )


def run_retrain(trigger: str = "scheduled") -> dict:
    """
    Core retraining function. Thread-safe via _lock.
    Returns a result dict — always, even on failure.
    """
    if not _lock.acquire(blocking=False):
        return {"success": False, "reason": "Retraining already in progress"}

    _status["running"] = True
    t0 = time.time()

    try:
        # ── Check xgboost is available ─────────────────────────────────────
        try:
            import xgboost as xgb
        except ImportError:
            return {"success": False, "reason": "xgboost not installed (pip install xgboost)"}

        import numpy as np
        import joblib
        from sklearn.preprocessing import RobustScaler
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import accuracy_score, mean_absolute_error

        # ── Load real NGX data from Supabase ───────────────────────────────
        ngx_history = {}
        try:
            from database import load_all_ngx_history, is_connected
            if is_connected():
                ngx_history = load_all_ngx_history(days=200)
        except Exception as e:
            log.warning(f"[RETRAIN] Could not load Supabase data: {e}")

        real_days = max((len(v) for v in ngx_history.values()), default=0)
        log.info(f"[RETRAIN] Real NGX data: {len(ngx_history)} tickers, {real_days} max days")

        # ── Guard: need ≥30 real days ──────────────────────────────────────
        if real_days < 30:
            msg = (
                f"Not enough real data yet ({real_days}/30 days minimum). "
                "Keep the site running and data will accumulate automatically."
            )
            log.info(f"[RETRAIN] Skipped — {msg}")
            return {"success": False, "reason": msg, "real_days": real_days}

        # ── Build feature matrices ─────────────────────────────────────────
        X, y_dir, y_chg = _build_ngx_features(ngx_history)
        if X is None or len(X) < 100:
            n = len(X) if X is not None else 0
            return {"success": False, "reason": f"Only {n} usable training rows — need ≥100"}

        log.info(f"[RETRAIN] Training XGBoost on {len(X)} samples...")

        # ── Fit scaler ─────────────────────────────────────────────────────
        scaler = RobustScaler()
        X_s    = scaler.fit_transform(X)

        # ── Direction classifier ───────────────────────────────────────────
        Xtr, Xval, ytr_d, yval_d = train_test_split(
            X_s, y_dir, test_size=0.15, shuffle=False
        )
        clf = xgb.XGBClassifier(
            n_estimators=400, max_depth=4, learning_rate=0.03,
            subsample=0.7, colsample_bytree=0.7,
            min_child_weight=5, gamma=0.1,
            reg_alpha=0.1, reg_lambda=1.5,
            eval_metric="logloss", verbosity=0, n_jobs=-1,
        )
        clf.fit(Xtr, ytr_d, eval_set=[(Xval, yval_d)], verbose=False)
        xgb_acc = float(accuracy_score(yval_d, clf.predict(Xval)))

        # ── Change regressor ───────────────────────────────────────────────
        Xtr2, Xval2, ytr_c, yval_c = train_test_split(
            X_s, y_chg, test_size=0.15, shuffle=False
        )
        reg = xgb.XGBRegressor(
            n_estimators=400, max_depth=4, learning_rate=0.03,
            subsample=0.7, colsample_bytree=0.7,
            verbosity=0, n_jobs=-1,
        )
        reg.fit(Xtr2, ytr_c, eval_set=[(Xval2, yval_c)], verbose=False)
        xgb_mae = float(mean_absolute_error(yval_c, reg.predict(Xval2)))

        # ── Save models ────────────────────────────────────────────────────
        _OUT.mkdir(parents=True, exist_ok=True)
        joblib.dump(clf,    _OUT / "xgb_direction.pkl")
        joblib.dump(reg,    _OUT / "xgb_change.pkl")
        joblib.dump(scaler, _OUT / "lstm_scaler.pkl")

        # ── Update model_meta.json ─────────────────────────────────────────
        meta_path = _OUT / "model_meta.json"
        try:
            meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
        except Exception:
            meta = {}

        now_str = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        meta.update({
            "last_retrain":      now_str,
            "retrain_trigger":   trigger,
            "n_samples":         int(len(X)),
            "real_ngx_days":     real_days,
            "real_ngx_tickers":  len(ngx_history),
            "xgb_direction_acc": round(xgb_acc, 4),
            "xgb_change_mae":    round(xgb_mae, 6),
        })
        meta_path.write_text(json.dumps(meta, indent=2))

        elapsed = round(time.time() - t0, 1)
        result = {
            "success":          True,
            "trigger":          trigger,
            "timestamp":        now_str,
            "samples":          int(len(X)),
            "real_ngx_days":    real_days,
            "real_ngx_tickers": len(ngx_history),
            "xgb_accuracy":     round(xgb_acc, 4),
            "xgb_accuracy_pct": f"{xgb_acc:.1%}",
            "xgb_mae_pct":      round(xgb_mae * 100, 3),
            "elapsed_sec":      elapsed,
        }

        # Read previous accuracy for comparison
        prev_acc = meta.get("xgb_direction_acc", 0.531)
        delta    = xgb_acc - prev_acc
        log.info(
            f"[RETRAIN] ✅ Done in {elapsed}s — "
            f"accuracy: {xgb_acc:.1%} ({delta:+.1%} vs prev {prev_acc:.1%})"
        )

        # ── Hot-reload predictor (zero-downtime) ───────────────────────────
        try:
            sys.path.insert(0, str(_BASE))
            import predictor as _pred
            _pred.reload_models()
            log.info("[RETRAIN] Predictor hot-reloaded ✅")
        except Exception as e:
            log.warning(f"[RETRAIN] Predictor reload failed: {e}")

        # Update status history (keep last 10)
        _status["last_run"]    = now_str
        _status["last_result"] = result
        _status["history"]     = ([result] + _status.get("history", []))[:10]
        return result

    except Exception as e:
        import traceback
        tb  = traceback.format_exc()
        err = {"success": False, "reason": str(e), "trigger": trigger}
        log.error(f"[RETRAIN] ❌ Failed: {e}\n{tb}")
        _status["last_result"] = err
        _status["history"]     = ([err] + _status.get("history", []))[:10]
        return err

    finally:
        _status["running"] = False
        _lock.release()


def run_retrain_background(trigger: str = "scheduled") -> threading.Thread:
    """Launch retrain() in a daemon thread. Returns immediately."""
    t = threading.Thread(
        target=run_retrain,
        args=(trigger,),
        daemon=True,
        name="retrain-worker",
    )
    t.start()
    return t


def start_scheduler():
    """
    Start APScheduler for weekly retraining (Sunday 02:00 UTC).
    Non-blocking — runs in a background thread pool.
    Returns the scheduler instance, or None if APScheduler is not installed.
    """
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.triggers.cron import CronTrigger
        import pytz

        scheduler = BackgroundScheduler(timezone=pytz.utc)
        scheduler.add_job(
            lambda: run_retrain_background("weekly_scheduled"),
            CronTrigger(day_of_week="sun", hour=2, minute=0),
            id="weekly_retrain",
            replace_existing=True,
            misfire_grace_time=3600,  # allow up to 1hr delay if server was restarting
        )
        scheduler.start()

        job      = scheduler.get_job("weekly_retrain")
        next_run = str(job.next_run_time) if job else "unknown"
        _status["next_run"] = next_run
        log.info(f"[SCHEDULER] ✅ Weekly retraining active — next run: {next_run}")
        return scheduler

    except ImportError:
        log.warning(
            "[SCHEDULER] APScheduler not installed — weekly retraining disabled. "
            "Install: pip install apscheduler pytz"
        )
        return None
    except Exception as e:
        log.error(f"[SCHEDULER] Failed to start: {e}")
        return None


def get_status() -> dict:
    """Return current scheduler + last retrain status."""
    return {**_status}
