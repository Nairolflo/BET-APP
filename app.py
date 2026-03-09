"""
app.py - Flask web interface ValueBet Bot
"""
import os
from flask import Flask, render_template, jsonify
from dotenv import load_dotenv
load_dotenv()

from api_clients import get_odds_api_usage
from database import (
    init_db, get_unique_bets, get_stats,
    get_stats_by_market, get_stats_by_league_detailed,
    get_bete_noire_bets, get_roi_over_time, get_streak,
)

app = Flask(__name__)

@app.before_request
def setup():
    init_db()

# ─────────────────────────────────────────────
# PAGES
# ─────────────────────────────────────────────

@app.route("/")
def index():
    stats    = get_stats()
    streak   = get_streak()
    roi_time = get_roi_over_time()
    return render_template("index.html", stats=stats, streak=streak, roi_time=roi_time)

@app.route("/history")
def history():
    bets = get_unique_bets(limit=500)
    # Collecte toutes les ligues disponibles pour le filtre
    leagues = sorted(set(b["league"] for b in bets if b.get("league")))
    return render_template("history.html", bets=bets, leagues=leagues)

@app.route("/stats")
def stats_page():
    stats      = get_stats()
    by_market  = get_stats_by_market()
    by_league  = get_stats_by_league_detailed()
    roi_time   = get_roi_over_time()
    streak     = get_streak()
    # Best market
    resolved    = [m for m in by_market if (m.get("total",0) - m.get("pending",0)) >= 3]
    best_market = max(resolved, key=lambda x: x.get("roi", -999)) if resolved else None
    bn_bets = get_bete_noire_bets(limit=500)
    return render_template(
        "stats.html",
        stats=stats,
        by_market=by_market,
        by_league=by_league,
        roi_time=roi_time,
        streak=streak,
        best_market=best_market,
        bn_bets=bn_bets,
    )

# bete_noire page merged into /stats

@app.route("/live")
def live():
    from datetime import datetime
    today     = datetime.utcnow().date().isoformat()
    bets      = get_unique_bets(limit=500)
    today_bets = [b for b in bets if b.get("match_date") == today]
    return render_template("live.html", bets=today_bets, today=today)

@app.route("/config")
def config_page():
    config = {
        "value_threshold":      float(os.getenv("VALUE_THRESHOLD", 0.02)),
        "min_probability":      float(os.getenv("MIN_PROBABILITY", 0.55)),
        "poisson_weight":       0.40,
        "days_ahead":           int(os.getenv("SCHEDULER_DAYS_AHEAD", 10)),
        "season":               int(os.getenv("SEASON", 2025)),
        "scheduler_hour":       int(os.getenv("SCHEDULER_HOUR", 8)),
        "h2h_seasons":          3,
        "has_odds_key":         bool(os.getenv("ODDS_API_KEY")),
        "has_footballdata_key": bool(os.getenv("FOOTBALLDATA_KEY")),
        "has_telegram_token":   bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "leagues": [
            {"id": 39,  "name": "Premier League",      "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "h2h": True,  "form": True},
            {"id": 61,  "name": "Ligue 1",              "flag": "🇫🇷", "h2h": True,  "form": True},
            {"id": 78,  "name": "Bundesliga",           "flag": "🇩🇪", "h2h": True,  "form": True},
            {"id": 135, "name": "Serie A",              "flag": "🇮🇹", "h2h": True,  "form": True},
            {"id": 140, "name": "La Liga",              "flag": "🇪🇸", "h2h": True,  "form": True},
            {"id": 88,  "name": "Eredivisie",           "flag": "🇳🇱", "h2h": True,  "form": True},
            {"id": 94,  "name": "Primeira Liga",        "flag": "🇵🇹", "h2h": True,  "form": True},
            {"id": 40,  "name": "Championship",         "flag": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "h2h": True,  "form": True},
            {"id": 2,   "name": "Champions League",     "flag": "🏆",  "h2h": True,  "form": True},
            {"id": 3,   "name": "Europa League",        "flag": "🇪🇺", "h2h": False, "form": False},
            {"id": 144, "name": "Belgium First Div",    "flag": "🇧🇪", "h2h": False, "form": False},
            {"id": 203, "name": "Turkey Super League",  "flag": "🇹🇷", "h2h": False, "form": False},
            {"id": 179, "name": "Scottish Premiership", "flag": "🏴󠁧󠁢󠁳󠁣󠁴󠁿", "h2h": False, "form": False},
        ],
    }
    quota = get_odds_api_usage()
    return render_template("config.html", config=config, quota=quota)

# ─────────────────────────────────────────────
# API JSON
# ─────────────────────────────────────────────

@app.route("/api/bets")
def api_bets():
    return jsonify(get_unique_bets(limit=500))

@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())

@app.route("/api/stats/market")
def api_stats_market():
    return jsonify(get_stats_by_market())

@app.route("/api/stats/league")
def api_stats_league():
    return jsonify(get_stats_by_league_detailed())

@app.route("/api/roi-time")
def api_roi_time():
    return jsonify(get_roi_over_time())

@app.route("/api/live")
def api_live():
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    bets  = get_unique_bets(limit=500)
    return jsonify([b for b in bets if b.get("match_date") == today])

@app.route("/api/quota")
def api_quota():
    return jsonify(get_odds_api_usage())





# ─────────────────────────────────────────────
# BIATHLON ROUTES
# ─────────────────────────────────────────────

@app.route("/biathlon")
def biathlon_live():
    """Page principale biathlon — H2H, vainqueur, podium, calendrier."""
    try:
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "biathlon"))
        from biathlon.biathlon_client import get_upcoming_races
        upcoming = get_upcoming_races(days_ahead=10)
    except Exception:
        upcoming = []

    # Bets biathlon depuis la DB
    try:
        from database import get_connection, ph, rows_to_dicts
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT * FROM biathlon_bets
            ORDER BY created_at DESC LIMIT 100
        """)
        bets = rows_to_dicts(cur, cur.fetchall())
        conn.close()
    except Exception:
        bets = []

    h2h_bets     = [b for b in bets if b.get("bet_type") == "H2H"]
    winner_bets  = [b for b in bets if b.get("bet_type") == "WINNER"]
    top3_bets    = [b for b in bets if b.get("bet_type") == "TOP3"]

    # Stats globales biathlon
    total   = len([b for b in bets if b.get("result") != "PENDING"])
    wins    = len([b for b in bets if b.get("result") == "WIN"])
    losses  = len([b for b in bets if b.get("result") == "LOSS"])
    pending = len([b for b in bets if b.get("result") == "PENDING"])
    win_rate = round(wins / total * 100, 1) if total else 0
    roi      = round((wins * 1.8 - losses) / total * 100, 1) if total else 0  # approx

    return render_template("biathlon.html",
        h2h_bets        = h2h_bets,
        winner_bets     = winner_bets,
        top3_predictions= [],   # rempli par le worker
        upcoming_races  = upcoming,
        stats           = {
            "total": total, "wins": wins, "losses": losses,
            "pending": pending, "win_rate": win_rate, "roi": roi
        }
    )

@app.route("/biathlon/h2h")
def biathlon_h2h():
    return redirect("/biathlon")

@app.route("/biathlon/podium")
def biathlon_podium():
    return redirect("/biathlon")

@app.route("/biathlon/stats")
def biathlon_stats():
    return redirect("/biathlon")

@app.route("/api/biathlon/bets")
def api_biathlon_bets():
    try:
        from database import get_connection, rows_to_dicts
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM biathlon_bets ORDER BY created_at DESC LIMIT 200")
        bets = rows_to_dicts(cur, cur.fetchall())
        conn.close()
        return jsonify(bets)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    init_db()
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)