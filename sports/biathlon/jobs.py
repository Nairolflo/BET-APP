"""
sports/biathlon/jobs.py — Jobs biathlon
Modèle basé sur stats IBU réelles : ranking, tirs, temps ski.
Pas d'endpoint AthResults → features calculées depuis résultats de courses récentes.
"""
import os
import math
import random
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BIATHLON_DAYS_AHEAD = int(os.getenv("BIATHLON_DAYS_AHEAD", 5))
ANALYSIS_HOUR       = int(os.getenv("BIATHLON_ANALYSIS_HOUR", 7))
RESULTS_HOUR        = int(os.getenv("BIATHLON_RESULTS_HOUR", 22))
N_RECENT_RACES      = int(os.getenv("BIATHLON_RECENT_RACES", 8))   # courses pour les stats
N_SIMULATIONS       = 200_000

state = {
    "last_run":     None,
    "last_results": None,
    "running":      False,
}


# ─────────────────────────────────────────────
# DB
# ─────────────────────────────────────────────

def init_db():
    from core.database import get_connection, is_postgres
    conn = get_connection()
    try:
        cur = conn.cursor()
        if is_postgres():
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_bets (
                    id           SERIAL PRIMARY KEY,
                    race_id      TEXT,
                    race_name    TEXT,
                    race_date    TEXT,
                    race_format  TEXT,
                    bet_type     TEXT,
                    pick         TEXT,
                    opponent     TEXT,
                    odd          REAL DEFAULT 0,
                    bookmaker    TEXT DEFAULT 'IBU Model',
                    prob_model   REAL,
                    prob_implied REAL DEFAULT 0,
                    value_pct    REAL DEFAULT 0,
                    kelly        REAL DEFAULT 0,
                    result       INTEGER DEFAULT -1,
                    created_at   TIMESTAMP DEFAULT NOW(),
                    resolved_at  TIMESTAMP
                )
            """)
        else:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS biathlon_bets (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    race_id      TEXT,
                    race_name    TEXT,
                    race_date    TEXT,
                    race_format  TEXT,
                    bet_type     TEXT,
                    pick         TEXT,
                    opponent     TEXT,
                    odd          REAL DEFAULT 0,
                    bookmaker    TEXT DEFAULT 'IBU Model',
                    prob_model   REAL,
                    prob_implied REAL DEFAULT 0,
                    value_pct    REAL DEFAULT 0,
                    kelly        REAL DEFAULT 0,
                    result       INTEGER DEFAULT -1,
                    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at  TIMESTAMP
                )
            """)
        conn.commit()
    finally:
        conn.close()


def save_bet(bet: dict) -> int:
    from core.database import get_connection, is_postgres, ph
    conn = get_connection()
    try:
        cur = conn.cursor()
        p   = ph()
        cur.execute(f"""
            SELECT id FROM biathlon_bets
            WHERE race_id = {p} AND bet_type = {p} AND pick = {p}
        """, (bet.get("race_id"), bet.get("bet_type"), bet.get("pick")))
        existing = cur.fetchone()
        if existing:
            return existing[0]
        if is_postgres():
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id, race_name, race_date, race_format, bet_type,
                     pick, opponent, odd, bookmaker, prob_model,
                     prob_implied, value_pct, kelly)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
            """, (
                bet.get("race_id"), bet.get("race_name"), bet.get("race_date"),
                bet.get("race_format"), bet.get("bet_type"), bet.get("pick"),
                bet.get("opponent"), bet.get("odd", 0), bet.get("bookmaker", "IBU Model"),
                bet.get("prob_model"), bet.get("prob_implied", 0),
                bet.get("value_pct", 0), bet.get("kelly", 0),
            ))
            return cur.fetchone()[0]
        else:
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id, race_name, race_date, race_format, bet_type,
                     pick, opponent, odd, bookmaker, prob_model,
                     prob_implied, value_pct, kelly)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                bet.get("race_id"), bet.get("race_name"), bet.get("race_date"),
                bet.get("race_format"), bet.get("bet_type"), bet.get("pick"),
                bet.get("opponent"), bet.get("odd", 0), bet.get("bookmaker", "IBU Model"),
                bet.get("prob_model"), bet.get("prob_implied", 0),
                bet.get("value_pct", 0), bet.get("kelly", 0),
            ))
            return cur.lastrowid
    finally:
        conn.commit()
        conn.close()


def get_pending_bets() -> list:
    from core.database import get_connection, rows_to_dicts
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("SELECT * FROM biathlon_bets WHERE result = -1 ORDER BY race_date ASC")
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()


def update_result(bet_id: int, result: int):
    from core.database import get_connection, ph, is_postgres
    conn = get_connection()
    try:
        cur = conn.cursor()
        p   = ph()
        ts  = "NOW()" if is_postgres() else "CURRENT_TIMESTAMP"
        cur.execute(f"""
            UPDATE biathlon_bets SET result = {p}, resolved_at = {ts} WHERE id = {p}
        """, (result, bet_id))
        conn.commit()
    finally:
        conn.close()


# ─────────────────────────────────────────────
# MODÈLE — Features depuis résultats de courses
# ─────────────────────────────────────────────

def _parse_shooting(s: str) -> dict:
    """Parse '10110 10101' → accuracy globale, couché, debout."""
    if not s:
        return {"acc": None, "prone": None, "standing": None, "misses": None}
    digits = [int(c) for c in s.replace(" ", "").replace("/", "") if c in "01"]
    if not digits:
        return {"acc": None, "prone": None, "standing": None, "misses": None}
    total = len(digits)
    hits  = sum(digits)
    half  = total // 2
    prone    = sum(digits[:half]) / half if half else None
    standing = sum(digits[half:]) / (total - half) if (total - half) > 0 else None
    return {
        "acc":      hits / total,
        "prone":    prone,
        "standing": standing,
        "misses":   total - hits,
    }


def _time_to_sec(t: str) -> float | None:
    """'00:23:45.2' → secondes."""
    if not t:
        return None
    try:
        t = t.lstrip("+").strip()
        parts = t.split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
    except Exception:
        return None


def build_athlete_stats(athletes_ids: list, race_ids: list) -> dict:
    """
    Construit les stats de chaque athlète depuis les résultats des courses récentes.
    Retourne {ibu_id: {name, nat, avg_rank, shoot_acc, prone_acc, standing_acc,
                       avg_misses, n_races, top3_rate, win_rate}}
    """
    from sports.biathlon.biathlon_client import get_results

    # Accumuler résultats par athlète
    data = {}  # ibu_id → liste de résultats

    for race_id in race_ids:
        try:
            results = get_results(race_id)
            n_finishers = len([r for r in results if r.get("Rank")])
            for r in results:
                ibu_id = r.get("IBUId", "")
                if not ibu_id or r.get("IRM"):  # IRM = disqualifié/abandon
                    continue
                rank = r.get("Rank")
                if not rank:
                    continue
                shoot = _parse_shooting(r.get("Shootings", ""))
                run_t = _time_to_sec(r.get("RunTime", ""))
                tot_t = _time_to_sec(r.get("TotalTime", ""))

                if ibu_id not in data:
                    data[ibu_id] = {
                        "name":    r.get("Name", ""),
                        "nat":     r.get("Nat", ""),
                        "results": [],
                    }
                data[ibu_id]["results"].append({
                    "rank":       int(rank),
                    "n_fin":      n_finishers,
                    "shoot_acc":  shoot["acc"],
                    "prone_acc":  shoot["prone"],
                    "standing_acc": shoot["standing"],
                    "misses":     shoot["misses"],
                    "run_time":   run_t,
                    "tot_time":   tot_t,
                })
        except Exception as e:
            log.warning(f"[Biathlon] build_stats {race_id}: {e}")

    # Calculer features agrégées
    stats = {}
    for ibu_id, d in data.items():
        res = d["results"]
        if not res:
            continue
        n = len(res)

        ranks      = [r["rank"] for r in res]
        n_fins     = [r["n_fin"] for r in res]
        # Rank relatif (0=premier, 1=dernier)
        rel_ranks  = [rk / max(nf, 1) for rk, nf in zip(ranks, n_fins)]

        accs     = [r["shoot_acc"] for r in res if r["shoot_acc"] is not None]
        prones   = [r["prone_acc"] for r in res if r["prone_acc"] is not None]
        standings= [r["standing_acc"] for r in res if r["standing_acc"] is not None]
        misses   = [r["misses"] for r in res if r["misses"] is not None]
        runs     = [r["run_time"] for r in res if r["run_time"] is not None]

        stats[ibu_id] = {
            "name":         d["name"],
            "nat":          d["nat"],
            "n_races":      n,
            "avg_rank":     sum(ranks) / n,
            "avg_rel_rank": sum(rel_ranks) / n,
            "top3_rate":    sum(1 for rk in ranks if rk <= 3) / n,
            "win_rate":     sum(1 for rk in ranks if rk == 1) / n,
            "top10_rate":   sum(1 for rk in ranks if rk <= 10) / n,
            "shoot_acc":    sum(accs) / len(accs) if accs else 0.82,
            "prone_acc":    sum(prones) / len(prones) if prones else 0.82,
            "standing_acc": sum(standings) / len(standings) if standings else 0.78,
            "avg_misses":   sum(misses) / len(misses) if misses else 2.0,
            "avg_run_time": sum(runs) / len(runs) if runs else None,
        }

    return stats


def calc_rating(s: dict, fmt_code: str) -> float:
    """
    Score composite 0-1 depuis les stats d'un athlète.
    Pondérations par format (sprint = ski 45%, tirs 40%, forme 15%).
    """
    weights = {
        "SP": {"ski": 0.45, "shoot": 0.40, "form": 0.15},
        "PU": {"ski": 0.50, "shoot": 0.35, "form": 0.15},
        "IN": {"ski": 0.38, "shoot": 0.47, "form": 0.15},
        "MS": {"ski": 0.55, "shoot": 0.30, "form": 0.15},
        "RL": {"ski": 0.50, "shoot": 0.38, "form": 0.12},
    }
    w = weights.get(fmt_code, weights["SP"])

    # Score ski : rank relatif inversé (0.0 = dernier, 1.0 = premier)
    ski_score  = max(0, 1.0 - s.get("avg_rel_rank", 0.5))
    # Score tir : accuracy pondérée (couché + debout)
    shoot_score = (s.get("prone_acc", 0.82) * 0.5 + s.get("standing_acc", 0.78) * 0.5)
    # Score forme : top3 rate
    form_score  = s.get("top3_rate", 0.1)

    return (w["ski"] * ski_score + w["shoot"] * shoot_score + w["form"] * form_score)


def simulate_podium(athletes: list, fmt_code: str, n_sim: int = N_SIMULATIONS) -> list:
    """
    Monte Carlo : simule N fois la course, retourne liste triée par P(Top3).
    athletes : [{ibu_id, name, nat, rating}]
    """
    sigma = {"SP": 0.12, "PU": 0.10, "IN": 0.14, "MS": 0.11, "RL": 0.09}.get(fmt_code, 0.12)
    counts = {a["ibu_id"]: {"win": 0, "top3": 0} for a in athletes}
    ratings = [a["rating"] for a in athletes]
    ids     = [a["ibu_id"] for a in athletes]

    batch = 1000
    for _ in range(n_sim // batch):
        for _ in range(batch):
            scores = [r + random.gauss(0, sigma) for r in ratings]
            ranked = sorted(zip(scores, ids), reverse=True)
            counts[ranked[0][1]]["win"] += 1
            for i in range(min(3, len(ranked))):
                counts[ranked[i][1]]["top3"] += 1

    total = (n_sim // batch) * batch
    result = []
    for a in athletes:
        aid = a["ibu_id"]
        result.append({
            "ibu_id":  aid,
            "name":    a["name"],
            "nat":     a.get("nat", ""),
            "rating":  round(a["rating"], 4),
            "p_win":   round(counts[aid]["win"]  / total, 4),
            "p_top3":  round(counts[aid]["top3"] / total, 4),
        })
    return sorted(result, key=lambda x: -x["p_top3"])


def h2h_prob(rating_a: float, rating_b: float) -> float:
    """P(A bat B) via logistique calibrée."""
    delta = rating_a - rating_b
    return 1 / (1 + math.exp(-15 * delta))


# ─────────────────────────────────────────────
# RÉCUPÉRATION ATHLÈTES
# ─────────────────────────────────────────────

def get_recent_race_ids_for(gender: str, fmt_code: str, n: int = N_RECENT_RACES) -> list:
    from sports.biathlon.biathlon_client import (
        get_recent_race_ids, CURRENT_SEASON, PREV_SEASON
    )
    ids = get_recent_race_ids(gender=gender, fmt_code=fmt_code,
                               season=CURRENT_SEASON, n=n)
    if len(ids) < 3:
        ids += get_recent_race_ids(gender=gender, fmt_code=fmt_code,
                                    season=PREV_SEASON, n=n - len(ids))
    return ids[:n]


# ─────────────────────────────────────────────
# RUN PRINCIPAL
# ─────────────────────────────────────────────

def run(silent=False):
    from core.telegram import send_message

    if state["running"]:
        if not silent:
            send_message("⏳ Analyse biathlon déjà en cours...")
        return

    state["running"] = True
    log.info("[Biathlon] Analyse démarrée")

    try:
        from sports.biathlon.biathlon_client import (
            get_upcoming_races, RACE_FORMATS, preload_competitions,
            CURRENT_SEASON, PREV_SEASON
        )

        # ── Précharger TOUTES les compétitions une seule fois ──
        log.info("[Biathlon] Préchargement compétitions...")
        preload_competitions(CURRENT_SEASON)
        preload_competitions(PREV_SEASON)
        log.info("[Biathlon] Préchargement terminé")

        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)
        if not races:
            if not silent:
                send_message("🎿 <b>Biathlon</b> : Aucune course dans les prochains jours.")
            state["running"] = False
            return

        msg = "🎿 <b>Prédictions Biathlon</b>\n\n"

        for race in races[:4]:
            race_id     = race.get("race_id", "")
            description = race.get("description", "Course")
            race_date   = race.get("date", "")
            fmt_code    = race.get("format", "SP")
            fmt_name    = race.get("format_name") or RACE_FORMATS.get(fmt_code, fmt_code)
            location    = race.get("location", "")
            gender      = race.get("gender", "M")
            gender_icon = "♀️" if gender == "W" else "♂️"

            msg += f"{gender_icon} <b>{description}</b>\n"
            msg += f"📅 {race_date}"
            if location:
                msg += f" · {location}"
            if fmt_name:
                msg += f" · {fmt_name}"
            msg += "\n"

            # Récupérer stats depuis courses récentes
            race_ids = get_recent_race_ids_for(gender, fmt_code)
            if not race_ids:
                msg += "<i>Pas de courses récentes disponibles</i>\n\n"
                continue

            athlete_stats = build_athlete_stats([], race_ids)
            if len(athlete_stats) < 4:
                msg += "<i>Données insuffisantes</i>\n\n"
                continue

            # Trier par avg_rank et prendre top 15
            top = sorted(athlete_stats.values(), key=lambda x: x["avg_rank"])[:15]

            # Calculer rating pour chaque athlète
            athletes_rated = []
            for s in top:
                # Trouver ibu_id
                ibu_id = next((k for k, v in athlete_stats.items()
                               if v["name"] == s["name"]), "")
                if not ibu_id:
                    continue
                rating = calc_rating(s, fmt_code)
                athletes_rated.append({
                    "ibu_id": ibu_id,
                    "name":   s["name"],
                    "nat":    s["nat"],
                    "rating": rating,
                    "stats":  s,
                })

            if len(athletes_rated) < 3:
                msg += "<i>Pas assez d'athlètes</i>\n\n"
                continue

            # ── Podium Monte Carlo ──
            podium = simulate_podium(athletes_rated, fmt_code, n_sim=100_000)

            msg += "\n🏆 <b>Podium prédit</b>\n"
            medals = ["🥇", "🥈", "🥉"]
            for i, a in enumerate(podium[:8]):
                s     = athlete_stats.get(a["ibu_id"], {})
                shoot = s.get("shoot_acc", 0)
                misses = s.get("avg_misses", "?")
                medal = medals[i] if i < 3 else f"  {i+1}."
                msg += (
                    f"{medal} <b>{a['name']}</b> {a['nat']} "
                    f"— Top3: <b>{round(a['p_top3']*100)}%</b> "
                    f"· Vict: {round(a['p_win']*100)}% "
                    f"· 🎯{round(shoot*100)}% ({misses:.1f} ratés)\n"
                )

                if i < 3:
                    save_bet({
                        "race_id":     race_id,
                        "race_name":   description,
                        "race_date":   race_date,
                        "race_format": fmt_code,
                        "bet_type":    "TOP3",
                        "pick":        a["name"],
                        "opponent":    "",
                        "prob_model":  a["p_top3"],
                    })

            # ── Marchés bookmakers réels ──
            # Vainqueur : paris disponibles sur Winamax/Betclic
            msg += "\n🎰 <b>Paris disponibles</b>\n"
            msg += "<i>Marchés biathlon : Vainqueur · Top 3</i>\n\n"

            msg += "🏅 <b>Vainqueur prédit</b>\n"
            for a in podium[:5]:
                s = athlete_stats.get(a["ibu_id"], {})
                # Cote juste (fair odd) = 1 / p_win
                fair_odd = round(1 / a["p_win"], 2) if a["p_win"] > 0.01 else 99.0
                shoot = s.get("shoot_acc", 0)
                misses = s.get("avg_misses", "?")
                msg += (
                    f"  {'⭐' if a['p_win'] > 0.20 else '•'} "
                    f"<b>{a['name']}</b> {a['nat']} "
                    f"→ {round(a['p_win']*100)}% "
                    f"(cote juste ~{fair_odd}) "
                    f"🎯{round(shoot*100)}%/{misses:.1f}r\n"
                )
                save_bet({
                    "race_id":     race_id,
                    "race_name":   description,
                    "race_date":   race_date,
                    "race_format": fmt_code,
                    "bet_type":    "WIN",
                    "pick":        a["name"],
                    "opponent":    "",
                    "prob_model":  a["p_win"],
                })

            msg += "\n📊 <b>Top 3 (chaque athlète)</b>\n"
            for a in podium[:6]:
                s = athlete_stats.get(a["ibu_id"], {})
                fair_top3 = round(1 / a["p_top3"], 2) if a["p_top3"] > 0.01 else 99.0
                n = s.get("n_races", 0)
                msg += (
                    f"  • <b>{a['name']}</b> {a['nat']} "
                    f"→ {round(a['p_top3']*100)}% "
                    f"(cote juste ~{fair_top3}) "
                    f"sur {n} courses\n"
                )

            msg += (
                "\n💡 <i>Compare ces cotes justes avec Winamax/Betclic "
                "→ si bookmaker cote plus haut = value bet ✅</i>\n"
            )

            msg += "\n💡 <i>Stats sur les {} derniers sprints</i>\n\n".format(len(race_ids))

        state["last_run"] = datetime.now(timezone.utc)
        state["running"]  = False

        if not silent:
            send_message(msg)

    except Exception as e:
        state["running"] = False
        log.error(f"[Biathlon] run error: {e}", exc_info=True)
        if not silent:
            send_message(f"❌ <b>Erreur analyse biathlon</b> : {e}")


# ─────────────────────────────────────────────
# CHECK RESULTS
# ─────────────────────────────────────────────

def check_results(silent=False):
    from core.telegram import send_message

    pending = get_pending_bets()
    if not pending:
        if not silent:
            send_message("🎿 Aucun bet biathlon en attente.")
        return

    from sports.biathlon.biathlon_client import get_results

    won, lost = [], []
    for bet in pending:
        try:
            results = get_results(bet["race_id"])
            if not results:
                continue

            if bet["bet_type"] == "H2H":
                pick_rank = next((int(r["Rank"]) for r in results
                                  if bet["pick"].lower() in r.get("Name","").lower()
                                  and r.get("Rank")), None)
                opp_rank  = next((int(r["Rank"]) for r in results
                                  if bet.get("opponent","").lower() in r.get("Name","").lower()
                                  and r.get("Rank")), None)
                if pick_rank is None or opp_rank is None:
                    continue
                success = 1 if pick_rank < opp_rank else 0

            elif bet["bet_type"] == "TOP3":
                pick_rank = next((int(r["Rank"]) for r in results
                                  if bet["pick"].lower() in r.get("Name","").lower()
                                  and r.get("Rank")), None)
                if pick_rank is None:
                    continue
                success = 1 if pick_rank <= 3 else 0
            else:
                continue

            update_result(bet["id"], success)
            (won if success == 1 else lost).append(bet)
        except Exception as e:
            log.warning(f"[Biathlon] check_result bet {bet['id']}: {e}")

    state["last_results"] = datetime.now(timezone.utc)

    if not won and not lost:
        if not silent:
            send_message("⏳ Résultats biathlon pas encore disponibles.")
        return

    msg = "🎿 <b>Résultats biathlon</b>\n\n"
    if won:
        msg += f"✅ <b>Gagnés ({len(won)})</b>\n"
        for b in won:
            t = "H2H" if b["bet_type"] == "H2H" else "Top3"
            msg += f"  • {b['pick']} [{t}] · {b['race_name']}\n"
    if lost:
        msg += f"\n❌ <b>Perdus ({len(lost)})</b>\n"
        for b in lost:
            t = "H2H" if b["bet_type"] == "H2H" else "Top3"
            msg += f"  • {b['pick']} [{t}] · {b['race_name']}\n"

    if not silent:
        send_message(msg)