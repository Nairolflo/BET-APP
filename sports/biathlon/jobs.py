"""
sports/biathlon/jobs.py — Jobs biathlon (analyse, résultats)
Mode prédiction pure : modèle IBU uniquement, sans cotes externes.
The Odds API ne couvre pas le biathlon → pas de value bet, juste les probas.
"""
import os
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

BIATHLON_DAYS_AHEAD = int(os.getenv("BIATHLON_DAYS_AHEAD", 3))
ANALYSIS_HOUR       = int(os.getenv("BIATHLON_ANALYSIS_HOUR", 7))
RESULTS_HOUR        = int(os.getenv("BIATHLON_RESULTS_HOUR", 22))

state = {
    "last_run":     None,
    "last_results": None,
    "running":      False,
}


def init_db():
    """Crée les tables biathlon si elles n'existent pas."""
    from database import get_connection, is_postgres
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
        log.info("[Biathlon] Tables DB initialisées")
    finally:
        conn.close()


def save_prediction(pred: dict) -> int:
    """Sauvegarde une prédiction H2H en DB (sans cotes)."""
    from database import get_connection, is_postgres, ph
    conn = get_connection()
    try:
        cur = conn.cursor()
        p   = ph()
        # Anti-doublon
        cur.execute(f"""
            SELECT id FROM biathlon_bets
            WHERE race_id = {p} AND bet_type = {p} AND pick = {p}
        """, (pred.get("race_id"), pred.get("bet_type"), pred.get("pick")))
        existing = cur.fetchone()
        if existing:
            return existing[0]

        if is_postgres():
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id, race_name, race_date, race_format, bet_type,
                     pick, opponent, odd, bookmaker, prob_model, prob_implied,
                     value_pct, kelly)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, (
                pred.get("race_id"), pred.get("race_name"), pred.get("race_date"),
                pred.get("race_format"), pred.get("bet_type"), pred.get("pick"),
                pred.get("opponent"), 0, "IBU Model",
                pred.get("prob_model", 0), 0, 0, 0,
            ))
            return cur.fetchone()[0]
        else:
            cur.execute("""
                INSERT INTO biathlon_bets
                    (race_id, race_name, race_date, race_format, bet_type,
                     pick, opponent, odd, bookmaker, prob_model, prob_implied,
                     value_pct, kelly)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                pred.get("race_id"), pred.get("race_name"), pred.get("race_date"),
                pred.get("race_format"), pred.get("bet_type"), pred.get("pick"),
                pred.get("opponent"), 0, "IBU Model",
                pred.get("prob_model", 0), 0, 0, 0,
            ))
            return cur.lastrowid
    finally:
        conn.commit()
        conn.close()


def get_pending_bets() -> list:
    from database import get_connection, rows_to_dicts
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM biathlon_bets WHERE result = -1
            ORDER BY race_date ASC
        """)
        return rows_to_dicts(cur, cur.fetchall())
    finally:
        conn.close()


def update_result(bet_id: int, result: int):
    from database import get_connection, ph, is_postgres
    conn = get_connection()
    try:
        cur = conn.cursor()
        p   = ph()
        if is_postgres():
            cur.execute(f"""
                UPDATE biathlon_bets
                SET result = {p}, resolved_at = NOW()
                WHERE id = {p}
            """, (result, bet_id))
        else:
            cur.execute(f"""
                UPDATE biathlon_bets
                SET result = {p}, resolved_at = CURRENT_TIMESTAMP
                WHERE id = {p}
            """, (result, bet_id))
        conn.commit()
    finally:
        conn.close()


def run(silent=False):
    """
    Analyse principale biathlon — mode prédiction pure.
    Calcule les probas H2H via le modèle IBU et les envoie sur Telegram.
    Pas de comparaison avec des cotes externes (The Odds API ne couvre pas le biathlon).
    """
    from core.telegram import send_message

    if state["running"]:
        send_message("⏳ Analyse biathlon déjà en cours...")
        return

    state["running"] = True
    log.info("[Biathlon] Analyse démarrée (mode prédiction pure)")

    try:
        from biathlon.biathlon_client import get_upcoming_races
        from biathlon.biathlon_model  import predict_h2h

        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)
        if not races:
            if not silent:
                send_message("🎿 <b>Biathlon</b> : Aucune course dans les prochains jours.")
            state["running"] = False
            return

        if not silent:
            send_message(
                f"🎿 <b>Analyse biathlon démarrée</b>\n"
                f"{len(races)} course(s) trouvée(s) — calcul des probabilités..."
            )

        saved    = 0
        msg_body = ""

        for race in races:
            race_id   = race.get("RaceId", "")
            race_name = race.get("ShortDescription", race.get("Description", "Course"))
            race_date = race.get("StartTime", "")[:10]
            race_fmt  = race.get("RaceTypeId", "")

            try:
                # predict_h2h retourne une liste de duels (athleteA, athleteB, prob_a, prob_b)
                duels = predict_h2h(race)
                if not duels:
                    continue

                # On garde les duels où le modèle est confiant (prob > 60%)
                top_duels = [d for d in duels if d.get("prob_a", 0) >= 0.60]
                if not top_duels:
                    top_duels = sorted(duels, key=lambda x: abs(x.get("prob_a", 0.5) - 0.5), reverse=True)[:3]

                msg_body += f"\n📅 <b>{race_name}</b> ({race_date})\n"

                for duel in top_duels[:5]:
                    athlete_a = duel.get("athlete_a", "?")
                    athlete_b = duel.get("athlete_b", "?")
                    prob_a    = duel.get("prob_a", 0)
                    prob_b    = 1 - prob_a

                    # Qui est favori ?
                    if prob_a >= 0.5:
                        fav, und = athlete_a, athlete_b
                        prob_fav = prob_a
                    else:
                        fav, und = athlete_b, athlete_a
                        prob_fav = prob_b

                    msg_body += (
                        f"  • <b>{fav}</b> vs {und} "
                        f"— {prob_fav*100:.0f}% favori\n"
                    )

                    # Sauvegarde en DB
                    try:
                        save_prediction({
                            "race_id":     race_id,
                            "race_name":   race_name,
                            "race_date":   race_date,
                            "race_format": race_fmt,
                            "bet_type":    "H2H",
                            "pick":        fav,
                            "opponent":    und,
                            "prob_model":  prob_fav,
                        })
                        saved += 1
                    except Exception as e:
                        log.error(f"[Biathlon] save_prediction: {e}")

            except Exception as e:
                log.warning(f"[Biathlon] predict_h2h {race_id}: {e}")
                continue

        state["last_run"] = datetime.now(timezone.utc)
        state["running"]  = False

        if not silent:
            if saved > 0:
                send_message(
                    f"🎯 <b>Biathlon — {saved} prédiction(s)</b>\n"
                    f"<i>Mode prédiction pure (pas de cotes disponibles)</i>\n"
                    + msg_body +
                    "\n⚠️ <i>Ces probabilités sont indicatives. Trouvez vos propres cotes chez Unibet/Betclic.</i>"
                )
            else:
                send_message("🎿 <b>Biathlon</b> : Aucune prédiction générée.")

    except Exception as e:
        state["running"] = False
        log.error(f"[Biathlon] run error: {e}")
        if not silent:
            send_message(f"❌ <b>Erreur analyse biathlon</b> : {e}")


def check_results(silent=False):
    """Vérifie les résultats des prédictions biathlon en attente."""
    from core.telegram import send_message

    pending = get_pending_bets()
    if not pending:
        if not silent:
            send_message("🎿 Aucun bet biathlon en attente.")
        return

    try:
        from biathlon.biathlon_client import get_results
    except ImportError as e:
        log.error(f"[Biathlon] Import error: {e}")
        return

    won, lost = [], []

    for bet in pending:
        try:
            results = get_results(bet["race_id"])
            if not results:
                continue
            pick_pos = next(
                (r.get("Rank") for r in results
                 if bet["pick"].lower() in r.get("Name", "").lower()),
                None
            )
            opp_pos = next(
                (r.get("Rank") for r in results
                 if bet.get("opponent", "").lower() in r.get("Name", "").lower()),
                None
            )
            if pick_pos is None or opp_pos is None:
                continue

            success = 1 if pick_pos < opp_pos else 0
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
        msg += f"✅ <b>Corrects ({len(won)})</b>\n"
        for b in won:
            msg += f"  • {b['pick']} vs {b.get('opponent','')} · {b['race_name']}\n"
    if lost:
        msg += f"\n❌ <b>Incorrects ({len(lost)})</b>\n"
        for b in lost:
            msg += f"  • {b['pick']} vs {b.get('opponent','')} · {b['race_name']}\n"

    if not silent:
        send_message(msg)