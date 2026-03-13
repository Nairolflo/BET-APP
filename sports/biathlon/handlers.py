"""
sports/biathlon/handlers.py
Flow H2H interactif — cache global par race_id pour éviter rechargements IBU.
"""
import math
import threading
import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# Cache stats global par race_id (chargé une fois, réutilisé pour toutes les pages)
_stats_cache: dict = {}
# Session par chat_id (athlète A sélectionné)
_session: dict = {}


def _fmt_name(fmt):
    return {"SP":"Sprint","PU":"Poursuite","IN":"Individuelle",
            "MS":"Mass Start","RL":"Relais","SR":"Relais Mixte"}.get(fmt, fmt)

def _gender_icon(g):
    return "♀️" if g == "W" else "♂️"

def _build_stats(gender, fmt, n=10):
    from sports.biathlon.jobs import build_stats_for
    return build_stats_for(gender, fmt, n)

def _get_race_stats(race_id: str) -> dict:
    """Stats mises en cache par race_id — appel IBU une seule fois."""
    if race_id in _stats_cache:
        return _stats_cache[race_id]
    from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON
    preload_competitions(CURRENT_SEASON)
    races  = get_upcoming_races(days_ahead=7)
    race   = next((r for r in races if r.get("race_id") == race_id), {})
    gender = race.get("gender","M")
    fmt    = race.get("format","SP")
    desc   = race.get("description","")
    stats  = _build_stats(gender, fmt, n=10)
    _stats_cache[race_id] = {"stats": stats, "fmt": fmt, "gender": gender, "desc": desc}
    return _stats_cache[race_id]

def _calc(sa, sb, fmt):
    from sports.biathlon.jobs import calc_rating, h2h_prob
    ra = calc_rating(sa, fmt); rb = calc_rating(sb, fmt)
    pa = h2h_prob(ra, rb)
    return pa, 1-pa


# ─── Handlers principaux ───────────────────────────────────────────────────

def handle_status():
    from core.telegram import send_message
    from sports.biathlon.jobs import state, BIATHLON_DAYS_AHEAD, ANALYSIS_HOUR
    try:
        from sports.biathlon.biathlon_client import get_upcoming_races
        races = get_upcoming_races(days_ahead=BIATHLON_DAYS_AHEAD)
    except Exception as e:
        send_message(f"❌ IBU API : {e}"); return
    last = state["last_run"]
    msg = (f"🎿 <b>Biathlon — Statut</b>\n\n"
           f"Dernière analyse : {last.strftime('%Y-%m-%d %H:%M UTC') if last else 'Aucune'}\n"
           f"Analyse auto : {ANALYSIS_HOUR:02d}h30 UTC\n\n")
    if not races:
        msg += "Aucune course prévue."
    else:
        for r in races[:8]:
            msg += f"{_gender_icon(r.get('gender','M'))} {r.get('description','')} · {r.get('date','')} · {_fmt_name(r.get('format',''))}\n"
    send_message(msg)

def handle_run():
    from core.telegram import send_message
    from sports.biathlon.jobs import run
    send_message("⏳ Analyse biathlon en cours...")
    threading.Thread(target=run, daemon=True).start()

def handle_results():
    from sports.biathlon.jobs import check_results
    threading.Thread(target=check_results, daemon=True).start()

def handle_stats():
    from core.telegram import send_message
    from sports.biathlon.jobs import get_pending_bets
    send_message(f"🎿 {len(get_pending_bets())} paris biathlon en attente.")


# ─── Flow H2H ──────────────────────────────────────────────────────────────

def handle_h2h_menu():
    """Étape 1 : courses à venir."""
    from core.telegram import send_message, make_keyboard
    from sports.biathlon.biathlon_client import get_upcoming_races, preload_competitions, CURRENT_SEASON
    try:
        preload_competitions(CURRENT_SEASON)
        races = [r for r in get_upcoming_races(days_ahead=7)
                 if r.get("format") not in ("RL","SR","MX")]
    except Exception as e:
        send_message(f"❌ IBU API : {e}"); return
    if not races:
        send_message("🎿 Aucune course individuelle à venir."); return

    rows = []
    for r in races[:8]:
        label = f"{_gender_icon(r.get('gender','M'))} {r.get('description','')} · {r.get('date','')}"
        rows.append([{"text": label, "callback_data": f"biat_race|{r.get('race_id','')}"}])
    rows.append([{"text": "◀️ Menu", "callback_data": "menu_biathlon"}])
    send_message("🎿 <b>Choisir une course :</b>", reply_markup=make_keyboard(rows))


def handle_race_menu(race_id: str):
    """Étape 2 : H2H ou Podium."""
    from core.telegram import send_message, make_keyboard
    try:
        cached = _get_race_stats(race_id)
        desc = cached["desc"]
    except Exception:
        desc = race_id
    kb = make_keyboard([
        [{"text": "⚔️ H2H — Choisir deux athlètes", "callback_data": f"biat_h2h|{race_id}"}],
        [{"text": "🏆 Podium — Top favoris",         "callback_data": f"biat_pod|{race_id}"}],
        [{"text": "◀️ Retour",                        "callback_data": "biat_h2h_menu"}],
    ])
    send_message(f"🎿 <b>{desc}</b>\n\nQue veux-tu analyser ?", reply_markup=kb)


def _send_athlete_list(race_id: str, page: int, chat_id: str, title: str,
                        cb_prefix: str, cb_select: str, extra_row=None):
    """Affiche une liste paginée d'athlètes. Réutilisé pour A et B."""
    from core.telegram import send_message, make_keyboard
    cached = _get_race_stats(race_id)
    stats  = cached["stats"]
    desc   = cached["desc"]

    if not stats:
        send_message("❌ Pas de stats disponibles."); return

    top = sorted(stats.items(), key=lambda x: x[1]["avg_rank"])
    PER_PAGE    = 10
    total_pages = math.ceil(len(top) / PER_PAGE)
    slice_      = top[page*PER_PAGE:(page+1)*PER_PAGE]

    rows = []
    for ibu, s in slice_:
        rows.append([{"text": f"{s['name']} {s['nat']} · #{round(s['avg_rank'],1)}",
                      "callback_data": f"{cb_select}|{race_id}|{ibu}"}])

    nav = []
    if page > 0:
        nav.append({"text": "◀️ Préc.", "callback_data": f"{cb_prefix}|{race_id}|{page-1}"})
    nav.append({"text": f"{page+1}/{total_pages} · {len(top)} athlètes", "callback_data": "noop"})
    if page < total_pages - 1:
        nav.append({"text": "Suiv. ▶️", "callback_data": f"{cb_prefix}|{race_id}|{page+1}"})
    rows.append(nav)
    if extra_row:
        rows.append(extra_row)
    rows.append([{"text": "◀️ Retour", "callback_data": f"biat_race|{race_id}"}])

    send_message(f"🎿 <b>{desc}</b>\n{title}",
                 reply_markup=make_keyboard(rows))

    if chat_id:
        _session[chat_id] = {**cached, "race_id": race_id}


def handle_h2h_athletes(race_id: str, page: int = 0, chat_id: str = None):
    """Étape 3 : choisir athlète A."""
    try:
        _send_athlete_list(race_id, page, chat_id,
                           title="👤 Choisir l'athlète A",
                           cb_prefix="biat_h2hp",
                           cb_select="biat_sel")
    except Exception as e:
        from core.telegram import send_message
        send_message(f"❌ {e}")


def handle_select_a(race_id: str, ibu_a: str, chat_id: str):
    """Étape 4 : A choisi, afficher liste B page 0."""
    _send_athlete_b(race_id, ibu_a, 0, chat_id)


def _send_athlete_b(race_id: str, ibu_a: str, page: int, chat_id: str):
    """Affiche la liste des adversaires B paginée."""
    from core.telegram import send_message, make_keyboard
    cached = _get_race_stats(race_id)
    stats  = cached["stats"]
    desc   = cached["desc"]

    if not stats:
        send_message("❌ Pas de stats."); return

    sa     = stats.get(ibu_a, {})
    name_a = sa.get("name", ibu_a)
    top    = [(ibu, s) for ibu, s in sorted(stats.items(), key=lambda x: x[1]["avg_rank"])
              if ibu != ibu_a]

    PER_PAGE    = 10
    total_pages = math.ceil(len(top) / PER_PAGE)
    slice_      = top[page*PER_PAGE:(page+1)*PER_PAGE]

    rows = []
    for ibu_b, sb in slice_:
        rows.append([{"text": f"{sb['name']} {sb['nat']} · #{round(sb['avg_rank'],1)}",
                      "callback_data": f"biat_vs|{race_id}|{ibu_a}|{ibu_b}"}])

    nav = []
    if page > 0:
        nav.append({"text": "◀️ Préc.", "callback_data": f"biat_selb|{race_id}|{ibu_a}|{page-1}"})
    nav.append({"text": f"{page+1}/{total_pages} · {len(top)} athlètes", "callback_data": "noop"})
    if page < total_pages - 1:
        nav.append({"text": "Suiv. ▶️", "callback_data": f"biat_selb|{race_id}|{ibu_a}|{page+1}"})
    rows.append(nav)
    rows.append([{"text": "◀️ Rechoisir A", "callback_data": f"biat_h2h|{race_id}"}])

    send_message(f"🎿 <b>{desc}</b>\n⚔️ <b>{name_a}</b> vs ... · page {page+1}/{total_pages}",
                 reply_markup=make_keyboard(rows))

    if chat_id:
        _session[chat_id] = {**cached, "race_id": race_id, "ibu_a": ibu_a}


def handle_select_b_page(race_id: str, ibu_a: str, page: int, chat_id: str):
    _send_athlete_b(race_id, ibu_a, page, chat_id)


def handle_duel(race_id: str, ibu_a: str, ibu_b: str, chat_id: str):
    """Étape 5 : fiche duel."""
    from core.telegram import send_message, make_keyboard
    cached = _get_race_stats(race_id)
    stats  = cached["stats"]
    fmt    = cached["fmt"]
    desc   = cached["desc"]

    sa = stats.get(ibu_a)
    sb = stats.get(ibu_b)
    if not sa or not sb:
        send_message("❌ Athlètes introuvables."); return

    pa, pb = _calc(sa, sb, fmt)
    fa, fb = round(1/pa, 2), round(1/pb, 2)
    winner = sa if pa > pb else sb

    msg = (
        f"⚔️ <b>{sa['name']} vs {sb['name']}</b>\n"
        f"🎿 {desc} · {_fmt_name(fmt)}\n\n"
        f"📊 <b>Probabilités modèle IBU</b>\n"
        f"  {sa['name']} : <b>{round(pa*100)}%</b> → c.j. {fa}\n"
        f"  {sb['name']} : <b>{round(pb*100)}%</b> → c.j. {fb}\n\n"
        f"🏆 Favori : <b>{winner['name']}</b> ({round(max(pa,pb)*100)}%)\n\n"
        f"🎯 <b>Stats tir</b>\n"
        f"  {sa['name']} : Couché {round(sa['prone_acc']*100)}% · Debout {round(sa['standing_acc']*100)}%\n"
        f"  {sb['name']} : Couché {round(sb['prone_acc']*100)}% · Debout {round(sb['standing_acc']*100)}%\n\n"
        f"⛷️ <b>Forme</b>\n"
        f"  {sa['name']} : Rang moy. #{round(sa['avg_rank'],1)} · Top3 {round(sa['top3_rate']*100)}% · {sa['n_races']} courses\n"
        f"  {sb['name']} : Rang moy. #{round(sb['avg_rank'],1)} · Top3 {round(sb['top3_rate']*100)}% · {sb['n_races']} courses\n\n"
        f"💡 <i>Si Winamax cote {sa['name']} > {fa} → value bet</i>"
    )
    kb = make_keyboard([
        [{"text": "🔄 Changer adversaire", "callback_data": f"biat_sel|{race_id}|{ibu_a}"}],
        [{"text": "◀️ Retour courses",     "callback_data": "biat_h2h_menu"}],
    ])
    send_message(msg, reply_markup=kb)


def handle_podium(race_id: str):
    """Top favoris de la course."""
    from core.telegram import send_message, make_keyboard
    from sports.biathlon.jobs import calc_rating
    cached = _get_race_stats(race_id)
    stats  = cached["stats"]
    fmt    = cached["fmt"]
    desc   = cached["desc"]

    if not stats:
        send_message("❌ Pas de stats."); return

    top   = sorted(stats.items(), key=lambda x: -calc_rating(x[1], fmt))[:8]
    total = sum(calc_rating(s, fmt) for _, s in top)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣"]

    msg = f"🏆 <b>Podium favori — {desc}</b>\n🎿 {_fmt_name(fmt)}\n\n"
    for i, (ibu, s) in enumerate(top):
        pct = round(calc_rating(s, fmt) / total * 100)
        msg += (f"{medals[i]} <b>{s['name']}</b> {s['nat']} — {pct}%\n"
                f"   #{round(s['avg_rank'],1)} · C:{round(s['prone_acc']*100)}%"
                f" D:{round(s['standing_acc']*100)}% · Top3:{round(s['top3_rate']*100)}%\n")

    kb = make_keyboard([
        [{"text": "⚔️ Voir H2H", "callback_data": f"biat_h2h|{race_id}"}],
        [{"text": "◀️ Retour",   "callback_data": f"biat_race|{race_id}"}],
    ])
    send_message(msg, reply_markup=kb)