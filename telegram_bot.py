"""
telegram_bot.py - Telegram notifications ValueBet Bot
"""
import os
import requests

TELEGRAM_API = "https://api.telegram.org/bot{token}/{method}"


def send_message(text: str, parse_mode: str = "HTML") -> bool:
    token   = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        print("[Telegram] Missing BOT_TOKEN or CHAT_ID — skipping.")
        return False
    url = TELEGRAM_API.format(token=token, method="sendMessage")
    try:
        resp = requests.post(url, json={
            "chat_id":    chat_id,
            "text":       text[:4096],  # Telegram limit
            "parse_mode": parse_mode,
        }, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[Telegram] Error: {e}")
        return False


def send_daily_summary(value_bets: list, extra: str = ""):
    """
    Envoie un résumé groupé par catégorie.
    Envoie une alerte séparée pour les bête noire.
    value_bets : list of (bet_dict, match_info_dict)
    """
    if not value_bets:
        send_message("📭 <b>Aucun nouveau value bet.</b> La chasse continue ⚽" + extra)
        return

    top_count = int(os.getenv("TOP_BETS_COUNT", 10))
    bets      = value_bets[:top_count]

    home_bets = [(b, m) for b, m in bets if b["market"] == "Home Win"]
    away_bets = [(b, m) for b, m in bets if b["market"] == "Away Win"]
    over_bets = [(b, m) for b, m in bets if b["market"] not in ("Home Win", "Away Win")]
    bn_bets   = [(b, m) for b, m in bets if b.get("bete_noire")]

    def fmt(bet, match_info):
        vp  = round(bet["value"] * 100, 1)
        pp  = round(bet["probability"] * 100, 0)
        bn  = " 🔥" if bet.get("bete_noire") else ""
        return (
            f"  <b>{match_info['home_team']} vs {match_info['away_team']}</b>{bn}\n"
            f"  📅 {match_info['date']} · {match_info.get('league','')}"
            f" · @ <b>{bet['bk_odds']}</b> · +{vp}% · {pp:.0f}% · {bet['bookmaker']}\n"
        )

    msg = f"🎯 <b>NOUVEAUX VALUE BETS — {len(bets)} sélection(s)</b>\n"

    if home_bets:
        msg += f"\n🏠 <b>Domicile ({len(home_bets)})</b>\n"
        for b, m in home_bets: msg += fmt(b, m)
    if away_bets:
        msg += f"\n✈️ <b>Extérieur ({len(away_bets)})</b>\n"
        for b, m in away_bets: msg += fmt(b, m)
    if over_bets:
        msg += f"\n⚽ <b>Over/Under ({len(over_bets)})</b>\n"
        for b, m in over_bets: msg += fmt(b, m)
    if bn_bets:
        msg += f"\n🔥 <b>Bête Noire ({len(bn_bets)})</b>\n"
        for b, m in bn_bets: msg += fmt(b, m)

    msg += "\n⚠️ <i>Pariez de façon responsable.</i>"
    if extra:
        msg += extra
    send_message(msg)

    # Alerte séparée pour les bête noire s'il y en a
    if bn_bets:
        alert = "🔥🔥 <b>ALERTE BÊTE NOIRE</b> 🔥🔥\n\n"
        for bet, match_info in bn_bets:
            rate = round((bet.get("bete_noire_rate") or 0) * 100)
            alert += (
                f"<b>{match_info['home_team']} vs {match_info['away_team']}</b>\n"
                f"📌 {bet['market']} @ <b>{bet['bk_odds']}</b>\n"
                f"🔥 Domination H2H : <b>{rate}%</b> de victoires historiques\n"
                f"💎 Value : <b>+{round(bet['value']*100,1)}%</b> | Proba : {round(bet['probability']*100,0):.0f}%\n\n"
            )
        send_message(alert)