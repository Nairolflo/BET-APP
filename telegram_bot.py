"""
telegram_bot.py - Telegram notifications for ValueBet Bot
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
            "text":       text,
            "parse_mode": parse_mode,
        }, timeout=10)
        resp.raise_for_status()
        return True
    except Exception as e:
        print(f"[Telegram] Error: {e}")
        return False


def send_daily_summary(value_bets: list):
    """
    Envoie un seul message groupé avec tous les nouveaux value bets.
    value_bets: list of (bet_dict, match_info_dict) tuples
    """
    if not value_bets:
        send_message(
            "📭 <b>Aucun nouveau value bet trouvé.</b>\n"
            "La chasse continue ! ⚽"
        )
        return

    top_count = int(os.getenv("TOP_BETS_COUNT", 10))
    bets = value_bets[:top_count]

    msg = (
        f"🎯 <b>NOUVEAUX VALUE BETS</b> — {len(bets)} sélection(s)\n"
        f"{'─' * 32}\n\n"
    )

    for bet, match_info in bets:
        value_pct = round(bet["value"] * 100, 1)
        prob_pct  = round(bet["probability"] * 100, 1)
        emoji     = "🟢" if value_pct >= 10 else "🟡"

        msg += (
            f"{emoji} <b>{match_info['home_team']} vs {match_info['away_team']}</b>\n"
            f"   📅 {match_info['date']} — {match_info.get('league', '')}\n"
            f"   📌 {bet['market']} @ <b>{bet['bk_odds']}</b>\n"
            f"   💎 Value : <b>+{value_pct}%</b> | Proba : {prob_pct}%\n"
            f"   🏦 {bet['bookmaker']}\n\n"
        )

    msg += (
        "⚠️ <i>Paris générés automatiquement. "
        "Pariez de façon responsable.</i>"
    )

    send_message(msg)