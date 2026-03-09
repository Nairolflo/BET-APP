# database.py — shim de compatibilité → core/database.py
from core.database import *  # noqa
from core.database import (
    get_connection, is_postgres, ph, row_to_dict, rows_to_dicts,
    init_db, save_bet, get_all_bets, update_bet_result,
    is_bet_notified, mark_bet_notified, get_pending_bets,
    save_team_stats, get_team_stats, get_stats,
    delete_today_pending_bets, get_unique_bets, reset_all_bets,
    get_stats_by_market, get_stats_by_league_detailed,
    get_bete_noire_bets, get_roi_over_time, get_streak,
    get_h2h_cache, set_h2h_cache, get_h2h_cache_status,
)
