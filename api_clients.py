# api_clients.py — shim de compatibilité → core/api_clients.py
from core.api_clients import *  # noqa
from core.api_clients import (
    get_odds_quota, odds_quota_ok, clear_odds_cache,
    get_fixtures, get_odds, get_team_standings,
    get_fixtures_results_batch, get_all_results_today,
    normalize_team_name, get_h2h, clear_h2h_cache,
    get_recent_form, clear_form_cache,
    get_odds_api_usage, prefetch_season_matches,
    FOOTBALLDATA_LEAGUE_MAP,
)
