"""
model.py - Poisson-based football match prediction model

Matching par NOM d'équipe (pas par ID) pour compatibilité
entre The Odds API et les stats FBref/fallback.
"""

import math


def poisson_prob(lam: float, k: int) -> float:
    if lam <= 0:
        return 1.0 if k == 0 else 0.0
    return (lam ** k * math.exp(-lam)) / math.factorial(k)


def build_score_matrix(lambda_home: float, lambda_away: float, max_goals: int = 8):
    return [
        [poisson_prob(lambda_home, i) * poisson_prob(lambda_away, j)
         for j in range(max_goals + 1)]
        for i in range(max_goals + 1)
    ]


def calc_1x2(matrix) -> dict:
    home_win = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i > j)
    draw     = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i == j)
    away_win = sum(matrix[i][j] for i in range(len(matrix)) for j in range(len(matrix[i])) if i < j)
    return {"home_win": home_win, "draw": draw, "away_win": away_win}


def calc_over_under(matrix, threshold: float = 2.5) -> dict:
    over = sum(
        matrix[i][j]
        for i in range(len(matrix))
        for j in range(len(matrix[i]))
        if (i + j) > threshold
    )
    return {"over_2_5": over, "under_2_5": 1 - over}


def calc_btts(matrix) -> dict:
    btts = sum(
        matrix[i][j]
        for i in range(1, len(matrix))
        for j in range(1, len(matrix[i]))
    )
    return {"btts_yes": btts, "btts_no": 1 - btts}


def calc_league_averages(team_stats: dict):
    """Calcule les moyennes de buts domicile/extérieur de la ligue."""
    total_home_scored = sum(s["home_goals_scored"] for s in team_stats.values())
    total_away_scored = sum(s["away_goals_scored"] for s in team_stats.values())
    total_home_games  = sum(s["home_games"] for s in team_stats.values())
    total_away_games  = sum(s["away_games"] for s in team_stats.values())

    avg_home = total_home_scored / max(total_home_games, 1)
    avg_away = total_away_scored / max(total_away_games, 1)
    return avg_home, avg_away


def calc_attack_defense_strength(team_stats: dict, league_avg_home: float, league_avg_away: float):
    """
    Calcule les forces attaque/défense par équipe.
    Indexé par NOM d'équipe (pas ID) pour matching fiable.
    """
    strengths = {}
    for tid, s in team_stats.items():
        h_games = max(s["home_games"], 1)
        a_games = max(s["away_games"], 1)

        home_scored_avg   = s["home_goals_scored"]   / h_games
        home_conceded_avg = s["home_goals_conceded"]  / h_games
        away_scored_avg   = s["away_goals_scored"]    / a_games
        away_conceded_avg = s["away_goals_conceded"]  / a_games

        name = s["team_name"]
        strengths[name] = {
            "att_home": home_scored_avg   / max(league_avg_home, 0.01),
            "def_home": home_conceded_avg / max(league_avg_away, 0.01),
            "att_away": away_scored_avg   / max(league_avg_away, 0.01),
            "def_away": away_conceded_avg / max(league_avg_home, 0.01),
        }

        # Alias normalisé pour matching approximatif
        strengths[name.lower()] = strengths[name]

    return strengths


def _fuzzy_get(strengths: dict, name: str):
    """
    Cherche une équipe dans strengths par nom exact,
    puis par correspondance partielle si introuvable.
    """
    if name in strengths:
        return strengths[name]

    name_lower = name.lower()
    if name_lower in strengths:
        return strengths[name_lower]

    # Matching partiel
    for key in strengths:
        if isinstance(key, str) and (key.lower() in name_lower or name_lower in key.lower()):
            return strengths[key]

    return None


def predict_match(home_name: str, away_name: str, strengths: dict, league_avg_home: float, league_avg_away: float):
    """
    Prédit un match via Poisson.
    Utilise le NOM des équipes pour le matching.
    """
    h = _fuzzy_get(strengths, home_name)
    a = _fuzzy_get(strengths, away_name)

    if not h or not a:
        return None

    lambda_home = h["att_home"] * a["def_away"] * league_avg_home
    lambda_away = a["att_away"] * h["def_home"] * league_avg_away

    lambda_home = max(0.3, min(lambda_home, 6.0))
    lambda_away = max(0.3, min(lambda_away, 6.0))

    matrix    = build_score_matrix(lambda_home, lambda_away)
    probs_1x2 = calc_1x2(matrix)
    probs_ou  = calc_over_under(matrix)
    probs_btts = calc_btts(matrix)

    return {
        "lambda_home": round(lambda_home, 3),
        "lambda_away": round(lambda_away, 3),
        "home_win":    round(probs_1x2["home_win"], 4),
        "draw":        round(probs_1x2["draw"], 4),
        "away_win":    round(probs_1x2["away_win"], 4),
        "over_2_5":    round(probs_ou["over_2_5"], 4),
        "under_2_5":   round(probs_ou["under_2_5"], 4),
        "btts_yes":    round(probs_btts["btts_yes"], 4),
        "btts_no":     round(probs_btts["btts_no"], 4),
    }


def find_value_bets(predictions: dict, odds: dict, value_threshold: float = 0.05, min_prob: float = 0.55):
    """
    Compare les probabilités du modèle aux cotes bookmakers.
    Retourne UN seul bet par marché — le bookmaker avec la meilleure cote.
    """
    market_map = {
        "home_win":  "Home Win",
        "draw":      "Draw",
        "away_win":  "Away Win",
        "over_2_5":  "Over 2.5",
        "under_2_5": "Under 2.5",
    }

    # Un seul bet par marché — meilleure cote
    best_per_market = {}

    for bk_name, bk_odds in odds.items():
        for market_key, market_label in market_map.items():
            prob   = predictions.get(market_key)
            bk_odd = bk_odds.get(market_key)

            if prob is None or bk_odd is None:
                continue
            if prob < min_prob:
                continue

            value = (bk_odd * prob) - 1
            if value <= value_threshold:
                continue

            existing = best_per_market.get(market_key)
            if existing is None or bk_odd > existing["bk_odds"]:
                best_per_market[market_key] = {
                    "market":      market_label,
                    "bookmaker":   bk_name,
                    "bk_odds":     round(bk_odd, 3),
                    "model_odds":  round(1 / prob, 3),
                    "probability": round(prob, 4),
                    "value":       round(value, 4),
                }

    return sorted(best_per_market.values(), key=lambda x: x["value"], reverse=True)