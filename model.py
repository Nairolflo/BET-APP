# model.py — shim de compatibilité → sports/football/model.py
from sports.football.model import *  # noqa
from sports.football.model import (
    calc_league_averages,
    calc_attack_defense_strength,
    predict_match,
    find_value_bets,
)
