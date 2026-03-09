"""
biathlon_client.py
------------------
Client pour l'API non officielle biathlonresults.com (IBU officiel).
Toutes les données sont publiques et gratuites.

Endpoints utilisés :
  GET http://biathlonresults.com/modules/sportapi/api/Events?SeasonId=...
  GET http://biathlonresults.com/modules/sportapi/api/Competitions?EventId=...
  GET http://biathlonresults.com/modules/sportapi/api/Results?RaceId=...
  GET http://biathlonresults.com/modules/sportapi/api/CupResults?IBU_ID=...
  GET http://biathlonresults.com/modules/sportapi/api/AthResults?IBU_ID=...
  GET http://biathlonresults.com/modules/sportapi/api/AnalyticResults?RaceId=...
"""

import os
import time
import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger(__name__)

IBU_BASE = "http://biathlonresults.com/modules/sportapi/api"

# Cache mémoire simple (TTL 1h pour les données live, 24h pour l'historique)
_cache: dict = {}

# Formats de course IBU
RACE_FORMATS = {
    "SR":  "Sprint",
    "PU":  "Poursuite",
    "IN":  "Individuelle",
    "MS":  "Mass Start",
    "RL":  "Relais",
    "MX":  "Relais Mixte",
    "SM":  "Single Mixed",
}

# Saisons disponibles
CURRENT_SEASON = "2526"  # 2025/26
PREV_SEASON    = "2425"  # 2024/25


def _get(endpoint: str, params: dict = None, ttl: int = 3600) -> Optional[dict | list]:
    """
    Appel GET avec cache mémoire.
    ttl : durée de validité du cache en secondes.
    """
    url = f"{IBU_BASE}/{endpoint}"
    cache_key = url + str(sorted((params or {}).items()))

    if cache_key in _cache:
        data, ts = _cache[cache_key]
        if time.time() - ts < ttl:
            return data

    try:
        resp = requests.get(url, params=params or {}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        _cache[cache_key] = (data, time.time())
        return data
    except Exception as e:
        log.warning(f"[IBU] {endpoint} : {e}")
        return None


def get_competitions(season: str = CURRENT_SEASON) -> list:
    """
    Retourne toutes les courses d'une saison IBU World Cup.
    Appelle Events → puis Competitions pour chaque event.

    Structure retournée :
    [
      {
        "RaceId": "BT2526SWRLCP01SWSP",
        "ShortDescription": "Sprint Women",
        "StartTime": "2025-11-28T...",
        "Location": "Kontiolahti",
        "Status": "Official",
        "RaceTypeId": "SP",
      }, ...
    ]
    """
    # Niveau 1 : liste des events (étapes CdM)
    events = _get("Events", {"SeasonId": season, "Level": "BMTIBT"}, ttl=3600)
    if not events:
        return []

    races = []
    for event in (events if isinstance(events, list) else []):
        event_id  = event.get("EventId", "")
        location  = event.get("ShortDescription", event.get("Organizer", ""))
        if not event_id:
            continue
        # Niveau 2 : courses dans cet event
        comps = _get("Competitions", {"EventId": event_id}, ttl=3600)
        if not comps:
            continue
        for c in (comps if isinstance(comps, list) else []):
            c["Location"] = location
            races.append(c)
    return races


def get_results(race_id: str) -> list:
    """
    Résultats complets d'une course.

    Structure par athlète :
    {
      "IBU_ID": "BTFRA22205199201",
      "Name": "JACQUELIN Emilien",
      "Nat": "FRA",
      "Rank": 3,
      "TotalTime": "00:23:45.2",
      "Behind": "+00:00:34.1",
      "Shootings": "1 0 1 0",   # 0=raté, 1=touché (couché puis debout pour sprint)
      "ShootingMisses": 2,
      "PenaltyLoops": 2,
    }
    """
    data = _get("Results", {"RaceId": race_id}, ttl=86400)  # 24h car résultats immuables
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Results", [])


def get_athlete_results(ibu_id: str, season: str = None) -> list:
    """
    Tous les résultats d'un athlète (toutes courses).
    Si season est None, retourne l'historique complet.
    """
    params = {"IBU_ID": ibu_id}
    if season:
        params["SeasonId"] = season
    data = _get("AthResults", params, ttl=3600)
    if not data:
        return []
    return (data if isinstance(data, list) else data.get("Results", [])) if data else []


def get_analytic_results(race_id: str) -> list:
    """
    Résultats analytiques d'une course : temps de ski pur, temps de tir, etc.

    Structure :
    {
      "IBU_ID": "...",
      "Name": "...",
      "SkiTime": "00:20:12.1",     # temps ski sans pénalités
      "ShootingTime": "00:01:33.0", # temps total aux stands
      "RangeTime": "...",
      "CourseTime": "...",
    }
    """
    data = _get("AnalyticResults", {"RaceId": race_id}, ttl=86400)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Results", [])


def get_cup_standings(season: str = CURRENT_SEASON, gender: str = "W") -> list:
    """
    Classement Coupe du Monde général.
    gender: "W" (femmes) ou "M" (hommes)

    IBU_ID format: BT{season}SWRLCP__SW pour femmes, BT{season}SWRLCP__SM pour hommes
    """
    suffix = "SW" if gender == "W" else "SM"
    ibu_id = f"BT{season}SWRLCP__{suffix}TS"  # TS = Total Score
    data = _get("CupResults", {"CupId": ibu_id, "Rnk": 1}, ttl=3600)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("Rows", [])


def get_upcoming_races(days_ahead: int = 10) -> list:
    """
    Retourne les prochaines courses dans les N jours.
    Filtre sur Status != "Official" (pas encore disputée).
    """
    races = get_competitions(CURRENT_SEASON)
    today = datetime.now(timezone.utc).date()
    cutoff = today + timedelta(days=days_ahead)

    upcoming = []
    for r in races:
        start_raw = r.get("StartTime", "")
        if not start_raw:
            continue
        try:
            start_date = datetime.fromisoformat(start_raw.replace("Z", "+00:00")).date()
        except ValueError:
            continue
        status = str(r.get("Status", "0"))
        if today <= start_date <= cutoff and status != "1":  # "1" = official/terminé
            upcoming.append({
                "race_id":    r.get("RaceId", ""),
                "description": r.get("Description", ""),
                "location":   r.get("Location", ""),
                "date":       start_date.isoformat(),
                "format":     r.get("RaceTypeId", ""),
                "format_name": RACE_FORMATS.get(r.get("RaceTypeId", ""), r.get("RaceTypeId", "")),
                "gender":     "W" if "SW" in r.get("RaceId", "") else "M",
            })
    return sorted(upcoming, key=lambda x: x["date"])


def parse_shooting_string(shootings_str: str) -> dict:
    """
    Parse la chaîne de tirs IBU : "1 0 1 0 / 1 1 0 1"
    (couché / debout pour sprint, 4 stands pour individuelle)

    Retourne :
    {
      "total_shots": 10,
      "hits": 8,
      "misses": 2,
      "prone_accuracy": 0.75,   # couché
      "standing_accuracy": 1.0, # debout
    }
    """
    if not shootings_str:
        return {"total_shots": 0, "hits": 0, "misses": 0, "prone_accuracy": None, "standing_accuracy": None}

    parts = shootings_str.replace(" ", "")
    # Supporte "10101011" ou "1010/1011"
    normalized = parts.replace("/", "")
    shots = [int(c) for c in normalized if c in "01"]

    total = len(shots)
    hits  = sum(shots)

    # Sprint : 5 couché + 5 debout
    # Individuelle : 5+5+5+5
    half = total // 2
    prone   = shots[:half]
    standing = shots[half:]

    prone_acc   = sum(prone)   / len(prone)   if prone   else None
    standing_acc = sum(standing) / len(standing) if standing else None

    return {
        "total_shots":        total,
        "hits":               hits,
        "misses":             total - hits,
        "accuracy":           hits / total if total else None,
        "prone_accuracy":     prone_acc,
        "standing_accuracy":  standing_acc,
    }


def time_to_seconds(time_str: str) -> Optional[float]:
    """Convertit '00:23:45.2' ou '+00:00:34.1' en secondes."""
    if not time_str:
        return None
    try:
        s = time_str.lstrip("+").strip()
        parts = s.split(":")
        if len(parts) == 3:
            h, m, sec = parts
            return int(h) * 3600 + int(m) * 60 + float(sec)
        elif len(parts) == 2:
            m, sec = parts
            return int(m) * 60 + float(sec)
        return float(s)
    except Exception:
        return None


def clear_cache():
    """Vide le cache mémoire."""
    global _cache
    _cache = {}
    log.info("[IBU] Cache vidé.")