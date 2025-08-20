from typing import Dict, Callable

def url_games(p):   return f"/games?year={p['season']}&week={p['week']}"
def url_games_teams(p):   return f"/games/teams?year={p['season']}&week={p['week']}"
def url_games_players(p):return f"/games/players?year={p['season']}&week={p['week']}"

ENDPOINTS: Dict[str, Dict] = {
    "games": {
        "url": url_games,
        "target": "games",             # base table name
        "mode": "explode_all",               # per season/week slice
    },
    "games_teams": {
        "url": url_games_teams,
        "target": "games_teams",
        "mode": "explode_all",              # per season slice
    },
    "games_players": {
        "url": url_games_players,
        "target": "games_players",
        "mode": "explode_all",
    },
}
