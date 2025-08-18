from typing import Dict, Callable

def url_games(p):   return f"/games?year={p['season']}&week={p['week']}&division={p.get('division','fbs')}"
def url_teams(p):   return f"/teams/fbs?year={p['season']}"
def url_team_box(p):return f"/team-box?year={p['season']}&week={p['week']}"

ENDPOINTS: Dict[str, Dict] = {
    "games": {
        "url": url_games,
        "target": "games",             # base table name
        "mode": "slice",               # per season/week slice
    },
    "teams": {
        "url": url_teams,
        "target": "teams",
        "mode": "season",              # per season slice
    },
    "teams_box": {
        "url": url_team_box,
        "target": "teams_box",
        "mode": "slice",
    },
}
