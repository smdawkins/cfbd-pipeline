import requests

def build_session(api_key: str) -> requests.Session:
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {api_key}"})
    s.timeout = 60
    return s

def fetch_json(session: requests.Session, base_url: str, path: str):
    r = session.get(f"{base_url}{path}")
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, list) else [data]
