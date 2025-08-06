import requests
from zoneinfo import ZoneInfo
from ..metadata import HISTORY_URL
cdt = ZoneInfo("America/Chicago")


def fetch_data_range(auth_manager, pid, start_time, end_time, step):
    params = {
        "pid": pid,
        "steps": [step],
        "time_ranges": [[start_time.strftime("%Y-%m-%dT%H:%M:%SZ"), end_time.strftime("%Y-%m-%dT%H:%M:%SZ")]],
        "delta": 15
    }
    # print("Params: ", params)
    for attempt in range(2):
        try:
            r = requests.get(HISTORY_URL, json=params, cookies=auth_manager.cookies, timeout=15)
            if r.status_code == 401 and attempt == 0:
                print("Session expired, retrying auth...")
                auth_manager.authenticate(force_new=True)
                continue
            r.raise_for_status()
            # print(f"Response status code: {r.status_code}")
            # print(f"Response content: {r.text[:800]}...")  # Print first 100 chars for brevity
            return r.json()
        except Exception as e:
            print(f"Attempt {attempt+1} failed for PID {pid}: {e}")
            if attempt == 1:
                return None