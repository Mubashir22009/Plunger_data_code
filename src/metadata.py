from zoneinfo import ZoneInfo

cdt = ZoneInfo("America/Chicago")



DATA_FOLDER="data/"
BASE_URL = "https://onping.plowtech.net"
AUTH_URL = f"{BASE_URL}/auth/page/plow/getAuthToken"
LOGIN_URL = f"{BASE_URL}/auth/page/plow/plowlogin"
HISTORY_URL = f"{BASE_URL}/json/listers/parameterHistoryLister"

COOKIE_FILE = "data/onping_cookies.pkl"
WELLS_CONFIG_FILE = "config/wells-config.json"