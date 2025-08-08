# --- Authentication Manager ---
import pickle 
import os
from dotenv import load_dotenv
load_dotenv()
import json
import requests

from src.metadata import (
    WELLS_CONFIG_FILE, COOKIE_FILE,
    BASE_URL, AUTH_URL, LOGIN_URL
)

class AuthManager:
    def __init__(self):
        self.cookies = None
        self.load_credentials()

    def load_credentials(self):
        if os.path.exists(WELLS_CONFIG_FILE):
            with open(WELLS_CONFIG_FILE) as f:
                config = json.load(f)
                self.username = os.getenv("ONPING_USERNAME")
                self.password = os.getenv("ONPING_PASSWORD")
                print("username :",self.username)
                # print("Username: ")
        else:
            self.username = os.getenv("ONPING_USERNAME")
            self.password = os.getenv("ONPING_PASSWORD")
            print("username :",self.username)


        if not self.username or not self.password:
            raise ValueError("Credentials not found")

    def authenticate(self, force_new=False):
        if not force_new and os.path.exists(COOKIE_FILE):
            with open(COOKIE_FILE, "rb") as f:
                self.cookies = pickle.load(f)
                if self._test_cookies():
                    print("Using cached cookies")
                    return True

        print("Authenticating...")
        try:
            auth_data = {
                "username": self.username,
                "password": self.password,
                "useragent": "PlungerLiftMonitor/1.0"
            }
            r = requests.post(AUTH_URL, json=auth_data, timeout=10)
            r.raise_for_status()
            auth_data = r.json()
            print(auth_data)

            if "Left" in auth_data:
                raise ValueError(f"Auth failed: {auth_data['Left']}")

            r = requests.post(LOGIN_URL, json=auth_data["Right"], timeout=10)
            r.raise_for_status()

            self.cookies = r.cookies
            with open(COOKIE_FILE, "wb") as f:
                pickle.dump(self.cookies, f)

            print("Auth successful")
            return True
        except Exception as e:
            print("Auth error:", e)
            if os.path.exists(COOKIE_FILE):
                os.remove(COOKIE_FILE)
            return False

    def _test_cookies(self):
        try:
            r = requests.get(f"{BASE_URL}/json/listers/companyLister", cookies=self.cookies, timeout=5)
            return r.status_code == 200
        except:
            return False