"""secret_keys.py
Reads key=value pairs from secret_keys.txt in the same directory.
Keys are stripped of whitespace; values preserve internal spaces (e.g. app passwords).

File format:
    GOOGLE_MAPS_API_KEY=AIzaSy...
    GOOGLE_APP_PASSWORD=xxxx xxxx xxxx xxxx
    CESIUM_TOKEN=eyJ...

Lines starting with # are treated as comments and ignored.
"""

import os

_KEYS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "secret_keys.txt")
_cache = {}

def _load():
    global _cache
    if _cache:
        return
    if not os.path.exists(_KEYS_FILE):
        raise FileNotFoundError(
            f"secret_keys.txt not found at {_KEYS_FILE}\n"
            "Create it with the following keys:\n"
            "  GOOGLE_MAPS_API_KEY=...\n"
            "  GOOGLE_APP_PASSWORD=...\n"
            "  CESIUM_TOKEN=...\n"
        )
    with open(_KEYS_FILE, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                _cache[key.strip()] = value  # preserve spaces in value (app passwords)

def get(key: str, default: str = "") -> str:
    """Return the value for key, or default if not found."""
    _load()
    return _cache.get(key, default)

# Convenience accessors
def google_maps_api_key() -> str:
    return get("GOOGLE_MAPS_API_KEY")

def google_app_password() -> str:
    return get("GOOGLE_APP_PASSWORD")

def cesium_token() -> str:
    return get("CESIUM_TOKEN")

def gmail_address() -> str:
    return get("GMAIL_ADDRESS")
