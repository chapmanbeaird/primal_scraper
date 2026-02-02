import re, random
from urllib.parse import urlparse
from fake_useragent import UserAgent

UA = UserAgent()
_DESKTOP_FAMS = ["chrome", "edge", "firefox", "safari"]
_MOBILE_RE   = re.compile(r"Mobile|Android|iPhone|iPad|Windows Phone", re.I)

def random_desktop_ua() -> str:
    while True:
        ua = getattr(UA, random.choice(_DESKTOP_FAMS))
        if not _MOBILE_RE.search(ua):
            return ua
