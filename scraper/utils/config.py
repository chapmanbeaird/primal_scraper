import os
from pathlib import Path

# Throttle settings
THROTTLE_TEXT = "Request was throttled"
THROTTLE_MAX_RELOADS   = 3
THROTTLE_BASE_WAIT_SEC = 4

# Selectors
CARD_ANY = "div.zg-grid-general-faceout:has(a[href*='/dp/'])"
NEXT_SELECTORS = [
    "li.a-last a",
    "a[aria-label='Next page']",
    "ul.a-pagination li.a-last a",
]

THROTTLE_PATTERNS = [
    "request was throttled",
    "throttled",
    "robot check",
    "sorry, we just need to make sure",
]
