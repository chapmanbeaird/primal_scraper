import random, asyncio
from datetime import datetime
from playwright.async_api import TimeoutError

from .config import (CARD_ANY, THROTTLE_TEXT,
                     THROTTLE_MAX_RELOADS, THROTTLE_BASE_WAIT_SEC)
from .utils_misc import random_desktop_ua

THROTTLE_TEXT = "Request was throttled"
THROTTLE_MAX_RELOADS   = 6
THROTTLE_BASE_WAIT_SEC = 4

async def get_rotated_context(playwright):
    ua = random_desktop_ua()
    vp = random.choice([
        {"width": 1920, "height": 1080},
        {"width": 1366, "height": 768},
        {"width": 1536, "height": 864},
        {"width": 1280, "height": 800},
        {"width": 1440, "height": 900},
    ])
    tz = random.choice(["UTC", "US/Eastern", "Europe/Berlin", "Asia/Tokyo"])

    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-features=VizDisplayCompositor",
            "--disable-blink-features=AutomationControlled",
        ],
    )

    context = await browser.new_context(
        user_agent=ua,
        viewport=vp,
        accept_downloads=False,
        locale="en-US",
        timezone_id=tz,
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Sec-CH-UA-Platform": random.choice(['"Windows"','"macOS"','"Linux"']),
            "Sec-CH-UA": '"Chromium";v="121", "Not:A-Brand";v="121"',
            "DNT": "1",
        }
    )

    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'mimeTypes', {get: () => [1,2,3]});
        const getParameter = WebGLRenderingContext.prototype.getParameter;
        WebGLRenderingContext.prototype.getParameter = function(param){
            if (param === 37445) return "Intel Inc.";
            if (param === 37446) return "Intel Iris OpenGL Engine";
            return getParameter(param);
        };
    """)
    return browser, context

async def check_not_blocked(playwright, url: str, ok_selector: str = CARD_ANY,
                            max_attempts_ctx: int = 10, sleep_after_load: float = 3.0,
                            goto_timeout: int = 60_000):
    for ctx_try in range(1, max_attempts_ctx + 1):
        print(f"\n🚀  Context attempt {ctx_try}/{max_attempts_ctx}")
        browser, context = await get_rotated_context(playwright)
        page = await context.new_page()
        success = False
        try:
            await page.goto(url, timeout=goto_timeout, wait_until="domcontentloaded")
            await asyncio.sleep(sleep_after_load)

            if await page.query_selector(ok_selector):
                print("✅  Context is clean; continuing…")
                success = True
                return browser, context, page

            if await page.locator(f"text={THROTTLE_TEXT}").count() > 0:
                for attempt in range(1, THROTTLE_MAX_RELOADS + 1):
                    wait_secs = THROTTLE_BASE_WAIT_SEC * (2 ** (attempt - 1)) + random.random()
                    print(f"⏳  Throttled – wait {wait_secs:.1f}s, reload {attempt}/{THROTTLE_MAX_RELOADS}…")
                    await asyncio.sleep(wait_secs)
                    await page.reload(wait_until="domcontentloaded")
                    await asyncio.sleep(sleep_after_load)
                    if await page.query_selector(ok_selector):
                        print(f"✅  Recovered after {attempt} reload(s); continuing in same context.")
                        success = True
                        return browser, context, page

            print(f"❌  Blocked")

        finally:
            if not success:
                await browser.close()

    print("❌  All context attempts failed.")
    return None, None, None
