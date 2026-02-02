import asyncio
from playwright.async_api import TimeoutError
from .config import CARD_ANY, THROTTLE_PATTERNS

async def _wait_for_cards_or_throttle(page, timeout=15_000) -> bool:
    try:
        await page.wait_for_function(
            """([sel, pats]) => {
                   if (document.querySelectorAll(sel).length) return true;
                   const body = document.body.innerText.toLowerCase();
                   return pats.some(p => body.includes(p));
               }""",
            arg=[CARD_ANY, [p.lower() for p in THROTTLE_PATTERNS]],
            timeout=timeout,
        )
    except TimeoutError:
        return False
    return bool(await page.query_selector("div[data-p13n-sc-list-item], .zg-grid-general-faceout"))

async def scroll_and_load_all_items(page,
                                    max_attempts: int = 3,
                                    scroll_steps: int = 12,
                                    items_selector: str = CARD_ANY):
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            print(f"🔄  Reloading page (attempt {attempt}/{max_attempts})...")
            await page.reload()
            await page.wait_for_load_state("networkidle")

        print("Scrolling to load all items...")
        items = []
        for i in range(scroll_steps):
            await page.evaluate("window.scrollBy(0, 800)")
            await asyncio.sleep(1)
            items = await page.query_selector_all(items_selector)
            print(f"After scroll {i + 1}: {len(items)} items loaded")
            if len(items) >= 50:
                break

        if items:
            break
        else:
            print("❌  No items found;" + (" will retry…" if attempt < max_attempts else " final attempt failed."))

    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    await asyncio.sleep(2)
    final_items = await page.query_selector_all(items_selector)
    print(f"Final item count after scrolling: {len(final_items)}")
    return final_items
