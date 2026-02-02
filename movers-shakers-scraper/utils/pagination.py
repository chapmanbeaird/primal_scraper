from .config import NEXT_SELECTORS
from .scrolling import _wait_for_cards_or_throttle

async def _collect_ranks(page, limit=120) -> list[int]:
    badges = await page.locator("span.zg-bdg-text, .zg-bdg-text").all()
    ranks = []
    for b in badges[:limit]:
        txt = (await b.inner_text()).strip().lstrip("#")
        if txt.isdigit():
            ranks.append(int(txt))
    return ranks

async def has_next_page(page) -> bool:
    for sel in NEXT_SELECTORS:
        link = await page.query_selector(sel)
        if link and await link.is_enabled():
            return True
    li_last = await page.query_selector("li.a-last")
    if not li_last:
        return False
    cls = (await li_last.get_attribute("class") or "").lower()
    return "a-disabled" not in cls

async def goto_next_page(page, page_num, max_retries=5) -> dict:
    if not await has_next_page(page):
        return {"ok": False, "reason": "no_next_button"}

    expected_start = (page_num - 1) * 50 + 1
    for attempt in range(max_retries):
        for sel in NEXT_SELECTORS:
            btn = await page.query_selector(sel)
            if not btn:
                continue
            try:
                async with page.expect_navigation(wait_until="domcontentloaded", timeout=8000):
                    await btn.click()
            except Exception:
                pass

            if not await _wait_for_cards_or_throttle(page):
                return {"ok": False, "reason": "throttled"}

            ranks = await _collect_ranks(page)
            inferred_page = (min(ranks) - 1) // 50 + 1 if ranks else None

            new_page_ranks = [r for r in ranks if r >= expected_start]
            if (inferred_page == page_num) or (
                expected_start in ranks
                and not any(r < expected_start for r in ranks)
                and len(new_page_ranks) >= 5
            ):
                print(f"✅ Reached page {page_num} (inferred={inferred_page}) sample={sorted(set(ranks))[:8]}")
                return {"ok": True, "reason": "moved"}

            print(f"🚫 Rank mismatch (attempt {attempt+1}/{max_retries}) inferred={inferred_page}, expected_start={expected_start}")
        print(f"↻  Full-page reload, retry {attempt+1}/{max_retries}")
        await page.reload(wait_until="domcontentloaded")

    return {"ok": False, "reason": "rank_mismatch"}
