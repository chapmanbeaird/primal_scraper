import asyncio, json
import numpy as np
import pandas as pd
from pathlib import Path
from playwright.async_api import async_playwright

from .config import CARD_ANY
from .extractors import JS_EXTRACT, JS_SINGLE_NODE_EXTRACT
from .playwright_ctx import check_not_blocked
from .scrolling import scroll_and_load_all_items
from .pagination import goto_next_page

# ── main scraper ───────────────────────────────────────────────────────
async def scrape_fast(url: str, max_pages: int = 2):
    rows = []

    async with async_playwright() as p:
        browser, context, page = await check_not_blocked(p, url)
        if not page:
            return rows

        page_num = 1
        retry_same = 0
        while page_num <= max_pages:
            # 1) scroll until the DOM is populated
            await scroll_and_load_all_items(page)

            # 2) extract every matching node
            page_data = await page.locator(CARD_ANY).evaluate_all(JS_EXTRACT)
            good_rows  = [r for r in page_data if not r["_missing"]]
            bad_rows   = [r for r in page_data if r["_missing"]]

            print(f"Bad rows: {bad_rows}")

            if bad_rows:
              print(f"⚠️  {len(bad_rows)} rows incomplete on page {page_num}")

              for row in bad_rows:
                  handle = await page.query_selector(f"[data-asin='{row['asin']}']")
                  if not handle:
                      continue  # card vanished during scrolling

                  # 👇 Force the card into the viewport so price/rating JS fires
                  await handle.scroll_into_view_if_needed()
                  await page.wait_for_timeout(1000)
                  # Re-run JS snippet but only for this node
                  patched = await handle.evaluate(JS_SINGLE_NODE_EXTRACT)
                  row.update(patched)

            fixed_rows     = [r for r in bad_rows if not r["_missing"]]
            still_bad_rows = [r for r in bad_rows if r["_missing"]]
            print(
                f"🔄 Patch result page {page_num}: "
                f"fixed {len(fixed_rows)} / {len(bad_rows)}  "
                f"(still missing: {len(still_bad_rows)})"
            )

            # 3) de-duplicate by ASIN (fallback: title)
            unique = {}
            for row in page_data:
                key = row.get("asin") or row.get("title")
                if key and key not in unique:
                    unique[key] = row
            kept = list(unique.values())

            rows.extend(kept)

            # quick terminal peek
            if page_data:
                print("  ↳ raw[0] :", page_data[0])
            if kept:
                print("  ↳ kept[0]:", kept[0])
            print("    …")

            result = await goto_next_page(page, page_num + 1)

            if result["ok"]:
                page_num += 1
                retry_same = 0
                continue
            elif result["reason"] == "throttled":
                print("Throttled going to page 2, spinning up new context")
                next_url = page.url

                # 1) clean-up the old browser
                await browser.close()

                # 2) get a brand-new context / page on the same URL
                browser, context, page = await check_not_blocked(p, next_url)

                if not page:          # gave up after all context attempts
                    break
                page_num += 1
                retry_same = 0        # we’re effectively starting fresh
                continue              # re-scroll & re-extract the same page_num

            elif result["reason"] == "rank_mismatch":
                retry_same += 1
                if retry_same >= 3:
                    print("🚫  Stuck; skipping this category")
                    break
                continue                       # retry same page_num
            else:  # no_next_button
                break
        await browser.close()
    return rows


async def run_scrape_job(urls: list[str]):
    all_products = []
    for i, url in enumerate(urls, start=1):
        print(f"\n--- Processing URL {i}/{len(urls)}: {url} ---")
        products = await scrape_fast(url, max_pages=2)
        if products:
            all_products.extend(products)

    if not all_products:
        print("❌ No products scraped.")
        return None

    df = pd.DataFrame(all_products)

    df["price"] = (
        df["price"].str.replace(r"[^0-9.]", "", regex=True)
                    .replace("", np.nan)
                    .astype(float)
    )
    df["rank"]    = pd.to_numeric(df["rank"],    errors="coerce").astype("Int64")
    df["reviews"] = pd.to_numeric(df["reviews"].str.replace(",", ""), errors="coerce").astype("Int64")
    df["rating"]  = pd.to_numeric(df["rating"],  errors="coerce")

    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    print(f"\n=== RESULTS ===")
    print(f"Total products scraped: {len(all_products)}")
    print(f"Columns: {list(df.columns)}")

    # Show some statistics
    print(f"\nData quality:")
    print(f"- Products with ASINs: {df['asin'].notna().sum()}")
    print(f"- Products with URLs: {df['product_url'].notna().sum()}")
    print(f"- Products with images: {df['image_url'].notna().sum()}")
    print(f"- Products with titles: {df['title'].notna().sum()}")
    print(f"- Products with prices: {df['price'].notna().sum()}")
    print(f"- Products with ratings: {df['rating'].notna().sum()}")
    print(f"- Products with ranks: {df['rank'].notna().sum()}")
    return df