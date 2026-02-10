import json
import time
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from dateutil.parser import isoparse
from supabase import create_client, Client
import os
from typing import List, Dict, Any, Optional, Tuple


def _chunked(iterable: List[Dict[str, Any]], size: int):
    """Yield successive chunks of the given size."""
    for i in range(0, len(iterable), size):
        yield iterable[i:i+size]


def _insert_with_retries(
    supabase: Client,
    table: str,
    records: List[Dict[str, Any]],
    returning: str = "minimal",
    max_retries: int = 3,
    backoff_sec: float = 1.5,
):
    """Insert with simple exponential backoff. Keeps payloads small and responses minimal."""
    attempt = 0
    while True:
        try:
            supabase.table(table).insert(records, returning=returning).execute()
            return
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_for = backoff_sec * (2 ** (attempt - 1))
            print(f"⚠️ Insert to {table} failed (attempt {attempt}/{max_retries}). Retrying in {sleep_for:.1f}s. Error: {e}")
            time.sleep(sleep_for)


def _update_with_retries(
    supabase: Client,
    table: str,
    updates: Dict[str, Any],
    category_name: str,
    asins: List[str],
    max_retries: int = 3,
    backoff_sec: float = 1.5,
):
    """Update rows with exponential backoff. Filters by category_name, is_current=true, and asin in list."""
    attempt = 0
    while True:
        try:
            supabase.table(table) \
                .update(updates) \
                .eq("category_name", category_name) \
                .eq("is_current", True) \
                .in_("asin", asins) \
                .execute()
            return
        except Exception as e:
            attempt += 1
            if attempt > max_retries:
                raise
            sleep_for = backoff_sec * (2 ** (attempt - 1))
            print(f"⚠️ Update to {table} failed (attempt {attempt}/{max_retries}). Retrying in {sleep_for:.1f}s. Error: {e}")
            time.sleep(sleep_for)


def _fetch_current_rows(supabase: Client, category_name: str) -> pd.DataFrame:
    """Fetch all is_current=true rows for a category from ms_products."""
    response = supabase.table("ms_products") \
        .select("asin, ms_rank, price, reviews, rating, sales_rank_now, sales_rank_before, change_pct, start_date") \
        .eq("category_name", category_name) \
        .eq("is_current", True) \
        .execute()

    if not response.data:
        return pd.DataFrame(columns=["asin", "ms_rank", "price", "reviews", "rating", "sales_rank_now", "sales_rank_before", "change_pct", "start_date"])

    return pd.DataFrame(response.data)


def _fetch_existing_products(supabase: Client, asins: List[str]) -> Dict[str, Dict[str, Any]]:
    """Fetch existing product_dim rows for given ASINs."""
    if not asins:
        return {}

    result = {}
    for chunk in _chunked(asins, 500):
        response = supabase.table("product_dim") \
            .select("asin, title, product_url, image_url") \
            .in_("asin", chunk) \
            .execute()

        for row in response.data:
            result[row["asin"]] = row

    return result


def _values_match(v1, v2) -> bool:
    """NULL-safe comparison of two values."""
    if pd.isna(v1) and pd.isna(v2):
        return True
    if pd.isna(v1) or pd.isna(v2):
        return False
    return v1 == v2


def _row_values_equal(today_data: Dict[str, Any], db_row: pd.Series, tracked_cols: List[str]) -> bool:
    """Compare tracked columns between today's data and DB row. NULL-safe."""
    for col in tracked_cols:
        today_val = today_data.get(col)
        db_val = db_row[col] if col in db_row.index else None
        if not _values_match(today_val, db_val):
            return False
    return True


def _safe_end_date(scrape_ts: str, existing_start: Optional[str]) -> str:
    """
    Ensure end_date > start_date to prevent zero-length intervals.

    If the existing row's start_date >= scrape_ts (e.g., rapid successive runs),
    bump end_date by 1 millisecond.
    """
    if existing_start:
        try:
            start_dt = isoparse(existing_start)
            scrape_dt = isoparse(scrape_ts)
            if start_dt >= scrape_dt:
                return (scrape_dt + timedelta(milliseconds=1)).isoformat()
        except (ValueError, TypeError):
            pass  # If parsing fails, just use scrape_ts
    return scrape_ts


def _prepare_product_records(df_products: pd.DataFrame, existing: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Prepare product records, preserving existing non-null values when new values are null."""
    records = []
    for _, row in df_products.iterrows():
        asin = row['asin']
        existing_row = existing.get(asin, {})
        record = {'asin': asin}

        for col in ['title', 'product_url', 'image_url']:
            new_val = row[col]
            if new_val is not None and not pd.isna(new_val):
                record[col] = new_val
            elif col in existing_row and existing_row[col] is not None:
                record[col] = existing_row[col]
            else:
                record[col] = None

        records.append(record)
    return records


def add_to_supabase(df: pd.DataFrame):
    """
    SCD Type II implementation for ms_products.

    Only creates new versions when tracked attributes actually change.

    Tracked columns: ms_rank, price, reviews, rating, sales_rank_now, sales_rank_before, change_pct

    Test cases:
    - Example 1 (no change): same values -> no update
    - Example 2 (rank change): ms_rank=51 -> ms_rank=49 -> close old, insert new
    - Example 3 (drop-off): ms_rank=49 -> missing -> close old, insert ms_rank=NULL
    - Example 4 (re-entry): ms_rank=NULL -> ms_rank=36 -> close drop, insert ranked
    """
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    TRACKED_COLS = ["ms_rank", "price", "reviews", "rating", "sales_rank_now", "sales_rank_before", "change_pct"]
    BATCH_SIZE = 800

    if df.empty:
        print("⚠️  No rows to load.")
        return

    if "category_name" not in df.columns:
        raise ValueError("DataFrame must include 'category_name' (grab it from the page <h1>).")

    # Phase A: Data Preparation
    df = df.copy()
    scrape_ts = datetime.now(timezone.utc).isoformat()

    # Type conversions - integers
    df["ms_rank"] = pd.to_numeric(df["movers_rank"], errors="coerce").astype("Int64")
    df["sales_rank_now"] = pd.to_numeric(df["sales_rank_now"], errors="coerce").astype("Int64")
    df["sales_rank_before"] = pd.to_numeric(df["sales_rank_before"], errors="coerce").astype("Int64")
    df["reviews"] = pd.to_numeric(df["reviews"], errors="coerce").astype("Int64")

    # Type conversions - floats
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    # "268,179%" → 268179.0
    df["change_pct"] = (
        df.get("change", pd.Series([""] * len(df)))
          .fillna("")
          .str.replace("[,%]", "", regex=True)
          .replace({"": None})
          .astype(float)
    )

    # Convert Int64 to int or None for JSON serialization
    for c in ["ms_rank", "sales_rank_now", "sales_rank_before", "reviews"]:
        df[c] = df[c].apply(lambda x: int(x) if pd.notna(x) else None)

    # Sanitize for JSON
    df = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)

    # Build df_products (asin, title, product_url, image_url)
    df_products = (
        df[["asin", "title", "product_url", "product_image"]]
        .drop_duplicates(subset="asin", keep="last")
        .rename(columns={"product_image": "image_url"})
    )

    # Build df_ms (category_name, asin + all tracked cols)
    df_ms = (
        df[["category_name", "asin", "ms_rank", "price", "reviews", "rating", "sales_rank_now", "sales_rank_before", "change_pct"]]
        .sort_values(["category_name", "ms_rank", "asin"])
        .drop_duplicates(subset=["category_name", "asin"], keep="first")
    )

    # Phase B: Upsert product_dim (preserving non-null values)
    all_asins = df_products["asin"].tolist()
    existing_products = _fetch_existing_products(supabase, all_asins)
    products_records = _prepare_product_records(df_products, existing_products)

    json.dumps(products_records, allow_nan=False)

    supabase.table("product_dim") \
        .upsert(products_records, on_conflict="asin", returning="minimal") \
        .execute()
    print("✅ product_dim upserted")

    # Phase C: SCD2 Processing Per Category
    rows_to_close_by_category: Dict[str, List[Tuple[str, Optional[str]]]] = {}  # {category: [(asin, start_date), ...]}
    rows_to_insert: List[Dict[str, Any]] = []

    categories_scraped = df_ms["category_name"].unique().tolist()

    for category_name in categories_scraped:
        cat_data = df_ms[df_ms["category_name"] == category_name]

        # Build today_map: Dict[asin] -> {ms_rank, price, reviews, rating, sales_rank_now, sales_rank_before, change_pct}
        today_map: Dict[str, Dict[str, Any]] = {}
        for _, row in cat_data.iterrows():
            today_map[row["asin"]] = {
                "ms_rank": row["ms_rank"],
                "price": row["price"],
                "reviews": row["reviews"],
                "rating": row["rating"],
                "sales_rank_now": row["sales_rank_now"],
                "sales_rank_before": row["sales_rank_before"],
                "change_pct": row["change_pct"],
            }

        # Fetch current rows from DB
        current_df = _fetch_current_rows(supabase, category_name)

        # Build current_map for easy lookup
        current_map: Dict[str, pd.Series] = {}
        for _, row in current_df.iterrows():
            current_map[row["asin"]] = row

        today_asins = set(today_map.keys())
        current_asins = set(current_map.keys())

        # Compute three sets
        to_insert_new = today_asins - current_asins
        to_check_updates = today_asins & current_asins
        to_check_dropoffs = current_asins - today_asins

        # Process new entries
        for asin in to_insert_new:
            data = today_map[asin]
            rows_to_insert.append({
                "asin": asin,
                "category_name": category_name,
                "ms_rank": data["ms_rank"],
                "price": data["price"],
                "reviews": data["reviews"],
                "rating": data["rating"],
                "sales_rank_now": data["sales_rank_now"],
                "sales_rank_before": data["sales_rank_before"],
                "change_pct": data["change_pct"],
                "start_date": scrape_ts,
                "end_date": None,
                "is_current": True,
            })

        # Process updates (check if values changed)
        for asin in to_check_updates:
            today_data = today_map[asin]
            db_row = current_map[asin]

            if not _row_values_equal(today_data, db_row, TRACKED_COLS):
                # Values changed - close old row and insert new
                if category_name not in rows_to_close_by_category:
                    rows_to_close_by_category[category_name] = []
                rows_to_close_by_category[category_name].append((asin, db_row.get("start_date")))

                rows_to_insert.append({
                    "asin": asin,
                    "category_name": category_name,
                    "ms_rank": today_data["ms_rank"],
                    "price": today_data["price"],
                    "reviews": today_data["reviews"],
                    "rating": today_data["rating"],
                    "sales_rank_now": today_data["sales_rank_now"],
                    "sales_rank_before": today_data["sales_rank_before"],
                    "change_pct": today_data["change_pct"],
                    "start_date": scrape_ts,
                    "end_date": None,
                    "is_current": True,
                })

        # Process drop-offs
        for asin in to_check_dropoffs:
            db_row = current_map[asin]
            db_rank = db_row["ms_rank"]

            # Only create drop-off if current ms_rank IS NOT NULL (not already dropped)
            if db_rank is not None and not pd.isna(db_rank):
                if category_name not in rows_to_close_by_category:
                    rows_to_close_by_category[category_name] = []
                rows_to_close_by_category[category_name].append((asin, db_row.get("start_date")))

                # Insert drop-off row with ms_rank=NULL
                rows_to_insert.append({
                    "asin": asin,
                    "category_name": category_name,
                    "ms_rank": None,
                    "price": db_row["price"],
                    "reviews": db_row["reviews"],
                    "rating": db_row["rating"],
                    "sales_rank_now": db_row["sales_rank_now"],
                    "sales_rank_before": db_row["sales_rank_before"],
                    "change_pct": db_row["change_pct"],
                    "start_date": scrape_ts,
                    "end_date": None,
                    "is_current": True,
                })

        # Log stats for this category
        new_count = len(to_insert_new)
        updated_count = len([a for a in to_check_updates if not _row_values_equal(today_map[a], current_map[a], TRACKED_COLS)])
        dropoff_count = len([a for a in to_check_dropoffs if current_map[a]["ms_rank"] is not None and not pd.isna(current_map[a]["ms_rank"])])
        unchanged_count = len(to_check_updates) - updated_count

        if new_count > 0 or updated_count > 0 or dropoff_count > 0:
            print(f"📊 {category_name}: {new_count} new, {updated_count} updated, {dropoff_count} drop-offs, {unchanged_count} unchanged")

    # Phase D: Batch Execute Updates and Inserts

    # Close rows (set is_current=false, end_date)
    # Group by end_date to batch updates efficiently while preventing zero-length intervals
    total_closed = 0
    for category_name, asin_start_pairs in rows_to_close_by_category.items():
        # Group ASINs by their safe end_date
        end_date_groups: Dict[str, List[str]] = {}
        for asin, start_date in asin_start_pairs:
            safe_end = _safe_end_date(scrape_ts, start_date)
            if safe_end not in end_date_groups:
                end_date_groups[safe_end] = []
            end_date_groups[safe_end].append(asin)

        # Batch update by end_date group
        for end_date, asins_to_close in end_date_groups.items():
            for chunk in _chunked(asins_to_close, 200):
                _update_with_retries(
                    supabase,
                    table="ms_products",
                    updates={"is_current": False, "end_date": end_date},
                    category_name=category_name,
                    asins=chunk,
                )
                total_closed += len(chunk)

    if total_closed > 0:
        print(f"✅ Closed {total_closed} rows across {len(rows_to_close_by_category)} categories")

    # Insert new rows
    if rows_to_insert:
        # Ensure integers are proper ints, not floats
        for rec in rows_to_insert:
            for col in ("ms_rank", "sales_rank_now", "sales_rank_before", "reviews"):
                val = rec.get(col)
                if isinstance(val, float) and not pd.isna(val) and val == int(val):
                    rec[col] = int(val)

        json.dumps(rows_to_insert, allow_nan=False)

        total = len(rows_to_insert)
        done = 0
        for chunk in _chunked(rows_to_insert, BATCH_SIZE):
            _insert_with_retries(
                supabase,
                table="ms_products",
                records=chunk,
                returning="minimal",
                max_retries=3,
                backoff_sec=1.5,
            )
            done += len(chunk)
            if done % 2000 == 0 or done == total:
                print(f"✅ ms_products insert progress: {done}/{total}")
        print(f"✅ ms_products insert complete ({total} rows)")
    else:
        print("✅ No changes detected - no rows to insert")

    # Phase E: Update ms_category_last_updated
    last_updated_records = [
        {"category_name": cat, "last_updated": scrape_ts}
        for cat in categories_scraped
    ]

    supabase.table("ms_category_last_updated") \
        .upsert(last_updated_records, on_conflict="category_name", returning="minimal") \
        .execute()
    print("✅ ms_category_last_updated updated")
