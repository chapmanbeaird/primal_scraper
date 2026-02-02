import json
import math
import time
import numpy as np
import pandas as pd
from datetime import datetime
from pytz import timezone as pytz_timezone
from supabase import create_client, Client
import os
from typing import List, Dict, Any, Optional

def _chunked(iterable: List[Dict[str, Any]], size: int):
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
    """
    Insert with simple exponential backoff. Keeps payloads small and responses minimal.
    """
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


def add_to_supabase(df: pd.DataFrame):
    BATCH = 500
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    if df.empty:
        print("⚠️  No rows to load.")
        return

    if "category_name" not in df.columns:
        raise ValueError("DataFrame must include 'category_name' (grab it from the page <h1>).")

    # ── copy & timestamp ─────────────────────────────────────────────
    df = df.copy()
    central = pytz_timezone("US/Central")
    scrape_ts = datetime.now(central).isoformat()
    print("🕒 Scrape timestamp:", scrape_ts)
    df["scraped_at"] = scrape_ts

    # ── type cleaning / coercion ─────────────────────────────────────
    # numbers
    df["price"]   = pd.to_numeric(df["price"], errors="coerce")
    df["rating"]  = pd.to_numeric(df["rating"], errors="coerce")
    df["reviews"] = pd.to_numeric(df["reviews"], errors="coerce").astype("Int64")

    # ranks
    df["ms_rank"]           = pd.to_numeric(df["movers_rank"], errors="coerce").astype("Int64")
    df["sales_rank_now"]    = pd.to_numeric(df["sales_rank_now"], errors="coerce").astype("Int64")
    df["sales_rank_before"] = pd.to_numeric(df["sales_rank_before"], errors="coerce").astype("Int64")

    # "268,179%" → 268179.0
    df["change_pct"] = (
        df.get("change", pd.Series([""] * len(df)))
          .fillna("")
          .str.replace("[,%]", "", regex=True)
          .replace({"": None})
          .astype(float)
    )

    # sanitize for JSON
    df = df.replace([np.inf, -np.inf], None)
    df = df.where(pd.notnull(df), None)

    # ── upsert product_dim (asin/title/url/image) ────────────────────
    df_products = (
        df[["asin", "title", "product_url", "product_image"]]
        .drop_duplicates(subset="asin", keep="last")
        .rename(columns={"product_image": "image_url"})
    )
    prod_records = json.loads(df_products.to_json(orient="records"))
    if prod_records:
        supabase.table("product_dim").upsert(
            prod_records,
            on_conflict="asin",
            returning="minimal"
        ).execute()
    print(f"✅ product_dim upserted: {len(prod_records)} rows")

    # ── prep ms_snapshots rows ───────────────────────────────────────
    df_ms = (
        df[[
            "category_name", "asin", "scraped_at",
            "ms_rank", "change_pct", "sales_rank_now", "sales_rank_before",
            "price", "rating", "reviews"
        ]]
        .sort_values(["category_name", "ms_rank", "asin"])
        .drop_duplicates(subset=["category_name", "asin"], keep="first")
    )

    # cast nullable Int64 → int (or None) so JSON is clean
    def _int_or_none(x):
        try:
            return int(x) if x is not None and pd.notna(x) else None
        except Exception:
            return None

    ms_records = json.loads(df_ms.to_json(orient="records", date_format="iso"))
    for r in ms_records:
        for k in ("ms_rank", "sales_rank_now", "sales_rank_before", "reviews"):
            r[k] = _int_or_none(r.get(k))

    # ── upsert into ms_snapshots (PK: category_name, asin, scraped_at) ───────
    total = 0
    for i in range(0, len(ms_records), BATCH):
        batch = ms_records[i:i+BATCH]
        if not batch:
            continue
        supabase.table("ms_snapshots").upsert(
            batch,
            on_conflict="category_name,asin,scraped_at",
            returning="minimal",
        ).execute()
        total += len(batch)
    print(f"✅ ms_snapshots upserted: {total} rows")

    # ── update ms_category_last_updated (separate from Best Sellers) ──
    ms_last_updated = [
        {"category_name": c, "last_updated": scrape_ts}
        for c in sorted(df["category_name"].dropna().unique())
    ]
    if ms_last_updated:
        supabase.table("ms_category_last_updated").upsert(
            ms_last_updated,
            on_conflict="category_name",
            returning="minimal"
        ).execute()
    print(f"✅ ms_category_last_updated updated for {len(ms_last_updated)} categories")

