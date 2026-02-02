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
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

    df = df.copy()
    central = pytz_timezone("US/Central")
    scrape_ts = datetime.now(central).isoformat()
    df["start_date"] = scrape_ts
    df["end_date"]   = None
    df["is_current"] = True

    df["rank"]    = pd.to_numeric(df["rank"],    errors="coerce").astype("Int64")
    df["reviews"] = pd.to_numeric(df["reviews"], errors="coerce").astype("Int64")
    df["price"]   = pd.to_numeric(df["price"],   errors="coerce")
    df["rating"]  = pd.to_numeric(df["rating"],  errors="coerce")

    for c in ["rank", "reviews"]:
        df[c] = df[c].apply(lambda x: int(x) if pd.notna(x) else None)

    df = df.replace([np.inf, -np.inf], np.nan).where(pd.notnull(df), None)

    # Duplicate checks (optional logs)
    # -----------------------------------------------
    SAFE_COLS = ["asin", "category_name", "rank", "price", "reviews", "rating"]
    exact_dups = df[df.duplicated(subset=SAFE_COLS, keep=False)]
    if not exact_dups.empty:
        print(f"⚠️ {len(exact_dups)} exact duplicates (same category+asin+rank)")
    # -----------------------------------------------


    df_products = df[["asin", "title", "product_url", "image_url"]].drop_duplicates(subset="asin", keep="last")

    df_categories = (
        df[["asin","start_date","end_date","category_name","price","rank","is_current","reviews","rating"]]
        .sort_values(["category_name", "rank", "asin"])
        .drop_duplicates(subset=["category_name","asin"], keep="first")
        .drop_duplicates(subset=["category_name","rank"], keep="first")
    )

    products_records   = json.loads(df_products.to_json(orient="records"))
    categories_records = json.loads(df_categories.to_json(orient="records"))

    for rec in categories_records:
        for col in ("rank", "reviews"):
            val = rec.get(col)
            if isinstance(val, float) and val.is_integer():
                rec[col] = int(val)

    json.dumps(products_records,   allow_nan=False)
    json.dumps(categories_records, allow_nan=False)

    supabase.table("product_dim") \
        .upsert(products_records, on_conflict="asin", returning="minimal") \
        .execute()
    print("✅ product_dim upserted")

    # batch category_products inserts + minimal returning
    BATCH_SIZE = 800  # tune 500-1000; 800 is a good middle ground
    total = len(categories_records)
    done = 0
    for chunk in _chunked(categories_records, BATCH_SIZE):
        _insert_with_retries(
            supabase,
            table="category_products",
            records=chunk,
            returning="minimal",  
            max_retries=3,
            backoff_sec=1.5,
        )
        done += len(chunk)
        if done % 2000 == 0 or done == total:
            print(f"✅ category_products insert progress: {done}/{total}")
    print("✅ category_products insert complete")

    # Handle drop-offs
    for cat, grp in df_categories.groupby("category_name"):
        current_asins = grp["asin"].unique().tolist()
        in_clause = "(" + ",".join(f"\"{a}\"" for a in current_asins) + ")" if current_asins else "(\"\")"

        expired_rows = (
            supabase.table("category_products")
            .select("asin, price, reviews, rating")
            .eq("category_name", cat)
            .eq("is_current", True)
            .filter("asin", "not.in", in_clause)
            .filter("rank", "not.is", "null")
            .execute()
            .data
        )

        if expired_rows:
            supabase.table("category_products") \
                .update({"is_current": False, "end_date": scrape_ts}) \
                .eq("category_name", cat) \
                .eq("is_current", True) \
                .filter("asin", "not.in", in_clause) \
                .filter("rank", "not.is", "null") \
                .execute()

            placeholders = [{
                "asin": r["asin"],
                "category_name": cat,
                "rank": None,
                "price": r["price"],
                "reviews": r["reviews"],
                "rating": r["rating"],
                "start_date": scrape_ts,
                "end_date": None,
                "is_current": True,
            } for r in expired_rows]

            supabase.table("category_products").insert(placeholders).execute()
            print(f"✅ {len(placeholders)} placeholder rows added for drop-offs in '{cat}'")

    last_updated_records = [
        {"category_name": cat, "last_updated": scrape_ts}
        for cat in df_categories["category_name"].unique()
    ]

    supabase.table("category_last_updated") \
        .upsert(last_updated_records, on_conflict="category_name", returning="minimal") \
        .execute()
    print("✅ category_last_updated updated")
