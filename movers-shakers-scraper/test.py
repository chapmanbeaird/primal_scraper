import asyncio
import traceback
from utils.scrape import run_scrape_job
from utils.supabase_io import add_to_supabase
from utils.s3_io import load_csv_from_s3


def main():
    try:
        s3_df = load_csv_from_s3(bucket="movers-shakers-urls", key="amazon_movers_and_shakers_urls.csv")
        urls = s3_df["url"].tolist()
        df = asyncio.run(run_scrape_job(urls=urls))
        add_to_supabase(df)
    except Exception as e:
        print(f"FATAL: {e}")
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()