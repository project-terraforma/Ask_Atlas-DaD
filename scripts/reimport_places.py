import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv

load_dotenv("/Users/VedaJanga/Ask_Atlas-DaD/.env")
engine = sa.create_engine(os.getenv("NEON_URI"))

base_path = "/Users/VedaJanga/Ask_Atlas-DaD/data/metrics/metrics"

SUM_COLS = [
    "total_count", "id_count", "names_count", "websites_count",
    "phones_count", "addresses_count", "brand_count"
]

for date_folder in sorted(os.listdir(base_path)):
    date_path = os.path.join(base_path, date_folder, "row_counts", "theme=places", "type=place")
    if not os.path.isdir(date_path):
        continue

    clean_date = date_folder.split(".")[0]

    for file in os.listdir(date_path):
        if not file.endswith(".csv"):
            continue

        df = pd.read_csv(os.path.join(date_path, file), on_bad_lines="skip")
        df.columns = df.columns.str.strip()
        df = df.dropna(how="all")

        group_cols = [c for c in ["place_countries", "primary_category", "change_type"] if c in df.columns]
        sum_cols = [c for c in SUM_COLS if c in df.columns]

        df[sum_cols] = df[sum_cols].fillna(0)
        df = df.groupby(group_cols, as_index=False)[sum_cols].sum()

        df["date"] = clean_date
        df["theme"] = "places"
        df["type"] = "place"

        for col in ["country", "subtype", "class", "subclass", "datasets"]:
            df[col] = None

        df.to_sql("overture_metrics", engine, if_exists="append",
                  index=False, method="multi", chunksize=500)
        print(f"✓ {clean_date} | places | {len(df)} rows")

print("\n✅ Places re-import done!")

# Verify
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM overture_metrics WHERE theme = 'places'"))
    print(f"Total places rows now: {result.fetchone()[0]}")