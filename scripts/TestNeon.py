from dotenv import load_dotenv
import os
import pandas as pd
import sqlalchemy as sa
from sqlalchemy import text

load_dotenv()
uri = os.getenv("NEON_URI")

engine = sa.create_engine(uri)
base_path = "/Users/VedaJanga/Ask_Atlas-DaD/data/metrics/metrics"

GROUP_BY = {
    "addresses|address":           ["country", "change_type"],
    "base|bathymetry":             ["change_type"],
    "base|infrastructure":         ["subtype", "class", "change_type"],
    "base|land":                   ["subtype", "class", "change_type"],
    "base|land_cover":             ["subtype", "change_type"],
    "base|land_use":               ["subtype", "class", "change_type"],
    "base|water":                  ["subtype", "class", "change_type"],
    "buildings|building":          ["subtype", "class", "change_type"],
    "buildings|building_part":     ["change_type"],
    "divisions|division":          ["country", "subtype", "class", "change_type"],
    "divisions|division_area":     ["country", "subtype", "class", "change_type"],
    "divisions|division_boundary": ["subtype", "class", "change_type"],
    "places|place":                ["place_countries", "change_type"],
    "transportation|connector":    ["change_type"],
    "transportation|segment":      ["subtype", "class", "subclass", "change_type"],
}

SUM_COLS = [
    "total_count", "id_count", "total_geometry_area_km2",
    "total_geometry_length_km", "names_count", "height_count",
    "num_floors_count", "population_count", "speed_limits_count",
    "routes_count", "websites_count", "phones_count",
    "addresses_count", "brand_count", "is_disputed_count",
    "postcode_count", "street_count", "number_count"
]

# Create table if it doesn't exist
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS overture_metrics (
            id SERIAL PRIMARY KEY,
            date TEXT,
            theme TEXT,
            type TEXT,
            change_type TEXT,
            country TEXT,
            subtype TEXT,
            class TEXT,
            subclass TEXT,
            place_countries TEXT,
            primary_category TEXT,
            datasets TEXT,
            total_count BIGINT,
            id_count BIGINT,
            total_geometry_area_km2 FLOAT,
            total_geometry_length_km FLOAT,
            names_count BIGINT,
            height_count BIGINT,
            num_floors_count BIGINT,
            population_count BIGINT,
            speed_limits_count BIGINT,
            routes_count BIGINT,
            websites_count BIGINT,
            phones_count BIGINT,
            addresses_count BIGINT,
            brand_count BIGINT,
            is_disputed_count BIGINT,
            postcode_count BIGINT,
            street_count BIGINT,
            number_count BIGINT
        )
    """))
    conn.commit()
    print("Table ready!")

# Check already imported combos
with engine.connect() as conn:
    result = conn.execute(text("SELECT DISTINCT date, theme, type FROM overture_metrics"))
    already_done = set((row[0], row[1], row[2]) for row in result)
print(f"Found {len(already_done)} already imported combos")

all_dfs = []

for date_folder in sorted(os.listdir(base_path)):
    date_path = os.path.join(base_path, date_folder, "row_counts")
    if not os.path.isdir(date_path):
        continue

    for theme_folder in sorted(os.listdir(date_path)):
        if theme_folder.endswith("$folder$") or not os.path.isdir(os.path.join(date_path, theme_folder)):
            continue

        theme = theme_folder.replace("theme=", "")
        theme_path = os.path.join(date_path, theme_folder)

        for type_folder in sorted(os.listdir(theme_path)):
            if type_folder.endswith("$folder$") or not os.path.isdir(os.path.join(theme_path, type_folder)):
                continue

            type_ = type_folder.replace("type=", "")
            type_path = os.path.join(theme_path, type_folder)
            clean_date = date_folder.split(".")[0]

            if (clean_date, theme, type_) in already_done:
                print(f"⏭ Skipping {clean_date} | {theme} | {type_}")
                continue

            for file in os.listdir(type_path):
                if not file.endswith(".csv"):
                    continue

                df = pd.read_csv(os.path.join(type_path, file), on_bad_lines="skip")

                # Clean
                df.columns = df.columns.str.strip()
                df = df.dropna(how="all")

                # Aggregate
                key = f"{theme}|{type_}"
                group_cols = [c for c in GROUP_BY.get(key, ["change_type"]) if c in df.columns]
                sum_cols = [c for c in SUM_COLS if c in df.columns]

                if group_cols:
                    df[sum_cols] = df[sum_cols].fillna(0)
                    df = df.groupby(group_cols, as_index=False)[sum_cols].sum()
                else:
                    df = df[sum_cols].sum().to_frame().T

                df["date"] = clean_date
                df["theme"] = theme
                df["type"] = type_

                # Add missing columns as None
                for col in ["country", "subtype", "class", "subclass", 
                            "place_countries", "primary_category", "datasets"]:
                    if col not in df.columns:
                        df[col] = None

                # Insert into Neon
                df.to_sql("overture_metrics", engine, if_exists="append", index=False,
                          method="multi", chunksize=500)

                print(f"✓ {clean_date} | {theme} | {type_} → {len(df)} docs")

print("\n✅ All done!")