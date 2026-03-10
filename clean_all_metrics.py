import duckdb
import os
import glob
import re

# --------------------------------------------------
# 1. Connect to DuckDB
# --------------------------------------------------
con = duckdb.connect("analytics.db")

BASE_PATH = "/Users/arpanakoilada/Downloads/data_pipline/raw_data/Metrics"
METRICS_PATH = f"{BASE_PATH}/metrics"

print("Scanning for releases...")

# --------------------------------------------------
# 2. Detect Releases Dynamically
# --------------------------------------------------
release_paths = glob.glob(f"{METRICS_PATH}/*")
releases = []

for path in release_paths:
    folder = os.path.basename(path)
    if re.match(r"\d{4}-\d{2}-\d{2}", folder):
        releases.append(folder)

releases = sorted(releases)
print("Found releases:", releases)

# --------------------------------------------------
# 3. Create Final Summary Tables (Empty Initially)
# --------------------------------------------------
con.execute("""
CREATE TABLE IF NOT EXISTS release_summary (
    release VARCHAR,
    theme VARCHAR,
    total_objects BIGINT,
    total_population BIGINT
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS country_summary (
    release VARCHAR,
    theme VARCHAR,
    country VARCHAR,
    total_objects BIGINT,
    total_population BIGINT
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS feature_summary (
    release VARCHAR,
    theme VARCHAR,
    type VARCHAR,
    subtype VARCHAR,
    class VARCHAR,
    total_objects BIGINT
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS change_summary (
    release VARCHAR,
    theme VARCHAR,
    country VARCHAR,
    change_type VARCHAR,
    total_objects BIGINT
);
""")

# --------------------------------------------------
# 4. Process ONE release at a time (memory safe)
# --------------------------------------------------
for release in releases:

    print(f"\nProcessing release: {release}")

    theme_paths = glob.glob(
    f"{METRICS_PATH}/{release}/row_counts/theme=*"
    )

    themes = []

    for path in theme_paths:
        folder_name = os.path.basename(path)
        theme_name = folder_name.replace("theme=", "")

        # Skip metadata / partition artifacts
        if "$" in theme_name:
            continue

        themes.append(theme_name)

    themes = sorted(list(set(themes)))
    print("Themes:", themes)

    for theme in themes:

        print(f"  Processing theme: {theme}")

        # --------------------------------------------------
        # 4A. Load raw CSV without assumptions
        # --------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE temp_source AS
            SELECT *
            FROM read_csv_auto(
                '{METRICS_PATH}/{release}/row_counts/theme={theme}/type=*/part-*.csv',
                HEADER=TRUE,
                union_by_name=TRUE
            );
        """)

        # Detect existing columns
        columns = con.execute("PRAGMA table_info(temp_source)").fetchall()
        existing_cols = {col[1] for col in columns}

        def text_col(col):
            if col in existing_cols:
                return f"LOWER(TRIM({col})) AS {col}"
            else:
                return f"NULL AS {col}"

        def numeric_col(col):
            if col in existing_cols:
                return f"COALESCE({col}, 0) AS {col}"
            else:
                return f"0 AS {col}"

        # --------------------------------------------------
        # 4B. Build cleaned temp_raw safely
        # --------------------------------------------------
        con.execute(f"""
            CREATE OR REPLACE TABLE temp_raw AS
            SELECT
                '{release}' AS release,
                '{theme}' AS theme,
                {text_col('country')},
                {text_col('type')},
                {text_col('subtype')},
                {text_col('class')},
                {text_col('change_type')},
                {numeric_col('total_count')},
                {numeric_col('population_count')}
            FROM temp_source;
        """)

        con.execute("DROP TABLE temp_source;")

        # --------------------------------------------------
        # 5. Aggregations
        # --------------------------------------------------

        # Release Summary
        con.execute("""
            INSERT INTO release_summary
            SELECT
                release,
                theme,
                SUM(total_count),
                SUM(population_count)
            FROM temp_raw
            GROUP BY release, theme;
        """)

        # Country Summary
        con.execute("""
            INSERT INTO country_summary
            SELECT
                release,
                theme,
                country,
                SUM(total_count),
                SUM(population_count)
            FROM temp_raw
            WHERE country IS NOT NULL
            GROUP BY release, theme, country;
        """)

        # Feature Summary
        con.execute("""
            INSERT INTO feature_summary
            SELECT
                release,
                theme,
                type,
                subtype,
                class,
                SUM(total_count)
            FROM temp_raw
            GROUP BY release, theme, type, subtype, class;
        """)

        # Change Summary
        con.execute("""
            INSERT INTO change_summary
            SELECT
                release,
                theme,
                country,
                change_type,
                SUM(total_count)
            FROM temp_raw
            WHERE change_type IS NOT NULL
            GROUP BY release, theme, country, change_type;
        """)

        # --------------------------------------------------
        # Drop temporary table immediately
        # --------------------------------------------------
        con.execute("DROP TABLE temp_raw;")

        print("    Aggregated safely.")

# --------------------------------------------------
# 6. Export ONLY Summary Tables
# --------------------------------------------------
print("\nExporting summary tables...")

con.execute("COPY release_summary TO 'release_summary.parquet' (FORMAT PARQUET);")
con.execute("COPY country_summary TO 'country_summary.parquet' (FORMAT PARQUET);")
con.execute("COPY feature_summary TO 'feature_summary.parquet' (FORMAT PARQUET);")
con.execute("COPY change_summary TO 'change_summary.parquet' (FORMAT PARQUET);")

print("Export complete.")
print("\nOnly aggregated data exported.")