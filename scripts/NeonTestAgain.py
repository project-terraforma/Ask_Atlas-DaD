import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
import os
load_dotenv()

uri = os.getenv("NEON_URI")
engine = sa.create_engine(uri)

with engine.connect() as conn:
    # Total rows
    total = conn.execute(text("SELECT COUNT(*) FROM overture_metrics")).fetchone()[0]
    print(f"Total rows: {total}")

    # Breakdown by date
    print("\nRows per date:")
    rows = conn.execute(text("SELECT date, COUNT(*) as count FROM overture_metrics GROUP BY date ORDER BY date"))
    for row in rows:
        print(f"  {row[0]} → {row[1]} rows")

    # Breakdown by theme
    print("\nRows per theme:")
    rows = conn.execute(text("SELECT theme, COUNT(*) as count FROM overture_metrics GROUP BY theme ORDER BY theme"))
    for row in rows:
        print(f"  {row[0]} → {row[1]} rows")