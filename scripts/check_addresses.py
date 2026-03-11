import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
import os

load_dotenv("/Users/VedaJanga/Ask_Atlas-DaD/.env")
engine = sa.create_engine(os.getenv("NEON_URI"))

with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT DISTINCT country 
        FROM overture_metrics 
        WHERE theme = 'addresses'
        ORDER BY country
    """))
    for row in result:
        print(row[0])