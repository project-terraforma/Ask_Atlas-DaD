import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
import os

load_dotenv("/Users/VedaJanga/Ask_Atlas-DaD/.env")
engine = sa.create_engine(os.getenv("NEON_URI"))

with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) FROM overture_metrics WHERE theme = 'places'"))
    print(f"Places rows before delete: {result.fetchone()[0]}")
    
    conn.execute(text("DELETE FROM overture_metrics WHERE theme = 'places'"))
    conn.commit()
    
    result = conn.execute(text("SELECT COUNT(*) FROM overture_metrics WHERE theme = 'places'"))
    print(f"Places rows after delete: {result.fetchone()[0]}")