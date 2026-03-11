import pandas as pd
from pymongo import MongoClient
import os

# ----------------------------------------
# 1. MongoDB Connection
# ----------------------------------------
MONGO_URI = "mongodb+srv://arpanak0505_db_user:Analytics123@atlas.uysnfbi.mongodb.net/?appName=Atlas"
client = MongoClient(MONGO_URI)

db = client["analytics_db"]

print("Connected to MongoDB.")

# ----------------------------------------
# 2. Parquet Files
# ----------------------------------------
FILES = {
    "release_summary": "release_summary.parquet",
    "country_summary": "country_summary.parquet",
    "feature_summary": "feature_summary.parquet",
    "change_summary": "change_summary.parquet"
}

# ----------------------------------------
# 3. Batch Insert Function
# ----------------------------------------
def insert_parquet(collection_name, file_path):

    print(f"\nProcessing {collection_name}...")

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    collection = db[collection_name]

    # Clear old data (full reload)
    collection.delete_many({})
    print("Old documents cleared.")

    df = pd.read_parquet(file_path)

    print(f"Rows to insert: {len(df)}")

    batch_size = 5000
    records = df.to_dict(orient="records")

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        collection.insert_many(batch)

    print(f"Inserted {len(records)} documents.")

# ----------------------------------------
# 4. Insert All Tables
# ----------------------------------------
for collection_name, file_path in FILES.items():
    insert_parquet(collection_name, file_path)

# ----------------------------------------
# 5. Create Indexes (IMPORTANT for RAG speed)
# ----------------------------------------
print("\nCreating indexes...")

db.release_summary.create_index([("release", 1), ("theme", 1)])
db.country_summary.create_index([("release", 1), ("theme", 1), ("country", 1)])
db.feature_summary.create_index([("release", 1), ("theme", 1), ("type", 1)])
db.change_summary.create_index([("release", 1), ("theme", 1), ("country", 1)])

print("Indexes created.")

print("\nLayer 4 complete. MongoDB is ready.")