import ollama
import sqlalchemy as sa
from sqlalchemy import text
from dotenv import load_dotenv
import os

load_dotenv("/Users/VedaJanga/Ask_Atlas-DaD/.env")
engine = sa.create_engine(os.getenv("NEON_URI"))

REGION_TO_COUNTRIES = {
    "africa": ["DZ","AO","BJ","BW","BF","BI","CM","CV","CF","TD","KM","CG","CD","DJ","EG",
               "GQ","ER","ET","GA","GM","GH","GN","GW","CI","KE","LS","LR","LY","MG","MW",
               "ML","MR","MU","MA","MZ","NA","NE","NG","RW","ST","SN","SL","SO","ZA","SS",
               "SD","SZ","TZ","TG","TN","UG","ZM","ZW"],
    "europe": ["AL","AD","AT","BY","BE","BA","BG","HR","CY","CZ","DK","EE","FI","FR","DE",
               "GR","HU","IS","IE","IT","XK","LV","LI","LT","LU","MT","MD","MC","ME","NL",
               "MK","NO","PL","PT","RO","RU","SM","RS","SK","SI","ES","SE","CH","UA","GB","VA"],
    "asia": ["AF","AM","AZ","BH","BD","BT","BN","KH","CN","GE","IN","ID","IR","IQ","IL",
             "JP","JO","KZ","KW","KG","LA","LB","MY","MV","MN","MM","NP","KP","OM","PK",
             "PS","PH","QA","SA","SG","KR","LK","SY","TW","TJ","TH","TL","TR","TM","AE","UZ","VN","YE"],
    "north america": ["AG","BS","BB","BZ","CA","CR","CU","DM","DO","SV","GD","GT","HT",
                      "HN","JM","MX","NI","PA","KN","LC","VC","TT","US"],
    "south america": ["AR","BO","BR","CL","CO","EC","GY","PY","PE","SR","UY","VE"],
    "oceania": ["AU","FJ","KI","MH","FM","NR","NZ","PW","PG","WS","SB","TO","TV","VU"],
    "middle east": ["BH","IR","IQ","IL","JO","KW","LB","OM","PS","QA","SA","SY","AE","YE"],
    "latin america": ["AR","BO","BR","CL","CO","CR","CU","DO","EC","SV","GT","HT","HN",
                      "MX","NI","PA","PY","PE","UY","VE"],
    "southeast asia": ["BN","KH","TL","ID","LA","MY","MM","PH","SG","TH","VN"],
    "central asia": ["KZ","KG","TJ","TM","UZ"],
    "east africa": ["BI","KM","DJ","ER","ET","KE","MG","MW","MU","MZ","RW","SO","SS","TZ","UG","ZM","ZW"],
    "west africa": ["BJ","BF","CV","CI","GM","GH","GN","GW","LR","ML","MR","NE","NG","SN","SL","TG"],
}

SCHEMA = """
You have access to a PostgreSQL table called overture_metrics with these columns:
- date (TEXT): monthly snapshot, values: '2025-01-22', '2025-02-19', '2025-03-19', '2025-04-23', '2025-05-21', '2025-06-25', '2025-07-23', '2025-08-20', '2025-09-24'
- theme (TEXT): 'addresses', 'base', 'buildings', 'divisions', 'places', 'transportation'
- type (TEXT): e.g. 'building', 'place', 'segment', 'division', 'address', 'land', 'water'
- change_type (TEXT): 'added' or 'removed'
- country (TEXT): UPPERCASE ISO 2-letter country code, ONLY for divisions and addresses themes e.g. 'US', 'DE', 'GB'
- subtype (TEXT): feature subtype
- class (TEXT): feature class
- subclass (TEXT): feature subclass (transportation only)
- place_countries (TEXT): UPPERCASE ISO 2-letter country code, ONLY for places theme e.g. 'US', 'DE', 'GB'
- primary_category (TEXT): category of place e.g. 'restaurant', 'park', 'school', 'hospital'
- total_count (BIGINT): total number of features
- id_count (BIGINT): number of unique IDs
- total_geometry_area_km2 (FLOAT): total area in km²
- total_geometry_length_km (FLOAT): total length in km
- names_count (BIGINT): features with names
- height_count (BIGINT): features with height data
- num_floors_count (BIGINT): features with floor count
- population_count (BIGINT): population data (divisions only)
- speed_limits_count (BIGINT): road segments with speed limits
- routes_count (BIGINT): route count
- websites_count (BIGINT): places with websites
- phones_count (BIGINT): places with phone numbers
- addresses_count (BIGINT): places with addresses
- brand_count (BIGINT): places with brand info
- is_disputed_count (BIGINT): disputed boundaries
- postcode_count (BIGINT): addresses with postcodes
- street_count (BIGINT): addresses with street names
- number_count (BIGINT): addresses with street numbers

CRITICAL NOTES:
- country column ONLY exists for divisions and addresses themes — do NOT use it for buildings, base, or transportation
- place_countries ONLY exists for places theme
- Buildings, base, and transportation have NO country breakdown — only global totals by subtype/class
- Country codes are stored in UPPERCASE e.g. 'US', 'DE', 'JP', 'GB'
- Always convert full country names to UPPERCASE ISO 2-letter codes
- theme, type, change_type values are lowercase e.g. 'places', 'buildings', 'added'
- type is NEVER a category like 'restaurant' or 'hospital' — use primary_category for categories
- For places, type is always 'place' — use primary_category for filtering by category
- Data covers January 2025 through September 2025 only
- If country codes are provided in the question notes, use them in an IN clause
"""

EXAMPLES = """
  Places (supports country/region filtering)
  • How many places are in the US?
  • How many restaurants are in Europe?
  • How many hospitals were added in Africa in 2025?
  • How did the number of places in Japan change over time?

  Divisions (supports country filtering)
  • How many divisions are there in Germany?
  • How has the number of divisions in Asia changed over time?

  Addresses (supports country filtering)
  • How many addresses are there in France?
  • Which country has the most addresses?
  • #How many addresses were added in Brazil?

  Buildings (global totals only — no country breakdown)
  • How many buildings were added in June 2025?
  • Which month had the most buildings added?
  • How has the total number of buildings changed over time?
  • How many buildings have height data?

  Roads & Transportation (global totals only)
  • How has the total number of road segments changed over time?
  • Which month saw the most road segments added?
  • How many roads have speed limit data?

  Land & Geography (global totals only)
  • How much total water area is in the dataset?
  • How has land use coverage changed over time?
  • What is the total area of land cover in the dataset?

  Note: Places, divisions, and addresses support country/region filtering.
        Buildings, transportation, and base are global totals only.
"""

def expand_regions(question):
    q_lower = question.lower()
    for region, codes in REGION_TO_COUNTRIES.items():
        if region in q_lower:
            codes_str = ", ".join(f"'{c.upper()}'" for c in codes)
            question = question + f"\n\n[Note: '{region}' refers to these UPPERCASE country codes: {codes_str}]"
            break
    return question

def fix_sql(sql):
    """Auto-fix common column mistakes from the model."""
    sql_stripped = sql.replace(" ", "").lower()
    if "place_countries" in sql and ("theme='addresses'" in sql_stripped or "theme='divisions'" in sql_stripped):
        sql = sql.replace("place_countries", "country")
    if "theme='places'" in sql_stripped and "place_countries" not in sql:
        sql = sql.replace("country =", "place_countries =")
        sql = sql.replace("country=", "place_countries =")
        sql = sql.replace("country IN", "place_countries IN")
    return sql

def ask(question):
    question = expand_regions(question)

    # Step 1: Generate SQL
    sql_response = ollama.chat(
        model="llama3",
        messages=[{
            "role": "user",
            "content": f"""{SCHEMA}

Write a single PostgreSQL SELECT query to answer this question:
"{question}"

Rules:
- Return ONLY the SQL query, no explanation, no markdown, no backticks
- ALWAYS use SUM(total_count) to get the actual feature count, NEVER use COUNT(*)
- Country codes must be UPPERCASE in WHERE clauses e.g. 'US', 'DE', 'JP'
- Always convert full country names to UPPERCASE ISO 2-letter codes
- theme, type, change_type values must be lowercase in WHERE clauses
- CRITICAL: places theme uses place_countries for country filtering, SELECT place_countries for country name
- CRITICAL: divisions and addresses themes use country for country filtering, SELECT country for country name
- CRITICAL: never use place_countries for divisions or addresses
- CRITICAL: never use country for places
- type is NEVER a category — always use primary_category for filtering by category e.g. primary_category = 'restaurant'
- Never use country or place_countries for buildings, base, or transportation themes
- For overall/total questions filter by date = '2025-09-24'
- Always add LIMIT 10 to queries returning multiple rows
- If asking about trends over time GROUP BY date ORDER BY date and do NOT filter by a single date
- If country codes are provided in the question notes use them in an IN clause
- Only use column names that exist in the schema above, never invent aliases or shorthand
- When using SELECT date with SUM(), always include GROUP BY date in the outer query
- Water is stored as theme = 'base' AND type = 'water', there is no theme called 'water'
- Land is stored as theme = 'base' AND type = 'land'
- Land cover is stored as theme = 'base' AND type = 'land_cover'
- Land use is stored as theme = 'base' AND type = 'land_use'
- Infrastructure is stored as theme = 'base' AND type = 'infrastructure'
- For area questions use SUM(total_geometry_area_km2) not SUM(total_count)
- Roads are stored as type = 'segment' not type = 'road'
- For questions about speed limits use SUM(speed_limits_count) not SUM(total_count)
- For questions about names use SUM(names_count)
- For questions about height use SUM(height_count)
- For questions about floors use SUM(num_floors_count)
- For questions about population use SUM(population_count)
"""
        }]
    )

    sql = sql_response["message"]["content"].strip()

    if "```" in sql:
        sql = sql.split("```")[1]
        if sql.startswith("sql"):
            sql = sql[3:]
        sql = sql.strip()

    sql = fix_sql(sql)
    sql = sql.replace(";\nLIMIT", "\nLIMIT").replace("; LIMIT", " LIMIT").rstrip(";")

    # Step 2: Run the query
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = result.fetchall()
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in rows]
    except Exception as e:
        return f"❌ SQL Error: {e}\nGenerated SQL was: {sql}"

    if not data or all(v is None for row in data for v in row.values()):
        return "I couldn't find data for that query. Type 'examples' to see what you can ask."

    # Calculate trend direction in Python so the model can't get it wrong
    trend_hint = ""
    if len(data) > 1:
        keys = list(data[0].keys())
        numeric_keys = [k for k in keys if k != "date" and data[0][k] is not None]
        if numeric_keys:
            first_val = data[0][numeric_keys[0]]
            last_val = data[-1][numeric_keys[0]]
            if first_val is not None and last_val is not None and isinstance(first_val, (int, float)) and isinstance(last_val, (int, float)):
                if last_val > first_val:
                    trend_hint = f"\n[Note: The value INCREASED from {first_val:,.0f} to {last_val:,.0f}]"
                elif last_val < first_val:
                    trend_hint = f"\n[Note: The value DECREASED from {first_val:,.0f} to {last_val:,.0f}]"
                else:
                    trend_hint = f"\n[Note: The value stayed the SAME at {first_val:,.0f}]"

    # Step 3: Generate natural language answer
    answer_response = ollama.chat(
        model="llama3",
        messages=[{
            "role": "user",
            "content": f"""The user asked: "{question}"

The database returned this data:
{data}
{trend_hint}

Write a clear single sentence answer based on this data.
Format large numbers with commas.
Do not use bullet points, headers, or speculation about why the data is the way it is.
Just state the facts from the data directly.
For questions asking "which country" or "which month", state the actual value from the data directly.
If describing a trend, use the trend direction from the Note above — do not calculate it yourself.
"""
        }]
    )

    return answer_response["message"]["content"].strip()


def main():
    print("=" * 52)
    print("        Ask Atlas — Geospatial Data Q&A")
    print("=" * 52)
    print(EXAMPLES)
    print("=" * 52)
    print("Type 'examples' to see example questions again.")
    print("Type 'quit' to exit.\n")

    while True:
        question = input("You: ").strip()
        if not question:
            continue
        if question.lower() in ["quit", "exit", "q"]:
            print("Goodbye!")
            break
        if question.lower() == "examples":
            print(EXAMPLES)
            continue
        print("\nThinking...")
        answer = ask(question)
        print(f"\nAtlas: {answer}\n")
        print("-" * 52)


if __name__ == "__main__":
    main()