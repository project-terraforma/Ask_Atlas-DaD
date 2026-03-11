import duckdb
import os
from dotenv import load_dotenv
import pandas as pd
from openai import OpenAI

load_dotenv()
# Initialize OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
# ----------------------------
# DuckDB Connection
# ----------------------------
con = duckdb.connect()

# ----------------------------
# Load Parquet Files as Views
# ----------------------------
DATA_FOLDER = "../"

TABLES = {}

print("\nLoading datasets...\n")

for file in os.listdir(DATA_FOLDER):

    if file.endswith(".parquet"):

        table_name = file.replace(".parquet", "")
        path = os.path.join(DATA_FOLDER, file)

        con.execute(f"""
        CREATE OR REPLACE VIEW {table_name} AS
        SELECT * FROM read_parquet('{path}')
        """)

        TABLES[table_name] = "dataset table"

        print(f"Loaded: {table_name}")

print("\nAvailable Tables:")
print(list(TABLES.keys()))

# ----------------------------
# Table Router
# ----------------------------
def route_question(question):

    table_list = "\n".join(TABLES.keys())

    prompt = f"""
You are a data router.

Choose the ONE table that best answers the question.

Available tables:
{table_list}

Return ONLY the table name.

Question:
{question}
"""

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    table = response.choices[0].message.content.strip()

    return table

# ----------------------------
# Get Table Schema
# ----------------------------
def get_table_schema(table):

    columns = con.execute(
        f"PRAGMA table_info({table})"
    ).fetchall()

    schema = f"\nTable: {table}\n"

    for col in columns:

        schema += f"{col[1]} ({col[2]})\n"

    return schema

# ----------------------------
# SQL Generator
# ----------------------------
def generate_sql(question):

    table = route_question(question)

    print("\nSelected Table:", table)

    schema = get_table_schema(table)

    prompt = f"""
You are a DuckDB SQL expert.

Table schema:
{schema}

Rules:
- Only use columns listed in the schema
- Do not invent columns
- Do not rename unrelated columns
- Country values use ISO codes like 'us', 'ca', 'gb'
- Use valid DuckDB SQL
- Use LIMIT when possible
- Return ONLY SQL
- Do NOT use markdown
- Do NOT use ``` blocks

Question:
{question}
"""

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    sql = response.choices[0].message.content.strip()

    # Remove markdown if the model adds it
    sql = sql.replace("```sql", "").replace("```", "").strip()

    return sql

# ----------------------------
# Run SQL Query
# ----------------------------
def run_sql(sql):

    try:

        df = con.execute(sql).df()

        total_rows = len(df)

        MAX_ROWS = 20

        sample = df.head(MAX_ROWS)

        return sample, total_rows

    except Exception as e:

        return f"SQL Error: {e}", 0

# ----------------------------
# Generate Clean Answer
# ----------------------------
def generate_answer(question, sql, result, total_rows):

    prompt = f"""
You are a data assistant.

Question:
{question}

Result:
{result}

Instructions:
- Answer in 1-2 sentences
- Focus only on the result
- Do NOT explain SQL
"""

    response = client.chat.completions.create(
        model="gpt-4.1-mini",
        messages=[{"role": "user", "content": prompt}]
    )

    return response.choices[0].message.content.strip()

# ----------------------------
# Main Pipeline
# ----------------------------
def ask(question):

    sql = generate_sql(question)

    print("\nGenerated SQL:\n")
    print(sql)

    result, total_rows = run_sql(sql)

    print("\nQuery Result Sample:\n")
    print(result)

    answer = generate_answer(question, sql, result, total_rows)

    return answer

# ----------------------------
# Interactive CLI
# ----------------------------
if __name__ == "__main__":

    print("\nAsk Atlas AI Data Assistant")
    print("Type 'exit' to quit\n")

    while True:

        question = input("Ask Atlas > ")

        if question.lower() in ["exit", "quit"]:

            print("\nGoodbye!\n")

            break

        answer = ask(question)

        print("\nAnswer:\n")
        print(answer)

        print("\n" + "-" * 50 + "\n")