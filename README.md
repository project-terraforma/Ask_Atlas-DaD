# Ask_Atlas-DaD
Project B: Dashboards Are Dead

## Team Members

Arpana Koilada and Veda Janga

## Project Description

This project explores how large geospatial datasets can be more accessible through natural language interaction instead of the traditional dashboards. Instead of relying on charts or fixed queries, our project represents Overture Maps data in a structured, text-based format which are designed specifically for Large Language Models (LLMs). Our goal is to provide a text file that allows an LLM to accurately answers questions about the data and to see how the way information is formatted and explained affects how well an LLM can understand complex data and avoid hallucinations.

# Ask Atlas 🗺️  
### Natural Language Q&A for Geospatial Data

![Python](https://img.shields.io/badge/Python-3.11-blue)
![DuckDB](https://img.shields.io/badge/Database-DuckDB-yellow)
![PostgreSQL](https://img.shields.io/badge/Database-PostgreSQL-blue)
![OpenAI](https://img.shields.io/badge/LLM-OpenAI-green)
![Ollama](https://img.shields.io/badge/LLM-Ollama-orange)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

Ask Atlas is a **natural language interface for querying geospatial data** from the **Overture Maps metrics dataset**.

The project explores **two different AI data architectures**:

1. **DuckDB + RAG pipeline**
2. **Neon PostgreSQL + LLM SQL generation**

and compares how well each approach answers analytical questions.

---

## Project Motivation

The **Overture Maps dataset** contains large-scale global geospatial data including:

- buildings  
- places  
- transportation networks  
- administrative divisions  
- land and water features  
- addresses  

While this data is powerful, it is difficult to explore without writing complex queries.

This project demonstrates how **LLMs can act as a natural language interface for geospatial analytics**.

---

## Example Questions

Users can ask questions such as:

```
What is the total number of address records?						
How many unique countries are represented in the addresses dataset?			
Which datasets or sources contribute the most address records, and what are their counts?
Which countries have the most address records with postal_city populated?
What is the count of address records per `change_type` (unchanged/added/removed/data_changed) for the latest release?
Top 5 `address_level_2` regions by total `id_count`?
Track total_geometry_area_km2 over releases for theme='base' AND type='water'.
```

The system translates these questions into **database queries and natural language answers**.

---

## Architecture Overview

The project explores **two different system designs**.

---

## DuckDB + RAG Pipeline

This approach focuses on **retrieval-augmented generation (RAG)** using local analytics.

### Workflow

```
Raw Metrics Data
        │
        ▼
Data Cleaning Pipeline (Python)
        │
        ▼
DuckDB Database
        │
        ▼
Retrieval Step
        │
        ▼
OpenAI API (LLM)
        │
        ▼
Natural Language Answer
```

### Key Components

- **DuckDB** for fast local analytics  
- **Parquet files** for efficient storage  
- **Python pipeline** for data cleaning  
- **OpenAI API** for natural language responses  

### Strengths

- Fast to prototype  
- Lightweight architecture  
- Efficient for broad dataset statistics  

### Limitations

- Harder to support complex filtering  
- Less effective for deep analytical queries  

---

## Neon PostgreSQL + LLM SQL Generation

The second system focuses on **structured query generation using an LLM**.

### Workflow

```
Raw Metrics Data
        │
        ▼
Data Exploration & Aggregation
        │
        ▼
Neon PostgreSQL Database
        │
        ▼
LLM (Llama3 via Ollama)
        │
Generate SQL Query
        ▼
Execute SQL Query
        │
        ▼
Query Results
        │
        ▼
Natural Language Answer
```

### Key Components

- **Neon PostgreSQL** cloud database  
- **SQLAlchemy** for database interaction  
- **Llama3 via Ollama** for SQL generation  
- Structured schema with query rules  

### Strengths

- Supports complex analytical queries  
- Allows filtering by:
  - country
  - region
  - category
  - time
- Produces more precise answers  

---

## Example Output

Below is an example interaction with the system.

### User Question

```
How many unique countries are represented in the addresses dataset?
```

### Generated SQL

```sql
SELECT COUNT(DISTINCT country) AS unique_countries FROM metrics_addresses_clean;

Query Result Sample:

   unique_countries
0                37
```

### Final Answer

```
There are 37 unique countries represented in the addresses dataset.
```

<p align="center">
  <img src="docs/Screenshot 2026-03-16 at 3.04.35 PM.jpg" width="800">
</p>

---

## Key Challenges

### Complex Dataset Structure

The dataset includes multiple dimensions:

- themes  
- feature types  
- subtypes  
- classes  
- monthly snapshots  

Designing a universal query system required careful schema understanding.

---

### Inconsistent Fields

Not all themes include the same metadata.

For example:

| Theme | Country Data |
|------|--------------|
| Places | Yes |
| Addresses | Yes |
| Buildings | No |
| Transportation | No |

This required **special query handling rules**.

---

### LLM Query Errors

LLMs occasionally generated:

- invalid columns  
- incorrect filters  
- unsupported query structures  

To address this we added:

- schema descriptions  
- SQL rules  
- example queries  
- automatic query correction logic  

---

### LLM Hallucinations

The model sometimes invented column names such as:

```
pc
```

instead of

```
place_countries
```

Guardrails were added to reduce hallucinated queries.

---

## Tech Stack

### Languages

- Python

### Databases

- DuckDB  
- Neon PostgreSQL  

### AI Models

- OpenAI API  
- Llama3 via Ollama  

### Libraries

- pandas  
- sqlalchemy  
- python-dotenv  
- duckdb  

---

## Running the Project

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

---

### 2. Create a `.env` file

```
OPENAI_API_KEY=your_api_key
NEON_URI=your_neon_database_uri
```

---

### 3. Run the pipeline

Example:

```bash
python rag_pipeline.py
```

---

## Key Takeaways

This project highlights how **LLMs can enable natural language interfaces for structured data systems**.

Key findings:

- **RAG pipelines** are fast and flexible for general queries  
- **SQL + LLM systems** perform better for analytical questions  
- Combining **LLMs with structured databases** produces more reliable answers  

---

## Future Improvements

Possible extensions include:

- adding visualization dashboards  
- integrating map-based outputs  
- supporting real-time geospatial queries  
- deploying the system as a web application  

---

## Contributors

- Arpana Koilada  
- Veda Janga  

---

## License

MIT License
