# Data Exploration Findings

## Data Format
- Files are CSV, not parquet
- Located in: `data/metrics/metrics/[release-date]/row_counts/`
- Structure: `theme=[theme]/type=[type]/part-*.csv`

## Themes
- **places**: Points of interest (restaurants, hotels, etc.)
- **addresses**: Address data
- **buildings**: Building footprints
- **divisions**: Administrative boundaries
- **base**: Infrastructure, land, water
- **transportation**: Roads and connectors

## Places Data Structure
Key columns:
- `datasets`: Source (meta, Microsoft, Foursquare)
- `place_countries`: ISO country code
- `primary_category`: Type of place
- `confidence`: 0.3-1.0 confidence score
- `change_type`: data_changed, added, unchanged, removed
- `*_count`: Various counts (id_count, names_count, etc.)

## Key Findings
- Most recent release: 2025-09-24.0
- Must filter out `change_type = 'removed'` for current counts
- Data is pre-aggregated by country + category + dataset
- Multiple datasets per country (meta, Microsoft, Foursquare)

## Next Steps
1. Focus on places theme for this project
2. Use 2025-09-24.0 release
3. Create aggregation script to calculate country/category totals
4. Build ground truth for evaluation questions
