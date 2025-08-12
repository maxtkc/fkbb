# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a Hugo-based static site that tracks Fastest Known BlueBike (FKBB) times between Boston BlueBike stations. The project automatically downloads daily BlueBike trip data, processes it to find fastest times between station pairs, and generates static pages showing these records.

## Development Commands

### Local Development
```bash
# Install Python dependencies
pip install pandas requests beautifulsoup4

# Process data locally (uses sample data)
python scripts/minimal_test.py

# Build and serve Hugo site locally
hugo server

# Build production site
hugo --gc --minify
```

### Testing Scripts
- `python scripts/minimal_test.py` - Process small sample dataset for testing
- `python scripts/test_data_processing.py` - Run data processing tests
- `python scripts/process_data.py` - Main production data processing (downloads latest data)

## Architecture

### Data Flow
1. **Data Ingestion**: `scripts/process_data.py` downloads latest BlueBike data from S3 bucket
2. **Processing**: Filters for classic bike trips, calculates durations, finds minimum times per station pair
3. **Content Generation**: Creates Hugo content files (`content/stations/*.md`) and data file (`data/stations.json`)
4. **Site Build**: Hugo generates static HTML from templates in `layouts/`

### Key Files
- `scripts/process_data.py` - Main data processing pipeline used by GitHub Actions
- `scripts/minimal_test.py` - Local testing with sample data
- `hugo.toml` - Hugo site configuration
- `data/stations.json` - Processed station data with FKBB times
- `layouts/_default/baseof.html` - Base HTML template with embedded CSS and search functionality
- `layouts/stations/` - Station-specific page templates
- `.github/workflows/hugo.yml` - Automated daily data processing and deployment

### Data Structure
- Station pages: `content/stations/{station_id}.md` - Individual station FKBB pages
- Station data: `data/stations.json` - JSON with station info and fastest times to all destinations
- Each record includes: destination station, fastest time (MM:SS), date achieved, ride ID

### Hugo Template System
- Uses Hugo's data files feature to populate station pages
- Custom CSS embedded in base template for responsive design
- JavaScript search functionality for filtering station tables
- Station pages auto-generated from data processing scripts

## GitHub Actions Workflow

The site automatically updates daily via GitHub Actions:
1. Downloads latest BlueBike trip data (6 AM UTC daily)
2. Processes data with Python scripts
3. Builds Hugo site
4. Deploys to GitHub Pages

Workflow file: `.github/workflows/hugo.yml`
Hugo version: 0.127.0
Python version: 3.11

## BlueBike Raw Data Formats

The BlueBike system has used different CSV column formats over time. The data processing scripts handle these automatically through format detection and column mapping.

### Legacy Format (May 2018 - December 2023)
**Detection:** Absence of `rideable_type` column  
**Files:** `YYYYMM-bluebikes-tripdata.csv` where YYYYMM is 201805 through 202312

**Columns:**
```
tripduration,starttime,stoptime,start station id,start station name,start station latitude,start station longitude,end station id,end station name,end station latitude,end station longitude,bikeid,usertype,birth year,gender
```

**Key characteristics:**
- All bikes considered "classic" (no electric bikes)
- Duration provided in seconds as `tripduration`
- Spaces in column names (e.g., "start station id")
- Coordinates as "start station latitude/longitude"
- User demographics included (birth year, gender until ~2020, then postal code)
- No `ride_id` - created from `bikeid + starttime`

**Column mapping to modern format:**
```python
{
    'starttime': 'started_at',
    'stoptime': 'ended_at', 
    'start station id': 'start_station_id',
    'start station name': 'start_station_name',
    'start station latitude': 'start_lat',
    'start station longitude': 'start_lng',
    'end station id': 'end_station_id',
    'end station name': 'end_station_name', 
    'end station latitude': 'end_lat',
    'end station longitude': 'end_lng'
}
```

### Modern Format (January 2024+)
**Detection:** Presence of `rideable_type` column  
**Files:** `YYYYMM-bluebikes-tripdata.csv` where YYYYMM is 202401 onwards

**Columns:**
```
ride_id,rideable_type,started_at,ended_at,start_station_name,start_station_id,end_station_name,end_station_id,start_lat,start_lng,end_lat,end_lng,member_casual
```

**Key characteristics:**
- Includes electric bikes - filtered to `rideable_type == 'classic_bike'`
- No `tripduration` - calculated from `ended_at - started_at`
- Underscores in column names (e.g., "start_station_id")
- Coordinates as "start_lat/start_lng"
- Simplified user classification (`member_casual` instead of detailed demographics)
- Has unique `ride_id`

### Data Processing Logic

```python
# Format detection
if 'rideable_type' in df.columns:
    # Modern format (2024+): filter for classic bikes
    df = df[df['rideable_type'] == 'classic_bike'].copy()
else:
    # Legacy format (2018-2023): all bikes are classic
    # Apply column renaming
    df = df.rename(columns=legacy_column_mapping)
    # Generate ride_id from bikeid + starttime
    df['ride_id'] = df['bikeid'].astype(str) + '_' + df['started_at'].astype(str)

# Duration calculation  
if 'tripduration' in df.columns:
    # Legacy: convert seconds to minutes
    df['duration_minutes'] = df['tripduration'] / 60
else:
    # Modern: calculate from timestamps
    df['duration_minutes'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
```

### Demographic Data Evolution

- **2018-2020:** `birth year` and `gender` columns
- **2021-2023:** `postal code` replaces demographics  
- **2024+:** `member_casual` classification only

### Caching System

Raw data files are cached in:
- `cache/zip/` - Original downloaded zip files
- `cache/csv/` - Extracted CSV files for faster processing

Cache automatically speeds up subsequent processing runs and includes validation.
- You can use jq, playwright
- You can use gh cli to deploy as needed, and write commits frequently
- write git commits as we go