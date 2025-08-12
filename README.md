# FKBB - Fastest Known BlueBike Times

A Hugo site that tracks the fastest recorded times between every pair of BlueBike stations in Boston.

## Features

- **Automated Data Collection**: GitHub Actions downloads the latest BlueBike trip data daily
- **Station Pages**: Individual pages for each station showing FKBB times to all destinations
- **Search**: Filter stations and destinations
- **Classic Bikes Only**: Analysis focuses on classic bike trips for fair comparison
- **Responsive Design**: Works on desktop and mobile

## How It Works

1. **Daily Data Fetch**: GitHub Actions runs daily at 6 AM UTC to download the latest data from the [official BlueBike S3 bucket](https://s3.amazonaws.com/hubway-data/index.html)
2. **Data Processing**: Python script filters for classic bike trips, calculates trip durations, and finds the fastest time between each station pair
3. **Site Generation**: Hugo generates static pages showing FKBB times for each station
4. **GitHub Pages**: Site is automatically deployed to GitHub Pages

## Data Processing

The system:
- Downloads the latest monthly trip data ZIP file
- Filters for `rideable_type=classic_bike` only
- Calculates trip duration from `started_at` to `ended_at`
- Filters out invalid trips (< 1 minute or > 24 hours)
- Groups by station pairs and finds minimum duration
- Generates Hugo content files and data

## Local Development

```bash
# Install dependencies
pip install pandas requests beautifulsoup4

# Process data (optional - for testing)
python scripts/minimal_test.py

# Build and serve site
hugo server
```

## Files Structure

- `scripts/process_data.py` - Main data processing script used by GitHub Actions
- `scripts/minimal_test.py` - Test script with smaller dataset
- `.github/workflows/hugo.yml` - GitHub Actions workflow
- `layouts/` - Hugo templates
- `content/` - Generated station pages
- `data/stations.json` - Processed station data

## GitHub Actions Setup

The workflow:
1. Downloads latest BlueBike data
2. Processes it with Python
3. Builds the Hugo site
4. Deploys to GitHub Pages

Make sure to enable GitHub Pages in your repository settings and set the source to "GitHub Actions".

## FKBB Format

Times are displayed as MM:SS (minutes:seconds). Each record shows:
- Destination station name
- Fastest time recorded
- Date when the fastest time was achieved
- Original ride ID for verification

---

*Data updated daily from official BlueBike trip data. Classic bike trips only.*
