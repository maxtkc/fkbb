#!/usr/bin/env python3

import requests
import pandas as pd
import json
import os
from datetime import datetime
import zipfile
import io

def minimal_processing():
    """Process a tiny sample to test the full pipeline with station IDs"""
    # Use a smaller, older dataset for testing
    data_url = "https://s3.amazonaws.com/hubway-data/202401-bluebikes-tripdata.zip"
    
    print(f"Downloading data from: {data_url}")
    
    # Download the zip file
    response = requests.get(data_url)
    response.raise_for_status()
    
    # Extract and read CSV
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        csv_filename = csv_files[0]
        
        with zip_file.open(csv_filename) as csv_file:
            df = pd.read_csv(csv_file)
    
    # Filter for classic bikes only and sample to make it manageable for testing
    df = df[df['rideable_type'] == 'classic_bike'].copy()
    df = df.sample(n=min(10000, len(df)), random_state=42)  # Sample 10k rows for testing
    
    print(f"Working with {len(df)} classic bike trips")
    
    # Convert timestamps
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])
    
    # Calculate trip duration in minutes
    df['duration_minutes'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    
    # Filter out invalid trips and ensure we have station IDs
    df = df[
        (df['duration_minutes'] > 1) &  # At least 1 minute
        (df['duration_minutes'] < 1440) &  # Less than 24 hours
        df['start_station_name'].notna() &
        df['end_station_name'].notna() &
        df['start_station_id'].notna() &
        df['end_station_id'].notna()
    ].copy()
    
    print(f"After filtering: {len(df)} valid trips")
    
    # Calculate fastest times between station pairs using IDs
    fastest_times = df.groupby(['start_station_id', 'end_station_id'])['duration_minutes'].min().reset_index()
    fastest_times.rename(columns={'duration_minutes': 'fastest_time_minutes'}, inplace=True)
    
    print(f"Found {len(fastest_times)} unique station pairs")
    
    # Get ride details for fastest rides
    fastest_rides = df.loc[df.groupby(['start_station_id', 'end_station_id'])['duration_minutes'].idxmin()]
    fastest_rides = fastest_rides[['start_station_id', 'end_station_id', 'start_station_name', 'end_station_name', 'duration_minutes', 'started_at', 'ride_id']]
    
    # Merge to get complete data
    result = fastest_times.merge(
        fastest_rides,
        left_on=['start_station_id', 'end_station_id', 'fastest_time_minutes'],
        right_on=['start_station_id', 'end_station_id', 'duration_minutes'],
        how='left'
    )
    
    # Create stations data structure using IDs
    stations = {}
    for _, row in result.iterrows():
        start_station_id = str(row['start_station_id'])  # Ensure string for JSON keys
        end_station_id = str(row['end_station_id'])
        start_station_name = row['start_station_name']
        end_station_name = row['end_station_name']
        
        if start_station_id not in stations:
            stations[start_station_id] = {
                'id': start_station_id,
                'name': start_station_name,
                'destinations': {}
            }
        
        stations[start_station_id]['destinations'][end_station_id] = {
            'id': end_station_id,
            'name': end_station_name,
            'fastest_time_minutes': row['fastest_time_minutes'],
            'fastest_time_formatted': f"{int(row['fastest_time_minutes'])}:{int((row['fastest_time_minutes'] % 1) * 60):02d}",
            'ride_id': row['ride_id'],
            'date': row['started_at'].strftime('%Y-%m-%d %H:%M:%S')
        }
    
    print(f"Created data for {len(stations)} stations")
    
    # Create directories
    os.makedirs('content/stations', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    
    # Save station data as JSON
    with open('data/stations.json', 'w') as f:
        json.dump(stations, f, indent=2)
    
    # Create individual station pages for ALL stations using IDs
    station_count = 0
    for station_id, station_data in stations.items():
        station_name = station_data['name']
        
        content = f'''---
title: "{station_name}"
station_id: "{station_id}"
station_name: "{station_name}"
type: "station"
---

# FKBB Times from {station_name}

This page shows the Fastest Known BlueBike (FKBB) times from **{station_name}** to all other stations.

'''
        
        with open(f'content/stations/{station_id.lower()}.md', 'w') as f:
            f.write(content)
        
        station_count += 1
    
    # Create index page
    index_content = f'''---
title: "Fastest Known BlueBike (FKBB) Times"
---

# Fastest Known BlueBike (FKBB) Times

Welcome to the FKBB tracker! This site shows the fastest recorded times between every pair of BlueBike stations.

## Test Data

This is a test version using a sample of January 2024 data ({len(df)} trips processed).

Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
'''
    
    with open('content/_index.md', 'w') as f:
        f.write(index_content)
    
    print("✓ Test processing completed successfully!")
    print(f"Created {station_count} station pages using station IDs")

if __name__ == "__main__":
    minimal_processing()