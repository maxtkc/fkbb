#!/usr/bin/env python3

import requests
import pandas as pd
import json
import os
from datetime import datetime
import zipfile
import io
import uuid
import hashlib

def haversine_distance(lat1, lng1, lat2, lng2):
    """Calculate the great-circle distance between two points on Earth (in km)"""
    import math
    
    # Convert latitude and longitude from degrees to radians
    lat1, lng1, lat2, lng2 = map(math.radians, [lat1, lng1, lat2, lng2])
    
    # Haversine formula
    dlat = lat2 - lat1
    dlng = lng2 - lng1
    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlng/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    # Radius of earth in kilometers
    r = 6371
    
    return c * r

def generate_station_uuid(lat, lng, precision=6):
    """Generate deterministic UUID based on lat/lng coordinates"""
    # Round coordinates to specified precision to handle minor GPS variations
    rounded_lat = round(float(lat), precision)
    rounded_lng = round(float(lng), precision)
    
    # Create a deterministic UUID based on coordinates
    coordinate_string = f"{rounded_lat},{rounded_lng}"
    # Use MD5 hash to create consistent UUID from coordinates
    hash_object = hashlib.md5(coordinate_string.encode())
    # Create UUID from first 16 bytes of hash
    return str(uuid.UUID(bytes=hash_object.digest()))

def minimal_processing():
    """Process a tiny sample to test the full pipeline with UUID-based stations"""
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
    
    # Filter out invalid trips including coordinate requirements
    df = df[
        (df['duration_minutes'] > 1) &  # At least 1 minute
        (df['duration_minutes'] < 1440) &  # Less than 24 hours
        df['start_station_name'].notna() &
        df['end_station_name'].notna() &
        df['start_lat'].notna() &
        df['start_lng'].notna() &
        df['end_lat'].notna() &
        df['end_lng'].notna()
    ].copy()
    
    print(f"After filtering: {len(df)} valid trips")
    
    # Create station registry based on coordinates
    print("Creating station registry...")
    station_registry = {}
    
    # Process all unique start and end station coordinates
    all_stations = pd.concat([
        df[['start_station_id', 'start_station_name', 'start_lat', 'start_lng']].rename(columns={
            'start_station_id': 'station_id', 'start_station_name': 'station_name', 
            'start_lat': 'lat', 'start_lng': 'lng'
        }),
        df[['end_station_id', 'end_station_name', 'end_lat', 'end_lng']].rename(columns={
            'end_station_id': 'station_id', 'end_station_name': 'station_name',
            'end_lat': 'lat', 'end_lng': 'lng'
        })
    ]).drop_duplicates()
    
    for _, row in all_stations.iterrows():
        lat, lng = row['lat'], row['lng']
        station_uuid = generate_station_uuid(lat, lng)
        bluebike_id = str(row['station_id'])
        station_name = row['station_name']
        
        if station_uuid not in station_registry:
            station_registry[station_uuid] = {
                'uuid': station_uuid,
                'lat': lat,
                'lng': lng,
                'current_name': station_name,
                'bluebike_ids': set(),
                'all_names': set()
            }
        
        station_registry[station_uuid]['bluebike_ids'].add(bluebike_id)
        station_registry[station_uuid]['all_names'].add(station_name)
        station_registry[station_uuid]['current_name'] = station_name
    
    # Convert sets to lists
    for station in station_registry.values():
        station['bluebike_ids'] = sorted(list(station['bluebike_ids']))
        station['all_names'] = sorted(list(station['all_names']))
    
    print(f"Created registry for {len(station_registry)} unique station locations")
    
    # Add UUIDs to dataframe
    coord_to_uuid = {}
    for station_uuid, station_info in station_registry.items():
        lat, lng = station_info['lat'], station_info['lng']
        coord_key = f"{round(lat, 6)},{round(lng, 6)}"
        coord_to_uuid[coord_key] = station_uuid
    
    df['start_station_uuid'] = df.apply(
        lambda row: coord_to_uuid.get(f"{round(row['start_lat'], 6)},{round(row['start_lng'], 6)}"),
        axis=1
    )
    df['end_station_uuid'] = df.apply(
        lambda row: coord_to_uuid.get(f"{round(row['end_lat'], 6)},{round(row['end_lng'], 6)}"),
        axis=1
    )
    
    # Filter out unmappable coordinates
    df = df.dropna(subset=['start_station_uuid', 'end_station_uuid'])
    print(f"After UUID mapping: {len(df)} trips")
    
    # Calculate fastest times between station UUID pairs with trip counts
    fastest_times = df.groupby(['start_station_uuid', 'end_station_uuid']).agg({
        'duration_minutes': 'min',
        'ride_id': 'count'
    }).reset_index()
    fastest_times.rename(columns={
        'duration_minutes': 'fastest_time_minutes',
        'ride_id': 'trip_count'
    }, inplace=True)
    
    print(f"Found {len(fastest_times)} unique station location pairs")
    
    # Get ride details for fastest rides
    fastest_rides = df.loc[df.groupby(['start_station_uuid', 'end_station_uuid'])['duration_minutes'].idxmin()]
    fastest_rides = fastest_rides[['start_station_uuid', 'end_station_uuid', 'start_station_name', 'end_station_name', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'duration_minutes', 'started_at', 'ride_id']]
    
    # Merge to get complete data
    result = fastest_times.merge(
        fastest_rides,
        left_on=['start_station_uuid', 'end_station_uuid', 'fastest_time_minutes'],
        right_on=['start_station_uuid', 'end_station_uuid', 'duration_minutes'],
        how='left'
    )
    
    # Calculate point-to-point distances
    print("Calculating point-to-point distances...")
    result['distance_km'] = result.apply(
        lambda row: haversine_distance(
            row['start_lat'], row['start_lng'], 
            row['end_lat'], row['end_lng']
        ), axis=1
    )
    
    # Create stations data structure using UUIDs
    stations = {}
    for _, row in result.iterrows():
        start_uuid = row['start_station_uuid']
        end_uuid = row['end_station_uuid']
        start_name = row['start_station_name']
        end_name = row['end_station_name']
        
        if start_uuid not in stations:
            station_info = station_registry[start_uuid]
            stations[start_uuid] = {
                'uuid': start_uuid,
                'name': station_info['current_name'],
                'lat': station_info['lat'],
                'lng': station_info['lng'],
                'bluebike_ids': station_info['bluebike_ids'],
                'all_names': station_info['all_names'],
                'destinations': {}
            }
        
        end_info = station_registry[end_uuid]
        stations[start_uuid]['destinations'][end_uuid] = {
            'uuid': end_uuid,
            'name': end_info['current_name'],
            'fastest_time_minutes': row['fastest_time_minutes'],
            'fastest_time_formatted': f"{int(row['fastest_time_minutes'])}:{int((row['fastest_time_minutes'] % 1) * 60):02d}",
            'trip_count': row.get('trip_count', 1),
            'distance_km': round(row.get('distance_km', 0), 2),
            'ride_id': row['ride_id'],
            'date': row['started_at'].strftime('%Y-%m-%d %H:%M:%S')
        }
    
    print(f"Created data for {len(stations)} station locations")
    
    # Create directories
    os.makedirs('content/stations', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    
    # Create new nested structure for station pairs
    station_pairs = {}
    
    for start_station_uuid, station_data in stations.items():
        station_pairs[start_station_uuid] = {}
        
        for end_station_uuid, dest_data in station_data.get('destinations', {}).items():
            station_pairs[start_station_uuid][end_station_uuid] = {
                'attempts': dest_data.get('trip_count', 1),
                'fastest_time_minutes': dest_data['fastest_time_minutes'],
                'fastest_time_formatted': dest_data['fastest_time_formatted'],
                'fastest_set_at': dest_data['date'],
                'ride_id': dest_data['ride_id'],
                'distance_km': dest_data.get('distance_km', 0)
            }
    
    # Save station data as JSON with new structure (remove underscores)
    stations_data = {
        'metadata': {
            'processed_months': ['202401'],
            'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC'),
            'total_stations': len(stations)
        },
        'station_registry': station_registry,
        'station_pairs': station_pairs
    }
    
    with open('data/stations.json', 'w') as f:
        json.dump(stations_data, f, indent=2)
    
    # Create individual station pages using station pairs and registry data
    station_count = 0
    for station_uuid in station_pairs.keys():
        station_info = station_registry.get(station_uuid, {})
        station_name = station_info.get('current_name', 'Unknown Station')
        safe_filename = station_uuid.replace('-', '')[:12]
        
        content = f'''---
title: "{station_name}"
station_uuid: "{station_uuid}"
station_name: "{station_name}"
lat: {station_info.get('lat', 0)}
lng: {station_info.get('lng', 0)}
bluebike_ids: {json.dumps(station_info.get('bluebike_ids', []))}
all_names: {json.dumps(station_info.get('all_names', []))}
type: "station"
---

# FKBB Times from {station_name}

This page shows the Fastest Known BlueBike (FKBB) times from **{station_name}** to all other stations.

**Location:** {station_data['lat']}, {station_data['lng']}  
**BlueBike Station IDs:** {', '.join(station_data['bluebike_ids'])}  
**All Known Names:** {', '.join(station_data['all_names'])}

'''
        
        with open(f'content/stations/{safe_filename}.md', 'w') as f:
            f.write(content)
        
        station_count += 1
    
    # Create index page
    index_content = f'''---
title: "Fastest Known BlueBike (FKBB) Times"
---

# Fastest Known BlueBike (FKBB) Times

Welcome to the FKBB tracker! This site shows the fastest recorded times between every pair of BlueBike stations.

Stations are now organized by geographic location (lat/lng) with generated UUIDs, so stations that have moved slightly or changed BlueBike IDs are treated as the same location.

## All Stations

'''
    
    # Add station links sorted by name
    sorted_stations = sorted(stations.items(), key=lambda x: x[1]['name'])
    for station_uuid, station_data in sorted_stations:
        station_name = station_data['name']
        safe_filename = station_uuid.replace('-', '')[:12]
        bluebike_ids = ', '.join(station_data['bluebike_ids'])
        index_content += f"- [{station_name}](stations/{safe_filename}/) (BlueBike IDs: {bluebike_ids})\n"
    
    index_content += f'''

## Test Data

This is a test version using a sample of January 2024 data ({len(df)} trips processed).

**Station locations:** {len(stations)}  
**Unique BlueBike station IDs:** {sum(len(s['bluebike_ids']) for s in stations.values())}

Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
'''
    
    with open('content/_index.md', 'w') as f:
        f.write(index_content)
    
    print("✓ Test processing completed successfully!")
    print(f"Created {station_count} station location pages")
    print(f"Station locations: {len(stations)}")
    print(f"Unique BlueBike station IDs: {sum(len(s['bluebike_ids']) for s in stations.values())}")

if __name__ == "__main__":
    minimal_processing()