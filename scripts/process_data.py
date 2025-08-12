#!/usr/bin/env python3

import requests
import pandas as pd
import json
import os
from datetime import datetime, timedelta
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import zipfile
import io
from tqdm import tqdm
import uuid
import hashlib

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

def create_station_registry(df):
    """Create a registry of stations based on lat/lng coordinates"""
    station_registry = {}
    
    # Process start stations
    start_stations = df[['start_station_id', 'start_station_name', 'start_lat', 'start_lng']].drop_duplicates()
    start_stations = start_stations.dropna()
    
    for _, row in start_stations.iterrows():
        lat, lng = row['start_lat'], row['start_lng']
        station_uuid = generate_station_uuid(lat, lng)
        bluebike_id = str(row['start_station_id'])
        station_name = row['start_station_name']
        
        if station_uuid not in station_registry:
            station_registry[station_uuid] = {
                'uuid': station_uuid,
                'lat': lat,
                'lng': lng,
                'current_name': station_name,  # Will be updated with most recent name
                'bluebike_ids': set(),
                'all_names': set()
            }
        
        station_registry[station_uuid]['bluebike_ids'].add(bluebike_id)
        station_registry[station_uuid]['all_names'].add(station_name)
        station_registry[station_uuid]['current_name'] = station_name  # Keep most recent
    
    # Process end stations
    end_stations = df[['end_station_id', 'end_station_name', 'end_lat', 'end_lng']].drop_duplicates()
    end_stations = end_stations.dropna()
    
    for _, row in end_stations.iterrows():
        lat, lng = row['end_lat'], row['end_lng']
        station_uuid = generate_station_uuid(lat, lng)
        bluebike_id = str(row['end_station_id'])
        station_name = row['end_station_name']
        
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
        station_registry[station_uuid]['current_name'] = station_name  # Keep most recent
    
    # Convert sets to lists for JSON serialization
    for station in station_registry.values():
        station['bluebike_ids'] = sorted(list(station['bluebike_ids']))
        station['all_names'] = sorted(list(station['all_names']))
    
    print(f"Created registry for {len(station_registry)} unique station locations")
    return station_registry

def add_station_uuids_to_dataframe(df, station_registry):
    """Add station UUIDs to dataframe based on lat/lng coordinates"""
    # Create lookup dictionaries for faster mapping
    coord_to_uuid = {}
    for station_uuid, station_info in station_registry.items():
        lat, lng = station_info['lat'], station_info['lng']
        coord_key = f"{round(lat, 6)},{round(lng, 6)}"
        coord_to_uuid[coord_key] = station_uuid
    
    # Map coordinates to UUIDs
    df['start_station_uuid'] = df.apply(
        lambda row: coord_to_uuid.get(f"{round(row['start_lat'], 6)},{round(row['start_lng'], 6)}"),
        axis=1
    )
    df['end_station_uuid'] = df.apply(
        lambda row: coord_to_uuid.get(f"{round(row['end_lat'], 6)},{round(row['end_lng'], 6)}"),
        axis=1
    )
    
    # Filter out rows where we couldn't map coordinates to UUIDs
    before_filter = len(df)
    df = df.dropna(subset=['start_station_uuid', 'end_station_uuid'])
    after_filter = len(df)
    
    if before_filter > after_filter:
        print(f"Filtered out {before_filter - after_filter} trips with unmappable coordinates")
    
    return df

def fetch_all_data_files():
    """Fetch list of all available Bluebike data files from S3"""
    base_url = "https://s3.amazonaws.com/hubway-data/"
    response = requests.get(base_url)
    response.raise_for_status()
    
    # Parse as XML since it's S3 bucket listing
    try:
        soup = BeautifulSoup(response.content, 'xml')
    except:
        # Fallback to html.parser if lxml not available
        soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all zip files (only bluebikes from 201805+)
    zip_files = []
    for key in soup.find_all('Key'):
        filename = key.text
        if filename.endswith('.zip') and 'bluebikes' in filename:
            # Include files from 201805 onwards
            year_month = filename[:6]
            if year_month >= '201805':
                zip_files.append(filename)
    
    if not zip_files:
        raise ValueError("No trip data files found")
    
    return sorted(zip_files)

def extract_month_from_filename(filename):
    """Extract YYYYMM from filename like '202401-bluebikes-tripdata.zip'"""
    return filename[:6]

def load_existing_data():
    """Load existing stations data, station registry, and metadata"""
    try:
        with open('data/stations.json', 'r') as f:
            data = json.load(f)
        
        # Extract metadata, station registry, and stations separately
        # Try new format first, fall back to old format
        if 'metadata' in data:
            metadata = data.pop('metadata', {})
            station_registry = data.pop('station_registry', {})
            # Convert station_pairs back to old format for compatibility
            station_pairs = data.pop('station_pairs', {})
            stations_data = {}
            for start_uuid, destinations in station_pairs.items():
                station_info = station_registry.get(start_uuid, {})
                stations_data[start_uuid] = {
                    'uuid': start_uuid,
                    'name': station_info.get('current_name', 'Unknown'),
                    'lat': station_info.get('lat'),
                    'lng': station_info.get('lng'),
                    'bluebike_ids': station_info.get('bluebike_ids', []),
                    'all_names': station_info.get('all_names', []),
                    'destinations': {}
                }
                for dest_uuid, dest_data in destinations.items():
                    dest_info = station_registry.get(dest_uuid, {})
                    stations_data[start_uuid]['destinations'][dest_uuid] = {
                        'uuid': dest_uuid,
                        'name': dest_info.get('current_name', 'Unknown'),
                        'fastest_time_minutes': dest_data['fastest_time_minutes'],
                        'fastest_time_formatted': dest_data['fastest_time_formatted'],
                        'trip_count': dest_data['attempts'],
                        'distance_km': dest_data.get('distance_km', 0),
                        'ride_id': dest_data['ride_id'],
                        'date': dest_data['fastest_set_at']
                    }
            data = stations_data
        else:
            # Old format
            metadata = data.pop('_metadata', {
                'processed_months': [],
                'last_updated': '',
                'total_stations': 0
            })
            station_registry = data.pop('_station_registry', {})
        
        return data, station_registry, metadata
    except FileNotFoundError:
        return {}, {}, {
            'processed_months': [],
            'last_updated': '',
            'total_stations': 0
        }

def get_cached_file_path(filename, cache_type='zip'):
    """Get the cache file path for a given filename"""
    cache_dir = f'cache/{cache_type}'
    os.makedirs(cache_dir, exist_ok=True)
    return os.path.join(cache_dir, filename.replace('.zip', '.csv' if cache_type == 'csv' else '.zip'))

def is_cache_valid(cache_path, expected_size=None):
    """Check if cached file exists and is valid"""
    if not os.path.exists(cache_path):
        return False
    
    # Check if file is not empty
    if os.path.getsize(cache_path) == 0:
        return False
        
    # Optional: check expected size if provided
    if expected_size and os.path.getsize(cache_path) != expected_size:
        return False
        
    return True

def download_with_cache(filename):
    """Download file with caching support"""
    base_url = "https://s3.amazonaws.com/hubway-data/"
    data_url = urljoin(base_url, filename)
    zip_cache_path = get_cached_file_path(filename, 'zip')
    
    # Check if zip file is already cached
    if is_cache_valid(zip_cache_path):
        print(f"📋 Using cached zip file: {zip_cache_path}")
        with open(zip_cache_path, 'rb') as f:
            content = f.read()
    else:
        # Download the zip file with progress
        print(f"📥 Downloading {filename}...")
        response = requests.get(data_url, stream=True)
        response.raise_for_status()
        
        file_size = int(response.headers.get('content-length', 0))
        if file_size > 0:
            with tqdm(total=file_size, unit='B', unit_scale=True, desc="Download") as pbar:
                content = b''
                for chunk in response.iter_content(chunk_size=8192):
                    content += chunk
                    pbar.update(len(chunk))
        else:
            content = response.content
        
        # Save to cache
        with open(zip_cache_path, 'wb') as f:
            f.write(content)
        print(f"💾 Cached zip file: {zip_cache_path}")
    
    return content

def extract_csv_with_cache(filename, zip_content):
    """Extract CSV with caching support"""
    csv_cache_path = get_cached_file_path(filename, 'csv')
    
    # Check if CSV is already cached
    if is_cache_valid(csv_cache_path):
        print(f"📋 Using cached CSV file: {csv_cache_path}")
        df = pd.read_csv(csv_cache_path)
    else:
        # Extract and read CSV
        print(f"📂 Extracting and reading CSV...")
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
            if not csv_files:
                raise ValueError("No CSV files found in zip")
            
            csv_filename = csv_files[0]
            with zip_file.open(csv_filename) as csv_file:
                df = pd.read_csv(csv_file)
        
        # Save CSV to cache
        df.to_csv(csv_cache_path, index=False)
        print(f"💾 Cached CSV file: {csv_cache_path}")
    
    return df

def download_and_process_data(filename):
    """Download and process a specific Bluebike data file with caching"""
    # Download zip file (with caching)
    zip_content = download_with_cache(filename)
    
    # Extract CSV (with caching)
    df = extract_csv_with_cache(filename, zip_content)
    
    print(f"Loaded {len(df)} rows")
    
    # Handle different column formats
    if 'rideable_type' in df.columns:
        # Modern format (2024+): filter for classic bikes
        df = df[df['rideable_type'] == 'classic_bike'].copy()
        print(f"After filtering for classic bikes: {len(df)} rows")
        # Already have started_at, ended_at, ride_id
    else:
        # Legacy format (2018-2023): all bikes are classic, need to convert columns
        print(f"Legacy format detected, processing all bikes as classic: {len(df)} rows")
        
        # Convert legacy column names to modern format
        df = df.rename(columns={
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
        })
        
        # Create ride_id from bikeid + starttime (legacy files don't have ride_id)
        df['ride_id'] = df['bikeid'].astype(str) + '_' + df['started_at'].astype(str)
    
    # Convert timestamps
    df['started_at'] = pd.to_datetime(df['started_at'])
    df['ended_at'] = pd.to_datetime(df['ended_at'])
    
    # Calculate trip duration in minutes
    if 'tripduration' in df.columns:
        # Legacy format has duration in seconds
        df['duration_minutes'] = df['tripduration'] / 60
    else:
        # Modern format calculates from timestamps
        df['duration_minutes'] = (df['ended_at'] - df['started_at']).dt.total_seconds() / 60
    
    # Filter out invalid trips (negative duration, too long, etc.) and ensure required fields exist
    df = df[
        (df['duration_minutes'] > 1) &  # At least 1 minute
        (df['duration_minutes'] < 1440) &  # Less than 24 hours
        df['start_station_name'].notna() &
        df['end_station_name'].notna() &
        df['start_station_id'].notna() &
        df['end_station_id'].notna() &
        df['start_lat'].notna() &
        df['start_lng'].notna() &
        df['end_lat'].notna() &
        df['end_lng'].notna()
    ].copy()
    
    print(f"After filtering invalid trips: {len(df)} rows")
    
    # Create station registry and add UUIDs to dataframe
    print("Creating station registry from coordinates...")
    station_registry = create_station_registry(df)
    
    print("Adding station UUIDs to dataframe...")
    df = add_station_uuids_to_dataframe(df, station_registry)
    
    return df, station_registry

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

def calculate_fastest_times(df):
    """Calculate fastest times between all station pairs using station UUIDs"""
    print("⚡ Calculating fastest times between stations...")
    
    # Group by station UUID pair and find minimum duration and count trips
    print("  🔍 Finding minimum durations and trip counts...")
    fastest_times = df.groupby(['start_station_uuid', 'end_station_uuid']).agg({
        'duration_minutes': 'min',
        'ride_id': 'count'
    }).reset_index()
    fastest_times.rename(columns={
        'duration_minutes': 'fastest_time_minutes',
        'ride_id': 'trip_count'
    }, inplace=True)
    
    # Also get the ride details for the fastest ride
    print("  📋 Getting ride details for fastest times...")
    fastest_rides = df.loc[df.groupby(['start_station_uuid', 'end_station_uuid'])['duration_minutes'].idxmin()]
    fastest_rides = fastest_rides[['start_station_uuid', 'end_station_uuid', 'start_station_name', 'end_station_name', 'start_lat', 'start_lng', 'end_lat', 'end_lng', 'duration_minutes', 'started_at', 'ride_id']]
    
    # Merge to get ride details
    print("  🔗 Merging data...")
    result = fastest_times.merge(
        fastest_rides,
        left_on=['start_station_uuid', 'end_station_uuid', 'fastest_time_minutes'],
        right_on=['start_station_uuid', 'end_station_uuid', 'duration_minutes'],
        how='left'
    )
    
    # Calculate point-to-point distances
    print("  🗺️ Calculating point-to-point distances...")
    result['distance_km'] = result.apply(
        lambda row: haversine_distance(
            row['start_lat'], row['start_lng'], 
            row['end_lat'], row['end_lng']
        ), axis=1
    )
    
    print(f"✅ Found {len(result)} unique station pairs")
    return result

def create_station_data(fastest_times_df, station_registry, existing_stations=None, existing_registry=None):
    """Create/update data structure organized by station using station UUIDs"""
    if existing_stations is None:
        existing_stations = {}
    if existing_registry is None:
        existing_registry = {}
    
    stations = existing_stations.copy()
    # Merge station registries, preferring newer information for current_name
    merged_registry = existing_registry.copy()
    for uuid, info in station_registry.items():
        if uuid in merged_registry:
            # Merge bluebike_ids and all_names
            merged_registry[uuid]['bluebike_ids'] = sorted(list(set(
                merged_registry[uuid]['bluebike_ids'] + info['bluebike_ids']
            )))
            merged_registry[uuid]['all_names'] = sorted(list(set(
                merged_registry[uuid]['all_names'] + info['all_names']
            )))
            # Keep the newer current_name
            merged_registry[uuid]['current_name'] = info['current_name']
        else:
            merged_registry[uuid] = info.copy()
    
    for _, row in fastest_times_df.iterrows():
        start_station_uuid = row['start_station_uuid']
        end_station_uuid = row['end_station_uuid']
        start_station_name = row['start_station_name']
        end_station_name = row['end_station_name']
        
        if start_station_uuid not in stations:
            station_info = merged_registry.get(start_station_uuid, {})
            stations[start_station_uuid] = {
                'uuid': start_station_uuid,
                'name': station_info.get('current_name', start_station_name),
                'lat': station_info.get('lat'),
                'lng': station_info.get('lng'),
                'bluebike_ids': station_info.get('bluebike_ids', []),
                'all_names': station_info.get('all_names', [start_station_name]),
                'destinations': {}
            }
        
        # Check if this destination already exists and if new time is faster
        current_time = row['fastest_time_minutes']
        existing_dest = stations[start_station_uuid]['destinations'].get(end_station_uuid)
        
        if existing_dest is None or current_time < existing_dest['fastest_time_minutes']:
            end_station_info = merged_registry.get(end_station_uuid, {})
            stations[start_station_uuid]['destinations'][end_station_uuid] = {
                'uuid': end_station_uuid,
                'name': end_station_info.get('current_name', end_station_name),
                'fastest_time_minutes': current_time,
                'fastest_time_formatted': f"{int(current_time)}:{int((current_time % 1) * 60):02d}",
                'trip_count': row.get('trip_count', 1),
                'distance_km': round(row.get('distance_km', 0), 2),
                'ride_id': row['ride_id'],
                'date': row['started_at'].strftime('%Y-%m-%d %H:%M:%S')
            }
        elif existing_dest is not None:
            # If time is not faster, still update trip count (add to existing)
            existing_dest['trip_count'] = existing_dest.get('trip_count', 0) + row.get('trip_count', 1)
    
    return stations, merged_registry

def generate_hugo_content(stations_data, station_registry, metadata):
    """Generate Hugo content files"""
    print("Generating Hugo content...")
    
    # Create content directories
    os.makedirs('content/stations', exist_ok=True)
    os.makedirs('data', exist_ok=True)
    
    # Create new nested structure for station pairs
    station_pairs = {}
    
    for start_station_uuid, station_data in stations_data.items():
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
    
    # Prepare data with new structure (remove underscores as requested)
    data_with_metadata = {
        'metadata': metadata,
        'station_registry': station_registry,
        'station_pairs': station_pairs
    }
    
    # Save station data as JSON for Hugo templates
    with open('data/stations.json', 'w') as f:
        json.dump(data_with_metadata, f, indent=2)
    
    # Create individual station pages using station UUIDs and registry data
    for station_uuid in station_pairs.keys():
        station_info = station_registry.get(station_uuid, {})
        station_name = station_info.get('current_name', 'Unknown Station')
        
        # Create safe filename from UUID (first 8 characters + shortened UUID)
        safe_filename = station_uuid.replace('-', '')[:12]
        
        content = f"""---
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

**Location:** {station_data.get('lat', 'Unknown')}, {station_data.get('lng', 'Unknown')}  
**BlueBike Station IDs:** {', '.join(station_data.get('bluebike_ids', []))}  
**All Known Names:** {', '.join(station_data.get('all_names', []))}

"""
        
        with open(f'content/stations/{safe_filename}.md', 'w') as f:
            f.write(content)
    
    # Create index page
    index_content = f"""---
title: "Fastest Known BlueBike (FKBB) Times"
---

# Fastest Known BlueBike (FKBB) Times

Welcome to the FKBB tracker! This site shows the fastest recorded times between every pair of BlueBike stations.

Stations are now organized by geographic location (lat/lng) with generated UUIDs, so stations that have moved slightly or changed BlueBike IDs are treated as the same location.

## All Stations

"""
    
    # Sort stations by name for the index using registry data
    station_items = [(uuid, info) for uuid, info in station_registry.items() if uuid in station_pairs]
    sorted_stations = sorted(station_items, key=lambda x: x[1]['current_name'])
    
    for station_uuid, station_info in sorted_stations:
        station_name = station_info['current_name']
        safe_filename = station_uuid.replace('-', '')[:12]
        bluebike_ids = ', '.join(station_info.get('bluebike_ids', []))
        index_content += f"- [{station_name}](stations/{safe_filename}/) (BlueBike IDs: {bluebike_ids})\n"
    
    index_content += f"""

## About

Data is updated daily from the [official BlueBike trip data](https://s3.amazonaws.com/hubway-data/index.html).
Only classic bike trips are included in the analysis.

Stations are grouped by geographic coordinates (lat/lng) and assigned UUIDs. This means that stations with the same location but different BlueBike station IDs over time are treated as one location.

**Total unique station locations:** {len(station_pairs)}  
**Total unique BlueBike station IDs tracked:** {sum(len(info.get('bluebike_ids', [])) for uuid, info in station_registry.items() if uuid in station_pairs)}

Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
"""
    
    with open('content/_index.md', 'w') as f:
        f.write(index_content)

def get_cache_stats():
    """Get cache statistics"""
    cache_stats = {'zip_files': 0, 'csv_files': 0, 'total_size_mb': 0}
    
    for cache_type in ['zip', 'csv']:
        cache_dir = f'cache/{cache_type}'
        if os.path.exists(cache_dir):
            files = [f for f in os.listdir(cache_dir) if f.endswith(f'.{cache_type}')]
            cache_stats[f'{cache_type}_files'] = len(files)
            
            for file in files:
                file_path = os.path.join(cache_dir, file)
                cache_stats['total_size_mb'] += os.path.getsize(file_path) / (1024 * 1024)
    
    return cache_stats

def clear_cache(cache_type=None):
    """Clear cache files"""
    if cache_type:
        cache_dir = f'cache/{cache_type}'
        if os.path.exists(cache_dir):
            import shutil
            shutil.rmtree(cache_dir)
            print(f"🗑️ Cleared {cache_type} cache")
    else:
        if os.path.exists('cache'):
            import shutil
            shutil.rmtree('cache')
            print("🗑️ Cleared all cache")

def main():
    try:
        # Show cache statistics
        cache_stats = get_cache_stats()
        if cache_stats['zip_files'] > 0 or cache_stats['csv_files'] > 0:
            print(f"💾 Cache stats: {cache_stats['zip_files']} zip files, {cache_stats['csv_files']} CSV files ({cache_stats['total_size_mb']:.1f} MB)")
        
        # Load existing data and metadata
        print("Loading existing data...")
        existing_stations, existing_registry, metadata = load_existing_data()
        
        # Fetch all available data files
        print("Fetching list of available data files...")
        all_files = fetch_all_data_files()
        
        # Extract months from all files
        all_months = [extract_month_from_filename(f) for f in all_files]
        processed_months = set(metadata['processed_months'])
        
        # Find unprocessed months
        unprocessed_months = [month for month in all_months if month not in processed_months]
        unprocessed_months.sort()  # Process from oldest to newest
        
        if not unprocessed_months:
            print("All months have been processed. No new data to process.")
            return
        
        print(f"Found {len(unprocessed_months)} unprocessed months: {unprocessed_months[:5]}{'...' if len(unprocessed_months) > 5 else ''}")
        
        stations_data = existing_stations
        merged_registry = existing_registry
        
        # Process each unprocessed month with progress bar
        print(f"\n📊 Starting data processing...")
        progress_bar = tqdm(unprocessed_months, desc="Processing months", unit="month")
        
        for month in progress_bar:
            # Find the filename for this month
            filename = None
            for f in all_files:
                if f.startswith(month):
                    filename = f
                    break
            
            if not filename:
                print(f"Warning: No file found for month {month}")
                continue
            
            progress_bar.set_description(f"Processing {month}")
            
            try:
                # Download and process this month's data (returns df and station_registry)
                df, station_registry = download_and_process_data(filename)
                
                # Calculate fastest times for this month
                fastest_times = calculate_fastest_times(df)
                
                # Merge with existing data (keeping fastest times)
                stations_data, merged_registry = create_station_data(
                    fastest_times, station_registry, stations_data, merged_registry
                )
                
                # Update metadata
                metadata['processed_months'].append(month)
                metadata['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
                metadata['total_stations'] = len(stations_data)
                
                # Save backup after each month to avoid losing progress
                backup_data = {
                    '_metadata': metadata,
                    '_station_registry': merged_registry,
                    **stations_data
                }
                os.makedirs('backups', exist_ok=True)
                with open(f'backups/stations_{month}.json', 'w') as f:
                    json.dump(backup_data, f, indent=2)
                
                progress_bar.set_postfix({
                    'stations': len(stations_data),
                    'processed': len(metadata['processed_months'])
                })
                
            except Exception as e:
                tqdm.write(f"❌ Error processing {month}: {e}")
                tqdm.write("   Continuing with next month...")
                continue
        
        # Close progress bar
        progress_bar.close()
        
        # Generate Hugo content with updated data
        print("\n📝 Generating Hugo content...")
        generate_hugo_content(stations_data, merged_registry, metadata)
        
        print(f"\n🎉 Data processing completed successfully!")
        print(f"📊 Processed {len(metadata['processed_months'])} total months")
        print(f"🚲 Total station locations: {len(stations_data)}")
        print(f"🆔 Total unique BlueBike station IDs: {sum(len(s.get('bluebike_ids', [])) for s in stations_data.values())}")
        print(f"✨ Newly processed months: {len(unprocessed_months)}")
        
    except Exception as e:
        print(f"Error in main processing: {e}")
        raise

if __name__ == "__main__":
    main()