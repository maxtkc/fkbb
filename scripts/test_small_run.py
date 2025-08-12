#!/usr/bin/env python3

import requests
import pandas as pd
import json
import os
from datetime import datetime
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import zipfile
import io

def test_small_data_processing():
    """Test with a small older dataset"""
    # Use a smaller, older dataset for testing
    data_url = "https://s3.amazonaws.com/hubway-data/202401-bluebikes-tripdata.zip"
    
    print(f"Downloading test data from: {data_url}")
    
    # Download the zip file
    response = requests.get(data_url)
    response.raise_for_status()
    
    # Extract and read CSV
    with zipfile.ZipFile(io.BytesIO(response.content)) as zip_file:
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]
        if not csv_files:
            raise ValueError("No CSV files found in zip")
        
        # Read the first CSV file
        csv_filename = csv_files[0]
        print(f"Processing file: {csv_filename}")
        
        with zip_file.open(csv_filename) as csv_file:
            df = pd.read_csv(csv_file)
    
    print(f"Loaded {len(df)} rows")
    print("Columns:", df.columns.tolist())
    print("\nFirst few rows:")
    print(df.head())
    
    # Check rideable_type values
    print(f"\nRideable types: {df['rideable_type'].unique()}")
    
    # Filter for classic bikes only
    df_classic = df[df['rideable_type'] == 'classic_bike'].copy()
    print(f"Classic bike rows: {len(df_classic)}")
    
    if len(df_classic) == 0:
        print("No classic bike data found!")
        return
    
    # Check for required columns
    required_cols = ['started_at', 'ended_at', 'start_station_name', 'end_station_name']
    missing_cols = [col for col in required_cols if col not in df_classic.columns]
    if missing_cols:
        print(f"Missing columns: {missing_cols}")
        return
    
    print("✓ Data structure looks good for processing!")

if __name__ == "__main__":
    test_small_data_processing()