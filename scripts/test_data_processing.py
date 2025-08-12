#!/usr/bin/env python3

import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def test_fetch_latest_url():
    """Test fetching the latest data URL"""
    base_url = "https://s3.amazonaws.com/hubway-data/"
    
    try:
        response = requests.get(base_url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'xml')
        
        zip_files = []
        for key in soup.find_all('Key'):
            filename = key.text
            if filename.endswith('.zip') and ('tripdata' in filename or 'bluebikes' in filename):
                zip_files.append(filename)
        
        print(f"Found {len(zip_files)} trip data files:")
        for i, file in enumerate(sorted(zip_files)[-5:]):  # Show last 5
            print(f"  {i+1}. {file}")
        
        if zip_files:
            latest_file = sorted(zip_files)[-1]
            latest_url = urljoin(base_url, latest_file)
            print(f"\nLatest file URL: {latest_url}")
            return latest_url
        else:
            print("No trip data files found")
            return None
            
    except Exception as e:
        print(f"Error: {e}")
        return None

if __name__ == "__main__":
    test_fetch_latest_url()