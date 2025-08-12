#!/usr/bin/env python3

import json
import re

def create_safe_filename(station_name):
    """Create safe filename using consistent logic"""
    safe_filename = re.sub(r'[^\w\s-]', '', station_name).strip().lower()
    safe_filename = re.sub(r'[-\s]+', '-', safe_filename)
    return safe_filename

# Load stations data
with open('data/stations.json', 'r') as f:
    stations_data = json.load(f)

# Create filename mapping
filename_mapping = {}
for station_name in stations_data.keys():
    safe_filename = create_safe_filename(station_name)
    filename_mapping[station_name] = safe_filename

# Save mapping for Hugo to use
with open('data/filename_mapping.json', 'w') as f:
    json.dump(filename_mapping, f, indent=2)

print(f"Created filename mapping for {len(filename_mapping)} stations")
print("Sample mappings:")
for i, (original, safe) in enumerate(filename_mapping.items()):
    if i < 5:
        print(f"  '{original}' -> '{safe}'")