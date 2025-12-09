#!/usr/bin/env python3
"""
Script to split large Apache access.log file by date
Processes 10M+ records and creates separate log files per day
"""

import os
import re
from datetime import datetime

# --- Configuration ---
# Source file path (modify as needed)
SOURCE_FILE = '/home/waly/Downloads/archive/access.log'

# Output directory for split files
OUTPUT_DIR = '/home/waly/project/landing_zone'

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"Created directory: {OUTPUT_DIR}")

# --- Main Logic ---
# Pattern to extract date from log: [22/Jan/2019
date_pattern = re.compile(r'\[(\d{2}/[A-Z][a-z]{2}/\d{4})')

print(f"Starting to split 10M records from: {SOURCE_FILE}")
print("Please wait, this might take 1-2 minutes...")

files_handles = {}
line_count = 0

try:
    with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as infile:
        for line in infile:
            line_count += 1
            
            # Progress indicator every 500k lines
            if line_count % 500000 == 0:
                print(f"Processed {line_count:,} records...", end='\r')
            
            # Search for date in log line
            match = date_pattern.search(line)
            if match:
                raw_date = match.group(1)  # e.g., 22/Jan/2019
                
                # Convert date to YYYY-MM-DD format for sorted filenames
                dt_obj = datetime.strptime(raw_date, '%d/%b/%Y')
                date_str = dt_obj.strftime('%Y-%m-%d')
                
                filename = f"access_log_{date_str}.log"
                file_path = os.path.join(OUTPUT_DIR, filename)
                
                # Open file and add to dictionary if not already open (performance optimization)
                if filename not in files_handles:
                    files_handles[filename] = open(file_path, 'a', encoding='utf-8')
                
                # Write line to appropriate date file
                files_handles[filename].write(line)
    
    print(f"\nDone! Successfully processed {line_count:,} records.")
    print(f"Check your files here: {OUTPUT_DIR}")

except FileNotFoundError:
    print(f"Error: File not found at {SOURCE_FILE}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close all open file handles
    for f in files_handles.values():
        f.close()
