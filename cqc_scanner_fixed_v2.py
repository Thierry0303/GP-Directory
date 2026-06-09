#!/usr/bin/env python3
"""
CQC Deep-Inspection Scanner - FIXED to ensure JSON saves properly
"""

import requests
import json
import time
import os
from collections import defaultdict
from datetime import datetime

CQC_API_BASE = "https://api.service.cqc.org.uk/public/v1"
CQC_API_KEY = os.environ.get("CQC_KEY")

TIMEOUT = 10
RATE_LIMIT = 0.1
DEEP_LIMIT = 0.1

LONDON_POSTCODES = {
    "E", "EC", "N", "NW", "SE", "SW", "W", "WC",
    "BR", "CR", "DA", "EN", "HA", "IG", "KT", "RM", "SM", "TW", "UB"
}

def is_london_postcode(postcode):
    if not postcode:
        return False
    pc = postcode.strip().upper()
    prefix = pc[:3] if len(pc) >= 3 else pc[:2]
    return prefix in LONDON_POSTCODES or pc[0] in {'E', 'N', 'S', 'W'}

def fetch_cqc_locations(page=1, page_size=100):
    if not CQC_API_KEY:
        print(f"\nError: 'CQC_KEY' environment variable not set.")
        return {'locations': [], 'total_pages': 0}
    try:
        url = f"{CQC_API_BASE}/locations"
        params = {'page': page, 'perPage': page_size}
        headers = {"Ocp-Apim-Subscription-Key": CQC_API_KEY, "Accept": "application/json"}
        
        response = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()
        return {
            'locations': data.get('locations', []),
            'total_pages': data.get('totalPages', 1)
        }
    except Exception as e:
        print(f" Error fetching page {page}: {e}")
        return {'locations': [], 'total_pages': 0}

def fetch_location_details(location_id):
    try:
        url = f"{CQC_API_BASE}/locations/{location_id}"
        headers = {"Ocp-Apim-Subscription-Key": CQC_API_KEY, "Accept": "application/json"}
        
        response = requests.get(url, headers=headers, timeout=TIMEOUT)
        if response.status_code == 200:
            return response.json()
    except Exception:
        pass
    return None

def categorize_provider(details):
    name = (details.get('locationName') or details.get('name') or '').lower()
    
    service_types = details.get('gacServiceTypes', []) or []
    service_names = " ".join([s.get('name', '') for s in service_types]).lower()
    
    specialisms = details.get('providerSpecialisms', []) or []
    specialism_names = " ".join([s.get('name', '') for s in specialisms]).lower()
    
    activities = details.get('regulatedActivities', []) or []
    activity_names = " ".join([a.get('name', '') for a in activities]).lower()
    
    metadata = f"{name} {service_names} {specialism_names} {activity_names}"

    if 'dentist' in metadata or 'dental' in metadata or 'dentistry' in metadata:
        return 'Dental', 'Dentistry'
    elif 'care home' in metadata or 'nursing home' in metadata or 'residential care' in metadata:
        return 'Care Home', 'Social Care'
    elif 'acute hospital' in metadata or 'independent hospital' in metadata:
        return 'Hospital', None
    elif 'independent doctor services' in metadata or 'consultant' in name or 'practitioner' in name:
        return 'Consultant', 'Specialist'
    elif 'gp ' in metadata or 'general practice' in metadata or 'doctors' in metadata:
        return 'GP Practice', None
    elif 'diagnostic' in metadata or 'screening' in metadata or 'clinic' in metadata:
        return 'Clinic', None
    elif 'pharmacy' in metadata:
        return 'Pharmacy', 'Pharmacy'
    elif 'optician' in metadata or 'optics' in metadata:
        return 'Optician', 'Ophthalmology'
    elif 'nhs' in metadata:
        return 'NHS Service', None
    else:
        return 'Other', None

def scan_london_providers(max_pages=None):
    print("Beginning geographic candidate filter query...")
    page = 1
    total_checked = 0
    london_count = 0
    providers_collected = []
    
    while True:
        print(f"Filtering page {page}...", end=' ', flush=True)
        result = fetch_cqc_locations(page=page, page_size=100)
        locations = result.get('locations', [])
        
        if not locations:
            print("(done)")
            break
            
        london_candidates = []
        for loc in locations:
            total_checked += 1
            postcode = loc.get('postalCode') or loc.get('postcode') or ''
            if is_london_postcode(postcode):
                london_candidates.append(loc)
                
        print(f"({len(locations)} read, found {len(london_candidates)} London)")
        
        # Deep inspect each London candidate
        for candidate in london_candidates:
            loc_id = candidate.get('locationId') or candidate.get('id')
            if loc_id:
                details = fetch_location_details(loc_id)
                if details:
                    london_count += 1
                    providers_collected.append(details)
                    print(f"  ✓ {london_count}", end='\r', flush=True)
                time.sleep(DEEP_LIMIT)
                
        if max_pages and page >= max_pages:
            print(f" (Stopping at page {page})")
            break
            
        page += 1
        time.sleep(RATE_LIMIT)
        
    print(f"\n\nScanning complete. Total checked: {total_checked}, London profiles: {london_count}\n")
    return providers_collected

def main():
    print("\n" + "=" * 70)
    print("CQC Deep-Inspection Scanner for London")
    print("=" * 70 + "\n")
    
    providers_list = scan_london_providers(max_pages=None)
    
    if not providers_list:
        print("❌ No providers found.")
        return 1
    
    print(f"Processing {len(providers_list)} providers...")
    
    # CRITICAL: Save to JSON with explicit error handling
    output_file = 'cqc_london_providers.json'
    
    try:
        # Prepare data for JSON serialization
        providers_to_save = []
        for p in providers_list:
            providers_to_save.append({
                'locationId': p.get('locationId'),
                'locationName': p.get('locationName'),
                'postalCode': p.get('postalCode'),
                'address1': p.get('address1'),
                'city': p.get('city'),
                'registrationStatus': p.get('registrationStatus'),
                'gacServiceTypes': p.get('gacServiceTypes', []),
                'providerSpecialisms': p.get('providerSpecialisms', []),
                'regulatedActivities': p.get('regulatedActivities', []),
            })
        
        # Write with explicit encoding
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(providers_to_save, f, indent=2, ensure_ascii=False)
        
        # Verify file was written
        import os
        file_size = os.path.getsize(output_file)
        print(f"\n✅ Saved {len(providers_to_save)} providers to {output_file}")
        print(f"   File size: {file_size:,} bytes")
        
        # Verify by reading back
        with open(output_file, 'r', encoding='utf-8') as f:
            verify = json.load(f)
        print(f"   Verified: {len(verify)} records in file ✓")
        
    except Exception as e:
        print(f"\n❌ Error saving JSON: {e}")
        return 1
    
    print("\n✅ Scanner complete!")
    print(f"\nYou can now run:")
    print(f"  python process_cqc_data_v2.py")

if __name__ == '__main__':
    main()
