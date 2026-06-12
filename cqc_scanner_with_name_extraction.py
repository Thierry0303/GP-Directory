#!/usr/bin/env python3
"""
CQC Scanner - Multiple Strategies to Extract Provider Names
Tries list response, detail response, and constructs fallback names
"""

import requests
import json
import time
import os
from collections import defaultdict

CQC_API_BASE = "https://api.service.cqc.org.uk/public/v1"
CQC_API_KEY = os.environ.get("CQC_KEY")

TIMEOUT = 10
RATE_LIMIT = 0.01
DEEP_LIMIT = 0.01

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

def extract_name_from_response(list_response, detail_response):
    """
    Try to extract provider name from either list or detail response
    """
    # Try list response first
    if list_response:
        for field in ['locationName', 'name', 'providerName']:
            val = list_response.get(field)
            if val and isinstance(val, str) and val.strip():
                return val.strip()
    
    # Try detail response
    if detail_response:
        for field in ['locationName', 'name', 'providerName', 'organisationName']:
            val = detail_response.get(field)
            if val and isinstance(val, str) and val.strip():
                return val.strip()
    
    # Fallback: construct from available data
    postcode = None
    if list_response:
        postcode = list_response.get('postalCode')
    elif detail_response:
        postcode = detail_response.get('postalCode')
    
    if postcode:
        return f"Provider at {postcode}"
    
    return None

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

def scan_london_providers(max_pages=None):
    print("Beginning geographic candidate filter query...")
    page = 1
    london_count = 0
    providers_collected = []
    
    while True:
        print(f"Page {page}...", end=' ', flush=True)
        result = fetch_cqc_locations(page=page, page_size=100)
        locations = result.get('locations', [])
        
        if not locations:
            print("(done)")
            break
            
        london_candidates = []
        for loc in locations:
            postcode = loc.get('postalCode') or loc.get('postcode') or ''
            if is_london_postcode(postcode):
                london_candidates.append(loc)
        
        # Deep inspect each London candidate
        for candidate in london_candidates:
            loc_id = candidate.get('locationId') or candidate.get('id')
            if loc_id:
                details = fetch_location_details(loc_id)
                
                # Extract name from both responses
                extracted_name = extract_name_from_response(candidate, details)
                
                # Merge data: use detail as base, add extracted name
                merged = details if details else candidate
                if extracted_name:
                    merged['locationName'] = extracted_name
                
                london_count += 1
                providers_collected.append(merged)
                time.sleep(DEEP_LIMIT)
        
        print(f"({len(london_candidates)} London providers, total: {london_count})")
        
        if max_pages and page >= max_pages:
            break
            
        page += 1
        time.sleep(RATE_LIMIT)
        
    print(f"\nScan complete: {london_count} London providers collected")
    return providers_collected

def main():
    print("\n" + "=" * 70)
    print("CQC Scanner - WITH NAME EXTRACTION")
    print("=" * 70 + "\n")
    
    providers_list = scan_london_providers(max_pages=None)
    
    if not providers_list:
        print("❌ No providers found.")
        return 1
    
    print(f"Processing {len(providers_list)} providers...")
    
    # Count providers with names
    with_names = [p for p in providers_list if p.get('locationName')]
    print(f"Providers with extracted names: {len(with_names)} / {len(providers_list)}")
    
    output_file = 'cqc_london_providers.json'
    
    try:
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
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(providers_to_save, f, indent=2, ensure_ascii=False)
        
        file_size = os.path.getsize(output_file)
        print(f"\n✅ Saved {len(providers_to_save)} providers to {output_file}")
        print(f"   File size: {file_size:,} bytes")
        print(f"   Providers with names: {len(with_names)}")
        
        with open(output_file, 'r', encoding='utf-8') as f:
            verify = json.load(f)
        print(f"   Verified: {len(verify)} records ✓")
        
    except Exception as e:
        print(f"\n❌ Error saving JSON: {e}")
        return 1
    
    print("\n✅ Scan complete!")

if __name__ == '__main__':
    main()
