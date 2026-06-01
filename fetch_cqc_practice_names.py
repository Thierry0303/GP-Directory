#!/usr/bin/env python3
"""
Fetch public-facing GP practice names from CQC API.

CQC (Care Quality Commission) inspects every UK GP practice by law.
They provide a free, public API with correct practice display names
(not NHS contract names like "S H Vaghela & Dr V N Patel").

This script:
1. Reads your current gps.json with ODS codes
2. Queries CQC API to find provider data by ODS code
3. Extracts the public-facing practice name
4. Updates gps.json with corrected names
5. Tracks which records were updated vs. remained unchanged

API: https://www.cqc.org.uk/api
No authentication required.
"""

import json
import requests
import time
from pathlib import Path
from collections import defaultdict

CQC_API_BASE = "https://www.cqc.org.uk/api/v1"
CQC_TIMEOUT = 10
CQC_RATE_LIMIT_DELAY = 0.5  # Be respectful to CQC API

def fetch_cqc_provider_by_ods(ods_code):
    """
    Query CQC API to find a provider by ODS code.
    
    CQC API returns provider details including the display name.
    Returns: dict with 'id', 'name', 'location' info, or None if not found.
    """
    try:
        # CQC API endpoint: search providers
        # Filter by ODS code in the external ID
        url = f"{CQC_API_BASE}/providers"
        params = {
            'filters': f"location.gpraqexternalid[eq]{ods_code}",
            'pageSize': 1,
        }
        
        response = requests.get(url, params=params, timeout=CQC_TIMEOUT)
        response.raise_for_status()
        
        data = response.json()
        locations = data.get('locations', [])
        
        if locations:
            # Return the first match
            loc = locations[0]
            return {
                'cqc_id': loc.get('id'),
                'cqc_name': loc.get('name'),
                'cqc_registered': loc.get('registered'),
                'cqc_type': loc.get('type'),
            }
        
        return None
    
    except requests.exceptions.Timeout:
        print(f"  CQC API timeout for ODS {ods_code}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  CQC API error for ODS {ods_code}: {e}")
        return None
    except Exception as e:
        print(f"  Unexpected error fetching CQC data for ODS {ods_code}: {e}")
        return None

def load_gps_json(filename='gps.json'):
    """Load current gps.json."""
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        return []

def save_gps_json(data, filename='gps.json'):
    """Save updated gps.json."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data)} records to {filename}")

def enrich_with_cqc_names(gps_records):
    """
    Enrich gps records with CQC public-facing names.
    
    Args:
        gps_records: list of practice dicts with 'ods_code', 'name', etc.
    
    Returns:
        Updated list with 'cqc_name', 'cqc_id' fields added/updated
    """
    
    print(f"Enriching {len(gps_records)} practices with CQC data...")
    
    updated = 0
    skipped = 0
    not_found = 0
    errors = 0
    
    for i, practice in enumerate(gps_records):
        ods_code = practice.get('ods_code')
        
        if not ods_code:
            skipped += 1
            continue
        
        if (i + 1) % 50 == 0 or i == len(gps_records) - 1:
            print(f"  {i + 1}/{len(gps_records)} processed...")
        
        # Fetch from CQC
        cqc_data = fetch_cqc_provider_by_ods(ods_code)
        
        if cqc_data:
            # Store original NHS name for reference
            practice['nhs_name'] = practice.get('name')
            
            # Update with CQC public-facing name
            cqc_name = cqc_data.get('cqc_name', '')
            if cqc_name and cqc_name != practice.get('name'):
                practice['name'] = cqc_name
                practice['name_source'] = 'cqc'
                updated += 1
            
            # Store CQC metadata
            practice['cqc_id'] = cqc_data.get('cqc_id')
            practice['cqc_registered'] = cqc_data.get('cqc_registered')
        else:
            not_found += 1
            # Mark that we tried but CQC didn't have it
            practice['name_source'] = practice.get('name_source', 'nhs_fhir')
        
        # Rate limiting - be respectful to CQC API
        time.sleep(CQC_RATE_LIMIT_DELAY)
    
    print(f"\nCQC Enrichment Results:")
    print(f"  ✅ Updated names: {updated}")
    print(f"  ℹ️  Not found in CQC: {not_found}")
    print(f"  ⊘ Skipped (no ODS): {skipped}")
    print(f"  Coverage: {updated}/{len(gps_records)} ({100*updated//len(gps_records)}%)")
    
    return gps_records

def compare_before_after(original, updated):
    """
    Generate a diff report showing which names changed.
    Useful for debugging and auditing.
    """
    changes = []
    
    for orig, upd in zip(original, updated):
        if orig.get('name') != upd.get('name'):
            changes.append({
                'ods_code': upd.get('ods_code'),
                'nhs_name': orig.get('name'),
                'cqc_name': upd.get('name'),
            })
    
    # Save report
    report_file = 'cqc_name_changes.json'
    with open(report_file, 'w') as f:
        json.dump(changes, f, indent=2)
    
    print(f"\nName changes saved to {report_file}")
    return changes

def main():
    print("=" * 70)
    print("CQC Practice Name Enrichment")
    print("=" * 70)
    
    # Load current data
    print("\nLoading gps.json...")
    gps_original = load_gps_json()
    if not gps_original:
        print("No data to enrich")
        return 1
    
    print(f"Loaded {len(gps_original)} practices")
    
    # Make a backup
    backup_file = 'gps.json.backup'
    with open(backup_file, 'w') as f:
        json.dump(gps_original, f, indent=2)
    print(f"Backup saved to {backup_file}")
    
    # Enrich with CQC data
    gps_updated = enrich_with_cqc_names(gps_original)
    
    # Generate diff report
    changes = compare_before_after(gps_original, gps_updated)
    
    # Save updated data
    save_gps_json(gps_updated)
    
    print("\n" + "=" * 70)
    print(f"✅ Enrichment complete!")
    print(f"   {len(changes)} practice names corrected")
    print(f"   Original backup: {backup_file}")
    print("=" * 70)
    
    return 0

if __name__ == '__main__':
    exit(main())
