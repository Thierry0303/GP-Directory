#!/usr/bin/env python3
"""
Simple CQC API Test
Shows exactly what the API returns for a single location detail
"""

import requests
import json
import os

CQC_API_KEY = os.environ.get("CQC_KEY")
CQC_API_BASE = "https://api.service.cqc.org.uk/public/v1"

if not CQC_API_KEY:
    print("❌ ERROR: CQC_KEY environment variable not set")
    print("Set it with: $env:CQC_KEY='your-key-here'  (PowerShell)")
    exit(1)

print("Fetching first page of locations...")
headers = {"Ocp-Apim-Subscription-Key": CQC_API_KEY, "Accept": "application/json"}

# Get first location
url = f"{CQC_API_BASE}/locations"
resp = requests.get(url, params={'page': 1, 'perPage': 1}, headers=headers)
data = resp.json()

if not data.get('locations'):
    print("❌ No locations returned")
    exit(1)

first_location = data['locations'][0]
loc_id = first_location.get('locationId')

print(f"\n✅ Got first location ID: {loc_id}")
print(f"\nFIRST LOCATION (list response):")
print(json.dumps(first_location, indent=2))

# Now fetch the detail
print("\n" + "=" * 70)
print("Fetching DETAIL for that location...")
print("=" * 70)

detail_url = f"{CQC_API_BASE}/locations/{loc_id}"
detail_resp = requests.get(detail_url, headers=headers)

if detail_resp.status_code == 200:
    detail_data = detail_resp.json()
    print(f"\n✅ Detail response received")
    print(f"\nDETAIL LOCATION (detail endpoint response):")
    print(json.dumps(detail_data, indent=2))
else:
    print(f"❌ Detail fetch failed: {detail_resp.status_code}")
    print(detail_resp.text)

# Analyze what fields we have
print("\n" + "=" * 70)
print("ANALYSIS")
print("=" * 70)
print(f"\nList response has these top-level fields:")
for key in first_location.keys():
    val = first_location[key]
    if isinstance(val, (dict, list)):
        print(f"  {key}: {type(val).__name__}")
    else:
        print(f"  {key}: {val}")

if detail_resp.status_code == 200:
    print(f"\nDetail response has these top-level fields:")
    for key in detail_data.keys():
        val = detail_data[key]
        if isinstance(val, (dict, list)):
            print(f"  {key}: {type(val).__name__}")
        else:
            print(f"  {key}: {val}")
