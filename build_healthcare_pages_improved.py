#!/usr/bin/env python3
"""Build Healthcare Provider Pages - Shows postcode/service type when name missing"""

import json
import os
from collections import defaultdict

BOROUGH_MAP = {
    "E": "Tower Hamlets", "EC": "City of London", "N": "Islington",
    "NW": "Brent", "SE": "Southwark", "SW": "Westminster", "W": "Kensington & Chelsea", "WC": "Camden",
    "BR": "Bromley", "CR": "Croydon", "DA": "Bexley", "EN": "Enfield",
    "HA": "Harrow", "IG": "Redbridge", "KT": "Kingston upon Thames",
    "RM": "Havering", "SM": "Sutton", "TW": "Richmond upon Thames", "UB": "Hillingdon",
}

CATEGORIES = {
    'dentist': {'title': 'Dentists', 'slug': 'dentists', 'emoji': '🦷'},
    'clinic': {'title': 'Clinics', 'slug': 'clinics', 'emoji': '🏥'},
    'hospital': {'title': 'Hospitals', 'slug': 'hospitals', 'emoji': '🏨'},
}

def load_cqc_raw_data(filename='cqc_london_providers.json'):
    """Load the raw CQC data instead of categorized data"""
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        return []

def extract_postcode_district(postcode):
    """Extract outward code for borough mapping"""
    if not postcode:
        return ""
    pc = postcode.strip().upper().replace(" ", "")
    # Extract first 2-3 characters
    import re
    m = re.match(r'^([A-Z]{1,2}\d{1,2}[A-Z]?).*', pc)
    return m.group(1) if m else ""

def get_borough(postcode):
    """Get borough from postcode"""
    postcode_district = extract_postcode_district(postcode)
    if not postcode_district:
        return "Unknown"
    for prefix, borough in BOROUGH_MAP.items():
        if postcode_district.startswith(prefix):
            return borough
    return "Unknown"

def extract_service_type(provider):
    """Get primary service type from provider"""
    service_types = provider.get('gacServiceTypes', []) or []
    if service_types and isinstance(service_types, list) and len(service_types) > 0:
        return service_types[0].get('name', 'Service Provider')
    return 'Service Provider'

def is_dentist(provider):
    """Check if provider is a dentist based on service types"""
    service_types = provider.get('gacServiceTypes', []) or []
    service_names = " ".join([s.get('name', '').lower() for s in service_types if isinstance(service_types, list)])
    return 'dentist' in service_names or 'dental' in service_names

def is_clinic(provider):
    """Check if provider is a clinic"""
    service_types = provider.get('gacServiceTypes', []) or []
    service_names = " ".join([s.get('name', '').lower() for s in service_types if isinstance(service_types, list)])
    return 'clinic' in service_names or 'diagnostic' in service_names

def is_hospital(provider):
    """Check if provider is a hospital"""
    service_types = provider.get('gacServiceTypes', []) or []
    service_names = " ".join([s.get('name', '').lower() for s in service_types if isinstance(service_types, list)])
    return 'hospital' in service_names or 'acute' in service_names

def get_provider_display_name(provider):
    """Generate a display name using available data"""
    name = provider.get('locationName')
    
    if name and name.strip():
        return name.strip()
    
    # Fallback: Use postcode + service type
    postcode = provider.get('postalCode', 'Unknown')
    service = extract_service_type(provider)
    loc_id = provider.get('locationId', '')
    
    # Extract provider ID from locationId (format: "1-10000367985" -> "10000367985")
    provider_id = loc_id.split('-')[-1] if '-' in loc_id else loc_id
    
    return f"{service} ({postcode})"

def build_category_pages(category, providers):
    """Build index and borough pages for a category"""
    
    if not providers:
        print(f"  ⚠️  No providers for {category}")
        return
    
    cat_info = CATEGORIES[category]
    base_dir = f"provider/{cat_info['slug']}"
    os.makedirs(base_dir, exist_ok=True)
    
    # Group by borough
    by_borough = defaultdict(list)
    all_boroughs = set()
    
    for provider in providers:
        borough = get_borough(provider.get('postalCode', ''))
        by_borough[borough].append(provider)
        all_boroughs.add(borough)
    
    # Category index page
    index_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>London {cat_info['title']} Directory</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; }}
        .container {{ max-width: 1200px; margin: 0 auto; padding: 40px 20px; }}
        h1 {{ font-size: 32px; margin-bottom: 10px; }}
        .subtitle {{ color: #666; font-size: 16px; margin-bottom: 40px; }}
        .borough-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 20px; }}
        .borough-card {{ background: white; padding: 24px; border-radius: 8px; text-decoration: none; color: inherit; transition: all 0.2s; border-left: 4px solid #0066cc; cursor: pointer; }}
        .borough-card:hover {{ box-shadow: 0 8px 24px rgba(0,0,0,0.12); transform: translateY(-4px); }}
        .borough-name {{ font-size: 18px; font-weight: 600; margin-bottom: 8px; }}
        .provider-count {{ color: #999; font-size: 14px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{cat_info['emoji']} {cat_info['title']} in London</h1>
        <p class="subtitle">Find {cat_info['title'].lower()} by borough</p>
        
        <div class="borough-grid">
"""
    
    for borough in sorted(all_boroughs):
        if borough == "Unknown":
            continue
        count = len(by_borough[borough])
        # Convert borough name to slug
        borough_slug = borough.lower().replace(' ', '-').replace('&', 'and')
        index_html += f"""            <a href="{borough_slug}/" class="borough-card">
                <div class="borough-name">{borough}</div>
                <div class="provider-count">{count} providers</div>
            </a>
"""
    
    index_html += """        </div>
    </div>
</body>
</html>
"""
    
    with open(f"{base_dir}/index.html", 'w', encoding='utf-8') as f:
        f.write(index_html)
    
    print(f"  ✅ {category} index: {base_dir}/index.html")
    
    # Borough pages
    borough_count = 0
    for borough in sorted(all_boroughs):
        if borough == "Unknown":
            continue
            
        providers_list = by_borough[borough]
        # Convert borough name to slug
        borough_slug = borough.lower().replace(' ', '-').replace('&', 'and')
        borough_dir = f"{base_dir}/{borough_slug}"
        os.makedirs(borough_dir, exist_ok=True)
        
        # Sort by display name
        providers_list = sorted(providers_list, key=lambda p: get_provider_display_name(p).lower())
        
        borough_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{borough} {cat_info['title']} in London</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f5f5; color: #333; }}
        .container {{ max-width: 900px; margin: 0 auto; padding: 40px 20px; }}
        h1 {{ font-size: 28px; margin-bottom: 10px; }}
        .subtitle {{ color: #666; margin-bottom: 30px; }}
        .provider-list {{ display: grid; gap: 12px; }}
        .provider-card {{ background: white; padding: 20px; border-radius: 8px; border-left: 4px solid #0066cc; }}
        .provider-name {{ font-size: 16px; font-weight: 600; margin-bottom: 8px; }}
        .provider-detail {{ color: #666; font-size: 13px; margin-bottom: 4px; }}
        .provider-id {{ color: #999; font-size: 12px; font-family: monospace; margin-top: 8px; padding-top: 8px; border-top: 1px solid #eee; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{cat_info['emoji']} {cat_info['title']} in {borough}</h1>
        <p class="subtitle">{len(providers_list)} providers</p>
        
        <div class="provider-list">
"""
        
        for provider in providers_list:
            display_name = get_provider_display_name(provider)
            postcode = provider.get('postalCode', '')
            service = extract_service_type(provider)
            loc_id = provider.get('locationId', '')
            status = provider.get('registrationStatus', '')
            
            status_note = f"<div class='provider-detail'><strong>Status:</strong> {status}</div>" if status and status != 'Active' else ""
            
            borough_html += f"""            <div class="provider-card">
                <div class="provider-name">{display_name}</div>
                <div class="provider-detail"><strong>Type:</strong> {service}</div>
                <div class="provider-detail"><strong>Postcode:</strong> {postcode}</div>
                {status_note}
                <div class="provider-id">ID: {loc_id}</div>
            </div>
"""
        
        borough_html += """        </div>
    </div>
</body>
</html>
"""
        
        with open(f"{borough_dir}/index.html", 'w', encoding='utf-8') as f:
            f.write(borough_html)
        
        borough_count += 1
    
    print(f"     ✅ Generated {borough_count} borough pages")

def main():
    print("\n" + "=" * 70)
    print("Building Healthcare Provider Pages (Updated)")
    print("=" * 70 + "\n")
    
    # Load raw CQC data
    providers = load_cqc_raw_data()
    if not providers:
        print("❌ No CQC data found. Run CQC scanner first.")
        return 1
    
    print(f"Loaded {len(providers)} CQC provider records\n")
    
    os.makedirs("provider", exist_ok=True)
    
    # Categorize and build pages
    for category in CATEGORIES.keys():
        print(f"Processing {CATEGORIES[category]['title']}...")
        
        # Filter providers by category
        if category == 'dentist':
            category_providers = [p for p in providers if is_dentist(p)]
        elif category == 'clinic':
            category_providers = [p for p in providers if is_clinic(p)]
        elif category == 'hospital':
            category_providers = [p for p in providers if is_hospital(p)]
        else:
            category_providers = []
        
        build_category_pages(category, category_providers)
    
    print("\n" + "=" * 70)
    print("✅ SUCCESS! All pages generated!")
    print("=" * 70)
    print("\nGenerated:")
    print("  provider/dentists/ (by borough)")
    print("  provider/clinics/ (by borough)")
    print("  provider/hospitals/ (by borough)")
    print("\nPages now show: Service Type, Postcode, Provider ID, Registration Status")

if __name__ == '__main__':
    main()
