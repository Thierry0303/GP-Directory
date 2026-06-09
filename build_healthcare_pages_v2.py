#!/usr/bin/env python3
"""Build Healthcare Provider Pages - FIXED for Windows encoding + None names"""

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

def load_category_data(category):
    """Load category JSON file."""
    filename = f"{category}s.json"
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []

def slug(text):
    """Convert to URL-safe slug."""
    return text.lower().replace(' ', '-').replace('&', 'and').replace("'", '')

def get_borough(postcode_district):
    """Get borough from postcode."""
    if not postcode_district:
        return "Unknown"
    for prefix, borough in BOROUGH_MAP.items():
        if postcode_district.startswith(prefix):
            return borough
    return "Unknown"

def build_category_pages(category, providers):
    """Build index and borough pages for a category."""
    
    cat_info = CATEGORIES[category]
    base_dir = f"provider/{cat_info['slug']}"
    os.makedirs(base_dir, exist_ok=True)
    
    # Group by borough
    by_borough = defaultdict(list)
    all_boroughs = set()
    
    for provider in providers:
        borough = get_borough(provider.get('postcode_district', ''))
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
        borough_slug = slug(borough)
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
    
    print(f"  OK {category} index: {base_dir}/index.html")
    
    # Borough pages
    borough_count = 0
    for borough in sorted(all_boroughs):
        if borough == "Unknown":
            continue
            
        providers_list = by_borough[borough]
        borough_slug = slug(borough)
        borough_dir = f"{base_dir}/{borough_slug}"
        os.makedirs(borough_dir, exist_ok=True)
        
        # FIX: Handle None names in sorting
        providers_list = sorted(providers_list, key=lambda p: (p.get('name') or 'Unnamed').lower())
        
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
        .provider-name {{ font-size: 16px; font-weight: 600; margin-bottom: 4px; }}
        .provider-spec {{ color: #666; font-size: 13px; margin-bottom: 6px; }}
        .provider-address {{ color: #999; font-size: 12px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{cat_info['emoji']} {cat_info['title']} in {borough}</h1>
        <p class="subtitle">{len(providers_list)} providers</p>
        
        <div class="provider-list">
"""
        
        for provider in providers_list:
            name = provider.get('name') or 'Unnamed Provider'
            spec = provider.get('specialty') or ''
            address = provider.get('address') or ''
            
            spec_html = f'<div class="provider-spec">{spec}</div>' if spec else ''
            addr_html = f'<div class="provider-address">{address}</div>' if address else ''
            
            borough_html += f"""            <div class="provider-card">
                <div class="provider-name">{name}</div>
                {spec_html}
                {addr_html}
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
    
    print(f"     Generated {borough_count} borough pages")

def main():
    print("\n" + "=" * 70)
    print("Building Healthcare Provider Pages")
    print("=" * 70 + "\n")
    
    os.makedirs("provider", exist_ok=True)
    
    for category in CATEGORIES.keys():
        print(f"Processing {CATEGORIES[category]['title']}...")
        providers = load_category_data(category)
        
        if not providers:
            print(f"  No data found")
            continue
        
        build_category_pages(category, providers)
    
    print("\n" + "=" * 70)
    print("SUCCESS! All pages generated!")
    print("=" * 70)
    print("\nGenerated:")
    print("  provider/dentists/ (all borough pages)")
    print("  provider/clinics/ (all borough pages)")
    print("  provider/hospitals/ (all borough pages)")
    print("\nReady to commit and deploy to GitHub!")

if __name__ == '__main__':
    main()