#!/usr/bin/env python3
"""
Process CQC API Response Data - Updated for Deep-Inspection Framework

Works with the enhanced CQC scanner that uses authenticated API.
Extracts: locationName, postalCode, regulatedActivities, gacServiceTypes, providerSpecialisms
"""

import json
import re
from collections import defaultdict

BOROUGH_MAP = {
    "E": "Tower Hamlets", "EC": "City of London", "N": "Islington",
    "NW": "Brent", "SE": "Southwark", "SW": "Westminster", "W": "Kensington & Chelsea", "WC": "Camden",
    "BR": "Bromley", "CR": "Croydon", "DA": "Bexley", "EN": "Enfield",
    "HA": "Harrow", "IG": "Redbridge", "KT": "Kingston upon Thames",
    "RM": "Havering", "SM": "Sutton", "TW": "Richmond upon Thames", "UB": "Hillingdon",
}

def load_cqc_api_response(filename='cqc_london_providers.json'):
    """Load CQC API response data."""
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: {filename} not found")
        return []

def extract_postcode_district(postcode):
    """Extract outward code for borough mapping."""
    if not postcode:
        return ""
    pc = postcode.strip().upper().replace(" ", "")
    m = re.match(r'^([A-Z]{1,2}\d{1,2}[A-Z]?).*', pc)
    return m.group(1) if m else ""

def get_borough_from_postcode(postcode_district):
    """Map postcode to borough."""
    if not postcode_district:
        return "Unknown"
    
    for prefix, borough in BOROUGH_MAP.items():
        if postcode_district.startswith(prefix):
            return borough
    return "Unknown"

def categorize_cqc_provider(provider):
    """
    Categorize provider using CQC metadata arrays.
    Uses: gacServiceTypes, providerSpecialisms, regulatedActivities
    """
    name = (provider.get('locationName') or provider.get('name') or '').lower()
    
    # Extract CQC metadata
    service_types = provider.get('gacServiceTypes', []) or []
    service_names = " ".join([s.get('name', '') for s in service_types if isinstance(service_types, list)]).lower()
    
    specialisms = provider.get('providerSpecialisms', []) or []
    specialism_names = " ".join([s.get('name', '') for s in specialisms if isinstance(specialisms, list)]).lower()
    
    activities = provider.get('regulatedActivities', []) or []
    activity_names = " ".join([a.get('name', '') for a in activities if isinstance(activities, list)]).lower()
    
    metadata = f"{name} {service_names} {specialism_names} {activity_names}"
    
    # Classification
    if 'dentist' in metadata or 'dental' in metadata or 'dentistry' in metadata:
        return 'dentist', 'Dentistry'
    elif 'care home' in metadata or 'nursing home' in metadata or 'residential care' in metadata:
        return 'care_home', 'Social Care'
    elif 'hospital' in metadata:
        return 'hospital', 'Hospital'
    elif 'consultant' in name or 'independent doctor' in metadata:
        return 'specialist', 'Specialist'
    elif 'gp' in metadata or 'general practice' in metadata:
        return 'gp', 'GP Practice'
    elif 'clinic' in metadata or 'diagnostic' in metadata:
        return 'clinic', 'Clinic'
    elif 'pharmacy' in metadata:
        return 'pharmacy', 'Pharmacy'
    elif 'optician' in metadata or 'optics' in metadata:
        return 'optician', 'Optometry'
    else:
        return 'other', None

def extract_specialty(provider):
    """Extract specialty from CQC metadata."""
    name = (provider.get('locationName') or '').lower()
    
    activities = provider.get('regulatedActivities', []) or []
    activity_string = " ".join([a.get('name', '') for a in activities if isinstance(activities, list)]).lower()
    
    specialisms = provider.get('providerSpecialisms', []) or []
    specialism_string = " ".join([s.get('name', '') for s in specialisms if isinstance(specialisms, list)]).lower()
    
    combined = f"{name} {activity_string} {specialism_string}"
    
    specialties = {
        'cardiology': ['cardiac', 'cardio', 'heart'],
        'dermatology': ['dermatolog', 'skin'],
        'orthopaedics': ['ortho', 'bone', 'joint'],
        'ophthalmology': ['ophthalmolog', 'eye'],
        'neurology': ['neurology', 'neuro', 'brain'],
        'oncology': ['cancer', 'oncolog'],
        'mental health': ['mental health', 'psychiatr'],
        'paediatrics': ['paediatric', 'pediatric', 'child'],
        'maternity': ['maternity', 'midwife'],
        'physiotherapy': ['physiotherapy', 'physio'],
        'surgery': ['surgical', 'surgeon'],
        'diagnostics': ['diagnostic', 'imaging', 'screening'],
        'cosmetic surgery': ['cosmetic', 'aesthetic'],
    }
    
    for specialty, keywords in specialties.items():
        for keyword in keywords:
            if keyword in combined:
                return specialty.title()
    return None

def process_providers(providers):
    """Process and categorize providers."""
    
    by_category = defaultdict(list)
    by_specialty = defaultdict(list)
    
    for provider in providers:
        category, specialty = categorize_cqc_provider(provider)
        
        postcode = provider.get('postalCode') or provider.get('postcode') or ''
        postcode_district = extract_postcode_district(postcode)
        borough = get_borough_from_postcode(postcode_district)
        
        if not specialty:
            specialty = extract_specialty(provider)
        
        # Normalize provider data
        normalized = {
            'id': provider.get('locationId') or provider.get('id'),
            'name': provider.get('locationName') or provider.get('name'),
            'postcode': postcode,
            'postcode_district': postcode_district,
            'borough': borough,
            'address': provider.get('address1') or provider.get('address'),
            'type': category,
            'specialty': specialty,
            'status': provider.get('registrationStatus'),
        }
        
        by_category[category].append(normalized)
        if specialty:
            by_specialty[specialty].append(normalized)
    
    return by_category, by_specialty

def save_categorized_data(by_category, by_specialty):
    """Save categorized data to files."""
    
    categories_to_save = {
        'dentist': 'dentists.json',
        'clinic': 'clinics.json',
        'hospital': 'hospitals.json',
        'optician': 'opticians.json',
        'specialist': 'specialists.json',
        'pharmacy': 'pharmacies.json',
    }
    
    print("\nSaving categorized data:")
    for category, filename in categories_to_save.items():
        providers = by_category.get(category, [])
        if providers:
            with open(filename, 'w') as f:
                json.dump(providers, f, indent=2)
            print(f"  ✅ {filename:.<35} {len(providers):>6} providers")
    
    # Specialties breakdown
    specialties_breakdown = {
        spec: len(providers) 
        for spec, providers in sorted(by_specialty.items(), key=lambda x: len(x[1]), reverse=True)
    }
    
    with open('specialties_breakdown.json', 'w') as f:
        json.dump(specialties_breakdown, f, indent=2)
    
    print(f"  ✅ {'specialties_breakdown.json':.<35} {len(specialties_breakdown):>6} specialties")

def main():
    print("\n" + "=" * 70)
    print("Processing CQC API Data - Categorizing Providers")
    print("=" * 70 + "\n")
    
    providers = load_cqc_api_response()
    if not providers:
        return 1
    
    print(f"Loaded {len(providers)} CQC provider records\n")
    print("Categorizing...")
    
    by_category, by_specialty = process_providers(providers)
    
    # Display summary
    print("\nCategory Breakdown:")
    for category in sorted(by_category.keys()):
        count = len(by_category[category])
        print(f"  {category:.<35} {count:>6}")
    
    print(f"\nTop 15 Specialties:")
    sorted_specs = sorted(by_specialty.items(), key=lambda x: len(x[1]), reverse=True)
    for specialty, providers_list in sorted_specs[:15]:
        print(f"  {specialty:.<35} {len(providers_list):>6}")
    
    # Save
    save_categorized_data(by_category, by_specialty)
    
    print("\n" + "=" * 70)
    print("✅ Processing complete!")
    print("=" * 70)
    print("\nNext: python build_healthcare_pages.py")

if __name__ == '__main__':
    main()
