#!/usr/bin/env python3
"""
Fetch London private healthcare clinics + consultants from the CQC public API.

This is the foundation for the private side of londongp.directory. It builds
the equivalent of gps.json but for non-NHS providers — private GPs, specialist
clinics, and the consultant-led clinics that bridge into the wider "private
doctor directory" you want to build later.

Output: private_clinics.json — array of records with the same shape as
gps.json's downstream merged.json so the existing template + borough +
practice page generators can render them with minimal changes.

Data source
-----------
CQC publishes a public REST API at https://api.cqc.org.uk/public/v1 that
contains every regulated healthcare location in England, free of charge.
You need a free Subscription Key (1-minute signup):

    https://www.cqc.org.uk/about-us/transparency/using-cqc-data

Once you have the key, set it in the environment before running:

    export CQC_KEY=xxxxxxxxxxxx
    python3 fetch_private_clinics.py

Or pass --key=... on the command line. The script will refuse to run
without one.

What it filters for
-------------------
A "private clinic" for our purposes is a CQC-registered location that:
  1. Has a London postcode (postcode prefix in our existing PC dict).
  2. Provides primary care or consultant-led services
     ("Doctors Consultation Service Independent",
      "Doctors Treatment Service",
      "Hospital Services for People with Mental Health Needs",
      etc. — see SERVICE_TYPES below).
  3. Is NOT already in gps.json (i.e. doesn't have an NHS GMS contract).
     This is how we separate "private" from "NHS".
  4. Is currently registered (not deregistered).

Layering on consultant-level data (PHIN, GMC) is a separate later step
that joins on this foundation.
"""

import json, os, re, sys, time, argparse, urllib.request, urllib.error, urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"
OUT_JSON = ROOT / "private_clinics.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

# Service types we care about (primary care + consultant-led private services).
# CQC has ~30 service-type strings; these are the ones relevant to a doctor
# directory. Edit if you want to broaden (e.g. add diagnostic services).
SERVICE_TYPES = {
    "Doctors consultation service - Independent",
    "Doctors treatment service",
    "Hospital services for people with mental health needs",
    "Acute services with overnight beds",
    "Acute services without overnight beds / listed acute services with or without overnight beds",
    "Diagnostic and screening service",
    "Diagnostic and/or screening services",
    "Specialist college service",
    "Long term conditions services",
    "Mobile doctors service",
    "Rehabilitation services",
}

# Post-filter speciality classification by name pattern. Useful for the
# borough/specialty pages we'll build later.
SPECIALITY_PATTERNS = [
    ("cardiology",      r"\b(cardio|heart)\b"),
    ("dermatology",     r"\b(derma|skin clinic)\b"),
    ("paediatrics",     r"\b(paediatric|child(?:ren)?'s clinic)\b"),
    ("orthopaedics",    r"\b(orthop|joint|spine|bone)\b"),
    ("ophthalmology",   r"\b(ophthalm|eye clinic|vision)\b"),
    ("ent",             r"\b(ent\b|ear,? nose|otolaryng)\b"),
    ("gynaecology",     r"\b(gynaec|women's health|fertility)\b"),
    ("psychiatry",      r"\b(psychiatr|mental health|psycholog)\b"),
    ("dentistry",       r"\bdent(?:al|ist)\b"),
    ("cosmetic",        r"\b(cosmet|aesthet|plastic surger)\b"),
    ("urology",         r"\burolog\b"),
    ("oncology",        r"\b(oncolog|cancer)\b"),
    ("gastroenterology",r"\bgastroenterolog\b"),
    ("private gp",      r"\b(general practi|private gp|gp service)\b"),
]
def classify_specialities(name, services):
    blob = (name or "") + " " + " ".join(services)
    found = []
    for tag, rx in SPECIALITY_PATTERNS:
        if re.search(rx, blob, re.IGNORECASE):
            found.append(tag)
    return found or ["other"]

# London postcodes (mirror of the dict in refresh_nhs_data.py — keep them in
# sync if you add new districts).
LONDON_POSTCODE_PREFIXES = {
    # Inner London (E, EC, N, NW, SE, SW, W, WC)
    "EC1A","EC1R","EC1V","EC2A","WC1B","WC1E","WC1N","WC1X","WC2A","WC2B","WC2H","WC2N",
    "E1","E2","E3","E4","E5","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15",
    "E16","E17","E18","E20",
    "N1","N4","N5","N6","N7","N8","N9","N10","N11","N12","N13","N14","N15","N16",
    "N17","N18","N19","N20","N21","N22",
    "NW1","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11",
    "SE1","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9","SE10","SE11","SE12",
    "SE13","SE14","SE15","SE16","SE17","SE18","SE19","SE20","SE21","SE22","SE23",
    "SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1P","SW1V","SW1W","SW1X","SW2","SW3","SW4","SW5","SW6","SW7",
    "SW8","SW9","SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18",
    "SW19","SW20",
    "W1","W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
    # Outer London — Greater London
    "BR1","BR2","BR3","BR4","BR5","BR6","BR7","BR8",                # Bromley
    "CR0","CR2","CR3","CR4","CR5","CR6","CR7","CR8","CR9",          # Croydon
    "DA1","DA5","DA6","DA7","DA8","DA14","DA15","DA16","DA17","DA18", # Bexley
    "EN1","EN2","EN3","EN4","EN5","EN7","EN8","EN9",                # Enfield
    "HA0","HA1","HA2","HA3","HA4","HA5","HA6","HA7","HA8","HA9",    # Harrow
    "IG1","IG2","IG3","IG4","IG5","IG6","IG7","IG8","IG11",         # Redbridge / B&D
    "KT1","KT2","KT3","KT4","KT5","KT6","KT7","KT8","KT9",          # Kingston
    "RM1","RM2","RM3","RM4","RM5","RM6","RM7","RM8","RM9","RM10","RM11","RM12","RM13","RM14",  # Havering
    "SM1","SM2","SM3","SM4","SM5","SM6",                            # Sutton
    "TW1","TW2","TW3","TW4","TW5","TW6","TW7","TW8","TW9","TW10","TW11","TW12","TW13","TW14",  # Richmond / Hounslow
    "UB1","UB2","UB3","UB4","UB5","UB6","UB7","UB8","UB9","UB10","UB11",  # Hillingdon / Ealing
}

BOROUGH_MAP = {
    # Same mapping as refresh_nhs_data.py — kept locally so this script is
    # self-contained. If you ever centralise this, import from a shared module.
    "E10":"Waltham Forest","E11":"Redbridge","E12":"Newham","E13":"Newham",
    "E14":"Tower Hamlets","E15":"Newham","E16":"Newham","E17":"Waltham Forest",
    "E18":"Redbridge","E20":"Newham",
    "EC1A":"City of London","EC1R":"Islington","EC1V":"Islington",
    "N10":"Haringey","N11":"Barnet","N12":"Barnet","N13":"Enfield",
    "N14":"Enfield","N15":"Haringey","N16":"Hackney","N17":"Haringey",
    "N18":"Enfield","N19":"Islington","N20":"Barnet","N21":"Enfield","N22":"Haringey",
    "NW1":"Camden","NW2":"Brent","NW3":"Camden","NW4":"Barnet","NW5":"Camden",
    "NW6":"Brent","NW7":"Barnet","NW8":"Westminster","NW9":"Brent",
    "NW10":"Brent","NW11":"Barnet",
    "SE1":"Southwark","SE2":"Greenwich","SE3":"Greenwich","SE4":"Lewisham",
    "SE5":"Southwark","SE6":"Lewisham","SE7":"Greenwich","SE8":"Lewisham",
    "SE9":"Greenwich","SE10":"Greenwich","SE11":"Lambeth","SE12":"Lewisham",
    "SE13":"Lewisham","SE14":"Lewisham","SE15":"Southwark","SE16":"Southwark",
    "SE17":"Southwark","SE18":"Greenwich","SE19":"Bromley","SE20":"Bromley",
    "SE21":"Southwark","SE22":"Southwark","SE23":"Lewisham","SE24":"Lambeth",
    "SE25":"Croydon","SE26":"Lewisham","SE27":"Lambeth","SE28":"Greenwich",
    "SW1A":"Westminster","SW1E":"Westminster","SW1P":"Westminster","SW1V":"Westminster",
    "SW1W":"Westminster","SW1X":"Westminster",
    "SW2":"Lambeth","SW3":"Kensington & Chelsea","SW4":"Lambeth",
    "SW5":"Kensington & Chelsea","SW6":"Hammersmith & Fulham",
    "SW7":"Kensington & Chelsea","SW8":"Lambeth","SW9":"Lambeth",
    "SW10":"Kensington & Chelsea","SW11":"Wandsworth","SW12":"Wandsworth",
    "SW13":"Richmond","SW14":"Richmond","SW15":"Wandsworth","SW16":"Lambeth",
    "SW17":"Wandsworth","SW18":"Wandsworth","SW19":"Merton","SW20":"Merton",
    "W1":"Westminster","W2":"Westminster","W3":"Ealing","W4":"Hounslow",
    "W5":"Ealing","W6":"Hammersmith & Fulham","W7":"Ealing","W8":"Kensington & Chelsea",
    "W9":"Westminster","W10":"Kensington & Chelsea","W11":"Kensington & Chelsea",
    "W12":"Hammersmith & Fulham","W13":"Ealing","W14":"Hammersmith & Fulham",
    "WC1B":"Camden","WC1E":"Camden","WC1N":"Camden","WC1X":"Islington",
    "WC2A":"Camden","WC2B":"Westminster","WC2H":"Westminster","WC2N":"Westminster",
    # Outer London
    "BR1":"Bromley","BR2":"Bromley","BR3":"Bromley","BR4":"Bromley","BR5":"Bromley",
    "BR6":"Bromley","BR7":"Bromley","BR8":"Bromley",
    "CR0":"Croydon","CR2":"Croydon","CR3":"Croydon","CR4":"Merton","CR5":"Croydon",
    "CR6":"Croydon","CR7":"Croydon","CR8":"Croydon","CR9":"Croydon",
    "DA1":"Bexley","DA5":"Bexley","DA6":"Bexley","DA7":"Bexley","DA8":"Bexley",
    "DA14":"Bexley","DA15":"Bexley","DA16":"Bexley","DA17":"Bexley","DA18":"Bexley",
    "EN1":"Enfield","EN2":"Enfield","EN3":"Enfield","EN4":"Enfield","EN5":"Barnet",
    "EN7":"Enfield","EN8":"Enfield","EN9":"Enfield",
    "HA0":"Brent","HA1":"Harrow","HA2":"Harrow","HA3":"Harrow","HA4":"Hillingdon",
    "HA5":"Harrow","HA6":"Hillingdon","HA7":"Harrow","HA8":"Barnet","HA9":"Brent",
    "IG1":"Redbridge","IG2":"Redbridge","IG3":"Redbridge","IG4":"Redbridge",
    "IG5":"Redbridge","IG6":"Redbridge","IG7":"Redbridge","IG8":"Redbridge",
    "IG11":"Barking & Dagenham",
    "KT1":"Kingston","KT2":"Kingston","KT3":"Kingston","KT4":"Kingston","KT5":"Kingston",
    "KT6":"Kingston","KT7":"Kingston","KT8":"Richmond","KT9":"Kingston",
    "RM1":"Havering","RM2":"Havering","RM3":"Havering","RM4":"Havering","RM5":"Havering",
    "RM6":"Barking & Dagenham","RM7":"Havering","RM8":"Barking & Dagenham",
    "RM9":"Barking & Dagenham","RM10":"Barking & Dagenham","RM11":"Havering",
    "RM12":"Havering","RM13":"Havering","RM14":"Havering",
    "SM1":"Sutton","SM2":"Sutton","SM3":"Sutton","SM4":"Merton","SM5":"Sutton","SM6":"Sutton",
    "TW1":"Richmond","TW2":"Richmond","TW3":"Hounslow","TW4":"Hounslow","TW5":"Hounslow",
    "TW6":"Hillingdon","TW7":"Hounslow","TW8":"Hounslow","TW9":"Richmond","TW10":"Richmond",
    "TW11":"Richmond","TW12":"Richmond","TW13":"Hounslow","TW14":"Hounslow",
    "UB1":"Ealing","UB2":"Ealing","UB3":"Hillingdon","UB4":"Hillingdon","UB5":"Ealing",
    "UB6":"Ealing","UB7":"Hillingdon","UB8":"Hillingdon","UB9":"Hillingdon",
    "UB10":"Hillingdon","UB11":"Hillingdon",
}

# ---------------------------------------------------------------- helpers

def postcode_district(pc):
    if not pc: return ""
    pc = pc.strip().upper()
    if " " in pc: return pc.split()[0]
    pc = pc.replace(" ", "")
    return pc[:-3] if len(pc) >= 5 else pc

def borough_for_postcode(pc):
    d = postcode_district(pc)
    if d in BOROUGH_MAP: return BOROUGH_MAP[d]
    m = re.match(r"^([A-Z]{1,2}\d)", d)
    return BOROUGH_MAP.get(m.group(1), "") if m else ""

def is_london(pc):
    d = postcode_district(pc)
    if d in LONDON_POSTCODE_PREFIXES: return True
    m = re.match(r"^([A-Z]{1,2}\d)", d)
    return bool(m and m.group(1) in LONDON_POSTCODE_PREFIXES)

# ---------------------------------------------------------------- CQC

def cqc_get(path, params, key, retries=3):
    """Fetch JSON from the CQC API with exponential backoff."""
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (private-clinics fetcher)",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                wait = 2 ** attempt
                print(f"  CQC {e.code} — retrying in {wait}s")
                time.sleep(wait)
                continue
            raise
        except urllib.error.URLError as e:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            raise

def fetch_london_locations(key):
    """
    Walk the CQC /locations endpoint, paginating through ALL UK locations
    and filtering to London by postcode prefix client-side.

    The CQC public API doesn't accept geographic region filters in the
    location index endpoint, so we have to pull everything and filter
    locally. ~50k locations total, takes 5–10 minutes.
    """
    print("Fetching CQC locations index (this takes 5–10 minutes)…")
    page = 1
    per_page = 1000
    london_locations = []
    total_seen = 0
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        items = data.get("locations", [])
        total_seen += len(items)
        # Filter to London postcodes — locations summary include postalCode
        london_items = [loc for loc in items
                        if is_london(loc.get("postalCode", ""))]
        london_locations.extend(london_items)
        total_pages = data.get("totalPages", 1)
        print(f"  page {page}/{total_pages} — {len(items)} fetched, "
              f"{len(london_items)} London ({len(london_locations)} cumulative)")
        if page >= total_pages: break
        page += 1
        time.sleep(0.2)
    print(f"Scanned {total_seen} UK locations, kept {len(london_locations)} London ones.")
    return london_locations

def fetch_location_detail(location_id, key):
    """Get full record for a single location (services, ratings, address)."""
    return cqc_get(f"/locations/{location_id}", {}, key)

# ---------------------------------------------------------------- main

def main():
    parser = argparse.ArgumentParser(description="Fetch London private clinics from CQC")
    parser.add_argument("--key", default=os.environ.get("CQC_KEY"),
        help="CQC API subscription key. Defaults to $CQC_KEY env var.")
    parser.add_argument("--limit", type=int, default=0,
        help="Stop after N detailed lookups (for testing). 0 = no limit.")
    args = parser.parse_args()

    if not args.key:
        sys.exit("Need a CQC API key. Set $CQC_KEY or pass --key=...\n"
                 "Register free at https://www.cqc.org.uk/about-us/transparency/using-cqc-data")

    # Load existing NHS GP ODS codes so we can exclude them.
    nhs_ods_codes = set()
    if GPS_JSON.exists():
        try:
            for d in json.loads(GPS_JSON.read_text()):
                code = d.get("ods_code") or d.get("o")
                if code: nhs_ods_codes.add(code.upper())
        except Exception as e:
            print(f"  warning: couldn't parse gps.json — {e}")
    print(f"Excluding {len(nhs_ods_codes)} NHS GP ODS codes already in gps.json.")

    # 1. Pull all London CQC locations (summary records).
    summary = fetch_london_locations(args.key)
    print(f"\n{len(summary)} CQC locations in London.")

    # 2. Filter summary records to candidates worth looking up in detail.
    #    We only want primary-care / consultant-led / specialist private
    #    healthcare. Drop care homes, dental-only, ambulance services, etc.
    candidates = []
    for loc in summary:
        loc_id = loc.get("locationId") or loc.get("locationID")
        if not loc_id: continue
        # CQC marks deregistered locations explicitly.
        if loc.get("deregistrationDate"): continue
        candidates.append(loc_id)

    if args.limit:
        candidates = candidates[:args.limit]
    print(f"{len(candidates)} active candidate locations — fetching details…")

    # 3. Fetch detail for each candidate. CQC API supports ~5 requests/s.
    out = []
    for i, loc_id in enumerate(candidates, 1):
        try:
            d = fetch_location_detail(loc_id, args.key)
        except Exception as e:
            print(f"  [{i}/{len(candidates)}] {loc_id}: skip ({e})")
            continue
        if i % 100 == 0:
            print(f"  {i}/{len(candidates)} fetched, {len(out)} kept so far")

        ods = (d.get("odsCode") or "").upper().strip()
        if ods and ods in nhs_ods_codes:
            continue  # this is an NHS practice — handled by refresh_nhs_data.py

        pc = (d.get("postalCode") or "").strip()
        if not is_london(pc):
            continue

        services = [s.get("name", "") for s in d.get("gacServiceTypes", [])]
        # Keep only locations whose service mix overlaps with our SERVICE_TYPES.
        if not any(s in SERVICE_TYPES for s in services):
            continue

        name = (d.get("name") or "").strip()
        addr_lines = [d.get("postalAddressLine1",""), d.get("postalAddressLine2","")]
        city = d.get("postalAddressTownCity", "London")
        address = ", ".join(filter(None, addr_lines + [city]))
        phone = (d.get("mainPhoneNumber") or "").strip()
        website = (d.get("website") or "").strip()
        rating = ((d.get("currentRatings", {}) or {}).get("overall", {}) or {}).get("rating", "")
        cqc_url = f"https://www.cqc.org.uk/location/{loc_id}"
        lat = (d.get("onspdLatitude") or
               (d.get("geolocation") or {}).get("latitude"))
        lng = (d.get("onspdLongitude") or
               (d.get("geolocation") or {}).get("longitude"))

        out.append({
            "id":     loc_id,                       # CQC location ID
            "n":      name,
            "a":      address,
            "p":      pc,
            "ph":     phone,
            "web":    website,
            "cqc":    rating,
            "cu":     cqc_url,
            "ar":     borough_for_postcode(pc),
            "la":     round(float(lat), 5) if lat else None,
            "ln":     round(float(lng), 5) if lng else None,
            "specialities": classify_specialities(name, services),
            "services":     services,
            "private":      True,
        })
        time.sleep(0.15)

    print(f"\nKept {len(out)} private London clinics after all filters.")

    # 4. Sort and write.
    out.sort(key=lambda r: (r.get("ar", ""), r.get("n", "").lower()))
    OUT_JSON.write_text(json.dumps(out, indent=2))
    print(f"Wrote {OUT_JSON.name} ({OUT_JSON.stat().st_size//1024} KB)")

    # 5. Quick summary by speciality.
    spec_counts = defaultdict(int)
    for r in out:
        for s in r["specialities"]: spec_counts[s] += 1
    print("\nBy speciality:")
    for s, c in sorted(spec_counts.items(), key=lambda x: -x[1]):
        print(f"  {s:20s} {c}")

    # 6. Quick summary by borough.
    borough_counts = defaultdict(int)
    for r in out:
        borough_counts[r.get("ar") or "(unknown)"] += 1
    print("\nBy borough:")
    for b, c in sorted(borough_counts.items(), key=lambda x: -x[1]):
        print(f"  {b:25s} {c}")

if __name__ == "__main__":
    main()
