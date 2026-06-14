#!/usr/bin/env python3
"""
cqc_scanner_fixed_v2.py  — clean replacement
=============================================
Scans CQC for ALL London healthcare providers with strict filtering:
  - London postcodes only (exact district matching, no prefix shortcuts)
  - Registered status only (no deregistered)
  - Excludes NHS Trusts, social care, residential, prisons
  - Saves to cqc_london_providers.json

This feeds process_cqc_data_v2.py which builds /private/ specialty pages.
"""

import requests, json, time, os, re
from pathlib import Path

CQC_API_BASE = "https://api.service.cqc.org.uk/public/v1"
CQC_API_KEY  = os.environ.get("CQC_KEY")
OUT_FILE     = Path("cqc_london_providers.json")

# ── Exact London postcode districts ─────────────────────────────────────────
LONDON_DISTRICTS = {
    "E1","E1W","E2","E3","E4","E5","E6","E7","E8","E9",
    "E10","E11","E12","E13","E14","E15","E16","E17","E18","E20",
    "EC1A","EC1M","EC1N","EC1R","EC1V","EC1Y",
    "EC2A","EC2M","EC2N","EC2R","EC2V","EC2Y",
    "EC3A","EC3M","EC3N","EC3R","EC3V",
    "EC4A","EC4M","EC4N","EC4R","EC4V","EC4Y",
    "N1","N1C","N2","N4","N5","N6","N7","N8","N9",
    "N10","N11","N12","N13","N14","N15","N16","N17","N18","N19","N20","N21","N22",
    "NW1","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11",
    "SE1","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9",
    "SE10","SE11","SE12","SE13","SE14","SE15","SE16","SE17","SE18","SE19",
    "SE20","SE21","SE22","SE23","SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1H","SW1P","SW1V","SW1W","SW1X","SW1Y",
    "SW2","SW3","SW4","SW5","SW6","SW7","SW8","SW9",
    "SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18","SW19","SW20",
    "W1","W1A","W1B","W1C","W1D","W1F","W1G","W1H","W1J","W1K","W1S","W1T","W1U","W1W",
    "W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
    "WC1A","WC1B","WC1E","WC1H","WC1N","WC1R","WC1V","WC1X",
    "WC2A","WC2B","WC2E","WC2H","WC2N","WC2R",
    "BR1","BR2","BR3","BR4","BR5","BR6","BR7","BR8",
    "CR0","CR2","CR3","CR4","CR5","CR6","CR7","CR8","CR9",
    "DA1","DA5","DA6","DA7","DA8","DA14","DA15","DA16","DA17","DA18",
    "EN1","EN2","EN3","EN4","EN5","EN7","EN8","EN9",
    "HA0","HA1","HA2","HA3","HA4","HA5","HA6","HA7","HA8","HA9",
    "IG1","IG2","IG3","IG4","IG5","IG6","IG7","IG8","IG11",
    "KT1","KT2","KT3","KT4","KT5","KT6","KT7","KT8","KT9",
    "RM1","RM2","RM3","RM4","RM5","RM6","RM7","RM8","RM9",
    "RM10","RM11","RM12","RM13","RM14",
    "SM1","SM2","SM3","SM4","SM5","SM6",
    "TW1","TW2","TW3","TW4","TW5","TW6","TW7","TW8","TW9",
    "TW10","TW11","TW12","TW13","TW14",
    "UB1","UB2","UB3","UB4","UB5","UB6","UB7","UB8","UB9","UB10","UB11",
}

def is_london(postcode):
    if not postcode: return False
    pc = postcode.strip().upper()
    district = pc.split()[0] if " " in pc else re.match(r"([A-Z]+\d+[A-Z]?)", pc)
    district = district.group(1) if hasattr(district, "group") else district
    return district in LONDON_DISTRICTS

# Names that definitely mean NHS/prison/not private
DROP_NAME_RE = re.compile(
    r"\b(?:hmp\b|prison|young\s+offender\s+institution|"
    r"trust\s+h(?:ead)?q|trust\s+headquarters|"
    r"nhs\s+(?:trust|foundation)|"
    r"university\s+hospital(?!\s+(?:private|independent))|"
    r"general\s+hospital(?!\s+(?:private|independent)))\b",
    re.IGNORECASE,
)

# Service types that mean non-clinical / social care
DROP_SERVICE = {
    "Ambulances", "Residential homes", "Nursing homes",
    "Homecare agencies", "Supported living", "Shared lives",
    "Supported housing", "Extra care housing",
}

# Must have at least one of these service types
KEEP_SERVICE = {
    "Doctors/Gps", "Clinic", "Diagnosis/screening",
    "Hospital", "Hospitals - Mental health/capacity",
    "Mobile doctors", "Phone/online advice",
    "Community services - Healthcare",
    "Rehabilitation (illness/injury)",
}

def fetch_page(page, page_size=1000):
    url = f"{CQC_API_BASE}/locations"
    headers = {"Ocp-Apim-Subscription-Key": CQC_API_KEY, "Accept": "application/json"}
    params  = {"page": page, "perPage": page_size}
    for attempt in range(3):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if attempt < 2: time.sleep(2 ** attempt)
            else: print(f"  ERROR page {page}: {e}")
    return {}

def main():
    if not CQC_API_KEY:
        raise SystemExit("Need CQC_KEY env var")

    print("Scanning CQC for London healthcare providers...")
    all_providers = []
    page = 1

    while True:
        data = fetch_page(page)
        locations = data.get("locations", [])
        if not locations:
            break

        for loc in locations:
            # 1. London postcode — exact matching
            if not is_london(loc.get("postalCode", "")):
                continue
            # 2. Must be registered
            if loc.get("registrationStatus") != "Registered":
                continue
            # 3. Name must not be NHS hospital / prison / trust HQ
            name = loc.get("locationName") or ""
            if DROP_NAME_RE.search(name):
                continue
            # 4. Service types filter
            svc_names = {s.get("name","") for s in loc.get("gacServiceTypes", [])}
            if svc_names & DROP_SERVICE and not (svc_names & KEEP_SERVICE):
                continue
            if not (svc_names & KEEP_SERVICE):
                continue

            all_providers.append({
                "locationId":         loc.get("locationId"),
                "locationName":       name,
                "postalCode":         loc.get("postalCode"),
                "address1":           loc.get("address1",""),
                "city":               loc.get("city",""),
                "registrationStatus": loc.get("registrationStatus"),
                "gacServiceTypes":    loc.get("gacServiceTypes", []),
                "providerSpecialisms":loc.get("providerSpecialisms", []),
            })

        total_pages = data.get("totalPages", 1)
        if page % 20 == 0 or page >= total_pages:
            print(f"  Page {page}/{total_pages} — kept {len(all_providers)}")
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.1)

    print(f"\nTotal clean London providers: {len(all_providers)}")
    OUT_FILE.write_text(json.dumps(all_providers, indent=2, ensure_ascii=False))
    print(f"Saved to {OUT_FILE}")

if __name__ == "__main__":
    main()
