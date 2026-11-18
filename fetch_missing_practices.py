#!/usr/bin/env python3
"""
fetch_missing_practices.py
--------------------------
Discovers active London GP practices missing from gps.json by querying
the NHS ODS API for every London ICB, then back-fills them.

Run manually or add to the full-refresh workflow BEFORE refresh_nhs_data.py:
    python fetch_missing_practices.py

No API key required — ODS is a public API.

What it does:
  1. Queries ODS for every active GP practice (RO177) across the 5 London ICBs
  2. Diffs against existing gps.json ODS codes
  3. Fetches full detail for each missing practice from the ODS FHIR API
  4. Appends them to gps.json with null GPPS/CQC fields (enriched on next run)
  5. Prints a clear summary of what was added
"""

from __future__ import annotations

import json
import re
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    import requests as _requests
    _USE_REQUESTS = True
except ImportError:
    _USE_REQUESTS = False

# User-Agent that the ODS REST API accepts (urllib's default gets 406)
_UA = "Mozilla/5.0 (compatible; londongp-directory/1.0; +https://londongp.directory)"

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

# London ICB ODS codes (the 5 Integrated Care Boards covering all 32 boroughs
# + City of London).  Using the commissioning-body relationship (RE4) to scope
# the query to practices actually commissioned in London.
# Source: https://directory.spineservices.nhs.uk/ORD/2-0-0/organisations?
#           PrimaryRoleId=RO98&Status=Active  (filtered to ICB role)
LONDON_ICBS = [
    "QMJ",   # NHS North East London ICB
    "QMF",   # NHS North Central London ICB
    "QRV",   # NHS North West London ICB
    "QKK",   # NHS South East London ICB
    "QWE",   # NHS South West London ICB
]

ODS_BASE = "https://directory.spineservices.nhs.uk/ORD/2-0-0"

# Postcode-district → borough mapping (outer London coverage only — inner is
# already handled via the BOROUGH_MAP in refresh_nhs_data.py).
BOROUGH_MAP: dict[str, str] = {
    "EN1":"Enfield","EN2":"Enfield","EN3":"Enfield","EN4":"Enfield",
    "EN7":"Enfield","EN8":"Enfield","EN9":"Enfield",
    "N1":"Islington","N2":"Barnet","N3":"Barnet","N4":"Haringey",
    "N5":"Islington","N6":"Haringey","N7":"Islington","N8":"Haringey",
    "N9":"Enfield","N10":"Haringey","N11":"Barnet","N12":"Barnet",
    "N13":"Enfield","N14":"Enfield","N15":"Haringey","N16":"Hackney",
    "N17":"Haringey","N18":"Enfield","N19":"Islington","N20":"Barnet",
    "N21":"Enfield","N22":"Haringey",
    "NW1":"Camden","NW2":"Brent","NW3":"Camden","NW4":"Barnet","NW5":"Camden",
    "NW6":"Brent","NW7":"Barnet","NW8":"Westminster","NW9":"Brent",
    "NW10":"Brent","NW11":"Barnet",
    "E1":"Tower Hamlets","E2":"Tower Hamlets","E3":"Tower Hamlets",
    "E4":"Waltham Forest","E5":"Hackney","E6":"Newham","E7":"Newham",
    "E8":"Hackney","E9":"Hackney","E10":"Waltham Forest","E11":"Redbridge",
    "E12":"Newham","E13":"Newham","E14":"Tower Hamlets","E15":"Newham",
    "E16":"Newham","E17":"Waltham Forest","E18":"Redbridge","E20":"Newham",
    "EC1A":"City of London","EC1R":"Islington","EC1V":"Islington",
    "EC2A":"City of London",
    "SE1":"Southwark","SE2":"Greenwich","SE3":"Greenwich","SE4":"Lewisham",
    "SE5":"Southwark","SE6":"Lewisham","SE7":"Greenwich","SE8":"Lewisham",
    "SE9":"Greenwich","SE10":"Greenwich","SE11":"Lambeth","SE12":"Lewisham",
    "SE13":"Lewisham","SE14":"Lewisham","SE15":"Southwark","SE16":"Southwark",
    "SE17":"Southwark","SE18":"Greenwich","SE19":"Bromley","SE20":"Bromley",
    "SE21":"Southwark","SE22":"Southwark","SE23":"Lewisham","SE24":"Lambeth",
    "SE25":"Croydon","SE26":"Lewisham","SE27":"Lambeth","SE28":"Greenwich",
    "SW1P":"Westminster","SW1V":"Westminster","SW1W":"Westminster","SW1X":"Westminster",
    "SW1E":"Westminster","SW1A":"Westminster","SW1H":"Westminster",
    "SW2":"Lambeth","SW3":"Kensington & Chelsea","SW4":"Lambeth",
    "SW5":"Kensington & Chelsea","SW6":"Hammersmith & Fulham",
    "SW7":"Kensington & Chelsea","SW8":"Lambeth","SW9":"Lambeth",
    "SW10":"Kensington & Chelsea","SW11":"Wandsworth","SW12":"Wandsworth",
    "SW13":"Richmond","SW14":"Richmond","SW15":"Wandsworth","SW16":"Lambeth",
    "SW17":"Wandsworth","SW18":"Wandsworth","SW19":"Merton","SW20":"Merton",
    "W1":"Westminster","W2":"Westminster","W3":"Ealing","W4":"Hounslow",
    "W5":"Ealing","W6":"Hammersmith & Fulham","W7":"Ealing",
    "W8":"Kensington & Chelsea","W9":"Westminster","W10":"Kensington & Chelsea",
    "W11":"Kensington & Chelsea","W12":"Hammersmith & Fulham",
    "W13":"Ealing","W14":"Hammersmith & Fulham",
    "WC1B":"Camden","WC1E":"Camden","WC1N":"Camden","WC1X":"Islington",
    "WC2A":"Camden","WC2B":"Westminster","WC2H":"Westminster","WC2N":"Westminster",
    "BR1":"Bromley","BR2":"Bromley","BR3":"Bromley","BR4":"Bromley",
    "BR5":"Bromley","BR6":"Bromley","BR7":"Bromley","BR8":"Bromley",
    "CR0":"Croydon","CR2":"Croydon","CR3":"Croydon","CR4":"Merton",
    "CR5":"Croydon","CR6":"Croydon","CR7":"Croydon","CR8":"Croydon",
    "CR9":"Croydon",
    "DA1":"Bexley","DA5":"Bexley","DA6":"Bexley","DA7":"Bexley","DA8":"Bexley",
    "DA14":"Bexley","DA15":"Bexley","DA16":"Bexley","DA17":"Bexley","DA18":"Bexley",
    "EN5":"Barnet",
    "HA0":"Brent","HA1":"Harrow","HA2":"Harrow","HA3":"Harrow","HA4":"Hillingdon",
    "HA5":"Harrow","HA6":"Hillingdon","HA7":"Harrow","HA8":"Barnet","HA9":"Brent",
    "IG1":"Redbridge","IG2":"Redbridge","IG3":"Redbridge","IG4":"Redbridge",
    "IG5":"Redbridge","IG6":"Redbridge","IG7":"Redbridge","IG8":"Redbridge",
    "IG11":"Barking & Dagenham",
    "KT1":"Kingston","KT2":"Kingston","KT3":"Kingston","KT4":"Kingston",
    "KT5":"Kingston","KT6":"Kingston","KT7":"Kingston","KT8":"Richmond",
    "KT9":"Kingston",
    "RM1":"Havering","RM2":"Havering","RM3":"Havering","RM4":"Havering",
    "RM5":"Havering","RM6":"Barking & Dagenham","RM7":"Havering",
    "RM8":"Barking & Dagenham","RM9":"Barking & Dagenham",
    "RM10":"Barking & Dagenham","RM11":"Havering","RM12":"Havering",
    "RM13":"Havering","RM14":"Havering",
    "SM1":"Sutton","SM2":"Sutton","SM3":"Sutton","SM4":"Merton",
    "SM5":"Sutton","SM6":"Sutton",
    "TW1":"Richmond","TW2":"Richmond","TW3":"Hounslow","TW4":"Hounslow",
    "TW5":"Hounslow","TW6":"Hillingdon","TW7":"Hounslow","TW8":"Hounslow",
    "TW9":"Richmond","TW10":"Richmond","TW11":"Richmond","TW12":"Richmond",
    "TW13":"Hounslow","TW14":"Hounslow",
    "UB1":"Ealing","UB2":"Ealing","UB3":"Hillingdon","UB4":"Hillingdon",
    "UB5":"Ealing","UB6":"Ealing","UB7":"Hillingdon","UB8":"Hillingdon",
    "UB9":"Hillingdon","UB10":"Hillingdon","UB11":"Hillingdon",
}


def _get_json(url: str, retries: int = 3) -> dict:
    headers = {"User-Agent": _UA, "Accept": "application/json"}
    for attempt in range(retries):
        try:
            if _USE_REQUESTS:
                r = _requests.get(url, headers=headers, timeout=10)
                if r.status_code == 404:
                    return {}
                r.raise_for_status()
                return r.json()
            else:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=10) as r:
                    return json.loads(r.read())
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    return {}


def get_district(postcode: str) -> str:
    if not postcode:
        return ""
    pc = postcode.strip().upper()
    return pc.split()[0] if " " in pc else pc[:-3] if len(pc) >= 5 else pc


def borough_from_postcode(postcode: str) -> str:
    return BOROUGH_MAP.get(get_district(postcode), "")


def fetch_icb_practices(icb_code: str) -> list[str]:
    """Return all active GP practice ODS codes commissioned by an ICB."""
    ods_codes = []
    url = (f"{ODS_BASE}/organisations"
           f"?PrimaryRoleId=RO177"
           f"&RelTypeId=RE4"
           f"&TargetOrgId={icb_code}"
           f"&Status=Active"
           f"&_format=json"
           f"&_count=1000")
    while url:
        data = _get_json(url)
        orgs = data.get("Organisations", [])
        ods_codes.extend(o["OrgId"] for o in orgs)
        # Pagination via Links
        url = None
        for link in data.get("Links", []):
            if link.get("rel") == "next":
                url = link.get("href")
                break
    return ods_codes


def fetch_practice_detail(ods_code: str) -> dict | None:
    """Fetch name, address, postcode, phone from ODS for a single practice."""
    data = _get_json(f"{ODS_BASE}/organisations/{ods_code}")
    org = data.get("Organisation")
    if not org:
        return None
    if org.get("Status", "").lower() != "active":
        return None

    name_raw = org.get("Name", "")
    name = name_raw.title() if name_raw.isupper() else name_raw

    geo = org.get("GeoLoc", {}).get("Location", {})
    lines = [
        geo.get("AddrLn1", ""),
        geo.get("AddrLn2", ""),
        geo.get("AddrLn3", ""),
        geo.get("Town", ""),
    ]
    address = ", ".join(l for l in lines if l)
    postcode = geo.get("PostCode", "").strip()

    phone = ""
    for contact in (org.get("Contacts") or {}).get("Contact", []):
        if contact.get("type") == "tel":
            phone = contact.get("value", "")
            break

    return {
        "ods_code": ods_code,
        "name": name,
        "address": address,
        "postcode": postcode,
        "phone": phone,
        "gpps_overall_pct": None,
        "gpps_contact_pct": None,
        "gpps_responses": None,
        "gpps_pcn": "",
        "cqc_rating": "",
        "cqc_url": "",
        "lat": None,
        "lng": None,
        "ar": borough_from_postcode(postcode),
    }


def main() -> None:
    print("Loading gps.json...")
    practices: list[dict] = json.loads(GPS_JSON.read_text())
    existing_ods = {p["ods_code"].upper() for p in practices}
    print(f"  {len(existing_ods)} practices already in gps.json")

    # --- Step 1: collect all ODS codes from London ICBs ---
    print("\nQuerying ODS for active London GP practices by ICB...")
    all_london_ods: set[str] = set()
    for icb in LONDON_ICBS:
        codes = fetch_icb_practices(icb)
        print(f"  ICB {icb}: {len(codes)} practices")
        all_london_ods.update(c.upper() for c in codes)
    print(f"  Total from ODS: {len(all_london_ods)} unique practice codes")

    missing = all_london_ods - existing_ods
    print(f"\n  Missing from gps.json: {len(missing)}")

    if not missing:
        print("Nothing to add — gps.json is complete.")
        return

    # --- Step 2: fetch detail for each missing practice ---
    print(f"\nFetching detail for {len(missing)} missing practices (20 concurrent)...")
    new_records: list[dict] = []
    failed: list[str] = []

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(fetch_practice_detail, ods): ods for ods in missing}
        done = 0
        for future in as_completed(futures):
            ods = futures[future]
            done += 1
            try:
                rec = future.result()
                if rec:
                    new_records.append(rec)
                else:
                    failed.append(ods)
            except Exception as e:
                print(f"  ERROR {ods}: {e}")
                failed.append(ods)
            if done % 20 == 0 or done == len(missing):
                print(f"  {done}/{len(missing)} fetched, {len(new_records)} ok")

    # --- Step 3: report and append ---
    print(f"\nResults:")
    print(f"  Added: {len(new_records)}")
    if failed:
        print(f"  Failed (will be retried on next run): {', '.join(sorted(failed))}")

    # Sort new records by postcode for a clean diff
    new_records.sort(key=lambda r: (r.get("postcode", ""), r.get("name", "")))

    for rec in new_records:
        print(f"  + {rec['ods_code']}  {rec['name']:<45}  {rec['postcode']}  {rec['ar']}")

    if not new_records:
        print("No valid records to add.")
        return

    practices.extend(new_records)
    GPS_JSON.write_text(json.dumps(practices, indent=2, ensure_ascii=False))
    print(f"\ngps.json updated: {len(practices)} total practices (+{len(new_records)} new)")
    print("Run refresh_nhs_data.py next to enrich the new records with live ODS data.")


if __name__ == "__main__":
    main()
