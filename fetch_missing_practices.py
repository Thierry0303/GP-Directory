#!/usr/bin/env python3
"""
fetch_missing_practices.py
--------------------------
Discovers active London GP practices missing from gps.json by querying
the NHS Service Search API (v3) for every postcode district that should
have practices but has fewer than expected, plus any district entirely
absent from gps.json.

Requires NHS_SERVICE_SEARCH_KEY (same secret used by fetch_nhs_service_search.py).

Run order in full-refresh.yml: BEFORE apply_gpps_scores.py and
refresh_nhs_data.py, so new practices get enriched on the same run.

What it does:
  1. Builds the set of London postcode districts expected to have GP
     practices (derived from BOROUGH_MAP below).
  2. For every district absent from gps.json, queries the NHS Service
     Search API for GP practices in that district.
  3. Diffs returned ODS codes against gps.json.
  4. For each missing ODS code fetches the full Service Search record and
     appends a new entry to gps.json.
  5. Prints a clear summary.
"""

from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

SS_URL = "https://api.service.nhs.uk/service-search-api"
SS_VERSION = "3"

# All London postcode districts → borough.  Every district here is expected
# to have at least one GP practice.
BOROUGH_MAP: dict[str, str] = {
    "EN1":"Enfield","EN2":"Enfield","EN3":"Enfield","EN4":"Enfield",
    "EN5":"Barnet","EN7":"Enfield","EN8":"Enfield","EN9":"Enfield",
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
    "SW1P":"Westminster","SW1V":"Westminster","SW1W":"Westminster",
    "SW1X":"Westminster","SW1E":"Westminster","SW1A":"Westminster",
    "SW1H":"Westminster",
    "SW2":"Lambeth","SW3":"Kensington & Chelsea","SW4":"Lambeth",
    "SW5":"Kensington & Chelsea","SW6":"Hammersmith & Fulham",
    "SW7":"Kensington & Chelsea","SW8":"Lambeth","SW9":"Lambeth",
    "SW10":"Kensington & Chelsea","SW11":"Wandsworth","SW12":"Wandsworth",
    "SW13":"Richmond","SW14":"Richmond","SW15":"Wandsworth","SW16":"Lambeth",
    "SW17":"Wandsworth","SW18":"Wandsworth","SW19":"Merton","SW20":"Merton",
    "W1D":"Westminster","W1H":"Westminster","W1K":"Westminster",
    "W1T":"Westminster","W2":"Westminster","W3":"Ealing","W4":"Hounslow",
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
    "DA1":"Bexley","DA5":"Bexley","DA6":"Bexley","DA7":"Bexley","DA8":"Bexley",
    "DA14":"Bexley","DA15":"Bexley","DA16":"Bexley","DA17":"Bexley","DA18":"Bexley",
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


def read_key() -> str:
    key = os.environ.get("NHS_SERVICE_SEARCH_KEY", "").strip()
    if not key:
        f = ROOT / "apikey_prod.txt"
        if f.exists():
            key = f.read_text(encoding="utf-8-sig", errors="replace").strip()
    if not key:
        sys.exit("Set NHS_SERVICE_SEARCH_KEY (or put your key in apikey_prod.txt).")
    return key


def get_district(postcode: str) -> str:
    if not postcode:
        return ""
    pc = postcode.strip().upper()
    return pc.split()[0] if " " in pc else pc[:-3] if len(pc) >= 5 else pc


def _ss_get(session, key, params):
    headers = {"apikey": key, "Content-Type": "application/json"}
    for attempt in range(3):
        try:
            r = session.get(SS_URL, params=params, headers=headers, timeout=15)
            r.raise_for_status()
            return r.json().get("value", [])
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                print(f"  WARNING: Service Search failed ({params.get('search')}): {e}")
    return []


def _is_gp(rec: dict) -> bool:
    t = (rec.get("OrganisationType") or "").lower()
    return "gp" in t or (rec.get("OrganisationTypeID") or "").upper() == "GPB"


_debug_printed = False


def search_gps_in_district(district: str, key: str,
                            session: requests.Session) -> list[dict]:
    """Try several query shapes; filter to GPs client-side."""
    global _debug_printed
    variants = [
        {"search": f"{district}*", "searchFields": "Postcode"},
        {"search": district, "searchFields": "Postcode"},
        {"search": f"{district} GP", "searchFields": "OrganisationName,Postcode"},
        {"search": district},
    ]
    for v in variants:
        params = {"api-version": SS_VERSION, "searchMode": "all", "$top": "100", **v}
        hits = _ss_get(session, key, params)
        gps = [h for h in hits if _is_gp(h)]
        if gps:
            return gps
        if hits and not _debug_printed:
            _debug_printed = True
            print(f"  DEBUG: {len(hits)} non-GP hits for {v}; sample types: "
                  f"{sorted({h.get('OrganisationType') for h in hits})[:5]}")
    return []


def fetch_by_ods(ods: str, key: str, session: requests.Session) -> dict | None:
    """Proven lookup (same call as fetch_nhs_service_search.py)."""
    params = {"api-version": SS_VERSION, "search": ods, "searchMode": "all",
              "searchFields": "ODSCode", "$top": "1"}
    hits = _ss_get(session, key, params)
    return hits[0] if hits else None


def load_seed_codes() -> list[str]:
    f = ROOT / "seed_practices.txt"
    if not f.exists():
        return []
    out = []
    for line in f.read_text().splitlines():
        line = line.split("#")[0].strip().upper()
        if line:
            out.append(line)
    return out


def record_from_ss(rec: dict, borough: str) -> dict:
    """Build a gps.json-compatible record from a Service Search hit."""
    ods = (rec.get("ODSCode") or "").strip().upper()
    name_raw = rec.get("OrganisationName") or ""
    name = name_raw.title() if name_raw.isupper() else name_raw

    parts = [rec.get("Address1"), rec.get("Address2"),
             rec.get("Address3"), rec.get("City")]
    address = ", ".join(p.strip() for p in parts if p and p.strip())
    if address.isupper():
        address = address.title()

    postcode = (rec.get("Postcode") or "").strip()

    phone = ""
    for c in (rec.get("Contacts") or []):
        if (c.get("ContactMethodType") or "").lower() == "telephone":
            phone = (c.get("ContactValue") or "").strip()
            if phone:
                break

    lat = rec.get("Latitude")
    lng = rec.get("Longitude")

    return {
        "ods_code": ods,
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
        "lat": round(lat, 6) if isinstance(lat, (int, float)) else None,
        "lng": round(lng, 6) if isinstance(lng, (int, float)) else None,
        "ar": borough,
    }


def main() -> None:
    key = read_key()

    print("Loading gps.json...")
    practices: list[dict] = json.loads(GPS_JSON.read_text())
    existing_ods = {p["ods_code"].upper() for p in practices}
    existing_districts = {get_district(p.get("postcode", "")) for p in practices}
    print(f"  {len(existing_ods)} practices, {len(existing_districts)} postcode districts")

    # Find districts in BOROUGH_MAP that are entirely absent from gps.json
    missing_districts = sorted(
        d for d in BOROUGH_MAP if d not in existing_districts
    )
    print(f"\n  Postcode districts with no practices in gps.json: "
          f"{len(missing_districts)}")
    if missing_districts:
        print(f"  {', '.join(missing_districts)}")

    print(f"\nQuerying NHS Service Search...")
    new_records: list[dict] = []
    seen_ods: set[str] = set(existing_ods)

    with requests.Session() as session:
        # Seed list: explicit ODS codes resolved via the proven lookup.
        for ods in load_seed_codes():
            if ods in seen_ods:
                continue
            rec = fetch_by_ods(ods, key, session)
            if not rec:
                print(f"  seed {ods}: not found in Service Search")
                continue
            borough = BOROUGH_MAP.get(get_district(rec.get("Postcode") or ""), "")
            new_records.append(record_from_ss(rec, borough))
            seen_ods.add(ods)
            print(f"  seed {ods}: added")
        for district in missing_districts:
            borough = BOROUGH_MAP[district]
            hits = search_gps_in_district(district, key, session)
            added = 0
            for rec in hits:
                ods = (rec.get("ODSCode") or "").strip().upper()
                if not ods or ods in seen_ods:
                    continue
                # Confirm it's actually in this district
                pc_district = get_district(rec.get("Postcode") or "")
                if pc_district != district:
                    continue
                new_records.append(record_from_ss(rec, borough))
                seen_ods.add(ods)
                added += 1
            print(f"  {district} ({borough}): {len(hits)} hits, {added} new")

    print(f"\nResults:")
    print(f"  New practices found: {len(new_records)}")

    if not new_records:
        print("Nothing to add.")
        return

    new_records.sort(key=lambda r: (r.get("postcode", ""), r.get("name", "")))
    for rec in new_records:
        print(f"  + {rec['ods_code']}  {rec['name']:<45}  {rec['postcode']}  {rec['ar']}")

    practices.extend(new_records)
    GPS_JSON.write_text(json.dumps(practices, indent=2, ensure_ascii=False))
    print(f"\ngps.json updated: {len(practices)} total (+{len(new_records)} new)")
    print("refresh_nhs_data.py will enrich the new records on this run.")


if __name__ == "__main__":
    main()
