#!/usr/bin/env python3
"""
Build gps.json by combining two endpoints that ARE reachable:

  1. OpenPrescribing.net public API → master list of all NHS GP practice
     ODS codes (mirrored from NHS Digital, served from an unblocked domain).

  2. NHS FHIR identifier lookup at directory.spineservices.nhs.uk → fills
     in postcode, address, and phone for each ODS code. This is the same
     endpoint refresh_nhs_data.py already uses successfully from CI.

Why this script exists
----------------------
NHS Digital's `files.digital.nhs.uk` (ePraccur ZIP) and the FHIR list/search
endpoints both 403 from data-centre AND now from residential browsers. So
we route around them entirely.

Output
------
A new gps.json with the same record shape your existing scripts expect.
GPPS scores from the OLD gps.json are preserved by ODS-code match, so
nothing is lost when this runs.
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

OP_BASE = "https://openprescribing.net/api/1.0"
FHIR_BASE = "https://directory.spineservices.nhs.uk/STU3"

# Inner + Outer London postcode districts.
LONDON_PREFIXES = {
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
    "BR1","BR2","BR3","BR4","BR5","BR6","BR7","BR8",
    "CR0","CR2","CR3","CR4","CR5","CR6","CR7","CR8","CR9",
    "DA1","DA5","DA6","DA7","DA8","DA14","DA15","DA16","DA17","DA18",
    "EN1","EN2","EN3","EN4","EN5","EN7","EN8","EN9",
    "HA0","HA1","HA2","HA3","HA4","HA5","HA6","HA7","HA8","HA9",
    "IG1","IG2","IG3","IG4","IG5","IG6","IG7","IG8","IG11",
    "KT1","KT2","KT3","KT4","KT5","KT6","KT7","KT8","KT9",
    "RM1","RM2","RM3","RM4","RM5","RM6","RM7","RM8","RM9","RM10","RM11","RM12","RM13","RM14",
    "SM1","SM2","SM3","SM4","SM5","SM6",
    "TW1","TW2","TW3","TW4","TW5","TW6","TW7","TW8","TW9","TW10","TW11","TW12","TW13","TW14",
    "UB1","UB2","UB3","UB4","UB5","UB6","UB7","UB8","UB9","UB10","UB11",
}
OUTER_AREAS = {"BR","CR","DA","EN","HA","IG","KT","RM","SM","TW","UB"}

def postcode_district(pc):
    if not pc: return ""
    pc = pc.strip().upper()
    if " " in pc: return pc.split()[0]
    pc = pc.replace(" ", "")
    return pc[:-3] if len(pc) >= 5 else pc

def is_london(pc):
    d = postcode_district(pc)
    if d in LONDON_PREFIXES: return True
    m = re.match(r"^([A-Z]{1,2}\d)", d)
    return bool(m and m.group(1) in LONDON_PREFIXES)

def area_letters(pc):
    pc = (pc or "").strip().upper()
    m = re.match(r"^([A-Z]+)", pc)
    return m.group(1) if m else ""

# ---------------------------------------------------------------- HTTP

def get_json(url, headers=None, timeout=20):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())

def list_all_practices_via_openprescribing():
    """OpenPrescribing returns the full national GP practice list as one JSON
    array (no pagination needed). ~7,000 records. Each has code + name."""
    print(f"Fetching practice list from OpenPrescribing… ({OP_BASE}/org_code/)")
    url = f"{OP_BASE}/org_code/?org_type=practice&format=json"
    data = get_json(url, headers={
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (build-via-openprescribing)",
    }, timeout=60)
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected OpenPrescribing response: {type(data).__name__}")
    # Filter to currently-open practices (left_date is null/empty).
    open_now = [p for p in data if not (p.get("left_date") or "").strip()]
    print(f"  OpenPrescribing returned {len(data)} practices, "
          f"{len(open_now)} are currently open.")
    return open_now

def fhir_lookup_by_ods(ods):
    """Same proven pattern refresh_nhs_data.py uses."""
    url = (f"{FHIR_BASE}/Organization"
           f"?identifier=https%3A%2F%2Ffhir.nhs.uk%2FId%2Fods-organization-code%7C{ods}"
           f"&_format=json")
    try:
        data = get_json(url, headers={"Accept": "application/json"}, timeout=10)
    except Exception:
        return None
    entries = data.get("entry", [])
    if not entries: return None
    res = entries[0].get("resource", {}) or {}
    if not res.get("active", True): return None
    raw_name = res.get("name", "") or ""
    name = raw_name.title() if raw_name.isupper() else raw_name
    addrs = res.get("address", []) or []
    addr = addrs[0] if addrs else {}
    pc = (addr.get("postalCode") or "").strip().upper()
    lines = addr.get("line", []) or []
    city = addr.get("city", "") or ""
    address = ", ".join(filter(None, lines + ([city] if city else [])))
    address = address.title() if address.isupper() else address
    phone = ""
    for tc in (res.get("telecom") or []):
        if tc.get("system") == "phone":
            phone = tc.get("value", "") or ""
            break
    return {"ods_code": ods, "name": name, "address": address,
            "postcode": pc, "phone": phone}

# ---------------------------------------------------------------- main

def main():
    # Load existing gps.json so we preserve GPPS / CQC fields by ODS match.
    existing_by_ods = {}
    if GPS_JSON.exists():
        try:
            old = json.loads(GPS_JSON.read_text())
            if isinstance(old, list):
                for d in old:
                    code = (d.get("ods_code") or "").upper()
                    if code: existing_by_ods[code] = d
                print(f"Loaded {len(existing_by_ods)} records from existing gps.json "
                      "(GPPS scores will be preserved).")
        except Exception as e:
            print(f"WARN: couldn't read existing gps.json — {e}")

    # 1. Get the full national practice list from OpenPrescribing.
    practices = list_all_practices_via_openprescribing()
    ods_codes = sorted({p["code"].upper() for p in practices if p.get("code")})
    print(f"  {len(ods_codes)} unique ODS codes to look up.")

    # 2. Fetch postcode + address + phone for each via FHIR identifier
    #    lookup. We need the postcode to filter to London (OpenPrescribing
    #    doesn't include postcodes in the basic org_code response).
    print(f"\nLooking up {len(ods_codes)} practices via FHIR (20 concurrent)…")
    london_records = []
    looked_up = 0
    fhir_fail = 0
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fhir_lookup_by_ods, ods): ods for ods in ods_codes}
        for fut in as_completed(futures):
            ods = futures[fut]
            looked_up += 1
            try:
                rec = fut.result()
            except Exception:
                rec = None
            if not rec:
                fhir_fail += 1
            elif is_london(rec.get("postcode", "")):
                old = existing_by_ods.get(ods, {})
                london_records.append({
                    "ods_code":         rec["ods_code"],
                    "name":             rec["name"],
                    "address":          rec["address"],
                    "postcode":         rec["postcode"],
                    "phone":            rec["phone"] or old.get("phone", ""),
                    "cqc_rating":       old.get("cqc_rating", ""),
                    "cqc_url":          old.get("cqc_url", ""),
                    "gpps_overall_pct": old.get("gpps_overall_pct"),
                    "gpps_contact_pct": old.get("gpps_contact_pct"),
                    "gpps_pcn":         old.get("gpps_pcn", ""),
                })
            if looked_up % 500 == 0 or looked_up == len(ods_codes):
                print(f"  {looked_up}/{len(ods_codes)} looked up "
                      f"({len(london_records)} London so far, {fhir_fail} FHIR failures)")

    if not london_records:
        sys.exit("ABORT: no London practices found. Likely FHIR endpoint is down. "
                 "gps.json left unchanged.")

    # 3. Safety: refuse to write a much-smaller list than what we already have.
    if existing_by_ods and len(london_records) < len(existing_by_ods) * 0.7:
        sys.exit(f"ABORT: built {len(london_records)} records but existing gps.json "
                 f"has {len(existing_by_ods)}. Likely a partial fetch — "
                 "gps.json left unchanged.")

    # 4. Write.
    GPS_JSON.write_text(json.dumps(london_records, indent=2))
    print(f"\nWrote gps.json — {len(london_records)} London practices, "
          f"{GPS_JSON.stat().st_size//1024} KB.")

    # 5. Coverage summary so we can verify TW etc. are now included.
    by_area = Counter()
    for r in london_records:
        by_area[area_letters(r.get("postcode", ""))] += 1
    print("\nFinal postcode-area coverage:")
    for a, n in sorted(by_area.items(), key=lambda x: -x[1]):
        flag = "  <-- outer London" if a in OUTER_AREAS else ""
        print(f"  {a:4s} {n}{flag}")

    if "TW" in by_area:
        print(f"\n✅ Twickenham/Richmond (TW): {by_area['TW']} practices.")
    else:
        print("\n⚠️  No TW practices found — something's wrong with London filtering.")

if __name__ == "__main__":
    main()
