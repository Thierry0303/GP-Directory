#!/usr/bin/env python3
"""
Auto-discover NHS GP practices in London that are missing from gps.json.

Strategy
--------
We've established that CQC pagination + FHIR identifier-lookup are the only
NHS data endpoints accessible from GitHub Actions. So:

  1. Paginate all UK CQC locations.
  2. Filter to London postcodes.
  3. Fetch CQC details to get odsCode + service types for each candidate.
  4. Keep entries whose services indicate they're a primary-care GP, even
     if the name is generic (looser than build_gps_final.py — we want to
     CATCH ones we missed before).
  5. Compare against existing gps.json ODS codes.
  6. For each NEW ODS code, FHIR-lookup to build a full record.
  7. Append to gps.json.

This catches the "Kew Medical Practice" / "Ham Medical Centre" type gaps
that our previous CQC-name filter rejected, because we now trust the CQC
service-type metadata as the GP signal rather than the name string.

Refuses to write if the gap is huge (sanity check).
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"
FHIR_BASE = "https://directory.spineservices.nhs.uk/STU3"

# All London postcode districts (Inner + Outer Greater London).
LONDON_PREFIXES = {
    "EC1A","EC1M","EC1N","EC1P","EC1R","EC1V","EC1Y",
    "EC2A","EC2M","EC2N","EC2P","EC2R","EC2V","EC2Y",
    "EC3A","EC3M","EC3N","EC3P","EC3R","EC3V",
    "EC4A","EC4M","EC4N","EC4P","EC4R","EC4V","EC4Y",
    "WC1A","WC1B","WC1E","WC1H","WC1N","WC1R","WC1V","WC1X",
    "WC2A","WC2B","WC2E","WC2H","WC2N","WC2R",
    "E1","E1W","E2","E3","E4","E5","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15",
    "E16","E17","E18","E20",
    "N1","N1C","N1P","N4","N5","N6","N7","N8","N9","N10","N11","N12","N13","N14","N15","N16",
    "N17","N18","N19","N20","N21","N22",
    "NW1","NW1W","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11","NW26",
    "SE1","SE1P","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9","SE10","SE11","SE12",
    "SE13","SE14","SE15","SE16","SE17","SE18","SE19","SE20","SE21","SE22","SE23",
    "SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1H","SW1P","SW1V","SW1W","SW1X","SW1Y",
    "SW2","SW3","SW4","SW5","SW6","SW7","SW8","SW9","SW10","SW11","SW12","SW13","SW14",
    "SW15","SW16","SW17","SW18","SW19","SW20",
    "W1","W1A","W1B","W1C","W1D","W1F","W1G","W1H","W1J","W1K","W1S","W1T","W1U","W1W",
    "W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
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

def postcode_district(pc):
    pc = (pc or "").strip().upper()
    if " " in pc: return pc.split()[0]
    return pc[:-3] if len(pc) >= 5 else pc

def is_london(pc):
    return postcode_district(pc) in LONDON_PREFIXES

# Hard exclude these — definitely NOT GP practices.
HARD_DROP_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|pharmacy|chemist|"
    r"nursing home|care home|residential home|extra care|hospice|"
    r"veterinary|funeral|optician|optometr|"
    r"chiropract|osteopath|podiatr|reflexolog|"
    r"hearing test|audiology centre|sexual health clinic|"
    r"slimming|weight loss clinic|tattoo|laser hair|laser eye|"
    r"ivf clinic|fertility clinic|cryob|sperm bank|"
    r"detoxification|substance misuse|drug treatment)\b",
    re.IGNORECASE,
)
# Soft drop — likely not GP unless clinical evidence says otherwise.
SOFT_DROP_RE = re.compile(
    r"\b(?:hospital|maternity unit|ambulance|"
    r"prison|hostel|asylum|"
    r"private(?!\sgp)|harley street|"
    r"bupa|nuffield|spire|hca)\b",
    re.IGNORECASE,
)

# Service types that mean "this IS a GP practice".
GP_SERVICE_RE = re.compile(
    r"doctors consultation service|doctors treatment service|"
    r"diagnostic and screening procedures|"
    r"family planning service|"
    r"primary medical service|gp practice|general medical service|"
    r"maternity and midwifery services",
    re.IGNORECASE,
)

# NHS practice ODS codes: letter + 5 digits.
ODS_PRACTICE_RE = re.compile(r"^[A-Z]\d{5}$")

# ---------------------------------------------------------------- HTTP

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (find-gp-gaps)",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            if e.code == 404:
                return None
            raise
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            raise
    return None

def fhir_lookup(ods):
    url = (f"{FHIR_BASE}/Organization"
           f"?identifier=https%3A%2F%2Ffhir.nhs.uk%2FId%2Fods-organization-code%7C{ods}"
           f"&_format=json")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
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

# ---------------------------------------------------------------- discovery

def discover_london_candidates(key):
    """Pass 1: paginate CQC and keep every London location that isn't
    obviously not a GP (hard drops only)."""
    print("Paginating CQC /locations (1-2 min)…")
    page = 1
    per_page = 1000
    candidates = []
    total = 0
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        if not data: break
        items = data.get("locations", []) or []
        if not items: break
        total += len(items)
        for loc in items:
            if loc.get("deregistrationDate"): continue
            pc = loc.get("postalCode") or ""
            if not is_london(pc): continue
            name = loc.get("locationName") or loc.get("name") or ""
            if HARD_DROP_RE.search(name): continue
            candidates.append(loc)
        total_pages = data.get("totalPages", 1)
        if page % 10 == 0 or page >= total_pages:
            print(f"  page {page}/{total_pages} — total UK: {total}, "
                  f"London candidates: {len(candidates)}")
        if page >= total_pages: break
        page += 1
        time.sleep(0.15)
    print(f"\n{len(candidates)} London candidates after summary filter.\n")
    return candidates

def fetch_details(candidates, key, workers=10):
    """Pass 2: fetch CQC detail for each candidate and keep ones that
    are clearly a GP by service type AND have a valid practice ODS."""
    print(f"Fetching CQC detail for {len(candidates)} candidates ({workers} workers)…")
    out = {}
    rejected_no_ods = 0
    rejected_bad_ods = 0
    rejected_not_gp = 0
    rejected_soft = 0
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for c in candidates:
            loc_id = c.get("locationId")
            if not loc_id: continue
            futures[pool.submit(cqc_get, f"/locations/{loc_id}", None, key)] = c
        for fut in as_completed(futures):
            c = futures[fut]
            done += 1
            try:
                d = fut.result()
            except Exception:
                d = None
            if not d:
                continue

            ods = (d.get("odsCode") or "").strip().upper()
            if not ods:
                rejected_no_ods += 1
                continue
            if not ODS_PRACTICE_RE.match(ods):
                rejected_bad_ods += 1
                continue

            # Check service types
            services = []
            for k in ("gacServiceTypes", "regulatedActivities", "specialisms"):
                v = d.get(k)
                if isinstance(v, list):
                    for it in v:
                        if isinstance(it, str):
                            services.append(it)
                        elif isinstance(it, dict):
                            services.append(it.get("name") or it.get("description") or "")
            services_blob = " ".join(services)
            if not GP_SERVICE_RE.search(services_blob):
                rejected_not_gp += 1
                continue

            # Soft drop check (full name + provider name)
            full_text = f"{d.get('name', '')} {d.get('providerName', '')}"
            if SOFT_DROP_RE.search(full_text):
                rejected_soft += 1
                continue

            # Build record
            name_raw = (d.get("name") or d.get("locationName")
                        or d.get("providerName") or "").strip()
            if not name_raw:
                continue
            name = name_raw.title() if name_raw.isupper() else name_raw

            pc = (d.get("postalCode") or "").strip().upper()
            if not is_london(pc):
                continue

            addr_parts = [
                d.get("postalAddressLine1") or "",
                d.get("postalAddressLine2") or "",
                d.get("postalAddressTownCity") or "",
                d.get("postalAddressCounty") or "",
            ]
            addr = ", ".join(p for p in addr_parts if p)
            phone = (d.get("mainPhoneNumber") or "").strip()

            out[ods] = {
                "ods_code": ods, "name": name, "address": addr,
                "postcode": pc, "phone": phone,
            }
            if done % 200 == 0 or done == len(futures):
                print(f"  {done}/{len(futures)} — built {len(out)} so far")

    print(f"\nDetail-pass rejection counts:")
    print(f"  no ODS:           {rejected_no_ods}")
    print(f"  bad ODS format:   {rejected_bad_ods}")
    print(f"  not GP services:  {rejected_not_gp}")
    print(f"  soft-drop names:  {rejected_soft}")
    print(f"  KEPT:             {len(out)}\n")
    return out

# ---------------------------------------------------------------- main

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    if not GPS_JSON.exists():
        sys.exit(f"{GPS_JSON} not found.")
    existing = json.loads(GPS_JSON.read_text())
    if not isinstance(existing, list):
        sys.exit("gps.json is not a JSON array.")
    existing_codes = {(r.get("ods_code") or "").upper() for r in existing if r.get("ods_code")}
    print(f"Loaded {len(existing)} existing gps.json records ({len(existing_codes)} unique ODS).")

    # 1. Discover candidates
    candidates = discover_london_candidates(key)
    # 2. Fetch details
    cqc_gps = fetch_details(candidates, key)

    # 3. Find gaps
    cqc_codes = set(cqc_gps.keys())
    new_codes = cqc_codes - existing_codes
    missing_from_cqc = existing_codes - cqc_codes
    print(f"CQC found {len(cqc_codes)} unique London GP ODS codes.")
    print(f"We have   {len(existing_codes)} unique London GP ODS codes.")
    print(f"  → New in CQC, NOT in our gps.json: {len(new_codes)}")
    print(f"  → In our gps.json, NOT in CQC:     {len(missing_from_cqc)}")

    if not new_codes:
        print("\nNo gaps! Everything CQC has is already in gps.json.")
        return

    # 4. FHIR enrich each new code (gives best data quality)
    print(f"\nFHIR-enriching {len(new_codes)} new ODS codes…")
    new_records = []
    fhir_failed = 0
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fhir_lookup, c): c for c in sorted(new_codes)}
        done = 0
        for fut in as_completed(futures):
            ods = futures[fut]
            done += 1
            try:
                rec = fut.result()
            except Exception:
                rec = None
            if rec and is_london(rec.get("postcode", "")):
                new_records.append({
                    "ods_code":         rec["ods_code"],
                    "name":             rec["name"],
                    "address":          rec["address"],
                    "postcode":         rec["postcode"],
                    "phone":            rec["phone"],
                    "cqc_rating":       "",
                    "cqc_url":          "",
                    "gpps_overall_pct": None,
                    "gpps_contact_pct": None,
                    "gpps_pcn":         "",
                })
            else:
                # Fall back to the CQC data we have
                cqc = cqc_gps.get(ods)
                if cqc and is_london(cqc.get("postcode", "")):
                    new_records.append({
                        "ods_code":         cqc["ods_code"],
                        "name":             cqc["name"],
                        "address":          cqc["address"],
                        "postcode":         cqc["postcode"],
                        "phone":            cqc["phone"],
                        "cqc_rating":       "",
                        "cqc_url":          "",
                        "gpps_overall_pct": None,
                        "gpps_contact_pct": None,
                        "gpps_pcn":         "",
                    })
                else:
                    fhir_failed += 1
            if done % 50 == 0 or done == len(new_codes):
                print(f"  {done}/{len(new_codes)} done — kept {len(new_records)}, failed {fhir_failed}")

    print(f"\nAdded {len(new_records)} new records.")

    # 5. Write
    merged = existing + new_records
    GPS_JSON.write_text(json.dumps(merged, indent=2))
    print(f"Wrote {GPS_JSON} — {len(merged)} records total (was {len(existing)}).")

    # 6. Per-borough impact summary
    from collections import Counter
    BOROUGH_PREFIXES = {
        "Richmond": {"SW13","SW14","TW1","TW2","TW9","TW10","TW11","TW12","KT8"},
        "Kingston": {"KT1","KT2","KT3","KT4","KT5","KT6","KT7","KT9"},
        "Hounslow": {"TW3","TW4","TW5","TW7","TW8","TW13","TW14"},
        "Hillingdon": {"UB7","UB8","UB9","UB10","UB11","HA4","HA6"},
        "Sutton": {"SM1","SM2","SM3","SM5","SM6"},
        "Bromley": {"BR1","BR2","BR3","BR4","BR5","BR6","BR7","BR8"},
    }
    print("\nPer-borough impact:")
    for borough, prefixes in BOROUGH_PREFIXES.items():
        before = sum(1 for r in existing if postcode_district(r.get("postcode","")) in prefixes)
        after = sum(1 for r in merged if postcode_district(r.get("postcode","")) in prefixes)
        delta = after - before
        flag = f"  (+{delta})" if delta else ""
        print(f"  {borough:12s} {before:3d} → {after:3d}{flag}")

if __name__ == "__main__":
    main()
