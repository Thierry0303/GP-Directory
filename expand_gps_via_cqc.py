#!/usr/bin/env python3
"""
Expand gps.json with missing London GP practices using CQC as the discovery
source.

Why this approach
-----------------
NHS Digital's `files.digital.nhs.uk` (ePraccur ZIP) and `directory.spineservices.nhs.uk`
list/search endpoints both block GitHub Actions egress IPs (HTTP 403/406).

The two endpoints that DO work from Actions:
  - CQC public API at api.service.cqc.org.uk — used by fetch_private_clinics.py.
    Returns `odsCode` for every registered location.
  - NHS FHIR identifier lookup at directory.spineservices.nhs.uk/STU3/Organization
    when queried with `?identifier=...|{ODS}` — used by refresh_nhs_data.py.

So we use CQC to *discover* ODS codes for outer-London postcodes, then use the
FHIR identifier lookup (already known to work) to fetch full names/addresses
for those codes.

What this does
--------------
1. Loads existing gps.json. (Refuses to run if it's empty — recovery first!)
2. For each outer-London postcode prefix that's under-represented, queries
   CQC `/locations?postalCode={pc}&perPage=500` to get every registered
   location in that area.
3. Filters to GP practices (gacServiceTypes contains "Doctors consultation
   service" or "Doctors treatment service", or regulatedActivities contains
   "Treatment of disease, disorder or injury" *and* it's not a hospital).
4. For each odsCode that isn't already in gps.json, calls the FHIR lookup
   endpoint to build the full record.
5. Appends the new records to gps.json (preserving existing ones intact).

Run order
---------
    export CQC_KEY=...
    python3 expand_gps_via_cqc.py
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import defaultdict, Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"
FHIR_BASE = "https://directory.spineservices.nhs.uk/STU3"

# We focus on the postcode areas missing from the current gps.json.
# Inner London is already well-covered; outer is patchy.
OUTER_LONDON_AREAS = [
    "BR", "CR", "DA", "EN", "HA", "IG", "KT", "RM", "SM", "TW", "UB",
]
# We can also re-pass over inner areas to catch any genuinely-missed ones.
INNER_LONDON_AREAS = [
    "E", "EC", "N", "NW", "SE", "SW", "W", "WC",
]

# Service types that identify a GP practice in CQC data.
GP_SERVICE_TERMS = [
    "doctors consultation service",
    "doctors treatment service",
    "diagnostic and screening procedures",  # GPs often have this too
]
# Things that look GP-ish but aren't (we want to exclude these).
NOT_GP_TERMS = [
    "hospital", "care home", "residential", "nursing home", "hospice",
    "ambulance", "supported living", "domiciliary", "personal care",
    "specialist college", "rehabilitation",
]

# ----------------------------------------------------------------- HTTP

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (expand-gps-via-cqc)",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
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

def fhir_lookup_by_ods(ods):
    """Same pattern refresh_nhs_data.py uses — proven to work from Actions."""
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
    if not entries:
        return None
    res = entries[0].get("resource", {}) or {}
    if not res.get("active", True):
        return None
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
    return {
        "ods_code":         ods,
        "name":             name,
        "address":          address,
        "postcode":         pc,
        "phone":            phone,
        "cqc_rating":       "",
        "cqc_url":          "",
        "gpps_overall_pct": None,
        "gpps_contact_pct": None,
        "gpps_pcn":         "",
    }

# ----------------------------------------------------------------- CQC discovery

def _looks_like_gp(loc):
    """Return True if this CQC location looks like a GP practice."""
    blob = " ".join(filter(None, [
        loc.get("name") or loc.get("locationName") or "",
        " ".join((loc.get("gacServiceTypes") or []) if isinstance(loc.get("gacServiceTypes"), list) else []),
        " ".join((loc.get("regulatedActivities") or []) if isinstance(loc.get("regulatedActivities"), list) else []),
        " ".join((loc.get("specialisms") or []) if isinstance(loc.get("specialisms"), list) else []),
    ])).lower()
    if not blob:
        return False
    if any(t in blob for t in NOT_GP_TERMS):
        # quick disqualification
        if not any(t in blob for t in ["doctors consultation", "doctors treatment", "general medical"]):
            return False
    return any(t in blob for t in GP_SERVICE_TERMS) or "general medical" in blob or "gp practice" in blob

def discover_ods_codes_for_postcode(postcode_prefix, key):
    """Walk CQC pages of locations whose postcode starts with the given prefix
    and return the set of ODS codes belonging to GP-like locations."""
    found = set()
    page = 1
    while page <= 30:
        data = cqc_get("/locations", {
            "postalCode": postcode_prefix,
            "perPage": 500,
            "page": page,
        }, key)
        if not data:
            break
        items = data.get("locations") or data.get("value") or []
        if not items:
            break
        for it in items:
            ods = (it.get("odsCode") or "").strip().upper()
            if not ods:
                continue
            if _looks_like_gp(it):
                found.add(ods)
        # Pagination — CQC uses page numbers in this endpoint
        total_pages = (data.get("totalPages") or
                       (data.get("page", page)+1 if len(items) >= 500 else page))
        if page >= total_pages:
            break
        page += 1
    return found

def discover_all(key, areas=OUTER_LONDON_AREAS, workers=4):
    print(f"Discovering ODS codes via CQC for {len(areas)} postcode areas…")
    out = defaultdict(set)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(discover_ods_codes_for_postcode, a, key): a for a in areas}
        for fut in as_completed(futures):
            area = futures[fut]
            try:
                codes = fut.result()
            except Exception as e:
                print(f"  {area}: error {e}")
                codes = set()
            out[area] = codes
            print(f"  {area:4s}: {len(codes)} GP ODS codes")
    return out

# ----------------------------------------------------------------- main

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    if not GPS_JSON.exists():
        sys.exit("gps.json not found. Restore it from git first!")
    existing = json.loads(GPS_JSON.read_text())
    if not isinstance(existing, list) or not existing:
        sys.exit(f"gps.json is empty or malformed ({len(existing) if isinstance(existing, list) else 'invalid'} records). "
                 "Restore it from git history first — do not run this against an empty file!")
    existing_codes = {(r.get("ods_code") or "").upper() for r in existing}
    print(f"Loaded {len(existing)} existing GPs ({len(existing_codes)} unique ODS codes).")

    # 1. Discover ODS codes via CQC
    discovered = discover_all(key)
    all_discovered = set()
    for codes in discovered.values():
        all_discovered |= codes

    new_codes = all_discovered - existing_codes
    print(f"\nDiscovered {len(all_discovered)} GP ODS codes; "
          f"{len(new_codes)} are NEW (not in existing gps.json).")

    if not new_codes:
        print("Nothing new to add.")
        return

    # 2. Look up each new code via FHIR (proven working from Actions)
    print(f"\nFetching {len(new_codes)} new records via FHIR identifier lookup…")
    new_records = []
    failed = []
    with ThreadPoolExecutor(max_workers=12) as pool:
        futures = {pool.submit(fhir_lookup_by_ods, ods): ods for ods in sorted(new_codes)}
        done = 0
        for fut in as_completed(futures):
            ods = futures[fut]
            try:
                rec = fut.result()
            except Exception:
                rec = None
            if rec:
                new_records.append(rec)
            else:
                failed.append(ods)
            done += 1
            if done % 50 == 0 or done == len(new_codes):
                print(f"  {done}/{len(new_codes)} fetched ({len(new_records)} ok, {len(failed)} failed)")

    if failed:
        print(f"\n{len(failed)} ODS codes failed FHIR lookup (likely closed/dormant). Sample:")
        for f in failed[:10]:
            print(f"  {f}")

    # 3. Merge and write
    merged = existing + new_records
    GPS_JSON.write_text(json.dumps(merged, indent=2))
    print(f"\nWrote gps.json — {len(merged)} practices "
          f"(was {len(existing)}, added {len(new_records)}). "
          f"{os.path.getsize(GPS_JSON)//1024} KB")

    # 4. Postcode-area summary so we can confirm Outer London is now covered
    by_area = Counter()
    for r in merged:
        pc = (r.get("postcode") or "").strip().upper()
        m = re.match(r"^([A-Z]+)", pc)
        if m: by_area[m.group(1)] += 1
    print("\nFinal postcode-area coverage:")
    for a, n in sorted(by_area.items(), key=lambda x: -x[1]):
        flag = "  <-- outer London" if a in OUTER_LONDON_AREAS else ""
        print(f"  {a:4s} {n}{flag}")

if __name__ == "__main__":
    main()
