#!/usr/bin/env python3
"""
Build/expand gps.json using ONLY endpoints that are proven to work from
GitHub Actions:

  - CQC public API at api.service.cqc.org.uk (your fetch_private_clinics.py
    paginates this weekly with no problem).
  - NHS FHIR identifier lookup at directory.spineservices.nhs.uk/STU3
    (your refresh_nhs_data.py uses this thousands of times per run).

Strategy
--------
1. Paginate all UK CQC locations (~50k).
2. Filter to London by postcode + GP-ish names.
3. Fetch the CQC detail record for each candidate to extract `odsCode`
   (the CQC summary endpoint does not include odsCode; detail does).
4. For any odsCode not already in gps.json, look it up via FHIR to
   get postcode/address/phone.
5. Merge with existing gps.json (preserving CQC ratings + GPPS scores
   for practices that were already in there).
6. Refuse to write if the merged record count drops by more than 10%.

Runtime: ~2-5 minutes.
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"
FHIR_BASE = "https://directory.spineservices.nhs.uk/STU3"

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

# Reject obvious non-GPs early to cut the detail-fetch workload.
DROP_NAME_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|pharmacy|chemist|ambulance|"
    r"nursing home|care home|residential home|extra care|hospice|"
    r"veterinary|funeral|optician|optometr|"
    r"chiropract|osteopath|podiatr|reflexolog|"
    r"hearing|audiology only|sexual health clinic|"
    r"slimming|weight loss clinic|tattoo|laser hair|laser eye|"
    r"\bivf\b|fertility|cryob|sperm bank)\b",
    re.IGNORECASE,
)
GP_KEEP_RE = re.compile(
    r"\b(?:medical (?:centre|practice|group|services)|surgery|"
    r"health centre|gp\b|general practi|family practice|"
    r"the practice|\bdrs?\b|doctors|partnership|"
    r"primary care)\b",
    re.IGNORECASE,
)

def looks_like_gp_summary(name):
    if not name: return True  # let detail decide
    if DROP_NAME_RE.search(name): return False
    return bool(GP_KEEP_RE.search(name)) or len(name) < 50  # short names often GPs

# ---------------------------------------------------------------- HTTP

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (build-gps-final)",
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

def fhir_lookup_by_ods(ods):
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

def discover_london_gp_candidates(key):
    """Paginate all UK CQC locations, return London GP-like summary records."""
    print("Paginating CQC /locations (this takes ~1-2 minutes)…")
    page = 1
    per_page = 1000
    london_candidates = []
    total_seen = 0
    diag_done = False
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        if not data:
            break
        items = data.get("locations", []) or []
        if not items:
            break
        if not diag_done:
            diag_done = True
            print(f"  DIAG sample fields: {sorted(items[0].keys())}")
        total_seen += len(items)
        for loc in items:
            if loc.get("deregistrationDate"):
                continue
            pc = loc.get("postalCode") or loc.get("postCode") or ""
            if not is_london(pc):
                continue
            name = (loc.get("name") or loc.get("locationName") or "")
            if not looks_like_gp_summary(name):
                continue
            london_candidates.append(loc)
        total_pages = data.get("totalPages", 1)
        if page % 5 == 0 or page >= total_pages:
            print(f"  page {page}/{total_pages} — seen {total_seen} UK, "
                  f"{len(london_candidates)} London GP candidates")
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.15)
    print(f"\n{len(london_candidates)} London CQC GP candidates "
          f"(from {total_seen} UK locations).")
    return london_candidates

def fetch_ods_codes_from_details(candidates, key, workers=10):
    """For each London CQC candidate, fetch detail to extract odsCode."""
    print(f"\nFetching CQC detail for {len(candidates)} candidates "
          f"(parallel, {workers} workers)…")
    out = {}  # ods -> { name, postcode } from CQC
    failed = 0
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for c in candidates:
            loc_id = c.get("locationId") or c.get("locationID")
            if not loc_id:
                failed += 1
                continue
            futures[pool.submit(cqc_get, f"/locations/{loc_id}", None, key)] = c
        for fut in as_completed(futures):
            c = futures[fut]
            done += 1
            try:
                d = fut.result()
            except Exception:
                d = None
                failed += 1
            if d:
                ods = (d.get("odsCode") or "").strip().upper()
                if ods:
                    out[ods] = {
                        "name":     c.get("name") or c.get("locationName") or "",
                        "postcode": c.get("postalCode") or c.get("postCode") or "",
                    }
            if done % 100 == 0 or done == len(futures):
                print(f"  {done}/{len(futures)} CQC details fetched, "
                      f"{len(out)} unique ODS codes so far")
    print(f"\n{len(out)} unique ODS codes discovered via CQC.")
    return out

# ---------------------------------------------------------------- main

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("CQC_KEY env var not set. Configure it as a GitHub secret.")

    # Load existing gps.json so we preserve GPPS / CQC ratings.
    existing_by_ods = {}
    if GPS_JSON.exists():
        try:
            old = json.loads(GPS_JSON.read_text())
            if isinstance(old, list):
                for d in old:
                    code = (d.get("ods_code") or "").upper()
                    if code: existing_by_ods[code] = d
                print(f"Loaded {len(existing_by_ods)} records from existing gps.json.")
        except Exception as e:
            print(f"WARN: couldn't read existing gps.json — {e}")
    if len(existing_by_ods) < 100:
        print("WARN: existing gps.json has <100 records. Will still attempt build, "
              "but safety guard may abort if results are tiny.")

    # 1. Discover via CQC
    candidates = discover_london_gp_candidates(key)
    cqc_codes = fetch_ods_codes_from_details(candidates, key)

    # 2. For ODS codes already in gps.json, keep the existing record.
    #    For NEW ones, FHIR-lookup to get full record.
    new_codes = set(cqc_codes.keys()) - set(existing_by_ods.keys())
    print(f"\n{len(new_codes)} ODS codes are NEW (not in existing gps.json).")

    new_records = []
    if new_codes:
        print(f"Fetching FHIR details for {len(new_codes)} new codes…")
        fhir_failed = 0
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {pool.submit(fhir_lookup_by_ods, ods): ods for ods in sorted(new_codes)}
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
                    fhir_failed += 1
                if done % 50 == 0 or done == len(new_codes):
                    print(f"  {done}/{len(new_codes)} FHIR lookups done "
                          f"({len(new_records)} ok, {fhir_failed} failed)")

    # 3. Merge: keep all existing records, add the new ones.
    merged = list(existing_by_ods.values()) + new_records
    print(f"\nMerged: {len(existing_by_ods)} existing + {len(new_records)} new "
          f"= {len(merged)} total.")

    # 4. Safety: refuse to write if we'd have fewer than 90% of existing.
    if existing_by_ods and len(merged) < len(existing_by_ods) * 0.9:
        sys.exit(f"ABORT: merged {len(merged)} < existing {len(existing_by_ods)} * 0.9.")

    # 5. Write.
    GPS_JSON.write_text(json.dumps(merged, indent=2))
    print(f"\nWrote gps.json — {len(merged)} practices, "
          f"{GPS_JSON.stat().st_size//1024} KB.")

    # 6. Coverage summary.
    by_area = Counter()
    for r in merged:
        by_area[area_letters(r.get("postcode", ""))] += 1
    print("\nFinal postcode-area coverage:")
    for a, n in sorted(by_area.items(), key=lambda x: -x[1]):
        flag = "  <-- outer London" if a in OUTER_AREAS else ""
        print(f"  {a:4s} {n}{flag}")
    if "TW" in by_area:
        print(f"\n✅ Twickenham/Richmond (TW): {by_area['TW']} practices.")
    else:
        print("\n⚠️  No TW practices found — check the CQC detail step above.")

if __name__ == "__main__":
    main()
