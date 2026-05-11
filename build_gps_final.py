#!/usr/bin/env python3
"""
Build/expand gps.json using ONLY the CQC public API.

Approach
--------
The CQC API returns full location details — name, postcode, address, phone,
ODS code, service types — in one call. So we don't need FHIR at all. Skip
that step entirely.

  1. Paginate all UK CQC locations (proven endpoint, your fetch_private_clinics.py
     uses this weekly).
  2. Filter to London by postcode + clear GP-like names.
  3. Fetch CQC detail for each candidate (extract ODS + address + phone + services).
  4. Filter by service type — must include "Doctors consultation/treatment"
     or "Diagnostic and screening procedures with medical doctors".
  5. Validate ODS code format (NHS practice codes are 6 alphanumeric chars).
  6. Build records from CQC data directly.
  7. Merge with existing gps.json (preserves CQC ratings + GPPS scores by ODS).
  8. Refuse to write if the merged count drops below 90% of existing.

Runtime: ~5-8 minutes.
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

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

# Tight name filter for the SUMMARY stage — must look like a GP practice
# name, not a dentist/pharmacy/hospital.
DROP_NAME_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|pharmacy|chemist|ambulance|"
    r"nursing home|care home|residential home|extra care|hospice|"
    r"veterinary|funeral|optician|optometr|"
    r"hospital|maternity unit|"
    r"chiropract|osteopath|podiatr|reflexolog|"
    r"hearing|audiology|sexual health clinic|"
    r"slimming|weight loss clinic|tattoo|laser hair|laser eye|"
    r"\bivf\b|fertility|cryob|sperm bank|"
    r"prison|hostel|asylum|"
    r"detoxification|substance misuse|drug treatment)\b",
    re.IGNORECASE,
)
GP_KEEP_RE = re.compile(
    r"\b(?:medical (?:centre|practice|group|services|partners?)|"
    r"surgery|surgeries|"
    r"health centre|gp\b|general practi|family practice|"
    r"the practice|\bdrs?\b|partnership|"
    r"primary care)\b",
    re.IGNORECASE,
)

def looks_like_gp_summary(name):
    if not name: return False
    if DROP_NAME_RE.search(name): return False
    return bool(GP_KEEP_RE.search(name))

# Service-type substrings that mean it's a GP (will check in CQC detail).
GP_SERVICE_SUBSTRINGS = [
    "doctors consultation service",
    "doctors treatment service",
    "diagnostic and screening procedures",
    "family planning service",
    "maternity and midwifery services",
    "primary medical",
]
NON_GP_SERVICE_SUBSTRINGS = [
    "residential",
    "accommodation for persons",
    "nursing care",
    "care home",
    "dental",
    "hospital service",
    "acute services",
    "ambulance",
]

def is_gp_by_services(services):
    blob = " ".join(s for s in services if s).lower()
    if not blob:
        return False
    if any(t in blob for t in NON_GP_SERVICE_SUBSTRINGS):
        return False
    return any(t in blob for t in GP_SERVICE_SUBSTRINGS)

ODS_PRACTICE_RE = re.compile(r"^[A-Z]\d{5}$")  # F83019, G81082, etc.

def is_practice_ods(code):
    """NHS GP practice codes are exactly 6 chars: a letter + 5 digits."""
    return bool(ODS_PRACTICE_RE.match((code or "").strip().upper()))

# ---------------------------------------------------------------- HTTP

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (build-gps-final-v2)",
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

# ---------------------------------------------------------------- discovery

def discover_london_gp_candidates(key):
    """Paginate all UK CQC locations, return London GP-like summary records."""
    print("Paginating CQC /locations (this takes ~1-2 minutes)…")
    page = 1
    per_page = 1000
    london_candidates = []
    total_seen = 0
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        if not data:
            break
        items = data.get("locations", []) or []
        if not items:
            break
        total_seen += len(items)
        for loc in items:
            if loc.get("deregistrationDate"):
                continue
            pc = loc.get("postalCode") or ""
            if not is_london(pc):
                continue
            name = loc.get("locationName") or loc.get("name") or ""
            if not looks_like_gp_summary(name):
                continue
            london_candidates.append(loc)
        total_pages = data.get("totalPages", 1)
        if page % 10 == 0 or page >= total_pages:
            print(f"  page {page}/{total_pages} — seen {total_seen} UK, "
                  f"{len(london_candidates)} London GP-named candidates")
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.15)
    print(f"\n{len(london_candidates)} London CQC GP-named candidates "
          f"(from {total_seen} UK locations).")
    return london_candidates

def build_records_from_details(candidates, key, workers=10):
    """For each candidate, fetch full detail and extract a GPS record."""
    print(f"\nFetching CQC detail for {len(candidates)} candidates "
          f"(parallel, {workers} workers)…")
    records_by_ods = {}
    rejected_no_ods = 0
    rejected_bad_ods = 0
    rejected_services = 0
    sample_rejects = []
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
                if done % 200 == 0:
                    print(f"  {done}/{len(futures)} fetched — built {len(records_by_ods)} so far")
                continue

            ods = (d.get("odsCode") or "").strip().upper()
            if not ods:
                rejected_no_ods += 1
                continue
            if not is_practice_ods(ods):
                rejected_bad_ods += 1
                continue

            # Service filter
            services = []
            for k in ("gacServiceTypes", "regulatedActivities", "specialisms"):
                v = d.get(k)
                if isinstance(v, list):
                    for it in v:
                        if isinstance(it, str):
                            services.append(it)
                        elif isinstance(it, dict):
                            services.append(it.get("name") or it.get("description") or "")
            if not is_gp_by_services(services):
                rejected_services += 1
                if len(sample_rejects) < 5:
                    sample_rejects.append((d.get("locationName") or "", ods, services[:3]))
                continue

            # Build record
            name_raw = d.get("locationName") or d.get("providerName") or ""
            name = name_raw.title() if name_raw.isupper() else name_raw

            pc = (d.get("postalCode") or "").strip().upper()
            addr_parts = [
                d.get("postalAddressLine1") or "",
                d.get("postalAddressLine2") or "",
                d.get("postalAddressTownCity") or "",
                d.get("postalAddressCounty") or "",
            ]
            addr = ", ".join(p for p in addr_parts if p)
            phone = (d.get("mainPhoneNumber") or "").strip()

            records_by_ods[ods] = {
                "ods_code":         ods,
                "name":             name,
                "address":          addr,
                "postcode":         pc,
                "phone":            phone,
            }
            if done % 200 == 0 or done == len(futures):
                print(f"  {done}/{len(futures)} fetched — built {len(records_by_ods)} so far")

    print(f"\nRejection summary:")
    print(f"  no ODS code:         {rejected_no_ods}")
    print(f"  ODS not practice format: {rejected_bad_ods}")
    print(f"  failed service filter: {rejected_services}")
    print(f"  → kept: {len(records_by_ods)} unique GP records")
    if sample_rejects:
        print("\nSample service-filter rejects (so you can tune if needed):")
        for n, o, svc in sample_rejects:
            print(f"  {o:8s} {n[:40]:40s}  services: {svc}")
    return records_by_ods

# ---------------------------------------------------------------- main

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("CQC_KEY env var not set. Configure it as a GitHub secret.")

    # Load existing gps.json for merge.
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

    # 1. Discover candidates via CQC summary pagination
    candidates = discover_london_gp_candidates(key)
    # 2. Fetch detail and build records
    cqc_records = build_records_from_details(candidates, key)

    # 3. Merge: keep all existing + add any new ones. For codes that are in
    #    both, prefer the existing (so we preserve CQC ratings and GPPS scores).
    merged_by_ods = dict(existing_by_ods)  # copy
    added = 0
    for ods, new_rec in cqc_records.items():
        if ods not in merged_by_ods:
            merged_by_ods[ods] = {
                "ods_code":         new_rec["ods_code"],
                "name":             new_rec["name"],
                "address":          new_rec["address"],
                "postcode":         new_rec["postcode"],
                "phone":            new_rec["phone"],
                "cqc_rating":       "",
                "cqc_url":          "",
                "gpps_overall_pct": None,
                "gpps_contact_pct": None,
                "gpps_pcn":         "",
            }
            added += 1
    merged = list(merged_by_ods.values())
    print(f"\nMerged: {len(existing_by_ods)} existing + {added} new "
          f"= {len(merged)} total.")

    # 4. Safety guard
    if existing_by_ods and len(merged) < len(existing_by_ods) * 0.9:
        sys.exit(f"ABORT: merged {len(merged)} < existing {len(existing_by_ods)} * 0.9.")

    # 5. Write
    GPS_JSON.write_text(json.dumps(merged, indent=2))
    print(f"\nWrote gps.json — {len(merged)} practices, "
          f"{GPS_JSON.stat().st_size//1024} KB.")

    # 6. Coverage summary
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
        print("\n⚠️  No TW practices — check the rejection summary above.")

if __name__ == "__main__":
    main()
