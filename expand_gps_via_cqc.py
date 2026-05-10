#!/usr/bin/env python3
"""
Expand gps.json with missing London GP practices using CQC pagination as the
discovery source. Same pattern fetch_private_clinics.py uses — proven to
work from GitHub Actions.

Why this approach
-----------------
NHS Digital's `files.digital.nhs.uk` (ePraccur ZIP) and the FHIR list/search
endpoints both block GitHub Actions IPs (HTTP 403/406). The two endpoints
that DO work from Actions:
  - CQC public API at api.service.cqc.org.uk (paginate all UK + filter
    client-side; postcode/region filter params are rejected with 400).
  - NHS FHIR identifier lookup when queried with `?identifier=...|{ODS}`
    (this is the pattern refresh_nhs_data.py uses thousands of times per run).

So we paginate the ~50k UK CQC locations, keep only London + GP-ish, get
their ODS codes, then look up any NEW codes via FHIR identifier.

Refuses to run if gps.json is empty (so we don't compound a previous
data-loss event by writing on top of it).
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"
FHIR_BASE = "https://directory.spineservices.nhs.uk/STU3"

# London postcode prefixes (Inner + Outer Greater London).
LONDON_POSTCODE_PREFIXES = {
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
    if d in LONDON_POSTCODE_PREFIXES: return True
    m = re.match(r"^([A-Z]{1,2}\d)", d)
    return bool(m and m.group(1) in LONDON_POSTCODE_PREFIXES)

def area_letters(pc):
    pc = (pc or "").strip().upper()
    m = re.match(r"^([A-Z]+)", pc)
    return m.group(1) if m else ""

# Rough name-based filter. NHS GP practices in CQC data have predictable
# names: "X Medical Centre", "X Surgery", "X Practice", "X Health Centre",
# "Drs Y & Z", etc. We exclude obvious non-GPs.
DROP_NAME_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|pharmacy|chemist|ambulance|"
    r"nursing home|care home|residential home|extra care|hospice|"
    r"veterinary|funeral|optician|optometr|"
    r"hospital(?! services? for mental)|"  # most hospitals aren't GPs but mental-health hosp can be
    r"chiropract|osteopath|podiatr|reflexolog|"
    r"hearing|audiology only|sexual health clinic|"
    r"slimming|weight loss clinic|tattoo|laser hair)\b",
    re.IGNORECASE,
)

GP_KEEP_RE = re.compile(
    r"\b(?:medical (?:centre|practice|group)|surgery|"
    r"health centre|gp\b|general practi|"
    r"family practice|the practice|"
    r"\bdrs?\b|doctors|partnership)\b",
    re.IGNORECASE,
)

def looks_like_gp(name):
    if not name: return False
    if DROP_NAME_RE.search(name): return False
    return bool(GP_KEEP_RE.search(name))

# ---------------------------------------------------------------- HTTP

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (expand-gps)",
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
    """Use the proven-working FHIR identifier-lookup pattern."""
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

# ---------------------------------------------------------------- discovery

def discover_london_gp_ods_codes(key):
    """Paginate all UK CQC locations, filter to London + GP-ish names,
    return the set of ODS codes encountered.
    """
    print("Paginating all UK CQC locations (this takes 3-5 minutes)…")
    page = 1
    per_page = 1000
    found_codes = set()
    by_area = Counter()
    by_area_kept = Counter()
    total_seen = 0
    diag = False
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        if not data:
            break
        items = data.get("locations", []) or []
        if not items:
            break
        if not diag:
            diag = True
            sample = items[0]
            print(f"  DIAG sample fields: {sorted(sample.keys())}")
            for k in ("postalCode", "postCode", "name", "locationName",
                      "odsCode", "ods_code", "deregistrationDate"):
                if k in sample:
                    print(f"  DIAG  {k}={sample[k]!r}")
        total_seen += len(items)

        for loc in items:
            if loc.get("deregistrationDate"):
                continue
            pc = loc.get("postalCode") or loc.get("postCode") or ""
            if not is_london(pc):
                continue
            area = area_letters(pc)
            by_area[area] += 1
            name = (loc.get("name") or loc.get("locationName") or "")
            ods = (loc.get("odsCode") or loc.get("ods_code") or "").strip().upper()
            if not ods:
                # No ODS in summary — skip; we can't link it without a code
                continue
            if not looks_like_gp(name):
                continue
            by_area_kept[area] += 1
            found_codes.add(ods)

        total_pages = data.get("totalPages", 1)
        if page % 5 == 0 or page >= total_pages:
            print(f"  page {page}/{total_pages} — seen {total_seen} UK, "
                  f"London-GP candidates: {len(found_codes)}")
        if page >= total_pages:
            break
        page += 1
        time.sleep(0.2)

    print(f"\nScanned {total_seen} UK locations.")
    print(f"London locations by area (kept GP-like / total London):")
    for area in sorted(by_area):
        flag = "  <-- outer" if area in OUTER_AREAS else ""
        print(f"  {area:4s} {by_area_kept.get(area,0):4d} / {by_area[area]:4d}{flag}")
    print(f"Total unique GP ODS codes discovered: {len(found_codes)}")
    return found_codes

# ---------------------------------------------------------------- main

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    if not GPS_JSON.exists():
        sys.exit("gps.json not found. Restore it from git first!")
    try:
        existing = json.loads(GPS_JSON.read_text())
    except Exception as e:
        sys.exit(f"gps.json malformed: {e}")
    if not isinstance(existing, list) or len(existing) < 100:
        sys.exit(
            f"gps.json has {len(existing) if isinstance(existing, list) else 'invalid'} records. "
            "Refusing to run on a clearly-broken file. Restore from git history first."
        )
    existing_codes = {(r.get("ods_code") or "").upper() for r in existing}
    print(f"Loaded {len(existing)} existing GPs ({len(existing_codes)} unique ODS codes).")

    # 1. Discover ODS codes via CQC pagination
    discovered = discover_london_gp_ods_codes(key)
    new_codes = discovered - existing_codes
    print(f"\n{len(new_codes)} ODS codes are NEW (not in existing gps.json).")

    if not new_codes:
        print("Nothing new to add. Exiting.")
        return

    # 2. Look up each new code via FHIR (proven from-Actions pattern)
    print(f"\nFetching {len(new_codes)} new records via FHIR identifier lookup…")
    new_records = []
    failed = []
    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fhir_lookup_by_ods, ods): ods for ods in sorted(new_codes)}
        done = 0
        for fut in as_completed(futures):
            ods = futures[fut]
            try:
                rec = fut.result()
            except Exception:
                rec = None
            if rec and is_london(rec.get("postcode", "")):
                new_records.append(rec)
            else:
                failed.append(ods)
            done += 1
            if done % 50 == 0 or done == len(new_codes):
                print(f"  {done}/{len(new_codes)} fetched ({len(new_records)} ok, {len(failed)} failed)")

    if not new_records:
        print("No FHIR lookups succeeded — leaving gps.json unchanged.")
        return

    # 3. Safety check before writing
    merged = existing + new_records
    if len(merged) < len(existing) * 0.95:
        sys.exit(f"ABORT: merged {len(merged)} < existing {len(existing)} * 0.95. "
                 "This shouldn't happen — leaving gps.json unchanged.")

    GPS_JSON.write_text(json.dumps(merged, indent=2))
    print(f"\nWrote gps.json — {len(merged)} practices "
          f"(was {len(existing)}, added {len(new_records)}, "
          f"FHIR-failed {len(failed)}).")

    # 4. Final coverage summary
    by_area = Counter()
    for r in merged:
        by_area[area_letters(r.get("postcode", ""))] += 1
    print("\nFinal postcode-area coverage:")
    for a, n in sorted(by_area.items(), key=lambda x: -x[1]):
        flag = "  <-- outer London" if a in OUTER_AREAS else ""
        print(f"  {a:4s} {n}{flag}")

if __name__ == "__main__":
    main()
