#!/usr/bin/env python3
"""
Build private_clinics.json — London private healthcare clinics from CQC.

Approach
--------
1. Paginate all UK CQC locations (proven working from CI).
2. Filter to London postcodes.
3. Filter to "doctor-led private healthcare" by service type.
4. Exclude anything whose ODS code is already in gps.json (those are NHS).
5. Classify each clinic by specialty from name + service types.
6. Output private_clinics.json with type="Private" and specialty list on each.
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"
OUT_JSON = ROOT / "private_clinics.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

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

# Substrings in CQC service-type strings that mean "doctor-led service".
PRIVATE_DOCTOR_SERVICES = [
    "doctors consultation service",
    "doctors treatment service",
    "diagnostic and screening procedures",
    "hospital services for people with mental",
    "long term conditions services",
    "mobile doctors service",
    "rehabilitation services",
    "primary medical services",
    "family planning",
    "termination of pregnancy",
]

NOT_CLINIC_SERVICES = [
    "residential",
    "accommodation for persons",
    "nursing care",
    "care home",
    "personal care",
    "supported living",
    "domiciliary",
    "ambulance",
]

HARD_DROP_NAME_RE = re.compile(
    r"\b(?:dental(?!\s*and\s*medical)|dentist|orthodont|pharmacy|chemist|"
    r"nursing home|care home|residential home|extra care|hospice|"
    r"veterinary|funeral|optician|optometr|"
    r"chiropract|osteopath|reflexolog|"
    r"audiology only|tattoo|piercing|"
    r"detoxification|substance misuse|drug treatment)\b",
    re.IGNORECASE,
)

# Provider-name patterns that indicate this is an NHS Trust / public body
# rather than a private clinic. Even if the location offers doctor-led
# services, it's not "private healthcare" in the consumer sense.
NHS_PUBLIC_PROVIDER_RE = re.compile(
    r"\b(?:nhs\s+(?:trust|foundation\s+trust|england|"
    r"integrated\s+care\s+board|integrated\s+care\s+system)|"
    r"\bicb\b|\bccg\b|\bnhs\b|"
    r"local\s+authority|borough\s+council|county\s+council|"
    r"community\s+health\s+services|"
    r"university\s+college\s+(?:london|hospital)|"
    r"\bnhs\s+foundation\b)",
    re.IGNORECASE,
)

# Name patterns that signal "this is genuinely a private clinic".
# We require AT LEAST one of these unless the service-type strongly
# indicates Independent provision.
PRIVATE_NAME_RE = re.compile(
    r"\b(?:private|harley\s+street|wimpole\s+street|devonshire\s+place|"
    r"the\s+\w+\s+clinic|specialist|consultant|"
    r"bupa|nuffield|spire|hca|circle|king\s+edward|"
    r"the\s+london\s+clinic|wellington|princess\s+grace|"
    r"cromwell|portland|royal\s+marsden\s+private|"
    r"\bclinic\b)",
    re.IGNORECASE,
)

# Service-type substrings that ALONE prove the location is private,
# even if name doesn't explicitly say "private".
INDEPENDENT_SERVICE_RE = re.compile(
    r"\b(?:independent|private)\s+(?:doctors?|hospital|ambulance|"
    r"consultation|treatment|clinic|service)",
    re.IGNORECASE,
)

SPECIALTY_PATTERNS = [
    ("cardiology",       r"\b(?:cardio|heart\s+(?:clinic|centre))\b"),
    ("dermatology",      r"\b(?:derma|skin\s+(?:clinic|centre))\b"),
    ("paediatrics",      r"\b(?:paediatric|child(?:ren)?'?s?\s+(?:clinic|hospital))\b"),
    ("orthopaedics",     r"\b(?:orthop|joint\s+clinic|spine\s+clinic|bone\s+clinic)\b"),
    ("ophthalmology",    r"\b(?:ophthalm|eye\s+(?:clinic|hospital)|vision)\b"),
    ("ent",              r"\b(?:ent\b|ear,?\s*nose|otolaryng)\b"),
    ("gynaecology",      r"\b(?:gynaec|women'?s\s+health|fertility|obstetric)\b"),
    ("psychiatry",       r"\b(?:psychiatr|mental\s+health|psycholog|wellbeing\s+clinic)\b"),
    ("cosmetic",         r"\b(?:cosmet|aesthet|plastic\s+surger|botox|filler)\b"),
    ("urology",          r"\burolog|men'?s\s+health\b"),
    ("oncology",         r"\b(?:oncolog|cancer\s+(?:clinic|centre))\b"),
    ("gastroenterology", r"\b(?:gastroenter|endoscopy)\b"),
    ("endocrinology",    r"\b(?:endocrin|diabetes\s+clinic|thyroid)\b"),
    ("rheumatology",     r"\b(?:rheumatolog|arthritis)\b"),
    ("neurology",        r"\b(?:neurolog|brain\s+clinic)\b"),
    ("private gp",       r"\b(?:general\s+practi|gp\s+(?:clinic|service|surgery)|private\s+gp|family\s+(?:doctor|practice))\b"),
    ("diagnostics",      r"\b(?:diagnos|imaging|\bmri\b|\bct\s+scan\b|radiolog|ultrasound)\b"),
    ("travel",           r"\btravel\s+(?:clinic|health|medicine)\b"),
    ("sexual health",    r"\b(?:sexual\s+health|sti\s+(?:clinic|test))\b"),
    ("physiotherapy",    r"\b(?:physiother|sports\s+(?:medicine|clinic))\b"),
    ("hospital",         r"\b(?:private\s+hospital|the\s+\w+\s+hospital)\b"),
]

def classify_specialty(name, services_blob):
    blob = f"{name} {services_blob}".lower()
    found = []
    for tag, pat in SPECIALTY_PATTERNS:
        if re.search(pat, blob, re.IGNORECASE):
            found.append(tag)
    return found or ["general"]

# ---------------------------------------------------------------- HTTP

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (private-clinics)",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt); continue
            if e.code == 404: return None
            raise
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2); continue
            raise
    return None

# ---------------------------------------------------------------- discovery

def paginate_london(key):
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
            if HARD_DROP_NAME_RE.search(name): continue
            candidates.append(loc)
        total_pages = data.get("totalPages", 1)
        if page % 10 == 0 or page >= total_pages:
            print(f"  page {page}/{total_pages} — London candidates: {len(candidates)}")
        if page >= total_pages: break
        page += 1
        time.sleep(0.15)
    print(f"\n{len(candidates)} London candidates after summary filter.\n")
    return candidates

def services_blob(detail):
    parts = []
    for k in ("gacServiceTypes", "regulatedActivities", "specialisms"):
        v = detail.get(k)
        if isinstance(v, list):
            for it in v:
                if isinstance(it, str):
                    parts.append(it)
                elif isinstance(it, dict):
                    parts.append(it.get("name") or it.get("description") or "")
    return " | ".join(parts).lower()

def build_records(candidates, key, nhs_ods, workers=10):
    print(f"Fetching CQC detail for {len(candidates)} candidates ({workers} workers)…")
    records = []
    rejected_nhs = 0
    rejected_services = 0
    rejected_not_clinic = 0
    rejected_public_provider = 0
    rejected_not_private = 0
    rejected_no_specialty = 0
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

            # ─ Gate 1: exclude NHS GPs already in gps.json
            ods = (d.get("odsCode") or "").strip().upper()
            if ods and ods in nhs_ods:
                rejected_nhs += 1
                continue

            # ─ Gate 2: exclude NHS Trusts and other public providers
            provider_name = d.get("providerName") or ""
            if NHS_PUBLIC_PROVIDER_RE.search(provider_name):
                rejected_public_provider += 1
                continue

            # ─ Gate 3: must have doctor-led services
            blob = services_blob(d)
            if any(t in blob for t in NOT_CLINIC_SERVICES):
                if not any(t in blob for t in PRIVATE_DOCTOR_SERVICES):
                    rejected_not_clinic += 1
                    continue
            if not any(t in blob for t in PRIVATE_DOCTOR_SERVICES):
                rejected_services += 1
                continue

            name_raw = (d.get("name") or d.get("locationName")
                        or d.get("providerName") or "").strip()
            if not name_raw: continue
            name = name_raw.title() if name_raw.isupper() else name_raw

            # ─ Gate 4: PRIVATE signal — either name says "private/clinic/
            #          Harley/etc" OR service type says "Independent/Private"
            has_private_name    = bool(PRIVATE_NAME_RE.search(name))
            has_independent_svc = bool(INDEPENDENT_SERVICE_RE.search(blob))
            if not (has_private_name or has_independent_svc):
                rejected_not_private += 1
                continue

            pc = (d.get("postalCode") or "").strip().upper()
            if not is_london(pc): continue

            addr_parts = [
                d.get("postalAddressLine1") or "",
                d.get("postalAddressLine2") or "",
                d.get("postalAddressTownCity") or "",
                d.get("postalAddressCounty") or "",
            ]
            addr = ", ".join(p for p in addr_parts if p)
            phone = (d.get("mainPhoneNumber") or "").strip()
            website = (d.get("website") or "").strip()

            specialties = classify_specialty(name, blob)

            # ─ Gate 5: drop generic "general"-only records — these are the
            #         catch-all that adds noise (1634 last run). A real
            #         private clinic should have at least one specific
            #         specialty OR be explicitly named as "private GP".
            if specialties == ["general"]:
                # One last chance: if name explicitly says private GP / family
                # doctor / private medical centre, keep as "private gp".
                if re.search(r"\b(?:private\s+(?:gp|medical|doctor|family)|"
                             r"family\s+doctor|private\s+medical\s+centre)\b",
                             name, re.IGNORECASE):
                    specialties = ["private gp"]
                else:
                    rejected_no_specialty += 1
                    continue

            rating = ((d.get("currentRatings", {}) or {})
                      .get("overall", {}) or {}).get("rating", "")
            loc_id = d.get("locationId", "")
            cqc_url = f"https://www.cqc.org.uk/location/{loc_id}" if loc_id else ""

            records.append({
                "ods_code":    ods,
                "cqc_id":      loc_id,
                "name":        name,
                "address":     addr,
                "postcode":    pc,
                "phone":       phone,
                "website":     website,
                "type":        "Private",
                "specialties": specialties,
                "cqc_rating":  rating,
                "cqc_url":     cqc_url,
            })

            if done % 200 == 0 or done == len(futures):
                print(f"  {done}/{len(futures)} — kept {len(records)} so far")

    print(f"\nRejection summary:")
    print(f"  NHS (in gps.json):              {rejected_nhs}")
    print(f"  NHS Trust / public provider:    {rejected_public_provider}")
    print(f"  services not doctor-led:        {rejected_services}")
    print(f"  not a clinic (residential etc): {rejected_not_clinic}")
    print(f"  no private signal (name/svc):   {rejected_not_private}")
    print(f"  no specialty match (generic):   {rejected_no_specialty}")
    print(f"  KEPT:                           {len(records)}\n")
    return records

# ---------------------------------------------------------------- main

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    nhs_ods = set()
    if GPS_JSON.exists():
        try:
            for r in json.loads(GPS_JSON.read_text()):
                code = (r.get("ods_code") or "").upper()
                if code: nhs_ods.add(code)
        except Exception as e:
            print(f"WARN: couldn't parse gps.json — {e}")
    print(f"Will exclude {len(nhs_ods)} NHS GP ODS codes.\n")

    candidates = paginate_london(key)
    records = build_records(candidates, key, nhs_ods)

    by_specialty = Counter()
    for r in records:
        for s in r["specialties"]:
            by_specialty[s] += 1
    print("Records by specialty:")
    for sp, n in by_specialty.most_common():
        print(f"  {sp:20s} {n}")

    OUT_JSON.write_text(json.dumps(records, indent=2))
    print(f"\nWrote {OUT_JSON} — {len(records)} private clinics, "
          f"{OUT_JSON.stat().st_size//1024} KB.")

if __name__ == "__main__":
    main()
