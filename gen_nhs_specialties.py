#!/usr/bin/env python3
"""
Generate nhs_specialties.json from cqc_london_cache.

Classifies NHS-affiliated CQC locations (NOT GP practices — those go through
refresh_nhs_data.py and gen_private_clinics.py) into useful categories:

  - nhs-hospital           NHS general hospitals
  - nhs-mental-health      NHS mental health hospitals + clinics
  - nhs-urgent-care        Walk-in / urgent treatment centres
  - nhs-diagnostic         NHS imaging / diagnostic centres
  - nhs-community          District nursing, community health, school nursing
  - nhs-ambulance          London Ambulance Service stations
  - nhs-hospice            NHS-funded hospices

Classification by:
  - gacServiceTypes (short names: "Hospital", "Urgent care centres", etc)
  - providerName / name containing NHS / Trust / Foundation
  - Excludes anything Independent (handled by gen_private_clinics.py)

Runs in seconds — no API calls.
"""
import gzip, json, re
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cqc_london_cache.json.gz"
OUT = ROOT / "nhs_specialties.json"

# ODS code pattern for NHS GP practices — we EXCLUDE these (they're in gps.json)
NHS_GP_ODS_RE = re.compile(r"^[A-HJ-NPSW-Y]\d{5}$")

DOMAINY_RE = re.compile(r"^[a-z0-9][\w\-.]*\.[a-z]{2,}", re.IGNORECASE)

# URLs confirmed dead by check_external_links.py — never publish these.
try:
    DEAD_LINKS = set(json.loads((ROOT / "dead_links.json").read_text()))
except Exception:
    DEAD_LINKS = set()

def normalize_url(u):
    if not u: return ""
    u = u.strip()
    low = u.lower()
    if low.startswith(("mailto:", "tel:", "fax:")): return ""
    if low.startswith(("http://", "https://")): return u
    if DOMAINY_RE.match(u): return "https://" + u
    return ""

def is_nhs(rec):
    """Decide if this location is NHS-affiliated.
    Uses ownershipType from CQC's provider record (most reliable),
    falls back to name patterns for cases where ownership not yet enriched."""
    # Primary signal: ownershipType from CQC provider detail
    ownership = (rec.get("ownershipType") or "").strip()
    if ownership:
        # Known NHS ownership types
        if ownership in ("NHS", "NHS Body", "NHS Trust", "NHS Foundation Trust", "Public"):
            return True
        # Known non-NHS types
        if ownership in ("Individual", "Partnership", "Organisation", "Charity"):
            return False
    # Fallback: name pattern matching
    prov = (rec.get("providerName") or "").lower()
    name = (rec.get("name") or "").lower()
    for m in ["nhs trust", "nhs foundation", "nhs england", "nhs london", " nhs ", " trust ", " icb ", "ccg "]:
        if m in f" {prov} " or m in f" {name} ":
            return True
    return False

def is_nhs_gp_practice(rec):
    """Already covered by gps.json — exclude from this generator."""
    gac = rec.get("gacServiceTypes", [])
    if "Doctors/Gps" not in gac: return False
    ods = (rec.get("odsCode") or "").upper()
    return bool(NHS_GP_ODS_RE.match(ods))

HQ_RE = re.compile(r"\b(headquarters|hq)\b|trust offices|head office", re.I)

def classify(rec):
    """Return category key or None to skip."""
    if rec.get("registrationStatus") == "Deregistered":
        return None                            # closed / defunct location
    if is_nhs_gp_practice(rec): return None    # covered by gps.json
    if not is_nhs(rec): return None            # private — covered elsewhere

    name_l = rec["name"].lower()
    prov_l = (rec.get("providerName") or "").lower()
    if HQ_RE.search(name_l):
        return None                            # admin buildings, not services

    gac = rec.get("gacServiceTypes", [])
    has_hospital = "Hospital" in gac
    has_mh_hosp  = "Hospitals - Mental health/capacity" in gac

    # Priority order matters: big acute hospitals register many service
    # types at one site — they must land in "hospital", not whatever
    # niche activity they also happen to be registered for.
    # Dedicated hospices only — acute hospitals register palliative-care
    # units too, but belong under "hospital".
    if ("Hospice" in gac or "Home hospice care" in gac) and not has_hospital:
        return "nhs-hospice"
    if "ambulance service" in prov_l:
        return "nhs-ambulance"
    # Mental health only when it's a dedicated MH site.
    if (has_mh_hosp and not has_hospital) \
       or any(m in name_l for m in ["mental health", "psychiatr", "cmht",
                                    "crisis team", "camhs"]) \
       or ("mental health" in prov_l and not has_hospital):
        return "nhs-mental-health"
    # Acute hospitals next — they register urgent-care/diagnostic/hospice
    # activities at the main site but are, to a user, hospitals.
    if has_hospital or has_mh_hosp:
        return "nhs-hospital"
    # Dedicated urgent-care sites (hospital UTCs are covered above).
    if "Urgent care centres" in gac \
       or any(m in name_l for m in ["urgent treatment", "walk-in centre",
                                    "walk in centre", "minor injuries"]):
        return "nhs-urgent-care"
    if "Diagnosis/screening" in gac:
        return "nhs-diagnostic"
    if any(g.startswith("Community services") for g in gac) \
       or "Long-term conditions" in gac or "Rehabilitation (illness/injury)" in gac:
        return "nhs-community"
    return None  # don't list

def display_name(rec):
    """Build a readable name when CQC's locationName is missing or
    is just the borough (common for NHS community services)."""
    name = (rec.get("name") or "").strip()
    prov = (rec.get("providerName") or "").strip()
    la   = (rec.get("localAuthority") or "").strip()
    # If name is empty or matches the borough, use the provider name
    if not name or name == la:
        if prov:
            return f"{prov}" + (f" — {la}" if la and la not in prov else "")
        return la or "(Unnamed service)"
    return name

def slim(rec, category):
    parts = [p for p in [rec.get("address1") or "", rec.get("address2") or "",
                          rec.get("town") or ""] if p]
    return {
        "cqc_id":         rec.get("locationId", ""),
        "name":           display_name(rec),
        "address":        ", ".join(parts),
        "postcode":       rec.get("postcode", ""),
        "phone":          rec.get("phone", ""),
        "website":        (lambda w: "" if w in DEAD_LINKS else w)(normalize_url(rec.get("website", ""))),
        "category":       category,
        "cqc_rating":     rec.get("currentRating", ""),
        "cqc_url":        rec.get("cqcUrl", ""),
        "localAuthority": rec.get("localAuthority", ""),
        "providerName":   rec.get("providerName", ""),
        "providerId":     rec.get("providerId", ""),
        "lat":            rec.get("lat"),
        "lon":            rec.get("lon"),
    }

CATEGORIES = {
    "nhs-hospital":      {"label": "NHS Hospitals",        "order": 1},
    "nhs-mental-health": {"label": "NHS Mental Health",    "order": 2},
    "nhs-urgent-care":   {"label": "NHS Urgent Care",      "order": 3},
    "nhs-diagnostic":    {"label": "NHS Diagnostic Centres","order": 4},
    "nhs-community":     {"label": "NHS Community Services","order": 5},
    "nhs-ambulance":     {"label": "NHS Ambulance Service", "order": 6},
    "nhs-hospice":       {"label": "NHS Hospices",         "order": 7},
}

def main():
    if not CACHE.exists():
        print(f"ERROR: {CACHE} not found.")
        return
    with gzip.open(CACHE, "rt") as f:
        cache = json.load(f)
    print(f"Loaded {len(cache):,} cached locations")

    output = []
    by_cat = Counter()
    by_borough = Counter()
    skipped_no_id = 0
    for rec in cache.values():
        cat = classify(rec)
        if not cat: continue
        # Skip records where we have nothing useful to display
        if not (rec.get('name') or rec.get('providerName')):
            skipped_no_id += 1; continue
        output.append(slim(rec, cat))
        by_cat[cat] += 1
        by_borough[rec.get("localAuthority", "(unknown)")] += 1

    # Deduplicate by (name, postcode)
    RATING_SCORE = {"Outstanding": 4, "Good": 3, "Requires improvement": 2,
                    "Inadequate": 1, "": 0}
    best = {}
    for r in output:
        k = (r["name"].lower().strip(), r["postcode"].strip().upper())
        if not k[0] or not k[1]: continue
        s = RATING_SCORE.get(r.get("cqc_rating", ""), 0)
        if k not in best or s > best[k][0]:
            best[k] = (s, r)
    output = [v[1] for v in best.values()]

    OUT.write_text(json.dumps(output, indent=2))
    print(f"\nWrote {len(output):,} NHS specialty services to {OUT.name}")

    print(f"\nBy category:")
    for c, info in sorted(CATEGORIES.items(), key=lambda x: x[1]["order"]):
        print(f"  {info['label']:30s} {by_cat[c]:4d}")

    print(f"\nBy borough (top 10):")
    for b, n in by_borough.most_common(10):
        print(f"  {b:30s} {n:4d}")

if __name__ == "__main__":
    main()
