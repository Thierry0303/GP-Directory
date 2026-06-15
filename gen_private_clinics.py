#!/usr/bin/env python3
"""
Generate private_clinics.json from cqc_london_cache, classified by specialty.

A record qualifies as a private clinic if:
  - gacServiceTypes contains ANY of the Independent service types
  - AND is not an NHS GP practice (no GP-pattern ODS)
  - AND name doesn't match known exclusions (care home etc - already
    filtered at cache stage)

Specialty classification uses CQC's specialisms[] field (authoritative)
PLUS name pattern fallback for clinics with no specialisms set.

Runs in seconds — no API calls.
"""
import gzip, json, re
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cqc_london_cache.json.gz"
OUT = ROOT / "private_clinics.json"

# Map CQC's specialisms[] strings to OUR specialty keys (used by site pages)
SPECIALISM_MAP = {
    "Treatment of disease, disorder or injury": None,  # too generic
    "Diagnostic and screening procedures": "diagnostic",
    "Surgical procedures": "surgery",
    "Maternity and midwifery services": "maternity",
    "Family planning": "gynaecology",
    "Termination of pregnancies": "gynaecology",
    "Services in slimming clinics": "weight",
    "Services for everyone": None,
    "Mental health conditions": "psychiatry",
    "Eating disorders": "psychiatry",
    "Learning disabilities": "psychiatry",
    "Substance misuse problems": "psychiatry",
    "Dementia": "psychiatry",
    "Cardiology": "cardiology",
    "Dermatology": "dermatology",
    "Endocrinology": "endocrinology",
    "Gastroenterology": "gastroenterology",
    "General surgery": "surgery",
    "Geriatrics": "geriatrics",
    "Gynaecology": "gynaecology",
    "Haematology": "haematology",
    "Neurology": "neurology",
    "Obstetrics": "gynaecology",
    "Oncology": "oncology",
    "Ophthalmology": "ophthalmology",
    "Orthopaedics": "orthopaedics",
    "Otorhinolaryngology": "ent",
    "Paediatrics": "paediatrics",
    "Plastic surgery": "plastic-surgery",
    "Radiology": "diagnostic",
    "Respiratory medicine": "respiratory",
    "Rheumatology": "rheumatology",
    "Urology": "urology",
    "Vascular surgery": "vascular",
}

# Name pattern fallback when specialisms is empty / generic
NAME_PATTERNS = [
    ("psychiatry",      r"\b(?:psychiatr|psycholog|mental\s+health|counsell|therap|trauma\s+therap|cbt\b|eating\s+disorder)\b"),
    ("cardiology",      r"\b(?:cardio|heart\s+clinic|cardiologist)\b"),
    ("dermatology",     r"\b(?:dermatolog|skin\s+clinic|skin\s+doctor)\b"),
    ("gynaecology",     r"\b(?:gynae|obstet|fertility|womens?\s+health|menopause)\b"),
    ("paediatrics",     r"\b(?:paediatr|childrens?|child\s+health)\b"),
    ("orthopaedics",    r"\b(?:orthopaed|musculoskelet|sports\s+med|joint\s+(?:clinic|specialist))\b"),
    ("urology",         r"\b(?:urology|urologist|prostate)\b"),
    ("ent",             r"\b(?:\bent\b|ear,?\s+nose|otolaryng)\b"),
    ("ophthalmology",   r"\b(?:ophthalm|eye\s+clinic|eye\s+specialist|laser\s+eye)\b"),
    ("gastroenterology", r"\b(?:gastroenterolog|endoscop|liver\s+clinic)\b"),
    ("oncology",        r"\b(?:oncolog|cancer\s+(?:clinic|centre))\b"),
    ("rheumatology",    r"\brheumatolog"),
    ("endocrinology",   r"\b(?:endocrinolog|diabetes\s+clinic|hormone)\b"),
    ("respiratory",     r"\b(?:respirator|lung\s+clinic|chest\s+clinic|sleep\s+clinic)\b"),
    ("neurology",       r"\b(?:neurolog|epileps|migraine)\b"),
    ("haematology",     r"\bhaematolog"),
    ("plastic-surgery", r"\b(?:plastic\s+surg|reconstruct|cosmetic\s+surg)\b"),
    ("vascular",        r"\b(?:vascular|vein\s+clinic|varicose)\b"),
    ("diagnostic",      r"\b(?:diagnostic|imaging|radiology|scan|mri|x-ray|ultrasound)\b"),
    ("aesthetic",       r"\b(?:aesthet|botox|laser\s+hair|cosmetic(?!\s+surg)|filler)\b"),
    ("weight",          r"\b(?:slimming|weight\s+loss|bariatric|obesity)\b"),
    ("private-gp",      r"\b(?:private\s+gp|harley\s+street(?!\s+aesthet)|family\s+(?:doctor|medicine))\b"),
]

def classify_specialties(rec):
    """Return list of specialty keys for a private clinic record."""
    specs = set()

    # 1. From CQC specialisms[]
    for s in rec.get("specialisms", []):
        key = SPECIALISM_MAP.get(s)
        if key: specs.add(key)

    # 2. From name patterns
    name_l = rec["name"].lower()
    for key, pat in NAME_PATTERNS:
        if re.search(pat, name_l): specs.add(key)

    # 3. Default to general consultation if nothing specific
    if not specs:
        specs.add("consultation")

    return sorted(specs)

def is_private(rec):
    """A real private medical clinic vs. NHS / non-medical / GP."""
    if not rec.get("isIndependent"): return False

    gac = rec.get("gacServiceTypes", [])
    # Must have an Independent doctors / hospital service
    has_private_service = any(
        ("Independent" in s) and any(t in s for t in [
            "Doctors", "Hospital", "Treatment", "Diagnostic", "Consultation"
        ])
        for s in gac
    )
    if not has_private_service: return False

    nm = rec["name"].lower()
    # Exclude things we don't want even though they're independent
    if any(bad in nm for bad in [
        "veterinary", "vet ", "funeral", "tattoo", "piercing"
    ]):
        return False

    return True

def to_merge_shape(rec, specs):
    """Map cache record into the EXACT shape that merge_into_dataset.py
    expects (snake_case fields it reads in normalise_private())."""
    # Build a single-line address: line1, line2, town
    parts = [p for p in [
        rec.get("address1") or "",
        rec.get("address2") or "",
        rec.get("town") or "",
    ] if p]
    address = ", ".join(parts)

    return {
        # CQC location ID becomes the unique key (used as "o" in DATA)
        "cqc_id":     rec.get("locationId", ""),
        "ods_code":   rec.get("odsCode", ""),
        "name":       rec.get("name", ""),
        "address":    address,
        "postcode":   rec.get("postcode", ""),
        "phone":      rec.get("phone", ""),
        "website":    rec.get("website", ""),
        "specialties": specs,
        "cqc_rating": rec.get("currentRating", ""),
        "cqc_url":    rec.get("cqcUrl", ""),
        # Keep some extras that merge_into_dataset.py ignores but useful
        # for future-proofing borough/specialty pages:
        "localAuthority": rec.get("localAuthority", ""),
        "lat":            rec.get("lat"),
        "lon":            rec.get("lon"),
        "providerName":   rec.get("providerName", ""),
    }

def main():
    if not CACHE.exists():
        print(f"ERROR: {CACHE} not found.")
        print("Run build_cqc_london_cache.py first to populate the cache.")
        return

    with gzip.open(CACHE, "rt") as f:
        cache = json.load(f)
    print(f"Loaded {len(cache):,} cached locations")

    output = []
    by_specialty = Counter()
    by_borough = Counter()

    for rec in cache.values():
        if not is_private(rec): continue
        specs = classify_specialties(rec)
        merge_rec = to_merge_shape(rec, specs)
        output.append(merge_rec)
        for s in specs: by_specialty[s] += 1
        by_borough[rec.get("localAuthority", "(unknown)")] += 1

    OUT.write_text(json.dumps(output, indent=2))
    print(f"\nWrote {len(output):,} private clinics to {OUT.name}")
    print(f"  (in merge_into_dataset.py-compatible shape)")

    print(f"\nBy specialty:")
    for s, n in by_specialty.most_common():
        print(f"  {s:25s} {n:4d}")

    print(f"\nBy borough (top 10):")
    for b, n in by_borough.most_common(10):
        print(f"  {b:30s} {n:4d}")

if __name__ == "__main__":
    main()
