#!/usr/bin/env python3
"""
Generate nhs_specialties.json — NHS hospitals, walk-in centres, urgent care,
and specialty clinics from cqc_london_cache.

These are NHS services that aren't GP practices but ARE part of the local
healthcare landscape.
"""
import gzip, json, re
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cqc_london_cache.json.gz"
OUT = ROOT / "nhs_specialties.json"

# Categories
NHS_HOSPITAL_RE = re.compile(r"\b(?:hospital|trust)\b", re.IGNORECASE)
WALK_IN_RE = re.compile(r"\b(?:walk-?in|urgent\s+care|minor\s+injuries|out\s+of\s+hours)\b", re.IGNORECASE)
MENTAL_HEALTH_RE = re.compile(r"\b(?:mental\s+health|psychiatr|psycholog|cmht|imhts|crisis\s+team)\b", re.IGNORECASE)
COMMUNITY_RE = re.compile(r"\b(?:community\s+(?:health|medical|mental)|district\s+nurs)\b", re.IGNORECASE)
DIAGNOSTIC_RE = re.compile(r"\b(?:diagnostic|imaging|radiology|x-ray|mri|ultrasound)\b", re.IGNORECASE)

def categorise(rec):
    if rec.get("isIndependent"): return None  # excluded
    nm = rec["name"].lower()
    if NHS_HOSPITAL_RE.search(nm): return "nhs-hospital"
    if WALK_IN_RE.search(nm): return "nhs-urgent-care"
    if MENTAL_HEALTH_RE.search(nm): return "nhs-mental-health"
    if DIAGNOSTIC_RE.search(nm): return "nhs-diagnostic"
    if COMMUNITY_RE.search(nm): return "nhs-community"
    # NHS GP practices excluded — gen_nhs_gps.py handles those
    if any(s.startswith("Doctors consultation service") and "Independent" not in s
           for s in rec.get("gacServiceTypes", [])):
        return None
    return None

def main():
    with gzip.open(CACHE, "rt") as f:
        cache = json.load(f)
    print(f"Loaded {len(cache):,} cached locations")

    items = []
    by_cat = Counter()
    for rec in cache.values():
        cat = categorise(rec)
        if not cat: continue
        enriched = dict(rec)
        enriched["category"] = cat
        items.append(enriched)
        by_cat[cat] += 1

    OUT.write_text(json.dumps(items, indent=2))
    print(f"\nWrote {len(items):,} NHS specialty services to {OUT.name}")
    print(f"\nBy category:")
    for c, n in by_cat.most_common():
        print(f"  {c:25s} {n:4d}")

if __name__ == "__main__":
    main()
