#!/usr/bin/env python3
"""
Generate nhs_gps.json from cqc_london_cache.

A record qualifies as an NHS GP practice if:
  - gacServiceTypes contains "Doctors consultation service" WITHOUT "Independent"
  - AND has a valid 6-char ODS code (letter + 5 digits)
  - AND is not in a known non-GP category

This runs in seconds — no API calls.
"""
import gzip, json, re
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cqc_london_cache.json.gz"
OUT = ROOT / "nhs_gps.json"

ODS_PRACTICE_RE = re.compile(r"^[A-EFG-MNPS-WY]\d{5}$")  # A-Z minus V (dental) etc

def is_nhs_gp(rec):
    gac = rec.get("gacServiceTypes", [])
    has_gp_service = any(
        s.startswith("Doctors consultation service")
        and "Independent" not in s
        for s in gac
    )
    if not has_gp_service: return False

    ods = (rec.get("odsCode") or "").upper()
    if not ODS_PRACTICE_RE.match(ods): return False

    # Exclude obviously non-GP names that slipped through
    nm = rec["name"].lower()
    if any(bad in nm for bad in ["walk-in centre", "urgent care", "out of hours",
                                  "minor injuries", "pcn hub", "extended hours"]):
        return False

    return True

def main():
    with gzip.open(CACHE, "rt") as f:
        cache = json.load(f)
    print(f"Loaded {len(cache):,} cached locations")

    gps = []
    by_borough = Counter()
    for rec in cache.values():
        if is_nhs_gp(rec):
            gps.append(rec)
            by_borough[rec.get("localAuthority", "(unknown)")] += 1

    OUT.write_text(json.dumps(gps, indent=2))
    print(f"\nWrote {len(gps):,} NHS GP practices to {OUT.name}")
    print(f"\nBy borough:")
    for b, n in sorted(by_borough.items()):
        print(f"  {b:30s} {n:4d}")

if __name__ == "__main__":
    main()
