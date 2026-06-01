#!/usr/bin/env python3
"""
Drop records that aren't really registerable NHS GP practices.

After enrich_cqc_ratings.py runs a full CQC pagination, any NHS record
that STILL has no cqc_url is almost certainly not a GMS-contracted GP
practice — it's a walk-in centre, out-of-hours service, NHS Trust
department, branch surgery, or closed practice. These are the same
records whose `gp-registration.nhs.uk/{ODS}` link 404s.

Rules:
  - has rating + url → keep (real GMS practice)
  - has url, no rating → keep (registered, just not inspected yet)
  - has neither      → drop (not a registerable NHS GP)

Private clinics are left alone.

Run order: AFTER enrich_cqc_ratings.py, BEFORE the page builders.
"""

import json, sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON    = ROOT / "gps.json"
MERGED_JSON = ROOT / "merged.json"

RATING_FIELDS = ["cqc_rating", "cqc"]
URL_FIELDS    = ["cqc_url", "cu"]
TYPE_FIELDS   = ["type"]

def get_first(rec, fields):
    for f in fields:
        v = rec.get(f)
        if v is not None and v != "":
            return v
    return ""

def is_nhs(rec):
    return (get_first(rec, TYPE_FIELDS) or "NHS") == "NHS"

def has_cqc_link(rec):
    return bool(get_first(rec, URL_FIELDS))

def process(path):
    if not path.exists():
        print(f"  {path.name}: not found.")
        return
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        print(f"  {path.name}: not a list.")
        return

    by_borough_before = Counter(r.get("ar") or "(none)"
                                for r in data if is_nhs(r))

    kept = []
    dropped_examples = []
    dropped = 0
    for rec in data:
        if is_nhs(rec) and not has_cqc_link(rec):
            dropped += 1
            if len(dropped_examples) < 8:
                dropped_examples.append({
                    "ods": get_first(rec, ["ods_code", "o"]),
                    "name": rec.get("name") or rec.get("n", ""),
                    "borough": rec.get("ar", "")
                })
            continue
        kept.append(rec)

    by_borough_after = Counter(r.get("ar") or "(none)"
                                for r in kept if is_nhs(r))

    # Safety check — refuse if we'd lose more than 50% of NHS records
    nhs_before = sum(by_borough_before.values())
    nhs_after  = sum(by_borough_after.values())
    if nhs_before and nhs_after < nhs_before * 0.5:
        print(f"  {path.name}: ABORT — would drop {nhs_before - nhs_after}/{nhs_before} "
              "NHS records (>50%). Likely enrich step didn't run. Skipping write.")
        return

    path.write_text(json.dumps(kept, indent=2))
    print(f"\n  {path.name}: dropped {dropped} non-GMS records "
          f"({len(data)} → {len(kept)})")

    if dropped_examples:
        print("\n  Sample of dropped records (should all look like non-primary-care):")
        for d in dropped_examples:
            print(f"    {d['ods']:8s} {d['name'][:50]:50s} {d['borough']}")

    print("\n  Per-borough NHS practice delta:")
    boroughs = sorted(set(by_borough_before) | set(by_borough_after))
    for b in boroughs:
        bef = by_borough_before.get(b, 0)
        aft = by_borough_after.get(b, 0)
        d = aft - bef
        if d != 0:
            print(f"    {b:30s} {bef:>5d} → {aft:>5d}  ({d:+d})")

def main():
    for path in [GPS_JSON, MERGED_JSON]:
        process(path)
    print("\nDone. Re-run merge_into_dataset.py + the page builders to rebuild "
          "the site without the dropped records.")

if __name__ == "__main__":
    main()
