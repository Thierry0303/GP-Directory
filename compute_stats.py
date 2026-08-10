#!/usr/bin/env python3
"""
compute_stats.py

Single source of truth for site-wide summary stats (NHS practice count,
private clinic count, dentist count, borough count, PCN count, avg score).

WHY THIS EXISTS
----------------
Previously the homepage (index.html, via merge_into_dataset.py) and the
/boroughs/ page (via build_borough_index.py) each computed their own counts
at DIFFERENT points in the pipeline. merge_into_dataset.py ran before
drop_non_gms.py and fix_boroughs.py, so it baked in pre-cleanup numbers.
build_borough_index.py ran after cleanup, so it showed corrected numbers.
Result: the two pages silently drifted apart (e.g. 1,530 vs 1,486 private
clinics) every time cleanup changed the dataset.

FIX
----
This script runs ONCE, at the very end of the pipeline, after ALL cleanup
steps (drop_non_gms.py, fix_boroughs.py, enrich_cqc_ratings.py, etc.) and
BEFORE the page-building steps. It reads the final, fully-corrected
merged.json and writes stats.json. Every page-building script
(merge_into_dataset.py's homepage stats block, build_borough_index.py,
etc.) should then READ from stats.json instead of computing its own
counts. One computation, many consumers — drift becomes structurally
impossible.

WHERE THIS SLOTS INTO THE WORKFLOW
------------------------------------
    ... fix_boroughs.py
    ... fetch_cqc_domain_ratings.py   (continue-on-error step)
    ---> compute_stats.py   <--- ADD HERE (new step)
    ... build_borough_pages.py
    ... build_practice_pages.py
    ... build_specialty_pages.py
    ... build_dentist_pages.py
    ... build_borough_index.py
    ... build_nhs_services_pages.py
    ... build_sitemap_unified.py

Then in merge_into_dataset.py and build_borough_index.py (and anywhere
else that prints/injects the "1007 NHS practices / 1530 private clinics"
style stats), replace the local count computation with:

    import json
    with open("stats.json") as f:
        stats = json.load(f)
    # stats["nhs_practice_count"], stats["private_clinic_count"], etc.

CONFIGURATION
--------------
This script does not know your exact merged.json field names, since I
(Claude) haven't seen that file. Edit the CONFIG block below so it matches
your schema — the script will tell you exactly what it can't find if the
field names are wrong, rather than silently producing bad numbers.
"""

import json
import sys
from pathlib import Path
from collections import defaultdict

# ============================================================
# CONFIG — adjust these to match your merged.json field names
# ============================================================

MERGED_JSON_PATH = "merged.json"
STATS_OUTPUT_PATH = "stats.json"

# The field on each record that distinguishes NHS practice vs private
# clinic vs dentist. Adjust FIELD_NAME and the three VALUE_* constants
# to match what's actually in merged.json.
TYPE_FIELD = "type"              # e.g. record["type"]
VALUE_NHS = "nhs"                # value meaning "NHS GP practice"
VALUE_PRIVATE = "private"        # value meaning "private clinic"
VALUE_DENTIST = "dentist"        # value meaning "dentist"

# Field holding the borough name/slug, used for borough count + PCN count
BOROUGH_FIELD = "borough"
PCN_FIELD = "pcn"                # Primary Care Network field (NHS records only)

# Field holding patient survey / satisfaction score (0-100), used for avg score
SCORE_FIELD = "patient_score"

# ============================================================


def load_merged(path: str):
    p = Path(path)
    if not p.exists():
        sys.exit(
            f"ERROR: {path} not found. This script must run after "
            f"merge_into_dataset.py has written it, and after all cleanup "
            f"steps (drop_non_gms.py, fix_boroughs.py) have modified it."
        )
    with open(p, encoding="utf-8") as f:
        data = json.load(f)

    # Handle both a bare list and a dict with a top-level "records" key
    if isinstance(data, dict):
        for key in ("records", "data", "practices", "items"):
            if key in data and isinstance(data[key], list):
                return data[key]
        sys.exit(
            f"ERROR: {path} is a dict but no list found under common keys "
            f"(records/data/practices/items). Top-level keys are: "
            f"{list(data.keys())}. Adjust load_merged() to match."
        )
    if isinstance(data, list):
        return data

    sys.exit(f"ERROR: unrecognised structure in {path}: {type(data)}")


def diagnose_field(records, field_name, label):
    """Fail loudly with a helpful sample if a configured field is missing."""
    missing = sum(1 for r in records if field_name not in r)
    if missing == len(records):
        sample_keys = list(records[0].keys()) if records else []
        sys.exit(
            f"ERROR: field '{field_name}' (configured for {label}) does not "
            f"appear on ANY record. Available fields on a sample record: "
            f"{sample_keys}. Fix the CONFIG block at the top of this script."
        )
    elif missing:
        print(
            f"WARNING: {missing}/{len(records)} records are missing "
            f"'{field_name}' ({label}). They'll be excluded from that count."
        )


def main():
    records = load_merged(MERGED_JSON_PATH)
    if not records:
        sys.exit("ERROR: merged.json loaded but contains zero records.")

    print(f"Loaded {len(records)} records from {MERGED_JSON_PATH}")

    diagnose_field(records, TYPE_FIELD, "record type")
    diagnose_field(records, BOROUGH_FIELD, "borough")

    nhs_count = sum(1 for r in records if r.get(TYPE_FIELD) == VALUE_NHS)
    private_count = sum(1 for r in records if r.get(TYPE_FIELD) == VALUE_PRIVATE)
    dentist_count = sum(1 for r in records if r.get(TYPE_FIELD) == VALUE_DENTIST)

    boroughs = {r[BOROUGH_FIELD] for r in records if r.get(BOROUGH_FIELD)}
    pcns = {r[PCN_FIELD] for r in records if r.get(PCN_FIELD)}

    scores = [
        r[SCORE_FIELD]
        for r in records
        if isinstance(r.get(SCORE_FIELD), (int, float))
    ]
    avg_score = round(sum(scores) / len(scores), 1) if scores else None

    # Per-borough breakdown (used by /boroughs/ index and borough pages)
    per_borough = defaultdict(lambda: {"nhs": 0, "private": 0, "dentist": 0})
    for r in records:
        b = r.get(BOROUGH_FIELD)
        if not b:
            continue
        t = r.get(TYPE_FIELD)
        if t == VALUE_NHS:
            per_borough[b]["nhs"] += 1
        elif t == VALUE_PRIVATE:
            per_borough[b]["private"] += 1
        elif t == VALUE_DENTIST:
            per_borough[b]["dentist"] += 1

    stats = {
        "generated_from": MERGED_JSON_PATH,
        "total_records": len(records),
        "nhs_practice_count": nhs_count,
        "private_clinic_count": private_count,
        "dentist_count": dentist_count,
        "borough_count": len(boroughs),
        "pcn_count": len(pcns),
        "avg_patient_score": avg_score,
        "per_borough": dict(per_borough),
    }

    with open(STATS_OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)

    print(f"\nWrote {STATS_OUTPUT_PATH}:")
    print(f"  NHS practices:    {nhs_count}")
    print(f"  Private clinics:  {private_count}")
    print(f"  Dentists:         {dentist_count}")
    print(f"  Boroughs:         {len(boroughs)}")
    print(f"  PCNs:             {len(pcns)}")
    print(f"  Avg patient score:{avg_score}")


if __name__ == "__main__":
    main()
