#!/usr/bin/env python3
r"""
update_index_counters.py

Writes the homepage's SEO-visible counters (cntNhs, cntPriv, cntAll, and any
others found) into index.html, using the FINAL post-cleanup numbers from
stats.json.

WHY THIS EXISTS
----------------
merge_into_dataset.py used to write these counters itself, using
len(new_private) and len(merged) at that point in the pipeline — i.e.
BEFORE drop_non_gms.py and fix_boroughs.py had a chance to drop or
reclassify records. That's why the homepage (1530 private clinics) drifted
from /boroughs/ (1,486 private clinics), which is built later from the
fully-cleaned merged.json.

This script replaces that logic. It runs at the very end of the pipeline,
after compute_stats.py, and writes the same final numbers everyone else
uses — so the homepage counters can never drift from the rest of the site
again.

WHERE THIS SLOTS INTO THE WORKFLOW
------------------------------------
    ... fix_boroughs.py
    ... fetch_cqc_domain_ratings.py
    ... compute_stats.py
    ---> update_index_counters.py   <--- ADD HERE (new step)
    ... build_borough_pages.py
    ... build_practice_pages.py
    ... (rest of build_* steps)

WHAT TO REMOVE FROM merge_into_dataset.py
-------------------------------------------
Delete this block (the counter-writing regexes and the
INDEX_HTML.write_text call directly under them):

    new_html = INDEX_HTML.read_text(encoding="utf-8")
    new_html = re.sub(
        r'(id="cntPriv">)\d+(</span>)',
        rf'\g<1>{len(new_private)}\g<2>',
        new_html
    )
    new_html = re.sub(
        r'(id="cntAll">)\d+(</span>)',
        rf'\g<1>{len(merged)}\g<2>',
        new_html
    )
    INDEX_HTML.write_text(new_html, encoding="utf-8")

merge_into_dataset.py should keep writing merged.json (that part is fine —
it's the correct, later-cleaned merged.json that downstream builders read).
It just shouldn't touch index.html's counters anymore; this script owns
that job now.
"""

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STATS_JSON = ROOT / "stats.json"
INDEX_HTML = ROOT / "index.html"

# Map: HTML element id -> key in stats.json
# Add/adjust entries here if index.html has more counters than these
# (e.g. a dentist count, PCN count, borough count on the homepage).
COUNTER_MAP = {
    "cntNhs": "nhs_practice_count",
    "cntPriv": "private_clinic_count",
    "cntAll": "total_records",
    "cntDentist": "dentist_count",
    "cntBoroughs": "borough_count",
    "cntPcns": "pcn_count",
}


def main():
    if not STATS_JSON.exists():
        sys.exit(
            f"ERROR: {STATS_JSON} not found. This script must run after "
            f"compute_stats.py in the workflow."
        )
    if not INDEX_HTML.exists():
        sys.exit(f"ERROR: {INDEX_HTML} not found.")

    stats = json.loads(STATS_JSON.read_text(encoding="utf-8"))
    html = INDEX_HTML.read_text(encoding="utf-8")

    updated = []
    skipped_no_id = []
    skipped_no_stat = []

    for elem_id, stat_key in COUNTER_MAP.items():
        pattern = rf'(id="{elem_id}">)[\d,]+(</span>)'

        if not re.search(pattern, html):
            skipped_no_id.append(elem_id)
            continue

        if stat_key not in stats or stats[stat_key] is None:
            skipped_no_stat.append((elem_id, stat_key))
            continue

        value = stats[stat_key]
        html, n = re.subn(pattern, rf'\g<1>{value}\g<2>', html)
        updated.append((elem_id, stat_key, value, n))

    INDEX_HTML.write_text(html, encoding="utf-8")

    print(f"Loaded stats from {STATS_JSON.name} (generated_from: "
          f"{stats.get('generated_from', '?')})")
    print()

    if updated:
        print("Updated counters:")
        for elem_id, stat_key, value, n in updated:
            print(f"  {elem_id:15s} <- {stat_key:22s} = {value}  "
                  f"({n} occurrence{'s' if n != 1 else ''} replaced)")

    if skipped_no_id:
        print(f"\nNo matching element in index.html for: {skipped_no_id} "
              f"(fine if index.html genuinely has no such counter — add "
              f"the real id to COUNTER_MAP if it does)")

    if skipped_no_stat:
        print(f"\nNo value in stats.json for: {skipped_no_stat} "
              f"(check compute_stats.py's CONFIG or stats.json contents)")

    if not updated:
        sys.exit(
            "\nERROR: zero counters were updated. Check that the id="
            "\"...\" attributes in COUNTER_MAP actually match index.html, "
            "and that stats.json has the expected keys."
        )

    print(f"\n✅ Wrote {INDEX_HTML.name} with {len(updated)} counter(s) "
          f"synced to stats.json")


if __name__ == "__main__":
    main()
