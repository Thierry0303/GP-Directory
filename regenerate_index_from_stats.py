#!/usr/bin/env python3
r"""
regenerate_index_from_stats.py

Regenerates index.html from index.template.html using the FINAL,
London-filtered counts in stats.json. This is now the ONLY script that
writes index.html's __NHS_COUNT__ / __PRIVATE_COUNT__ / __PRACTICE_COUNT__
/ __BOROUGH_NAV__ / __UPDATED_DATE__ placeholders.

WHY THIS EXISTS
----------------
Previously THREE different scripts wrote index.html at three different
points in the pipeline, each with different numbers:
  1. refresh_nhs_data.py   — step 1, before private clinics even exist
                              (__PRIVATE_COUNT__ hardcoded to "0")
  2. fix_boroughs.py       — step 13, counted EVERY record in merged.json,
                              including ones its own postcode lookup had
                              just determined were outside London (it
                              clears their "ar" field but leaves them in
                              merged.json)
  3. (nothing after this)  — so whichever of the above ran LAST won, and
                              it was always fix_boroughs.py's inflated,
                              non-London-inclusive count.

Meanwhile /boroughs/ (build_borough_index.py) only ever sums records that
DO have a valid borough — so it naturally excluded the non-London records
fix_boroughs.py had cleared. That mismatch (records with no borough,
counted on the homepage but not on /boroughs/) was the entire 28/46
NHS/private gap.

compute_stats.py now excludes those same non-London records (see its
`has_borough` filter), so stats.json's numbers already match what
/boroughs/ shows. This script just needs to get those numbers into
index.html, and nothing else may touch index.html's placeholders again.

WHERE THIS SLOTS INTO THE WORKFLOW
------------------------------------
    ... fix_boroughs.py            (now only refreshes data.json)
    ... fetch_cqc_domain_ratings.py
    ... compute_stats.py
    ---> regenerate_index_from_stats.py   <--- ADD HERE (new step)
    ... build_borough_pages.py
    ... (rest of build_* steps)

update_index_counters.py (added earlier) can be REMOVED from the workflow
— it was editing the client-side cntNHS/cntPriv/cntAll spans, which are
harmless placeholders overwritten instantly by JS on page load and were
never the actual bug.

WHAT TO REMOVE FROM refresh_nhs_data.py
-------------------------------------------
Find wherever it does something like:
    html = (html
            .replace("__NHS_COUNT__", str(len(merged)))
            .replace("__PRIVATE_COUNT__", "0")
            ...)
    INDEX_HTML.write_text(...)
and delete it. refresh_nhs_data.py should keep writing gps.json / whatever
NHS-only data it produces, but should not touch index.html at all —
running first in the pipeline, it can never have the right numbers anyway.
"""

import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
STATS_JSON = ROOT / "stats.json"
MERGED_JSON = ROOT / "merged.json"
TEMPLATE_HTML = ROOT / "index.template.html"
INDEX_HTML = ROOT / "index.html"

# Which field on merged.json records holds the borough name — must match
# BOROUGH_FIELD in compute_stats.py.
BOROUGH_FIELD = "ar"


def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower().replace("&", "and")).strip("-")


def main():
    for path in (STATS_JSON, MERGED_JSON, TEMPLATE_HTML):
        if not path.exists():
            sys.exit(f"ERROR: {path} not found.")

    stats = json.loads(STATS_JSON.read_text(encoding="utf-8"))
    combined = json.loads(MERGED_JSON.read_text(encoding="utf-8"))
    template = TEMPLATE_HTML.read_text(encoding="utf-8")

    required = ("nhs_practice_count", "private_clinic_count", "total_records")
    missing = [k for k in required if k not in stats]
    if missing:
        sys.exit(
            f"ERROR: stats.json is missing {missing}. Re-run compute_stats.py "
            f"first (it must run before this script)."
        )

    # Borough nav: same set of boroughs compute_stats.py counted (i.e.
    # only records with a non-empty borough — excludes the non-London
    # ones fix_boroughs.py cleared).
    boroughs = sorted({
        r[BOROUGH_FIELD] for r in combined if r.get(BOROUGH_FIELD)
    })
    borough_nav = "\n      ".join(
        f'<a href="/practice/{slugify(b)}/">{b}</a>' for b in boroughs
    )

    try:
        today = datetime.now().strftime("%-d %B %Y")
    except ValueError:
        today = datetime.now().strftime("%d %B %Y")

    html = (template
            .replace("__NHS_COUNT__", str(stats["nhs_practice_count"]))
            .replace("__PRIVATE_COUNT__", str(stats["private_clinic_count"]))
            .replace("__PRACTICE_COUNT__", str(stats["total_records"]))
            .replace("__BOROUGH_NAV__", borough_nav)
            .replace("__UPDATED_DATE__", today))

    if re.search(r"__[A-Z_]+__", html):
        leftover = sorted(set(re.findall(r"__[A-Z_]+__", html)))
        print(f"WARNING: unreplaced placeholders remain in index.html: "
              f"{leftover}. Add them to this script if they're real.")

    INDEX_HTML.write_text(html, encoding="utf-8")

    print(f"Regenerated {INDEX_HTML.name} from stats.json "
          f"(generated_from: {stats.get('generated_from', '?')})")
    print(f"  NHS practices:   {stats['nhs_practice_count']}")
    print(f"  Private clinics: {stats['private_clinic_count']}")
    print(f"  Total:           {stats['total_records']}")
    print(f"  Boroughs:        {len(boroughs)}")
    if stats.get("excluded_non_london"):
        print(f"  (excluded {stats['excluded_non_london']} non-London "
              f"records — these are still in merged.json/data.json for "
              f"search, just not counted here)")


if __name__ == "__main__":
    main()
