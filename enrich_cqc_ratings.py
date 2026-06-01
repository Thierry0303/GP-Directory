#!/usr/bin/env python3
"""
Backfill CQC ratings for every record in gps.json (and merged.json)
where the rating is missing.

Why this exists
---------------
find_london_gp_gaps.py and build_gps_final.py added many NHS GP practices
via CQC discovery but stored them with empty cqc_rating / cqc_url fields.
While they were misclassified into wrong boroughs the gap was invisible.
Now that fix_boroughs.py has routed them to the right borough pages, the
"Not rated" cards stand out — even though most of these practices DO have
a CQC rating that we just didn't fetch.

How it works
------------
1. Load gps.json (and merged.json if it exists).
2. Find every record where cqc_rating is empty / missing.
3. For each, look up the CQC location by ODS code via the CQC API.
4. Extract the current overall rating + location URL.
5. Write back.

Run order
---------
After fetch_private_clinics.py + refresh_nhs_data.py + merge_into_dataset.py,
EITHER before or after fix_boroughs.py (doesn't matter — they touch
different fields).
"""

import json, os, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON    = ROOT / "gps.json"
MERGED_JSON = ROOT / "merged.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

# Field names for CQC fields on each record type.
# gps.json uses snake_case; merged.json's compact format uses cqc / cu.
RATING_FIELDS = ["cqc_rating", "cqc"]
URL_FIELDS    = ["cqc_url", "cu"]
ODS_FIELDS    = ["ods_code", "o"]

def get_first(rec, fields):
    for f in fields:
        v = rec.get(f)
        if v is not None and v != "":
            return v
    return ""

def get_ods(rec):
    return get_first(rec, ODS_FIELDS).strip().upper()

def get_rating(rec):
    return get_first(rec, RATING_FIELDS)

def get_url(rec):
    return get_first(rec, URL_FIELDS)

def set_rating(rec, rating, url):
    """Set rating + url on whichever fields the record uses."""
    if "cqc_rating" in rec or "cqc" not in rec:
        rec["cqc_rating"] = rating
    if "cqc" in rec or "cqc_rating" not in rec:
        rec["cqc"] = rating
    if "cqc_url" in rec or "cu" not in rec:
        rec["cqc_url"] = url
    if "cu" in rec or "cqc_url" not in rec:
        rec["cu"] = url

# ---------------------------------------------------------------- HTTP

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (cqc-enrichment)",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt); continue
            if e.code == 404: return None
            raise
        except Exception:
            if attempt < retries - 1:
                time.sleep(1); continue
            raise
    return None

def fetch_rating_for_ods(ods, key):
    """Return (rating, cqc_url) for an ODS code, or ('', '') if not found."""
    if not ods:
        return ("", "")
    # Step 1: find the CQC location whose odsCode matches this practice.
    try:
        data = cqc_get("/locations", {"odsCode": ods, "perPage": 5}, key)
    except Exception:
        return ("", "")
    if not data:
        return ("", "")
    locs = data.get("locations", []) or []
    if not locs:
        return ("", "")
    # Prefer a still-registered location over a deregistered one
    active = [l for l in locs if not l.get("deregistrationDate")] or locs
    loc_id = active[0].get("locationId", "")
    if not loc_id:
        return ("", "")
    # Step 2: get the detail for that location → currentRatings.overall.rating
    try:
        detail = cqc_get(f"/locations/{loc_id}", None, key)
    except Exception:
        return ("", "")
    if not detail:
        return ("", "")
    rating = ((detail.get("currentRatings", {}) or {})
              .get("overall", {}) or {}).get("rating", "")
    url = f"https://www.cqc.org.uk/location/{loc_id}"
    return (rating or "", url)

# ---------------------------------------------------------------- main

def enrich_file(path, key, workers=10):
    if not path.exists():
        print(f"  {path.name}: not found, skipping.")
        return Counter()

    data = json.loads(path.read_text())
    if not isinstance(data, list):
        print(f"  {path.name}: not a list, skipping.")
        return Counter()

    needs = [(i, get_ods(r)) for i, r in enumerate(data)
             if not get_rating(r) and get_ods(r)]
    print(f"\n  {path.name}: {len(data)} records, {len(needs)} missing CQC rating")

    if not needs:
        return Counter()

    found = 0
    not_in_cqc = 0
    no_rating = 0
    status_counts = Counter()

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_rating_for_ods, ods, key): (i, ods)
                   for i, ods in needs}
        done = 0
        for fut in as_completed(futures):
            i, ods = futures[fut]
            done += 1
            try:
                rating, url = fut.result()
            except Exception:
                rating, url = ("", "")
            if rating:
                set_rating(data[i], rating, url)
                found += 1
                status_counts[rating] += 1
            elif url:
                # Location exists in CQC but has no overall rating yet
                set_rating(data[i], "", url)
                no_rating += 1
                status_counts["(unrated)"] += 1
            else:
                not_in_cqc += 1
                status_counts["(not-in-cqc)"] += 1
            if done % 100 == 0 or done == len(needs):
                print(f"    {done}/{len(needs)} done — "
                      f"{found} rated, {no_rating} unrated, {not_in_cqc} not in CQC")

    path.write_text(json.dumps(data, indent=2))

    print(f"\n  {path.name} — final rating distribution for newly-enriched records:")
    for r, n in status_counts.most_common():
        print(f"    {r:20s} {n}")

    return status_counts

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    if not any(p.exists() for p in [GPS_JSON, MERGED_JSON]):
        sys.exit("Neither gps.json nor merged.json found.")

    for path in [GPS_JSON, MERGED_JSON]:
        enrich_file(path, key)

    print("\nDone. Re-run merge_into_dataset.py (or just hard-refresh the site) "
          "to surface the new ratings.")

if __name__ == "__main__":
    main()
