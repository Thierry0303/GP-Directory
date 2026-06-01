#!/usr/bin/env python3
"""
Backfill CQC ratings for every record where it's empty, by paginating the
full UK CQC locations index and building an ODS-code → location-id map
client-side.

Why this approach
-----------------
The CQC public API does NOT actually support `?odsCode=...` as a filter on
/locations. It silently returns random pages, so the previous lookup
approach returned 0 matches for every code. The only reliable way is the
same one fetch_private_clinics.py uses: paginate everything (~50k UK
locations, ~3-5 minutes) and match on the ODS code present in each
summary record.

After we have the ODS → locationId map we fetch details only for the
~1000 locations we actually care about (in parallel) to get the rating.
"""

import json, os, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON    = ROOT / "gps.json"
MERGED_JSON = ROOT / "merged.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

RATING_FIELDS = ["cqc_rating", "cqc"]
URL_FIELDS    = ["cqc_url", "cu"]
ODS_FIELDS    = ["ods_code", "o"]

def get_first(rec, fields):
    for f in fields:
        v = rec.get(f)
        if v is not None and v != "":
            return v
    return ""

def get_ods(rec):    return get_first(rec, ODS_FIELDS).strip().upper()
def get_rating(rec): return get_first(rec, RATING_FIELDS)

def set_rating(rec, rating, url):
    """Update both naming conventions if either is present, otherwise add
    snake_case form (matches gps.json convention)."""
    has_snake = "cqc_rating" in rec or "cqc_url" in rec
    has_short = "cqc" in rec or "cu" in rec
    if has_snake or not has_short:
        rec["cqc_rating"] = rating
        rec["cqc_url"]    = url
    if has_short:
        rec["cqc"] = rating
        rec["cu"]  = url

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
            with urllib.request.urlopen(req, timeout=30) as r:
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

# ---------------------------------------------------------------- discovery

def build_ods_to_loc_map(key, wanted_ods_set):
    """Paginate /locations and capture loc_id for every ODS code we need.

    Only summary records are scanned (perPage=1000). The summary doesn't
    include the rating, but it does include odsCode + locationId. We stop
    as soon as we've seen every code we want."""
    print(f"Paginating CQC /locations to find {len(wanted_ods_set)} ODS codes…")
    page = 1
    per_page = 1000
    found = {}  # ods -> locationId
    total = 0
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        if not data: break
        items = data.get("locations", []) or []
        if not items: break
        total += len(items)
        for loc in items:
            ods = (loc.get("odsCode") or "").strip().upper()
            if ods and ods in wanted_ods_set and ods not in found:
                found[ods] = loc.get("locationId", "")
        if len(found) >= len(wanted_ods_set):
            print(f"  page {page} — found ALL {len(found)} target codes, stopping early.")
            break
        total_pages = data.get("totalPages", 1)
        if page % 10 == 0 or page >= total_pages:
            print(f"  page {page}/{total_pages} — "
                  f"{total} UK seen, {len(found)}/{len(wanted_ods_set)} target codes found")
        if page >= total_pages: break
        page += 1
        time.sleep(0.15)
    return found

def fetch_rating(loc_id, key):
    if not loc_id: return ("", "")
    d = cqc_get(f"/locations/{loc_id}", None, key)
    if not d: return ("", "")
    rating = ((d.get("currentRatings", {}) or {})
              .get("overall", {}) or {}).get("rating", "")
    return (rating or "", f"https://www.cqc.org.uk/location/{loc_id}")

# ---------------------------------------------------------------- main

def enrich_file(path, key, ods_to_loc, rating_cache):
    """Apply (possibly cached) ratings to one file. Returns Counter."""
    if not path.exists(): return Counter()
    data = json.loads(path.read_text())
    if not isinstance(data, list): return Counter()

    needs = [(i, get_ods(r)) for i, r in enumerate(data)
             if not get_rating(r) and get_ods(r)]
    print(f"\n  {path.name}: {len(data)} records, {len(needs)} needing rating")
    if not needs: return Counter()

    # Resolve ratings for any ODS codes we haven't fetched yet
    uncached = [ods for _, ods in needs if ods in ods_to_loc and ods not in rating_cache]
    uncached_unique = list(set(uncached))
    if uncached_unique:
        print(f"    Fetching detail for {len(uncached_unique)} locations (parallel)…")
        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = {pool.submit(fetch_rating, ods_to_loc[ods], key): ods
                       for ods in uncached_unique}
            done = 0
            for fut in as_completed(futures):
                ods = futures[fut]
                done += 1
                try:
                    rating, url = fut.result()
                except Exception:
                    rating, url = ("", "")
                rating_cache[ods] = (rating, url)
                if done % 100 == 0 or done == len(uncached_unique):
                    rated = sum(1 for r, _ in rating_cache.values() if r)
                    print(f"      {done}/{len(uncached_unique)} — {rated} have a rating")

    # Apply to records
    status = Counter()
    for i, ods in needs:
        rating, url = rating_cache.get(ods, ("", ""))
        if rating:
            set_rating(data[i], rating, url)
            status[rating] += 1
        elif url:
            set_rating(data[i], "", url)
            status["(unrated)"] += 1
        else:
            status["(no-cqc-record)"] += 1

    path.write_text(json.dumps(data, indent=2))
    print(f"\n  {path.name} — rating distribution for newly-enriched records:")
    for r, n in status.most_common():
        print(f"    {r:25s} {n}")
    return status

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    # Collect every ODS code that needs a rating across both files
    wanted = set()
    for path in [GPS_JSON, MERGED_JSON]:
        if not path.exists(): continue
        for r in json.loads(path.read_text()):
            if not get_rating(r):
                ods = get_ods(r)
                if ods: wanted.add(ods)

    if not wanted:
        print("Nothing to do — every record already has a rating.")
        return
    print(f"Need ratings for {len(wanted)} unique ODS codes.\n")

    # One-pass pagination to map ODS → location_id
    ods_to_loc = build_ods_to_loc_map(key, wanted)
    print(f"\nMatched {len(ods_to_loc)}/{len(wanted)} ODS codes to CQC locations.")
    missing = wanted - set(ods_to_loc)
    if missing:
        print(f"({len(missing)} not present in CQC's location index — "
              "likely closed practices or branch surgeries.)\n")

    # Apply to each file using a shared rating cache so we only fetch
    # detail once per ODS code even though we touch two files.
    rating_cache = {}
    for path in [GPS_JSON, MERGED_JSON]:
        enrich_file(path, key, ods_to_loc, rating_cache)

if __name__ == "__main__":
    main()
