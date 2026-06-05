#!/usr/bin/env python3
"""
Fix borough assignment for every record in gps.json, private_clinics.json
and merged.json using authoritative ONS postcode geography (via postcodes.io).

Why this exists
---------------
Our previous BOROUGH_MAP was a hand-coded postcode-district → borough table.
That's coarse: districts like N4, N16, N19, EC1, NW5 span multiple boroughs.
ICB documents confirm the gap — e.g. Islington should show 31 NHS GP
practices but our site shows ~12 because some are mis-routed to Hackney,
Haringey or Camden.

postcodes.io exposes ONS Postcode Directory (the canonical source). Every
UK postcode → exactly one Local Authority District. Bulk endpoint takes
100 postcodes at a time.

What this script does
---------------------
1. Loads every record across gps.json, private_clinics.json, merged.json.
2. Extracts unique postcodes.
3. Bulk-queries postcodes.io for the admin_district (London borough) of each.
4. Normalises the borough name to match our existing convention
   ("City of Westminster" → "Westminster", "Kensington and Chelsea" →
   "Kensington & Chelsea", etc.).
5. Rewrites the `ar` / `area` / `borough` field on every record.
6. Reports per-borough deltas — which boroughs gained / lost practices.

Output
------
Writes the three JSON files back in place. Prints a per-borough summary
of before/after counts so we can verify the fix.

Run order
---------
This should run AFTER fetch_private_clinics.py + refresh_nhs_data.py,
and BEFORE merge_into_dataset.py / build_borough_pages.py.
"""

import json, sys, time, urllib.request, urllib.error, urllib.parse
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON      = ROOT / "gps.json"
PRIVATE_JSON  = ROOT / "private_clinics.json"
MERGED_JSON   = ROOT / "merged.json"

POSTCODES_API = "https://api.postcodes.io/postcodes"
BATCH_SIZE    = 100

# Normalise postcodes.io's `admin_district` strings to the borough names
# the rest of the site already uses. Anything not in this map is taken
# verbatim from postcodes.io.
NAME_NORMALISE = {
    "City of Westminster":        "Westminster",
    "Kensington and Chelsea":     "Kensington & Chelsea",
    "Hammersmith and Fulham":     "Hammersmith & Fulham",
    "Barking and Dagenham":       "Barking & Dagenham",
    "Richmond upon Thames":       "Richmond upon Thames",
    "Kingston upon Thames":       "Kingston upon Thames",
    "City of London":             "City of London",
    "Westminster":                "Westminster",
}

# Local authorities considered "London" — keeps us from accidentally
# tagging non-London records (e.g. a Surrey practice that crept in).
LONDON_AUTHORITIES = {
    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley",
    "Camden", "City of London", "Croydon", "Ealing", "Enfield",
    "Greenwich", "Hackney", "Hammersmith and Fulham", "Haringey",
    "Harrow", "Havering", "Hillingdon", "Hounslow", "Islington",
    "Kensington and Chelsea", "Kingston upon Thames", "Lambeth",
    "Lewisham", "Merton", "Newham", "Redbridge", "Richmond upon Thames",
    "Southwark", "Sutton", "Tower Hamlets", "Waltham Forest",
    "Wandsworth", "Westminster", "City of Westminster",
}

# Field names used for the borough on each record type.
# gps.json / private_clinics.json use snake_case; merged.json uses the
# compact `ar` key consumed by the template.
BOROUGH_FIELDS = ["ar", "borough", "area"]
POSTCODE_FIELDS = ["p", "postcode", "post_code"]

def normalise_borough(s):
    if not s: return ""
    return NAME_NORMALISE.get(s, s)

def is_london(authority):
    return (authority or "") in LONDON_AUTHORITIES

def get_postcode(rec):
    for f in POSTCODE_FIELDS:
        v = rec.get(f)
        if v: return v.strip().upper()
    return ""

def set_borough(rec, borough):
    """Set whichever borough field already exists; create `ar` if none."""
    found = False
    for f in BOROUGH_FIELDS:
        if f in rec:
            rec[f] = borough
            found = True
    if not found:
        rec["ar"] = borough

def get_borough(rec):
    for f in BOROUGH_FIELDS:
        v = rec.get(f)
        if v: return v
    return ""

def bulk_lookup(postcodes, retries=3):
    """Query postcodes.io for up to 100 postcodes. Returns {pc: borough}."""
    body = json.dumps({"postcodes": list(postcodes)}).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (borough-mapping)",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(POSTCODES_API, data=body,
                                          headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.loads(r.read())
            break
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt); continue
            raise
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1); continue
            raise

    out = {}
    for entry in data.get("result", []) or []:
        query = entry.get("query", "")
        result = entry.get("result")
        if not result:
            out[query] = None
            continue
        district = result.get("admin_district") or ""
        if not is_london(district):
            out[query] = None  # outside London
            continue
        out[query] = normalise_borough(district)
    return out

def build_postcode_lookup(all_postcodes):
    """Bulk-lookup every unique postcode. Returns {pc: borough}."""
    print(f"Looking up {len(all_postcodes)} unique postcodes via postcodes.io…")
    lookup = {}
    batches = [list(all_postcodes)[i:i+BATCH_SIZE]
               for i in range(0, len(all_postcodes), BATCH_SIZE)]
    for i, batch in enumerate(batches, 1):
        result = bulk_lookup(batch)
        lookup.update(result)
        print(f"  batch {i}/{len(batches)} — {sum(1 for v in lookup.values() if v)} resolved")
        # Small courtesy delay between batches.
        if i < len(batches):
            time.sleep(0.3)
    not_london = sum(1 for v in lookup.values() if v is None)
    print(f"\n  Resolved London boroughs: {sum(1 for v in lookup.values() if v)}")
    print(f"  Outside London / unresolved:  {not_london}")
    return lookup

def apply_to_file(path, lookup):
    """Rewrite borough field on every record in the file. Returns delta dict."""
    if not path.exists():
        print(f"\n  {path.name}: not found, skipping.")
        return Counter(), Counter()
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        print(f"\n  {path.name}: not a list, skipping.")
        return Counter(), Counter()

    before = Counter()
    after = Counter()
    fixed = 0
    cleared = 0
    for rec in data:
        old = get_borough(rec) or "(none)"
        before[old] += 1
        pc = get_postcode(rec)
        new = lookup.get(pc)
        if new is None:
            # Not a London postcode — strip the borough so it doesn't
            # falsely appear under any borough page.
            if old != "(none)":
                set_borough(rec, "")
                cleared += 1
            after["(non-London)"] += 1
        else:
            if old != new:
                set_borough(rec, new)
                fixed += 1
            after[new] += 1

    path.write_text(json.dumps(data, indent=2))
    print(f"\n  {path.name}: {len(data)} records — "
          f"{fixed} borough fixes, {cleared} non-London cleared")
    return before, after

def print_delta(name, before, after):
    print(f"\n{name} — per-borough delta")
    print(f"{'Borough':32s} {'Before':>8s} {'After':>8s} {'Δ':>8s}")
    print("-" * 60)
    boroughs = sorted(set(before.keys()) | set(after.keys()))
    for b in boroughs:
        bef = before.get(b, 0)
        aft = after.get(b, 0)
        delta = aft - bef
        flag = ""
        if delta > 0: flag = f"+{delta}"
        elif delta < 0: flag = f"{delta}"
        if delta != 0:
            print(f"  {b:30s} {bef:>8d} {aft:>8d} {flag:>8s}")

def main():
    paths = [GPS_JSON, PRIVATE_JSON, MERGED_JSON]
    existing = [p for p in paths if p.exists()]
    if not existing:
        sys.exit("None of gps.json / private_clinics.json / merged.json found.")

    # Collect every unique postcode across all files.
    all_postcodes = set()
    for p in existing:
        data = json.loads(p.read_text())
        if not isinstance(data, list): continue
        for rec in data:
            pc = get_postcode(rec)
            if pc:
                all_postcodes.add(pc)

    if not all_postcodes:
        sys.exit("No postcodes found in any input file.")

    lookup = build_postcode_lookup(all_postcodes)

    # Apply to each file, capture before/after for the most-visible one.
    for p in existing:
        before, after = apply_to_file(p, lookup)
        if p.name == "merged.json":
            print_delta(p.name, before, after)

    print("\nDone. Re-run the page builders (build_borough_pages.py, "
          "build_specialty_pages.py) to regenerate the site with correct "
          "borough assignments.")

if __name__ == "__main__":
    main()
