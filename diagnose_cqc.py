#!/usr/bin/env python3
"""
Find a CQC location by name/postcode and dump the full detail JSON.

Use this to figure out which API field a rating actually lives in for a
practice we KNOW is rated on cqc.org.uk but shows "Not rated" in our data.

Usage:
    CQC_KEY=xxx python diagnose_cqc.py "Albany Practice" TW8
    CQC_KEY=xxx python diagnose_cqc.py "Bridgestock"
"""

import json, os, sys, time, urllib.request, urllib.error, urllib.parse

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

def cqc_get(path, params, key):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/diagnose/1.0",
    }
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                time.sleep(2 ** attempt); continue
            if e.code == 404: return None
            raise

def find_locations(name_fragment, postcode_hint, key):
    """Paginate CQC locations looking for matches."""
    print(f"Searching CQC for '{name_fragment}'"
          + (f" near {postcode_hint}" if postcode_hint else "") + "…")
    matches = []
    page = 1
    name_low = name_fragment.lower()
    pc_low = (postcode_hint or "").lower()
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": 1000}, key)
        if not data: break
        items = data.get("locations", []) or []
        if not items: break
        for loc in items:
            nm = (loc.get("locationName") or loc.get("name") or "").lower()
            pc = (loc.get("postalCode") or "").lower()
            if name_low in nm and (not pc_low or pc.startswith(pc_low)):
                matches.append(loc)
        total_pages = data.get("totalPages", 1)
        if page % 20 == 0:
            print(f"  page {page}/{total_pages} — {len(matches)} matches so far")
        if page >= total_pages: break
        page += 1
    print(f"\nFound {len(matches)} matching summary records.\n")
    return matches

def walk_for_rating(obj, path=""):
    """Walk a nested dict/list looking for any key that contains 'rating'
    or any value that's one of the four CQC ratings."""
    ratings = {"outstanding", "good", "requires improvement", "inadequate"}
    hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}" if path else k
            if "rating" in k.lower() or "assessment" in k.lower():
                if isinstance(v, (str, int, float)) or v is None:
                    hits.append((p, repr(v)))
                else:
                    hits.append((p, f"<{type(v).__name__}>"))
            if isinstance(v, str) and v.strip().lower() in ratings:
                hits.append((f"{p}  ⟵ RATING VALUE", repr(v)))
            hits.extend(walk_for_rating(v, p))
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:5]):
            hits.extend(walk_for_rating(v, f"{path}[{i}]"))
    return hits

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")
    if len(sys.argv) < 2:
        sys.exit("Usage: diagnose_cqc.py 'Practice Name' [postcode-prefix]")
    name = sys.argv[1]
    pc = sys.argv[2] if len(sys.argv) > 2 else ""

    matches = find_locations(name, pc, key)
    if not matches:
        print("No matches. Try a shorter name fragment.")
        return

    for i, loc in enumerate(matches[:5]):
        loc_id = loc.get("locationId", "")
        print("=" * 78)
        print(f"Match {i+1}: {loc.get('locationName')}")
        print(f"  locationId: {loc_id}")
        print(f"  postcode  : {loc.get('postalCode')}")
        print(f"  odsCode   : {loc.get('odsCode', '(not in summary)')}")
        print(f"  URL       : https://www.cqc.org.uk/location/{loc_id}")
        print()

        if not loc_id: continue
        detail = cqc_get(f"/locations/{loc_id}", None, key)
        if not detail:
            print("  (failed to fetch detail)")
            continue

        print(f"  Detail has top-level keys:")
        print(f"    {sorted(detail.keys())}\n")

        print(f"  odsCode in detail: {detail.get('odsCode', '(missing)')}")
        print(f"  type            : {detail.get('type', '(missing)')}")
        print()

        print("  All rating-related paths found by walking the JSON tree:")
        for p, v in walk_for_rating(detail):
            print(f"    {p:60s} = {v}")
        print()

if __name__ == "__main__":
    main()
