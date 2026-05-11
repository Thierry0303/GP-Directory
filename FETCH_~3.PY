#!/usr/bin/env python3
"""
Replace NHS-contract names in gps.json with the official names from the
NHS Directory of Healthcare Services (Service Search) API.

This is the authoritative source NHS.uk itself uses. Unlike scraping
NHS.uk directly, this API is designed for automated access and works
from GitHub Actions.

Endpoint
--------
  POST https://api.service.nhs.uk/service-search-api/search?api-version=3

Authentication
--------------
  Header: subscription-key: <KEY>      (most NHS Digital APIs)
  Or:     apikey: <KEY>                 (fallback)
  Or:     Ocp-Apim-Subscription-Key: <KEY>  (older NHS APIs)

The script tries the standard header first and falls back automatically.

Setup
-----
1. Subscribe to "Directory of Healthcare Services (Service Search) API"
   at https://digital.nhs.uk/developer/api-catalogue → get a key.
2. Add a GitHub secret `NHS_SERVICE_SEARCH_KEY` = your key.
3. Run the workflow.

Output
------
Each renamed record gets:
  name           ← official NHS name (e.g. "The Old Surgery")
  official_name  ← preserved original contract name
"""

import json, os, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

BASE_URL = "https://api.service.nhs.uk/service-search-api/search?api-version=3"

# The Service Search API uses HTTP Bearer authentication, per the API docs:
#   Authorization: Bearer <token>
# We also keep some legacy candidate headers in case the auth changes or the
# subscription tier uses a different scheme.
AUTH_HEADER_CANDIDATES = [
    ("Authorization", "Bearer "),     # HTTP Bearer (the documented one)
    ("subscription-key", ""),         # APIM key-style (fallback)
    ("apikey", ""),
    ("Ocp-Apim-Subscription-Key", ""),
]

# Determined at runtime — set on first successful response.
WORKING_AUTH_HEADER = None

def query_nhs(ods, key, timeout=10):
    """Query Service Search for one ODS code. Returns (status, name)."""
    global WORKING_AUTH_HEADER
    body = json.dumps({
        "search": ods,
        "searchMode": "all",
        "searchFields": "ODSCode",
        "top": 1,
        "select": "ODSCode,OrganisationName,OrganisationType,OrganisationSubType",
    }).encode("utf-8")
    base_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0",
    }
    # Try the working header first; otherwise iterate candidates.
    if WORKING_AUTH_HEADER:
        candidates = [WORKING_AUTH_HEADER]
    else:
        candidates = AUTH_HEADER_CANDIDATES

    last_err = None
    for header_name, value_prefix in candidates:
        try:
            req = urllib.request.Request(
                BASE_URL,
                data=body,
                headers={**base_headers, header_name: f"{value_prefix}{key}"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read())
            # Set the working header so future calls go straight to it.
            if WORKING_AUTH_HEADER is None:
                WORKING_AUTH_HEADER = (header_name, value_prefix)
                print(f"  AUTH OK with header: {header_name} (prefix: '{value_prefix}')")
            # Response is either {value: [...]} or just [...] depending on API version
            results = data.get("value", data) if isinstance(data, dict) else data
            if not results:
                return ("not-found", None)
            top = results[0]
            name = (top.get("OrganisationName") or "").strip()
            # Make sure the result is actually for THIS ODS code.
            if (top.get("ODSCode") or "").upper() != ods.upper():
                return ("wrong-ods", None)
            if not name:
                return ("no-name", None)
            return ("ok", name)
        except urllib.error.HTTPError as e:
            if e.code == 401:
                last_err = ("401", header_name)
                continue
            if e.code == 404:
                return ("not-found", None)
            if e.code in (429, 503):
                time.sleep(1)
                continue
            return ("http-error", f"{e.code}")
        except Exception as e:
            return ("error", str(e)[:40])

    return ("auth-failed", str(last_err))

def main():
    key = os.environ.get("NHS_SERVICE_SEARCH_KEY")
    if not key:
        sys.exit("NHS_SERVICE_SEARCH_KEY env var not set. Add it as a GitHub secret.")

    if not GPS_JSON.exists():
        sys.exit(f"{GPS_JSON} not found.")
    data = json.loads(GPS_JSON.read_text())
    if not isinstance(data, list):
        sys.exit("gps.json is not a JSON array.")
    print(f"Loaded {len(data)} records.")

    ods_codes = [(i, (r.get("ods_code") or "").strip().upper())
                 for i, r in enumerate(data)]
    ods_codes = [(i, c) for i, c in ods_codes if c]
    print(f"Looking up {len(ods_codes)} ODS codes via Service Search API…\n")

    # Do a single warmup call first to settle on the right auth header
    # before fanning out (otherwise every worker tries 3 headers).
    print("Warmup call to determine auth header…")
    warmup_status, warmup_name = query_nhs(ods_codes[0][1], key)
    print(f"  warmup: {warmup_status}  name={warmup_name}\n")
    if WORKING_AUTH_HEADER is None and warmup_status != "ok":
        sys.exit(f"ABORT: warmup failed with {warmup_status}. "
                 "Likely the subscription key is invalid, the wrong API was "
                 "subscribed, or the endpoint moved. Check the GitHub secret "
                 "and the API portal subscription status.")

    status_counts = Counter()
    sample_renames = []
    record_status = {}

    with ThreadPoolExecutor(max_workers=15) as pool:
        futures = {pool.submit(query_nhs, c, key): (i, c) for i, c in ods_codes}
        done = 0
        for fut in as_completed(futures):
            i, ods = futures[fut]
            done += 1
            try:
                status, name = fut.result()
            except Exception as e:
                status, name = "exception", str(e)[:40]
            status_counts[status] += 1
            record_status[i] = status

            if status == "ok" and name:
                old = (data[i].get("name") or "").strip()
                if name != old:
                    if "official_name" not in data[i]:
                        data[i]["official_name"] = old
                    data[i]["name"] = name
                    if len(sample_renames) < 25:
                        sample_renames.append((ods, old, name))

            if done % 100 == 0 or done == len(ods_codes):
                ok = sum(1 for s in record_status.values() if s == "ok")
                print(f"  {done}/{len(ods_codes)} done — "
                      f"ok: {ok}, "
                      f"not-found: {sum(1 for s in record_status.values() if s == 'not-found')}, "
                      f"errors: {sum(1 for s in record_status.values() if s not in ('ok','not-found'))}")

    print(f"\nStatus counts:")
    for s, n in status_counts.most_common():
        print(f"  {s:15s} {n}")

    if sample_renames:
        print(f"\nSample renames ({len(sample_renames)}):")
        for ods, old, new in sample_renames:
            print(f"  {ods:8s} {old[:35]:35s}  →  {new}")

    # Drop records the API says don't exist
    kept = [r for i, r in enumerate(data) if record_status.get(i) != "not-found"]
    dropped = len(data) - len(kept)
    print(f"\nKept:    {len(kept)}")
    print(f"Dropped: {dropped} (Service Search returned no result)")

    # Safety: refuse to drop more than 25%
    if dropped > len(data) * 0.25:
        sys.exit(f"\nABORT: would drop {dropped}/{len(data)} > 25%. "
                 "Likely an API issue. gps.json left unchanged.")

    GPS_JSON.write_text(json.dumps(kept, indent=2))
    print(f"\nWrote {GPS_JSON} — {len(kept)} records.")

if __name__ == "__main__":
    main()
