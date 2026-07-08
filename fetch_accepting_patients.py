#!/usr/bin/env python3
"""
fetch_accepting_patients.py
Fetch the "accepting new patients" status for every London GP practice
from the NHS Directory of Healthcare Services (Service Search) API and
write accepting_patients.json:  { "<ODS>": true | false, ... }

refresh_nhs_data.py picks this file up (if present) and adds an "anp"
field to each NHS record, which powers the homepage filter and the
practice-page notice.

Setup (same key as fetch_nhs_service_search.py):
  1. Subscribe to "Directory of Healthcare Services (Service Search) API"
     at https://digital.nhs.uk/developer/api-catalogue
  2. Add GitHub secret NHS_SERVICE_SEARCH_KEY
Run BEFORE refresh_nhs_data.py in the workflow. Exits 0 (no-op) when the
key is missing so the pipeline never breaks.

The API's field for this has changed name over versions, so the script
requests the full document for the first practice, logs the fields it
sees, and then parses any of the known variants defensively.
"""
import json, os, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "accepting_patients.json"

BASE_URL = "https://api.service.nhs.uk/service-search-api/search?api-version=3"

AUTH_HEADER_CANDIDATES = [
    ("Authorization", "Bearer "),
    ("subscription-key", ""),
    ("apikey", ""),
    ("Ocp-Apim-Subscription-Key", ""),
]
WORKING_AUTH_HEADER = None

# Field names this API has used for acceptance status across versions.
FIELD_CANDIDATES = [
    "AcceptingNewPatients", "AcceptingPatients",
    "IsAcceptingPatients", "GPAcceptingNewPatients",
]

def parse_accepting(doc):
    """Return True/False/None from whatever shape the API gives us."""
    for field in FIELD_CANDIDATES:
        if field not in doc or doc[field] in (None, ""):
            continue
        v = doc[field]
        # Sometimes a JSON-encoded string
        if isinstance(v, str):
            s = v.strip().lower()
            if s in ("true", "yes", "y", "1"):  return True
            if s in ("false", "no", "n", "0"):  return False
            try:
                v = json.loads(v)
            except (ValueError, TypeError):
                continue
        if isinstance(v, bool):
            return v
        # Sometimes a list of dicts, e.g. [{"Id":.., "AcceptingPatients": true}]
        if isinstance(v, list):
            vals = []
            for item in v:
                if isinstance(item, dict):
                    for k, val in item.items():
                        if "accept" in k.lower() and isinstance(val, bool):
                            vals.append(val)
                elif isinstance(item, bool):
                    vals.append(item)
            if vals:
                return any(vals)
    return None

def load_ods_codes():
    for fname in ("merged.json", "data.json", "gps.json"):
        f = ROOT / fname
        if not f.exists():
            continue
        data = json.loads(f.read_text())
        codes = []
        for r in data:
            if r.get("type") == "Private":
                continue
            ods = (r.get("o") or r.get("ods_code") or "").strip().upper()
            if ods:
                codes.append(ods)
        if codes:
            print(f"{len(codes)} ODS codes from {fname}")
            return codes
    sys.exit("No merged.json / data.json / gps.json with ODS codes found.")

def query(ods, key, select=None, timeout=10):
    global WORKING_AUTH_HEADER
    payload = {"search": ods, "searchMode": "all",
               "searchFields": "ODSCode", "top": 1}
    if select:
        payload["select"] = select
    body = json.dumps(payload).encode()
    base = {"Content-Type": "application/json", "Accept": "application/json",
            "User-Agent": "londongp.directory/1.0"}
    candidates = [WORKING_AUTH_HEADER] if WORKING_AUTH_HEADER else AUTH_HEADER_CANDIDATES
    for hname, prefix in candidates:
        try:
            req = urllib.request.Request(
                BASE_URL, data=body,
                headers={**base, hname: f"{prefix}{key}"}, method="POST")
            with urllib.request.urlopen(req, timeout=timeout) as r:
                data = json.loads(r.read())
            if WORKING_AUTH_HEADER is None:
                WORKING_AUTH_HEADER = (hname, prefix)
                print(f"  AUTH OK with header: {hname}")
            results = data.get("value", data) if isinstance(data, dict) else data
            if not results:
                return None
            doc = results[0]
            if (doc.get("ODSCode") or "").upper() != ods:
                return None
            return doc
        except urllib.error.HTTPError as e:
            if e.code == 401:
                continue
            if e.code in (429, 503):
                time.sleep(1.5)
                continue
            return None
        except Exception:
            return None
    return None

def main():
    key = os.environ.get("NHS_SERVICE_SEARCH_KEY")
    if not key:
        print("NHS_SERVICE_SEARCH_KEY not set — skipping accepting-patients "
              "refresh (existing accepting_patients.json, if any, is kept).")
        return  # exit 0: never break the pipeline

    codes = load_ods_codes()

    # Probe: full document for the first code, so the logs show what
    # fields this API version actually returns.
    probe = query(codes[0], key)
    if probe is None:
        print("WARNING: probe query failed (auth or availability) — skipping.")
        return
    accepting_fields = [k for k in probe if "accept" in k.lower()]
    print(f"Probe {codes[0]}: acceptance-related fields = {accepting_fields or 'NONE'}")
    if not accepting_fields:
        print("WARNING: this API version exposes no acceptance field — skipping.")
        return

    results, stats = {}, Counter()
    def work(ods):
        doc = query(ods, key)
        return ods, (parse_accepting(doc) if doc else None)

    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = [ex.submit(work, c) for c in codes]
        for i, fut in enumerate(as_completed(futures), 1):
            ods, val = fut.result()
            if val is not None:
                results[ods] = val
            stats["yes" if val is True else "no" if val is False else "unknown"] += 1
            if i % 200 == 0:
                print(f"  {i}/{len(codes)}…")

    print(f"Done: {stats['yes']} accepting, {stats['no']} not accepting, "
          f"{stats['unknown']} unknown")
    # Don't clobber a good file with a bad run
    if not results:
        print("WARNING: no results — keeping previous accepting_patients.json.")
        return
    OUT.write_text(json.dumps(results, indent=1, sort_keys=True))
    print(f"Wrote {OUT.name} ({len(results)} practices)")

if __name__ == "__main__":
    main()
