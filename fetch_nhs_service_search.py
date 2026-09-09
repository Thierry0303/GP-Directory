#!/usr/bin/env python3
"""
Enrich London GP data from the NHS Directory of Healthcare Services
(Service Search) API v3 — the authoritative source NHS.uk itself uses.

For every ODS code in gps.json this fetches the official record and writes a
cache, nhs_service_search.json (ODS -> fields), which refresh_nhs_data.py then
merges into the site. Pulls: official name, address, postcode, phone, website,
precise coordinates, live status, and opening hours.

Proven production method (verified end-to-end):
    GET https://api.service.nhs.uk/service-search-api?api-version=3
        &search=<ODS>&searchMode=all&searchFields=ODSCode&$top=1
    Header: apikey: <KEY>
(v1/v2 were retired 2 Feb 2026 — v3 only. Auth is a simple API key, no JWT.)

Setup
-----
1. Put your PRODUCTION API key in the env var NHS_SERVICE_SEARCH_KEY, e.g.
      Windows PowerShell:  $env:NHS_SERVICE_SEARCH_KEY = "<your key>"
      macOS/Linux:         export NHS_SERVICE_SEARCH_KEY="<your key>"
   (or drop it in a local file apikey_prod.txt next to this script — that file
    is git-ignored and must never be committed).
2. pip install requests
3. python fetch_nhs_service_search.py
4. Commit the regenerated nhs_service_search.json (public NHS data — safe to
   commit; your key is not).
"""

import json, os, sys, time, uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"
OUT_JSON = ROOT / "nhs_service_search.json"

URL = "https://api.service.nhs.uk/service-search-api"
API_VERSION = "3"
WEEK_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday",
              "Friday", "Saturday", "Sunday"]


def read_key() -> str:
    key = os.environ.get("NHS_SERVICE_SEARCH_KEY", "").strip()
    if not key:
        f = ROOT / "apikey_prod.txt"
        if f.exists():
            key = f.read_text(encoding="utf-8-sig", errors="replace").strip()
    if not key:
        sys.exit("Set NHS_SERVICE_SEARCH_KEY (or put your key in apikey_prod.txt).")
    return key


def _contact(contacts, method):
    """First ContactValue whose ContactMethodType matches (e.g. Website)."""
    for c in contacts or []:
        if (c.get("ContactMethodType") or "").lower() == method.lower():
            v = (c.get("ContactValue") or "").strip()
            if v:
                return v
    return ""


def _website(contacts):
    w = _contact(contacts, "Website")
    if w and not w.lower().startswith(("http://", "https://")):
        w = "https://" + w
    return w


def _address(rec):
    parts = [rec.get("Address1"), rec.get("Address2"), rec.get("Address3"),
             rec.get("City")]
    addr = ", ".join(p.strip() for p in parts if p and p.strip())
    return addr.title() if addr.isupper() else addr


def _opening_hours(times):
    """Build {Weekday: 'HH:MM-HH:MM' or 'Closed'} from the 'General' entries
    (the practice's overall open hours; 'Surgery' = split consulting sessions).
    Falls back to Surgery if a practice has no General entries."""
    def collect(kind):
        by_day = {}
        for t in times or []:
            if (t.get("OpeningTimeType") or "") != kind:
                continue
            day = t.get("Weekday")
            if day not in WEEK_ORDER:
                continue
            if t.get("IsOpen") and t.get("OpeningTime") and t.get("ClosingTime"):
                by_day.setdefault(day, []).append(
                    f'{t["OpeningTime"]}–{t["ClosingTime"]}')
            else:
                by_day.setdefault(day, by_day.get(day, []))  # ensure key exists
        return by_day

    src = collect("General") or {}
    if not src:
        src = collect("Surgery") or {}
    if not src:
        return None
    out = {}
    for day in WEEK_ORDER:
        if day in src:
            ranges = src[day]
            out[day] = ", ".join(ranges) if ranges else "Closed"
    return out or None


def query(ods, key, timeout=20):
    params = {"api-version": API_VERSION, "search": ods, "searchMode": "all",
              "searchFields": "ODSCode", "$top": "1"}
    headers = {"apikey": key, "Accept": "application/json",
               "X-Request-ID": str(uuid.uuid4())}
    for attempt in range(3):
        try:
            r = requests.get(URL, params=params, headers=headers, timeout=timeout)
        except Exception as e:
            return ("error", str(e)[:60])
        if r.status_code in (429, 503):
            time.sleep(1 + attempt)
            continue
        if r.status_code == 401:
            return ("auth-failed", r.text[:120])
        if r.status_code != 200:
            return ("http-%d" % r.status_code, r.text[:120])
        recs = (r.json() or {}).get("value", [])
        if not recs:
            return ("not-found", None)
        rec = recs[0]
        if (rec.get("ODSCode") or "").upper() != ods.upper():
            return ("wrong-ods", None)
        contacts = rec.get("Contacts") or []
        lat, lng = rec.get("Latitude"), rec.get("Longitude")
        return ("ok", {
            "name": (rec.get("OrganisationName") or "").strip(),
            "status": rec.get("OrganisationStatus") or "",
            "type": rec.get("OrganisationType") or "",
            "address": _address(rec),
            "postcode": (rec.get("Postcode") or "").strip(),
            "phone": _contact(contacts, "Telephone"),
            "website": _website(contacts),
            "lat": round(lat, 6) if isinstance(lat, (int, float)) else None,
            "lng": round(lng, 6) if isinstance(lng, (int, float)) else None,
            "opening": _opening_hours(rec.get("OpeningTimes")),
        })
    return ("throttled", None)


def main():
    key = read_key()
    if not GPS_JSON.exists():
        sys.exit(f"{GPS_JSON} not found.")
    data = json.loads(GPS_JSON.read_text())
    ods_codes = sorted({(r.get("ods_code") or "").strip().upper()
                        for r in data if (r.get("ods_code") or "").strip()})
    print(f"Loaded {len(data)} records; {len(ods_codes)} unique ODS codes.\n")

    # Warmup to fail fast on a bad key / wrong subscription.
    print("Warmup:", ods_codes[0])
    st, val = query(ods_codes[0], key)
    print("  ->", st, (val or {}).get("name") if isinstance(val, dict) else val, "\n")
    if st in ("auth-failed",) or (st != "ok" and st != "not-found"):
        sys.exit(f"ABORT: warmup returned {st}: {val}. "
                 "Check the key is your enabled production Service Search key.")

    cache, status_counts = {}, Counter()
    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = {pool.submit(query, c, key): c for c in ods_codes}
        done = 0
        for fut in as_completed(futs):
            ods = futs[fut]
            st, val = fut.result()
            status_counts[st] += 1
            if st == "ok" and isinstance(val, dict):
                cache[ods] = val
            done += 1
            if done % 100 == 0 or done == len(ods_codes):
                print(f"  {done}/{len(ods_codes)} — ok:{status_counts['ok']} "
                      f"not-found:{status_counts['not-found']} "
                      f"other:{done - status_counts['ok'] - status_counts['not-found']}")

    print("\nStatus counts:")
    for s, n in status_counts.most_common():
        print(f"  {s:14s} {n}")

    # Coverage sanity check.
    with_hours = sum(1 for v in cache.values() if v.get("opening"))
    with_web = sum(1 for v in cache.values() if v.get("website"))
    with_geo = sum(1 for v in cache.values() if v.get("lat"))
    print(f"\nEnriched {len(cache)} practices — "
          f"coords:{with_geo}, website:{with_web}, opening hours:{with_hours}")

    if len(cache) < len(ods_codes) * 0.5:
        sys.exit(f"ABORT: only {len(cache)}/{len(ods_codes)} enriched (<50%). "
                 "Not overwriting the cache — investigate before committing.")

    OUT_JSON.write_text(json.dumps(cache, indent=2, ensure_ascii=False,
                                   sort_keys=True))
    print(f"\nWrote {OUT_JSON.name} ({len(cache)} records, "
          f"{OUT_JSON.stat().st_size // 1024} KB).")
    print("Next: python refresh_nhs_data.py  (merges this in), then rebuild pages.")


if __name__ == "__main__":
    main()
