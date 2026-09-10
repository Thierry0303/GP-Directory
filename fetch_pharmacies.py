#!/usr/bin/env python3
"""
Build a London PHARMACIES dataset directly from the NHS Directory of Healthcare
Services (Service Search) API v3 — authoritative, with opening hours, website
and location.

The API has no filterable geo field, so we page through all UK pharmacies
(OrganisationTypeId = 'PHA') and keep those whose postcode is in a London
borough (london_geo.borough_from_postcode). Writes pharmacies.json for the
page builder.

Auth: NHS_SERVICE_SEARCH_KEY env var, or a local git-ignored apikey_prod.txt.
Setup: pip install requests
Run:   python fetch_pharmacies.py
"""
import json, os, sys, time, uuid
from pathlib import Path

import requests
from london_geo import borough_from_postcode

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "pharmacies.json"
URL = "https://api.service.nhs.uk/service-search-api"
PAGE = 1000                      # requested page size (API may cap lower)
WEEK = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday",
        "Saturday", "Sunday"]


def read_key() -> str:
    key = os.environ.get("NHS_SERVICE_SEARCH_KEY", "").strip()
    if not key:
        f = ROOT / "apikey_prod.txt"
        if f.exists():
            key = f.read_text(encoding="utf-8-sig", errors="replace").strip()
    if not key:
        sys.exit("Set NHS_SERVICE_SEARCH_KEY (or put your key in apikey_prod.txt).")
    return key


def contact(contacts, method):
    for c in contacts or []:
        if (c.get("ContactMethodType") or "").lower() == method.lower():
            v = (c.get("ContactValue") or "").strip()
            if v:
                return v
    return ""


def website(contacts):
    w = contact(contacts, "Website")
    if w and not w.lower().startswith(("http://", "https://")):
        w = "https://" + w
    return w


def address(rec):
    parts = [rec.get("Address1"), rec.get("Address2"), rec.get("Address3"),
             rec.get("City")]
    a = ", ".join(p.strip() for p in parts if p and p.strip())
    return a.title() if a.isupper() else a


def opening_hours(times):
    def collect(kind):
        by = {}
        for t in times or []:
            if (t.get("OpeningTimeType") or "") != kind:
                continue
            day = t.get("Weekday")
            if day not in WEEK:
                continue
            if t.get("IsOpen") and t.get("OpeningTime") and t.get("ClosingTime"):
                by.setdefault(day, []).append(f'{t["OpeningTime"]}–{t["ClosingTime"]}')
            else:
                by.setdefault(day, by.get(day, []))
        return by
    src = collect("General") or collect("Pharmacy") or collect("Surgery")
    if not src:
        return None
    out = {}
    for day in WEEK:
        if day in src:
            out[day] = ", ".join(src[day]) if src[day] else "Closed"
    return out or None


def parse(rec):
    contacts = rec.get("Contacts") or []
    lat, lng = rec.get("Latitude"), rec.get("Longitude")
    if not (isinstance(lat, (int, float)) and isinstance(lng, (int, float)) and (lat or lng)):
        lat = lng = None
    pc = (rec.get("Postcode") or "").strip()
    name = (rec.get("OrganisationName") or "").strip()
    name = name.title() if name.isupper() else name
    return {
        "o": rec.get("ODSCode") or "",
        "n": name,
        "a": address(rec),
        "p": pc,
        "ph": contact(contacts, "Telephone"),
        "web": website(contacts),
        "oh": opening_hours(rec.get("OpeningTimes")),
        "la": round(lat, 6) if lat is not None else None,
        "ln": round(lng, 6) if lng is not None else None,
        "ar": borough_from_postcode(pc),
        "type": "Pharmacy",
    }


def main():
    key = read_key()
    headers = {"apikey": key, "Accept": "application/json"}
    base = {"api-version": "3", "search": "*", "searchMode": "all",
            "$filter": "OrganisationTypeId eq 'PHA'", "$orderby": "ODSCode",
            "$count": "true", "$top": str(PAGE)}

    london, seen = [], set()
    skip, total, fetched = 0, None, 0
    while True:
        params = dict(base, **{"$skip": str(skip)})
        req_headers = {**headers, "X-Request-ID": str(uuid.uuid4())}
        for attempt in range(4):
            r = requests.get(URL, params=params, headers=req_headers, timeout=40)
            if r.status_code in (429, 503):
                time.sleep(1 + attempt); continue
            break
        if r.status_code != 200:
            sys.exit(f"HTTP {r.status_code} at skip={skip}: {r.text[:200]}")
        data = r.json()
        if total is None:
            total = data.get("@odata.count")
            print(f"UK pharmacies reported: {total}")
        recs = data.get("value", [])
        if not recs:
            break
        for rec in recs:
            fetched += 1
            ods = (rec.get("ODSCode") or "").upper()
            if ods in seen:
                continue
            seen.add(ods)
            pc = rec.get("Postcode") or ""
            if borough_from_postcode(pc):
                london.append(parse(rec))
        print(f"  fetched {fetched}"
              + (f"/{total}" if total else "") + f" — London so far: {len(london)}")
        skip += len(recs)
        if len(recs) < PAGE or (total and skip >= total):
            break

    # Sanity: London should be a meaningful slice of the UK total.
    if len(london) < 200:
        sys.exit(f"ABORT: only {len(london)} London pharmacies found — "
                 "something is off (filter/pagination). Not writing the file.")

    london.sort(key=lambda r: (r.get("ar", ""), r.get("n", "")))
    with_hours = sum(1 for r in london if r.get("oh"))
    with_web = sum(1 for r in london if r.get("web"))
    with_geo = sum(1 for r in london if r.get("la"))
    boroughs = len({r["ar"] for r in london if r.get("ar")})
    print(f"\nLondon pharmacies: {len(london)} across {boroughs} boroughs")
    print(f"  opening hours: {with_hours}, website: {with_web}, coords: {with_geo}")

    OUT.write_text(json.dumps(london, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {OUT.name} ({OUT.stat().st_size // 1024} KB).")


if __name__ == "__main__":
    main()
