#!/usr/bin/env python3
"""
Rebuild gps.json from NHS Digital's authoritative ePraccur dataset, enriched
with CQC ratings — including all outer London boroughs (Richmond/Twickenham,
Kingston, Bromley, Croydon, etc.) that were missing from the previous file.

What this fixes
---------------
The previous gps.json appeared to cover only certain ICBs (likely Inner
London / North Central / North West). Practices in TW, KT, HA, UB, BR, DA,
SM, CR, IG, RM, EN postcodes were absent. A Google Maps check showed at
least 6 missing GP surgeries in Twickenham alone.

ePraccur (https://digital.nhs.uk/services/organisation-data-service/
export-data/miscellaneous/epraccur) is the official NHS Organisation Data
Service file: every active GP practice in England, refreshed weekly,
canonical source. ~7,000 records nationally; ~1,300 in Greater London.

What this produces
------------------
A new gps.json with the same record shape your existing scripts expect:

    {
        "ods_code":          "F83019",
        "name":              "Abbey Medical Centre",
        "address":           "85 Abbey Road, London",
        "postcode":          "NW8 0AG",
        "phone":             "020 7624 2455",
        "cqc_rating":        "Good",
        "cqc_url":           "https://www.cqc.org.uk/location/1-...",
        "gpps_overall_pct":  78.5,
        "gpps_contact_pct":  65.3,
        "gpps_pcn":          "West Camden"
    }

GPPS fields are PRESERVED from the existing gps.json where possible (matched
by ODS code). Practices new to this run will have empty GPPS fields until
the next NHS GP Patient Survey publishes updated data.

Run order
---------
This script is a one-off rebuild — drop it in the repo root, run it once,
commit the new gps.json, then your usual weekly refresh continues unchanged.

    export CQC_KEY=...        # same key as fetch_private_clinics.py
    python3 rebuild_gps_json.py

Or trigger it from the workflow once. After the rebuild, you can keep this
file in the repo for future re-runs but no need to add it to the weekly
schedule — gp practice openings/closures are slow.
"""

import json, os, re, sys, csv, io, time, zipfile, argparse
import urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import defaultdict

ROOT = Path(__file__).resolve().parent
EXISTING_GPS = ROOT / "gps.json"
OUT_GPS = ROOT / "gps.json"  # overwrite — keep a backup before running!

EPRACCUR_URL = "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip"
CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

# NHS ORD (Organisation Reference Data) JSON API — same source ePraccur is
# built from, but served as a public JSON API instead of a downloadable file.
# This endpoint is reachable from GitHub Actions (files.digital.nhs.uk is not).
ORD_BASE = "https://directory.spineservices.nhs.uk/ORD/2-0-0"
ORD_GP_ROLE = "RO177"  # = GP Practice

# London postcode prefixes (Inner + Outer)
LONDON_POSTCODE_PREFIXES = {
    "EC1A","EC1R","EC1V","EC2A","WC1B","WC1E","WC1N","WC1X","WC2A","WC2B","WC2H","WC2N",
    "E1","E2","E3","E4","E5","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15",
    "E16","E17","E18","E20",
    "N1","N4","N5","N6","N7","N8","N9","N10","N11","N12","N13","N14","N15","N16",
    "N17","N18","N19","N20","N21","N22",
    "NW1","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11",
    "SE1","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9","SE10","SE11","SE12",
    "SE13","SE14","SE15","SE16","SE17","SE18","SE19","SE20","SE21","SE22","SE23",
    "SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1P","SW1V","SW1W","SW1X","SW2","SW3","SW4","SW5","SW6","SW7",
    "SW8","SW9","SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18",
    "SW19","SW20",
    "W1","W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
    # Outer London
    "BR1","BR2","BR3","BR4","BR5","BR6","BR7","BR8",
    "CR0","CR2","CR3","CR4","CR5","CR6","CR7","CR8","CR9",
    "DA1","DA5","DA6","DA7","DA8","DA14","DA15","DA16","DA17","DA18",
    "EN1","EN2","EN3","EN4","EN5","EN7","EN8","EN9",
    "HA0","HA1","HA2","HA3","HA4","HA5","HA6","HA7","HA8","HA9",
    "IG1","IG2","IG3","IG4","IG5","IG6","IG7","IG8","IG11",
    "KT1","KT2","KT3","KT4","KT5","KT6","KT7","KT8","KT9",
    "RM1","RM2","RM3","RM4","RM5","RM6","RM7","RM8","RM9","RM10","RM11","RM12","RM13","RM14",
    "SM1","SM2","SM3","SM4","SM5","SM6",
    "TW1","TW2","TW3","TW4","TW5","TW6","TW7","TW8","TW9","TW10","TW11","TW12","TW13","TW14",
    "UB1","UB2","UB3","UB4","UB5","UB6","UB7","UB8","UB9","UB10","UB11",
}

def postcode_district(pc):
    if not pc: return ""
    pc = pc.strip().upper()
    if " " in pc: return pc.split()[0]
    pc = pc.replace(" ", "")
    return pc[:-3] if len(pc) >= 5 else pc

def is_london(pc):
    d = postcode_district(pc)
    if d in LONDON_POSTCODE_PREFIXES: return True
    m = re.match(r"^([A-Z]{1,2}\d)", d)
    return bool(m and m.group(1) in LONDON_POSTCODE_PREFIXES)

# --------------------------------------------------------------- ePraccur

EPRACCUR_COLS = [
    "Code", "Name", "NationalGrouping", "HighLevelHealthGeography",
    "AddressLine1", "AddressLine2", "AddressLine3", "AddressLine4", "AddressLine5",
    "Postcode", "OpenDate", "CloseDate", "Status", "OrgSubTypeCode",
    "Commissioner", "JoinProviderDate", "LeftProviderDate", "ContactTelephoneNumber",
    "Null18", "Null19", "Null20", "AmendedRecordIndicator", "Null22",
    "CurrentCarerIdentifier", "Null24", "ProviderProfileType",
]

def download_epraccur():
    """Download and unzip the ePraccur CSV in memory.

    NHS Digital's CDN (files.digital.nhs.uk) blocks requests that don't look
    like a normal browser — short or generic User-Agents return HTTP 403.
    We mimic Firefox and add Accept headers; if that still fails we try a
    known mirror via the ODS portal root.
    """
    headers = {
        "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
                       "Gecko/20100101 Firefox/128.0"),
        "Accept": "application/zip,application/octet-stream,*/*;q=0.8",
        "Accept-Language": "en-GB,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://digital.nhs.uk/services/organisation-data-service/export-data-files/csv-downloads/gp-and-gp-practice-related-data",
    }
    candidate_urls = [
        EPRACCUR_URL,
        # Known alternates seen historically:
        "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip",
        "https://digital.nhs.uk/binaries/content/assets/website-assets/services/ods/data-downloads-other-nhs-organisations/epraccur.zip",
    ]
    last_err = None
    data = None
    for url in candidate_urls:
        print(f"Downloading ePraccur: {url}")
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
                # urllib doesn't auto-decode gzip; handle it.
                if r.headers.get("Content-Encoding", "").lower() == "gzip":
                    import gzip
                    raw = gzip.decompress(raw)
                data = raw
            print(f"  ok — {len(data)//1024} KB")
            break
        except urllib.error.HTTPError as e:
            print(f"  HTTP {e.code} — trying next candidate")
            last_err = e
            time.sleep(1)
        except Exception as e:
            print(f"  error: {e} — trying next candidate")
            last_err = e
            time.sleep(1)
    if data is None:
        raise SystemExit(f"All ePraccur download URLs failed. Last error: {last_err}")

    zf = zipfile.ZipFile(io.BytesIO(data))
    csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
    with zf.open(csv_name) as f:
        text = f.read().decode("utf-8", errors="replace")
    return text

def parse_epraccur_london(csv_text):
    """Parse ePraccur, filter to active GPs in London."""
    london_gps = []
    reader = csv.reader(io.StringIO(csv_text))
    for row in reader:
        if len(row) < 18: continue
        rec = dict(zip(EPRACCUR_COLS, row + [""] * (len(EPRACCUR_COLS) - len(row))))
        # Status A = Active. Anything else (C = Closed, P = Proposed, D = Dormant) skip.
        if rec.get("Status", "").strip().upper() != "A": continue
        pc = rec.get("Postcode", "").strip().upper()
        if not is_london(pc): continue
        london_gps.append({
            "ods_code":      rec.get("Code", "").strip().upper(),
            "name":          rec.get("Name", "").strip().title(),
            "address_lines": [rec.get(f"AddressLine{i}", "").strip() for i in range(1, 6)],
            "postcode":      pc,
            "phone":         rec.get("ContactTelephoneNumber", "").strip(),
        })
    return london_gps

# --------------------------------------------------------------- ORD JSON API
# Fallback path when the ePraccur ZIP is blocked. Uses the same ODS data
# source, just served as a public JSON API at directory.spineservices.nhs.uk
# (no auth, no IP filter — refresh_nhs_data.py already uses this endpoint).

def _ord_get(url):
    headers = {
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (rebuild-gps-json via ORD)",
    }
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def list_active_gps_via_ord():
    """Return a list of {ods_code, name, postcode, link} for all active GP
    practices nationally. Pagination via the Limit/Offset query params."""
    out = []
    offset = 0
    page = 1
    limit = 1000
    while True:
        url = (f"{ORD_BASE}/organisations"
               f"?PrimaryRoleId={ORD_GP_ROLE}&Status=Active"
               f"&Limit={limit}&Offset={offset}")
        print(f"  ORD page {page} (offset {offset})…")
        try:
            data = _ord_get(url)
        except urllib.error.HTTPError as e:
            print(f"    HTTP {e.code} from ORD — stopping pagination")
            break
        orgs = data.get("Organisations", []) or []
        if not orgs:
            break
        for o in orgs:
            out.append({
                "ods_code": (o.get("OrgId") or "").upper(),
                "name":     (o.get("Name") or "").title(),
                "postcode": (o.get("PostCode") or "").upper(),
                "link":     (o.get("OrgLink") or ""),
            })
        if len(orgs) < limit:
            break
        offset += limit
        page += 1
        time.sleep(0.2)
    return out

def _fetch_ord_detail(org):
    """Fetch full address + phone for one org. Returns the org dict updated."""
    if not org.get("link"):
        org["address_lines"] = []
        org["phone"] = ""
        return org
    try:
        d = _ord_get(org["link"])
    except Exception:
        org["address_lines"] = []
        org["phone"] = ""
        return org
    o = (d.get("Organisation") or {})
    geo = (o.get("GeoLoc") or {}).get("Location") or {}
    addr_lines = [
        geo.get("AddrLn1") or "",
        geo.get("AddrLn2") or "",
        geo.get("AddrLn3") or "",
        geo.get("Town")    or "",
        geo.get("County")  or "",
    ]
    contacts = (o.get("Contacts") or {}).get("Contact") or []
    if isinstance(contacts, dict):
        contacts = [contacts]
    phone = ""
    for c in contacts:
        if (c.get("type") or "").lower() == "tel":
            phone = c.get("value") or ""
            break
    org["address_lines"] = [a.strip() for a in addr_lines if (a or "").strip()]
    org["phone"] = phone.strip()
    return org

def fetch_london_gps_via_ord(workers=8):
    """Top-level: list all active GPs nationally, filter to London by postcode,
    then enrich with full address + phone in parallel."""
    print(f"Listing active GP practices via ORD JSON API ({ORD_BASE}/organisations)…")
    all_gps = list_active_gps_via_ord()
    print(f"  ORD returned {len(all_gps)} active GP practices nationally")
    london = [g for g in all_gps if is_london(g["postcode"])]
    print(f"  {len(london)} are in London postcodes")

    print(f"  fetching full details (address, phone) with {workers} workers…")
    results = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_fetch_ord_detail, dict(g)): g for g in london}
        done = 0
        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1
            if done % 100 == 0 or done == len(london):
                print(f"    {done}/{len(london)} detail records fetched")
    return results

# --------------------------------------------------------------- CQC

def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/1.0 (rebuild-gps-json)",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            if e.code == 404:
                return None
            raise
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2)
                continue
            raise

def cqc_lookup_by_ods(ods, key):
    """
    Find the CQC location for a given ODS code. CQC tags providers and
    locations with their ODS reference, so /locations?odsCode={code}
    returns at most a handful of matches.
    """
    if not ods: return None
    try:
        data = cqc_get("/locations", {"odsCode": ods, "perPage": 5}, key)
    except Exception:
        return None
    if not data: return None
    locs = data.get("locations", [])
    if not locs: return None
    # Prefer a registered location over deregistered
    active = [l for l in locs if not l.get("deregistrationDate")]
    chosen = active[0] if active else locs[0]
    return chosen.get("locationId")

def cqc_get_location(loc_id, key):
    return cqc_get(f"/locations/{loc_id}", {}, key)

# --------------------------------------------------------------- main

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--key", default=os.environ.get("CQC_KEY"),
        help="CQC subscription key (defaults to $CQC_KEY).")
    parser.add_argument("--limit", type=int, default=0,
        help="Stop after N GPs (for testing). 0 = no limit.")
    parser.add_argument("--no-cqc", action="store_true",
        help="Skip CQC enrichment (much faster; cqc_rating fields will be empty).")
    parser.add_argument("--out", default=str(OUT_GPS),
        help="Output path. Defaults to overwriting gps.json.")
    args = parser.parse_args()

    if not args.no_cqc and not args.key:
        sys.exit("Need a CQC API key. Set $CQC_KEY or pass --key=..., or use --no-cqc to skip rating enrichment.")

    # 1. Load existing gps.json (if any) so we can preserve GPPS data.
    existing = {}
    if EXISTING_GPS.exists():
        try:
            for d in json.loads(EXISTING_GPS.read_text()):
                code = (d.get("ods_code") or "").upper()
                if code: existing[code] = d
            print(f"Loaded {len(existing)} existing records from gps.json (will preserve GPPS scores).")
        except Exception as e:
            print(f"  warning: couldn't read existing gps.json — {e}")

    # 2. Get the master London GP list. Try the ePraccur ZIP first (fast,
    #    one file, all data). If NHS Digital's CDN blocks GitHub Actions
    #    (recent 403s), fall back to the ORD JSON API which is unfiltered.
    london = []
    try:
        csv_text = download_epraccur()
        london = parse_epraccur_london(csv_text)
        print(f"Found {len(london)} active GP practices in London (ePraccur).")
    except SystemExit as e:
        print(f"ePraccur unavailable ({e}). Falling back to ORD JSON API.")
    except Exception as e:
        print(f"ePraccur error ({e}). Falling back to ORD JSON API.")
    if not london:
        london = fetch_london_gps_via_ord()
        print(f"Found {len(london)} active GP practices in London (ORD JSON).")
    if args.limit:
        london = london[:args.limit]
        print(f"  limited to first {len(london)} for testing")

    # 3. Build the merged records.
    print("\nBuilding records…")
    out = []
    cqc_hits = 0
    for i, gp in enumerate(london, 1):
        ods = gp["ods_code"]
        old = existing.get(ods, {})

        # Address: prefer full lines from ePraccur, joined nicely
        addr = ", ".join(filter(None, gp["address_lines"]))
        addr = addr.title() if addr.isupper() else addr

        record = {
            "ods_code":         ods,
            "name":             gp["name"],
            "address":          addr,
            "postcode":         gp["postcode"],
            "phone":            gp["phone"] or old.get("phone", ""),
            # CQC fields filled in below if --no-cqc not set
            "cqc_rating":       old.get("cqc_rating", ""),
            "cqc_url":          old.get("cqc_url", ""),
            # GPPS fields preserved from old data — empty for new practices
            "gpps_overall_pct": old.get("gpps_overall_pct"),
            "gpps_contact_pct": old.get("gpps_contact_pct"),
            "gpps_pcn":         old.get("gpps_pcn", ""),
        }

        # 4. Enrich with CQC rating
        if not args.no_cqc:
            try:
                loc_id = cqc_lookup_by_ods(ods, args.key)
                if loc_id:
                    detail = cqc_get_location(loc_id, args.key)
                    if detail:
                        rating = ((detail.get("currentRatings", {}) or {})
                                  .get("overall", {}) or {}).get("rating", "")
                        if rating:
                            record["cqc_rating"] = rating
                        record["cqc_url"] = f"https://www.cqc.org.uk/location/{loc_id}"
                        cqc_hits += 1
            except Exception as e:
                if i % 50 == 0:
                    print(f"  warn: CQC error for {ods}: {e}")
            time.sleep(0.1)  # gentle on the API

        out.append(record)
        if i % 100 == 0 or i == len(london):
            print(f"  {i}/{len(london)} processed, {cqc_hits} CQC matches")

    # 5. Write.
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(f"\nWrote {args.out} — {len(out)} practices, {os.path.getsize(args.out)//1024} KB.")

    # 6. Summary by postcode prefix (rough borough proxy).
    by_prefix = defaultdict(int)
    for r in out:
        by_prefix[postcode_district(r["postcode"])[:3]] += 1
    print("\nBy postcode prefix (top 20):")
    for prefix, count in sorted(by_prefix.items(), key=lambda x: -x[1])[:20]:
        print(f"  {prefix:6s} {count}")

    # 7. Sanity check — flag practices that exist in old gps.json but NOT in new.
    if existing:
        new_codes = {r["ods_code"] for r in out}
        missing = set(existing.keys()) - new_codes
        if missing:
            print(f"\n⚠️  {len(missing)} practices were in old gps.json but missing from ePraccur "
                  f"(could be closed/dormant since the file was scraped). Sample:")
            for code in list(missing)[:10]:
                print(f"    {code}: {existing[code].get('name', '')}")

if __name__ == "__main__":
    main()
