#!/usr/bin/env python3
"""
Build a local cache of every London CQC location with full classification data.

Pipeline:
  1. Paginate /locations summaries (3 min, ~120 pages)
  2. Filter to active London by postcode prefix (~16k)
  3. Drop obvious non-medical from summary names (dental/pharmacy/care home/etc)
     so we don't waste API calls on them (~12k remain)
  4. Skip locations already in cache (resume capability)
  5. Fetch detail in parallel (5 workers, generous 429 backoff)
  6. Extract a SLIM record per location with just what we need
  7. Save gzipped JSON cache after every 500 records

Resumable: If interrupted, re-run; it picks up from where it stopped.

Output: cqc_london_cache.json.gz (~5MB for ~12k records)
"""

import gzip, json, os, re, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "cqc_london_cache.json.gz"
CACHE_META = ROOT / "cqc_london_cache.meta.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

# Workers: CQC's quota tolerates ~5-8 req/sec across the API.
# 5 workers with built-in backoff = sustainable throughput.
WORKERS = 5
CHECKPOINT_EVERY = 500

# London postcode prefixes (Inner + Outer Greater London)
LONDON_PREFIXES = {
    "EC1A","EC1M","EC1N","EC1P","EC1R","EC1V","EC1Y",
    "EC2A","EC2M","EC2N","EC2P","EC2R","EC2V","EC2Y",
    "EC3A","EC3M","EC3N","EC3P","EC3R","EC3V",
    "EC4A","EC4M","EC4N","EC4P","EC4R","EC4V","EC4Y",
    "WC1A","WC1B","WC1E","WC1H","WC1N","WC1R","WC1V","WC1X",
    "WC2A","WC2B","WC2E","WC2H","WC2N","WC2R",
    "E1","E1W","E2","E3","E4","E5","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15","E16","E17","E18","E20",
    "N1","N1C","N1P","N4","N5","N6","N7","N8","N9","N10","N11","N12","N13","N14","N15","N16","N17","N18","N19","N20","N21","N22",
    "NW1","NW1W","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11","NW26",
    "SE1","SE1P","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9","SE10","SE11","SE12","SE13","SE14","SE15","SE16","SE17","SE18","SE19","SE20","SE21","SE22","SE23","SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1H","SW1P","SW1V","SW1W","SW1X","SW1Y","SW2","SW3","SW4","SW5","SW6","SW7","SW8","SW9","SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18","SW19","SW20",
    "W1","W1A","W1B","W1C","W1D","W1F","W1G","W1H","W1J","W1K","W1S","W1T","W1U","W1W","W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
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

# Drop these from the summary stage — clearly not medical practices/clinics.
# (Pharmacies, dental, care homes, vets etc. We're a GP/clinic directory,
# not a comprehensive health register.)
SUMMARY_DROP_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|denture|"
    r"smile\s+(?:clinic|studio|centre|practice)|"
    r"pharmacy|chemist|drugstore|"
    r"care\s+home|nursing\s+home|residential\s+home|"
    r"hospice|tattoo|piercing|funeral|veterinary|"
    r"opticians?|optometr|eye\s+(?:wear|laser)|"
    r"podiatr|chiropod)\b",
    re.IGNORECASE,
)

def is_london(pc):
    pc = (pc or "").strip().upper()
    district = pc.split()[0] if " " in pc else (pc[:-3] if len(pc) >= 5 else pc)
    return district in LONDON_PREFIXES

# ----------------------------------------------------------------------- HTTP

def cqc_get(path, max_attempts=6):
    key = os.environ["CQC_KEY"]
    req = urllib.request.Request(f"{CQC_BASE}{path}", headers={
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/cache-builder/1.0",
    })
    last_error = None
    for attempt in range(max_attempts):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            last_error = e
            if e.code == 404:
                return None
            if e.code in (429, 503) and attempt < max_attempts - 1:
                wait = min(10 * (2 ** attempt), 300)  # 10, 20, 40, 80, 160, 300 max
                sys.stderr.write(f"      [{e.code} backoff {wait}s]\n")
                sys.stderr.flush()
                time.sleep(wait)
                continue
            if attempt < max_attempts - 1:
                time.sleep(3); continue
            raise
        except Exception as e:
            last_error = e
            if attempt < max_attempts - 1:
                time.sleep(3); continue
            raise
    if last_error: raise last_error
    return None

# ----------------------------------------------------------------------- slim

def slim(loc, detail):
    """Extract ONLY the fields we need from the detail response.
    Avoids bloating the cache file."""
    if not detail: return None

    # Service types as a flat list of names + a flag for Independent
    gac = detail.get("gacServiceTypes", []) or []
    gac_names = [
        (s.get("name", "") if isinstance(s, dict) else str(s)).strip()
        for s in gac
    ]
    is_independent = any("Independent" in n for n in gac_names)
    is_nhs_service = any(
        n.startswith(("Doctors consultation service", "Doctors treatment service",
                      "Hospital services for people", "Community", "Urgent care"))
        and "Independent" not in n
        for n in gac_names
    )

    # Specialisms (clinical specialties)
    specs = detail.get("specialisms", []) or []
    spec_names = [
        (s.get("name", "") if isinstance(s, dict) else str(s)).strip()
        for s in specs
    ]

    # Regulated activities
    acts = detail.get("regulatedActivities", []) or []
    act_names = [
        (a.get("name", "") if isinstance(a, dict) else str(a)).strip()
        for a in acts
    ]

    # Rating — try NEW assessment framework first, then legacy
    rating = ""
    assessment = detail.get("assessment", []) or []
    if isinstance(assessment, list):
        best_date = ""
        for a in assessment:
            if not isinstance(a, dict): continue
            asg = (((a.get("ratings", {}) or {}).get("asgRatings", [])) or [])
            for entry in asg:
                if not isinstance(entry, dict): continue
                if entry.get("assessmentPlanStatus") != "Active": continue
                r = (entry.get("rating") or "").strip()
                d = entry.get("assessmentDate") or ""
                if r in {"Outstanding","Good","Requires improvement","Inadequate"} and d > best_date:
                    rating = r
                    best_date = d
    if not rating:
        cur = ((detail.get("currentRatings", {}) or {}).get("overall", {}) or {}).get("rating", "")
        if cur in {"Outstanding","Good","Requires improvement","Inadequate"}:
            rating = cur
    if not rating:
        for h in (detail.get("historicRatings", []) or []):
            cand = ((h.get("overall", {}) or {}).get("rating", ""))
            if cand in {"Outstanding","Good","Requires improvement","Inadequate"}:
                rating = cand
                break

    return {
        "locationId": detail.get("locationId", loc.get("locationId", "")),
        "name": (loc.get("locationName") or detail.get("name") or "").strip(),
        "odsCode": (detail.get("odsCode") or "").strip().upper(),
        "providerId": detail.get("providerId", ""),
        "providerName": detail.get("providerName", ""),
        "postcode": (loc.get("postalCode") or detail.get("postalCode") or "").strip(),
        "address1": detail.get("postalAddressLine1", ""),
        "address2": detail.get("postalAddressLine2", ""),
        "town": detail.get("postalAddressTownCity", ""),
        "county": detail.get("postalAddressCounty", ""),
        "localAuthority": detail.get("localAuthority", ""),
        "region": detail.get("region", ""),
        "phone": detail.get("mainPhoneNumber", ""),
        "website": detail.get("website", ""),
        "lat": detail.get("onspdLatitude"),
        "lon": detail.get("onspdLongitude"),
        "gacServiceTypes": gac_names,
        "specialisms": spec_names,
        "regulatedActivities": act_names,
        "isIndependent": is_independent,
        "hasNhsService": is_nhs_service,
        "currentRating": rating,
        "registrationStatus": detail.get("registrationStatus", ""),
        "registrationDate": detail.get("registrationDate", ""),
        "cqcUrl": f"https://www.cqc.org.uk/location/{detail.get('locationId', loc.get('locationId', ''))}",
    }

# ----------------------------------------------------------------------- cache I/O

def load_cache():
    if not CACHE_FILE.exists():
        return {}
    with gzip.open(CACHE_FILE, "rt", encoding="utf-8") as f:
        return json.load(f)

def save_cache(cache):
    tmp = CACHE_FILE.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(cache, f, separators=(",", ":"))
    tmp.replace(CACHE_FILE)
    CACHE_META.write_text(json.dumps({
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "record_count": len(cache),
    }, indent=2))

# ----------------------------------------------------------------------- main

def main():
    if not os.environ.get("CQC_KEY"):
        sys.exit("Need CQC_KEY env var.")

    cache = load_cache()
    print(f"Loaded existing cache: {len(cache):,} records")

    # Pass 1: paginate summaries
    print("\nPaginating CQC /locations for London summaries...")
    summaries = []
    page = 1
    while True:
        data = cqc_get(f"/locations?page={page}&perPage=1000")
        if not data: break
        for loc in (data.get("locations") or []):
            if loc.get("deregistrationDate"): continue
            if not is_london(loc.get("postalCode")): continue
            if not loc.get("locationId"): continue
            summaries.append(loc)
        if page % 10 == 0:
            print(f"  page {page}/{data.get('totalPages',1)} - {len(summaries):,} London active")
        if page >= data.get("totalPages", 1): break
        page += 1
        time.sleep(0.2)
    print(f"  TOTAL: {len(summaries):,} active London locations\n")

    # Pre-filter: drop obvious non-medical from summary names
    keep = []
    dropped_summary = 0
    for loc in summaries:
        nm = (loc.get("locationName") or "").lower()
        if SUMMARY_DROP_RE.search(nm):
            dropped_summary += 1
            continue
        keep.append(loc)
    print(f"Pre-filtered {dropped_summary:,} obvious non-medical from summary stage.")
    print(f"  {len(keep):,} candidates for detail fetch.\n")

    # Resume: skip locations already in cache
    todo = [loc for loc in keep if loc["locationId"] not in cache]
    print(f"Cache already has {len(keep) - len(todo):,} of these.")
    print(f"  {len(todo):,} new locations need detail fetched.\n")

    if not todo:
        print("Cache fully up to date. Done.")
        save_cache(cache)
        return

    # Pass 2: parallel detail fetch
    print(f"Fetching detail ({WORKERS} workers, 429-aware backoff)...")
    print(f"  Estimated time: {len(todo) // (WORKERS * 4):.0f}-{len(todo) // (WORKERS * 2):.0f} min\n")

    done = 0
    errors = 0
    start = time.time()

    def worker(loc):
        try:
            d = cqc_get(f"/locations/{loc['locationId']}")
            return (loc, d)
        except Exception as e:
            return (loc, {"__error__": str(e)})

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker, loc): loc for loc in todo}
        try:
            for fut in as_completed(futures):
                loc, detail = fut.result()
                done += 1
                if detail and "__error__" not in detail:
                    rec = slim(loc, detail)
                    if rec:
                        cache[loc["locationId"]] = rec
                else:
                    errors += 1

                if done % 100 == 0:
                    elapsed = time.time() - start
                    rate = done / elapsed if elapsed > 0 else 0
                    eta = (len(todo) - done) / rate if rate > 0 else 0
                    print(f"  {done:5d}/{len(todo)} ({100*done/len(todo):.1f}%) "
                          f"rate={rate:.1f}/s eta={eta/60:.0f}min "
                          f"errors={errors}")

                if done % CHECKPOINT_EVERY == 0:
                    save_cache(cache)
                    print(f"    [checkpoint saved at {done}]")
        except KeyboardInterrupt:
            print("\nInterrupted — saving partial cache...")
            save_cache(cache)
            print(f"  {len(cache):,} records saved. Re-run to resume.")
            return

    save_cache(cache)
    print(f"\n{'='*60}")
    print(f"Done. Cache: {len(cache):,} records")
    print(f"Errors: {errors}")
    print(f"File: {CACHE_FILE} ({CACHE_FILE.stat().st_size / 1024 / 1024:.1f} MB)")
    print(f"Time: {(time.time() - start) / 60:.1f} min")

if __name__ == "__main__":
    main()
