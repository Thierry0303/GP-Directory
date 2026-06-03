#!/usr/bin/env python3
"""
Backfill CQC ratings for every record where it's empty.

How it works (corrected approach)
---------------------------------
The CQC public API's /locations SUMMARY response only contains
locationId, locationName and postalCode. The odsCode is ONLY in the
DETAIL response (/locations/{id}). So we can't match by ODS code at the
summary stage.

What we do:
  1. Paginate CQC /locations to get every London summary record
     (filter by postcode prefix — ~16k records).
  2. Drop the obvious non-primary-care names (dental/pharmacy/etc.) so
     we don't waste detail fetches.
  3. Fetch detail for each remaining candidate in parallel (~2k-5k
     detail fetches at 15 workers = ~5-8 minutes).
  4. From each detail, extract odsCode + overall rating.
  5. Build {odsCode: (rating, locationId)} map.
  6. Apply to every record in gps.json + merged.json that has an empty
     rating, also populating cqc_url.
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON    = ROOT / "gps.json"
MERGED_JSON = ROOT / "merged.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

# All London postcode prefixes (Inner + Outer Greater London).
LONDON_PREFIXES = {
    "EC1A","EC1M","EC1N","EC1P","EC1R","EC1V","EC1Y",
    "EC2A","EC2M","EC2N","EC2P","EC2R","EC2V","EC2Y",
    "EC3A","EC3M","EC3N","EC3P","EC3R","EC3V",
    "EC4A","EC4M","EC4N","EC4P","EC4R","EC4V","EC4Y",
    "WC1A","WC1B","WC1E","WC1H","WC1N","WC1R","WC1V","WC1X",
    "WC2A","WC2B","WC2E","WC2H","WC2N","WC2R",
    "E1","E1W","E2","E3","E4","E5","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15",
    "E16","E17","E18","E20",
    "N1","N1C","N1P","N4","N5","N6","N7","N8","N9","N10","N11","N12","N13","N14","N15","N16",
    "N17","N18","N19","N20","N21","N22",
    "NW1","NW1W","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11","NW26",
    "SE1","SE1P","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9","SE10","SE11","SE12",
    "SE13","SE14","SE15","SE16","SE17","SE18","SE19","SE20","SE21","SE22","SE23",
    "SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1H","SW1P","SW1V","SW1W","SW1X","SW1Y",
    "SW2","SW3","SW4","SW5","SW6","SW7","SW8","SW9","SW10","SW11","SW12","SW13","SW14",
    "SW15","SW16","SW17","SW18","SW19","SW20",
    "W1","W1A","W1B","W1C","W1D","W1F","W1G","W1H","W1J","W1K","W1S","W1T","W1U","W1W",
    "W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
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
    pc = (pc or "").strip().upper()
    if " " in pc: return pc.split()[0]
    return pc[:-3] if len(pc) >= 5 else pc

def is_london(pc):
    return postcode_district(pc) in LONDON_PREFIXES

# Drop summary records whose name is obviously non-primary-care to avoid
# wasting detail fetches on them.
HARD_DROP_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|pharmacy|chemist|"
    r"care home|residential home|nursing home|hospice|"
    r"veterinary|funeral|optician|optometr|"
    r"chiropract|osteopath|reflexolog|"
    r"audiology|hearing test|sexual health clinic|"
    r"tattoo|piercing)\b",
    re.IGNORECASE,
)

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
    """Update both naming conventions if either is present."""
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

def paginate_london_candidates(key):
    """Pass 1: collect every London CQC location summary, dropping
    obvious non-primary-care names."""
    print("Paginating CQC /locations to collect London candidates…")
    page = 1
    per_page = 1000
    candidates = []
    total = 0
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        if not data: break
        items = data.get("locations", []) or []
        if not items: break
        total += len(items)
        for loc in items:
            if loc.get("deregistrationDate"): continue
            pc = loc.get("postalCode") or ""
            if not is_london(pc): continue
            name = loc.get("locationName") or loc.get("name") or ""
            if HARD_DROP_RE.search(name): continue
            loc_id = loc.get("locationId", "")
            if loc_id:
                candidates.append(loc_id)
        total_pages = data.get("totalPages", 1)
        if page % 10 == 0 or page >= total_pages:
            print(f"  page {page}/{total_pages} — total UK seen: {total}, "
                  f"London candidates: {len(candidates)}")
        if page >= total_pages: break
        page += 1
        time.sleep(0.15)
    print(f"\n{len(candidates)} London candidates worth a detail fetch.\n")
    return candidates

# The ONLY four values CQC issues as actual ratings. Status strings like
# "Inspected but not rated", "No published rating", "Not yet inspected" are
# NOT ratings — they're metadata flags that should be treated as empty.
VALID_RATINGS = {"Outstanding", "Good", "Requires improvement", "Inadequate"}

def clean_rating(s):
    """Return the string only if it's an actual CQC rating; else empty."""
    if not s: return ""
    s = s.strip()
    # CQC sometimes uses lowercase or different capitalisation
    for valid in VALID_RATINGS:
        if s.lower() == valid.lower():
            return valid
    return ""

def extract_rating(d):
    """Pick the best rating, preferring CQC's NEW 2024+ assessment
    framework (assessment[].ratings.asgRatings[]) over the legacy
    inspection framework (currentRatings.overall.rating).

    Why this order: CQC migrated to "assessments" in 2024. For practices
    that have been assessed under the new framework, the legacy
    currentRatings.overall block is EMPTY, while currentRatings.serviceRatings
    still holds the *previous* (now superseded) rating. So reading from
    currentRatings first would either return nothing or return a stale
    rating. The authoritative current rating lives in
    assessment[].ratings.asgRatings[] where assessmentPlanStatus == "Active".
    """
    # 1. NEW FRAMEWORK: assessment[].ratings.asgRatings[]
    #    Pick the most recent Active asgRating.
    assessments = d.get("assessment", []) or []
    best_new = None  # (date, rating)
    if isinstance(assessments, list):
        for a in assessments:
            if not isinstance(a, dict): continue
            asg = (((a.get("ratings", {}) or {})
                    .get("asgRatings", [])) or [])
            for entry in asg:
                if not isinstance(entry, dict): continue
                if entry.get("assessmentPlanStatus") != "Active":
                    continue
                r = clean_rating(entry.get("rating", ""))
                if not r: continue
                date = entry.get("assessmentDate") or ""
                if best_new is None or date > best_new[0]:
                    best_new = (date, r)
    if best_new:
        return best_new[1]

    # 2. LEGACY: currentRatings.overall.rating
    cur = ((d.get("currentRatings", {}) or {})
           .get("overall", {}) or {}).get("rating", "")
    r = clean_rating(cur)
    if r: return r

    # 3. LEGACY: lastInspection / latestInspection
    for k in ("lastInspection", "latestInspection"):
        last = d.get(k, {}) or {}
        if isinstance(last, dict):
            cand = (last.get("overall", {}) or {}).get("rating", "") \
                or last.get("rating", "")
            r = clean_rating(cand)
            if r: return r

    # 4. LEGACY: historicRatings[0].overall.rating
    historic = d.get("historicRatings", []) or []
    if isinstance(historic, list):
        for h in historic:
            cand = ((h.get("overall", {}) or {}).get("rating", ""))
            r = clean_rating(cand)
            if r: return r

    # 5. Last resort: top-level overallRating
    r = clean_rating(d.get("overallRating", ""))
    return r or ""

# Normalize a practice name so "Bridgestock Surgery" and "Bridgestock Road
# Surgery" both compare to the same core token set. Used as a fallback
# when the odsCode doesn't match (CQC's odsCode for a location can lag
# behind NHS Digital's).
NAME_STOP_RE = re.compile(
    r"\b(?:the|surgery|surgeries|practice|practices|medical|centre|center|"
    r"health|healthcare|clinic|partnership|gp|drs?|family|community|hub|"
    r"primary|care|nhs|ltd|limited|services?)\b",
    re.IGNORECASE,
)
def normalize_name(s):
    s = (s or "").lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = NAME_STOP_RE.sub(" ", s)
    return " ".join(s.split())

def fetch_detail_for_rating(loc_id, key):
    """Return (odsCode, locationName, rating) for one location."""
    d = cqc_get(f"/locations/{loc_id}", None, key)
    if not d: return ("", "", "")
    ods = (d.get("odsCode") or "").strip().upper()
    name = d.get("locationName") or d.get("name") or ""
    rating = extract_rating(d)
    return (ods, name, rating)

def build_ods_to_rating_map(candidates, key, wanted_ods, workers=15):
    """Pass 2: fetch detail for every London candidate in parallel.
    Returns two indices:
      ods_map  = {odsCode: (rating, loc_id)} for codes in wanted_ods
      name_map = {normalized_name: (rating, loc_id)} — ALL London locations,
                 used as fallback when ODS doesn't match.
    """
    print(f"Fetching CQC detail for {len(candidates)} candidates "
          f"({workers} workers)…")
    ods_map = {}
    name_map = {}
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch_detail_for_rating, lid, key): lid
                   for lid in candidates}
        try:
            for fut in as_completed(futures):
                lid = futures[fut]
                done += 1
                try:
                    ods, name, rating = fut.result()
                except Exception:
                    ods, name, rating = ("", "", "")
                # ODS-based index — only for wanted codes
                if ods and ods in wanted_ods and ods not in ods_map:
                    ods_map[ods] = (rating, lid)
                # Name-based index — for ALL locations with a rating.
                # Skip if normalized name is too short to be unique.
                norm = normalize_name(name)
                if rating and len(norm) >= 4 and norm not in name_map:
                    name_map[norm] = (rating, lid)
                if done % 250 == 0 or done == len(candidates):
                    print(f"  {done}/{len(candidates)} — "
                          f"{len(ods_map)}/{len(wanted_ods)} ODS resolved, "
                          f"{len(name_map)} names indexed")
        except KeyboardInterrupt:
            print("\nInterrupted.")
    return ods_map, name_map

# ---------------------------------------------------------------- main

NAME_RECORD_FIELDS = ["name", "n"]
def get_name(rec): return get_first(rec, NAME_RECORD_FIELDS) or ""

def enrich_file(path, ods_map, name_map):
    """Re-rate EVERY record with an ODS code (not just empty ones).
    Otherwise a stale rating (e.g. from a previous run that read the
    wrong field) is never refreshed. Safety: if CQC doesn't return a
    rating, we leave the existing one in place rather than blanking it."""
    if not path.exists(): return Counter()
    data = json.loads(path.read_text())
    if not isinstance(data, list): return Counter()

    needs = [i for i, r in enumerate(data) if get_ods(r)]
    print(f"\n  {path.name}: {len(data)} records, {len(needs)} eligible "
          "for re-rating")
    if not needs:
        return Counter()

    status = Counter()
    name_hits = 0
    changed = 0
    unchanged = 0
    for i in needs:
        rec = data[i]
        existing = get_rating(rec)
        ods = get_ods(rec)
        entry = ods_map.get(ods)

        # Fallback: try matching by normalized name
        if not entry:
            norm = normalize_name(get_name(rec))
            if norm and norm in name_map:
                entry = name_map[norm]
                name_hits += 1

        if entry:
            rating, loc_id = entry
            url = f"https://www.cqc.org.uk/location/{loc_id}" if loc_id else ""
            if rating:
                # Have a fresh CQC rating — apply it (this is what fixes
                # records that had stale ratings from a previous run)
                if rating != existing:
                    set_rating(rec, rating, url)
                    changed += 1
                else:
                    unchanged += 1
                status[rating] += 1
            elif not existing and url:
                # No rating in CQC, no existing rating — record the URL
                set_rating(rec, "", url)
                status["(unrated)"] += 1
            else:
                # No rating in CQC but we already have one — KEEP IT.
                # Don't blank out a real rating just because CQC's
                # detail response was empty this run.
                status["(kept existing)"] += 1
        else:
            if not existing:
                status["(no-cqc-record)"] += 1
            else:
                status["(kept existing)"] += 1

    path.write_text(json.dumps(data, indent=2))
    print(f"\n  {path.name} — final rating distribution:")
    for r, n in status.most_common():
        print(f"    {r:25s} {n}")
    print(f"  Changed: {changed}  Unchanged: {unchanged}")
    if name_hits:
        print(f"  ({name_hits} records resolved via name-fallback after "
              "ODS mismatch)")
    return status

def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    # Collect EVERY ODS code across both files (not just empty-rating).
    # We always re-rate everything so stale ratings from previous runs
    # get refreshed against the latest CQC data.
    wanted = set()
    for path in [GPS_JSON, MERGED_JSON]:
        if not path.exists(): continue
        for r in json.loads(path.read_text()):
            ods = get_ods(r)
            if ods: wanted.add(ods)

    if not wanted:
        print("Nothing to do — no records have an ODS code.")
        return
    print(f"Will look up ratings for {len(wanted)} unique ODS codes "
          "(re-rating ALL records so stale entries get refreshed).\n")

    candidates = paginate_london_candidates(key)
    ods_map, name_map = build_ods_to_rating_map(candidates, key, wanted)

    print(f"\nResolved {len(ods_map)}/{len(wanted)} ODS codes "
          f"({100*len(ods_map)/max(1,len(wanted)):.1f}%) via ODS match.")
    print(f"Built name index covering {len(name_map)} London CQC locations "
          "(used as fallback when ODS doesn't match).")
    missing = wanted - set(ods_map)
    if missing:
        print(f"({len(missing)} ODS codes had no ODS match — name-fallback "
              "will catch some; rest get dropped by drop_non_gms.py.)\n")

    for path in [GPS_JSON, MERGED_JSON]:
        enrich_file(path, ods_map, name_map)

if __name__ == "__main__":
    main()
