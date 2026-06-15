#!/usr/bin/env python3
"""
Backfill CQC ratings for NHS GP and private records.

What this does:
  1. Paginate CQC /locations to collect every London location summary
     (~16,000 records, filter by postcode prefix).
  2. Drop obvious non-primary-care names (dental/pharmacy/care home).
  3. Fetch detail for each remaining candidate in parallel (5 workers,
     rate-limit-aware with exponential backoff).
  4. Extract rating using the NEW 2024 assessment framework first
     (assessment[].ratings.asgRatings[]) then fall back to legacy paths.
  5. Build odsCode -> (rating, locationId) map + normalised-name fallback.
  6. Re-rate EVERY record with an ODS code in gps.json and merged.json.
     Never overwrites a real rating with empty.
"""
import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON    = ROOT / "gps.json"
MERGED_JSON = ROOT / "merged.json"
CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

WORKERS = 5

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
    "BR1","BR2","BR3","BR4","BR5","BR6","BR7","BR8","CR0","CR2","CR3","CR4","CR5","CR6","CR7","CR8","CR9",
    "DA1","DA5","DA6","DA7","DA8","DA14","DA15","DA16","DA17","DA18","EN1","EN2","EN3","EN4","EN5","EN7","EN8","EN9",
    "HA0","HA1","HA2","HA3","HA4","HA5","HA6","HA7","HA8","HA9","IG1","IG2","IG3","IG4","IG5","IG6","IG7","IG8","IG11",
    "KT1","KT2","KT3","KT4","KT5","KT6","KT7","KT8","KT9","RM1","RM2","RM3","RM4","RM5","RM6","RM7","RM8","RM9","RM10","RM11","RM12","RM13","RM14",
    "SM1","SM2","SM3","SM4","SM5","SM6","TW1","TW2","TW3","TW4","TW5","TW6","TW7","TW8","TW9","TW10","TW11","TW12","TW13","TW14",
    "UB1","UB2","UB3","UB4","UB5","UB6","UB7","UB8","UB9","UB10","UB11",
}

def postcode_district(pc):
    pc = (pc or "").strip().upper()
    if " " in pc: return pc.split()[0]
    return pc[:-3] if len(pc) >= 5 else pc

def is_london(pc):
    return postcode_district(pc) in LONDON_PREFIXES

HARD_DROP_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|pharmacy|chemist|"
    r"care home|residential home|nursing home|hospice|"
    r"veterinary|funeral|optician|optometr|chiropract|osteopath|"
    r"audiology|hearing test|sexual health clinic|tattoo|piercing)\b",
    re.IGNORECASE,
)

RATING_FIELDS = ["cqc_rating", "cqc"]
URL_FIELDS    = ["cqc_url", "cu"]
ODS_FIELDS    = ["ods_code", "o"]
NAME_FIELDS   = ["name", "n"]

def get_first(rec, fields):
    for f in fields:
        v = rec.get(f)
        if v not in (None, ""): return v
    return ""

def get_ods(rec):    return get_first(rec, ODS_FIELDS).strip().upper()
def get_rating(rec): return get_first(rec, RATING_FIELDS)
def get_name(rec):   return get_first(rec, NAME_FIELDS) or ""

def set_rating(rec, rating, url):
    has_snake = "cqc_rating" in rec or "cqc_url" in rec
    has_short = "cqc" in rec or "cu" in rec
    if has_snake or not has_short:
        rec["cqc_rating"] = rating
        rec["cqc_url"]    = url
    if has_short:
        rec["cqc"] = rating
        rec["cu"]  = url

def cqc_get(path, params, key, retries=4):
    url = f"{CQC_BASE}{path}?{urllib.parse.urlencode(params)}" if params else f"{CQC_BASE}{path}"
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/enrich/2.0",
    }
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries - 1:
                wait = min(10 * (2 ** attempt), 240)
                sys.stderr.write(f"    [{e.code} sleep {wait}s]\n"); sys.stderr.flush()
                time.sleep(wait); continue
            if e.code == 404: return None
            raise
        except Exception:
            if attempt < retries - 1:
                time.sleep(3); continue
            raise
    return None

def paginate_london_candidates(key):
    print("Paginating CQC /locations for London candidates...")
    candidates = []
    page = 1
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": 1000}, key)
        if not data: break
        for loc in (data.get("locations") or []):
            if loc.get("deregistrationDate"): continue
            if not is_london(loc.get("postalCode") or ""): continue
            name = loc.get("locationName") or loc.get("name") or ""
            if HARD_DROP_RE.search(name): continue
            lid = loc.get("locationId", "")
            if lid: candidates.append(lid)
        tp = data.get("totalPages", 1)
        if page % 10 == 0:
            print(f"  page {page}/{tp} - {len(candidates)} candidates")
        if page >= tp: break
        page += 1
        time.sleep(0.2)
    print(f"  Total: {len(candidates)} candidates for detail fetch.\n")
    return candidates

VALID_RATINGS = {"Outstanding", "Good", "Requires improvement", "Inadequate"}

def clean_rating(s):
    if not s: return ""
    s = s.strip()
    for v in VALID_RATINGS:
        if s.lower() == v.lower(): return v
    return ""

def extract_rating(d):
    """Prefer NEW 2024 assessment framework over legacy currentRatings."""
    # 1. NEW: assessment[].ratings.asgRatings[] with Active status
    best = None
    for a in (d.get("assessment", []) or []):
        if not isinstance(a, dict): continue
        for entry in (((a.get("ratings", {}) or {}).get("asgRatings", [])) or []):
            if not isinstance(entry, dict): continue
            if entry.get("assessmentPlanStatus") != "Active": continue
            r = clean_rating(entry.get("rating", ""))
            if not r: continue
            date = entry.get("assessmentDate") or ""
            if best is None or date > best[0]:
                best = (date, r)
    if best: return best[1]

    # 2. Legacy: currentRatings.overall.rating
    r = clean_rating(((d.get("currentRatings", {}) or {}).get("overall", {}) or {}).get("rating", ""))
    if r: return r

    # 3. Legacy: historicRatings[0].overall.rating
    for h in (d.get("historicRatings", []) or []):
        r = clean_rating(((h.get("overall", {}) or {}).get("rating", "")))
        if r: return r
    return ""

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

def fetch_detail(lid, key):
    d = cqc_get(f"/locations/{lid}", None, key)
    if not d: return ("", "", "")
    ods = (d.get("odsCode") or "").strip().upper()
    name = d.get("locationName") or d.get("name") or ""
    return (ods, name, extract_rating(d))

def build_maps(candidates, key):
    print(f"Fetching detail ({WORKERS} workers, rate-limit-aware)...")
    ods_map = {}
    name_map = {}
    done = 0
    def worker(lid):
        try: return (lid, fetch_detail(lid, key))
        except Exception as e: return (lid, ("", "", ""))
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker, lid): lid for lid in candidates}
        for fut in as_completed(futures):
            lid, (ods, name, rating) = fut.result()
            done += 1
            if ods and ods not in ods_map:
                ods_map[ods] = (rating, lid)
            if rating:
                norm = normalize_name(name)
                if len(norm) >= 4 and norm not in name_map:
                    name_map[norm] = (rating, lid)
            if done % 500 == 0:
                print(f"  {done}/{len(candidates)} - {len(ods_map)} ODS, {len(name_map)} names")
    print(f"Done. {len(ods_map)} ODS codes, {len(name_map)} name-indexed records.\n")
    return ods_map, name_map

def enrich_file(path, ods_map, name_map):
    if not path.exists(): return Counter()
    data = json.loads(path.read_text())
    if not isinstance(data, list): return Counter()

    needs = [i for i, r in enumerate(data) if get_ods(r)]
    print(f"\n  {path.name}: {len(data)} records, {len(needs)} eligible for re-rating")

    status = Counter()
    changed = 0
    for i in needs:
        rec = data[i]
        existing = get_rating(rec)
        ods = get_ods(rec)
        entry = ods_map.get(ods)
        if not entry:
            norm = normalize_name(get_name(rec))
            if norm and norm in name_map: entry = name_map[norm]
        if entry:
            rating, lid = entry
            url = f"https://www.cqc.org.uk/location/{lid}" if lid else ""
            if rating:
                if rating != existing:
                    set_rating(rec, rating, url)
                    changed += 1
                status[rating] += 1
            elif not existing and url:
                set_rating(rec, "", url)
                status["(unrated)"] += 1
            else:
                status["(kept existing)"] += 1
        else:
            status["(no-cqc-record)" if not existing else "(kept existing)"] += 1

    path.write_text(json.dumps(data, indent=2))
    print(f"\n  {path.name} - final distribution:")
    for r, n in status.most_common():
        print(f"    {r:25s} {n}")
    print(f"  Changed: {changed}")

def main():
    key = os.environ.get("CQC_KEY")
    if not key: sys.exit("Need CQC_KEY env var.")
    candidates = paginate_london_candidates(key)
    ods_map, name_map = build_maps(candidates, key)
    for path in [GPS_JSON, MERGED_JSON]:
        enrich_file(path, ods_map, name_map)

if __name__ == "__main__":
    main()
