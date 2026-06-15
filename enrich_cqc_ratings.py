#!/usr/bin/env python3
"""Survey CQC London with rate-limit-safe parallelism + summary pre-filter."""
import csv, json, os, re, ssl, sys, time
import urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"
OUT_CSV = Path("london_private_survey.csv")

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
    "SW1A","SW1E","SW1H","SW1P","SW1V","SW1W","SW1X","SW1Y",
    "SW2","SW3","SW4","SW5","SW6","SW7","SW8","SW9","SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18","SW19","SW20",
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

def is_london(pc):
    pc = (pc or "").strip().upper()
    district = pc.split()[0] if " " in pc else (pc[:-3] if len(pc) >= 5 else pc)
    return district in LONDON_PREFIXES

def cqc_get(path):
    key = os.environ["CQC_KEY"]
    req = urllib.request.Request(f"{CQC_BASE}{path}", headers={
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/survey/2.0",
    })
    for attempt in range(6):
        try:
            with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 5:
                wait = 10 * (2 ** attempt)
                print(f"    [rate-limited, sleeping {wait}s]", file=sys.stderr)
                time.sleep(wait); continue
            if e.code == 404: return None
            raise
        except Exception:
            if attempt < 5:
                time.sleep(3); continue
            raise
    return None

# Summary-only filter for obvious non-medical (avoids detail fetches)
SUMMARY_DROP_RE = re.compile(
    r"\b(?:dental|dentist|orthodont|smile\s+(?:clinic|studio|centre)|"
    r"pharmacy|chemist|care\s+home|nursing\s+home|residential\s+home|"
    r"hospice|tattoo|piercing|funeral|veterinary)\b",
    re.IGNORECASE,
)

GP_NAME_RE = re.compile(r"\b(?:surgery|practice|medical\s+centre|health\s+centre|\bgp\b|the\s+practice|family\s+(?:practice|doctor))\b", re.IGNORECASE)
HOSPITAL_RE = re.compile(r"\bhospital\b", re.IGNORECASE)
DIAGNOSTIC_RE = re.compile(r"\b(?:diagnostic|imaging|radiology|scan|x-ray|mri)\b", re.IGNORECASE)
AESTHETIC_RE = re.compile(r"\b(?:aesthet|botox|laser\s+hair|cosmetic|slimming)\b", re.IGNORECASE)

SPECIALTY_PATTERNS = [
    ("psychiatry",  r"\b(?:psychiatr|psycholog|mental\s+health|counsell|therap)\b"),
    ("cardiology",  r"\b(?:cardio|heart\s+clinic)\b"),
    ("dermatology", r"\b(?:dermatolog|skin\s+clinic)\b"),
    ("gynaecology", r"\b(?:gynae|obstet|fertility|womens?\s+health)\b"),
    ("paediatrics", r"\b(?:paediatr|children|child\s+health)\b"),
    ("orthopaedic", r"\b(?:orthopaed|musculoskelet|sports\s+med)\b"),
    ("urology", r"\b(?:urology|urologist)\b"),
    ("ent", r"\b(?:\bent\b|ear,?\s+nose|otolaryng)\b"),
    ("ophthalmology", r"\b(?:ophthalm|eye\s+clinic)\b"),
    ("gastroenterology", r"\b(?:gastroenterolog|endoscop|liver\s+clinic)\b"),
    ("oncology", r"\b(?:oncolog|cancer\s+(?:clinic|centre))\b"),
    ("rheumatology", r"\b(?:rheumatolog)\b"),
    ("endocrinology", r"\b(?:endocrinolog|diabetes\s+clinic)\b"),
    ("respiratory", r"\b(?:respirator|lung\s+clinic|chest\s+clinic|sleep\s+clinic)\b"),
    ("neurology", r"\b(?:neurolog|epileps)\b"),
    ("plastic surgery", r"\b(?:plastic\s+surg|reconstruct|cosmetic\s+surg)\b"),
    ("private gp", r"\b(?:private\s+gp|harley\s+street|private\s+doctor)\b"),
]

def find_specialties(text):
    text_l = text.lower()
    return [name for name, pat in SPECIALTY_PATTERNS if re.search(pat, text_l)]

def is_independent(detail):
    gac = detail.get("gacServiceTypes", []) or []
    for s in gac:
        nm = s.get("name", "") if isinstance(s, dict) else str(s)
        if "Independent" in nm: return True
    prov_name = (detail.get("providerName") or "").lower()
    if "nhs" in prov_name or "trust" in prov_name or "ccg" in prov_name or "icb" in prov_name:
        return False
    return None

def classify(loc, detail):
    name = (loc.get("locationName") or detail.get("name") or "").strip()
    gac = detail.get("gacServiceTypes", []) or []
    gac_names = " ".join((s.get("name", "") if isinstance(s, dict) else str(s)) for s in gac)
    specialisms = detail.get("specialisms", []) or []
    spec_names = " ".join((s.get("name", "") if isinstance(s, dict) else str(s)) for s in specialisms)
    blob = f"{name} {gac_names} {spec_names}"
    independent = is_independent(detail)
    if independent is False:
        if GP_NAME_RE.search(name) or "Doctors consultation service" in gac_names:
            return ("nhs_gp", [], False)
        return ("nhs_other", [], False)
    if HOSPITAL_RE.search(name) and independent:
        return ("private_hospital", find_specialties(blob), True)
    if "Doctors consultation service - Independent" in gac_names or "Doctors treatment service" in gac_names:
        return ("private_consultation", find_specialties(blob), True)
    if DIAGNOSTIC_RE.search(name) or "Diagnostic" in gac_names:
        return ("private_diagnostic", find_specialties(blob), True)
    if AESTHETIC_RE.search(name): return ("aesthetic_cosmetic", [], True)
    if independent is True: return ("other_independent", find_specialties(blob), True)
    return ("unclassified", [], False)

def main():
    if not os.environ.get("CQC_KEY"):
        sys.exit("Need CQC_KEY env var.")
    print("Paginating CQC /locations for London...")
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
            print(f"  page {page}/{data.get('totalPages',1)} - {len(summaries)} London active")
        if page >= data.get("totalPages", 1): break
        page += 1
        time.sleep(0.3)
    print(f"\n{len(summaries)} active London locations.\n")

    # Pre-filter: drop obvious non-medical from summary names alone
    prefilter = Counter()
    keep = []
    for loc in summaries:
        nm = (loc.get("locationName") or "").lower()
        if SUMMARY_DROP_RE.search(nm):
            if "dent" in nm or "orthodont" in nm or "smile" in nm: prefilter["dental"] += 1
            elif "pharmac" in nm or "chemist" in nm: prefilter["pharmacy"] += 1
            elif "care home" in nm or "nursing home" in nm or "residential" in nm: prefilter["care_home"] += 1
            elif "hospice" in nm: prefilter["hospice"] += 1
            else: prefilter["other_non_medical"] += 1
            continue
        keep.append(loc)
    print("Pre-filtered from summary (no detail fetch needed):")
    for cat, n in prefilter.most_common():
        print(f"  {cat:25s} {n}")
    print(f"\n{len(keep)} locations remaining for detail fetch.\n")

    print("Fetching detail (3 workers, generous backoff)...")
    results = []
    done = 0
    def worker(loc):
        try:
            d = cqc_get(f"/locations/{loc['locationId']}")
            return (loc, d or {})
        except Exception as e:
            return (loc, {"__error__": str(e)})
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(worker, loc): loc for loc in keep}
        for fut in as_completed(futures):
            loc, detail = fut.result()
            done += 1
            if "__error__" in detail:
                results.append({"locationId": loc.get("locationId",""),"name": loc.get("locationName",""),"postcode": loc.get("postalCode",""),"category": "error","is_private": False,"specialties": "","providerName": "","url": f"https://www.cqc.org.uk/location/{loc.get('locationId','')}"})
            else:
                cat, specs, is_pri = classify(loc, detail)
                results.append({"locationId": loc.get("locationId",""),"name": loc.get("locationName",""),"postcode": loc.get("postalCode",""),"category": cat,"is_private": is_pri,"specialties": ";".join(specs),"providerName": detail.get("providerName",""),"url": f"https://www.cqc.org.uk/location/{loc.get('locationId','')}"})
            if done % 250 == 0:
                print(f"  {done}/{len(keep)}")
                # Checkpoint to CSV every 1000
                if done % 1000 == 0:
                    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
                        w = csv.DictWriter(f, fieldnames=["locationId","name","postcode","category","is_private","specialties","providerName","url"])
                        w.writeheader()
                        for r in results: w.writerow(r)

    by_cat = Counter(r["category"] for r in results)
    for cat, n in prefilter.items():
        by_cat[cat] += n
    private_specs = Counter()
    for r in results:
        if r["is_private"] and r["specialties"]:
            for s in r["specialties"].split(";"): private_specs[s] += 1

    print(f"\n{'='*68}")
    print(f"London CQC survey - {len(results) + sum(prefilter.values())} active locations")
    print(f"{'='*68}\n")
    print("By category:")
    for cat, n in by_cat.most_common():
        marker = "  -> private" if cat.startswith(("private_", "aesthetic", "other_independent")) else ""
        print(f"  {cat:25s} {n:5d} {marker}")
    total_private = sum(n for c, n in by_cat.items() if c.startswith(("private_", "aesthetic", "other_independent")))
    print(f"\n  TOTAL PRIVATE MEDICAL LOCATIONS IN LONDON: {total_private}")
    print(f"  (vs ~234 currently in your directory)\n")
    print("Top specialties among private locations:")
    for s, n in private_specs.most_common(20):
        print(f"  {s:25s} {n}")

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["locationId","name","postcode","category","is_private","specialties","providerName","url"])
        w.writeheader()
        for r in results: w.writerow(r)
    print(f"\nFull CSV: {OUT_CSV.resolve()}")

if __name__ == "__main__":
    main()
'@ | Set-Content -Path "C:\nhs-rename\survey_cqc_london.py" -Encoding UTF8
