#!/usr/bin/env python3
"""
Generate private_clinics.json from cqc_london_cache, classified by specialty.

Outputs in the EXACT shape that merge_into_dataset.py expects (snake_case
fields). After this runs:
    private_clinics.json -> merge_into_dataset.py -> merged.json + index.html

Improvements over previous version:
  - URLs prefixed with https:// at source (no more broken Website buttons)
  - Address-only names like "10 Harley Street" get the providerName appended
    so multiple consultants at the same address show as distinct cards
  - Expanded specialty patterns (women's health, sexual health, travel,
    sports medicine, physiotherapy, etc.) — moves records out of the
    generic "consultation" bucket
  - "consultation" bucket renamed to "private-gp" (more meaningful)

Runs in seconds — no API calls (cache is local).
"""
import gzip, json, re
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cqc_london_cache.json.gz"
OUT = ROOT / "private_clinics.json"

# NHS GP practice ODS code pattern: letter (not V/X) + 5 digits.
NHS_GP_ODS_RE = re.compile(r"^[A-HJ-NPSW-Y]\d{5}$")

# Service types CQC actually returns (short names, not the schema's full names)
MEDICAL_SERVICE_TYPES = {
    "Doctors/Gps",
    "Mobile doctors",
    "Diagnosis/screening",
    "Clinic",
    "Hospital",
    "Hospitals - Mental health/capacity",
    "Urgent care centres",
    "Phone/online advice",
    "Long-term conditions",
    "Rehabilitation (illness/injury)",
    "Hyperbaric chamber services",
    "Hospice",
    "Home hospice care",
}

# Service types we explicitly DO NOT include in private clinic listings
EXCLUDE_SERVICE_TYPES = {
    "Homecare agencies",
    "Residential homes",
    "Nursing homes",
    "Supported living",
    "Supported housing",
    "Shared lives",
    "Dentist",
    "Prison healthcare",
    "Ambulances",
    "Blood and transplant service",
    "Specialist college service",
    "Community services - Substance abuse",
    "Rehabilitation (substance abuse)",
}

# Name patterns to identify clinical specialty.
# Order matters: first match wins for ambiguous names.
NAME_PATTERNS = [
    ("psychiatry",       r"\b(?:psychiatr|psycholog|mental\s+health|counsell|therap|cbt\b|"
                         r"eating\s+disorder|addiction|anxiety|depression|trauma)"),
    ("cardiology",       r"\b(?:cardio|heart\s+clinic|cardiologist|arrhythmia)"),
    ("dermatology",      r"\b(?:dermatolog|skin\s+(?:clinic|doctor|specialist)|"
                         r"\bskin\b|mole\s+clinic|acne\s+clinic)"),
    ("gynaecology",      r"\b(?:gynae|obstet|fertility|ivf\b|womens?\s+(?:health|clinic)|"
                         r"menopause|\bpms\b|premenstrual|maternity|pregnan|cervical|"
                         r"endometri|miscarriage|coil|contracep)"),
    ("paediatrics",      r"\b(?:paediatr|childrens?\s+(?:clinic|health|hospital)|"
                         r"child\s+(?:health|psych))"),
    ("orthopaedics",     r"\b(?:orthopaed|musculoskelet|sports\s+(?:med|injury|clinic)|"
                         r"joint\s+(?:clinic|specialist)|knee\s+clinic|shoulder\s+clinic|"
                         r"spine\s+(?:clinic|surgery)|back\s+pain\s+clinic)"),
    ("urology",          r"\b(?:urolog|prostate|kidney\s+(?:clinic|stone)|"
                         r"erectile|circumcis|vasectom)"),
    ("ent",              r"\b(?:\bent\b|ear,?\s+nose|otolaryng|hearing\s+(?:test|clinic)|"
                         r"sinus\s+clinic|voice\s+clinic)"),
    ("ophthalmology",    r"\b(?:ophthalm|eye\s+(?:clinic|specialist|hospital)|"
                         r"\boptometr|laser\s+eye|cataract|vision\s+clinic)"),
    ("gastroenterology", r"\b(?:gastroenterolog|endoscop|colonoscop|liver\s+clinic|"
                         r"\bibd\b|crohn|colitis|gut\s+clinic|gastrointestinal)"),
    ("oncology",         r"\b(?:oncolog|cancer\s+(?:clinic|centre|care)|chemotherap|"
                         r"radiotherap|tumour|mammogram)"),
    ("rheumatology",     r"\b(?:rheumatolog|arthritis\s+clinic|gout\s+clinic|fibromyalg|lupus)"),
    ("endocrinology",    r"\b(?:endocrinolog|diabetes\s+(?:clinic|centre)|hormone|"
                         r"thyroid\s+(?:clinic|specialist))"),
    ("respiratory",      r"\b(?:respirator|lung\s+(?:clinic|cancer)|chest\s+clinic|"
                         r"sleep\s+(?:clinic|disorder|apno?ea)|asthma\s+clinic|copd)"),
    ("neurology",        r"\b(?:neurolog|epileps|migraine|parkinson|mult.+sclerosis|"
                         r"memory\s+clinic|stroke\s+clinic|headache\s+clinic)"),
    ("haematology",      r"\b(?:haematolog|hematolog|blood\s+(?:clinic|disorder)|"
                         r"anaem|leukaemia|lymphoma)"),
    ("plastic-surgery",  r"\b(?:plastic\s+surg|reconstruct\s+surg|cosmetic\s+surg|"
                         r"rhinoplast|breast\s+(?:augment|reduct|implant)|liposuct)"),
    ("vascular",         r"\b(?:vascular|vein\s+clinic|varicose|venous|phlebolog|"
                         r"thrombosis|deep\s+vein)"),
    ("diagnostic",       r"\b(?:diagnostic|imaging|radiology|scan(?:ning)?\s+(?:clinic|centre)|"
                         r"\bmri\b|\bx-?ray\b|ultrasound|ct\s+scan|pet\s+scan|"
                         r"echocardio|ecg\b)"),
    ("aesthetic",        r"\b(?:aesthet|botox|laser\s+hair|cosmetic(?!\s+surg)|"
                         r"filler|anti-aging|anti-?wrinkle|rejuvenat|beauty\s+clinic)"),
    ("weight-loss",      r"\b(?:slimming|weight\s+(?:loss|management)|bariatric|"
                         r"obesity\s+clinic|gastric\s+(?:band|sleeve|bypass))"),
    ("travel-health",    r"\b(?:travel\s+(?:clinic|health|medicine|vaccin)|yellow\s+fever|"
                         r"tropical\s+med|expat\s+health)"),
    ("sexual-health",    r"\b(?:sexual\s+health|\bsti\b|\bstd\b|\bhiv\b|genitourinary|"
                         r"\bgum\s+clinic|hpv\s+clinic)"),
    ("private-gp",       r"\b(?:private\s+(?:gp|doctor|practice|family)|harley\s+street|"
                         r"family\s+(?:doctor|medicine|practice)|general\s+practice|"
                         r"medicentre|medicus|medic\s+clinic|doctor\s+now|doctap)"),
    ("physiotherapy",    r"\b(?:physiotherap|chiropract|osteopath|pilates|rehab)"),
]

# CQC regulated activities that imply a specialty when the name is generic.
ACTIVITY_SPECIALTIES = {
    "Family planning": "sexual-health",
    "Maternity and midwifery services": "gynaecology",
    "Termination of pregnancies": "gynaecology",
    "Services in slimming clinics": "weight-loss",
}

def classify_specialties(rec):
    """Return list of specialty keys for a private clinic record."""
    specs = set()
    # Match against location name, provider name and website domain —
    # e.g. "10 Harley Street" tells us nothing, but its provider
    # "XYZ Dermatology Ltd" or website drhausdermatology.com does.
    text = " ".join([
        rec.get("name", ""),
        rec.get("providerName", ""),
        (rec.get("website", "") or "").replace("-", " ").replace(".", " ").replace("/", " "),
    ]).lower()

    # 1. Name pattern matching
    for key, pat in NAME_PATTERNS:
        if re.search(pat, text):
            specs.add(key)

    # 2. Regulated-activity hints (only when the name tells us nothing —
    #    a full-service hospital registers many activities)
    if not specs:
        for act, key in ACTIVITY_SPECIALTIES.items():
            if act in rec.get("regulatedActivities", []):
                specs.add(key)

    # 3. Default by service type if nothing specific matched
    if not specs:
        gac = rec.get("gacServiceTypes", [])
        if "Diagnosis/screening" in gac:
            specs.add("diagnostic")
        elif "Hospital" in gac or "Hospitals - Mental health/capacity" in gac:
            specs.add("hospital")
        elif "Urgent care centres" in gac:
            specs.add("urgent-care")
        elif "Hospice" in gac or "Home hospice care" in gac:
            specs.add("hospice")
        elif "Mobile doctors" in gac:
            specs.add("private-gp")
        else:
            specs.add("private-gp")

    return sorted(specs)

# Person-led providers: an individual doctor registered with CQC in their
# own name — the closest open-data proxy for "private consultant".
PERSON_RE = re.compile(
    r"^(dr|mr|mrs|ms|miss|prof(?:essor)?)\.?\s+[a-z]", re.I)

def is_consultant_led(rec):
    if rec.get("ownershipType") == "Individual":
        return True
    return bool(PERSON_RE.match(rec.get("providerName", "")) or
                PERSON_RE.match(rec.get("name", "")))

def is_nhs_gp(rec):
    """A real NHS GMS GP practice."""
    gac = rec.get("gacServiceTypes", [])
    if "Doctors/Gps" not in gac: return False
    ods = (rec.get("odsCode") or "").upper()
    return bool(NHS_GP_ODS_RE.match(ods))

def is_private(rec):
    """A private medical clinic / consultant / hospital — NOT an NHS GP."""
    if rec.get("registrationStatus") == "Deregistered":
        return False
    gac = rec.get("gacServiceTypes", [])

    if any(g in EXCLUDE_SERVICE_TYPES for g in gac):
        return False
    if not any(g in MEDICAL_SERVICE_TYPES for g in gac):
        return False
    if is_nhs_gp(rec):
        return False

    prov = (rec.get("providerName") or "").lower()
    if any(t in prov for t in ["nhs trust", "nhs foundation", " trust"]):
        return False

    nm = rec["name"].lower()
    if any(t in nm for t in [
        " nhs trust", "nhs foundation", "community nhs",
        "urgent treatment centre", "walk-in centre", "walk in centre",
    ]):
        return False
    if any(bad in nm for bad in ["veterinary", "funeral", "tattoo", "piercing"]):
        return False

    return True

DOMAINY_RE = re.compile(r"^[a-z0-9][\w\-.]*\.[a-z]{2,}", re.IGNORECASE)

def normalize_url(u):
    """Ensure website URL has https:// prefix; drop garbage."""
    if not u: return ""
    u = u.strip()
    if not u: return ""
    low = u.lower()
    if low.startswith(("mailto:", "tel:", "fax:", "sms:")):
        return ""
    if low.startswith(("http://", "https://")):
        return u
    if DOMAINY_RE.match(u):
        return "https://" + u
    return ""

def display_name(rec):
    """If the CQC location name is just an address (e.g. '10 Harley Street'),
    append the provider name so duplicates show as distinct cards."""
    name = (rec.get("name") or "").strip()
    prov = (rec.get("providerName") or "").strip()
    if not prov or not name:
        return name
    # Address-only pattern: starts with number, ends with street type, no
    # medical noun
    looks_like_address = bool(re.match(
        r"^\d+[a-z]?\s*[-–]?\s*\d*\s+[A-Z]",
        name
    )) and not re.search(
        r"\b(surgery|practice|clinic|centre|center|medical|health|hospital)\b",
        name, re.IGNORECASE
    )
    if looks_like_address:
        # Keep provider name short to avoid bloating card display
        if len(prov) > 50:
            prov = prov[:47] + "..."
        return f"{name} — {prov}"
    return name

def to_merge_shape(rec, specs):
    parts = [p for p in [
        rec.get("address1") or "",
        rec.get("address2") or "",
        rec.get("town") or "",
    ] if p]
    address = ", ".join(parts)
    return {
        "cqc_id":         rec.get("locationId", ""),
        "ods_code":       rec.get("odsCode", ""),
        "name":           display_name(rec),
        "address":        address,
        "postcode":       rec.get("postcode", ""),
        "phone":          rec.get("phone", ""),
        "website":        normalize_url(rec.get("website", "")),
        "specialties":    specs,
        "cqc_rating":     rec.get("currentRating", ""),
        "cqc_url":        rec.get("cqcUrl", ""),
        "localAuthority": rec.get("localAuthority", ""),
        "lat":            rec.get("lat"),
        "lon":            rec.get("lon"),
        "providerName":   rec.get("providerName", ""),
    }


def deduplicate(records):
    """When multiple records share (name, postcode), keep the best-rated one.
    CQC sometimes registers many consultants at the same shared address with
    identical name strings — showing 14 identical 'Harley Street' cards is
    bad UX."""
    RATING_SCORE = {"Outstanding": 4, "Good": 3, "Requires improvement": 2,
                    "Inadequate": 1, "": 0}
    best = {}
    for r in records:
        key = (r["name"].strip().lower(), r["postcode"].strip().upper())
        if not key[0] or not key[1]:
            continue
        score = (RATING_SCORE.get(r.get("cqc_rating", ""), 0),
                 len(r.get("specialties", [])))
        if key not in best or score > best[key][0]:
            best[key] = (score, r)
    return [v[1] for v in best.values()]

def main():
    if not CACHE.exists():
        print(f"ERROR: {CACHE} not found.")
        return
    with gzip.open(CACHE, "rt") as f:
        cache = json.load(f)
    print(f"Loaded {len(cache):,} cached locations")

    output = []
    by_specialty = Counter()
    by_borough = Counter()
    for rec in cache.values():
        if not is_private(rec): continue
        specs = classify_specialties(rec)
        if is_consultant_led(rec):
            specs = sorted(set(specs) | {"consultant"})
        output.append(to_merge_shape(rec, specs))
        for s in specs: by_specialty[s] += 1
        by_borough[rec.get("localAuthority", "(unknown)")] += 1

    before = len(output)
    output = deduplicate(output)
    print(f"Deduplicated: {before:,} -> {len(output):,} ({before-len(output)} duplicates removed)")
    OUT.write_text(json.dumps(output, indent=2))
    print(f"\nWrote {len(output):,} private clinics to {OUT.name}")
    print(f"\nBy specialty:")
    for s, n in by_specialty.most_common():
        print(f"  {s:25s} {n:4d}")
    print(f"\nBy borough (top 10):")
    for b, n in by_borough.most_common(10):
        print(f"  {b:30s} {n:4d}")

if __name__ == "__main__":
    main()
