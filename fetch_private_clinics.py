#!/usr/bin/env python3
"""
fetch_private_clinics.py  —  v3 (broadened, stricter quality)
==============================================================
Pulls ALL independently-operated, doctor-led, currently-registered
CQC locations in London.

Key changes vs previous version:
- Pulls organisationType=IndependentProvider only (excludes NHS Trusts, social care)
- Requires registrationStatus=Registered (no deregistered noise)
- Hard-excludes NHS hospitals, prisons, trust HQs by name pattern
- Hard-excludes non-clinical service types (ambulances, homecare, residential)
- Broad specialty classifier: 25 specialties instead of 15
- Keeps "cosmetic" and "sexual health" which were previously missed
- Outputs private_clinics.json with same schema as before
"""

import json, os, re, sys, time, urllib.request, urllib.error, urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT     = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"
OUT_JSON = ROOT / "private_clinics.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"

# ── London postcode districts ────────────────────────────────────────────────
LONDON_PREFIXES = {
    "EC1A","EC1M","EC1N","EC1P","EC1R","EC1V","EC1Y",
    "EC2A","EC2M","EC2N","EC2P","EC2R","EC2V","EC2Y",
    "EC3A","EC3M","EC3N","EC3P","EC3R","EC3V",
    "EC4A","EC4M","EC4N","EC4P","EC4R","EC4V","EC4Y",
    "WC1A","WC1B","WC1E","WC1H","WC1N","WC1R","WC1V","WC1X",
    "WC2A","WC2B","WC2E","WC2H","WC2N","WC2R",
    "E1","E1W","E2","E3","E4","E5","E6","E7","E8","E9",
    "E10","E11","E12","E13","E14","E15","E16","E17","E18","E20",
    "N1","N1C","N1P","N4","N5","N6","N7","N8","N9",
    "N10","N11","N12","N13","N14","N15","N16","N17","N18","N19","N20","N21","N22",
    "NW1","NW1W","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11",
    "SE1","SE1P","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9",
    "SE10","SE11","SE12","SE13","SE14","SE15","SE16","SE17","SE18","SE19",
    "SE20","SE21","SE22","SE23","SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1H","SW1P","SW1V","SW1W","SW1X","SW1Y",
    "SW2","SW3","SW4","SW5","SW6","SW7","SW8","SW9",
    "SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18","SW19","SW20",
    "W1","W1A","W1B","W1C","W1D","W1F","W1G","W1H","W1J","W1K","W1S","W1T","W1U","W1W",
    "W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
    "BR1","BR2","BR3","BR4","BR5","BR6","BR7","BR8",
    "CR0","CR2","CR3","CR4","CR5","CR6","CR7","CR8","CR9",
    "DA1","DA5","DA6","DA7","DA8","DA14","DA15","DA16","DA17","DA18",
    "EN1","EN2","EN3","EN4","EN5","EN7","EN8","EN9",
    "HA0","HA1","HA2","HA3","HA4","HA5","HA6","HA7","HA8","HA9",
    "IG1","IG2","IG3","IG4","IG5","IG6","IG7","IG8","IG11",
    "KT1","KT2","KT3","KT4","KT5","KT6","KT7","KT8","KT9",
    "RM1","RM2","RM3","RM4","RM5","RM6","RM7","RM8","RM9",
    "RM10","RM11","RM12","RM13","RM14",
    "SM1","SM2","SM3","SM4","SM5","SM6",
    "TW1","TW2","TW3","TW4","TW5","TW6","TW7","TW8","TW9",
    "TW10","TW11","TW12","TW13","TW14",
    "UB1","UB2","UB3","UB4","UB5","UB6","UB7","UB8","UB9","UB10","UB11",
}

def postcode_district(pc):
    pc = (pc or "").strip().upper()
    return pc.split()[0] if " " in pc else (pc[:-3] if len(pc) >= 5 else pc)

def is_london(pc):
    return postcode_district(pc) in LONDON_PREFIXES

# ── Hard DROP by name ────────────────────────────────────────────────────────
# NHS hospitals, prisons, trust admin, social care admin
DROP_NAME_RE = re.compile(
    r"\b(?:"
    # NHS hospitals and trusts
    r"nhs\s+(?:trust|foundation|england)|"
    r"\bnhs\b.*\b(?:trust|foundation)\b|"
    r"university\s+hospital|"
    r"general\s+hospital|"
    r"(?:royal|st\.?\s+\w+)'?s?\s+hospital\b|"
    r"\bhmp\b|prison|young\s+offender|"
    # Admin / non-clinical
    r"trust\s+h(?:ead)?q|trust\s+headquarters|"
    r"cqc\s+registration|"
    # Pure dental (separate section)
    r"\bdent(?:ist|al|istry)\b(?!\s+and\s+(?:medical|aesthetics?\s+clinic))|"
    r"\borthodont|"
    # Pure social care
    r"care\s+home|nursing\s+home|residential\s+home|"
    r"supported\s+living|extra\s+care\s+housing|"
    r"domiciliary|homecare\b|"
    # Veterinary / other
    r"veterinary|funeral|crematorium|"
    r"pharmacy\b|dispensing\b|chemist\b"
    r")\b",
    re.IGNORECASE,
)

# Hard DROP by service type
DROP_SERVICE_RE = re.compile(
    r"\b(?:ambulance|residential\s+home|nursing\s+home|"
    r"care\s+home|homecare|supported\s+living|"
    r"hospice|long\s+stay|prison\s+healthcare)\b",
    re.IGNORECASE,
)

# Must have at least one of these service types to be kept
KEEP_SERVICE_RE = re.compile(
    r"\b(?:"
    r"doctors?\s+(?:consultation|treatment|service)|"
    r"independent\s+(?:doctor|hospital|clinic|medical)|"
    r"private\s+(?:doctor|hospital|clinic|gp|medical)|"
    r"diagnostic\s+and\s+screening|"
    r"primary\s+medical\s+services|"
    r"mobile\s+doctors?\s+service|"
    r"rehabilitation\s+services|"
    r"family\s+planning|"
    r"termination\s+of\s+pregnancy|"
    r"hospital\s+services|"
    r"urgent\s+care|"
    r"mental\s+health|"
    r"long\s+term\s+conditions"
    r")\b",
    re.IGNORECASE,
)

# ── Specialty classifier — 25 categories ────────────────────────────────────
SPECIALTY_PATTERNS = [
    ("private gp",       r"\b(?:general\s+pract|gp\s+(?:clinic|service|surgery|centre)|private\s+gp|family\s+(?:doctor|practice|medicine)|primary\s+care\s+clinic)\b"),
    ("psychiatry",       r"\b(?:psychiatr|mental\s+health|psycholog|psychother|wellbeing\s+clinic|talking\s+therap|adhd|autism|neurodevelopment|eating\s+disorder|addiction\s+treatment)\b"),
    ("dermatology",      r"\b(?:dermatol|skin\s+(?:clinic|centre|specialist)|cosmetic\s+derm|mole\s+|eczema|psoriasis)\b"),
    ("cardiology",       r"\b(?:cardiol|heart\s+(?:clinic|centre|specialist)|cardiovascular|echocardiograph|palpitation)\b"),
    ("gynaecology",      r"\b(?:gynaecol|gynecol|women'?s\s+health|fertility|ivf\b|obstetric|antenatal|menopause|endometri|cervical)\b"),
    ("oncology",         r"\b(?:oncol|cancer\s+(?:clinic|centre|care)|chemotherapy|radiotherapy|tumour)\b"),
    ("orthopaedics",     r"\b(?:orthop(?:aed)?|joint\s+(?:clinic|replacement)|spine\s+(?:clinic|centre)|bone\s+(?:clinic|specialist)|sports\s+(?:injury|medicine|clinic|ortho)|knee\s+|hip\s+replacement)\b"),
    ("ophthalmology",    r"\b(?:ophthalm|eye\s+(?:clinic|centre|hospital|specialist)|vision\s+(?:clinic|centre)|laser\s+eye|cataract|glaucoma|retina)\b"),
    ("ent",              r"\b(?:ent\b|ear,?\s*nose\s+(?:and|&)\s*throat|otolaryng|hearing\s+(?:clinic|loss|aid)|tinnitus|sinus|rhinoplasty)\b"),
    ("urology",          r"\b(?:urol|urinary|bladder\s+clinic|prostate|kidney\s+(?:stone|clinic)|men'?s\s+health\s+clinic)\b"),
    ("neurology",        r"\b(?:neurol|brain\s+(?:clinic|specialist)|headache\s+clinic|migraine\s+clinic|epilepsy|multiple\s+sclerosis|ms\s+clinic|stroke\s+clinic)\b"),
    ("gastroenterology", r"\b(?:gastroenter|endoscopy|colonoscopy|bowel\s+(?:clinic|cancer)|ibs\b|crohn|irritable\s+bowel|liver\s+(?:clinic|specialist))\b"),
    ("endocrinology",    r"\b(?:endocrin|diabetes\s+(?:clinic|specialist|centre)|thyroid\s+(?:clinic|specialist)|hormone\s+(?:clinic|specialist)|weight\s+management\s+clinic)\b"),
    ("rheumatology",     r"\b(?:rheumatol|arthritis\s+(?:clinic|specialist)|joint\s+pain\s+clinic|lupus\b|fibromyalgia)\b"),
    ("paediatrics",      r"\b(?:paediatric|pediatric|child(?:ren)?'?s?\s+(?:clinic|hospital|health)|child\s+development|neonatal|adolescent\s+health)\b"),
    ("cosmetic",         r"\b(?:cosmet|aesthet|plastic\s+surg|botox|filler|rhinoplasty|breast\s+(?:augment|reduction|implant)|liposuction|facelift|anti.ageing)\b"),
    ("sexual health",    r"\b(?:sexual\s+health|sti\b|std\b|genitourin|hiv\s+(?:clinic|test|treatment)|contraception\s+clinic|prep\b)\b"),
    ("diagnostics",      r"\b(?:diagnos|imaging\b|mri\b|\bct\s+scan\b|radiolog|ultrasound|x.ray\s+clinic|pathology|blood\s+test\s+clinic|health\s+screen)\b"),
    ("physiotherapy",    r"\b(?:physiother|physio\b|sports\s+rehab|musculoskeletal|osteopath|chiropractic|occupational\s+ther)\b"),
    ("travel",           r"\b(?:travel\s+(?:clinic|health|medicine|vaccine)|tropical\s+medicine|yellow\s+fever|malaria\s+(?:clinic|prophylaxis))\b"),
    ("allergy",          r"\b(?:allerg|immunol|hay\s+fever\s+clinic|food\s+intolerance\s+clinic|anaphylaxis)\b"),
    ("pain management",  r"\b(?:pain\s+(?:clinic|management|specialist|centre)|chronic\s+pain|pain\s+relief\s+clinic)\b"),
    ("respiratory",      r"\b(?:respirat|pulmonol|lung\s+(?:clinic|specialist)|asthma\s+clinic|copd\b|sleep\s+(?:clinic|apnoea))\b"),
    ("hospital",         r"\b(?:private\s+hospital|independent\s+hospital|nuffield\s+health|spire\s+hospital|bupa\s+hospital|hca\b|circle\s+health|the\s+\w+\s+hospital\s+(?:london|private))\b"),
    ("sexual health",    r"\b(?:sexual\s+health|sti\b|genitourin|hiv\s+clinic|prep\b|contraception)\b"),
]

# Deduplicate patterns list
seen = set()
SPECIALTY_PATTERNS = [p for p in SPECIALTY_PATTERNS if not (p[0] in seen or seen.add(p[0]))]

def classify_specialty(name, services_blob):
    blob = f"{name} {services_blob}".lower()
    found = [tag for tag, pat in SPECIALTY_PATTERNS if re.search(pat, blob, re.IGNORECASE)]
    return found if found else ["general medicine"]

# ── Borough from postcode ────────────────────────────────────────────────────
BOROUGH_MAP = {
    'E1':'Tower Hamlets','E2':'Tower Hamlets','E3':'Tower Hamlets',
    'E4':'Waltham Forest','E5':'Hackney','E6':'Newham','E7':'Newham',
    'E8':'Hackney','E9':'Hackney','E10':'Waltham Forest',
    'E11':'Waltham Forest','E12':'Newham','E13':'Newham',
    'E14':'Tower Hamlets','E15':'Newham','E16':'Newham',
    'E17':'Waltham Forest','E18':'Redbridge','E20':'Newham',
    'EC1':'Islington','EC2':'City of London','EC3':'City of London','EC4':'City of London',
    'N1':'Islington','N2':'Barnet','N3':'Barnet','N4':'Haringey',
    'N5':'Islington','N6':'Haringey','N7':'Islington','N8':'Haringey',
    'N9':'Enfield','N10':'Haringey','N11':'Barnet','N12':'Barnet',
    'N13':'Enfield','N14':'Enfield','N15':'Haringey','N16':'Hackney',
    'N17':'Haringey','N18':'Enfield','N19':'Islington','N20':'Barnet',
    'N21':'Enfield','N22':'Haringey',
    'NW1':'Camden','NW2':'Brent','NW3':'Camden','NW4':'Barnet',
    'NW5':'Camden','NW6':'Brent','NW7':'Barnet','NW8':'Westminster',
    'NW9':'Brent','NW10':'Brent','NW11':'Barnet',
    'SE1':'Southwark','SE2':'Greenwich','SE3':'Greenwich','SE4':'Lewisham',
    'SE5':'Southwark','SE6':'Lewisham','SE7':'Greenwich','SE8':'Lewisham',
    'SE9':'Greenwich','SE10':'Greenwich','SE11':'Lambeth','SE12':'Lewisham',
    'SE13':'Lewisham','SE14':'Lewisham','SE15':'Southwark','SE16':'Southwark',
    'SE17':'Southwark','SE18':'Greenwich','SE19':'Bromley','SE20':'Bromley',
    'SE21':'Southwark','SE22':'Southwark','SE23':'Lewisham','SE24':'Lambeth',
    'SE25':'Croydon','SE26':'Lewisham','SE27':'Lambeth','SE28':'Greenwich',
    'SW1':'Westminster','SW2':'Lambeth','SW3':'Kensington & Chelsea',
    'SW4':'Lambeth','SW5':'Kensington & Chelsea','SW6':'Hammersmith & Fulham',
    'SW7':'Kensington & Chelsea','SW8':'Lambeth','SW9':'Lambeth',
    'SW10':'Kensington & Chelsea','SW11':'Wandsworth','SW12':'Wandsworth',
    'SW13':'Richmond','SW14':'Richmond','SW15':'Wandsworth','SW16':'Lambeth',
    'SW17':'Wandsworth','SW18':'Wandsworth','SW19':'Merton','SW20':'Merton',
    'W1':'Westminster','W2':'Westminster','W3':'Ealing','W4':'Hounslow',
    'W5':'Ealing','W6':'Hammersmith & Fulham','W7':'Ealing',
    'W8':'Kensington & Chelsea','W9':'Westminster','W10':'Kensington & Chelsea',
    'W11':'Kensington & Chelsea','W12':'Hammersmith & Fulham',
    'W13':'Ealing','W14':'Hammersmith & Fulham',
    'WC1':'Camden','WC2':'Westminster',
    'BR1':'Bromley','BR2':'Bromley','BR3':'Bromley','BR4':'Bromley',
    'BR5':'Bromley','BR6':'Bromley','BR7':'Bromley',
    'CR0':'Croydon','CR2':'Croydon','CR4':'Merton','CR5':'Croydon',
    'CR7':'Croydon','CR8':'Croydon',
    'DA1':'Bexley','DA5':'Bexley','DA6':'Bexley','DA7':'Bexley',
    'DA8':'Bexley','DA14':'Bexley','DA15':'Bexley','DA16':'Bexley','DA17':'Bexley',
    'EN1':'Enfield','EN2':'Enfield','EN3':'Enfield','EN4':'Barnet',
    'EN5':'Barnet','EN8':'Enfield','EN9':'Enfield',
    'HA0':'Brent','HA1':'Harrow','HA2':'Harrow','HA3':'Harrow',
    'HA4':'Hillingdon','HA5':'Harrow','HA6':'Hillingdon',
    'HA7':'Harrow','HA8':'Barnet','HA9':'Brent',
    'IG1':'Redbridge','IG2':'Redbridge','IG3':'Redbridge','IG4':'Redbridge',
    'IG5':'Redbridge','IG6':'Redbridge','IG7':'Redbridge','IG8':'Redbridge',
    'IG11':'Barking & Dagenham',
    'KT1':'Kingston','KT2':'Kingston','KT3':'Kingston','KT4':'Kingston',
    'KT5':'Kingston','KT6':'Kingston','KT7':'Kingston','KT8':'Richmond','KT9':'Kingston',
    'RM1':'Havering','RM2':'Havering','RM3':'Havering','RM5':'Havering',
    'RM6':'Barking & Dagenham','RM7':'Havering','RM8':'Barking & Dagenham',
    'RM9':'Barking & Dagenham','RM10':'Barking & Dagenham',
    'RM11':'Havering','RM12':'Havering','RM13':'Havering','RM14':'Havering',
    'SM1':'Sutton','SM2':'Sutton','SM3':'Sutton','SM4':'Merton',
    'SM5':'Sutton','SM6':'Sutton',
    'TW1':'Richmond','TW2':'Richmond','TW3':'Hounslow','TW4':'Hounslow',
    'TW5':'Hounslow','TW6':'Hounslow','TW7':'Hounslow','TW8':'Hounslow',
    'TW9':'Richmond','TW10':'Richmond','TW11':'Richmond','TW12':'Richmond',
    'TW13':'Hounslow','TW14':'Hounslow',
    'UB1':'Ealing','UB2':'Ealing','UB3':'Hillingdon','UB4':'Hillingdon',
    'UB5':'Ealing','UB6':'Ealing','UB7':'Hillingdon','UB8':'Hillingdon',
    'UB9':'Hillingdon','UB10':'Hillingdon',
}

def borough_from_postcode(pc):
    if not pc: return ""
    pc = pc.strip().upper()
    district = pc.split()[0] if ' ' in pc else pc
    if district in BOROUGH_MAP: return BOROUGH_MAP[district]
    m = re.match(r'([A-Z]+\d+)', district)
    return BOROUGH_MAP.get(m.group(1), "") if m else ""

# ── HTTP helper ──────────────────────────────────────────────────────────────
def cqc_get(path, params, key, retries=3):
    url = f"{CQC_BASE}{path}"
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = {
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/3.0",
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
        except urllib.error.URLError:
            if attempt < retries - 1:
                time.sleep(2); continue
            raise
    return None

def services_blob(detail):
    parts = []
    for k in ("gacServiceTypes", "regulatedActivities", "specialisms", "serviceTypes"):
        v = detail.get(k)
        if isinstance(v, list):
            for it in v:
                parts.append(it if isinstance(it, str) else
                             (it.get("name") or it.get("description") or ""))
    return " | ".join(p for p in parts if p).lower()

# ── Pagination ───────────────────────────────────────────────────────────────
def paginate_london(key):
    print("Paginating CQC /locations …")
    candidates = []
    page = 1
    per_page = 1000
    while True:
        data = cqc_get("/locations", {"page": page, "perPage": per_page}, key)
        if not data: break
        items = data.get("locations", []) or []
        if not items: break
        for loc in items:
            # Only active, London, independent
            if loc.get("deregistrationDate"): continue
            if loc.get("registrationStatus") != "Registered": continue
            pc = loc.get("postalCode") or ""
            if not is_london(pc): continue
            name = loc.get("locationName") or loc.get("name") or ""
            if DROP_NAME_RE.search(name): continue
            candidates.append(loc)
        total_pages = data.get("totalPages", 1)
        if page % 10 == 0:
            print(f"  page {page}/{total_pages} — candidates: {len(candidates)}")
        if page >= total_pages: break
        page += 1
        time.sleep(0.15)
    print(f"\n{len(candidates)} London independent candidates found.\n")
    return candidates

# ── Detail fetch + filter ────────────────────────────────────────────────────
def build_records(candidates, key, nhs_ods, workers=12):
    print(f"Fetching detail for {len(candidates)} candidates …")
    records = []
    rejected = Counter()
    done = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(cqc_get, f"/locations/{c['locationId']}", None, key): c
            for c in candidates if c.get("locationId")
        }
        for fut in as_completed(futures):
            c = futures[fut]
            done += 1
            try:
                d = fut.result()
            except Exception:
                d = None
            if not d:
                rejected["no_detail"] += 1
                continue

            # Gate 1: not already an NHS GP
            ods = (d.get("odsCode") or "").strip().upper()
            if ods and ods in nhs_ods:
                rejected["nhs_gp"] += 1
                continue

            # Gate 2: must be independent (not NHS Trust, not Social Care Org)
            provider_type = (d.get("providerType") or "").lower()
            org_type = (d.get("organisationType") or "").lower()
            if "nhs" in provider_type or "nhs" in org_type:
                rejected["nhs_provider"] += 1
                continue
            if "social care" in provider_type:
                rejected["social_care"] += 1
                continue

            # Gate 3: name check (second pass on full name from detail)
            name_raw = (d.get("name") or d.get("locationName") or
                        d.get("providerName") or "").strip()
            if not name_raw:
                rejected["no_name"] += 1
                continue
            if DROP_NAME_RE.search(name_raw):
                rejected["bad_name"] += 1
                continue

            # Gate 4: service type must include doctor-led services
            blob = services_blob(d)
            if DROP_SERVICE_RE.search(blob) and not KEEP_SERVICE_RE.search(blob):
                rejected["social_service"] += 1
                continue
            if not KEEP_SERVICE_RE.search(blob):
                rejected["no_medical_service"] += 1
                continue

            pc = (d.get("postalCode") or "").strip().upper()
            if not is_london(pc):
                rejected["not_london"] += 1
                continue

            name = name_raw.title() if name_raw.isupper() else name_raw
            specialties = classify_specialty(name, blob)

            addr_parts = [
                d.get("postalAddressLine1") or "",
                d.get("postalAddressLine2") or "",
                d.get("postalAddressTownCity") or "",
            ]
            address = ", ".join(p for p in addr_parts if p)
            phone   = (d.get("mainPhoneNumber") or "").strip()
            website = (d.get("website") or "").strip()
            loc_id  = d.get("locationId", "")
            rating  = ((d.get("currentRatings") or {})
                       .get("overall") or {}).get("rating", "")

            records.append({
                "ods_code":    ods,
                "cqc_id":      loc_id,
                "name":        name,
                "address":     address,
                "postcode":    pc,
                "borough":     borough_from_postcode(pc),
                "phone":       phone,
                "website":     website,
                "type":        "Private",
                "specialties": specialties,
                "cqc_rating":  rating,
                "cqc_url":     f"https://www.cqc.org.uk/location/{loc_id}" if loc_id else "",
            })

            if done % 500 == 0 or done == len(futures):
                print(f"  {done}/{len(futures)} processed — kept {len(records)}")

    print(f"\nRejection summary: {dict(rejected)}")
    return records

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        sys.exit("Need CQC_KEY env var.")

    # Load NHS GP ODS codes to exclude
    nhs_ods = set()
    if GPS_JSON.exists():
        try:
            for r in json.loads(GPS_JSON.read_text()):
                code = (r.get("ods_code") or r.get("o") or "").upper()
                if code: nhs_ods.add(code)
        except Exception as e:
            print(f"WARN: couldn't load gps.json — {e}")
    print(f"Excluding {len(nhs_ods)} NHS GP ODS codes.\n")

    candidates = paginate_london(key)
    records    = build_records(candidates, key, nhs_ods)

    # Summary
    by_spec = Counter()
    for r in records:
        for s in r["specialties"]:
            by_spec[s] += 1
    print("\nRecords by specialty:")
    for sp, n in by_spec.most_common():
        print(f"  {sp:<25} {n}")

    OUT_JSON.write_text(json.dumps(records, indent=2, ensure_ascii=False))
    print(f"\nWrote {OUT_JSON} — {len(records)} private providers, "
          f"{OUT_JSON.stat().st_size // 1024} KB")

if __name__ == "__main__":
    main()
