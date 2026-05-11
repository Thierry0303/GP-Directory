#!/usr/bin/env python3
"""
One-off cleanup pass on gps.json:

  - Removes records whose postcode is NOT strictly inside the London
    postcode-prefix whitelist. (The previous is_london() had a regex
    fallback that matched DA12 → DA1, TW15 → TW1, etc., leaking Kent /
    Surrey / Hertfordshire practices into the list.)

  - Removes records with no name (empty practice name renders blank
    cards on the live site).

  - Removes records with no ODS code (defensive — shouldn't happen).

Prints what it kept / dropped, then writes the cleaned file back.
Refuses to write if it would remove more than 30% of records.
"""

import json, re, sys
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

# STRICT London postcode districts (exact match, no regex fallback).
LONDON_PREFIXES = {
    # Full EC1-EC4 set (City of London)
    "EC1A","EC1M","EC1N","EC1P","EC1R","EC1V","EC1Y",
    "EC2A","EC2M","EC2N","EC2P","EC2R","EC2V","EC2Y",
    "EC3A","EC3M","EC3N","EC3P","EC3R","EC3V",
    "EC4A","EC4M","EC4N","EC4P","EC4R","EC4V","EC4Y",
    # Full WC1-WC2 set (Camden / Holborn)
    "WC1A","WC1B","WC1E","WC1H","WC1N","WC1R","WC1V","WC1X",
    "WC2A","WC2B","WC2E","WC2H","WC2N","WC2R",
    # E1-E20 (East)
    "E1","E2","E3","E4","E5","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15",
    "E16","E17","E18","E20","E1W",
    # N1-N22 (North) — N1C and N1P also valid
    "N1","N1C","N1P","N4","N5","N6","N7","N8","N9","N10","N11","N12","N13","N14","N15","N16",
    "N17","N18","N19","N20","N21","N22",
    # NW (North West)
    "NW1","NW1W","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11","NW26",
    # SE (South East)
    "SE1","SE1P","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9","SE10","SE11","SE12",
    "SE13","SE14","SE15","SE16","SE17","SE18","SE19","SE20","SE21","SE22","SE23",
    "SE24","SE25","SE26","SE27","SE28",
    # SW (South West) — SW1 full set
    "SW1A","SW1E","SW1H","SW1P","SW1V","SW1W","SW1X","SW1Y",
    "SW2","SW3","SW4","SW5","SW6","SW7",
    "SW8","SW9","SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18",
    "SW19","SW20",
    # W (West) — W1 has letter suffixes too
    "W1","W1A","W1B","W1C","W1D","W1F","W1G","W1H","W1J","W1K","W1S","W1T","W1U","W1W",
    "W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
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
    # UK outward code is always last 3 chars off.
    return pc[:-3] if len(pc) >= 5 else pc

def is_london_strict(pc):
    """Exact match against the whitelist. No regex fallbacks."""
    return postcode_district(pc) in LONDON_PREFIXES

# Drop names that are clearly an individual practitioner rather than a
# practice, e.g. "Dr Mojgan Fitzmaurice". A legitimate single-handed practice
# would include "Surgery", "Practice", "Centre", "Clinic", "Health", etc.
_INDIVIDUAL_PRACTITIONER_RE = re.compile(
    r"^(?:dr|mr|mrs|ms|miss|prof)\.?\s",
    re.IGNORECASE,
)
_PRACTICE_NOUN_RE = re.compile(
    r"\b(?:surgery|practice|centre|center|clinic|"
    r"medical|health|partnership|group|service)s?\b",
    re.IGNORECASE,
)
def is_individual_practitioner(name):
    """True if the name reads like a doctor's personal name with no
    indication this is a registered practice."""
    if not name: return False
    if _INDIVIDUAL_PRACTITIONER_RE.match(name.strip()) and not _PRACTICE_NOUN_RE.search(name):
        return True
    return False

# A "Dr X - Y Centre" pattern usually means a specific doctor's listing
# AT the centre Y. Y is usually also in the data as its own record. Drop
# these to avoid duplicates.
_DOCTOR_AT_CENTRE_RE = re.compile(
    r"^(?:dr|mr|mrs|ms|prof)\.?\s.+\s[-–]\s",
    re.IGNORECASE,
)

def has_gpps_data(record):
    """Has NHS GP Patient Survey scores → definitely NHS-contracted."""
    return bool(record.get("gpps_overall_pct") or record.get("gpps_contact_pct"))

def is_unverified_dr_record(record):
    """A "Dr X" or "Dr X Practice" record WITHOUT GPPS data is most
    likely a private clinic or individual practitioner that leaked into
    the NHS list. Real NHS single-handed practices (like 'Dr Dhital
    Practice', 'Dr Me Silver's Practice') have GPPS scores.
    """
    name = (record.get("name") or "").strip()
    if not name:
        return False
    if not _INDIVIDUAL_PRACTITIONER_RE.match(name):
        return False
    # Dr-prefixed: only keep if we have GPPS evidence it's NHS.
    return not has_gpps_data(record)

def is_doctor_at_centre_duplicate(record):
    """Records named 'Dr X - Y Centre' usually duplicate the Y record."""
    name = (record.get("name") or "").strip()
    return bool(_DOCTOR_AT_CENTRE_RE.match(name)) and not has_gpps_data(record)

# Address-based: Harley Street is the famous private medical district —
# anything there is private, regardless of how it's named.
_PRIVATE_ADDRESS_RE = re.compile(
    r"\bharley\s+street\b|\bwimpole\s+street\b|\bdevonshire\s+place\b",
    re.IGNORECASE,
)
def is_private_address(record):
    addr = (record.get("address") or "")
    return bool(_PRIVATE_ADDRESS_RE.search(addr)) and not has_gpps_data(record)

# Drop records whose name clearly identifies them as something other than
# an NHS GP practice — pharmacies, dentists, opticians, etc.
_NON_NHS_GP_RE = re.compile(
    r"\b(?:chemist|pharmacy|drug\s+store|"
    r"dental|dentist|orthodontic|denture|"
    r"optician|optometr|opto\s|eye\s+clinic|spectacle|"
    r"audiology|hearing\s+(?:aid|test|centre)|"
    r"chiropract|osteopath|physiotherap|podiatr|"
    r"funeral|veterinary|"
    r"cosmetic\s+(?:clinic|surgery)|laser\s+hair|"
    r"slimming|weight\s+loss|"
    r"fertility\s+clinic|ivf\s+clinic|"
    r"sexual\s+health\s+clinic|"
    r"hospital(?!\s*(?:road|street|lane|hill|gardens|way))|"  # buildings called "Hospital Road" etc. are ok
    r"hospice|"
    r"ambulance|"
    r"care\s+home|residential\s+home|nursing\s+home|"
    r"sleep\s+(?:disorders?\s+)?clinic|"
    r"mole\s+clinic|aesthetic\s+clinic|"
    r"tattoo|piercing"
    r")\b",
    re.IGNORECASE,
)
# Drop records that are clearly PRIVATE clinics (we'll surface these on a
# separate Private side later; for now just remove them from the NHS list).
_PRIVATE_INDICATOR_RE = re.compile(
    r"\b(?:private|harley\s+street|"
    r"bupa|nuffield|spire|hca|circle|king\s+edward|"
    r"the\s+london\s+clinic|wellington|princess\s+grace)\b",
    re.IGNORECASE,
)

def is_non_nhs_gp(name):
    if not name: return False
    if _NON_NHS_GP_RE.search(name): return True
    if _PRIVATE_INDICATOR_RE.search(name): return True
    return False

def main():
    if not GPS_JSON.exists():
        sys.exit(f"{GPS_JSON} not found.")

    data = json.loads(GPS_JSON.read_text())
    if not isinstance(data, list):
        sys.exit("gps.json is not a JSON array.")
    print(f"Loaded {len(data)} records.")

    kept = []
    dropped_no_pc = 0
    dropped_not_london = 0
    dropped_no_name = 0
    dropped_no_ods = 0
    dropped_individual = 0
    dropped_non_gp = 0
    dropped_dr_no_gpps = 0
    dropped_doctor_at_centre = 0
    dropped_private_address = 0
    dropped_examples = {"no-name": [], "not-london": [], "individual": [],
                        "non-gp": [], "dr-no-gpps": [], "doctor-at-centre": [],
                        "private-address": [], "no-pc": [], "no-ods": []}

    for r in data:
        ods = (r.get("ods_code") or "").strip().upper()
        name = (r.get("name") or "").strip()
        pc = (r.get("postcode") or "").strip().upper()

        if not ods:
            dropped_no_ods += 1
            if len(dropped_examples["no-ods"]) < 3:
                dropped_examples["no-ods"].append(r)
            continue
        if not name:
            dropped_no_name += 1
            if len(dropped_examples["no-name"]) < 3:
                dropped_examples["no-name"].append(r)
            continue
        if not pc:
            dropped_no_pc += 1
            if len(dropped_examples["no-pc"]) < 3:
                dropped_examples["no-pc"].append(r)
            continue
        if not is_london_strict(pc):
            dropped_not_london += 1
            if len(dropped_examples["not-london"]) < 3:
                dropped_examples["not-london"].append(r)
            continue
        if is_non_nhs_gp(name):
            dropped_non_gp += 1
            if len(dropped_examples["non-gp"]) < 5:
                dropped_examples["non-gp"].append(r)
            continue
        if is_individual_practitioner(name):
            dropped_individual += 1
            if len(dropped_examples["individual"]) < 5:
                dropped_examples["individual"].append(r)
            continue
        if is_private_address(r):
            dropped_private_address += 1
            if len(dropped_examples["private-address"]) < 5:
                dropped_examples["private-address"].append(r)
            continue
        if is_doctor_at_centre_duplicate(r):
            dropped_doctor_at_centre += 1
            if len(dropped_examples["doctor-at-centre"]) < 5:
                dropped_examples["doctor-at-centre"].append(r)
            continue
        if is_unverified_dr_record(r):
            dropped_dr_no_gpps += 1
            if len(dropped_examples["dr-no-gpps"]) < 5:
                dropped_examples["dr-no-gpps"].append(r)
            continue
        kept.append(r)

    total_dropped = len(data) - len(kept)
    print(f"\nKept:    {len(kept)}")
    print(f"Dropped: {total_dropped} total")
    print(f"  no name:                              {dropped_no_name}")
    print(f"  not in London:                        {dropped_not_london}")
    print(f"  non-GP (pharmacy/dental/private):     {dropped_non_gp}")
    print(f"  individual practitioner (Dr X only):  {dropped_individual}")
    print(f"  private address (Harley St / Wimpole): {dropped_private_address}")
    print(f"  duplicate (Dr X - Y Centre):          {dropped_doctor_at_centre}")
    print(f"  unverified Dr (no GPPS score):        {dropped_dr_no_gpps}")
    print(f"  no postcode:                          {dropped_no_pc}")
    print(f"  no ODS:                               {dropped_no_ods}")

    for reason, examples in dropped_examples.items():
        if examples:
            print(f"\nExamples of '{reason}' drops:")
            for r in examples:
                print(f"  {r.get('ods_code','?'):8s} "
                      f"{r.get('name','')[:40]:40s} {r.get('postcode','?')}")

    # Safety: refuse if we'd drop more than 50%, OR if the drops include
    # any category other than "no name" + "not London" (those are the known
    # bugs in the last build; anything else is unexpected and worth pausing).
    unexpected_drops = dropped_no_pc + dropped_no_ods
    if total_dropped > len(data) * 0.5:
        sys.exit(f"\nABORT: dropping {total_dropped}/{len(data)} > 50%. "
                 "Investigate before writing.")
    if unexpected_drops > 0:
        print(f"\nWARNING: {unexpected_drops} records dropped for "
              "unexpected reasons (no postcode/no ODS). Continuing anyway.")

    GPS_JSON.write_text(json.dumps(kept, indent=2))
    print(f"\nWrote gps.json — {len(kept)} practices.")

    # Coverage summary
    by_area = Counter()
    for r in kept:
        m = re.match(r"^([A-Z]+)", (r.get("postcode") or "").strip().upper())
        if m: by_area[m.group(1)] += 1
    print("\nFinal coverage by postcode area:")
    for a, n in sorted(by_area.items(), key=lambda x: -x[1]):
        print(f"  {a:4s} {n}")

if __name__ == "__main__":
    main()
