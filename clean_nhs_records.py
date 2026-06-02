#!/usr/bin/env python3
"""
Strip non-GP-practice records from gps.json / merged.json.

Why
---
When find_london_gp_gaps.py + build_gps_final.py paginated CQC to discover
"missing" GP practices, the filter was too loose. Dental practices,
orthodontists, private clinics, and address-only records (e.g.
"1-5 Orchard Road") slipped through. They're tagged as NHS but their
Register-with-NHS and NHS-profile buttons 404 because they're not in
NHS Digital's GMS practice register.

Cleanup signals (any one drops the record)
------------------------------------------
1. ODS code prefix:
   - V-codes (V*****) are NHS dental practitioners, not GPs.
   - Other suspect prefixes can be added here later.

2. Name patterns:
   - Dental, dentist, orthodontic, oral surgery → not a GP.
   - "Smile clinic / studio / centre" → dental cosmetic.
   - Names that are just an address ("1-5 Orchard Road", "28 Harley Place")
     → not really a practice name at all.

3. Name has none of the standard GP practice nouns AND no GPPS score AND
   no CQC rating → very likely not a real GMS GP.

Private clinic records (type=Private) are NOT touched — they live in a
different list with different rules.

Safety: refuses to drop more than 35% of NHS records in one pass.
"""

import json, re, sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON    = ROOT / "gps.json"
MERGED_JSON = ROOT / "merged.json"

# Field accessors (gps.json uses snake_case; merged.json compact form)
TYPE_FIELDS    = ["type"]
ODS_FIELDS     = ["ods_code", "o"]
NAME_FIELDS    = ["name", "n"]
RATING_FIELDS  = ["cqc_rating", "cqc"]
URL_FIELDS     = ["cqc_url", "cu"]
GPPS_FIELDS    = ["gpps_overall_pct", "s"]

def first(rec, fields):
    for f in fields:
        v = rec.get(f)
        if v not in (None, ""): return v
    return ""

def is_nhs(rec):  return (first(rec, TYPE_FIELDS) or "NHS") == "NHS"
def ods(rec):     return first(rec, ODS_FIELDS).strip().upper()
def name(rec):    return first(rec, NAME_FIELDS) or ""
def rating(rec):  return first(rec, RATING_FIELDS)
def cqc_url(rec): return first(rec, URL_FIELDS)
def gpps(rec):    return first(rec, GPPS_FIELDS)

# ODS code prefixes that are NEVER NHS GP practices.
# V = NHS dental practitioner organisations.
NON_GP_ODS_PREFIXES = {"V"}

# Practice ODS codes are 6 alphanumeric chars (letter + 5 digits typically).
# Anything else is suspect.
ODS_PRACTICE_RE = re.compile(r"^[A-Z]\d{5}$")

# Names that explicitly indicate the record is NOT a GP practice.
NON_GP_NAME_RE = re.compile(
    r"\b(?:"
    r"orthodont|dental|dentist|denture|oral\s+(?:health|surgery|hygien)|"
    r"smile\s+(?:clinic|studio|centre|center|practice|company|works)|"
    r"\bteeth\b|"
    r"veterinary|funeral|"
    r"pharmacy|chemist(?!\s+road)|drug\s+store|"
    r"optician|optometr|eye\s+(?:wear|laser)|"
    r"chiropract|osteopath|reflexolog|tattoo|piercing|"
    r"\bivf\b|fertility\s+clinic|cryob|sperm\s+bank|"
    r"slimming|weight\s+loss\s+clinic|laser\s+hair|aesthet|botox"
    r")\b",
    re.IGNORECASE,
)

# Practice-ish nouns. If a record has none of these AND no GPPS/CQC data,
# we treat it as non-GP.
GP_NOUN_RE = re.compile(
    r"\b(?:"
    r"surgery|surgeries|practice|"
    r"medical\s+(?:centre|center|practice|group|services|partners?|clinic)|"
    r"health(?:care)?\s+(?:centre|center|practice|hub)|"
    r"\bgp\b|general\s+practi|family\s+(?:practice|doctor|health)|"
    r"the\s+practice|\bdrs?\b|partnership|"
    r"primary\s+care|community\s+(?:health|medical)|"
    r"wellbeing|wellness"
    r")\b",
    re.IGNORECASE,
)

# Address-only names: number + street type, no practice noun.
ADDRESS_ONLY_RE = re.compile(
    r"^\s*\d+[a-z]?\s*[-–]?\s*\d*\s+(?:[A-Z][\w']+\s+){0,4}"
    r"(?:road|street|avenue|lane|way|drive|hill|place|park|gardens?|crescent|"
    r"close|walk|broadway|mews|terrace|grove|court)\s*$",
    re.IGNORECASE,
)

def classify(rec):
    """Return a reason string if we should drop, else None."""
    if not is_nhs(rec):
        return None  # private records get a different cleanup path
    o = ods(rec)
    n = name(rec)
    # Signal 1: ODS prefix
    if o and o[0] in NON_GP_ODS_PREFIXES:
        return f"ODS prefix '{o[0]}' (dental)"
    # Signal 2: ODS format
    if o and not ODS_PRACTICE_RE.match(o):
        return f"non-practice ODS format '{o}'"
    # Signal 3: explicit non-GP name
    if NON_GP_NAME_RE.search(n):
        return "non-GP name pattern"
    # Signal 4: address-only name (no GP noun)
    if ADDRESS_ONLY_RE.match(n) and not GP_NOUN_RE.search(n):
        return "address-only name"
    # Signal 5: no GP nouns AND no NHS evidence (rating/GPPS)
    if (not GP_NOUN_RE.search(n)
        and not rating(rec)
        and not gpps(rec)
        and not cqc_url(rec)):
        return "no GP noun + no NHS evidence"
    return None

def process(path):
    if not path.exists():
        print(f"  {path.name}: not found.")
        return
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        print(f"  {path.name}: not a list.")
        return

    kept = []
    dropped_reasons = Counter()
    sample = []
    nhs_before = sum(1 for r in data if is_nhs(r))

    for rec in data:
        reason = classify(rec)
        if reason:
            dropped_reasons[reason] += 1
            if len(sample) < 12:
                sample.append((ods(rec) or "?", name(rec)[:48], reason))
            continue
        kept.append(rec)

    nhs_after = sum(1 for r in kept if is_nhs(r))
    nhs_dropped = nhs_before - nhs_after

    # Safety: refuse if dropping >35%
    if nhs_before and nhs_dropped > nhs_before * 0.35:
        print(f"\n  {path.name}: ABORT — would drop {nhs_dropped}/{nhs_before} "
              "NHS records (>35%). Investigate before writing.")
        for reason, n in dropped_reasons.most_common():
            print(f"    {reason:40s} {n}")
        return

    path.write_text(json.dumps(kept, indent=2))
    print(f"\n  {path.name}: {len(data)} → {len(kept)} records "
          f"(dropped {len(data) - len(kept)})")
    print(f"  NHS practices: {nhs_before} → {nhs_after}")

    print("\n  Drop reasons:")
    for reason, n in dropped_reasons.most_common():
        print(f"    {reason:40s} {n}")

    if sample:
        print("\n  Sample dropped records:")
        for o, nm, reason in sample:
            print(f"    {o:8s} {nm:48s}  ({reason})")

def main():
    print("Cleaning non-GP records from NHS list…\n")
    for path in [GPS_JSON, MERGED_JSON]:
        process(path)
    print("\nDone. Re-run merge_into_dataset.py to rebuild index.html, "
          "then build_borough_pages.py + build_specialty_pages.py.")

if __name__ == "__main__":
    main()
