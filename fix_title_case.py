#!/usr/bin/env python3
"""
Fix title-case mistakes in gps.json names. Python's .title() treats every
non-alphanumeric character as a word boundary, so "silver's" comes out as
"Silver'S". We undo that for the common contraction suffixes:

  Silver'S → Silver's
  Don'T    → Don't
  I'D      → I'd     (etc.)
  Drs'     → Drs'    (unchanged — trailing apostrophe, no letter after)

Also fixes a few small typo-style cases we've seen:
  Mcdonalds  → McDonalds  (capital after "Mc")
  Of/And/The mid-word → keep lowercase
"""

import json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

# Contraction suffixes that should be lowercase after the apostrophe.
APOSTROPHE_SUFFIXES = ("S", "T", "D", "M", "Re", "Ve", "Ll", "S'")

def fix_apostrophes(s):
    # "Silver'S" → "Silver's", etc.
    s = re.sub(r"(\w)'([STDM])\b", lambda m: f"{m.group(1)}'{m.group(2).lower()}", s)
    s = re.sub(r"(\w)'(Re|Ve|Ll)\b", lambda m: f"{m.group(1)}'{m.group(2).lower()}", s)
    return s

def fix_mc(s):
    # "Mcdonalds" → "McDonalds", "Mckenzie" → "McKenzie"
    s = re.sub(r"\bMc([a-z])", lambda m: f"Mc{m.group(1).upper()}", s)
    s = re.sub(r"\bO'([a-z])", lambda m: f"O'{m.group(1).upper()}", s)
    return s

# Words that should stay lowercase in the middle of a multi-word name.
LOWERCASE_MIDWORDS = {"And", "Of", "The", "On", "In", "At", "By", "For",
                     "With", "A", "An"}

def fix_lowercase_midwords(s):
    parts = s.split()
    out = []
    for i, p in enumerate(parts):
        if i > 0 and i < len(parts) - 1 and p in LOWERCASE_MIDWORDS:
            out.append(p.lower())
        else:
            out.append(p)
    return " ".join(out)

def smart_title(s):
    if not s: return s
    s = fix_apostrophes(s)
    s = fix_mc(s)
    s = fix_lowercase_midwords(s)
    return s

def main():
    if not GPS_JSON.exists():
        sys.exit(f"{GPS_JSON} not found.")
    data = json.loads(GPS_JSON.read_text())
    if not isinstance(data, list):
        sys.exit("gps.json is not a JSON array.")
    print(f"Loaded {len(data)} records.")

    fixed = 0
    sample = []
    for r in data:
        original = r.get("name") or ""
        new = smart_title(original)
        if new != original:
            r["name"] = new
            fixed += 1
            if len(sample) < 15:
                sample.append((original, new))

    if sample:
        print(f"\nFixed {fixed} names. Sample:")
        for old, new in sample:
            print(f"  {old:50s}  →  {new}")
    else:
        print("\nNo name fixes needed.")

    GPS_JSON.write_text(json.dumps(data, indent=2))
    print(f"\nWrote {GPS_JSON}.")

if __name__ == "__main__":
    main()
