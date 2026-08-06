#!/usr/bin/env python3
"""
Generate dentists.json from cqc_london_cache.json.gz — no new API calls,
uses the same local cache gen_private_clinics.py already reads.

Outputs in the same snake_case shape merge_into_dataset.py expects, so it
plugs into the existing pipeline:
    dentists.json -> merge_into_dataset.py -> merged.json + pages

Classifies each dentist as:
  - "nhs"      if it accepts NHS patients (regulated activity says so, or
                the name/provider explicitly mentions NHS)
  - "private"  if nothing suggests NHS acceptance
  - "orthodontics" / "cosmetic-dentistry" added on top when the name signals it

Run: python3 gen_dentists.py
"""
import gzip, json, re
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cqc_london_cache.json.gz"
OUT = ROOT / "dentists.json"

try:
    DEAD_LINKS = set(json.loads((ROOT / "dead_links.json").read_text()))
except Exception:
    DEAD_LINKS = set()

NAME_PATTERNS = [
    ("orthodontics",       r"\b(?:orthodont|braces|invisalign)"),
    ("cosmetic-dentistry", r"\b(?:cosmetic\s+dent|smile\s+(?:clinic|design|studio)|"
                            r"teeth\s+whiten|veneer)"),
    ("implants",           r"\b(?:implant\s+(?:dent|centre|clinic)|dental\s+implant)"),
    ("paediatric-dentistry", r"\b(?:child(?:ren)?.?s?\s+dent|paediatric\s+dent|kids\s+dent)"),
    ("sedation-dentistry", r"\b(?:sedation|nervous\s+patient|dental\s+phobia)"),
]

NHS_HINT_RE = re.compile(r"\bnhs\b", re.I)

DOMAINY_RE = re.compile(r"^[a-z0-9][\w\-.]*\.[a-z]{2,}", re.IGNORECASE)

def normalize_url(u):
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

def is_dentist(rec):
    if rec.get("registrationStatus") == "Deregistered":
        return False
    return "Dentist" in rec.get("gacServiceTypes", [])

def classify(rec):
    text = " ".join([
        rec.get("name", ""),
        rec.get("providerName", ""),
        (rec.get("website", "") or "").replace("-", " ").replace(".", " "),
    ]).lower()

    tags = set()
    for key, pat in NAME_PATTERNS:
        if re.search(pat, text):
            tags.add(key)

    # NHS acceptance: regulated activities list, or explicit "NHS" mention.
    # CQC doesn't reliably flag NHS-contract status for dentists, so this is
    # a best-effort signal, not a guarantee — worth a methodology note.
    accepts_nhs = bool(NHS_HINT_RE.search(text)) or rec.get("hasNhsService", False)
    tags.add("nhs" if accepts_nhs else "private")
    return sorted(tags)

def display_name(rec):
    name = (rec.get("name") or "").strip()
    prov = (rec.get("providerName") or "").strip()
    if not prov or not name:
        return name
    looks_like_address = bool(re.match(r"^\d+[a-z]?\s*[-–]?\s*\d*\s+[A-Z]", name)) and not re.search(
        r"\b(dental|dentist|surgery|practice|clinic|centre|center)\b", name, re.IGNORECASE)
    if looks_like_address:
        if len(prov) > 50:
            prov = prov[:47] + "..."
        return f"{name} — {prov}"
    return name

def to_merge_shape(rec, tags):
    parts = [p for p in [rec.get("address1") or "", rec.get("address2") or "", rec.get("town") or ""] if p]
    return {
        "cqc_id":         rec.get("locationId", ""),
        "name":           display_name(rec),
        "address":        ", ".join(parts),
        "postcode":       rec.get("postcode", ""),
        "phone":          rec.get("phone", ""),
        "website": (lambda w: "" if w in DEAD_LINKS else w)(normalize_url(rec.get("website", ""))),
        "tags":           tags,
        "cqc_rating":     rec.get("currentRating", ""),
        "cqc_url":        rec.get("cqcUrl", ""),
        "localAuthority": rec.get("localAuthority", ""),
        "lat":            rec.get("lat"),
        "lon":            rec.get("lon"),
        "providerName":   rec.get("providerName", ""),
    }

def deduplicate(records):
    RATING_SCORE = {"Outstanding": 4, "Good": 3, "Requires improvement": 2, "Inadequate": 1, "": 0}
    best = {}
    for r in records:
        key = (r["name"].strip().lower(), r["postcode"].strip().upper())
        if not key[0] or not key[1]:
            continue
        score = (RATING_SCORE.get(r.get("cqc_rating", ""), 0), len(r.get("tags", [])))
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
    by_tag = Counter()
    by_borough = Counter()
    for rec in cache.values():
        if not is_dentist(rec): continue
        tags = classify(rec)
        shaped = to_merge_shape(rec, tags)
        output.append(shaped)
        for t in tags: by_tag[t] += 1
        by_borough[rec.get("localAuthority", "(unknown)")] += 1

    before = len(output)
    output = deduplicate(output)
    print(f"Deduplicated: {before:,} -> {len(output):,} ({before-len(output)} duplicates removed)")
    OUT.write_text(json.dumps(output, indent=2))
    print(f"\nWrote {len(output):,} dentists to {OUT.name}")
    print(f"\nBy tag:")
    for t, n in by_tag.most_common():
        print(f"  {t:25s} {n:4d}")
    print(f"\nBy borough (top 10):")
    for b, n in by_borough.most_common(10):
        print(f"  {b:30s} {n:4d}")

if __name__ == "__main__":
    main()
