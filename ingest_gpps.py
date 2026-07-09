#!/usr/bin/env python3
"""
ingest_gpps.py — enrich gps.json with extra GP Patient Survey metrics.

Run LOCALLY once a year (gp-patient.co.uk blocks data-centre IPs, same as
NHS Digital's CDN — see rebuild_gps_json.py for the precedent):

  1. Download the practice-level weighted CSV from
     https://gp-patient.co.uk/downloads  (e.g. "Practice data (weighted)").
  2. Run:  python3 ingest_gpps.py <that-file.csv or .zip>
  3. Review the printed column matches, then commit the updated gps.json.

What it adds to each practice in gps.json (percentages, 1dp):
  gpps_trust_pct       — confidence and trust in the healthcare professional
  gpps_needs_met_pct   — needs met at last appointment
  gpps_reception_pct   — receptionists/staff helpful
  gpps_continuity_pct  — sees preferred healthcare professional when wanted

GPPS renames its column codes most years, so instead of hard-coding codes
this script scans the header row for keyword patterns and asks you to
confirm. If a metric can't be found it is skipped with a warning — adjust
the KEYWORDS mapping below and re-run.

refresh_nhs_data.py then carries these fields into merged.json/data.json
(compact keys t / nm / rc / ct) and build_practice_pages.py renders them.
"""
import csv, io, json, re, sys, zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

# metric -> (all-of-these-keyword-groups, field name in gps.json)
# A column matches if, lowercased, it contains at least one keyword from
# EVERY group. Tune these if GPPS rewords its headers.
KEYWORDS = {
    "gpps_trust_pct":      ([["confidence", "trust"]],                     ),
    "gpps_needs_met_pct":  ([["needs"], ["met"]],                          ),
    "gpps_reception_pct":  ([["reception"], ["helpful"]],                  ),
    "gpps_continuity_pct": ([["prefer"], ["gp", "healthcare professional"]],),
}
# Prefer the "% positive"-style summary column when several match.
POSITIVE_HINTS = ["%", "pct", "positive", "good"]

ODS_HINTS = ["practice_code", "practice code", "praccode", "ods"]


def read_rows(path):
    p = Path(path)
    if p.suffix.lower() == ".zip":
        with zipfile.ZipFile(p) as zf:
            name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            text = zf.read(name).decode("utf-8-sig", errors="replace")
    else:
        text = p.read_text(encoding="utf-8-sig", errors="replace")
    return list(csv.reader(io.StringIO(text)))


def find_column(header, groups):
    """Return index of the best-matching column, or None."""
    candidates = []
    for i, col in enumerate(header):
        c = col.lower()
        if all(any(kw in c for kw in group) for group in groups):
            candidates.append(i)
    if not candidates:
        return None
    # Prefer columns that look like a positive-percentage summary
    for i in candidates:
        c = header[i].lower()
        if any(h in c for h in POSITIVE_HINTS):
            return i
    return candidates[0]


def to_pct(value):
    """GPPS files store proportions as 0–1 or 0–100; normalise to 0–100."""
    try:
        v = float(str(value).strip().rstrip("%"))
    except (ValueError, TypeError):
        return None
    if v <= 1.0:
        v *= 100.0
    return round(v, 1) if 0 <= v <= 100 else None


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    rows = read_rows(sys.argv[1])
    header = rows[0]

    ods_idx = find_column(header, [ODS_HINTS])
    if ods_idx is None:
        sys.exit("Could not find the practice-code column. Header was:\n"
                 + ", ".join(header[:40]))

    col_map = {}
    print("Column matches (verify these look right):")
    print(f"  practice code            -> [{ods_idx}] {header[ods_idx]}")
    for field, (groups,) in KEYWORDS.items():
        idx = find_column(header, groups)
        if idx is None:
            print(f"  {field:24s} -> NOT FOUND (skipping — adjust KEYWORDS)")
        else:
            col_map[field] = idx
            print(f"  {field:24s} -> [{idx}] {header[idx]}")
    if not col_map:
        sys.exit("No metric columns found — adjust KEYWORDS and re-run.")

    metrics = {}
    for row in rows[1:]:
        if len(row) <= ods_idx:
            continue
        ods = row[ods_idx].strip().upper()
        if not re.match(r"^[A-Z]\d{5}$", ods):
            continue
        vals = {}
        for field, idx in col_map.items():
            if len(row) > idx:
                v = to_pct(row[idx])
                if v is not None:
                    vals[field] = v
        if vals:
            metrics[ods] = vals

    print(f"\nParsed metrics for {len(metrics)} practices.")

    gps = json.loads(GPS_JSON.read_text())
    updated = 0
    for rec in gps:
        vals = metrics.get((rec.get("ods_code") or "").upper())
        if vals:
            rec.update(vals)
            updated += 1
    GPS_JSON.write_text(json.dumps(gps, indent=2))
    print(f"Updated {updated}/{len(gps)} practices in gps.json — review, "
          f"commit, and let the daily refresh rebuild the site.")


if __name__ == "__main__":
    main()
