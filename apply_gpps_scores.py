#!/usr/bin/env python3
"""
apply_gpps_scores.py — write GP Patient Survey scores into gps.json.

Reads gpps_slim.csv (practice_code, gpps_overall_pct, gpps_contact_pct,
gpps_responses) and sets those fields on the matching practice in gps.json,
keyed by ODS practice code. refresh_nhs_data.py then carries the scores into
merged.json / data.json as the "s" (satisfaction) and "c" (contact ease)
fields, and the page builders render them.

This runs automatically at the start of the daily refresh (see
.github/workflows/full-refresh.yml), so the committed gpps_slim.csv is the
single source of truth for NHS patient-survey scores. It is idempotent: on a
normal day nothing changes; gps.json only changes when gpps_slim.csv is
updated.

--- Updating the scores (once a year, when GPPS publishes) ---
The GP Patient Survey blocks data-centre IPs, so the raw file can't be fetched
from CI — do this step locally:

  1. Download "Practice data (weighted)" (CSV) from https://gp-patient.co.uk/downloads
  2. python3 extract_gpps_scores.py "GPPS_20XX_Practice_data_(weighted)_(csv)_PUBLIC.csv"
  3. Commit the regenerated gpps_slim.csv. The next refresh applies it.
"""
import csv, json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SLIM = ROOT / "gpps_slim.csv"
GPS_JSON = ROOT / "gps.json"


def load_slim():
    scores = {}
    with SLIM.open(newline="", encoding="utf-8-sig", errors="replace") as f:
        for r in csv.DictReader(f):
            code = (r.get("practice_code") or "").strip().upper()
            if not code:
                continue
            def num(key):
                v = (r.get(key) or "").strip()
                try:
                    return float(v)
                except ValueError:
                    return None
            scores[code] = {
                "gpps_overall_pct": num("gpps_overall_pct"),
                "gpps_contact_pct": num("gpps_contact_pct"),
                "gpps_responses": num("gpps_responses"),
            }
    return scores


def main():
    if not SLIM.exists():
        print(f"{SLIM.name} not found — skipping GPPS score apply.")
        return
    if not GPS_JSON.exists():
        print(f"{GPS_JSON.name} not found — skipping GPPS score apply.")
        return

    scores = load_slim()
    gps = json.loads(GPS_JSON.read_text())

    matched = changed = 0
    for rec in gps:
        s = scores.get((rec.get("ods_code") or "").upper())
        if not s or s["gpps_overall_pct"] is None:
            continue
        matched += 1
        new = {
            "gpps_overall_pct": s["gpps_overall_pct"],
            "gpps_contact_pct": s["gpps_contact_pct"],
        }
        if s["gpps_responses"] is not None:
            new["gpps_responses"] = int(s["gpps_responses"])
        if any(rec.get(k) != v for k, v in new.items()):
            changed += 1
        rec.update(new)

    with_score = sum(1 for r in gps if r.get("gpps_overall_pct") not in (None, ""))
    if changed:
        GPS_JSON.write_text(json.dumps(gps, indent=2))
    print(f"GPPS scores: matched {matched}, changed {changed}; "
          f"{with_score}/{len(gps)} practices now have a score.")


if __name__ == "__main__":
    main()
