#!/usr/bin/env python3
"""
extract_gpps_scores.py — shrink the big GP Patient Survey practice CSV down to
a tiny file that can be uploaded.

The full "Practice data (weighted)" CSV from gp-patient.co.uk is ~97 MB with
1,360 columns. This reads it and writes a small CSV with just the two scores
the directory shows on each practice card:

    practice_code, gpps_overall_pct, gpps_contact_pct, gpps_responses

  * gpps_overall_pct  = "Overall experience of GP practice"     (% good)
  * gpps_contact_pct  = "Overall experience of contacting the practice" (% good)

Both are the top-two-box positive score (Very good + Good), matching how the
existing values in the site were calculated.

USAGE (needs Python 3 — already on most machines; on Windows try `py` instead
of `python3`):

    python3 extract_gpps_scores.py "GPPS_2026_Practice_data_(weighted)_(csv)_PUBLIC.csv"

It writes  gpps_slim.csv  next to this script. Upload that file back.
"""
import csv, re, sys
from pathlib import Path

# GPPS question stems -> output field. Score = (option1 + option2).pct * 100.
OVERALL_STEM = "overallexp"          # overall experience of the practice
CONTACT_STEM = "gpcontactoverall"    # overall experience of contacting them
PRACTICE_CODE_RE = re.compile(r"^[A-Z]\d{5}$")


def col_index(header, name):
    try:
        return header.index(name)
    except ValueError:
        return None


def pct_positive(row, i1, i2):
    """Top-two-box positive percentage from two proportion (0-1) columns."""
    try:
        v = float(row[i1]) + float(row[i2])
    except (ValueError, TypeError, IndexError):
        return ""
    return round(v * 100, 1) if 0 <= v <= 1.0001 else ""


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    src = Path(sys.argv[1])
    if not src.exists():
        sys.exit(f"File not found: {src}")

    with src.open(newline="", encoding="utf-8-sig", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader)

        ov1 = col_index(header, f"{OVERALL_STEM}_1.pct")
        ov2 = col_index(header, f"{OVERALL_STEM}_2.pct")
        ct1 = col_index(header, f"{CONTACT_STEM}_1.pct")
        ct2 = col_index(header, f"{CONTACT_STEM}_2.pct")
        resp = col_index(header, f"{OVERALL_STEM}.counteval")
        if None in (ov1, ov2, ct1, ct2):
            sys.exit("Could not find the expected GPPS columns "
                     f"({OVERALL_STEM}_1.pct etc). Is this the *Practice data "
                     "(weighted)* CSV? Header had %d columns." % len(header))

        # Auto-detect the practice-code column: the one whose values look like
        # an ODS practice code (e.g. E83004). Sample the first data rows.
        sample = []
        for _, r in zip(range(200), reader):
            sample.append(r)
        code_idx = None
        best = -1
        for i in range(len(header)):
            hits = sum(1 for r in sample
                       if i < len(r) and PRACTICE_CODE_RE.match(r[i].strip().upper()))
            if hits > best:
                best, code_idx = hits, i
        if best <= 0:
            sys.exit("Could not find a practice-code column (values like E83004).")

        out_rows = []
        for r in sample + list(reader):
            if code_idx >= len(r):
                continue
            code = r[code_idx].strip().upper()
            if not PRACTICE_CODE_RE.match(code):
                continue
            ov = pct_positive(r, ov1, ov2)
            ct = pct_positive(r, ct1, ct2)
            responses = ""
            if resp is not None and resp < len(r):
                try:
                    responses = round(float(r[resp]))
                except (ValueError, TypeError):
                    responses = ""
            out_rows.append((code, ov, ct, responses))

    # De-duplicate on practice code, keep the first occurrence.
    seen = set()
    deduped = []
    for row in out_rows:
        if row[0] in seen:
            continue
        seen.add(row[0])
        deduped.append(row)

    out = Path(__file__).resolve().parent / "gpps_slim.csv"
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["practice_code", "gpps_overall_pct",
                    "gpps_contact_pct", "gpps_responses"])
        w.writerows(deduped)

    with_score = sum(1 for r in deduped if r[1] != "")
    print(f"Read practice code column [{code_idx}] = {header[code_idx]!r}")
    print(f"Wrote {out} — {len(deduped)} practices "
          f"({with_score} with an overall score).")
    print("Upload gpps_slim.csv back to the chat.")


if __name__ == "__main__":
    main()
