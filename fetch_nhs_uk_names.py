#!/usr/bin/env python3
"""
Replace the NHS-contract name on each gps.json record with the colloquial
name that NHS.uk uses on its public GP surgery profile pages.

Why
---
NHS contract names (from ePraccur / CQC) are often the doctor's name —
"S H Vaghela & Dr V N Patel". The NHS.uk page for the same ODS code uses
the common name everyone in the area knows — "The Old Surgery". The
common name is the only one users actually recognise.

Approach
--------
For each ODS code we fetch:

    https://www.nhs.uk/services/gp-surgery/-/{ODS}

NHS.uk redirects/renders the practice page; the `<h1>` text is the
colloquial name. We extract it.

Records get:
  name           ← the NHS.uk colloquial name
  official_name  ← preserved original contract name (already set if a
                   previous normalize step ran)

Practices that NHS.uk doesn't know about (private / non-GMS / closed)
are left untouched.

Runtime: ~5-10 minutes with 20 parallel workers and short timeouts.
"""

import json, re, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"

NHS_PROFILE_URL = "https://www.nhs.uk/services/gp-surgery/-/{ods}"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (X11; Linux x86_64; rv:128.0) "
                   "Gecko/20100101 Firefox/128.0"),
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-GB,en;q=0.9",
}

# Match the FIRST <h1>...</h1>. NHS.uk pages have one h1 = practice name.
H1_RE = re.compile(r"<h1\b[^>]*>(.*?)</h1>", re.IGNORECASE | re.DOTALL)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")

def strip_html(s):
    s = TAG_RE.sub("", s)
    s = (s.replace("&amp;", "&").replace("&nbsp;", " ")
           .replace("&#39;", "'").replace("&apos;", "'")
           .replace("&quot;", '"').replace("&lt;", "<").replace("&gt;", ">"))
    return WS_RE.sub(" ", s).strip()

def fetch_nhs_uk_name(ods, timeout=8):
    """Return the colloquial name from NHS.uk for this ODS code, or None."""
    url = NHS_PROFILE_URL.format(ods=ods)
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            html = r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        if e.code in (404, 410):
            return ("not-found", None)
        return ("http-error", e.code)
    except Exception as e:
        return ("error", str(e)[:40])

    m = H1_RE.search(html)
    if not m:
        return ("no-h1", None)
    name = strip_html(m.group(1))
    if not name or len(name) > 120:
        return ("bad-name", name[:50] if name else None)
    # NHS.uk uses some generic placeholders for unknown practices.
    if name.lower() in {"find a gp", "gp surgery", "not found", "search"}:
        return ("placeholder", name)
    return ("ok", name)

def main():
    if not GPS_JSON.exists():
        sys.exit(f"{GPS_JSON} not found.")
    data = json.loads(GPS_JSON.read_text())
    if not isinstance(data, list):
        sys.exit("gps.json is not a JSON array.")

    print(f"Loaded {len(data)} records.")
    ods_codes = [(i, (r.get("ods_code") or "").strip().upper())
                 for i, r in enumerate(data)]
    ods_codes = [(i, c) for i, c in ods_codes if c]
    print(f"Looking up {len(ods_codes)} ODS codes on NHS.uk…\n")

    status_counts = Counter()
    sample_renames = []
    renamed = 0
    skipped = 0
    failed = 0

    with ThreadPoolExecutor(max_workers=20) as pool:
        futures = {pool.submit(fetch_nhs_uk_name, c): (i, c) for i, c in ods_codes}
        done = 0
        for fut in as_completed(futures):
            i, ods = futures[fut]
            done += 1
            try:
                status, name = fut.result()
            except Exception as e:
                status, name = "exception", str(e)[:40]
            status_counts[status] += 1

            if status == "ok" and name:
                old = (data[i].get("name") or "").strip()
                if name and name != old:
                    if "official_name" not in data[i]:
                        data[i]["official_name"] = old
                    data[i]["name"] = name
                    renamed += 1
                    if len(sample_renames) < 20:
                        sample_renames.append((ods, old, name))
            elif status in ("not-found", "placeholder"):
                skipped += 1
            else:
                failed += 1

            if done % 100 == 0 or done == len(ods_codes):
                print(f"  {done}/{len(ods_codes)} done — "
                      f"renamed {renamed}, skipped {skipped}, failed {failed}")

    print(f"\nStatus counts:")
    for s, n in status_counts.most_common():
        print(f"  {s:15s} {n}")

    if sample_renames:
        print("\nSample renames:")
        for ods, old, new in sample_renames:
            print(f"  {ods:8s} {old[:35]:35s}  →  {new}")

    GPS_JSON.write_text(json.dumps(data, indent=2))
    print(f"\nWrote {GPS_JSON} — {renamed} records renamed.")

if __name__ == "__main__":
    main()
