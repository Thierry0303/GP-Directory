#!/usr/bin/env python3
"""
Make sure every `website` / `web` URL stored across our JSON files starts
with a protocol — otherwise the browser treats them as relative paths and
turns `www.ct-dent.co.uk` into `londongp.directory/www.ct-dent.co.uk`.

Cleans:
  - leading/trailing whitespace
  - mailto: / tel: prefixes (these aren't websites)
  - prepends https:// when missing
  - drops anything that doesn't look like a URL at all
"""

import json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
FILES = [ROOT / "private_clinics.json", ROOT / "merged.json"]

URL_FIELDS = ["website", "web"]

# Anything that looks like a credible website — domain + dot + tld
DOMAINY_RE = re.compile(r"^[a-z0-9][\w\-.]*\.[a-z]{2,}", re.IGNORECASE)

def normalise(url):
    if not url: return ""
    url = url.strip()
    if not url: return ""
    low = url.lower()
    # Drop non-website prefixes
    if low.startswith(("mailto:", "tel:", "fax:", "sms:")):
        return ""
    # Already has protocol? Leave it
    if low.startswith(("http://", "https://")):
        return url
    # Looks like a bare domain → prepend https://
    if DOMAINY_RE.match(url):
        return "https://" + url
    # Anything else (garbage, just a phone number etc.) — drop
    return ""

def process(path):
    if not path.exists():
        print(f"  {path.name}: not found, skipping.")
        return
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        print(f"  {path.name}: not a list, skipping.")
        return
    fixed = 0
    dropped = 0
    for rec in data:
        for f in URL_FIELDS:
            if f not in rec: continue
            old = rec[f]
            new = normalise(old)
            if new != old:
                rec[f] = new
                if new == "": dropped += 1
                else: fixed += 1
    path.write_text(json.dumps(data, indent=2))
    print(f"  {path.name}: {fixed} URLs prefixed with https://, "
          f"{dropped} garbage URLs cleared")

def main():
    print("Normalising website URLs across JSON files…")
    for p in FILES:
        process(p)
    print("\nDone. Re-run merge_into_dataset.py if you want the homepage "
          "to pick up the changes immediately.")

if __name__ == "__main__":
    main()
