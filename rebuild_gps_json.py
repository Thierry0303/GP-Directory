#!/usr/bin/env python3
"""
Build gps.json from ePraccur — run once, on your own machine.

Why local? NHS Digital's CDN (files.digital.nhs.uk) blocks data-centre IPs
(GitHub Actions, AWS, etc.) with HTTP 403. From your home internet
connection it works fine. So: do the rebuild once locally, commit the
result, never run it in CI again.

Usage
-----
1. Download ePraccur in your browser (right-click → Save As):
       https://files.digital.nhs.uk/assets/ods/current/epraccur.zip
   Save it next to this script.

2. Run:
       python3 build_gps_locally.py epraccur.zip

3. The script writes `gps.json` next to itself. Replace your repo's
   gps.json with this file, commit, push.

That's it. The next scheduled NHS refresh will enrich CQC ratings on top
of the new master list.

If you want to merge with an existing gps.json (preserving GPPS scores
for practices that haven't changed), pass `--merge ./old_gps.json`.
"""

import argparse, csv, io, json, re, sys, zipfile
from collections import Counter
from pathlib import Path

EPRACCUR_COLS = [
    "Code", "Name", "NationalGrouping", "HighLevelHealthGeography",
    "AddressLine1", "AddressLine2", "AddressLine3", "AddressLine4", "AddressLine5",
    "Postcode", "OpenDate", "CloseDate", "Status", "OrgSubTypeCode",
    "Commissioner", "JoinProviderDate", "LeftProviderDate", "ContactTelephoneNumber",
]

# Inner + Outer London postcode districts.
LONDON_PREFIXES = {
    "EC1A","EC1R","EC1V","EC2A","WC1B","WC1E","WC1N","WC1X","WC2A","WC2B","WC2H","WC2N",
    "E1","E2","E3","E4","E5","E6","E7","E8","E9","E10","E11","E12","E13","E14","E15",
    "E16","E17","E18","E20",
    "N1","N4","N5","N6","N7","N8","N9","N10","N11","N12","N13","N14","N15","N16",
    "N17","N18","N19","N20","N21","N22",
    "NW1","NW2","NW3","NW4","NW5","NW6","NW7","NW8","NW9","NW10","NW11",
    "SE1","SE2","SE3","SE4","SE5","SE6","SE7","SE8","SE9","SE10","SE11","SE12",
    "SE13","SE14","SE15","SE16","SE17","SE18","SE19","SE20","SE21","SE22","SE23",
    "SE24","SE25","SE26","SE27","SE28",
    "SW1A","SW1E","SW1P","SW1V","SW1W","SW1X","SW2","SW3","SW4","SW5","SW6","SW7",
    "SW8","SW9","SW10","SW11","SW12","SW13","SW14","SW15","SW16","SW17","SW18",
    "SW19","SW20",
    "W1","W2","W3","W4","W5","W6","W7","W8","W9","W10","W11","W12","W13","W14",
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
    if " " in pc:
        return pc.split()[0]
    pc = pc.replace(" ", "")
    return pc[:-3] if len(pc) >= 5 else pc

def is_london(pc):
    d = postcode_district(pc)
    if d in LONDON_PREFIXES:
        return True
    m = re.match(r"^([A-Z]{1,2}\d)", d)
    return bool(m and m.group(1) in LONDON_PREFIXES)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("zipfile", help="Path to epraccur.zip you downloaded.")
    ap.add_argument("--merge", help="Optional path to existing gps.json — "
                                    "phone/CQC/GPPS fields will be preserved "
                                    "by ODS code where the practice still exists.")
    ap.add_argument("--out", default="gps.json", help="Output path (default: gps.json).")
    args = ap.parse_args()

    zip_path = Path(args.zipfile)
    if not zip_path.exists():
        sys.exit(f"Can't find {zip_path}. Download from "
                 "https://files.digital.nhs.uk/assets/ods/current/epraccur.zip")

    # Load existing gps.json to preserve GPPS scores etc.
    existing = {}
    if args.merge:
        try:
            for d in json.loads(Path(args.merge).read_text()):
                code = (d.get("ods_code") or "").upper()
                if code: existing[code] = d
            print(f"Loaded {len(existing)} records from {args.merge} for merge.")
        except Exception as e:
            print(f"WARN: couldn't merge from {args.merge}: {e}")

    # Parse ePraccur.
    print(f"Reading {zip_path}…")
    with zipfile.ZipFile(zip_path) as zf:
        csv_name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(csv_name) as f:
            text = f.read().decode("utf-8", errors="replace")

    london = []
    total = 0
    active = 0
    for row in csv.reader(io.StringIO(text)):
        total += 1
        if len(row) < 13:
            continue
        rec = dict(zip(EPRACCUR_COLS, row + [""] * (len(EPRACCUR_COLS) - len(row))))
        if rec.get("Status", "").strip().upper() != "A":
            continue
        active += 1
        pc = rec.get("Postcode", "").strip().upper()
        if not is_london(pc):
            continue
        ods = rec.get("Code", "").strip().upper()
        old = existing.get(ods, {})

        addr_lines = [rec.get(f"AddressLine{i}", "").strip()
                      for i in range(1, 6)]
        addr = ", ".join(filter(None, addr_lines))
        if addr.isupper():
            addr = addr.title()
        raw_name = rec.get("Name", "").strip()
        name = raw_name.title() if raw_name.isupper() else raw_name

        london.append({
            "ods_code":         ods,
            "name":             name,
            "address":          addr,
            "postcode":         pc,
            "phone":            rec.get("ContactTelephoneNumber", "").strip()
                                or old.get("phone", ""),
            "cqc_rating":       old.get("cqc_rating", ""),
            "cqc_url":          old.get("cqc_url", ""),
            "gpps_overall_pct": old.get("gpps_overall_pct"),
            "gpps_contact_pct": old.get("gpps_contact_pct"),
            "gpps_pcn":         old.get("gpps_pcn", ""),
        })

    if not london:
        sys.exit("ABORT: no London GPs parsed — check the input zip is correct.")

    Path(args.out).write_text(json.dumps(london, indent=2))
    size_kb = Path(args.out).stat().st_size // 1024
    print(f"\nRead {total} rows, {active} active practices nationally.")
    print(f"Wrote {args.out}: {len(london)} London GPs, {size_kb} KB.")

    # Coverage summary
    by_area = Counter()
    for r in london:
        m = re.match(r"^([A-Z]+)", r["postcode"])
        if m: by_area[m.group(1)] += 1
    outer = {"BR","CR","DA","EN","HA","IG","KT","RM","SM","TW","UB"}
    print("\nCoverage by postcode area:")
    for area, n in sorted(by_area.items(), key=lambda x: -x[1]):
        flag = "  <-- outer London" if area in outer else ""
        print(f"  {area:4s} {n}{flag}")

    if "TW" not in by_area:
        print("\n⚠️  No TW practices found — that's the bug we set out to fix. "
              "Check the input zip is the latest ePraccur.")
    else:
        print(f"\n✅ Twickenham/Richmond (TW): {by_area['TW']} practices.")

if __name__ == "__main__":
    main()
