#!/usr/bin/env python3
"""
merge_into_dataset.py
Merges private_clinics.json into index.html DATA array AND writes merged.json
that downstream page builders read.

Pipeline order:
    refresh_nhs_data.py     -> gps.json + NHS data into index.html DATA
    gen_private_clinics.py  -> private_clinics.json (from CQC cache)
    merge_into_dataset.py   -> updates index.html DATA + writes merged.json
    build_*_pages.py        -> read merged.json
"""
import json, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PRIVATE_JSON = ROOT / "private_clinics.json"
INDEX_HTML   = ROOT / "index.html"
MERGED_JSON  = ROOT / "merged.json"   # <-- downstream builders read this

BOROUGH_MAP = {
    'E1':'Tower Hamlets','E2':'Tower Hamlets','E3':'Tower Hamlets',
    'E4':'Waltham Forest','E5':'Hackney','E6':'Newham','E7':'Newham',
    'E8':'Hackney','E9':'Hackney','E10':'Waltham Forest',
    'E11':'Waltham Forest','E12':'Newham','E13':'Newham',
    'E14':'Tower Hamlets','E15':'Newham','E16':'Newham',
    'E17':'Waltham Forest','E18':'Redbridge','E20':'Newham',
    'EC1':'Islington','EC2':'City of London','EC3':'City of London','EC4':'City of London',
    'N1':'Islington','N2':'Barnet','N3':'Barnet','N4':'Haringey',
    'N5':'Islington','N6':'Haringey','N7':'Islington','N8':'Haringey',
    'N9':'Enfield','N10':'Haringey','N11':'Barnet','N12':'Barnet',
    'N13':'Enfield','N14':'Enfield','N15':'Haringey','N16':'Hackney',
    'N17':'Haringey','N18':'Enfield','N19':'Islington','N20':'Barnet',
    'N21':'Enfield','N22':'Haringey',
    'NW1':'Camden','NW2':'Brent','NW3':'Camden','NW4':'Barnet',
    'NW5':'Camden','NW6':'Brent','NW7':'Barnet','NW8':'Westminster',
    'NW9':'Brent','NW10':'Brent','NW11':'Barnet',
    'SE1':'Southwark','SE2':'Greenwich','SE3':'Greenwich','SE4':'Lewisham',
    'SE5':'Southwark','SE6':'Lewisham','SE7':'Greenwich','SE8':'Lewisham',
    'SE9':'Greenwich','SE10':'Greenwich','SE11':'Lambeth','SE12':'Lewisham',
    'SE13':'Lewisham','SE14':'Lewisham','SE15':'Southwark','SE16':'Southwark',
    'SE17':'Southwark','SE18':'Greenwich','SE19':'Bromley','SE20':'Bromley',
    'SE21':'Southwark','SE22':'Southwark','SE23':'Lewisham','SE24':'Lambeth',
    'SE25':'Croydon','SE26':'Lewisham','SE27':'Lambeth','SE28':'Greenwich',
    'SW1':'Westminster','SW2':'Lambeth','SW3':'Kensington & Chelsea',
    'SW4':'Lambeth','SW5':'Kensington & Chelsea','SW6':'Hammersmith & Fulham',
    'SW7':'Kensington & Chelsea','SW8':'Lambeth','SW9':'Lambeth',
    'SW10':'Kensington & Chelsea','SW11':'Wandsworth','SW12':'Wandsworth',
    'SW13':'Richmond','SW14':'Richmond','SW15':'Wandsworth','SW16':'Lambeth',
    'SW17':'Wandsworth','SW18':'Wandsworth','SW19':'Merton','SW20':'Merton',
    'W1':'Westminster','W2':'Westminster','W3':'Ealing','W4':'Hounslow',
    'W5':'Ealing','W6':'Hammersmith & Fulham','W7':'Ealing',
    'W8':'Kensington & Chelsea','W9':'Westminster','W10':'Kensington & Chelsea',
    'W11':'Kensington & Chelsea','W12':'Hammersmith & Fulham',
    'W13':'Ealing','W14':'Hammersmith & Fulham',
    'WC1':'Camden','WC2':'Westminster',
    'BR1':'Bromley','BR2':'Bromley','BR3':'Bromley','BR4':'Bromley',
    'BR5':'Bromley','BR6':'Bromley','BR7':'Bromley',
    'CR0':'Croydon','CR2':'Croydon','CR4':'Merton','CR5':'Croydon',
    'CR7':'Croydon','CR8':'Croydon',
    'DA1':'Bexley','DA5':'Bexley','DA6':'Bexley','DA7':'Bexley',
    'DA8':'Bexley','DA14':'Bexley','DA15':'Bexley','DA16':'Bexley','DA17':'Bexley',
    'EN1':'Enfield','EN2':'Enfield','EN3':'Enfield','EN4':'Barnet',
    'EN5':'Barnet','EN8':'Enfield','EN9':'Enfield',
    'HA0':'Brent','HA1':'Harrow','HA2':'Harrow','HA3':'Harrow',
    'HA4':'Hillingdon','HA5':'Harrow','HA6':'Hillingdon',
    'HA7':'Harrow','HA8':'Barnet','HA9':'Brent',
    'IG1':'Redbridge','IG2':'Redbridge','IG3':'Redbridge','IG4':'Redbridge',
    'IG5':'Redbridge','IG6':'Redbridge','IG7':'Redbridge','IG8':'Redbridge',
    'IG11':'Barking & Dagenham',
    'KT1':'Kingston','KT2':'Kingston','KT3':'Kingston','KT4':'Kingston',
    'KT5':'Kingston','KT6':'Kingston','KT7':'Kingston','KT8':'Richmond','KT9':'Kingston',
    'RM1':'Havering','RM2':'Havering','RM3':'Havering','RM5':'Havering',
    'RM6':'Barking & Dagenham','RM7':'Havering','RM8':'Barking & Dagenham',
    'RM9':'Barking & Dagenham','RM10':'Barking & Dagenham',
    'RM11':'Havering','RM12':'Havering','RM13':'Havering','RM14':'Havering',
    'SM1':'Sutton','SM2':'Sutton','SM3':'Sutton','SM4':'Merton',
    'SM5':'Sutton','SM6':'Sutton',
    'TW1':'Richmond','TW2':'Richmond','TW3':'Hounslow','TW4':'Hounslow',
    'TW5':'Hounslow','TW6':'Hounslow','TW7':'Hounslow','TW8':'Hounslow',
    'TW9':'Richmond','TW10':'Richmond','TW11':'Richmond','TW12':'Richmond',
    'TW13':'Hounslow','TW14':'Hounslow',
    'UB1':'Ealing','UB2':'Ealing','UB3':'Hillingdon','UB4':'Hillingdon',
    'UB5':'Ealing','UB6':'Ealing','UB7':'Hillingdon','UB8':'Hillingdon',
    'UB9':'Hillingdon','UB10':'Hillingdon',
}

def borough_from_postcode(pc):
    if not pc: return ""
    pc = pc.strip().upper()
    district = pc.split()[0] if ' ' in pc else re.match(r'([A-Z]+\d+[A-Z]?)', pc)
    if hasattr(district, 'group'):
        district = district.group(1)
    if district in BOROUGH_MAP:
        return BOROUGH_MAP[district]
    m = re.match(r'([A-Z]+\d+)', district or '')
    if m:
        return BOROUGH_MAP.get(m.group(1), "")
    return ""

def normalise_private(r):
    pc = (r.get("postcode") or "").strip()
    addr = r.get("address") or ""
    borough = borough_from_postcode(pc)
    specialties = r.get("specialties") or []
    spec_str = ", ".join(specialties)
    cqc_rating = r.get("cqc_rating") or ""
    return {
        "o":    r.get("cqc_id") or r.get("ods_code") or "",
        "n":    r.get("name") or "",
        "a":    addr,
        "p":    pc,
        "ph":   r.get("phone") or "",
        "s":    None,
        "c":    None,
        "pcn":  spec_str,
        "specs": specialties,   # <-- array form, used by JS for filter chips
        "cqc":  cqc_rating,
        "cu":   r.get("cqc_url") or "",
        "ar":   borough,
        "la":   None,
        "ln":   None,
        "type": "Private",
        "web":  r.get("website") or "",
    }

def main():
    if not PRIVATE_JSON.exists():
        sys.exit(f"ERROR: {PRIVATE_JSON} not found. Run gen_private_clinics.py first.")
    private_records = json.loads(PRIVATE_JSON.read_text())
    print(f"Loaded {len(private_records)} private records from {PRIVATE_JSON.name}")

    if not INDEX_HTML.exists():
        sys.exit(f"ERROR: {INDEX_HTML} not found.")
    html = INDEX_HTML.read_text(encoding="utf-8")

    data_start_marker = "const DATA = "
    data_start = html.find(data_start_marker)
    if data_start == -1:
        sys.exit("ERROR: Could not find 'const DATA = ' in index.html")
    arr_start = data_start + len(data_start_marker)
    depth = 0
    i = arr_start
    while i < len(html):
        if html[i] == '[':
            depth += 1
        elif html[i] == ']':
            depth -= 1
            if depth == 0:
                arr_end = i + 1
                break
        i += 1
    else:
        sys.exit("ERROR: Could not find end of DATA array in index.html")
    existing_json = html[arr_start:arr_end]

    try:
        existing = json.loads(existing_json)
    except json.JSONDecodeError as e:
        sys.exit(f"ERROR: Could not parse DATA array: {e}")
    nhs_records = [r for r in existing if r.get("type") != "Private"]
    # Ensure every NHS record explicitly has type="NHS" — page builders
    # may filter strictly and reject records with no type field.
    for r in nhs_records:
        if not r.get("type"):
            r["type"] = "NHS"
    print(f"  NHS records: {len(nhs_records)}")
    print(f"  Old Private records removed: {len(existing) - len(nhs_records)}")

    new_private = [normalise_private(r) for r in private_records]
    dropped = len(new_private) - len([r for r in new_private if r["n"] and r["ar"]])
    new_private = [r for r in new_private if r["n"] and r["ar"]]
    if dropped:
        print(f"  Dropped {dropped} private records (empty name or non-London "
              "postcode — fix_boroughs.py won't recover these)")
    print(f"  New Private records to merge: {len(new_private)}")

    merged = nhs_records + new_private
    print(f"  Total merged records: {len(merged)}")

    new_json_compact = json.dumps(merged, separators=(',', ':'), ensure_ascii=False)
    new_html = (
        html[:data_start]
        + data_start_marker
        + new_json_compact
        + html[arr_end:]
    )
    new_html = re.sub(
        r'(id="cntPriv">)\d+(</span>)',
        rf'\g<1>{len(new_private)}\g<2>',
        new_html
    )
    new_html = re.sub(
        r'(id="cntAll">)\d+(</span>)',
        rf'\g<1>{len(merged)}\g<2>',
        new_html
    )
    INDEX_HTML.write_text(new_html, encoding="utf-8")

    # NEW: write merged.json — downstream page builders read this
    MERGED_JSON.write_text(
        json.dumps(merged, indent=2, ensure_ascii=False),
        encoding="utf-8"
    )

    print(f"\n✅ Wrote {INDEX_HTML.name} and {MERGED_JSON.name}")
    print(f"   NHS:     {len(nhs_records)}")
    print(f"   Private: {len(new_private)}")
    print(f"   Total:   {len(merged)}")
    print(f"\nDownstream builders (build_borough_pages.py, build_specialty_pages.py, "
          "build_practice_pages.py) now read merged.json with both NHS and Private.")

if __name__ == "__main__":
    main()
