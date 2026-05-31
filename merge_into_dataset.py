#!/usr/bin/env python3
"""
Merge gps.json (NHS) + private_clinics.json (Private) into a single
combined dataset that the index.template.html expects.

Run AFTER refresh_nhs_data.py has produced merged.json.
"""

import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MERGED_JSON = ROOT / "merged.json"
PRIVATE_JSON = ROOT / "private_clinics.json"

BOROUGH_MAP = {
    "E10":"Waltham Forest","E11":"Redbridge","E12":"Newham","E13":"Newham",
    "E14":"Tower Hamlets","E15":"Newham","E16":"Newham","E17":"Waltham Forest",
    "E18":"Redbridge","E20":"Newham","E1":"Tower Hamlets","E2":"Tower Hamlets",
    "E3":"Tower Hamlets","E4":"Waltham Forest","E5":"Hackney","E6":"Newham",
    "E7":"Newham","E8":"Hackney","E9":"Hackney",
    "EC1A":"City of London","EC1M":"Islington","EC1N":"Camden","EC1R":"Islington",
    "EC1V":"Islington","EC1Y":"Islington","EC2A":"Hackney","EC2M":"City of London",
    "EC2N":"City of London","EC2R":"City of London","EC2V":"City of London",
    "EC2Y":"City of London","EC3A":"City of London","EC3M":"City of London",
    "EC3N":"City of London","EC3R":"City of London","EC3V":"City of London",
    "EC4A":"City of London","EC4M":"City of London","EC4N":"City of London",
    "EC4R":"City of London","EC4V":"City of London","EC4Y":"City of London",
    "N10":"Haringey","N11":"Barnet","N12":"Barnet","N13":"Enfield",
    "N14":"Enfield","N15":"Haringey","N16":"Hackney","N17":"Haringey",
    "N18":"Enfield","N19":"Islington","N20":"Barnet","N21":"Enfield","N22":"Haringey",
    "N1":"Islington","N4":"Hackney","N5":"Islington","N6":"Haringey",
    "N7":"Islington","N8":"Haringey","N9":"Enfield",
    "NW1":"Camden","NW2":"Brent","NW3":"Camden","NW4":"Barnet","NW5":"Camden",
    "NW6":"Brent","NW7":"Barnet","NW8":"Westminster","NW9":"Brent",
    "NW10":"Brent","NW11":"Barnet",
    "SE1":"Southwark","SE2":"Greenwich","SE3":"Greenwich","SE4":"Lewisham",
    "SE5":"Southwark","SE6":"Lewisham","SE7":"Greenwich","SE8":"Lewisham",
    "SE9":"Greenwich","SE10":"Greenwich","SE11":"Lambeth","SE12":"Lewisham",
    "SE13":"Lewisham","SE14":"Lewisham","SE15":"Southwark","SE16":"Southwark",
    "SE17":"Southwark","SE18":"Greenwich","SE19":"Bromley","SE20":"Bromley",
    "SE21":"Southwark","SE22":"Southwark","SE23":"Lewisham","SE24":"Lambeth",
    "SE25":"Croydon","SE26":"Lewisham","SE27":"Lambeth","SE28":"Greenwich",
    "SW1A":"Westminster","SW1E":"Westminster","SW1H":"Westminster","SW1P":"Westminster",
    "SW1V":"Westminster","SW1W":"Westminster","SW1X":"Westminster","SW1Y":"Westminster",
    "SW2":"Lambeth","SW3":"Kensington & Chelsea","SW4":"Lambeth",
    "SW5":"Kensington & Chelsea","SW6":"Hammersmith & Fulham",
    "SW7":"Kensington & Chelsea","SW8":"Lambeth","SW9":"Lambeth",
    "SW10":"Kensington & Chelsea","SW11":"Wandsworth","SW12":"Wandsworth",
    "SW13":"Richmond","SW14":"Richmond","SW15":"Wandsworth","SW16":"Lambeth",
    "SW17":"Wandsworth","SW18":"Wandsworth","SW19":"Merton","SW20":"Merton",
    "W1":"Westminster","W1A":"Westminster","W1B":"Westminster","W1C":"Westminster",
    "W1D":"Westminster","W1F":"Westminster","W1G":"Westminster","W1H":"Westminster",
    "W1J":"Westminster","W1K":"Westminster","W1S":"Westminster","W1T":"Westminster",
    "W1U":"Westminster","W1W":"Westminster",
    "W2":"Westminster","W3":"Ealing","W4":"Hounslow","W5":"Ealing",
    "W6":"Hammersmith & Fulham","W7":"Ealing","W8":"Kensington & Chelsea",
    "W9":"Westminster","W10":"Kensington & Chelsea","W11":"Kensington & Chelsea",
    "W12":"Hammersmith & Fulham","W13":"Ealing","W14":"Hammersmith & Fulham",
    "WC1A":"Camden","WC1B":"Camden","WC1E":"Camden","WC1H":"Camden",
    "WC1N":"Camden","WC1R":"Camden","WC1V":"Camden","WC1X":"Islington",
    "WC2A":"Camden","WC2B":"Westminster","WC2E":"Westminster","WC2H":"Westminster",
    "WC2N":"Westminster","WC2R":"Westminster",
    "BR1":"Bromley","BR2":"Bromley","BR3":"Bromley","BR4":"Bromley","BR5":"Bromley",
    "BR6":"Bromley","BR7":"Bromley","BR8":"Bromley",
    "CR0":"Croydon","CR2":"Croydon","CR3":"Croydon","CR4":"Merton","CR5":"Croydon",
    "CR6":"Croydon","CR7":"Croydon","CR8":"Croydon","CR9":"Croydon",
    "DA1":"Bexley","DA5":"Bexley","DA6":"Bexley","DA7":"Bexley","DA8":"Bexley",
    "DA14":"Bexley","DA15":"Bexley","DA16":"Bexley","DA17":"Bexley","DA18":"Bexley",
    "EN1":"Enfield","EN2":"Enfield","EN3":"Enfield","EN4":"Enfield","EN5":"Barnet",
    "EN7":"Enfield","EN8":"Enfield","EN9":"Enfield",
    "HA0":"Brent","HA1":"Harrow","HA2":"Harrow","HA3":"Harrow","HA4":"Hillingdon",
    "HA5":"Harrow","HA6":"Hillingdon","HA7":"Harrow","HA8":"Barnet","HA9":"Brent",
    "IG1":"Redbridge","IG2":"Redbridge","IG3":"Redbridge","IG4":"Redbridge",
    "IG5":"Redbridge","IG6":"Redbridge","IG7":"Redbridge","IG8":"Redbridge",
    "IG11":"Barking & Dagenham",
    "KT1":"Kingston","KT2":"Kingston","KT3":"Kingston","KT4":"Kingston","KT5":"Kingston",
    "KT6":"Kingston","KT7":"Kingston","KT8":"Richmond","KT9":"Kingston",
    "RM1":"Havering","RM2":"Havering","RM3":"Havering","RM4":"Havering","RM5":"Havering",
    "RM6":"Barking & Dagenham","RM7":"Havering","RM8":"Barking & Dagenham",
    "RM9":"Barking & Dagenham","RM10":"Barking & Dagenham","RM11":"Havering",
    "RM12":"Havering","RM13":"Havering","RM14":"Havering",
    "SM1":"Sutton","SM2":"Sutton","SM3":"Sutton","SM4":"Merton","SM5":"Sutton","SM6":"Sutton",
    "TW1":"Richmond","TW2":"Richmond","TW3":"Hounslow","TW4":"Hounslow","TW5":"Hounslow",
    "TW6":"Hillingdon","TW7":"Hounslow","TW8":"Hounslow","TW9":"Richmond","TW10":"Richmond",
    "TW11":"Richmond","TW12":"Richmond","TW13":"Hounslow","TW14":"Hounslow",
    "UB1":"Ealing","UB2":"Ealing","UB3":"Hillingdon","UB4":"Hillingdon","UB5":"Ealing",
    "UB6":"Ealing","UB7":"Hillingdon","UB8":"Hillingdon","UB9":"Hillingdon",
    "UB10":"Hillingdon","UB11":"Hillingdon",
}

def postcode_district(pc):
    pc = (pc or "").strip().upper()
    if " " in pc: return pc.split()[0]
    return pc[:-3] if len(pc) >= 5 else pc

def borough_for(pc):
    return BOROUGH_MAP.get(postcode_district(pc), "")

def main():
    if not MERGED_JSON.exists():
        sys.exit(f"{MERGED_JSON} not found. Run refresh_nhs_data.py first.")
    merged = json.loads(MERGED_JSON.read_text())
    if not isinstance(merged, list):
        sys.exit("merged.json is not a JSON array.")
    print(f"Loaded {len(merged)} NHS records from merged.json.")

    for r in merged:
        r.setdefault("type", "NHS")

    if not PRIVATE_JSON.exists():
        print(f"No {PRIVATE_JSON} — skipping private merge.")
        MERGED_JSON.write_text(json.dumps(merged, indent=2))
        return

    private = json.loads(PRIVATE_JSON.read_text())
    if not isinstance(private, list):
        sys.exit("private_clinics.json is not a JSON array.")
    print(f"Loaded {len(private)} private records.")

    converted = []
    for r in private:
        pc = (r.get("postcode") or "").strip().upper()
        converted.append({
            "n":     r.get("name", ""),
            "a":     r.get("address", ""),
            "p":     pc,
            "ph":    r.get("phone", ""),
            "cqc":   r.get("cqc_rating", ""),
            "cu":    r.get("cqc_url", ""),
            "ar":    borough_for(pc),
            "o":     r.get("ods_code", "") or r.get("cqc_id", ""),
            "type":  "Private",
            "specs": r.get("specialties", []),
            "web":   r.get("website", ""),
        })

    combined = merged + converted
    MERGED_JSON.write_text(json.dumps(combined, indent=2))
    print(f"Wrote merged.json — {len(combined)} total "
          f"({len(merged)} NHS + {len(converted)} Private).")

if __name__ == "__main__":
    main()
