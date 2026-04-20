#!/usr/bin/env python3
import json, re, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

print("Loading gps.json...")
with open("gps.json") as f:
    BASE_DATA = json.load(f)

# Filter to genuine GP practices only
# Keep: has GPPS score, has CQC rating, or standard GP ODS prefix
GP_PREFIXES = ('E83','E84','E85','E86','E87','F83','F84','F85','F86',
               'G83','G84','G85','H83','H84','H85')
BASE_DATA = [d for d in BASE_DATA if
    d.get('gpps_overall_pct') or
    d.get('cqc_rating') or
    d.get('ods_code','')[:3] in GP_PREFIXES]

base_by_ods = {d["ods_code"]: d for d in BASE_DATA}
ods_codes = list(base_by_ods.keys())
print(f"  {len(ods_codes)} genuine GP practices")

def fetch(ods):
    url = (f"https://directory.spineservices.nhs.uk/STU3/Organization"
           f"?identifier=https%3A%2F%2Ffhir.nhs.uk%2FId%2Fods-organization-code%7C{ods}"
           f"&_format=json")
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read())
            entries = data.get("entry", [])
            if not entries: return ods, None
            res = entries[0].get("resource", {})
            if not res.get("active", True): return ods, {"inactive": True}
            tc = res.get("telecom", [])
            phone = next((t.get("value","") for t in tc if t.get("system")=="phone"), "")
            addrs = res.get("address", [])
            addr = addrs[0] if addrs else {}
            postcode = addr.get("postalCode","").strip()
            raw_name = res.get("name","")
            name = raw_name.title() if raw_name.isupper() else raw_name
            lines = addr.get("line",[])
            city = addr.get("city","")
            address = ", ".join(filter(None, lines + ([city] if city else [])))
            address = address.title() if address.isupper() else address
            return ods, {"name":name,"phone":phone,"address":address,"postcode":postcode}
    except Exception:
        return ods, None

print(f"Fetching from NHS ODS API (20 concurrent)...")
results = {}
done = 0
with ThreadPoolExecutor(max_workers=20) as ex:
    futures = {ex.submit(fetch, ods): ods for ods in ods_codes}
    for future in as_completed(futures):
        ods, result = future.result()
        results[ods] = result
        done += 1
        if done % 100 == 0 or done == len(ods_codes):
            ok = sum(1 for v in results.values() if v and not v.get("inactive"))
            print(f"  {done}/{len(ods_codes)} done, {ok} ok")

PC = {
  "EC1A":(51.5193,-0.1010),"EC1R":(51.5238,-0.1082),"EC1V":(51.5265,-0.0907),
  "EC2A":(51.5221,-0.0829),"WC1B":(51.5208,-0.1267),"WC1E":(51.5242,-0.1321),
  "WC1N":(51.5234,-0.1183),"WC1X":(51.5267,-0.1118),"WC2A":(51.5145,-0.1149),
  "WC2B":(51.5148,-0.1236),"WC2H":(51.5131,-0.1284),"WC2N":(51.5091,-0.1265),
  "E1":(51.5157,-0.0706),"E2":(51.5281,-0.0614),"E3":(51.5300,-0.0186),
  "E4":(51.6294,-0.0028),"E5":(51.5621,-0.0527),"E6":(51.5377,0.0501),
  "E7":(51.5490,0.0247),"E8":(51.5453,-0.0644),"E9":(51.5427,-0.0407),
  "E10":(51.5690,-0.0074),"E11":(51.5688,0.0135),"E12":(51.5542,0.0534),
  "E13":(51.5303,0.0340),"E14":(51.5051,-0.0235),"E15":(51.5413,0.0052),
  "E16":(51.5091,0.0266),"E17":(51.5889,-0.0198),"E18":(51.5922,0.0286),
  "E20":(51.5456,-0.0164),
  "N1":(51.5375,-0.1036),"N4":(51.5703,-0.0984),"N5":(51.5570,-0.0978),
  "N6":(51.5741,-0.1490),"N7":(51.5545,-0.1167),"N8":(51.5880,-0.1089),
  "N9":(51.6263,-0.0622),"N10":(51.5999,-0.1457),"N11":(51.6070,-0.1449),
  "N12":(51.6106,-0.1762),"N13":(51.6227,-0.1031),"N14":(51.6342,-0.1186),
  "N15":(51.5874,-0.0831),"N16":(51.5650,-0.0791),"N17":(51.5982,-0.0700),
  "N18":(51.6127,-0.0596),"N19":(51.5660,-0.1310),"N20":(51.6262,-0.1696),
  "N21":(51.6358,-0.0923),"N22":(51.6010,-0.1106),
  "NW1":(51.5342,-0.1437),"NW2":(51.5564,-0.2129),"NW3":(51.5543,-0.1731),
  "NW4":(51.5879,-0.2250),"NW5":(51.5540,-0.1427),"NW6":(51.5414,-0.2041),
  "NW7":(51.6145,-0.2422),"NW8":(51.5311,-0.1704),"NW9":(51.5922,-0.2504),
  "NW10":(51.5363,-0.2552),"NW11":(51.5792,-0.1999),
  "SE1":(51.5014,-0.0948),"SE2":(51.4920,0.1110),"SE3":(51.4759,0.0182),
  "SE4":(51.4623,-0.0352),"SE5":(51.4763,-0.0863),"SE6":(51.4489,-0.0224),
  "SE7":(51.4840,0.0598),"SE8":(51.4771,-0.0329),"SE9":(51.4540,0.0598),
  "SE10":(51.4792,-0.0108),"SE11":(51.4885,-0.1068),"SE12":(51.4553,0.0044),
  "SE13":(51.4590,-0.0159),"SE14":(51.4764,-0.0471),"SE15":(51.4707,-0.0607),
  "SE16":(51.4977,-0.0524),"SE17":(51.4899,-0.0934),"SE18":(51.4892,0.0716),
  "SE19":(51.4148,-0.0830),"SE20":(51.4116,-0.0566),"SE21":(51.4419,-0.0817),
  "SE22":(51.4523,-0.0607),"SE23":(51.4399,-0.0378),"SE24":(51.4541,-0.0992),
  "SE25":(51.4023,-0.0605),"SE26":(51.4267,-0.0399),"SE27":(51.4359,-0.1049),
  "SE28":(51.5019,0.1066),
  "SW1A":(51.5034,-0.1276),"SW1E":(51.4970,-0.1355),"SW1P":(51.4951,-0.1317),
  "SW1V":(51.4894,-0.1430),"SW1W":(51.4927,-0.1509),"SW1X":(51.4978,-0.1588),
  "SW2":(51.4538,-0.1159),"SW3":(51.4859,-0.1694),"SW4":(51.4627,-0.1434),
  "SW5":(51.4888,-0.1936),"SW6":(51.4711,-0.1938),"SW7":(51.4940,-0.1792),
  "SW8":(51.4769,-0.1283),"SW9":(51.4703,-0.1120),"SW10":(51.4818,-0.1840),
  "SW11":(51.4640,-0.1654),"SW12":(51.4518,-0.1497),"SW13":(51.4813,-0.2464),
  "SW14":(51.4668,-0.2570),"SW15":(51.4564,-0.2219),"SW16":(51.4154,-0.1189),
  "SW17":(51.4278,-0.1659),"SW18":(51.4558,-0.1927),"SW19":(51.4214,-0.2019),
  "SW20":(51.4102,-0.2241),
  "W1":(51.5184,-0.1437),"W2":(51.5139,-0.1835),"W3":(51.5118,-0.2727),
  "W4":(51.4943,-0.2618),"W5":(51.5054,-0.3016),"W6":(51.4920,-0.2246),
  "W7":(51.5096,-0.3298),"W8":(51.5027,-0.1958),"W9":(51.5233,-0.1938),
  "W10":(51.5208,-0.2153),"W11":(51.5105,-0.2034),"W12":(51.5067,-0.2311),
  "W13":(51.5045,-0.3178),"W14":(51.4969,-0.2158),
}
BOROUGH_MAP = {
    "E10":"Waltham Forest","E11":"Redbridge","E12":"Newham","E13":"Newham",
    "E14":"Tower Hamlets","E15":"Newham","E16":"Newham","E17":"Waltham Forest",
    "E18":"Redbridge","E20":"Newham",
    "EC1A":"City of London","EC1R":"Islington","EC1V":"Islington",
    "N10":"Haringey","N11":"Barnet","N12":"Barnet","N13":"Enfield",
    "N14":"Enfield","N15":"Haringey","N16":"Hackney","N17":"Haringey",
    "N18":"Enfield","N19":"Islington","N20":"Barnet","N21":"Enfield","N22":"Haringey",
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
    "SW1P":"Westminster","SW1V":"Westminster","SW1W":"Westminster","SW1X":"Westminster",
    "SW2":"Lambeth","SW3":"Kensington & Chelsea","SW4":"Lambeth",
    "SW5":"Kensington & Chelsea","SW6":"Hammersmith & Fulham",
    "SW7":"Kensington & Chelsea","SW8":"Lambeth","SW9":"Lambeth",
    "SW10":"Kensington & Chelsea","SW11":"Wandsworth","SW12":"Wandsworth",
    "SW13":"Richmond","SW14":"Richmond","SW15":"Wandsworth","SW16":"Lambeth",
    "SW17":"Wandsworth","SW18":"Wandsworth","SW19":"Merton","SW20":"Merton",
    "W10":"Kensington & Chelsea","W11":"Kensington & Chelsea",
    "W12":"Hammersmith & Fulham","W13":"Ealing","W14":"Hammersmith & Fulham",
    "WC1B":"Camden","WC1E":"Camden","WC1N":"Camden","WC1X":"Islington",
    "WC2A":"Camden","WC2B":"Westminster","WC2H":"Westminster","WC2N":"Westminster",
}

def get_district(pc):
    """Extract outward postcode district. Handles both spaced and unspaced."""
    if not pc: return ""
    pc = pc.strip().upper()
    if " " in pc:
        return pc.split()[0]
    pc = pc.replace(" ", "")
    return pc[:-3] if len(pc) >= 5 else pc

def geo(pc):
    if not pc: return None, None
    d = get_district(pc)
    if d in PC: return PC[d]
    # Try shorter prefix
    m = re.match(r'^([A-Z]{1,2}\d)', d)
    d2 = m.group(1) if m else ""
    return PC.get(d2, (None, None))

def area(pc):
    d = get_district(pc)
    return BOROUGH_MAP.get(d, "")

print("Building merged dataset...")
merged = []
for ods in ods_codes:
    live = results.get(ods) or {}
    if live.get("inactive"): continue
    base = base_by_ods.get(ods, {})
    pcn = (base.get("gpps_pcn","") or "").replace(" PCN","").replace(" Pcn","").strip()
    pc = live.get("postcode") or base.get("postcode","") or ""
    lat, lng = geo(pc)
    n = live.get("name") or base.get("name","")
    a = live.get("address") or base.get("address","")
    merged.append({
        "o": ods,
        "n": n.title() if n.isupper() else n,
        "a": a.title() if a.isupper() else a,
        "p": pc,
        "ph": live.get("phone") or base.get("phone","") or "",
        "s": base.get("gpps_overall_pct"),
        "c": base.get("gpps_contact_pct"),
        "pcn": pcn,
        "cqc": base.get("cqc_rating",""),
        "cu": base.get("cqc_url",""),
        "ar": area(pc),
        "la": round(lat,5) if lat else None,
        "ln": round(lng,5) if lng else None,
    })

print(f"  {len(merged)} active GP practices")

print("Writing index.html...")
DATA_JS = json.dumps(merged, separators=(",",":"))
date = datetime.utcnow().strftime("%-d %B %Y")
with open("index.template.html") as f:
    html = f.read()
html = (html
    .replace("__DATA_PLACEHOLDER__", DATA_JS)
    .replace("__UPDATED_DATE__", date)
    .replace("__PRACTICE_COUNT__", str(len(merged))))
with open("index.html","w") as f:
    f.write(html)
print(f"Done! {len(merged)} practices, {date}, {len(html)//1024}KB")
