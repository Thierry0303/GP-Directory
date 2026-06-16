#!/usr/bin/env python3
"""
process_cqc_data_v2.py  — clean replacement
============================================
Reads cqc_london_providers.json (output of cqc_scanner_fixed_v2.py)
and rebuilds all /private/{specialty}/ pages.

Specialty classification uses service types + name keywords.
Deduplicates providers. Excludes deregistered.
"""

import json, re, os
from pathlib import Path
from collections import defaultdict

CQC_JSON = Path("cqc_london_providers.json")
OUT_ROOT = Path("private")

# ── Borough from postcode ────────────────────────────────────────────────────
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
    district = pc.split()[0] if " " in pc else pc
    if district in BOROUGH_MAP: return BOROUGH_MAP[district]
    m = re.match(r"([A-Z]+\d+)", district)
    return BOROUGH_MAP.get(m.group(1), "") if m else ""

# ── Service type → specialty mapping ─────────────────────────────────────────
SERVICE_TO_SPEC = {
    "Doctors/Gps":                          "private gp",
    "Mobile doctors":                       "private gp",
    "Phone/online advice":                  "private gp",
    "Clinic":                               None,  # use name classifier
    "Diagnosis/screening":                  "diagnostics",
    "Hospital":                             "hospital",
    "Hospitals - Mental health/capacity":   "psychiatry",
    "Community services - Healthcare":      None,
    "Rehabilitation (illness/injury)":      "physiotherapy",
}

# Name-based specialty classifier
SPEC_PATTERNS = [
    ("private gp",       r"\b(?:gp\b|general\s+pract|family\s+(?:doctor|practice|medicine)|primary\s+care|walk.in\s+clinic)\b"),
    ("psychiatry",       r"\b(?:psychiatr|mental\s+health|psycholog|psychother|counsell|adhd|autism|addiction|eating\s+disorder|wellbeing\s+clinic)\b"),
    ("dermatology",      r"\b(?:dermatol|skin\s+(?:clinic|centre|specialist)|mole\s+|eczema|acne)\b"),
    ("cardiology",       r"\b(?:cardiol|heart\s+(?:clinic|centre)|cardiovascular|echocardiograph)\b"),
    ("gynaecology",      r"\b(?:gynaecol|gynecol|women.s\s+health|fertility|ivf\b|obstetric|menopause)\b"),
    ("oncology",         r"\b(?:oncol|cancer\s+(?:clinic|centre|care)|chemotherapy|radiotherapy)\b"),
    ("orthopaedics",     r"\b(?:orthop|joint\s+(?:clinic|replacement)|spine\s+clinic|sports\s+(?:injury|medicine|clinic)|knee\b|hip\s+replacement)\b"),
    ("ophthalmology",    r"\b(?:ophthalm|eye\s+(?:clinic|centre|specialist)|laser\s+eye|cataract|glaucoma|retina)\b"),
    ("ent",              r"\b(?:ent\b|ear.nose|otolaryng|hearing\s+(?:clinic|loss)|tinnitus|sinus)\b"),
    ("urology",          r"\b(?:urol|urinary|bladder\s+clinic|prostate|kidney\s+stone|men.s\s+health)\b"),
    ("neurology",        r"\b(?:neurol|headache\s+clinic|migraine\s+clinic|epilepsy|multiple\s+sclerosis)\b"),
    ("gastroenterology", r"\b(?:gastroenter|endoscopy|colonoscopy|bowel|ibs\b|crohn|liver\s+(?:clinic|specialist))\b"),
    ("endocrinology",    r"\b(?:endocrin|diabetes\s+clinic|thyroid\s+clinic|hormone\s+clinic|weight\s+management)\b"),
    ("rheumatology",     r"\b(?:rheumatol|arthritis\s+clinic|joint\s+pain\s+clinic|lupus\b)\b"),
    ("paediatrics",      r"\b(?:paediatric|pediatric|child(?:ren).s\s+(?:clinic|health)|child\s+development)\b"),
    ("cosmetic",         r"\b(?:cosmet|aesthet|plastic\s+surg|botox|filler|rhinoplasty|breast\s+(?:augment|implant))\b"),
    ("sexual health",    r"\b(?:sexual\s+health|sti\b|genitourin|hiv\s+clinic|contraception)\b"),
    ("diagnostics",      r"\b(?:diagnos|imaging\b|mri\b|ct\s+scan|ultrasound|x.ray\s+clinic|health\s+screen|scan\b)\b"),
    ("physiotherapy",    r"\b(?:physiother|physio\b|sports\s+rehab|musculoskeletal|osteopath)\b"),
    ("travel",           r"\b(?:travel\s+(?:clinic|health|vaccine)|yellow\s+fever|malaria)\b"),
    ("allergy",          r"\b(?:allerg|immunol|hay\s+fever|food\s+intolerance)\b"),
    ("pain management",  r"\b(?:pain\s+(?:clinic|management|specialist)|chronic\s+pain)\b"),
    ("respiratory",      r"\b(?:respirat|pulmonol|lung\s+clinic|asthma\s+clinic|sleep\s+(?:clinic|apnoea))\b"),
    ("hospital",         r"\b(?:private\s+hospital|nuffield\s+health\s+hospital|spire\s+hospital|hca\b|bupa\s+hospital|circle\s+health)\b"),
]

def classify(name, svc_names):
    # First try service type
    for svc in svc_names:
        spec = SERVICE_TO_SPEC.get(svc)
        if spec:
            return spec
    # Then try name patterns
    blob = name.lower()
    for spec, pat in SPEC_PATTERNS:
        if re.search(pat, blob, re.IGNORECASE):
            return spec
    # Fallback based on service type
    if "Clinic" in svc_names or "Community services - Healthcare" in svc_names:
        return "private gp"
    return None  # skip uncategorisable

# ── HTML generation ───────────────────────────────────────────────────────────
SPECIALTY_META = {
    "private gp":        ("🏠", "Private GP & Walk-in Clinics"),
    "psychiatry":        ("🧠", "Psychiatry & Mental Health"),
    "dermatology":       ("🔬", "Dermatology & Skin Clinics"),
    "cardiology":        ("❤️",  "Cardiology & Heart Clinics"),
    "gynaecology":       ("🌸", "Gynaecology & Women's Health"),
    "oncology":          ("🎗️", "Oncology & Cancer Care"),
    "orthopaedics":      ("🦴", "Orthopaedics & Sports Medicine"),
    "ophthalmology":     ("👁️", "Ophthalmology & Eye Clinics"),
    "ent":               ("👂", "ENT — Ear, Nose & Throat"),
    "urology":           ("🫁", "Urology & Men's Health"),
    "neurology":         ("🧬", "Neurology"),
    "gastroenterology":  ("🫃", "Gastroenterology"),
    "endocrinology":     ("⚗️",  "Endocrinology & Diabetes"),
    "rheumatology":      ("🦿", "Rheumatology & Arthritis"),
    "paediatrics":       ("👶", "Paediatrics & Child Health"),
    "cosmetic":          ("✨", "Cosmetic & Aesthetic Medicine"),
    "sexual health":     ("🩺", "Sexual Health"),
    "diagnostics":       ("🔭", "Diagnostics & Imaging"),
    "physiotherapy":     ("💪", "Physiotherapy & Rehabilitation"),
    "travel":            ("✈️",  "Travel Health & Vaccines"),
    "allergy":           ("🌿", "Allergy & Immunology"),
    "pain management":   ("💊", "Pain Management"),
    "respiratory":       ("🫀", "Respiratory & Lung Health"),
    "hospital":          ("🏥", "Private Hospitals"),
}

CSS = """<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.5}
a{text-decoration:none;color:inherit}
.nav{background:#003087;padding:10px 24px;display:flex;gap:20px;flex-wrap:wrap}
.nav a{color:rgba(255,255,255,.8);font-size:.85rem}.nav a:hover{color:#fff}
.header{background:linear-gradient(135deg,#003087,#0047bb);color:#fff;padding:36px 24px}
.header h1{font-size:1.9rem;font-weight:700;margin-bottom:8px}
.header p{opacity:.8;max-width:680px}
.stats{display:flex;gap:28px;margin-top:18px;flex-wrap:wrap}
.stat strong{display:block;font-size:1.5rem;font-weight:300}
.stat span{font-size:.7rem;opacity:.7;text-transform:uppercase;letter-spacing:.05em}
.content{max-width:1100px;margin:0 auto;padding:24px}
.filter-bar{background:#fff;border-radius:10px;padding:12px 16px;margin-bottom:18px;display:flex;flex-wrap:wrap;gap:7px;align-items:center}
.fl{font-size:.78rem;color:#888;margin-right:4px;white-space:nowrap}
.chip{padding:3px 11px;border-radius:14px;border:1.5px solid #ddd;background:#fff;cursor:pointer;font-size:.75rem;font-weight:500;color:#555;transition:all .15s}
.chip.active{background:#003087;color:#fff;border-color:#003087}
.back{display:inline-block;color:#003087;font-size:.85rem;margin-bottom:16px}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px}
.card{background:#fff;border-radius:10px;padding:16px;border:1px solid #e8e8e8;transition:box-shadow .15s}
.card:hover{box-shadow:0 2px 12px rgba(0,0,0,.09)}
.card h3{font-weight:600;font-size:.95rem;color:#003087;margin-bottom:4px}
.card .addr{font-size:.77rem;color:#777;margin-bottom:8px}
.tags{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:8px}
.tag{padding:2px 8px;border-radius:7px;font-size:.67rem;font-weight:600}
.tag.priv{background:#f0fdf4;color:#166534}
.tag.bor{background:#fff7ed;color:#9a3412}
.cqc{background:#dcfce7;color:#166534}
.cqc.ri{background:#fef3c7;color:#92400e}
.cqc.i{background:#fee2e2;color:#991b1b}
.cqc.nr{background:#f3f4f6;color:#6b7280}
.actions{display:flex;gap:7px;flex-wrap:wrap;margin-top:10px;align-items:center}
.btn{padding:4px 11px;border-radius:6px;font-size:.73rem;font-weight:600;color:#fff}
.btn-cqc{background:#003087}.btn-web{background:#0072CE}
.ph{font-size:.77rem;color:#003087}
/* Hub */
.spec-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(240px,1fr));gap:14px;margin-top:20px}
.spec-card{background:#fff;border-radius:12px;padding:18px;border:1px solid #e8e8e8;display:block;transition:box-shadow .15s}
.spec-card:hover{box-shadow:0 4px 14px rgba(0,48,135,.12);border-color:#bfdbfe}
.spec-emoji{font-size:1.8rem;margin-bottom:8px}
.spec-card h2{font-size:.95rem;font-weight:700;color:#003087;margin-bottom:4px}
.spec-count{font-size:.78rem;color:#888;margin-top:4px}
.spec-count strong{color:#003087}
.hidden{display:none!important}
footer{text-align:center;padding:28px;font-size:.77rem;color:#aaa;margin-top:32px;border-top:1px solid #eee}
@media(max-width:600px){.cards{grid-template-columns:1fr}}
</style>"""

NAV = """<nav class="nav">
  <a href="/">🏠 Home</a>
  <a href="/private/">💊 Private Clinics</a>
  <a href="/specialists/">🔬 By Specialty</a>
</nav>"""

FILTER_JS = """<script>
(function(){
  var cards=Array.from(document.querySelectorAll('.card'));
  var ab='';
  document.querySelectorAll('.chip').forEach(function(b){
    b.addEventListener('click',function(){
      document.querySelectorAll('.chip').forEach(c=>c.classList.remove('active'));
      b.classList.add('active');
      ab=b.dataset.b;
      cards.forEach(function(c){
        c.classList.toggle('hidden',ab&&c.dataset.b!==ab);
      });
    });
  });
})();
</script>"""

def cqc_badge(rating):
    cls = {'Good':'','Requires improvement':' ri','Inadequate':' i'}.get(rating,' nr')
    label = rating or 'Not rated'
    return f'<span class="tag cqc{cls}">{label}</span>'

def slugify(s):
    return re.sub(r'[^a-z0-9]+', '-', s.lower().replace('&','and')).strip('-')

def build_specialty_page(spec, emoji, label, providers):
    boroughs = sorted({p['borough'] for p in providers if p.get('borough')})
    chips = '<button class="chip active" data-b="">All London</button>' + \
            ''.join(f'<button class="chip" data-b="{b}">{b} ({sum(1 for p in providers if p.get("borough")==b)})</button>' for b in boroughs)

    cards_html = ''
    for p in sorted(providers, key=lambda x: x['name'].lower()):
        actions = []
        if p.get('phone'):    actions.append(f'<span class="ph">📞 {p["phone"]}</span>')
        if p.get('cqc_url'):  actions.append(f'<a class="btn btn-cqc" href="{p["cqc_url"]}" target="_blank" rel="noopener">CQC</a>')
        if p.get('website'):  actions.append(f'<a class="btn btn-web" href="{p["website"]}" target="_blank" rel="noopener">Website</a>')
        borough_tag = f'<span class="tag bor">{p["borough"]}</span>' if p.get('borough') else ''
        cards_html += f"""<div class="card" data-b="{p.get('borough','')}">
  <h3>{p['name']}</h3>
  <div class="addr">{p.get('address','')}{', ' + p['postcode'] if p.get('postcode') else ''}</div>
  <div class="tags"><span class="tag priv">Private</span>{borough_tag}{cqc_badge(p.get('cqc_rating',''))}</div>
  <div class="actions">{''.join(actions)}</div>
</div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Private {label} in London — {len(providers)} Clinics | London Healthcare Directory</title>
<meta name="description" content="Find {len(providers)} private {label.lower()} clinics in London, all CQC-registered. Browse by borough.">
<link rel="canonical" href="https://londongp.directory/private/{slugify(spec)}/">
{CSS}</head>
<body>{NAV}
<div class="header">
  <h1>{emoji} Private {label} in London</h1>
  <p>All CQC-registered private {label.lower()} providers across London, updated weekly.</p>
  <div class="stats">
    <div class="stat"><strong>{len(providers)}</strong><span>Providers</span></div>
    <div class="stat"><strong>{len(boroughs)}</strong><span>Boroughs</span></div>
    <div class="stat"><strong>{sum(1 for p in providers if p.get('cqc_rating') in ('Good','Outstanding'))}</strong><span>Good or Outstanding</span></div>
  </div>
</div>
<div class="content">
  <a class="back" href="/private/">← All specialties</a>
  <div class="filter-bar"><span class="fl">Borough:</span>{chips}</div>
  <div class="cards">{cards_html}</div>
</div>
<footer>Data: CQC · London Healthcare Directory · <a href="/">Home</a> · <a href="/about.html">About</a></footer>
{FILTER_JS}</body></html>"""

def build_hub(by_spec):
    cards = ''
    for spec in sorted(by_spec.keys(), key=lambda s: len(by_spec[s]), reverse=True):
        emoji, label = SPECIALTY_META.get(spec, ('🩺', spec.title()))
        n = len(by_spec[spec])
        good = sum(1 for p in by_spec[spec] if p.get('cqc_rating') in ('Good','Outstanding'))
        cards += f"""<a class="spec-card" href="/private/{slugify(spec)}/">
  <div class="spec-emoji">{emoji}</div>
  <h2>{label}</h2>
  <p class="spec-count"><strong>{n}</strong> providers · <strong>{good}</strong> Good or Outstanding</p>
</a>"""

    total = sum(len(v) for v in by_spec.values())
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Private Healthcare Clinics in London — Browse by Specialty</title>
<meta name="description" content="Browse {total} CQC-registered private healthcare clinics in London by specialty. Find consultants, hospitals and specialist clinics across all 32 boroughs.">
<link rel="canonical" href="https://londongp.directory/private/">
{CSS}</head>
<body>{NAV}
<div class="header">
  <h1>💊 Private Healthcare Clinics in London</h1>
  <p>Browse {total} CQC-registered private clinics by specialty across London's 32 boroughs.</p>
  <div class="stats">
    <div class="stat"><strong>{total}</strong><span>Total providers</span></div>
    <div class="stat"><strong>{len(by_spec)}</strong><span>Specialties</span></div>
  </div>
</div>
<div class="content">
  <div class="spec-grid">{cards}</div>
</div>
<footer>Data: CQC · London Healthcare Directory · <a href="/">Home</a> · <a href="/about.html">About</a></footer>
</body></html>"""

def main():
    if not CQC_JSON.exists():
        raise SystemExit(f"ERROR: {CQC_JSON} not found. Run cqc_scanner_fixed_v2.py first.")

    raw = json.loads(CQC_JSON.read_text())
    print(f"Loaded {len(raw)} records from {CQC_JSON}")

    by_spec = defaultdict(list)
    skipped = 0

    for r in raw:
        name = (r.get("locationName") or "").strip()
        if not name: continue

        svc_names = {s.get("name","") for s in r.get("gacServiceTypes",[])}
        spec = classify(name, svc_names)
        if not spec:
            skipped += 1
            continue

        pc = (r.get("postalCode") or "").strip().upper()
        borough = borough_from_postcode(pc)

        by_spec[spec].append({
            "name":       name,
            "address":    r.get("address1",""),
            "postcode":   pc,
            "borough":    borough,
            "phone":      r.get("phone",""),
            "website":    r.get("website",""),
            "cqc_rating": r.get("currentRating",""),
            "cqc_url":    f"https://www.cqc.org.uk/location/{r['locationId']}" if r.get("locationId") else "",
        })

    print(f"Skipped {skipped} unclassifiable records")
    for spec, providers in sorted(by_spec.items(), key=lambda x: len(x[1]), reverse=True):
        print(f"  {spec}: {len(providers)}")

    # Build hub
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    (OUT_ROOT / "index.html").write_text(build_hub(by_spec), encoding="utf-8")
    print(f"\n✓ Hub: {len(by_spec)} specialties")

    # Build specialty pages
    for spec, providers in by_spec.items():
        emoji, label = SPECIALTY_META.get(spec, ('🩺', spec.title()))
        spec_dir = OUT_ROOT / slugify(spec)
        spec_dir.mkdir(parents=True, exist_ok=True)
        (spec_dir / "index.html").write_text(
            build_specialty_page(spec, emoji, label, providers), encoding="utf-8"
        )
        print(f"  ✓ /private/{slugify(spec)}/  — {len(providers)} providers")

    print(f"\n✅ All /private/ pages rebuilt.")

if __name__ == "__main__":
    main()
