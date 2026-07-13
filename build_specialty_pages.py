#!/usr/bin/env python3
"""
build_specialists_pages.py — UPDATED for new pipeline

Builds /private/{specialty}/index.html pages from:
  - private_clinics.json (from gen_private_clinics.py) — uses NEW specialty keys
  - gps.json (NHS GPs)

Changes vs old version:
  - Reads `localAuthority` field (the actual field in new private_clinics.json)
    falling back to `borough` for old data
  - SPECIALTY_META aligned with gen_private_clinics.py output keys:
    private-gp, diagnostic, aesthetic, sexual-health, travel-health, etc.
  - DELETES stale specialty folders that no longer have records
"""
import json, re, shutil
from pathlib import Path
from collections import defaultdict

ROOT          = Path(__file__).resolve().parent
PRIVATE_JSON  = ROOT / "private_clinics.json"
GPS_JSON      = ROOT / "gps.json"
OUT_DIR       = ROOT / "private"

# Specialty metadata. Keys MUST match what gen_private_clinics.py outputs.
SPECIALTY_META = {
    "psychiatry":        {"emoji": "🧠", "label": "Psychiatry & Mental Health",   "desc": "Private psychiatrists, psychologists and mental health clinics in London. Services cover depression, anxiety, ADHD, PTSD, addiction and more."},
    "dermatology":       {"emoji": "🔬", "label": "Dermatology & Skin",            "desc": "Private dermatologists and skin clinics in London for acne, eczema, mole checks, psoriasis and cosmetic skin treatments."},
    "cardiology":        {"emoji": "❤️",  "label": "Cardiology & Heart",            "desc": "Private cardiologists in London offering heart health checks, ECGs, echocardiograms and specialist cardiac care."},
    "gynaecology":       {"emoji": "🌸", "label": "Gynaecology & Women's Health",  "desc": "Private gynaecologists in London covering fertility, menopause, obstetrics and women's health specialist care."},
    "oncology":          {"emoji": "🎗️", "label": "Oncology & Cancer Care",        "desc": "Private oncologists and cancer clinics in London offering diagnosis, chemotherapy, radiotherapy and specialist cancer care."},
    "orthopaedics":      {"emoji": "🦴", "label": "Orthopaedics & Sports Medicine","desc": "Private orthopaedic surgeons and sports medicine clinics in London for joints, spine, sports injuries and bone conditions."},
    "ophthalmology":     {"emoji": "👁️", "label": "Ophthalmology & Eye Care",      "desc": "Private eye clinics and ophthalmologists in London for vision, cataracts, laser eye surgery and glaucoma treatment."},
    "ent":               {"emoji": "👂", "label": "ENT — Ear, Nose & Throat",      "desc": "Private ENT specialists in London for hearing loss, sinusitis, tonsils, rhinoplasty and ear, nose and throat conditions."},
    "urology":           {"emoji": "🫁", "label": "Urology & Men's Health",        "desc": "Private urologists in London for prostate, kidney stones, bladder and men's health conditions."},
    "neurology":         {"emoji": "🧬", "label": "Neurology & Brain Health",      "desc": "Private neurologists in London for headaches, migraines, epilepsy, MS and other neurological conditions."},
    "gastroenterology":  {"emoji": "🫃", "label": "Gastroenterology & Digestion",  "desc": "Private gastroenterologists in London for endoscopy, colonoscopy, IBS, Crohn's and digestive conditions."},
    "endocrinology":     {"emoji": "⚗️", "label": "Endocrinology & Diabetes",      "desc": "Private endocrinologists in London for diabetes, thyroid disorders, hormonal conditions and weight management."},
    "rheumatology":      {"emoji": "🦿", "label": "Rheumatology & Arthritis",      "desc": "Private rheumatologists in London for arthritis, lupus, fibromyalgia and musculoskeletal conditions."},
    "paediatrics":       {"emoji": "👶", "label": "Paediatrics & Child Health",    "desc": "Private paediatricians and children's health clinics in London for child development, illness and specialist care."},
    "aesthetic":         {"emoji": "✨", "label": "Aesthetic & Cosmetic Medicine",  "desc": "Private cosmetic and aesthetic medicine clinics in London for Botox, fillers, anti-wrinkle treatments and skin rejuvenation."},
    "plastic-surgery":   {"emoji": "💎", "label": "Plastic & Reconstructive Surgery", "desc": "Private plastic surgeons in London offering rhinoplasty, breast surgery, reconstruction and cosmetic surgical procedures."},
    "sexual-health":     {"emoji": "🩺", "label": "Sexual Health",                 "desc": "Private sexual health clinics in London for STI testing, HIV care, contraception and genitourinary medicine."},
    "diagnostic":        {"emoji": "🔭", "label": "Diagnostics & Imaging",         "desc": "Private diagnostic imaging and health screening clinics in London offering MRI, CT scans, ultrasound and blood tests."},
    "physiotherapy":     {"emoji": "💪", "label": "Physiotherapy & Rehab",         "desc": "Private physiotherapists, chiropractors and osteopaths in London for injury recovery and musculoskeletal treatment."},
    "travel-health":     {"emoji": "✈️", "label": "Travel Health & Vaccines",       "desc": "Private travel health clinics in London for vaccinations, malaria prevention and travel medicine advice."},
    "respiratory":       {"emoji": "🫀", "label": "Respiratory & Lung Health",     "desc": "Private respiratory physicians in London for asthma, COPD, sleep apnoea and lung conditions."},
    "haematology":       {"emoji": "🩸", "label": "Haematology & Blood",           "desc": "Private haematologists in London for blood disorders, anaemia, leukaemia and clotting conditions."},
    "vascular":          {"emoji": "🩹", "label": "Vascular & Veins",              "desc": "Private vascular specialists in London for varicose veins, deep vein thrombosis and arterial conditions."},
    "weight-loss":       {"emoji": "⚖️", "label": "Weight Loss & Bariatrics",      "desc": "Private weight management and bariatric clinics in London for medical weight loss, gastric procedures and obesity care."},
    "hospital":          {"emoji": "🏥", "label": "Private Hospitals",             "desc": "Private hospitals in London offering surgical procedures, overnight stays and comprehensive specialist care."},
    "urgent-care":       {"emoji": "🚑", "label": "Urgent Care",                   "desc": "Walk-in and urgent care centres in London for same-day treatment of non-emergency injuries and illnesses."},
    "hospice":           {"emoji": "🕊️", "label": "Hospices",                      "desc": "Hospice and end-of-life care services in London providing specialist palliative care and bereavement support."},
    "private-gp":        {"emoji": "🏠", "label": "Private GP & Family Medicine",  "desc": "Private GP clinics in London for same-day appointments, health checks, prescriptions and general medical care."},
    "consultant":        {"emoji": "🩺", "label": "Consultant-Led Practices",   "desc": "Doctor-led private practices in London — CQC-registered clinics run by an individual consultant or doctor in their own name."},
}

# NHS GPs appear on the "private-gp" page as an alternative option
NHS_INCLUDE_SPECS = {"private-gp"}

NAV = """<nav class="site-nav">
  <a class="brand" href="/">London GP <em>Directory</em></a>
  <a href="/">Search</a>
  <a href="/boroughs/">Boroughs</a>
  <a href="/nhs-services/">NHS Services</a>
  <a href="/private/" class="active">Private Clinics</a>
  <a href="/methodology.html">Methodology</a>
  <a href="/sources.html">Sources</a>
</nav>"""

CSS = """<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.5}
a{text-decoration:none;color:inherit}
.site-nav{background:#003087;padding:12px 24px;display:flex;gap:20px;flex-wrap:wrap;align-items:center}
.site-nav a{color:rgba(255,255,255,.8);font-size:.85rem}
.site-nav a:hover,.site-nav a.active{color:#fff}
.site-nav .brand{font-family:Georgia,serif;font-size:1.05rem;font-weight:700;color:#fff;margin-right:14px}
.site-nav .brand em{color:#B5D4F4;font-style:italic;font-weight:400}
.page-header{background:#003087;color:#fff;padding:32px 24px;border-bottom:4px solid #0072CE}
.page-header h1{font-family:Georgia,serif;font-size:1.7rem;font-weight:700;line-height:1.15;margin-bottom:8px}
.page-header h1 em{color:#B5D4F4;font-style:italic;font-weight:400}
.page-header p{opacity:.8;max-width:680px;line-height:1.45;font-size:.9rem}
.stats{display:flex;gap:28px;margin-top:16px;flex-wrap:wrap}
.stat strong{display:block;font-size:1.4rem;font-weight:300;letter-spacing:-.03em}
.stat span{font-size:.7rem;opacity:.6;text-transform:uppercase;letter-spacing:.05em}
.content{max-width:1100px;margin:0 auto;padding:24px}
.filter-bar{background:#fff;border-radius:10px;padding:14px 16px;margin-bottom:20px;display:flex;flex-wrap:wrap;gap:8px;align-items:center}
.filter-label{font-size:.8rem;color:#888;margin-right:4px}
.filter-chip{padding:4px 12px;border-radius:16px;border:1.5px solid #ddd;background:#fff;cursor:pointer;font-size:.78rem;font-weight:500;color:#555;transition:all .15s}
.filter-chip.active{background:#003087;color:#fff;border-color:#003087}
.type-switch{display:flex;gap:6px;margin-left:auto}
.type-btn{padding:5px 14px;border-radius:20px;border:1.5px solid #ddd;background:#fff;cursor:pointer;font-size:.82rem;font-weight:500;color:#555;transition:all .15s}
.type-btn.active{background:#003087;color:#fff;border-color:#003087}
.section-title{font-size:1rem;font-weight:700;color:#003087;margin:20px 0 12px;display:flex;align-items:center;gap:8px}
.section-title .count{background:#e8f0fe;color:#003087;padding:2px 8px;border-radius:10px;font-size:.75rem;font-weight:600}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:12px}
.card{background:#fff;border-radius:10px;padding:16px;border:1px solid #e8e8e8;transition:box-shadow .15s;position:relative}
.card:hover{box-shadow:0 2px 12px rgba(0,0,0,.09)}
.card:has(.card-name-link):hover{border-color:#003087;box-shadow:0 2px 12px rgba(0,48,135,.12);cursor:pointer}
.card-name-link{color:inherit;text-decoration:none}
.card-name-link::after{content:"";position:absolute;inset:0;border-radius:inherit}
.card .card-actions,.card .card-actions a{position:relative;z-index:1}
.card-name{font-family:Georgia,serif;font-weight:700;font-size:.9rem;color:#003087;margin-bottom:4px;line-height:1.3}
.card-addr{font-size:.78rem;color:#777;margin-bottom:8px}
.card-tags{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:8px}
.tag{padding:2px 8px;border-radius:8px;font-size:.68rem;font-weight:600}
.tag.private{background:#f0fdf4;color:#166534}
.tag.nhs{background:#e8f0fe;color:#1a56db}
.tag.borough{background:#fff7ed;color:#9a3412}
.tag.spec{background:#f5f3ff;color:#5521b5;font-weight:400}
.cqc-badge{display:inline-block;padding:2px 8px;border-radius:6px;font-size:.65rem;font-weight:700}
.cqc-O{background:#d1fae5;color:#065f46}
.cqc-G{background:#dcfce7;color:#166534}
.cqc-RI{background:#fef3c7;color:#92400e}
.cqc-I{background:#fee2e2;color:#991b1b}
.cqc-{background:#f3f4f6;color:#6b7280}
.card-actions{display:flex;gap:8px;margin-top:10px;flex-wrap:wrap;align-items:center}
.btn{padding:4px 12px;border-radius:6px;font-size:.75rem;font-weight:600;text-decoration:none}
.btn-cqc{background:#003087;color:#fff}
.btn-web{background:#0072CE;color:#fff}
.btn-nhs{background:#005EB8;color:#fff}
.btn-phone{color:#003087;font-size:.78rem}
.spec-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:14px;margin-top:20px}
.spec-card{background:#fff;border-radius:12px;padding:15px 16px;border:1px solid #ddd;display:block;transition:box-shadow .15s,border-color .15s;color:inherit}
.spec-card:hover{box-shadow:0 2px 10px rgba(0,48,135,.12);border-color:#003087}
.spec-emoji{display:inline-block;font-size:1.05rem;margin-right:7px;vertical-align:-1px}
.spec-card h2{font-family:Georgia,serif;font-size:.95rem;font-weight:700;color:#003087;margin-bottom:4px;line-height:1.3}
.spec-card p{font-size:.8rem;color:#666}
.spec-count{font-size:.75rem;color:#888;margin-top:6px}
.spec-count strong{color:#003087}
.back{display:inline-flex;align-items:center;gap:6px;color:#003087;font-size:.85rem;margin-bottom:16px}
footer{text-align:center;padding:32px 24px;font-size:.78rem;color:#999;border-top:1px solid #e8e8e8;margin-top:40px}
.hidden{display:none!important}
@media(max-width:600px){.cards{grid-template-columns:1fr}.stats{gap:16px}}
</style>"""

def borough_of(r):
    """Read borough from either new (localAuthority) or old (borough) field."""
    return (r.get("localAuthority") or r.get("borough") or "").strip()

def slugify(s):
    return re.sub(r'[^a-z0-9]+', '-', s.lower().replace('&', 'and').replace("'", '')).strip('-')

def cqc_class(r):
    return {'Outstanding':'O','Good':'G','Requires improvement':'RI','Inadequate':'I'}.get(r, '')

def cqc_label(r):
    return r or 'Not rated'

_PAGE_URLS = None
def _page_urls():
    """cqc_id -> /practice/<b>/<slug>/ from merged.json (the same records
    build_practice_pages.py generates from), verified to exist on disk —
    CQC's localAuthority and our postcode-derived borough disagree at times,
    so never guess the URL from raw CQC data."""
    global _PAGE_URLS
    if _PAGE_URLS is not None:
        return _PAGE_URLS
    import json as _json
    _PAGE_URLS = {}
    merged = ROOT / "merged.json"
    if merged.exists():
        for r in _json.loads(merged.read_text()):
            if r.get("type") != "Private":
                continue
            b, n = slugify(r.get("ar", "")), slugify(r.get("n", ""))
            if b and n and (ROOT / "practice" / b / n / "index.html").exists():
                _PAGE_URLS[r.get("o", "")] = f"/practice/{b}/{n}/"
    return _PAGE_URLS

def render_private_card(p):
    rating  = p.get('cqc_rating','')
    badge   = f'<span class="cqc-badge cqc-{cqc_class(rating)}">{cqc_label(rating)}</span>' if rating else ''
    borough = borough_of(p)
    borough_tag = f'<span class="tag borough">{borough}</span>' if borough else ''
    spec_tags = ''.join(f'<span class="tag spec">{s}</span>' for s in (p.get('specialties') or [])[:2])
    actions = []
    if p.get('phone'):    actions.append(f'<span class="btn-phone">📞 {p["phone"]}</span>')
    if p.get('cqc_url'):  actions.append(f'<a class="btn btn-cqc" href="{p["cqc_url"]}" target="_blank" rel="noopener">CQC</a>')
    if p.get('website'):  actions.append(f'<a class="btn btn-web" href="{p["website"]}" target="_blank" rel="noopener">Website</a>')
    page_url = _page_urls().get(p.get("cqc_id", ""), "")
    name_html = (f'<a class="card-name-link" href="{page_url}">{p["name"]}</a>'
                 if page_url else p['name'])
    return f"""<div class="card" data-borough="{borough}" data-type="private">
  <div class="card-name">{name_html}</div>
  <div class="card-addr">{p.get('address','')}{', ' + p['postcode'] if p.get('postcode') else ''}</div>
  <div class="card-tags">
    <span class="tag private">Private</span>
    {borough_tag}{spec_tags}{badge}
  </div>
  <div class="card-actions">{' '.join(actions)}</div>
</div>"""

def render_nhs_card(g):
    rating = g.get('cqc','')
    badge  = f'<span class="cqc-badge cqc-{cqc_class(rating)}">{cqc_label(rating)}</span>' if rating else ''
    borough = g.get('ar','')
    score  = f'<span style="font-size:.78rem;color:#555">Patient score: <strong>{g["s"]}%</strong></span>' if g.get('s') else ''
    actions = []
    if g.get('ph'): actions.append(f'<span class="btn-phone">📞 {g["ph"]}</span>')
    if g.get('cu'): actions.append(f'<a class="btn btn-cqc" href="{g["cu"]}" target="_blank" rel="noopener">CQC</a>')
    slug_name = slugify(g.get('n',''))
    page_url = (f"/practice/{slugify(borough)}/{slug_name}/"
                if borough and slug_name else "")
    if page_url:
        actions.append(f'<a class="btn btn-nhs" href="{page_url}">View</a>')
    name_html = (f'<a class="card-name-link" href="{page_url}">{g.get("n","")}</a>'
                 if page_url else g.get('n',''))
    return f"""<div class="card" data-borough="{borough}" data-type="nhs">
  <div class="card-name">{name_html}</div>
  <div class="card-addr">{g.get('a','')}{', ' + g['p'] if g.get('p') else ''}</div>
  <div class="card-tags">
    <span class="tag nhs">NHS</span>
    {f'<span class="tag borough">{borough}</span>' if borough else ''}
    {badge}{score}
  </div>
  <div class="card-actions">{' '.join(actions)}</div>
</div>"""

FILTER_JS = """
<script>
(function(){
  var cards = Array.from(document.querySelectorAll('.card'));
  var activeBorough = '';
  var activeType = 'all';
  function applyFilters(){
    cards.forEach(function(c){
      var bOk = !activeBorough || c.dataset.borough === activeBorough;
      var tOk = activeType === 'all' || c.dataset.type === activeType;
      c.classList.toggle('hidden', !(bOk && tOk));
    });
    var vis = cards.filter(c => !c.classList.contains('hidden'));
    var privVis = vis.filter(c => c.dataset.type === 'private').length;
    var nhsVis  = vis.filter(c => c.dataset.type === 'nhs').length;
    var p = document.getElementById('countPriv'); if (p) p.textContent = privVis + ' private';
    var n = document.getElementById('countNhs'); if (n) n.textContent  = nhsVis + ' NHS';
  }
  document.querySelectorAll('.filter-chip').forEach(function(btn){
    btn.addEventListener('click', function(){
      document.querySelectorAll('.filter-chip').forEach(b=>b.classList.remove('active'));
      activeBorough = btn.dataset.borough;
      btn.classList.add('active');
      applyFilters();
    });
  });
  document.querySelectorAll('.type-btn').forEach(function(btn){
    btn.addEventListener('click', function(){
      document.querySelectorAll('.type-btn').forEach(b=>b.classList.remove('active'));
      activeType = btn.dataset.type;
      btn.classList.add('active');
      applyFilters();
    });
  });
})();
</script>
"""

def build_specialty_page(spec, meta, private_records, nhs_records):
    emoji = meta.get('emoji','🩺')
    label = meta.get('label', spec.title())
    desc  = meta.get('desc','')
    boroughs = sorted(set(borough_of(r) for r in private_records if borough_of(r))
                       | set(g.get('ar','') for g in nhs_records if g.get('ar','')))
    borough_chips = '<button class="filter-chip active" data-borough="">All London</button>'
    for b in boroughs:
        borough_chips += f'<button class="filter-chip" data-borough="{b}">{b}</button>'
    priv_cards = '\n'.join(render_private_card(p) for p in
                           sorted(private_records, key=lambda x: x.get('name','').lower()))
    nhs_cards  = '\n'.join(render_nhs_card(g) for g in
                           sorted(nhs_records, key=lambda x: x.get('n','').lower()))
    nhs_section = ''
    if nhs_records:
        nhs_section = f"""
<div class="section-title">🏥 NHS GP Practices in London <span class="count">{len(nhs_records)}</span></div>
<div class="cards">{nhs_cards}</div>"""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{label} in London — NHS &amp; Private | London GP Directory</title>
<meta name="description" content="{desc}">
<link rel="canonical" href="https://londongp.directory/private/{spec}/">
{CSS}
</head>
<body>
{NAV}
<div class="page-header">
  <h1>{emoji} {label} in London</h1>
  <p>{desc}</p>
  <div class="stats">
    <div class="stat"><strong id="countPriv">{len(private_records)} private</strong><span>Private providers</span></div>
    {'<div class="stat"><strong id="countNhs">' + str(len(nhs_records)) + ' NHS</strong><span>NHS practices</span></div>' if nhs_records else ''}
    <div class="stat"><strong>{len(boroughs)}</strong><span>Boroughs</span></div>
  </div>
</div>
<div class="content">
  <a class="back" href="/private/">← All specialties</a>
  <div class="filter-bar">
    <span class="filter-label">Borough:</span>{borough_chips}
    <div class="type-switch">
      <button class="type-btn active" data-type="all">All</button>
      <button class="type-btn" data-type="private">Private</button>
      {'<button class="type-btn" data-type="nhs">NHS</button>' if nhs_records else ''}
    </div>
  </div>
  <div class="section-title">💊 Private {label} <span class="count">{len(private_records)}</span></div>
  <div class="cards">{priv_cards}</div>
  {nhs_section}
</div>
<footer>
  Data: CQC · NHS ODS · GP Patient Survey · London GP Directory<br>
  <a href="/">Home</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a>
</footer>
{FILTER_JS}
</body>
</html>"""

def build_hub_page(specialty_counts):
    cards = ''
    for spec in sorted(specialty_counts.keys(),
                       key=lambda s: specialty_counts[s][0] + specialty_counts[s][1],
                       reverse=True):
        meta   = SPECIALTY_META.get(spec, {})
        emoji  = meta.get('emoji','🩺')
        label  = meta.get('label', spec.replace('-', ' ').title())
        n_priv, n_nhs = specialty_counts[spec]
        cards += f"""<a class="spec-card" href="/private/{spec}/">
  <h2><span class="spec-emoji">{emoji}</span>{label}</h2>
  <p class="spec-count">
    <strong>{n_priv}</strong> private{f' · <strong>{n_nhs}</strong> NHS' if n_nhs else ''}
  </p>
</a>"""
    total_priv = sum(v[0] for v in specialty_counts.values())
    total_nhs  = max(v[1] for v in specialty_counts.values()) if specialty_counts else 0
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>London Healthcare Specialists — NHS &amp; Private by Specialty</title>
<meta name="description" content="Browse NHS and private healthcare specialists in London by specialty. Find consultants, clinics and hospitals across all 32 boroughs.">
<link rel="canonical" href="https://londongp.directory/private/">
{CSS}
</head>
<body>
{NAV}
<div class="page-header">
  <h1>Healthcare Specialists in London <em>by specialty</em></h1>
  <p>Browse NHS and private consultants, clinics and hospitals across London by specialty. All providers CQC-registered.</p>
  <div class="stats">
    <div class="stat"><strong>{total_priv}</strong><span>Private providers</span></div>
    <div class="stat"><strong>{total_nhs}</strong><span>NHS GP practices</span></div>
    <div class="stat"><strong>{len(specialty_counts)}</strong><span>Specialties</span></div>
  </div>
</div>
<div class="content">
  <div class="spec-grid">{cards}</div>
</div>
<footer>
  Data: CQC · NHS ODS · GP Patient Survey · London GP Directory<br>
  <a href="/">Home</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a>
</footer>
</body>
</html>"""

def main():
    if not PRIVATE_JSON.exists():
        print(f"ERROR: {PRIVATE_JSON} not found.")
        return
    private_all = json.loads(PRIVATE_JSON.read_text())
    print(f"Loaded {len(private_all)} private records")

    nhs_all = []
    if GPS_JSON.exists():
        nhs_all = json.loads(GPS_JSON.read_text())
        print(f"Loaded {len(nhs_all)} NHS GP records")

    # Group private by specialty (drop records with no borough — they wouldn't filter well)
    priv_by_spec = defaultdict(list)
    skipped = 0
    for r in private_all:
        if not r.get('name'):
            skipped += 1; continue
        if not borough_of(r):
            skipped += 1; continue
        for spec in (r.get('specialties') or ['private-gp']):
            priv_by_spec[spec].append(r)
    if skipped:
        print(f"  Skipped {skipped} records with missing name/borough")

    # Build the count table — only include specialties with data
    specialty_counts = {}
    for spec, recs in priv_by_spec.items():
        n_priv = len(recs)
        n_nhs  = len(nhs_all) if spec in NHS_INCLUDE_SPECS else 0
        if n_priv > 0:
            specialty_counts[spec] = (n_priv, n_nhs)

    # Clean up old folders that no longer have data
    if OUT_DIR.exists():
        keep = set(specialty_counts.keys()) | {""}  # "" = index.html itself
        for child in OUT_DIR.iterdir():
            if child.is_dir() and child.name not in keep:
                print(f"  Removing stale folder: /private/{child.name}/")
                shutil.rmtree(child)

    # Build hub
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "index.html").write_text(build_hub_page(specialty_counts), encoding="utf-8")
    print(f"✓ Hub: {len(specialty_counts)} specialties")

    # Build per-specialty pages
    for spec, (n_priv, _) in sorted(specialty_counts.items()):
        priv_recs = priv_by_spec.get(spec, [])
        nhs_recs  = nhs_all if spec in NHS_INCLUDE_SPECS else []
        meta      = SPECIALTY_META.get(spec, {"emoji":"🩺",
                                              "label": spec.replace('-', ' ').title(),
                                              "desc": f"Private {spec.replace('-', ' ')} clinics in London."})
        spec_dir = OUT_DIR / spec
        spec_dir.mkdir(parents=True, exist_ok=True)
        page = build_specialty_page(spec, meta, priv_recs, nhs_recs)
        (spec_dir / "index.html").write_text(page, encoding="utf-8")
        print(f"  ✓ /private/{spec}/: {n_priv} private, {len(nhs_recs)} NHS")

    print(f"\n✅ Built {len(specialty_counts)} specialty pages in {OUT_DIR}/")

if __name__ == "__main__":
    main()
