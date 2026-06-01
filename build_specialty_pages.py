#!/usr/bin/env python3
"""
Build specialty hub pages at /private/{specialty-slug}/index.html plus
a /private/ index page listing all specialties.

These pages target high-commercial-intent searches like
"private cardiologist London", "private psychiatrist Camden", etc.

Each specialty page:
  - SEO-optimised H1, title, meta description, JSON-LD CollectionPage
  - 200-word specialty intro for substantive content
  - Borough filter chips (lets users narrow to a borough)
  - Card grid of every private clinic in that specialty
  - Cross-links to related specialties + boroughs

Reads merged.json (combined NHS + Private produced by merge_into_dataset.py).
"""

import json, re, sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MERGED_JSON = ROOT / "merged.json"
OUT_DIR = ROOT / "private"
SITEMAP = ROOT / "sitemap.xml"

BASE_URL = "https://londongp.directory"

# Display name + SEO intro for each specialty.
SPECIALTY_META = {
    "private gp": {
        "title":  "Private GP",
        "h1":     "Private GPs in London",
        "noun":   "private GPs",
        "intro": (
            "Private GPs in London offer same-day or next-day appointments, "
            "longer consultations (typically 20-30 minutes vs the NHS 10 minutes), "
            "and continuity with a single doctor. They handle the full range of "
            "general medical care — health checks, repeat prescriptions, travel "
            "vaccinations, sexual health screening and referrals to private "
            "specialists. Most operate on a pay-per-visit basis (£100-£250 per "
            "consultation) or via a registered membership model. Many are CQC-rated "
            "Good or Outstanding. We list every private GP service in London below, "
            "with addresses, websites and CQC ratings."
        ),
    },
    "psychiatry": {
        "title":  "Private Psychiatry",
        "h1":     "Private Psychiatrists & Mental Health Clinics in London",
        "noun":   "private psychiatry services",
        "intro": (
            "Private psychiatrists in London offer faster access to assessment, "
            "diagnosis and ongoing treatment than the NHS pathway. Services cover "
            "depression, anxiety, ADHD, bipolar disorder, PTSD, eating disorders, "
            "addiction and personality disorders. A first consultation is typically "
            "£250-£500 and lasts 60-90 minutes. Follow-up sessions, prescription "
            "management and talking therapies are usually arranged through the same "
            "clinic. Many practitioners are NHS consultants who also work privately, "
            "and most clinics accept private medical insurance. The directory below "
            "lists every private psychiatry and mental health clinic registered "
            "with the CQC in London."
        ),
    },
    "cardiology": {
        "title":  "Private Cardiology",
        "h1":     "Private Cardiologists in London",
        "noun":   "private cardiology clinics",
        "intro": (
            "Private cardiologists in London handle chest pain investigation, "
            "palpitations, suspected heart disease, valve problems and high blood "
            "pressure. Most offer same-week consultations with an ECG and "
            "echocardiogram on the day, plus access to CT coronary angiography, "
            "stress testing and 24-hour Holter monitoring. Consultation fees "
            "typically range £200-£400. Many cardiologists hold senior NHS posts "
            "at Royal Brompton, Barts Heart Centre or St George's and offer private "
            "appointments alongside. Compare every CQC-registered private cardiology "
            "service in London below."
        ),
    },
    "dermatology": {
        "title":  "Private Dermatology",
        "h1":     "Private Dermatologists in London",
        "noun":   "private dermatology clinics",
        "intro": (
            "Private dermatologists in London cover skin cancer screening, mole "
            "checks, acne, eczema, psoriasis, rosacea, hair loss and aesthetic "
            "treatments. Most offer dermoscopy at the first consultation and can "
            "biopsy or excise lesions on the same visit. Consultation fees are "
            "typically £200-£350 and minor surgery £400-£800. Look for clinics "
            "registered with the British Association of Dermatologists. The "
            "directory below lists every CQC-registered private dermatology clinic "
            "in London."
        ),
    },
    "ophthalmology": {
        "title":  "Private Ophthalmology",
        "h1":     "Private Ophthalmologists & Eye Clinics in London",
        "noun":   "private eye clinics",
        "intro": (
            "Private ophthalmologists in London handle cataract surgery, refractive "
            "laser eye treatment, glaucoma, macular degeneration, diabetic eye "
            "screening and paediatric ophthalmology. Many clinics offer same-day "
            "OCT scans and visual field testing. Cataract surgery is typically "
            "£2,500-£3,800 per eye; LASIK starts around £1,800 per eye. Compare "
            "every private eye clinic registered with the CQC in London below."
        ),
    },
    "gynaecology": {
        "title":  "Private Gynaecology",
        "h1":     "Private Gynaecologists & Women's Health Clinics in London",
        "noun":   "private gynaecology clinics",
        "intro": (
            "Private gynaecologists in London offer rapid access to fertility "
            "investigation, menopause management, contraception, abnormal bleeding, "
            "endometriosis, fibroids and gynaecological cancer screening. Most "
            "clinics include ultrasound on the day. Consultation fees are typically "
            "£250-£450. Many specialists also run NHS clinics at Imperial, UCLH or "
            "King's. Find every CQC-registered private women's health clinic in "
            "London below."
        ),
    },
    "ent": {
        "title":  "Private ENT",
        "h1":     "Private ENT Specialists in London",
        "noun":   "private ENT clinics",
        "intro": (
            "Private ear, nose and throat specialists in London diagnose and treat "
            "hearing loss, sinusitis, tonsillitis, snoring, dizziness, voice "
            "problems and head and neck cancers. Most clinics offer audiology and "
            "fibre-optic endoscopy at the first consultation. Consultation fees "
            "are typically £200-£350; minor surgery £600-£2,500. The directory "
            "below lists every private ENT service in London registered with the "
            "CQC."
        ),
    },
    "orthopaedics": {
        "title":  "Private Orthopaedics",
        "h1":     "Private Orthopaedic Surgeons in London",
        "noun":   "private orthopaedic clinics",
        "intro": (
            "Private orthopaedic surgeons in London handle joint pain, sports "
            "injuries, fractures, arthritis, spinal problems and knee and hip "
            "replacement. Most clinics offer MRI within 48 hours and surgery within "
            "2-4 weeks. Consultation fees are typically £250-£400; arthroscopy "
            "£3,000-£5,000; hip or knee replacement £12,000-£18,000. Compare every "
            "CQC-registered private orthopaedic clinic in London below."
        ),
    },
    "urology": {
        "title":  "Private Urology",
        "h1":     "Private Urologists in London",
        "noun":   "private urology clinics",
        "intro": (
            "Private urologists in London handle prostate problems, kidney stones, "
            "bladder cancer screening, incontinence, erectile dysfunction and "
            "vasectomy. Most clinics include flow studies and ultrasound at the "
            "first consultation. Compare every CQC-registered private urology "
            "service in London below."
        ),
    },
    "oncology": {
        "title":  "Private Oncology",
        "h1":     "Private Oncologists in London",
        "noun":   "private oncology clinics",
        "intro": (
            "Private oncologists in London offer rapid access to staging, second "
            "opinions, chemotherapy, targeted therapy and immunotherapy across all "
            "common cancers. Most clinics are linked to private hospitals like HCA, "
            "Bupa Cromwell, Royal Marsden Private Care, The London Clinic or "
            "Cleveland Clinic London. The directory below lists every CQC-"
            "registered private oncology service in London."
        ),
    },
    "gastroenterology": {
        "title":  "Private Gastroenterology",
        "h1":     "Private Gastroenterologists in London",
        "noun":   "private gastroenterology clinics",
        "intro": (
            "Private gastroenterologists in London handle reflux, irritable bowel "
            "syndrome, inflammatory bowel disease, coeliac disease, liver disease "
            "and bowel cancer screening. Most clinics include same-week endoscopy "
            "or colonoscopy with sedation. Consultation fees are typically £250-"
            "£400; gastroscopy £1,400-£2,000; colonoscopy £2,000-£2,800."
        ),
    },
    "paediatrics": {
        "title":  "Private Paediatrics",
        "h1":     "Private Paediatricians & Children's Clinics in London",
        "noun":   "private paediatric clinics",
        "intro": (
            "Private paediatricians in London offer rapid access for growth and "
            "development concerns, asthma, eczema, allergies, ADHD, autism "
            "assessment and general childhood illness. Many also run NHS clinics "
            "at Great Ormond Street, the Royal London or Evelina Children's "
            "Hospital. Consultation fees are typically £200-£400."
        ),
    },
    "diagnostics": {
        "title":  "Private Diagnostics",
        "h1":     "Private Diagnostic & Imaging Clinics in London",
        "noun":   "private diagnostic centres",
        "intro": (
            "Private diagnostic centres in London offer MRI, CT, ultrasound, "
            "X-ray, mammography and full body health screens, often within 48 hours "
            "and at significantly lower cost than going through a private hospital. "
            "Used for sports injuries, suspected cancer, neurological symptoms, "
            "pregnancy scanning and health checks. MRI scans typically £350-£700; "
            "CT £400-£800; ultrasound £200-£400."
        ),
    },
    "hospital": {
        "title":  "Private Hospital",
        "h1":     "Private Hospitals in London",
        "noun":   "private hospitals",
        "intro": (
            "London's private hospitals — HCA's Princess Grace, Wellington and "
            "Portland; Bupa Cromwell; The London Clinic; King Edward VII's; "
            "Cleveland Clinic London; Royal Marsden Private; Spire Bushey; and "
            "the major NHS Private Patient Units at Imperial, UCLH, Guy's and "
            "St Thomas' — offer full inpatient care, surgery, intensive care and "
            "complex medicine. Most accept all major private medical insurance "
            "and self-pay patients."
        ),
    },
    "physiotherapy": {
        "title":  "Private Physiotherapy",
        "h1":     "Private Physiotherapy Clinics in London",
        "noun":   "private physiotherapy clinics",
        "intro": (
            "Private physiotherapists in London handle back and neck pain, sports "
            "injuries, post-surgical rehabilitation, joint pain and posture "
            "problems. Most clinics offer same-week appointments and accept "
            "self-referral. Consultation fees are typically £60-£100 per session "
            "in zones 2-6, £90-£140 in zone 1."
        ),
    },
    "travel": {
        "title":  "Travel Health",
        "h1":     "Private Travel Clinics in London",
        "noun":   "private travel clinics",
        "intro": (
            "Private travel clinics in London offer rapid access to yellow fever, "
            "rabies, hepatitis, typhoid, meningitis and Japanese encephalitis "
            "vaccinations, plus anti-malarial prescribing. Most clinics provide a "
            "personalised travel risk assessment and same-day vaccination "
            "(yellow fever requires a registered centre). Walk-in availability "
            "and weekend appointments are common."
        ),
    },
}

def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower().replace("&", "and")).strip("-")

def cqc_class(r):
    if not r: return "cqc-N"
    if r == "Outstanding": return "cqc-O"
    if r == "Good":        return "cqc-G"
    if r.startswith("Requires"): return "cqc-R"
    if r == "Inadequate":  return "cqc-I"
    return "cqc-N"

def render_card(d, all_specs):
    cc = cqc_class(d.get("cqc"))
    cqc_label = d.get("cqc") or "Not rated"
    name = d.get("n", "")
    addr = d.get("a", "")
    pc = d.get("p", "")
    ph = d.get("ph", "")
    ar = d.get("ar", "")
    specs = d.get("specs", []) or []
    web = d.get("web", "")
    spec_badges = "".join(
        f'<span class="spec-badge">{s}</span>' for s in specs[:3]
    )
    phone_html = (f'<a class="card-phone" href="tel:{ph.replace(" ","")}">📞 {ph}</a>'
                  if ph else "<span></span>")
    cqc_btn = (f'<a class="pill pill-cqc" href="{d.get("cu","")}" target="_blank">CQC</a>'
               if d.get("cu") else "")
    web_btn = f'<a class="pill pill-web" href="{web}" target="_blank">Website →</a>' if web else ""
    borough_chip = (f'<a href="/practice/{slugify(ar)}/" class="borough-chip">{ar}</a>'
                    if ar else "")
    return f"""<div class="card" data-borough="{ar}">
      <div class="card-top">
        <div class="card-name">{name}</div>
        <span class="cqc {cc}">{cqc_label}</span>
      </div>
      <div class="card-badges">
        <span class="type-badge t-priv">Private</span>
        {spec_badges}
      </div>
      {borough_chip}
      <div class="card-addr">{addr}{', ' + pc if pc else ''}</div>
      <div class="card-foot">
        {phone_html}
        <div class="actions">{web_btn}{cqc_btn}</div>
      </div>
    </div>"""

SHARED_STYLES = """*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.5}
a{text-decoration:none;color:inherit}
.hdr{background:linear-gradient(135deg,#003087 0%,#0072CE 100%);color:#fff;padding:28px 24px;border-bottom:4px solid #0072CE}
.hdr-in{max-width:1300px;margin:0 auto}
.crumbs{font-size:12px;opacity:.7;margin-bottom:10px}
.crumbs a{color:#B5D4F4}
.hdr h1{font-family:Georgia,serif;font-size:1.9rem;font-weight:700;line-height:1.15;margin-bottom:12px;letter-spacing:-0.01em}
.hdr h1 em{color:#FAE7F3;font-style:italic;font-weight:400}
.hdr-sub{font-size:.95rem;opacity:.9;max-width:720px;line-height:1.55}
.stats{display:flex;gap:28px;flex-wrap:wrap;margin-top:18px}
.stat strong{display:block;font-size:1.5rem;font-weight:300}
.stat span{font-size:.7rem;opacity:.7;text-transform:uppercase;letter-spacing:.05em}
.intro-zone{background:#fff;border-bottom:1px solid #e5e5e3;padding:24px}
.intro-inner{max-width:880px;margin:0 auto;font-size:15px;color:#444;line-height:1.7}
.filter-zone{background:#FDFAFA;border-bottom:1px solid #e5e5e3;padding:14px 24px}
.filter-inner{max-width:1300px;margin:0 auto;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.filter-label{font-size:11px;text-transform:uppercase;color:#888;font-weight:700;margin-right:6px}
.borough-filter{padding:5px 12px;border-radius:99px;border:1px solid #ddd;background:#fff;cursor:pointer;font-family:inherit;font-size:12.5px;color:#555;font-weight:500}
.borough-filter.active{background:#A02670;color:#fff;border-color:#A02670}
.borough-filter:hover:not(.active){border-color:#A02670;color:#A02670}
.wrap{max-width:1300px;margin:0 auto;padding:24px}
.results-bar{font-size:13px;color:#888;margin-bottom:14px}
.results-bar strong{color:#222}
#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:13px}
.card{background:#fff;border:1px solid #ddd;border-radius:12px;padding:15px 16px;display:flex;flex-direction:column}
.card-top{display:flex;justify-content:space-between;align-items:flex-start;gap:8px;margin-bottom:7px}
.card-name{font-family:Georgia,serif;font-size:14px;font-weight:700;color:#003087;flex:1;line-height:1.3}
.cqc{flex-shrink:0;font-size:9.5px;font-weight:600;padding:2px 8px;border-radius:99px;white-space:nowrap}
.cqc-O{background:#E1F5EE;color:#0F6E56}.cqc-G{background:#D8EFE3;color:#007F3B}
.cqc-R{background:#FAEEDA;color:#BA7517}.cqc-I{background:#FCEBEB;color:#A32D2D}.cqc-N{background:#f0f0ee;color:#777}
.card-badges{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:7px}
.type-badge{font-size:10px;font-weight:600;padding:2px 8px;border-radius:99px;text-transform:uppercase;letter-spacing:.04em}
.type-badge.t-priv{background:#FAE7F3;color:#A02670}
.spec-badge{font-size:10px;padding:2px 8px;border-radius:99px;background:#F5F0E8;color:#7A5D2F;text-transform:capitalize}
.borough-chip{display:inline-block;font-size:10px;color:#003087;background:#EDF4FC;padding:2px 9px;border-radius:99px;margin-bottom:8px;font-weight:500;align-self:flex-start}
.borough-chip:hover{background:#003087;color:#fff}
.card-addr{font-size:11.5px;color:#888;margin-bottom:10px;line-height:1.4}
.card-foot{display:flex;align-items:center;justify-content:space-between;border-top:1px solid #f0f0ee;padding-top:10px;gap:8px;margin-top:auto}
.card-phone{font-size:11.5px;color:#444;font-weight:500}
.actions{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}
.pill{font-size:10.5px;padding:4px 9px;border-radius:6px;font-weight:600;white-space:nowrap}
.pill-cqc{background:#D8EFE3;color:#007F3B}.pill-web{background:#FAE7F3;color:#A02670}
.related{background:#fff;border-top:1px solid #e5e5e3;padding:24px}
.related-inner{max-width:1300px;margin:0 auto}
.related h2{font-family:Georgia,serif;font-size:1.05rem;font-weight:700;color:#003087;margin-bottom:12px}
.related-list{display:flex;flex-wrap:wrap;gap:6px}
.related-list a{padding:7px 13px;border-radius:99px;background:#FAE7F3;color:#A02670;font-size:13px;font-weight:600}
.related-list a:hover{background:#A02670;color:#fff}
.empty{text-align:center;padding:4rem 2rem;color:#888}
footer{background:#003087;color:rgba(255,255,255,.5);text-align:center;padding:14px 24px;font-size:11.5px}
footer a{color:rgba(255,255,255,.8)}
@media(max-width:600px){
  .hdr{padding:18px 16px}
  .hdr h1{font-size:1.35rem}
  .hdr-sub{font-size:.85rem}
  .stats{gap:16px}
  .intro-zone{padding:18px 16px}
  .intro-inner{font-size:14px}
  .filter-zone{padding:11px 16px}
  .filter-label{display:none}
  .wrap{padding:16px}
  #grid{grid-template-columns:1fr}
}"""

def render_specialty_page(spec_key, records, all_specs, today):
    meta = SPECIALTY_META.get(spec_key, {
        "title": spec_key.title(),
        "h1": f"Private {spec_key.title()} in London",
        "noun": f"private {spec_key} clinics",
        "intro": f"Private {spec_key} services across London.",
    })
    slug = slugify(spec_key)
    title = meta["title"]
    h1 = meta["h1"]
    intro = meta["intro"]
    noun = meta["noun"]

    # Borough counts for this specialty
    borough_counts = Counter()
    for r in records:
        if r.get("ar"): borough_counts[r["ar"]] += 1

    borough_filters = ['<button class="borough-filter active" data-borough="all">All London</button>']
    for b, n in sorted(borough_counts.items(), key=lambda x: -x[1]):
        borough_filters.append(
            f'<button class="borough-filter" data-borough="{b}">{b} ({n})</button>'
        )

    cards_html = "\n".join(
        render_card(r, all_specs)
        for r in sorted(records, key=lambda x: x.get("n", ""))
    )

    # Related specialties (most popular other ones)
    related_specs = [s for s in all_specs if s != spec_key][:8]
    related_links = "".join(
        f'<a href="/private/{slugify(s)}/">{SPECIALTY_META.get(s,{}).get("title", s.title())}</a>'
        for s in related_specs
    )

    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": h1,
        "url": f"{BASE_URL}/private/{slug}/",
        "description": (f"Directory of {len(records)} CQC-registered {noun} in London. "
                        "Compare by rating, location and contact details."),
        "isPartOf": {
            "@type": "WebSite",
            "name": "London GP Directory",
            "url": BASE_URL,
        },
        "breadcrumb": {
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "Home", "item": BASE_URL},
                {"@type": "ListItem", "position": 2, "name": "Private clinics",
                 "item": f"{BASE_URL}/private/"},
                {"@type": "ListItem", "position": 3, "name": title,
                 "item": f"{BASE_URL}/private/{slug}/"},
            ],
        },
    }, separators=(",", ":"))

    return slug, f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{h1} — Compare {len(records)} Clinics</title>
<meta name="description" content="Compare {len(records)} private {title.lower().replace('private ','')} services in London. CQC ratings, addresses, websites &amp; contact details. Updated weekly.">
<link rel="canonical" href="{BASE_URL}/private/{slug}/">
<meta property="og:title" content="{h1}">
<meta property="og:description" content="Compare {len(records)} CQC-registered {noun} in London.">
<meta property="og:url" content="{BASE_URL}/private/{slug}/">
<meta property="og:type" content="website">
<meta name="theme-color" content="#A02670">
<script type="application/ld+json">{json_ld}</script>
<style>{SHARED_STYLES}</style>
</head>
<body>
<header class="hdr">
  <div class="hdr-in">
    <div class="crumbs"><a href="/">Home</a> ⟩ <a href="/private/">Private clinics</a> ⟩ <strong>{title}</strong></div>
    <h1>{h1.replace('Private', '<em>Private</em>', 1) if 'Private' in h1 else h1}</h1>
    <p class="hdr-sub">Compare every CQC-registered {noun} in London — addresses, websites, ratings and specialties.</p>
    <div class="stats">
      <div class="stat"><strong>{len(records)}</strong><span>Clinics</span></div>
      <div class="stat"><strong>{len(borough_counts)}</strong><span>London boroughs</span></div>
      <div class="stat"><strong>{sum(1 for r in records if (r.get('cqc') or '') in ('Good', 'Outstanding'))}</strong><span>Good or Outstanding</span></div>
    </div>
  </div>
</header>
<section class="intro-zone">
  <div class="intro-inner">{intro}</div>
</section>
<section class="filter-zone">
  <div class="filter-inner">
    <span class="filter-label">Borough:</span>
    {''.join(borough_filters)}
  </div>
</section>
<main class="wrap">
  <div class="results-bar" id="resCt">Showing <strong>{len(records)}</strong> {noun} across London</div>
  <div id="grid">{cards_html}</div>
</main>
<section class="related">
  <div class="related-inner">
    <h2>Other private specialties in London</h2>
    <div class="related-list">{related_links}</div>
  </div>
</section>
<footer>
  London GP Directory · Updated {today} · <a href="/">All London</a> · <a href="/private/">All specialties</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a>
</footer>
<script>
const filters = document.querySelectorAll('.borough-filter');
const cards = document.querySelectorAll('#grid .card');
const resCt = document.getElementById('resCt');
const TOTAL = {len(records)};
const NOUN = {json.dumps(noun)};

filters.forEach(f => f.addEventListener('click', () => {{
  const target = f.dataset.borough;
  filters.forEach(x => x.classList.toggle('active', x === f));
  let shown = 0;
  cards.forEach(c => {{
    const ok = target === 'all' || c.dataset.borough === target;
    c.style.display = ok ? '' : 'none';
    if (ok) shown++;
  }});
  const scope = target === 'all' ? 'across London' : 'in ' + target;
  resCt.innerHTML = `Showing <strong>${{shown}}</strong> of <strong>${{TOTAL}}</strong> ${{NOUN}} ${{scope}}`;
}}));
</script>
</body>
</html>"""

def render_index_page(by_specialty, total, today):
    """The /private/ landing page listing all specialties."""
    items_html = ""
    for spec_key, records in sorted(by_specialty.items(),
                                     key=lambda x: -len(x[1])):
        meta = SPECIALTY_META.get(spec_key, {})
        title = meta.get("title", spec_key.title())
        slug = slugify(spec_key)
        n = len(records)
        items_html += f"""<a href="/private/{slug}/" class="spec-tile">
          <div class="spec-tile-name">{title}</div>
          <div class="spec-tile-count">{n} clinic{'' if n==1 else 's'}</div>
        </a>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Private Clinics in London — Browse by Specialty</title>
<meta name="description" content="Browse {total} CQC-registered private healthcare clinics in London by specialty — psychiatry, cardiology, dermatology, private GPs and more.">
<link rel="canonical" href="{BASE_URL}/private/">
<meta name="theme-color" content="#A02670">
<style>{SHARED_STYLES}
.spec-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px}}
.spec-tile{{background:#fff;border:1px solid #ddd;border-radius:12px;padding:18px 20px;transition:all .15s}}
.spec-tile:hover{{border-color:#A02670;box-shadow:0 3px 14px rgba(160,38,112,.1);transform:translateY(-1px)}}
.spec-tile-name{{font-family:Georgia,serif;font-size:16px;font-weight:700;color:#A02670;margin-bottom:4px}}
.spec-tile-count{{font-size:12.5px;color:#888}}
</style>
</head>
<body>
<header class="hdr">
  <div class="hdr-in">
    <div class="crumbs"><a href="/">Home</a> ⟩ <strong>Private clinics</strong></div>
    <h1>Private Healthcare <em>Clinics</em> in London</h1>
    <p class="hdr-sub">Browse {total} CQC-registered private clinics in London by specialty. Faster access, longer consultations, specialist expertise — paid for directly or through private medical insurance.</p>
  </div>
</header>
<main class="wrap">
  <div class="spec-grid">
    {items_html}
  </div>
</main>
<footer>
  London GP Directory · Updated {today} · <a href="/">All London</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a> · <a href="/sources.html">Sources</a>
</footer>
</body>
</html>"""

def main():
    if not MERGED_JSON.exists():
        sys.exit(f"{MERGED_JSON} not found. Run merge_into_dataset.py first.")
    data = json.loads(MERGED_JSON.read_text())

    # Group private records by specialty (each clinic may appear under multiple)
    by_specialty = defaultdict(list)
    for r in data:
        if r.get("type") != "Private": continue
        for s in (r.get("specs") or []):
            by_specialty[s].append(r)

    print(f"Found {sum(1 for r in data if r.get('type')=='Private')} private clinics "
          f"across {len(by_specialty)} specialties.\n")

    today = datetime.now().strftime("%Y-%m-%d")
    all_specs = sorted(by_specialty.keys(), key=lambda s: -len(by_specialty[s]))

    OUT_DIR.mkdir(exist_ok=True)
    new_sitemap_urls = []

    # Individual specialty pages
    for spec_key, records in sorted(by_specialty.items()):
        slug, html = render_specialty_page(spec_key, records, all_specs, today)
        spec_dir = OUT_DIR / slug
        spec_dir.mkdir(exist_ok=True)
        (spec_dir / "index.html").write_text(html, encoding="utf-8")
        title = SPECIALTY_META.get(spec_key, {}).get("title", spec_key.title())
        print(f"  /private/{slug}/ — {title} ({len(records)} clinics)")
        new_sitemap_urls.append(
            f'  <url><loc>{BASE_URL}/private/{slug}/</loc>'
            f'<lastmod>{today}</lastmod><changefreq>weekly</changefreq>'
            f'<priority>0.7</priority></url>'
        )

    # Index page
    index_html = render_index_page(by_specialty,
                                    sum(1 for r in data if r.get("type")=="Private"),
                                    today)
    (OUT_DIR / "index.html").write_text(index_html, encoding="utf-8")
    new_sitemap_urls.insert(0,
        f'  <url><loc>{BASE_URL}/private/</loc>'
        f'<lastmod>{today}</lastmod><changefreq>weekly</changefreq>'
        f'<priority>0.8</priority></url>'
    )
    print(f"\n  /private/ — index page listing {len(by_specialty)} specialties")

    # Append to existing sitemap if it exists (don't clobber borough URLs)
    if SITEMAP.exists():
        existing = SITEMAP.read_text()
        # Strip closing urlset to insert new URLs before it
        if "</urlset>" in existing:
            head = existing.split("</urlset>")[0]
            new_xml = head + "\n".join(new_sitemap_urls) + "\n</urlset>\n"
            SITEMAP.write_text(new_xml, encoding="utf-8")
            print(f"\nAppended {len(new_sitemap_urls)} URLs to sitemap.xml")
        else:
            print("\n⚠️  sitemap.xml malformed — not updated. Re-run build_borough_pages.py.")
    else:
        sitemap_xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
        sitemap_xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        sitemap_xml += "\n".join(new_sitemap_urls) + "\n</urlset>\n"
        SITEMAP.write_text(sitemap_xml, encoding="utf-8")
        print(f"\nCreated sitemap.xml with {len(new_sitemap_urls)} URLs")

if __name__ == "__main__":
    main()
