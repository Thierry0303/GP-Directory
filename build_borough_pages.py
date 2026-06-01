#!/usr/bin/env python3
"""
Build per-borough hub pages at /practice/{borough-slug}/index.html.

Each page now mirrors the homepage:
  - NHS / Private / All segmented tabs with live counts (borough-scoped).
  - Specialty chips when Private is active.
  - Same card rendering as homepage (NHS or Private with appropriate badges).
  - SEO-tuned title/description/JSON-LD CollectionPage for the borough.

Reads merged.json (combined NHS + Private dataset produced by
merge_into_dataset.py).
"""

import json, re, sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MERGED_JSON = ROOT / "merged.json"
OUT_DIR = ROOT / "practice"
SITEMAP = ROOT / "sitemap.xml"

BASE_URL = "https://londongp.directory"

def slugify(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower().replace("&", "and")).strip("-")

def cqc_class(r):
    if not r: return "cqc-N"
    if r == "Outstanding": return "cqc-O"
    if r == "Good":        return "cqc-G"
    if r.startswith("Requires"): return "cqc-R"
    if r == "Inadequate":  return "cqc-I"
    return "cqc-N"

def render_card(d):
    rec_type = d.get("type") or "NHS"
    is_priv = rec_type == "Private"
    cc = cqc_class(d.get("cqc"))
    cqc_label = d.get("cqc") or "Not rated"
    name = d.get("n", "")
    addr = d.get("a", "")
    pc = d.get("p", "")
    ph = d.get("ph", "")
    o = d.get("o", "")
    ar = d.get("ar", "")
    specs = d.get("specs", []) or []
    web = d.get("web", "")

    type_badge = (f'<span class="type-badge t-priv">Private</span>'
                  if is_priv else
                  f'<span class="type-badge t-nhs">NHS</span>')
    spec_badges = ""
    if is_priv and specs:
        spec_badges = "".join(
            f'<span class="spec-badge">{s}</span>' for s in specs[:2]
        )

    metrics = ""
    if not is_priv:
        s = d.get("s")
        c = d.get("c")
        s_bar = f'<div class="m-bar" style="width:{s}%;background:#0072CE"></div>' if s else ""
        c_bar = f'<div class="m-bar" style="width:{c}%;background:#0F6E56"></div>' if c else ""
        s_val = f'<div class="m-val">{s:.1f}%</div>' if s else '<div class="m-na">—</div>'
        c_val = f'<div class="m-val">{c:.1f}%</div>' if c else '<div class="m-na">—</div>'
        metrics = f"""<div class="metrics">
          <div class="metric"><div class="m-lbl">Satisfaction</div><div class="m-track">{s_bar}</div>{s_val}</div>
          <div class="metric"><div class="m-lbl">Contact ease</div><div class="m-track">{c_bar}</div>{c_val}</div>
        </div>"""

    phone_html = (f'<a class="card-phone" href="tel:{ph.replace(" ","")}">📞 {ph}</a>'
                  if ph else "<span></span>")
    cqc_btn = (f'<a class="pill pill-cqc" href="{d.get("cu","")}" target="_blank">CQC</a>'
               if d.get("cu") else "")
    if is_priv:
        web_btn = f'<a class="pill pill-web" href="{web}" target="_blank">Website →</a>' if web else ""
        actions = f"{web_btn}{cqc_btn}"
    else:
        actions = (
            f'<a class="pill pill-reg" href="https://gp-registration.nhs.uk/{o}" target="_blank">Register →</a>'
            f'{cqc_btn}'
            f'<a class="pill pill-ods" href="https://www.nhs.uk/services/gp-surgery/-/X{o}" target="_blank">NHS</a>'
        )

    return f"""<div class="card" data-type="{rec_type}" data-specs="{','.join(specs)}">
      <div class="card-top">
        <div class="card-name">{name}</div>
        <span class="cqc {cc}">{cqc_label}</span>
      </div>
      <div class="card-badges">{type_badge}{spec_badges}</div>
      <div class="card-addr">{addr}{', ' + pc if pc else ''}</div>
      {metrics}
      <div class="card-foot">
        {phone_html}
        <div class="actions">{actions}</div>
      </div>
    </div>"""

def render_borough_page(borough, records, all_boroughs, today):
    slug = slugify(borough)
    nhs = [r for r in records if (r.get("type") or "NHS") == "NHS"]
    priv = [r for r in records if r.get("type") == "Private"]

    # Specialty counts (for Private chips)
    spec_counts = Counter()
    for r in priv:
        for s in (r.get("specs") or []):
            spec_counts[s] += 1

    cards_html = "\n".join(render_card(r) for r in sorted(records, key=lambda x: x.get("n", "")))

    chips_html = (
        f'<button class="specialty-chip active" data-spec="all">All <span class="specialty-chip-count">{len(priv)}</span></button>'
        + "".join(
            f'<button class="specialty-chip" data-spec="{sp}">{sp} <span class="specialty-chip-count">{n}</span></button>'
            for sp, n in spec_counts.most_common()
        )
    )

    # Other borough nav
    other_boroughs = sorted(b for b in all_boroughs if b != borough)
    borough_links = " ".join(
        f'<a href="/practice/{slugify(b)}/">{b}</a>' for b in other_boroughs[:8]
    )

    avg_nhs_score = (sum(r.get("s") or 0 for r in nhs if r.get("s")) /
                     max(1, sum(1 for r in nhs if r.get("s"))))

    # Stats for the hero section
    good_or_outstanding = sum(
        1 for r in records
        if (r.get("cqc") or "") in ("Good", "Outstanding")
    )

    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": f"NHS GP Practices & Private Clinics in {borough}",
        "url": f"{BASE_URL}/practice/{slug}/",
        "description": (f"Complete directory of NHS GP practices and private "
                        f"healthcare clinics in {borough}, London — compare "
                        f"by CQC rating, patient satisfaction and specialty."),
        "isPartOf": {
            "@type": "WebSite",
            "name": "London GP Directory",
            "url": BASE_URL,
        },
        "breadcrumb": {
            "@type": "BreadcrumbList",
            "itemListElement": [
                {"@type": "ListItem", "position": 1, "name": "Home", "item": BASE_URL},
                {"@type": "ListItem", "position": 2, "name": "Boroughs",
                 "item": f"{BASE_URL}/practice/"},
                {"@type": "ListItem", "position": 3, "name": borough,
                 "item": f"{BASE_URL}/practice/{slug}/"},
            ],
        },
    }, separators=(",", ":"))

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GP Practices &amp; Private Clinics in {borough} — London GP Directory</title>
<meta name="description" content="Compare {len(nhs)} NHS GP practices and {len(priv)} private clinics in {borough}, London. Patient satisfaction, CQC ratings, specialties &amp; contact details.">
<link rel="canonical" href="{BASE_URL}/practice/{slug}/">
<meta property="og:title" content="GP Practices &amp; Private Clinics in {borough} — London GP Directory">
<meta property="og:description" content="Compare {len(nhs)} NHS GPs and {len(priv)} private clinics in {borough}.">
<meta property="og:url" content="{BASE_URL}/practice/{slug}/">
<meta property="og:type" content="website">
<meta name="theme-color" content="#003087">
<script type="application/ld+json">{json_ld}</script>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.5}}
a{{text-decoration:none;color:inherit}}
.hdr{{background:#003087;color:#fff;padding:24px;border-bottom:4px solid #0072CE}}
.hdr-in{{max-width:1300px;margin:0 auto}}
.crumbs{{font-size:12px;opacity:.65;margin-bottom:10px}}
.crumbs a{{color:#B5D4F4}}
.hdr h1{{font-family:Georgia,serif;font-size:1.7rem;font-weight:700;line-height:1.15;margin-bottom:8px}}
.hdr h1 em{{color:#B5D4F4;font-style:italic;font-weight:400}}
.hdr-sub{{font-size:.9rem;opacity:.8;max-width:680px;margin-bottom:14px;line-height:1.45}}
.stats{{display:flex;gap:28px;flex-wrap:wrap;margin-top:14px}}
.stat strong{{display:block;font-size:1.4rem;font-weight:300}}
.stat span{{font-size:.7rem;opacity:.6;text-transform:uppercase;letter-spacing:.05em}}
.type-zone{{background:#fff;border-bottom:1px solid #e5e5e3;padding:14px 24px}}
.type-inner{{max-width:1300px;margin:0 auto}}
.type-tabs{{display:flex;gap:6px;flex-wrap:wrap}}
.type-tab{{padding:8px 16px;border-radius:99px;border:1.5px solid #ddd;background:#fff;cursor:pointer;font-family:inherit;font-size:13.5px;font-weight:600;color:#555;transition:all .15s}}
.type-tab.active{{background:#003087;color:#fff;border-color:#003087}}
.type-tab-count{{font-size:11px;opacity:.7;margin-left:4px}}
.specialty-zone{{margin-top:10px;display:none;flex-wrap:wrap;gap:5px}}
.specialty-zone.active{{display:flex}}
.specialty-chip{{padding:5px 11px;border-radius:99px;border:1px solid #ddd;background:#fff;cursor:pointer;font-size:12px;color:#666;text-transform:capitalize}}
.specialty-chip.active{{background:#0072CE;color:#fff;border-color:#0072CE}}
.specialty-chip-count{{font-size:10px;opacity:.7;margin-left:3px}}
.wrap{{max-width:1300px;margin:0 auto;padding:24px}}
.results-bar{{font-size:13px;color:#888;margin-bottom:14px}}
.results-bar strong{{color:#222}}
#grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:13px}}
.card{{background:#fff;border:1px solid #ddd;border-radius:12px;padding:15px 16px;display:flex;flex-direction:column}}
.card-top{{display:flex;justify-content:space-between;align-items:flex-start;gap:8px;margin-bottom:7px}}
.card-name{{font-family:Georgia,serif;font-size:14px;font-weight:700;color:#003087;flex:1;line-height:1.3}}
.cqc{{flex-shrink:0;font-size:9.5px;font-weight:600;padding:2px 8px;border-radius:99px;white-space:nowrap}}
.cqc-O{{background:#E1F5EE;color:#0F6E56}}.cqc-G{{background:#D8EFE3;color:#007F3B}}
.cqc-R{{background:#FAEEDA;color:#BA7517}}.cqc-I{{background:#FCEBEB;color:#A32D2D}}.cqc-N{{background:#f0f0ee;color:#777}}
.card-badges{{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:7px}}
.type-badge{{font-size:10px;font-weight:600;padding:2px 8px;border-radius:99px;text-transform:uppercase;letter-spacing:.04em}}
.type-badge.t-nhs{{background:#EDF4FC;color:#003087}}
.type-badge.t-priv{{background:#FAE7F3;color:#A02670}}
.spec-badge{{font-size:10px;padding:2px 8px;border-radius:99px;background:#F5F0E8;color:#7A5D2F;text-transform:capitalize}}
.card-addr{{font-size:11.5px;color:#888;margin-bottom:10px;line-height:1.4}}
.metrics{{display:flex;gap:12px;margin-bottom:12px}}
.metric{{flex:1}}
.m-lbl{{font-size:9px;text-transform:uppercase;color:#aaa;margin-bottom:2px}}
.m-track{{height:3px;background:#eee;border-radius:99px;overflow:hidden;margin-bottom:2px}}
.m-bar{{height:100%;border-radius:99px}}
.m-val{{font-size:11.5px;font-weight:600;color:#444}}
.m-na{{font-size:11.5px;color:#ccc}}
.card-foot{{display:flex;align-items:center;justify-content:space-between;border-top:1px solid #f0f0ee;padding-top:10px;gap:8px;margin-top:auto}}
.card-phone{{font-size:11.5px;color:#444;font-weight:500}}
.actions{{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}}
.pill{{font-size:10.5px;padding:4px 9px;border-radius:6px;font-weight:600;white-space:nowrap}}
.pill-reg{{background:#003087;color:#fff}}.pill-cqc{{background:#D8EFE3;color:#007F3B}}
.pill-ods{{background:#EDF4FC;color:#0072CE}}.pill-web{{background:#FAE7F3;color:#A02670}}
.bottom-nav{{background:#fff;border-top:1px solid #e5e5e3;padding:18px 24px;margin-top:32px}}
.bottom-nav-inner{{max-width:1300px;margin:0 auto;text-align:center;font-size:13px;color:#888}}
.bottom-nav a{{color:#003087;font-weight:600;margin:0 6px}}
.bottom-nav a:hover{{text-decoration:underline}}
.empty{{text-align:center;padding:4rem 2rem;color:#888}}
footer{{background:#003087;color:rgba(255,255,255,.5);text-align:center;padding:14px 24px;font-size:11.5px}}
footer a{{color:rgba(255,255,255,.8)}}
@media(max-width:600px){{
  .hdr{{padding:18px 16px}}
  .hdr h1{{font-size:1.3rem}}
  .stats{{gap:14px}}
  .type-zone{{padding:12px 16px}}
  .wrap{{padding:16px}}
  #grid{{grid-template-columns:1fr}}
}}
</style>
</head>
<body>
<header class="hdr">
  <div class="hdr-in">
    <div class="crumbs"><a href="/">Home</a> ⟩ Boroughs ⟩ <strong>{borough}</strong></div>
    <h1>NHS GP Practices &amp; Private Clinics in <em>{borough}</em></h1>
    <p class="hdr-sub">Compare every NHS GP practice and private healthcare clinic in {borough} — by CQC rating, patient survey, contact details and specialty.</p>
    <div class="stats">
      <div class="stat"><strong>{len(nhs)}</strong><span>NHS practices</span></div>
      <div class="stat"><strong>{len(priv)}</strong><span>Private clinics</span></div>
      <div class="stat"><strong>{good_or_outstanding}</strong><span>Good or Outstanding</span></div>
      <div class="stat"><strong>{avg_nhs_score:.1f}%</strong><span>Avg NHS patient score</span></div>
    </div>
  </div>
</header>
<div class="type-zone">
  <div class="type-inner">
    <div class="type-tabs" id="typeTabs">
      <button class="type-tab active" data-type="NHS">NHS practices <span class="type-tab-count">{len(nhs)}</span></button>
      <button class="type-tab" data-type="Private">Private clinics <span class="type-tab-count">{len(priv)}</span></button>
      <button class="type-tab" data-type="All">All <span class="type-tab-count">{len(records)}</span></button>
    </div>
    <div class="specialty-zone" id="specialtyZone">{chips_html}</div>
  </div>
</div>
<main class="wrap">
  <div class="results-bar" id="resCt">Showing <strong>{len(nhs)}</strong> NHS practices in {borough}</div>
  <div id="grid">{cards_html}</div>
</main>
<nav class="bottom-nav">
  <div class="bottom-nav-inner">Other boroughs: {borough_links} <a href="/">All London ›</a></div>
</nav>
<footer>
  London GP Directory · Data refreshed {today} · <a href="/">All London</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a> · <a href="/sources.html">Sources</a>
</footer>
<script>
const tabs = document.querySelectorAll('.type-tab');
const chipsZone = document.getElementById('specialtyZone');
const grid = document.getElementById('grid');
const resCt = document.getElementById('resCt');
const BOROUGH = {json.dumps(borough)};
const TOTAL_NHS = {len(nhs)};
const TOTAL_PRIV = {len(priv)};
let selType = 'NHS';
let selSpec = 'all';

function applyFilters() {{
  let shown = 0;
  document.querySelectorAll('#grid .card').forEach(card => {{
    const t = card.dataset.type;
    const specs = (card.dataset.specs || '').split(',').filter(Boolean);
    const typeOk = selType === 'All' || t === selType;
    const specOk = selType !== 'Private' || selSpec === 'all' || specs.includes(selSpec);
    if (typeOk && specOk) {{ card.style.display = ''; shown++; }}
    else card.style.display = 'none';
  }});
  const totalForType = selType === 'NHS' ? TOTAL_NHS
                     : selType === 'Private' ? TOTAL_PRIV
                     : TOTAL_NHS + TOTAL_PRIV;
  const label = selType === 'NHS' ? 'NHS practices'
              : selType === 'Private' ? 'private clinics'
              : 'practices &amp; clinics';
  resCt.innerHTML = `Showing <strong>${{shown}}</strong> of <strong>${{totalForType}}</strong> ${{label}} in ${{BOROUGH}}`;
}}

tabs.forEach(tab => tab.addEventListener('click', () => {{
  selType = tab.dataset.type;
  selSpec = 'all';
  tabs.forEach(t => t.classList.toggle('active', t === tab));
  chipsZone.classList.toggle('active', selType === 'Private');
  chipsZone.querySelectorAll('.specialty-chip').forEach(c =>
    c.classList.toggle('active', c.dataset.spec === 'all')
  );
  applyFilters();
}}));

chipsZone.querySelectorAll('.specialty-chip').forEach(chip => chip.addEventListener('click', () => {{
  selSpec = chip.dataset.spec;
  chipsZone.querySelectorAll('.specialty-chip').forEach(c => c.classList.toggle('active', c === chip));
  applyFilters();
}}));

applyFilters();
</script>
</body>
</html>"""
    return slug, html

def build_sitemap_entry(slug, today):
    return (f'  <url><loc>{BASE_URL}/practice/{slug}/</loc>'
            f'<lastmod>{today}</lastmod><changefreq>weekly</changefreq>'
            f'<priority>0.8</priority></url>')

def main():
    if not MERGED_JSON.exists():
        sys.exit(f"{MERGED_JSON} not found. Run refresh_nhs_data.py + merge_into_dataset.py first.")
    data = json.loads(MERGED_JSON.read_text())
    if not isinstance(data, list):
        sys.exit("merged.json is not a JSON array.")

    today = datetime.now().strftime("%Y-%m-%d")
    by_borough = defaultdict(list)
    for r in data:
        ar = r.get("ar")
        if ar: by_borough[ar].append(r)

    all_boroughs = sorted(by_borough.keys())
    print(f"Building {len(all_boroughs)} borough pages…")

    OUT_DIR.mkdir(exist_ok=True)
    sitemap_entries = [
        f'  <url><loc>{BASE_URL}/</loc><lastmod>{today}</lastmod><changefreq>weekly</changefreq><priority>1.0</priority></url>'
    ]

    for borough, records in sorted(by_borough.items()):
        slug, html = render_borough_page(borough, records, all_boroughs, today)
        borough_dir = OUT_DIR / slug
        borough_dir.mkdir(exist_ok=True)
        (borough_dir / "index.html").write_text(html, encoding="utf-8")
        nhs_count = sum(1 for r in records if (r.get("type") or "NHS") == "NHS")
        priv_count = sum(1 for r in records if r.get("type") == "Private")
        print(f"  /practice/{slug}/ — {len(records)} total ({nhs_count} NHS + {priv_count} Private)")
        sitemap_entries.append(build_sitemap_entry(slug, today))

    sitemap_xml = '<?xml version="1.0" encoding="UTF-8"?>\n'
    sitemap_xml += '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
    sitemap_xml += "\n".join(sitemap_entries)
    sitemap_xml += "\n</urlset>\n"
    SITEMAP.write_text(sitemap_xml, encoding="utf-8")
    print(f"\nWrote sitemap.xml — {len(sitemap_entries)} URLs.")

if __name__ == "__main__":
    main()
