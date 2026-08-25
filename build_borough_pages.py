#!/usr/bin/env python3
"""
Build per-borough hub pages at /practice/{borough-slug}/index.html.
Reads merged.json (combined NHS + Private dataset).

Key fix vs previous version: canonicalises borough names so the same place
isn't split across "Hammersmith and Fulham" (from postcodes.io via
fix_boroughs.py) and "Hammersmith & Fulham" (from merge_into_dataset.py's
hardcoded map). Without this, both keys slug to the same folder and the
alphabetically-later one silently overwrites the earlier - dropping half
the records.
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

# Canonicalise "&" boroughs - fix_boroughs.py uses "and" form,
# merge_into_dataset.py uses "&" form. Standardise on "&".
AMPERSAND_BOROUGHS = {
    "Hammersmith and Fulham": "Hammersmith & Fulham",
    "Barking and Dagenham":    "Barking & Dagenham",
    "Kensington and Chelsea":  "Kensington & Chelsea",
}
def canonical_borough(ar):
    return AMPERSAND_BOROUGHS.get((ar or "").strip(), ar)

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
    specs = d.get("specs", []) or []
    web = d.get("web", "")
    type_badge = '<span class="type-badge t-priv">Private</span>' if is_priv else '<span class="type-badge t-nhs">NHS</span>'
    spec_badges = ""
    if is_priv and specs:
        spec_badges = "".join(f'<span class="spec-badge">{s}</span>' for s in specs[:2])
    metrics = ""
    if not is_priv:
        s = d.get("s")
        c = d.get("c")
        s_bar = f'<div class="m-bar" style="width:{s}%;background:#0072CE"></div>' if s else ""
        c_bar = f'<div class="m-bar" style="width:{c}%;background:#0F6E56"></div>' if c else ""
        s_val = f'<div class="m-val">{s:.1f}%</div>' if s else '<div class="m-na">-</div>'
        c_val = f'<div class="m-val">{c:.1f}%</div>' if c else '<div class="m-na">-</div>'
        metrics = ('<div class="metrics">'
                   f'<div class="metric"><div class="m-lbl">Satisfaction</div><div class="m-track">{s_bar}</div>{s_val}</div>'
                   f'<div class="metric"><div class="m-lbl">Contact ease</div><div class="m-track">{c_bar}</div>{c_val}</div>'
                   '</div>')
    phone_html = f'<a class="card-phone" href="tel:{ph.replace(" ","")}">{ph}</a>' if ph else "<span></span>"
    cqc_btn = f'<a class="pill pill-cqc" href="{d.get("cu","")}" target="_blank">CQC</a>' if d.get("cu") else ""
    if is_priv:
        web_btn = f'<a class="pill pill-web" href="{web}" target="_blank">Website</a>' if web else ""
        actions = web_btn + cqc_btn
    else:
        actions = (f'<a class="pill pill-reg" href="https://gp-registration.nhs.uk/{o}" target="_blank">Register</a>'
                   + cqc_btn
                   + f'<a class="pill pill-ods" href="https://www.nhs.uk/services/gp-surgery/-/X{o}" target="_blank">NHS</a>')
    pslug = slugify(d.get("n", ""))
    bslug = slugify(d.get("ar", ""))
    page_url = f"/practice/{bslug}/{pslug}/" if pslug and bslug else ""
    name_html = (f'<a class="card-name-link" href="{page_url}">{name}</a>'
                 if page_url else name)
    return (f'<div class="card" data-type="{rec_type}" data-specs="{",".join(specs)}">'
            f'<div class="card-top"><div class="card-name">{name_html}</div>'
            f'<span class="cqc {cc}">{cqc_label}</span></div>'
            f'<div class="card-badges">{type_badge}{spec_badges}</div>'
            f'<div class="card-addr">{addr}{", " + pc if pc else ""}</div>'
            f'{metrics}'
            f'<div class="card-foot">{phone_html}<div class="actions">{actions}</div></div>'
            f'</div>')

def render_borough_page(borough, records, all_boroughs, today):
    slug = slugify(borough)
    nhs = [r for r in records if (r.get("type") or "NHS") == "NHS"]
    priv = [r for r in records if r.get("type") == "Private"]
    spec_counts = Counter()
    for r in priv:
        for s in (r.get("specs") or []):
            spec_counts[s] += 1
    cards_html = "\n".join(render_card(r) for r in sorted(records, key=lambda x: x.get("n", "")))
    chips_html = (f'<button class="specialty-chip active" data-spec="all">All <span class="specialty-chip-count">{len(priv)}</span></button>'
                  + "".join(f'<button class="specialty-chip" data-spec="{sp}">{sp} <span class="specialty-chip-count">{n}</span></button>'
                            for sp, n in spec_counts.most_common()))
    other_boroughs = sorted(b for b in all_boroughs if b != borough)
    borough_links = " ".join(f'<a href="/practice/{slugify(b)}/">{b}</a>' for b in other_boroughs[:8])
    nhs_with_score = [r for r in nhs if r.get("s")]
    avg_nhs_score = (sum(r.get("s") for r in nhs_with_score) / max(1, len(nhs_with_score)))
    good_or_outstanding = sum(1 for r in records if (r.get("cqc") or "") in ("Good", "Outstanding"))
    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": f"NHS GP Practices & Private Clinics in {borough}",
        "url": f"{BASE_URL}/practice/{slug}/",
        "description": f"Complete directory of NHS GP practices and private healthcare clinics in {borough}, London.",
        "isPartOf": {"@type": "WebSite", "name": "London GP Directory", "url": BASE_URL},
        "breadcrumb": {"@type": "BreadcrumbList", "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": BASE_URL},
            {"@type": "ListItem", "position": 2, "name": "Boroughs", "item": f"{BASE_URL}/boroughs/"},
            {"@type": "ListItem", "position": 3, "name": borough, "item": f"{BASE_URL}/practice/{slug}/"},
        ]},
    }, separators=(",", ":"))
    # Use string concatenation instead of f-string for the big HTML to avoid brace headaches
    html = (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>GP Practices & Private Clinics in {borough} - London GP Directory</title>\n'
        f'<meta name="description" content="Compare {len(nhs)} NHS GP practices and {len(priv)} private clinics in {borough}, London.">\n'
        f'<link rel="canonical" href="{BASE_URL}/practice/{slug}/">\n'
        f'<script type="application/ld+json">{json_ld}</script>\n'
        '<style>\n'
        '*{box-sizing:border-box;margin:0;padding:0}\n'
        "body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.5}\n"
        'a{text-decoration:none;color:inherit}\n'
        '.hdr{background:#003087;color:#fff;padding:0}\n'
        '.hdr-top{padding:14px 24px;border-bottom:1px solid rgba(255,255,255,.08)}\n'
        '.hdr-in{max-width:1300px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}\n'
        '.logo h1{font-family:Georgia,serif;font-size:1.4rem;font-weight:700;letter-spacing:-0.02em;line-height:1.1}\n'
        '.logo h1 em{color:#B5D4F4;font-style:italic;font-weight:400}\n'
        '.crumbs{font-size:12px;opacity:.65;margin-top:4px}\n'
        '.crumbs a{color:#B5D4F4}\n'
        '.main-nav{background:rgba(0,0,0,.18);border-bottom:4px solid #0072CE}\n'
        '.main-nav-in{max-width:1300px;margin:0 auto;padding:10px 24px;display:flex;flex-wrap:wrap;gap:8px 20px;font-size:.88rem;align-items:center}\n'
        '.main-nav a{color:rgba(255,255,255,.78);font-weight:500;transition:color .12s;padding:4px 0;white-space:nowrap}\n'
        '.main-nav a:hover,.main-nav a.active{color:#fff}\n'
        '.main-nav a.active{border-bottom:2px solid #B5D4F4}\n'
        '.hdr-page-title{background:#003087;padding:18px 24px;border-bottom:1px solid rgba(255,255,255,.08)}\n'
        '.hdr-page-title-in{max-width:1300px;margin:0 auto;color:#fff}\n'
        '.hdr-page-title-in h1{font-family:Georgia,serif;font-size:1.6rem;font-weight:700;line-height:1.15;margin-bottom:8px}\n'
        '.hdr-page-title-in h1 em{color:#B5D4F4;font-style:italic;font-weight:400}\n'
        '.hdr-sub{font-size:.9rem;opacity:.8;max-width:680px;margin-bottom:14px;line-height:1.45}\n'
        '.stats{display:flex;gap:28px;flex-wrap:wrap;margin-top:14px}\n'
        '.stat strong{display:block;font-size:1.4rem;font-weight:300}\n'
        '.stat span{font-size:.7rem;opacity:.6;text-transform:uppercase;letter-spacing:.05em}\n'
        '.type-zone{background:#fff;border-bottom:1px solid #e5e5e3;padding:14px 24px}\n'
        '.type-inner{max-width:1300px;margin:0 auto}\n'
        '.type-tabs{display:flex;gap:6px;flex-wrap:wrap}\n'
        '.type-tab{padding:8px 16px;border-radius:99px;border:1.5px solid #ddd;background:#fff;cursor:pointer;font-family:inherit;font-size:13.5px;font-weight:600;color:#555}\n'
        '.type-tab.active{background:#003087;color:#fff;border-color:#003087}\n'
        '.type-tab-count{font-size:11px;opacity:.7;margin-left:4px}\n'
        '.specialty-zone{margin-top:10px;display:none;flex-wrap:wrap;gap:5px}\n'
        '.specialty-zone.active{display:flex}\n'
        '.specialty-chip{padding:5px 11px;border-radius:99px;border:1px solid #ddd;background:#fff;cursor:pointer;font-size:12px;color:#666;text-transform:capitalize}\n'
        '.specialty-chip.active{background:#0072CE;color:#fff;border-color:#0072CE}\n'
        '.specialty-chip-count{font-size:10px;opacity:.7;margin-left:3px}\n'
        '.wrap{max-width:1300px;margin:0 auto;padding:24px}\n'
        '.results-bar{font-size:13px;color:#888;margin-bottom:14px}\n'
        '.results-bar strong{color:#222}\n'
        '#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:13px}\n'
        '.card{background:#fff;border:1px solid #ddd;border-radius:12px;padding:15px 16px;display:flex;flex-direction:column;position:relative;transition:box-shadow .15s,border-color .15s}\n'
        '.card:has(.card-name-link):hover{border-color:#003087;box-shadow:0 2px 10px rgba(0,48,135,.12);cursor:pointer}\n'
        '.card-name-link{color:inherit;text-decoration:none}\n'
        '.card-name-link::after{content:"";position:absolute;inset:0;border-radius:inherit}\n'
        '.card .card-phone,.card .actions,.card .pill{position:relative;z-index:1}\n'
        '.card-top{display:flex;justify-content:space-between;align-items:flex-start;gap:8px;margin-bottom:7px}\n'
        '.card-name{font-family:Georgia,serif;font-size:14px;font-weight:700;color:#003087;flex:1;line-height:1.3}\n'
        '.cqc{flex-shrink:0;font-size:9.5px;font-weight:600;padding:2px 8px;border-radius:99px;white-space:nowrap}\n'
        '.cqc-O{background:#E1F5EE;color:#0F6E56}.cqc-G{background:#D8EFE3;color:#007F3B}\n'
        '.cqc-R{background:#FAEEDA;color:#BA7517}.cqc-I{background:#FCEBEB;color:#A32D2D}.cqc-N{background:#f0f0ee;color:#777}\n'
        '.card-badges{display:flex;flex-wrap:wrap;gap:4px;margin-bottom:7px}\n'
        '.type-badge{font-size:10px;font-weight:600;padding:2px 8px;border-radius:99px;text-transform:uppercase}\n'
        '.type-badge.t-nhs{background:#EDF4FC;color:#003087}\n'
        '.type-badge.t-priv{background:#FAE7F3;color:#A02670}\n'
        '.spec-badge{font-size:10px;padding:2px 8px;border-radius:99px;background:#F5F0E8;color:#7A5D2F;text-transform:capitalize}\n'
        '.card-addr{font-size:11.5px;color:#888;margin-bottom:10px}\n'
        '.metrics{display:flex;gap:12px;margin-bottom:12px}\n'
        '.metric{flex:1}\n'
        '.m-lbl{font-size:9px;text-transform:uppercase;color:#aaa;margin-bottom:2px}\n'
        '.m-track{height:3px;background:#eee;border-radius:99px;overflow:hidden;margin-bottom:2px}\n'
        '.m-bar{height:100%;border-radius:99px}\n'
        '.m-val{font-size:11.5px;font-weight:600;color:#444}\n'
        '.m-na{font-size:11.5px;color:#ccc}\n'
        '.card-foot{display:flex;align-items:center;justify-content:space-between;border-top:1px solid #f0f0ee;padding-top:10px;gap:8px;margin-top:auto}\n'
        '.card-phone{font-size:11.5px;color:#444;font-weight:500}\n'
        '.actions{display:flex;gap:5px;flex-wrap:wrap;justify-content:flex-end}\n'
        '.pill{font-size:10.5px;padding:4px 9px;border-radius:6px;font-weight:600;white-space:nowrap}\n'
        '.pill-reg{background:#003087;color:#fff}.pill-cqc{background:#D8EFE3;color:#007F3B}\n'
        '.pill-ods{background:#EDF4FC;color:#0072CE}.pill-web{background:#FAE7F3;color:#A02670}\n'
        '.bottom-nav{background:#fff;border-top:1px solid #e5e5e3;padding:18px 24px;margin-top:32px}\n'
        '.bottom-nav-inner{max-width:1300px;margin:0 auto;text-align:center;font-size:13px;color:#888}\n'
        '.bottom-nav a{color:#003087;font-weight:600;margin:0 6px}\n'
        'footer{background:#003087;color:rgba(255,255,255,.5);text-align:center;padding:14px 24px;font-size:11.5px}\n'
        'footer a{color:rgba(255,255,255,.8)}\n'
        '@media(max-width:600px){.hdr-top{padding:10px 16px}.hdr-page-title{padding:14px 16px}.hdr-page-title-in h1{font-size:1.3rem}.main-nav-in{padding:8px 16px;gap:6px 14px;font-size:.82rem}.wrap{padding:16px}#grid{grid-template-columns:1fr}}\n'
        '</style>\n'
        '</head>\n<body>\n'
        '<header class="hdr">\n'
        '  <div class="hdr-top"><div class="hdr-in"><div class="logo"><h1>London GP <em>Directory</em></h1></div></div></div>\n'
        '  <nav class="main-nav" aria-label="Main navigation"><div class="main-nav-in">'
        '<a href="/">Search</a><a href="/boroughs/">Boroughs</a><a href="/nhs-services/">NHS Services</a>'
        '<a href="/private/">Private Clinics</a><a href="/dentists/">Dentists</a><a href="/guides/">Guides</a>'
        '<a href="/methodology.html">Methodology</a><a href="/sources.html">Sources</a>'
        '</div></nav>\n'
        '  <div class="hdr-page-title"><div class="hdr-page-title-in">'
        f'<div class="crumbs"><a href="/">Home</a> &rsaquo; <a href="/boroughs/">Boroughs</a> &rsaquo; <strong>{borough}</strong></div>'
        f'<h1>NHS GP Practices &amp; Private Clinics in <em>{borough}</em></h1>'
        f'<p class="hdr-sub">Compare every NHS GP practice and private healthcare clinic in {borough}.</p>'
        '<div class="stats">'
        f'<div class="stat"><strong>{len(nhs)}</strong><span>NHS practices</span></div>'
        f'<div class="stat"><strong>{len(priv)}</strong><span>Private clinics</span></div>'
        f'<div class="stat"><strong>{good_or_outstanding}</strong><span>Good or Outstanding</span></div>'
        f'<div class="stat"><strong>{avg_nhs_score:.1f}%</strong><span>Avg NHS patient score</span></div>'
        '</div></div></div>\n'
        '</header>\n'
        '<div class="type-zone"><div class="type-inner">'
        '<div class="type-tabs" id="typeTabs">'
        f'<button class="type-tab active" data-type="NHS">NHS practices <span class="type-tab-count">{len(nhs)}</span></button>'
        f'<button class="type-tab" data-type="Private">Private clinics <span class="type-tab-count">{len(priv)}</span></button>'
        f'<button class="type-tab" data-type="All">All <span class="type-tab-count">{len(records)}</span></button>'
        '</div>'
        f'<div class="specialty-zone" id="specialtyZone">{chips_html}</div>'
        '</div></div>\n'
        f'<main class="wrap"><div class="results-bar" id="resCt">Showing <strong>{len(nhs)}</strong> NHS practices in {borough}</div>'
        f'<div id="grid">{cards_html}</div></main>\n'
        f'<nav class="bottom-nav"><div class="bottom-nav-inner">Other boroughs: {borough_links} <a href="/boroughs/">All 32 ></a></div></nav>\n'
        f'<footer>London GP Directory · Data refreshed {today} · <a href="/">Home</a> · <a href="/boroughs/">Boroughs</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a></footer>\n'
        '<script>\n'
        "const tabs = document.querySelectorAll('.type-tab');\n"
        "const chipsZone = document.getElementById('specialtyZone');\n"
        "const resCt = document.getElementById('resCt');\n"
        f"const BOROUGH = {json.dumps(borough)};\n"
        f"const TOTAL_NHS = {len(nhs)};\n"
        f"const TOTAL_PRIV = {len(priv)};\n"
        "let selType = 'NHS';\n"
        "let selSpec = 'all';\n"
        "function applyFilters() {\n"
        "  let shown = 0;\n"
        "  document.querySelectorAll('#grid .card').forEach(card => {\n"
        "    const t = card.dataset.type;\n"
        "    const specs = (card.dataset.specs || '').split(',').filter(Boolean);\n"
        "    const typeOk = selType === 'All' || t === selType;\n"
        "    const specOk = selType !== 'Private' || selSpec === 'all' || specs.includes(selSpec);\n"
        "    if (typeOk && specOk) { card.style.display = ''; shown++; }\n"
        "    else card.style.display = 'none';\n"
        "  });\n"
        "  const totalForType = selType === 'NHS' ? TOTAL_NHS : selType === 'Private' ? TOTAL_PRIV : TOTAL_NHS + TOTAL_PRIV;\n"
        "  const label = selType === 'NHS' ? 'NHS practices' : selType === 'Private' ? 'private clinics' : 'practices & clinics';\n"
        "  resCt.innerHTML = `Showing <strong>${shown}</strong> of <strong>${totalForType}</strong> ${label} in ${BOROUGH}`;\n"
        "}\n"
        "tabs.forEach(tab => tab.addEventListener('click', () => {\n"
        "  selType = tab.dataset.type; selSpec = 'all';\n"
        "  tabs.forEach(t => t.classList.toggle('active', t === tab));\n"
        "  chipsZone.classList.toggle('active', selType === 'Private');\n"
        "  chipsZone.querySelectorAll('.specialty-chip').forEach(c => c.classList.toggle('active', c.dataset.spec === 'all'));\n"
        "  applyFilters();\n"
        "}));\n"
        "chipsZone.querySelectorAll('.specialty-chip').forEach(chip => chip.addEventListener('click', () => {\n"
        "  selSpec = chip.dataset.spec;\n"
        "  chipsZone.querySelectorAll('.specialty-chip').forEach(c => c.classList.toggle('active', c === chip));\n"
        "  applyFilters();\n"
        "}));\n"
        "applyFilters();\n"
        '</script>\n</body>\n</html>'
    )
    return slug, html

def build_sitemap_entry(slug, today):
    return (f'  <url><loc>{BASE_URL}/practice/{slug}/</loc>'
            f'<lastmod>{today}</lastmod><changefreq>weekly</changefreq>'
            f'<priority>0.8</priority></url>')

def main():
    if not MERGED_JSON.exists():
        sys.exit(f"{MERGED_JSON} not found. Run merge_into_dataset.py first.")
    data = json.loads(MERGED_JSON.read_text())
    today = datetime.now().strftime("%Y-%m-%d")
    by_borough = defaultdict(list)
    for r in data:
        ar = canonical_borough(r.get("ar"))  # <-- normalisation applied
        if ar: by_borough[ar].append(r)
    all_boroughs = sorted(by_borough.keys())
    print(f"Building {len(all_boroughs)} borough pages...")
    OUT_DIR.mkdir(exist_ok=True)
    sitemap_entries = [f'  <url><loc>{BASE_URL}/</loc><lastmod>{today}</lastmod><changefreq>weekly</changefreq><priority>1.0</priority></url>']
    for borough, records in sorted(by_borough.items()):
        slug, html = render_borough_page(borough, records, all_boroughs, today)
        borough_dir = OUT_DIR / slug
        borough_dir.mkdir(exist_ok=True)
        (borough_dir / "index.html").write_text(html, encoding="utf-8")
        nhs_count = sum(1 for r in records if (r.get("type") or "NHS") == "NHS")
        priv_count = sum(1 for r in records if r.get("type") == "Private")
        print(f"  /practice/{slug}/ - {len(records)} total ({nhs_count} NHS + {priv_count} Private)")
        sitemap_entries.append(build_sitemap_entry(slug, today))
    sitemap_xml = '<?xml version="1.0" encoding="UTF-8"?>\n<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n' + "\n".join(sitemap_entries) + "\n</urlset>\n"
    SITEMAP.write_text(sitemap_xml, encoding="utf-8")
    print(f"\nWrote sitemap.xml - {len(sitemap_entries)} URLs.")

if __name__ == "__main__":
    main()
