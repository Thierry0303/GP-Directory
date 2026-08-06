#!/usr/bin/env python3
"""
build_dentist_pages.py — builds /dentists/ pages from dentists.json.

Structure mirrors build_specialty_pages.py:
  /dentists/index.html            — hub, one card per borough
  /dentists/{borough-slug}/       — all dentists in that borough, with
                                     orthodontics/cosmetic/implants filter chips

Borough-first (not specialty-first like /private/) because dentist search
intent is "near me", not "which specialty" — people search
"dentist in Hackney", not "orthodontist London".

Run: python3 build_dentist_pages.py   (after gen_dentists.py)
"""
import json, re
from pathlib import Path
from collections import defaultdict

ROOT      = Path(__file__).resolve().parent
DATA      = ROOT / "dentists.json"
OUT_DIR   = ROOT / "dentists"

NAV = """<nav class="site-nav">
  <a class="brand" href="/">London GP <em>Directory</em></a>
  <a href="/">Search</a>
  <a href="/boroughs/">Boroughs</a>
  <a href="/nhs-services/">NHS Services</a>
  <a href="/private/">Private Clinics</a>
  <a href="/dentists/" class="active">Dentists</a>
  <a href="/methodology.html">Methodology</a>
  <a href="/sources.html">Sources</a>
</nav>"""

# Reuses the same visual language as the rest of the site (navy #003087 /
# blue #0072CE) so it doesn't look bolted on.
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
.filter-chip{padding:4px 12px;border-radius:16px;border:1.5px solid #ddd;background:#fff;cursor:pointer;font-size:.78rem;font-weight:500;color:#555}
.filter-chip.active{background:#003087;color:#fff;border-color:#003087}
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:12px}
.card{background:#fff;border-radius:10px;padding:16px;border:1px solid #e8e8e8}
.card:hover{box-shadow:0 2px 12px rgba(0,0,0,.09)}
.card-name{font-family:Georgia,serif;font-weight:700;font-size:.9rem;color:#003087;margin-bottom:4px;line-height:1.3}
.card-addr{font-size:.78rem;color:#777;margin-bottom:8px}
.card-tags{display:flex;flex-wrap:wrap;gap:5px;margin-bottom:8px}
.tag{padding:2px 8px;border-radius:8px;font-size:.68rem;font-weight:600}
.tag.dent{background:#eef2ff;color:#3730a3}
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
.btn-phone{color:#003087;font-size:.78rem}
.spec-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:14px;margin-top:20px}
.spec-card{background:#fff;border-radius:12px;padding:15px 16px;border:1px solid #ddd;display:block;color:inherit}
.spec-card:hover{box-shadow:0 2px 10px rgba(0,48,135,.12);border-color:#003087}
.spec-card h2{font-family:Georgia,serif;font-size:.95rem;font-weight:700;color:#003087;margin-bottom:4px}
.spec-count{font-size:.75rem;color:#888;margin-top:6px}
.spec-count strong{color:#003087}
.back{display:inline-flex;align-items:center;gap:6px;color:#003087;font-size:.85rem;margin-bottom:16px}
.notice{background:#fffbeb;border:1px solid #fde68a;border-radius:8px;padding:10px 14px;font-size:.78rem;color:#92400e;margin-bottom:16px}
footer{text-align:center;padding:32px 24px;font-size:.78rem;color:#999;border-top:1px solid #e8e8e8;margin-top:40px}
.hidden{display:none!important}
@media(max-width:600px){.cards{grid-template-columns:1fr}.stats{gap:16px}}
</style>"""

FILTER_JS = """
<script>
(function(){
  var cards = Array.from(document.querySelectorAll('.card'));
  var active = '';
  function apply(){
    cards.forEach(function(c){
      var ok = !active || (c.dataset.tags || '').split(',').includes(active);
      c.classList.toggle('hidden', !ok);
    });
  }
  document.querySelectorAll('.filter-chip').forEach(function(btn){
    btn.addEventListener('click', function(){
      document.querySelectorAll('.filter-chip').forEach(b=>b.classList.remove('active'));
      active = btn.dataset.tag || '';
      btn.classList.add('active');
      apply();
    });
  });
})();
</script>
"""

def slugify(s):
    return re.sub(r'[^a-z0-9]+', '-', s.lower().replace('&', 'and').replace("'", '')).strip('-')

def cqc_class(r):
    return {'Outstanding':'O','Good':'G','Requires improvement':'RI','Inadequate':'I'}.get(r, '')

def render_card(d):
    rating = d.get('cqc_rating', '')
    badge  = f'<span class="cqc-badge cqc-{cqc_class(rating)}">{rating}</span>' if rating else ''
    tags   = [t for t in (d.get('tags') or []) if t not in ('private', 'nhs')]
    tag_html = ''.join(f'<span class="tag spec">{t.replace("-", " ")}</span>' for t in tags)
    actions = []
    if d.get('phone'):   actions.append(f'<span class="btn-phone">📞 {d["phone"]}</span>')
    if d.get('cqc_url'): actions.append(f'<a class="btn btn-cqc" href="{d["cqc_url"]}" target="_blank" rel="noopener">CQC</a>')
    if d.get('website'): actions.append(f'<a class="btn btn-web" href="{d["website"]}" target="_blank" rel="noopener">Website</a>')
    data_tags = ','.join(tags)
    return f"""<div class="card" data-tags="{data_tags}">
  <div class="card-name">{d['name']}</div>
  <div class="card-addr">{d.get('address','')}{', ' + d['postcode'] if d.get('postcode') else ''}</div>
  <div class="card-tags"><span class="tag dent">Dentist</span>{tag_html}{badge}</div>
  <div class="card-actions">{' '.join(actions)}</div>
</div>"""

def build_borough_page(borough, records):
    all_tags = sorted({t for d in records for t in (d.get('tags') or []) if t not in ('private', 'nhs')})
    chips = '<button class="filter-chip active" data-tag="">All</button>'
    for t in all_tags:
        chips += f'<button class="filter-chip" data-tag="{t}">{t.replace("-", " ").title()}</button>'
    cards = '\n'.join(render_card(d) for d in sorted(records, key=lambda x: x.get('name', '').lower()))
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dentists in {borough} — London GP Directory</title>
<meta name="description" content="Compare {len(records)} CQC-registered dental practices in {borough}, London. Ratings, contact details and websites.">
<link rel="canonical" href="https://londongp.directory/dentists/{slugify(borough)}/">
{CSS}
</head>
<body>
{NAV}
<div class="page-header">
  <h1>🦷 Dentists in <em>{borough}</em></h1>
  <p>CQC-registered dental practices in {borough}, London.</p>
  <div class="stats"><div class="stat"><strong>{len(records)}</strong><span>Dental practices</span></div></div>
</div>
<div class="content">
  <a class="back" href="/dentists/">← All boroughs</a>
  <div class="notice">NHS vs. private acceptance isn't reliably flagged in CQC's open data — call ahead to confirm before booking.</div>
  {'<div class="filter-bar"><span class="filter-label">Filter:</span>' + chips + '</div>' if all_tags else ''}
  <div class="cards">{cards}</div>
</div>
<footer>
  Data: CQC · London GP Directory<br>
  <a href="/">Home</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a>
</footer>
{FILTER_JS}
</body>
</html>"""

def build_hub_page(by_borough):
    cards = ''
    for borough in sorted(by_borough.keys(), key=lambda b: -len(by_borough[b])):
        n = len(by_borough[borough])
        cards += f"""<a class="spec-card" href="/dentists/{slugify(borough)}/">
  <h2>{borough}</h2>
  <p class="spec-count"><strong>{n}</strong> dentists</p>
</a>"""
    total = sum(len(v) for v in by_borough.values())
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Dentists in London by Borough — London GP Directory</title>
<meta name="description" content="Browse {total} CQC-registered dental practices across all London boroughs. Ratings, addresses and contact details.">
<link rel="canonical" href="https://londongp.directory/dentists/">
{CSS}
</head>
<body>
{NAV}
<div class="page-header">
  <h1>Dentists in London <em>by borough</em></h1>
  <p>Browse {total} CQC-registered dental practices across all London boroughs.</p>
  <div class="stats">
    <div class="stat"><strong>{total}</strong><span>Dental practices</span></div>
    <div class="stat"><strong>{len(by_borough)}</strong><span>Boroughs</span></div>
  </div>
</div>
<div class="content">
  <div class="spec-grid">{cards}</div>
</div>
<footer>
  Data: CQC · London GP Directory<br>
  <a href="/">Home</a> · <a href="/about.html">About</a> · <a href="/methodology.html">Methodology</a>
</footer>
</body>
</html>"""

def main():
    if not DATA.exists():
        print(f"ERROR: {DATA} not found — run gen_dentists.py first.")
        return
    records = json.loads(DATA.read_text())
    print(f"Loaded {len(records)} dentists")

    by_borough = defaultdict(list)
    skipped = 0
    for r in records:
        b = (r.get('localAuthority') or '').strip()
        if not b or not r.get('name'):
            skipped += 1; continue
        by_borough[b].append(r)
    if skipped:
        print(f"  Skipped {skipped} records with missing name/borough")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "index.html").write_text(build_hub_page(by_borough), encoding="utf-8")
    print(f"✓ Hub: {len(by_borough)} boroughs")

    for borough, recs in sorted(by_borough.items()):
        bdir = OUT_DIR / slugify(borough)
        bdir.mkdir(parents=True, exist_ok=True)
        (bdir / "index.html").write_text(build_borough_page(borough, recs), encoding="utf-8")
        print(f"  ✓ /dentists/{slugify(borough)}/: {len(recs)} dentists")

    print(f"\n✅ Built {len(by_borough)} borough pages in {OUT_DIR}/")

if __name__ == "__main__":
    main()
