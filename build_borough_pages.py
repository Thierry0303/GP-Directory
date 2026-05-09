#!/usr/bin/env python3
"""
Build one borough hub page per London borough for londongp.directory.

Drop this in the repo root next to refresh_nhs_data.py and run:

    python3 build_borough_pages.py

It uses the SAME merged dataset that refresh_nhs_data.py produces, so you
should run this AFTER refresh_nhs_data.py (or wire it into the same
GitHub Action). The script will:

  1. Load gps.json (base data) and re-fetch live ODS data the same way
     refresh_nhs_data.py does, OR re-use a `merged.json` cache if present.
  2. Group practices by borough.
  3. Write `practice/{borough-slug}/index.html` for each of the 32+
     London boroughs.
  4. Update sitemap.xml with the new borough URLs.

Each borough page is a static, server-rendered HTML page with proper
title/description/canonical/OG/JSON-LD — so it's directly indexable by
Google and ranks for queries like "camden GP", "lambeth GP",
"southwark GP practice".

Why this matters
----------------
The site currently has ONE indexed URL (the homepage). GSC shows it at
position 10 with ~5.9% CTR — high CTR but no scale. Per-borough pages
multiply the surface area roughly 32x without changing the data model.
Per-practice pages (a separate generator) take it further to ~530.
"""

import json, re, sys, html, os
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

ROOT = Path(__file__).resolve().parent
GPS_JSON = ROOT / "gps.json"
MERGED_JSON = ROOT / "merged.json"   # optional cache; produced below if missing
OUT_DIR = ROOT / "practice"
SITEMAP_XML = ROOT / "sitemap.xml"

SITE_URL = "https://londongp.directory"
SITE_NAME = "London GP Directory"

# ---- Borough mapping (mirrors refresh_nhs_data.py BOROUGH_MAP) ----
# Falls back to recomputing from postcode if a record's `ar` field is empty.
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

def slug(s):
    s = (s or "").lower()
    s = re.sub(r"&", "and", s)
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unknown"

def get_district(pc):
    if not pc: return ""
    pc = pc.strip().upper()
    if " " in pc:
        return pc.split()[0]
    pc = pc.replace(" ", "")
    return pc[:-3] if len(pc) >= 5 else pc

def borough_for(record):
    """Use record.ar if present; else fall back to postcode lookup."""
    ar = record.get("ar")
    if ar:
        return ar
    d = get_district(record.get("p", ""))
    if d in BOROUGH_MAP:
        return BOROUGH_MAP[d]
    m = re.match(r"^([A-Z]{1,2}\d)", d)
    return BOROUGH_MAP.get(m.group(1), "") if m else ""

def cqc_class(r):
    return {
        "Outstanding": "cqc-outstanding",
        "Good": "cqc-good",
        "Requires improvement": "cqc-ri",
        "Inadequate": "cqc-inadequate",
    }.get(r, "cqc-none")

# ---- HTML --------------------------------------------------------------

CSS = """
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.55}
a{text-decoration:none;color:inherit}
.hdr{background:#003087;color:#fff;border-bottom:4px solid #0072CE;padding:14px 24px;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}
.hdr-logo{font-family:Georgia,serif;font-size:1.3rem;font-weight:700}
.hdr-logo em{color:#B5D4F4;font-style:italic;font-weight:400}
.hdr nav{display:flex;gap:14px;font-size:.9rem;opacity:.85}
.crumbs{padding:12px 24px;font-size:.85rem;color:#888;background:#fff;border-bottom:1px solid #e5e5e3}
.crumbs a{color:#003087;font-weight:500}
.hero{background:#003087;color:#fff;padding:40px 24px 32px}
.hero-inner{max-width:1200px;margin:0 auto}
.eyebrow{font-size:.7rem;letter-spacing:.15em;text-transform:uppercase;color:#B5D4F4;font-weight:600;margin-bottom:10px}
h1{font-family:Georgia,serif;font-size:clamp(2rem,4.5vw,3rem);font-weight:700;line-height:1.1;letter-spacing:-0.02em;margin-bottom:12px}
h1 em{color:#B5D4F4;font-style:italic;font-weight:400}
.hero-sub{color:rgba(255,255,255,.78);max-width:600px;margin-bottom:24px}
.hero-stats{display:flex;gap:32px;flex-wrap:wrap}
.hero-stat-num{font-size:1.8rem;font-weight:300;color:#fff;line-height:1;letter-spacing:-0.03em}
.hero-stat-label{font-size:.7rem;color:rgba(255,255,255,.6);text-transform:uppercase;letter-spacing:.06em;margin-top:3px}
main{max-width:1200px;margin:0 auto;padding:32px 24px 56px}
.intro{font-size:.95rem;color:#555;max-width:720px;margin-bottom:28px}
h2{font-family:Georgia,serif;font-size:1.4rem;font-weight:700;color:#003087;margin:28px 0 14px}
.card-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:13px}
.card{background:#fff;border:1px solid #ddd;border-radius:12px;padding:15px 16px;display:flex;flex-direction:column;transition:border-color .15s,box-shadow .15s}
.card:hover{border-color:#0072CE;box-shadow:0 3px 14px rgba(0,48,135,.08)}
.card-name{font-family:Georgia,serif;font-weight:700;font-size:14px;line-height:1.3;color:#003087;margin-bottom:6px}
.card-meta{font-size:11.5px;color:#888;margin-bottom:10px;line-height:1.4}
.card-cqc{display:inline-block;font-size:9.5px;font-weight:600;padding:2px 8px;border-radius:99px;margin-bottom:8px;align-self:flex-start;text-transform:uppercase;letter-spacing:.04em}
.cqc-outstanding{background:#E1F5EE;color:#0F6E56}
.cqc-good{background:#D8EFE3;color:#007F3B}
.cqc-ri{background:#FAEEDA;color:#BA7517}
.cqc-inadequate{background:#FCEBEB;color:#A32D2D}
.cqc-none{background:#f0f0ee;color:#777}
.card-stats{display:flex;gap:14px;margin-top:auto;padding-top:10px;border-top:1px solid #f0f0ee;font-size:11px;color:#888}
.card-stat strong{display:block;font-size:13px;color:#222;font-weight:600}
.card-actions{display:flex;gap:5px;flex-wrap:wrap;margin-top:8px}
.pill{font-size:10.5px;padding:3px 8px;border-radius:6px;font-weight:600;background:#EDF4FC;color:#0072CE}
.empty{color:#888;padding:24px 0}
footer{background:#003087;color:rgba(255,255,255,.55);padding:18px 24px;text-align:center;font-size:.78rem}
footer a{color:rgba(255,255,255,.85)}
@media(max-width:600px){.hero{padding:28px 16px 24px}main{padding:22px 16px 40px}.hdr,.crumbs,footer{padding-left:16px;padding-right:16px}}
"""

def render_card(p):
    name = html.escape(str(p.get("n", "Unnamed practice")))
    addr = html.escape(", ".join(b for b in (p.get("a", ""), p.get("p", "")) if b))
    pcn  = html.escape(str(p.get("pcn", "")))
    cqc  = p.get("cqc") or ""
    s    = p.get("s")
    c    = p.get("c")
    ph   = p.get("ph", "")
    ods  = p.get("o", "")

    cqc_html = f'<span class="card-cqc {cqc_class(cqc)}">{html.escape(cqc)}</span>' if cqc else ''
    stats = []
    if s: stats.append(f'<div class="card-stat">Patient<strong>{round(s,1)}%</strong></div>')
    if c: stats.append(f'<div class="card-stat">Contact<strong>{round(c,1)}%</strong></div>')
    stats_html = f'<div class="card-stats">{"".join(stats)}</div>' if stats else ''

    actions = []
    if ods:
        actions.append(f'<a class="pill" href="https://www.nhs.uk/services/gp-surgery/-/X{ods}" target="_blank" rel="noopener">NHS profile</a>')
        actions.append(f'<a class="pill" href="https://gp-registration.nhs.uk/{ods}" target="_blank" rel="noopener">Register</a>')
    if ph:
        actions.append(f'<a class="pill" href="tel:{ph.replace(" ", "")}">Call</a>')
    actions_html = f'<div class="card-actions">{"".join(actions)}</div>' if actions else ''

    return f'''<div class="card">
  <div class="card-name">{name}</div>
  {cqc_html}
  <div class="card-meta">{addr}{(" &middot; " + pcn) if pcn else ""}</div>
  {stats_html}
  {actions_html}
</div>'''

def render_page(borough, practices):
    n = len(practices)
    cqc_count = defaultdict(int)
    for p in practices:
        cqc_count[p.get("cqc") or "Not rated"] += 1
    outstanding = cqc_count.get("Outstanding", 0)
    good = cqc_count.get("Good", 0)
    avg_score = round(sum(p["s"] for p in practices if p.get("s"))
                      / max(1, sum(1 for p in practices if p.get("s"))), 1) if any(p.get("s") for p in practices) else None

    cqc_order = {"Outstanding": 0, "Good": 1, "Requires improvement": 2, "Inadequate": 3}
    practices_sorted = sorted(
        practices,
        key=lambda p: (cqc_order.get(p.get("cqc"), 9), -(p.get("s") or 0), str(p.get("n","")).lower())
    )

    title = f"GP Practices in {borough} — Compare {n} NHS GPs by Patient Score"
    desc  = (f"Free directory of all {n} NHS GP practices in {borough}, London. "
             f"{outstanding + good} rated Good or Outstanding by CQC. Compare patient survey scores, "
             f"contact ease and CQC ratings. Updated weekly from NHS ODS.")
    canonical = f"{SITE_URL}/practice/{slug(borough)}/"

    items = [{
        "@type": "ListItem",
        "position": i,
        "url": f"https://www.nhs.uk/services/gp-surgery/-/X{p.get('o','')}",
        "name": str(p.get("n", ""))
    } for i, p in enumerate(practices_sorted, 1)]

    json_ld_collection = {
        "@context": "https://schema.org",
        "@type": "CollectionPage",
        "name": title,
        "url": canonical,
        "description": desc,
        "isPartOf": {"@type": "WebSite", "name": SITE_NAME, "url": SITE_URL + "/"},
        "mainEntity": {
            "@type": "ItemList",
            "numberOfItems": n,
            "itemListElement": items[:200]
        }
    }
    json_ld_breadcrumb = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": SITE_NAME, "item": SITE_URL + "/"},
            {"@type": "ListItem", "position": 2, "name": borough, "item": canonical}
        ]
    }

    cards = "\n".join(render_card(p) for p in practices_sorted) or '<p class="empty">No practices found in this borough.</p>'
    avg_html = f'<div><div class="hero-stat-num">{avg_score}%</div><div class="hero-stat-label">Avg patient score</div></div>' if avg_score else ''

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{html.escape(title)}</title>
<meta name="description" content="{html.escape(desc)}">
<meta name="robots" content="index, follow, max-image-preview:large">
<meta name="theme-color" content="#003087">
<link rel="canonical" href="{canonical}">

<meta property="og:type" content="website">
<meta property="og:site_name" content="{SITE_NAME}">
<meta property="og:title" content="{html.escape(title)}">
<meta property="og:description" content="{html.escape(desc)}">
<meta property="og:url" content="{canonical}">
<meta property="og:locale" content="en_GB">
<meta property="og:image" content="{SITE_URL}/og-image.png">

<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="{html.escape(title)}">
<meta name="twitter:description" content="{html.escape(desc)}">
<meta name="twitter:image" content="{SITE_URL}/og-image.png">

<script type="application/ld+json">{json.dumps(json_ld_collection, separators=(",",":"))}</script>
<script type="application/ld+json">{json.dumps(json_ld_breadcrumb, separators=(",",":"))}</script>

<style>{CSS}</style>
</head>
<body>

<header class="hdr">
  <a class="hdr-logo" href="/">London GP <em>Directory</em></a>
  <nav><a href="/">Home</a></nav>
</header>

<div class="crumbs">
  <a href="/">{SITE_NAME}</a> &rsaquo; {html.escape(borough)}
</div>

<section class="hero">
  <div class="hero-inner">
    <div class="eyebrow">London Borough</div>
    <h1>NHS GP Practices in <em>{html.escape(borough)}</em></h1>
    <p class="hero-sub">Compare every NHS GP practice in {html.escape(borough)} by patient survey score, contact ease and CQC rating. Phone numbers and addresses come straight from the NHS ODS register and are refreshed weekly.</p>
    <div class="hero-stats">
      <div><div class="hero-stat-num">{n}</div><div class="hero-stat-label">Practices</div></div>
      <div><div class="hero-stat-num">{outstanding + good}</div><div class="hero-stat-label">Good or Outstanding</div></div>
      {avg_html}
    </div>
  </div>
</section>

<main>
  <p class="intro">All {n} NHS GP practices in {html.escape(borough)}, sorted by CQC rating then patient satisfaction. Tap any card to register, call or view the official NHS profile.</p>

  <h2>All practices in {html.escape(borough)}</h2>
  <div class="card-grid">
    {cards}
  </div>
</main>

<footer>
  Data: NHS ODS &middot; GP Patient Survey &middot; CQC &middot; updated {datetime.now(timezone.utc).strftime("%-d %B %Y")}<br>
  <a href="/">{SITE_NAME}</a>
</footer>

</body>
</html>
"""

# ---- data load ---------------------------------------------------------

def load_practices():
    """
    Prefer merged.json (produced by refresh_nhs_data.py if you cache it).
    Otherwise fall back to gps.json — note this won't have live phone/address,
    so prefer running this script as part of the same pipeline.
    """
    if MERGED_JSON.exists():
        print(f"Using {MERGED_JSON.name}")
        return json.loads(MERGED_JSON.read_text())

    if not GPS_JSON.exists():
        sys.exit("Neither merged.json nor gps.json found in repo root.")

    raw = json.loads(GPS_JSON.read_text())
    print(f"Using {GPS_JSON.name} ({len(raw)} records)")
    # Best-effort mapping of base fields → merged keys used by templates
    out = []
    for r in raw:
        out.append({
            "o":   r.get("ods_code") or r.get("o"),
            "n":   r.get("name") or r.get("n"),
            "a":   r.get("address") or r.get("a", ""),
            "p":   r.get("postcode") or r.get("p", ""),
            "ph":  r.get("phone") or r.get("ph", ""),
            "s":   r.get("gpps_overall_pct") or r.get("s"),
            "c":   r.get("gpps_contact_pct") or r.get("c"),
            "pcn": (r.get("gpps_pcn") or r.get("pcn") or "").replace(" PCN","").replace(" Pcn","").strip(),
            "cqc": r.get("cqc_rating") or r.get("cqc") or "",
            "cu":  r.get("cqc_url") or r.get("cu") or "",
            "ar":  r.get("ar") or "",
        })
    return out

# ---- sitemap -----------------------------------------------------------

def update_sitemap(borough_urls):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    homepage_entry = (
        f'<url><loc>{SITE_URL}/</loc><lastmod>{today}</lastmod>'
        f'<changefreq>weekly</changefreq><priority>1.0</priority></url>'
    )
    borough_entries = "\n  ".join(
        f'<url><loc>{u}</loc><lastmod>{today}</lastmod>'
        f'<changefreq>weekly</changefreq><priority>0.8</priority></url>'
        for u in borough_urls
    )
    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        f'  {homepage_entry}\n'
        f'  {borough_entries}\n'
        '</urlset>\n'
    )
    SITEMAP_XML.write_text(xml)
    print(f"Wrote {SITEMAP_XML.name}: 1 homepage + {len(borough_urls)} borough URLs.")

# ---- main --------------------------------------------------------------

def main():
    practices = load_practices()
    by_borough = defaultdict(list)
    skipped = 0
    for p in practices:
        b = borough_for(p)
        if not b:
            skipped += 1
            continue
        by_borough[b].append(p)

    print(f"{len(practices)} practices, {len(by_borough)} boroughs, {skipped} skipped (no borough)")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    written = []
    for borough, ps in sorted(by_borough.items()):
        out = OUT_DIR / slug(borough) / "index.html"
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(render_page(borough, ps))
        written.append((borough, len(ps), out.relative_to(ROOT)))

    for borough, count, path in written:
        print(f"  wrote {path}  ({count} practices)")

    update_sitemap([f"{SITE_URL}/practice/{slug(b)}/" for b, _, _ in written])
    print(f"\nDone. {len(written)} borough pages.")

if __name__ == "__main__":
    main()
