#!/usr/bin/env python3
"""
build_provider_pages.py
Rebuilds all /provider/{type}/{borough}/index.html pages from the fixed JSON files.
Run from repo root: python3 build_provider_pages.py

Reads:  files/dentists.json, files/clinics.json (and hospitals/care/homecare/mental)
Writes: provider/dentists/index.html
        provider/dentists/{borough-slug}/index.html
        (same for clinics, hospitals, care, homecare, mental)
"""

import json, os, re
from pathlib import Path
from collections import defaultdict

# ── Config ──────────────────────────────────────────────────────────────────
CATEGORIES = {
    "dentists":  {"emoji": "🦷", "label": "Dentists",        "file": "files/dentists.json"},
    "clinics":   {"emoji": "🏥", "label": "Clinics",         "file": "files/clinics.json"},
    "hospitals": {"emoji": "🏨", "label": "Hospitals",       "file": "files/hospitals.json"},
    "care":      {"emoji": "❤️",  "label": "Care Homes",      "file": "files/cares.json"},
    "homecare":  {"emoji": "🏠", "label": "Home Care",       "file": "files/homecares.json"},
    "mental":    {"emoji": "🧠", "label": "Mental Health",   "file": "files/mentals.json"},
}

ALL_BOROUGHS = [
    "Barking & Dagenham","Barnet","Bexley","Brent","Bromley","Camden",
    "City of London","Croydon","Ealing","Enfield","Greenwich","Hackney",
    "Hammersmith & Fulham","Haringey","Harrow","Havering","Hillingdon",
    "Hounslow","Islington","Kensington & Chelsea","Kingston","Lambeth",
    "Lewisham","Merton","Newham","Redbridge","Richmond","Southwark",
    "Sutton","Tower Hamlets","Waltham Forest","Wandsworth","Westminster",
]

NAV_LINKS = """
<nav class="site-nav">
  <a href="/">🏠 GP Directory</a>
  <a href="/provider/dentists/">🦷 Dentists</a>
  <a href="/provider/clinics/">🏥 Clinics</a>
  <a href="/provider/hospitals/">🏨 Hospitals</a>
  <a href="/provider/care/">❤️ Care Homes</a>
  <a href="/provider/homecare/">🏠 Home Care</a>
  <a href="/provider/mental/">🧠 Mental Health</a>
</nav>
"""

BASE_CSS = """
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.5}
.site-nav{background:#003087;padding:10px 24px;display:flex;gap:16px;flex-wrap:wrap}
.site-nav a{color:#fff;text-decoration:none;font-size:.85rem;opacity:.85}
.site-nav a:hover{opacity:1}
.page-header{background:#003087;color:#fff;padding:32px 24px}
.page-header h1{font-size:2rem;font-weight:700;margin-bottom:6px}
.page-header p{opacity:.75;font-size:1rem}
.content{max-width:1100px;margin:32px auto;padding:0 24px}
.borough-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;margin-top:24px}
.borough-card{background:#fff;border-radius:12px;padding:20px 24px;border-left:4px solid #003087;text-decoration:none;color:#1a1a1a;transition:box-shadow .15s;display:block}
.borough-card:hover{box-shadow:0 4px 16px rgba(0,48,135,.15)}
.borough-card h2{font-size:1.1rem;font-weight:600;color:#003087;margin-bottom:4px}
.borough-card p{font-size:.85rem;color:#666}
.provider-list{display:flex;flex-direction:column;gap:12px;margin-top:24px}
.provider-card{background:#fff;border-radius:12px;padding:16px 20px;border:1px solid #e8e8e8;transition:box-shadow .15s}
.provider-card:hover{box-shadow:0 2px 12px rgba(0,0,0,.08)}
.provider-card h3{font-size:1rem;font-weight:600;color:#003087;margin-bottom:6px}
.provider-meta{display:flex;flex-wrap:wrap;gap:8px;font-size:.8rem;color:#555;margin-bottom:6px}
.provider-meta span{background:#f0f4ff;padding:2px 8px;border-radius:8px}
.provider-meta span.status-ok{background:#dcfce7;color:#166534}
.provider-meta span.status-bad{background:#fee2e2;color:#991b1b}
.cqc-link{font-size:.8rem;color:#0072CE;text-decoration:none}
.cqc-link:hover{text-decoration:underline}
.back-link{display:inline-block;margin-bottom:20px;color:#003087;text-decoration:none;font-size:.9rem}
.back-link:hover{text-decoration:underline}
.count-badge{font-size:.85rem;color:#666;margin-top:8px}
</style>
"""

def slugify(s):
    s = s.lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")

def status_class(status):
    return "status-ok" if status == "Registered" else "status-bad"

def build_borough_page(cat_slug, cat_cfg, borough, records):
    """Build /provider/{cat}/{borough-slug}/index.html"""
    b_slug = slugify(borough)
    emoji = cat_cfg["emoji"]
    label = cat_cfg["label"]
    
    # Filter to registered only for display, but show count of all
    registered = [r for r in records if r.get("status") == "Registered"]
    
    cards = ""
    for r in sorted(registered, key=lambda x: x.get("name","").lower()):
        name = r.get("name") or "Unknown"
        postcode = r.get("postcode","")
        address = r.get("address","")
        status = r.get("status","")
        cqc_url = r.get("cqc_url","")
        services = ", ".join(r.get("services",[]))
        
        cqc_btn = f'<a class="cqc-link" href="{cqc_url}" target="_blank" rel="noopener">View on CQC →</a>' if cqc_url else ""
        
        cards += f"""
<div class="provider-card">
  <h3>{name}</h3>
  <div class="provider-meta">
    <span>{postcode}</span>
    {f'<span>{services}</span>' if services else ''}
    <span class="{status_class(status)}">{status}</span>
  </div>
  {f'<div style="font-size:.8rem;color:#777;margin-bottom:6px">{address}</div>' if address else ''}
  {cqc_btn}
</div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{borough} {label} in London</title>
<meta name="description" content="Find {label.lower()} in {borough}, London. {len(registered)} registered providers listed with CQC details.">
<link rel="canonical" href="https://londongp.directory/provider/{cat_slug}/{b_slug}/">
{BASE_CSS}
</head>
<body>
{NAV_LINKS}
<div class="page-header">
  <h1>{emoji} {label} in {borough}</h1>
  <p>{len(registered)} registered providers</p>
</div>
<div class="content">
  <a class="back-link" href="/provider/{cat_slug}/">← All {label} boroughs</a>
  <div class="provider-list">{cards}</div>
</div>
</body>
</html>"""
    return html

def build_category_index(cat_slug, cat_cfg, borough_counts):
    """Build /provider/{cat}/index.html listing all boroughs"""
    emoji = cat_cfg["emoji"]
    label = cat_cfg["label"]
    
    cards = ""
    for borough in sorted(borough_counts.keys()):
        count = borough_counts[borough]
        b_slug = slugify(borough)
        cards += f"""
<a class="borough-card" href="{b_slug}/">
  <h2>{borough}</h2>
  <p>{count} providers</p>
</a>"""
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>London {label} Directory</title>
<meta name="description" content="Find {label.lower()} across all London boroughs. Browse by area.">
<link rel="canonical" href="https://londongp.directory/provider/{cat_slug}/">
{BASE_CSS}
</head>
<body>
{NAV_LINKS}
<div class="page-header">
  <h1>{emoji} {label} in London</h1>
  <p>Find {label.lower()} by borough</p>
</div>
<div class="content">
  <div class="borough-grid">{cards}</div>
</div>
</body>
</html>"""
    return html

def main():
    base = Path(".")
    
    for cat_slug, cat_cfg in CATEGORIES.items():
        json_path = base / cat_cfg["file"]
        if not json_path.exists():
            print(f"⚠️  {json_path} not found — skipping {cat_slug}")
            continue
        
        with open(json_path) as f:
            records = json.load(f)
        
        # Filter: must have a name and be London
        records = [r for r in records if r.get("name") and r.get("postcode")]
        
        # Group by borough
        by_borough = defaultdict(list)
        for r in records:
            b = r.get("borough")
            if b and b in ALL_BOROUGHS:
                by_borough[b].append(r)
        
        registered_by_borough = {
            b: [r for r in recs if r.get("status") == "Registered"]
            for b, recs in by_borough.items()
            if any(r.get("status") == "Registered" for r in recs)
        }
        
        print(f"\n{cat_slug}: {len(records)} total records, {len(registered_by_borough)} boroughs with registered providers")
        
        # Write borough pages
        for borough, recs in registered_by_borough.items():
            b_slug = slugify(borough)
            out_dir = base / "provider" / cat_slug / b_slug
            out_dir.mkdir(parents=True, exist_ok=True)
            html = build_borough_page(cat_slug, cat_cfg, borough, recs)
            (out_dir / "index.html").write_text(html, encoding="utf-8")
            print(f"  ✓ {borough}: {len(recs)} registered")
        
        # Write category index
        borough_counts = {b: len(recs) for b, recs in registered_by_borough.items()}
        index_dir = base / "provider" / cat_slug
        index_dir.mkdir(parents=True, exist_ok=True)
        index_html = build_category_index(cat_slug, cat_cfg, borough_counts)
        (index_dir / "index.html").write_text(index_html, encoding="utf-8")
        print(f"  ✓ Category index: {len(borough_counts)} boroughs")
    
    print("\n✅ All provider pages rebuilt successfully.")
    print("Commit all changes in provider/ and push to deploy.")

if __name__ == "__main__":
    main()
