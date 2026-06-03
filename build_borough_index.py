#!/usr/bin/env python3
"""
Generate /boroughs/index.html — a clean directory of all 32 London boroughs
with live NHS + private clinic counts. Each row links to /practice/{slug}/.

Slug convention: lowercase, "&" dropped, spaces → "-".
  "Barking & Dagenham"      -> "barking-dagenham"
  "Kensington & Chelsea"    -> "kensington-chelsea"
  "Richmond upon Thames"    -> "richmond-upon-thames"

If build_borough_pages.py uses a different slug, change SLUG_STYLE below.
"""

import json, re, sys
from pathlib import Path
from collections import Counter
from datetime import date

ROOT = Path(__file__).resolve().parent
MERGED_JSON = ROOT / "merged.json"
OUT_DIR     = ROOT / "boroughs"

# Canonical list of 32 London boroughs (alphabetical)
BOROUGHS = [
    "Barking & Dagenham", "Barnet", "Bexley", "Brent", "Bromley",
    "Camden", "City of London", "Croydon", "Ealing", "Enfield",
    "Greenwich", "Hackney", "Hammersmith & Fulham", "Haringey",
    "Harrow", "Havering", "Hillingdon", "Hounslow", "Islington",
    "Kensington & Chelsea", "Kingston upon Thames", "Lambeth",
    "Lewisham", "Merton", "Newham", "Redbridge",
    "Richmond upon Thames", "Southwark", "Sutton", "Tower Hamlets",
    "Waltham Forest", "Wandsworth", "Westminster",
]

def slug(name):
    s = name.lower()
    s = s.replace(" & ", " ")          # drop ampersand
    s = re.sub(r"[^a-z0-9\s-]", "", s) # strip punctuation
    s = re.sub(r"\s+", "-", s).strip("-")
    return s

def normalize(name):
    """Match borough names from data even if they have minor variations."""
    return name.lower().replace(" & ", " and ").replace("  ", " ").strip()

def main():
    if not MERGED_JSON.exists():
        sys.exit(f"merged.json not found at {MERGED_JSON}")

    data = json.loads(MERGED_JSON.read_text())

    # Count by canonical borough name (handle minor format variations
    # by normalising both sides of the comparison)
    canonical_by_norm = {normalize(b): b for b in BOROUGHS}
    counts_nhs = Counter()
    counts_private = Counter()
    unrecognised = Counter()

    for rec in data:
        b_raw = (rec.get("ar") or rec.get("area_name") or "").strip()
        if not b_raw: continue
        canonical = canonical_by_norm.get(normalize(b_raw))
        if not canonical:
            unrecognised[b_raw] += 1
            continue
        if (rec.get("type") or "NHS") == "NHS":
            counts_nhs[canonical] += 1
        else:
            counts_private[canonical] += 1

    total_nhs     = sum(counts_nhs.values())
    total_private = sum(counts_private.values())

    # Build the grid rows
    rows = []
    for b in sorted(BOROUGHS):
        n = counts_nhs.get(b, 0)
        p = counts_private.get(b, 0)
        total = n + p
        if total == 0:
            # Borough exists but no data — show muted
            row = f'''  <a class="row row-empty" href="/practice/{slug(b)}/">
    <span class="name">{b}</span>
    <span class="counts"><span class="cnt cnt-empty">no data yet</span></span>
  </a>'''
        else:
            row = f'''  <a class="row" href="/practice/{slug(b)}/">
    <span class="name">{b}</span>
    <span class="counts">
      <span class="cnt cnt-nhs">{n} NHS</span>
      {'<span class="cnt cnt-private">' + str(p) + ' private</span>' if p else ''}
    </span>
  </a>'''
        rows.append(row)

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>All London boroughs — London GP Directory</title>
<meta name="description" content="Find GP practices in any of London's 32 boroughs. NHS GPs and private clinics, with patient ratings and CQC data.">
<link rel="canonical" href="https://londongp.directory/boroughs/">
<meta name="theme-color" content="#003087">
<meta property="og:title" content="All London boroughs — London GP Directory">
<meta property="og:description" content="Browse {total_nhs:,} NHS GP practices and {total_private:,} private clinics across all 32 London boroughs.">
<meta property="og:url" content="https://londongp.directory/boroughs/">
<meta property="og:type" content="website">
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#fff;color:#1a1a1a;font-size:16px;line-height:1.6}}
a{{color:#003087;text-decoration:none}}
.hdr{{padding:22px 24px;border-bottom:1px solid #e5e5e3;background:#fff}}
.hdr-in{{max-width:980px;margin:0 auto;display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:16px}}
.brand a{{font-weight:700;color:#003087;font-size:1.05rem}}
.nav{{display:flex;gap:18px;font-size:.95rem}}
.nav a{{color:#555}}
.nav a:hover,.nav a.active{{color:#003087;font-weight:600}}
.wrap{{max-width:980px;margin:0 auto;padding:36px 24px}}
.crumbs{{font-size:13px;color:#666;margin-bottom:14px}}
.crumbs a{{color:#003087}}
h1{{font-family:Georgia,serif;color:#003087;font-size:2.1rem;margin-bottom:10px;line-height:1.15;font-weight:700}}
.lede{{color:#444;margin-bottom:26px;font-size:1.05rem;max-width:680px}}
.stats{{display:flex;gap:32px;background:#EDF4FC;border:1px solid #B5D4F4;border-radius:10px;padding:18px 22px;margin-bottom:30px;font-size:.95rem;flex-wrap:wrap}}
.stats .stat strong{{display:block;color:#003087;font-size:1.5rem;font-weight:700;line-height:1}}
.stats .stat span{{color:#555;font-size:.9rem;margin-top:2px;display:block}}
.grid{{display:grid;grid-template-columns:repeat(2,1fr);gap:10px}}
.row{{display:flex;justify-content:space-between;align-items:center;background:#fff;border:1px solid #e5e5e3;border-radius:10px;padding:14px 18px;transition:all .15s;gap:10px}}
.row:hover{{border-color:#003087;background:#f7faff;text-decoration:none;transform:translateY(-1px);box-shadow:0 4px 12px rgba(0,48,135,.07)}}
.row-empty{{opacity:.6}}
.row-empty:hover{{opacity:.9}}
.name{{font-weight:600;color:#003087}}
.counts{{display:flex;gap:6px;font-size:.82rem;flex-shrink:0}}
.cnt{{padding:3px 10px;border-radius:99px;font-weight:600;letter-spacing:.02em;white-space:nowrap}}
.cnt-nhs{{background:#EDF4FC;color:#003087}}
.cnt-private{{background:#F4F0E8;color:#7a5a1e}}
.cnt-empty{{background:#F0F0EF;color:#777;font-weight:400}}
footer{{background:#003087;color:rgba(255,255,255,.65);text-align:center;padding:22px 24px;font-size:13px;margin-top:50px}}
footer a{{color:rgba(255,255,255,.9);margin:0 6px}}
@media(max-width:760px){{
  .grid{{grid-template-columns:1fr}}
  h1{{font-size:1.6rem}}
  .stats{{gap:20px}}
  .hdr-in{{flex-direction:column;align-items:flex-start;gap:10px}}
  .wrap{{padding:24px 18px}}
}}
</style>
</head>
<body>
<header class="hdr">
  <div class="hdr-in">
    <div class="brand"><a href="/">London GP Directory</a></div>
    <nav class="nav">
      <a href="/">Search</a>
      <a href="/boroughs/" class="active">Boroughs</a>
      <a href="/methodology.html">Methodology</a>
      <a href="/sources.html">Sources</a>
    </nav>
  </div>
</header>
<main class="wrap">
  <div class="crumbs"><a href="/">Home</a> &rsaquo; Boroughs</div>
  <h1>All London boroughs</h1>
  <p class="lede">Find GP practices in any of London's 32 boroughs. Pick one to see every NHS GP and private clinic in that area, with patient ratings and CQC data.</p>

  <div class="stats">
    <div class="stat"><strong>{total_nhs:,}</strong><span>NHS GP practices</span></div>
    <div class="stat"><strong>{total_private:,}</strong><span>private clinics</span></div>
    <div class="stat"><strong>32</strong><span>London boroughs</span></div>
  </div>

  <div class="grid">
{chr(10).join(rows)}
  </div>
</main>
<footer>
  London GP Directory &middot; Updated {date.today().strftime('%d %B %Y').lstrip('0')} &middot;
  <a href="/">All London</a> &middot;
  <a href="/about.html">About</a> &middot;
  <a href="/methodology.html">Methodology</a> &middot;
  <a href="/sources.html">Sources</a> &middot;
  <a href="/corrections.html">Corrections</a>
</footer>
</body>
</html>
'''

    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "index.html").write_text(html, encoding="utf-8")

    print(f"Generated /boroughs/index.html")
    print(f"  {total_nhs:,} NHS practices · {total_private:,} private clinics")
    print()
    print("  Borough breakdown:")
    for b in sorted(BOROUGHS):
        n = counts_nhs.get(b, 0)
        p = counts_private.get(b, 0)
        flag = "  " if (n + p) >= 5 else " !"
        print(f"  {flag} {b:25s} {n:4d} NHS, {p:3d} private  -> /practice/{slug(b)}/")

    if unrecognised:
        print("\n  ⚠ Records with unrecognised borough names:")
        for b, n in unrecognised.most_common(10):
            print(f"    '{b}': {n} records")

if __name__ == "__main__":
    main()
