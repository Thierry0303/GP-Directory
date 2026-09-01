#!/usr/bin/env python3
"""
Generate /nhs-services/ landing page + per-category pages from
nhs_specialties.json.

Pages produced:
  /nhs-services/index.html           — list of all categories with counts
  /nhs-services/{category}/index.html — list of records in that category
"""
import json, re
from pathlib import Path
from collections import defaultdict
from datetime import date

ROOT = Path(__file__).resolve().parent
NHS_JSON = ROOT / "nhs_specialties.json"
OUT_DIR  = ROOT / "nhs-services"

CATEGORIES = [
    ("nhs-hospital",      "NHS Hospitals",          "Acute hospitals operated by NHS Trusts in London."),
    ("nhs-mental-health", "NHS Mental Health",      "Mental health hospitals and services run by NHS Trusts."),
    ("nhs-urgent-care",   "NHS Urgent Care",        "Walk-in centres, urgent treatment centres, and out-of-hours services."),
    ("nhs-diagnostic",    "NHS Diagnostic Centres", "NHS imaging, scanning, and diagnostic facilities."),
    ("nhs-community",     "NHS Community Services", "Community nursing, school health, district services, rehabilitation."),
    ("nhs-ambulance",     "NHS Ambulance Service",  "London Ambulance Service stations and emergency response."),
    ("nhs-hospice",       "NHS Hospices",           "End-of-life and palliative care provided by NHS-affiliated hospices."),
]

CSS = """*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#fff;color:#1a1a1a;font-size:16px;line-height:1.6}
a{color:#003087;text-decoration:none}
.site-nav{background:#003087;padding:12px 24px;display:flex;gap:20px;flex-wrap:wrap;align-items:center}
.site-nav a{color:rgba(255,255,255,.8);font-size:.85rem}
.site-nav a:hover,.site-nav a.active{color:#fff}
.site-nav .brand{font-family:Georgia,serif;font-size:1.05rem;font-weight:700;color:#fff;margin-right:14px}
.site-nav .brand em{color:#B5D4F4;font-style:italic;font-weight:400}
.page-header{background:#003087;color:#fff;padding:32px 24px;border-bottom:4px solid #0072CE}
.page-header .ph-in{max-width:980px;margin:0 auto}
.wrap{max-width:980px;margin:0 auto;padding:36px 24px}
.crumbs{font-size:13px;color:rgba(255,255,255,.7);margin-bottom:14px}
.crumbs a{color:#B5D4F4}
.page-header h1{font-family:Georgia,serif;color:#fff;font-size:1.9rem;margin-bottom:10px;line-height:1.15}
.page-header .lede{color:rgba(255,255,255,.82);margin-bottom:0;max-width:680px}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:12px}
.row{display:flex;flex-direction:column;background:#fff;border:1px solid #e5e5e3;border-radius:10px;padding:14px 18px;transition:all .15s;min-height:120px}
.row h3{margin-bottom:6px}
.row p{flex:1}
.row:hover{border-color:#003087;background:#f7faff;box-shadow:0 4px 12px rgba(0,48,135,.07)}
.row h3{color:#003087;font-size:1.05rem;margin-bottom:4px}
.row p{font-size:.85rem;color:#666;margin-bottom:3px}
.row .meta{font-size:.78rem;color:#888;margin-top:6px}
.cnt{display:inline-block;background:#EDF4FC;color:#003087;padding:2px 9px;border-radius:99px;font-size:.78rem;font-weight:600;margin-left:8px}
.cqc-tag{display:inline-block;font-size:10px;font-weight:600;padding:2px 8px;border-radius:99px;margin-left:6px}
.cqc-Outstanding{background:#E1F5EE;color:#0F6E56}
.cqc-Good{background:#D8EFE3;color:#007F3B}
.cqc-Requires{background:#FAEEDA;color:#BA7517}
.cqc-Inadequate{background:#FCEBEB;color:#A32D2D}
.cqc-{background:#f0f0ee;color:#777}
footer{background:#003087;color:rgba(255,255,255,.65);text-align:center;padding:18px 24px;font-size:13px;margin-top:50px}
footer a{color:rgba(255,255,255,.9);margin:0 6px}"""

NAV = """<nav class="site-nav">
  <a class="brand" href="/">London GP <em>Directory</em></a>
  <a href="/">Search</a>
  <a href="/boroughs/">Boroughs</a>
  <a href="/nhs-services/" class="{nhs_active}">NHS Services</a>
  <a href="/private/">Private Clinics</a>
  <a href="/dentists/">Dentists</a>
  <a href="/guides/">Guides</a>
  <a href="/methodology.html">Methodology</a>
  <a href="/sources.html">Sources</a>
</nav>"""

FOOTER = f"""<footer>
  London GP Directory · Updated {date.today().strftime('%d %B %Y').lstrip('0')} ·
  <a href="/">Home</a> · <a href="/about.html">About</a> ·
  <a href="/methodology.html">Methodology</a> · <a href="/sources.html">Sources</a>
</footer>"""

def rating_class(r):
    if r and r.startswith("Requires"): return "cqc-Requires"
    return f"cqc-{r or ''}"

def slug(s):
    return re.sub(r"[^a-z0-9]+", "-", (s or "").lower()).strip("-")

def index_page(grouped):
    cards = []
    for slug_key, label, blurb in CATEGORIES:
        n = len(grouped.get(slug_key, []))
        if not n:
            continue
        cards.append(f'''<a class="row" href="/nhs-services/{slug_key}/">
            <h3>{label} <span class="cnt">{n}</span></h3>
            <p>{blurb}</p>
        </a>''')
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NHS Services in London — London GP Directory</title>
<meta name="description" content="Browse NHS hospitals, urgent care, mental health, diagnostic and community services across London.">
<link rel="canonical" href="https://londongp.directory/nhs-services/">
<style>{CSS}
.row{{position:relative;transition:box-shadow .15s,border-color .15s}}
.row:has(.row-link):hover{{border-color:#003087;box-shadow:0 2px 10px rgba(0,48,135,.12);cursor:pointer}}
.row-link{{color:inherit;text-decoration:none}}
.row-link::after{{content:"";position:absolute;inset:0;border-radius:inherit}}
.row .meta a,.row .meta{{position:relative;z-index:1}}
</style></head><body>
{NAV.format(nhs_active='active')}
<div class="page-header"><div class="ph-in">
<div class="crumbs"><a href="/">Home</a> &rsaquo; NHS Services</div>
<h1>NHS services in London</h1>
<p class="lede">Beyond GP practices, NHS-funded healthcare in London covers urgent care centres, hospitals, mental health services, community nursing, diagnostic centres and the ambulance service. Browse by category.</p>
</div></div>
<main class="wrap">
<div class="grid">{''.join(cards)}</div>
</main>
{FOOTER}</body></html>"""

def category_page(slug_key, label, blurb, records):
    records.sort(key=lambda r: r["name"])
    cards = []
    import json as _json
    prov_file = NHS_JSON.parent / "provider_ratings.json"
    prov_ratings = _json.loads(prov_file.read_text()) if prov_file.exists() else {}
    for r in records:
        rating = r.get("cqc_rating", "")
        trust_rating = "" if rating else prov_ratings.get(r.get("providerId",""), "")
        if rating:
            chip = f'<span class="cqc-tag {rating_class(rating)}">{rating}</span>'
        elif trust_rating:
            chip = (f'<span class="cqc-tag {rating_class(trust_rating)}" '
                    f'title="CQC rating of the NHS trust that runs this service">'
                    f'Trust: {trust_rating}</span>')
        else:
            chip = ""   # showing "Not rated" on every card reads as broken data
        actions = []
        if r.get("phone"):
            actions.append(f'<a href="tel:{r["phone"]}">📞 {r["phone"]}</a>')
        if r.get("website"):
            actions.append(f'<a href="{r["website"]}" target="_blank">Website ↗</a>')
        if r.get("cqc_url"):
            actions.append(f'<a href="{r["cqc_url"]}" target="_blank">CQC ↗</a>')
        # CQC often names NHS community locations after the borough they
        # cover ("Lewisham") — meaningless on a card. Prefer the provider.
        name = (r["name"] or "").strip()
        prov = (r.get("providerName") or "").strip()
        la   = (r.get("localAuthority") or "").strip()
        boroughs = {rec.get("localAuthority","") for rec in records}
        if prov and (not name or name == la or name in boroughs):
            display = f"{prov}" + (f" ({name})" if name and name not in prov else "")
        else:
            display = name or prov or "Unnamed service"
        addr = r.get("address","")
        if r.get("postcode"): addr = (addr + ", " + r["postcode"]).strip(", ")
        # Whole card clicks through to the service's website, falling back
        # to its CQC profile. Inner links stay independently clickable.
        main_href = r.get("website") or r.get("cqc_url") or ""
        name_html = (f'<a class="row-link" href="{main_href}" target="_blank" rel="noopener">{display}</a>'
                     if main_href else display)
        cards.append(f'''<div class="row">
            <h3>{name_html} {chip}</h3>
            {f"<p>{addr}</p>" if addr else ""}
            <div class="meta">{la}{" · " + " · ".join(actions) if actions else ""}</div>
        </div>''')
    grid = ''.join(cards) if cards else '<p style="color:#888;padding:24px 0">No records in this category yet. We rely on CQC data — if you know of a missing service, please <a href="/corrections.html">tell us</a>.</p>'
    return f"""<!DOCTYPE html>
<html lang="en"><head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{label} in London — London GP Directory</title>
<meta name="description" content="{blurb}">
<link rel="canonical" href="https://londongp.directory/nhs-services/{slug_key}/">
<style>{CSS}
.row{{position:relative;transition:box-shadow .15s,border-color .15s}}
.row:has(.row-link):hover{{border-color:#003087;box-shadow:0 2px 10px rgba(0,48,135,.12);cursor:pointer}}
.row-link{{color:inherit;text-decoration:none}}
.row-link::after{{content:"";position:absolute;inset:0;border-radius:inherit}}
.row .meta a,.row .meta{{position:relative;z-index:1}}
</style></head><body>
{NAV.format(nhs_active='active')}
<div class="page-header"><div class="ph-in">
<div class="crumbs"><a href="/">Home</a> &rsaquo; <a href="/nhs-services/">NHS Services</a> &rsaquo; {label}</div>
<h1>{label} in London</h1>
<p class="lede">{blurb} {len(records)} {('service' if len(records)==1 else 'services')} currently listed.</p>
</div></div>
<main class="wrap">
<div class="grid">{grid}</div>
</main>
{FOOTER}</body></html>"""

def main():
    if not NHS_JSON.exists():
        print(f"ERROR: {NHS_JSON} not found. Run gen_nhs_specialties.py first.")
        return
    data = json.loads(NHS_JSON.read_text())
    grouped = defaultdict(list)
    for r in data:
        grouped[r["category"]].append(r)

    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / "index.html").write_text(index_page(grouped), encoding="utf-8")
    print(f"Wrote /nhs-services/index.html")

    import shutil
    for slug_key, label, blurb in CATEGORIES:
        page_dir = OUT_DIR / slug_key
        recs = grouped.get(slug_key, [])
        if not recs:
            # No real services in this category — remove the page entirely
            # rather than serving an empty (or stale) listing.
            if page_dir.exists():
                shutil.rmtree(page_dir)
                print(f"  /nhs-services/{slug_key}/   pruned (0 records)")
            continue
        page_dir.mkdir(exist_ok=True)
        (page_dir / "index.html").write_text(
            category_page(slug_key, label, blurb, recs), encoding="utf-8"
        )
        print(f"  /nhs-services/{slug_key}/   ({len(recs)} records)")

    print(f"\nDone. {sum(len(v) for v in grouped.values())} total NHS service records across {len(CATEGORIES)} categories.")

if __name__ == "__main__":
    main()
