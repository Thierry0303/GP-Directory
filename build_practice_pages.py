#!/usr/bin/env python3
"""
Build one static HTML profile page per GP practice for londongp.directory.

Drop this in the repo root next to refresh_nhs_data.py and build_borough_pages.py.
Run AFTER refresh_nhs_data.py and AFTER build_borough_pages.py:

    python3 refresh_nhs_data.py     # writes merged.json + index.html
    python3 build_borough_pages.py  # 32 borough hub pages
    python3 build_practice_pages.py # ~430 per-practice pages + final sitemap

Each output:  practice/{borough-slug}/{practice-slug}/index.html

Why this exists
---------------
The GP site currently has 1 indexed homepage + 32 borough hubs. Long-tail
queries like "[practice name]", "GP near [postcode]", "register at [practice]"
have no specific landing page. Per-practice pages multiply the indexable
surface ~14× and let each practice rank for its own brand name.

Each page carries:
  - Title / description / canonical / OG / Twitter
  - MedicalBusiness JSON-LD with PostalAddress and GeoCoordinates
  - BreadcrumbList JSON-LD (Home → Borough → Practice)
  - Hero with name, CQC badge, address, phone
  - Patient survey + CQC stats
  - Action buttons: Register, Call, NHS profile, CQC report
  - "Other practices in {borough}" cross-links (4 nearest by name)
"""

import json, re, sys, html
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

ROOT = Path(__file__).resolve().parent
MERGED_JSON = ROOT / "merged.json"
GPS_JSON = ROOT / "gps.json"
OUT_DIR = ROOT / "practice"
SITEMAP_XML = ROOT / "sitemap.xml"

SITE_URL = "https://londongp.directory"
SITE_NAME = "London GP Directory"

# ---------------------------------------------------------------- helpers

def slug(s):
    s = (s or "").lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s or "unknown"

def cqc_class(r):
    return {
        "Outstanding": "cqc-outstanding",
        "Good": "cqc-good",
        "Requires improvement": "cqc-ri",
        "Inadequate": "cqc-inadequate",
    }.get(r, "cqc-none")

def normalise_phone(ph):
    """Strip spaces / parens for tel: links while keeping +44 etc."""
    if not ph: return ""
    return re.sub(r"[^0-9+]", "", ph)

def normalise_for_schema(addr, postcode):
    """Best-effort split of single-line address into a PostalAddress."""
    parts = [p.strip() for p in (addr or "").split(",") if p.strip()]
    street = parts[0] if parts else ""
    locality = parts[-2] if len(parts) >= 2 else "London"
    return {
        "@type": "PostalAddress",
        "streetAddress": street,
        "addressLocality": locality or "London",
        "postalCode": postcode or "",
        "addressRegion": "Greater London",
        "addressCountry": "GB",
    }

# ---------------------------------------------------------------- CSS

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
.crumbs a:hover{text-decoration:underline}
.hero{background:#003087;color:#fff;padding:40px 24px 32px}
.hero-inner{max-width:1100px;margin:0 auto}
.eyebrow{display:flex;align-items:center;gap:10px;font-size:.7rem;letter-spacing:.15em;text-transform:uppercase;color:#B5D4F4;font-weight:600;margin-bottom:12px}
.eyebrow a{color:#B5D4F4;text-decoration:underline}
h1{font-family:Georgia,serif;font-size:clamp(1.7rem,3.8vw,2.5rem);font-weight:700;line-height:1.15;letter-spacing:-0.01em;margin-bottom:14px;max-width:780px}
.hero-meta{display:flex;flex-wrap:wrap;gap:18px;font-size:.95rem;color:rgba(255,255,255,.85)}
.hero-meta span{display:inline-flex;align-items:center;gap:6px}
.hero-meta a{color:#fff;text-decoration:underline}
.cqc-badge{display:inline-block;padding:4px 11px;border-radius:99px;font-size:.7rem;font-weight:700;letter-spacing:.06em;text-transform:uppercase;margin-bottom:14px}
.cqc-outstanding{background:#E1F5EE;color:#0F6E56}
.cqc-good{background:#D8EFE3;color:#007F3B}
.cqc-ri{background:#FAEEDA;color:#BA7517}
.cqc-inadequate{background:#FCEBEB;color:#A32D2D}
.cqc-none{background:#f0f0ee;color:#777}
main{max-width:1100px;margin:0 auto;padding:32px 24px 56px;display:grid;grid-template-columns:2fr 1fr;gap:36px;align-items:start}
.intro{font-size:.95rem;color:#555;margin-bottom:24px;max-width:680px}
h2{font-family:Georgia,serif;font-size:1.35rem;font-weight:700;color:#003087;margin:24px 0 14px}
h2:first-child{margin-top:0}
.metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:14px;margin-bottom:28px}
.metric{background:#fff;border:1px solid #ddd;border-radius:10px;padding:14px 16px}
.metric-lbl{font-size:.7rem;text-transform:uppercase;letter-spacing:.06em;color:#888;margin-bottom:6px}
.metric-val{font-family:Georgia,serif;font-size:1.6rem;font-weight:700;color:#003087;line-height:1.05}
.metric-track{height:4px;background:#eee;border-radius:99px;overflow:hidden;margin-top:8px}
.metric-bar{height:100%;border-radius:99px}
.actions{display:flex;flex-wrap:wrap;gap:8px;margin:18px 0 28px}
.btn{display:inline-flex;align-items:center;gap:6px;padding:9px 14px;border-radius:8px;font-weight:600;font-size:.88rem;transition:opacity .15s,transform .1s}
.btn:hover{opacity:.92}
.btn:active{transform:translateY(1px)}
.btn-primary{background:#003087;color:#fff}
.btn-secondary{background:#fff;color:#003087;border:1.5px solid #B5D4F4}
.aside{background:#fff;border:1px solid #ddd;border-radius:12px;padding:18px 20px;position:sticky;top:18px}
.aside h3{font-family:Georgia,serif;font-size:1.05rem;color:#003087;margin-bottom:12px}
.aside ul{list-style:none;display:flex;flex-direction:column;gap:8px}
.aside li a{display:block;padding:8px 10px;border-radius:6px;font-size:.85rem;color:#003087;font-weight:500;border:1px solid #eee}
.aside li a:hover{background:#EDF4FC;border-color:#B5D4F4}
.aside .addr-block{font-size:.85rem;color:#555;line-height:1.55;margin-bottom:14px}
.aside .addr-block strong{color:#1a1a1a;display:block;margin-bottom:3px;font-weight:600}
.about{background:#fff;border:1px solid #ddd;border-radius:12px;padding:18px 20px;margin-bottom:24px;font-size:.92rem;line-height:1.65;color:#444}
.about p+p{margin-top:10px}
.faq{background:#fff;border:1px solid #ddd;border-radius:12px;padding:8px 0;margin-bottom:24px}
.faq details{padding:13px 22px;border-bottom:1px solid #f0f0ee}
.faq details:last-child{border-bottom:none}
.faq summary{font-weight:600;font-size:.92rem;color:#1a1a1a;cursor:pointer;list-style:none}
.faq summary::-webkit-details-marker{display:none}
.faq summary::after{content:"+";float:right;color:#003087;font-weight:700}
.faq details[open] summary::after{content:"\\2212"}
.faq details>div{margin-top:9px;font-size:.88rem;color:#555;line-height:1.6}
footer{background:#003087;color:rgba(255,255,255,.55);padding:18px 24px;text-align:center;font-size:.78rem}
footer a{color:rgba(255,255,255,.85)}
@media(max-width:880px){main{grid-template-columns:1fr;padding:24px 18px 40px}.aside{position:static;margin-top:14px}.hero{padding:30px 18px 24px}.hdr,.crumbs,footer{padding-left:18px;padding-right:18px}}
"""

# ---------------------------------------------------------------- render

def render_aside(practice, neighbours):
    addr = html.escape(", ".join(b for b in (practice.get("a", ""), practice.get("p", "")) if b))
    pcn = html.escape(str(practice.get("pcn", "")))
    ph = practice.get("ph", "")
    addr_block = f'<strong>Address</strong>{addr}'
    if ph:
        addr_block += f'<br><br><strong>Phone</strong><a href="tel:{normalise_phone(ph)}" style="color:#003087">{html.escape(ph)}</a>'
    if pcn:
        addr_block += f'<br><br><strong>PCN</strong>{pcn}'

    nbs = ""
    if neighbours:
        nbs = "<h3>Other practices nearby</h3><ul>" + "".join(
            f'<li><a href="/practice/{slug(n["ar"])}/{slug(n["n"])}/">{html.escape(n["n"])}</a></li>'
            for n in neighbours
        ) + "</ul>"
    return f'<aside class="aside"><div class="addr-block">{addr_block}</div>{nbs}</aside>'

def render_metrics(practice):
    blocks = []
    cqc = practice.get("cqc")
    if cqc:
        blocks.append(f'<div class="metric"><div class="metric-lbl">CQC Rating</div><div class="metric-val">{html.escape(cqc)}</div></div>')
    s = practice.get("s")
    if s:
        blocks.append(f'<div class="metric"><div class="metric-lbl">Patient Satisfaction</div><div class="metric-val">{round(s,1)}%</div><div class="metric-track"><div class="metric-bar" style="width:{round(s,1)}%;background:#0072CE"></div></div></div>')
    c = practice.get("c")
    if c:
        blocks.append(f'<div class="metric"><div class="metric-lbl">Contact Ease</div><div class="metric-val">{round(c,1)}%</div><div class="metric-track"><div class="metric-bar" style="width:{round(c,1)}%;background:#0F6E56"></div></div></div>')
    return f'<div class="metrics">{"".join(blocks)}</div>' if blocks else ""

def render_actions(practice):
    out = []
    ods = practice.get("o", "")
    if ods:
        out.append(f'<a class="btn btn-primary" href="https://gp-registration.nhs.uk/{ods}" target="_blank" rel="noopener">Register online &rarr;</a>')
    ph = practice.get("ph", "")
    if ph:
        out.append(f'<a class="btn btn-secondary" href="tel:{normalise_phone(ph)}">Call {html.escape(ph)}</a>')
    if ods:
        out.append(f'<a class="btn btn-secondary" href="https://www.nhs.uk/services/gp-surgery/-/X{ods}" target="_blank" rel="noopener">View on NHS</a>')
    cu = practice.get("cu")
    if cu:
        out.append(f'<a class="btn btn-secondary" href="{html.escape(cu)}" target="_blank" rel="noopener">CQC report</a>')
    return f'<div class="actions">{"".join(out)}</div>' if out else ""

def render_about(practice):
    name = html.escape(practice.get("n", ""))
    borough = html.escape(practice.get("ar", "London"))
    cqc = practice.get("cqc")
    s = practice.get("s")
    pcn = practice.get("pcn", "")
    parts = [
        f"<p>{name} is an NHS GP practice located in {borough}, London. "
        f"All patient registrations are managed through the NHS — there is "
        f"no fee to register. Use the button above to start your registration online.</p>"
    ]
    if cqc:
        parts.append(
            f'<p>The practice is rated <strong>{html.escape(cqc)}</strong> by '
            f'the Care Quality Commission (CQC), the independent regulator '
            f'for health and social care services in England.</p>'
        )
    if s:
        parts.append(
            f"<p>According to the most recent NHS GP Patient Survey, "
            f"{round(s,1)}% of patients reported a positive overall experience "
            f"of care at this practice.</p>"
        )
    if pcn:
        parts.append(
            f'<p>This practice is part of the <strong>{html.escape(pcn)}</strong> '
            f'Primary Care Network (PCN).</p>'
        )
    return f'<div class="about">{"".join(parts)}</div>'

def render_faq(practice):
    name = html.escape(practice.get("n", ""))
    borough = html.escape(practice.get("ar", "London"))
    ods = practice.get("o", "")
    ph = practice.get("ph", "")

    qa = [
        ("How do I register with this practice?",
         f'You can register online at no cost via the NHS at '
         f'<a href="https://gp-registration.nhs.uk/{ods}" target="_blank" rel="noopener">'
         f'gp-registration.nhs.uk</a>, or call the surgery directly. You\'ll need '
         f'proof of address and ID.'),
        ("What area does this practice serve?",
         f"This practice serves patients in {borough} and surrounding postcodes "
         f"in London. Practice catchment areas are set by the practice itself — "
         f"call to confirm whether your address is in their boundary."),
        ("Where is the data on this page from?",
         f"Phone numbers and addresses come directly from the NHS Organisation "
         f"Data Service (ODS) and are refreshed weekly. Patient survey scores "
         f"come from the NHS GP Patient Survey. CQC ratings come from the "
         f"Care Quality Commission."),
    ]
    if ph:
        qa.insert(1, ("How do I contact the practice?",
                     f'Call <a href="tel:{normalise_phone(ph)}">{html.escape(ph)}</a> '
                     f'during opening hours, or visit them at the address shown above.'))
    items = "".join(
        f'<details><summary>{q}</summary><div>{a}</div></details>'
        for q, a in qa
    )
    return f'<div class="faq">{items}</div>'

def render_page(practice, neighbours):
    name = practice.get("n", "Unnamed practice")
    name_h = html.escape(name)
    borough = practice.get("ar", "London")
    borough_h = html.escape(borough)
    bslug = slug(borough)
    pslug = slug(name)
    canonical = f"{SITE_URL}/practice/{bslug}/{pslug}/"

    cqc = practice.get("cqc", "")
    cqc_html = f'<span class="cqc-badge {cqc_class(cqc)}">{html.escape(cqc)}</span>' if cqc else ""

    addr_short = ", ".join(b for b in (practice.get("a", ""), practice.get("p", "")) if b)
    addr_short_h = html.escape(addr_short)
    ph = practice.get("ph", "")
    s = practice.get("s")

    title = f"{name} — {cqc + ' ' if cqc else ''}NHS GP in {borough}, London"
    if len(title) > 65:
        title = f"{name} — NHS GP in {borough}"
    desc_bits = [f"NHS GP practice in {borough}, London."]
    if cqc:
        desc_bits.append(f"Rated {cqc} by the CQC.")
    if s:
        desc_bits.append(f"{round(s)}% patient satisfaction.")
    desc_bits.append("Register online, see opening hours, contact details and reviews.")
    desc = " ".join(desc_bits)

    # ---- JSON-LD MedicalBusiness
    ld = {
        "@context": "https://schema.org",
        "@type": "MedicalBusiness",
        "@id": canonical,
        "name": name,
        "url": canonical,
        "address": normalise_for_schema(practice.get("a", ""), practice.get("p", "")),
        "areaServed": {"@type": "AdministrativeArea", "name": borough},
        "medicalSpecialty": "PrimaryCare",
        "isPartOf": {"@type": "GovernmentOrganization", "name": "NHS England"},
    }
    if ph:
        ld["telephone"] = ph
    la, ln = practice.get("la"), practice.get("ln")
    if la and ln:
        ld["geo"] = {"@type": "GeoCoordinates", "latitude": la, "longitude": ln}
    if s:
        ld["aggregateRating"] = {
            "@type": "AggregateRating",
            "ratingValue": round(s/20, 1),  # 0-100 → 0-5 scale
            "bestRating": 5, "worstRating": 0,
            "ratingCount": 1,
            "reviewAspect": "Overall patient experience (NHS GP Patient Survey)",
        }

    breadcrumb = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": SITE_NAME, "item": SITE_URL + "/"},
            {"@type": "ListItem", "position": 2, "name": borough,
             "item": f"{SITE_URL}/practice/{bslug}/"},
            {"@type": "ListItem", "position": 3, "name": name, "item": canonical},
        ]
    }

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

<script type="application/ld+json">{json.dumps(ld, separators=(",",":"))}</script>
<script type="application/ld+json">{json.dumps(breadcrumb, separators=(",",":"))}</script>

<style>{CSS}</style>
</head>
<body>

<header class="hdr">
  <a class="hdr-logo" href="/">London GP <em>Directory</em></a>
  <nav><a href="/">Home</a><a href="/practice/{bslug}/">{borough_h}</a></nav>
</header>

<div class="crumbs">
  <a href="/">{SITE_NAME}</a> &rsaquo;
  <a href="/practice/{bslug}/">{borough_h}</a> &rsaquo;
  {name_h}
</div>

<section class="hero">
  <div class="hero-inner">
    <div class="eyebrow">NHS GP Practice &middot; <a href="/practice/{bslug}/">More practices in {borough_h}</a></div>
    <h1>{name_h}</h1>
    {cqc_html}
    <div class="hero-meta">
      {"<span>" + addr_short_h + "</span>" if addr_short else ""}
      {"<span><a href='tel:" + normalise_phone(ph) + "'>" + html.escape(ph) + "</a></span>" if ph else ""}
    </div>
  </div>
</section>

<main>
  <div>
    <h2>About {name_h}</h2>
    {render_about(practice)}

    <h2>Patient survey &amp; CQC scores</h2>
    {render_metrics(practice)}

    <h2>Register, call or get directions</h2>
    {render_actions(practice)}

    <h2>Frequently asked questions</h2>
    {render_faq(practice)}
  </div>

  {render_aside(practice, neighbours)}
</main>

<footer>
  Data: NHS ODS &middot; GP Patient Survey &middot; CQC &middot; updated {datetime.now(timezone.utc).strftime("%-d %B %Y")}<br>
  <a href="/">{SITE_NAME}</a> &middot; <a href="/practice/{bslug}/">All practices in {borough_h}</a>
</footer>

</body>
</html>
"""

# ---------------------------------------------------------------- data load

def load_practices():
    if MERGED_JSON.exists():
        print(f"Using {MERGED_JSON.name}")
        return json.loads(MERGED_JSON.read_text())
    if not GPS_JSON.exists():
        sys.exit("Neither merged.json nor gps.json found in repo root.")
    print(f"Falling back to {GPS_JSON.name} — phone/address may be stale.")
    raw = json.loads(GPS_JSON.read_text())
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
            "la":  r.get("la"), "ln": r.get("ln"),
        })
    return out

# ---------------------------------------------------------------- sitemap

def write_sitemap(borough_slugs, practice_urls):
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = ['<?xml version="1.0" encoding="UTF-8"?>',
             '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">',
             f'  <url><loc>{SITE_URL}/</loc><lastmod>{today}</lastmod>'
             f'<changefreq>weekly</changefreq><priority>1.0</priority></url>']
    for bs in sorted(borough_slugs):
        lines.append(f'  <url><loc>{SITE_URL}/practice/{bs}/</loc><lastmod>{today}</lastmod>'
                     f'<changefreq>weekly</changefreq><priority>0.8</priority></url>')
    for url in sorted(practice_urls):
        lines.append(f'  <url><loc>{url}</loc><lastmod>{today}</lastmod>'
                     f'<changefreq>weekly</changefreq><priority>0.6</priority></url>')
    lines.append('</urlset>')
    SITEMAP_XML.write_text("\n".join(lines) + "\n")
    print(f"Wrote {SITEMAP_XML.name}: 1 home + {len(borough_slugs)} boroughs + "
          f"{len(practice_urls)} practices ({len(lines)-2} URLs total).")

# ---------------------------------------------------------------- main

def main():
    practices = load_practices()
    by_borough = defaultdict(list)
    for p in practices:
        b = p.get("ar")
        if b: by_borough[b].append(p)

    print(f"{len(practices)} practices, {len(by_borough)} boroughs")

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    practice_urls = []
    written = 0
    for borough, plist in sorted(by_borough.items()):
        bslug = slug(borough)
        # Sort once so neighbour lists are stable across runs
        plist_sorted = sorted(plist, key=lambda p: str(p.get("n","")).lower())
        for i, p in enumerate(plist_sorted):
            pslug = slug(p.get("n", ""))
            if not pslug or pslug == "unknown":
                continue
            # Pick 4 neighbours (other practices in same borough), prefer closest by index
            neighbours = [q for j, q in enumerate(plist_sorted) if j != i][:4]
            out = OUT_DIR / bslug / pslug / "index.html"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(render_page(p, neighbours))
            practice_urls.append(f"{SITE_URL}/practice/{bslug}/{pslug}/")
            written += 1
        print(f"  {borough:30s} {len(plist_sorted):3d} pages")

    write_sitemap(set(slug(b) for b in by_borough), practice_urls)
    print(f"\nDone. {written} practice pages.")

if __name__ == "__main__":
    main()
