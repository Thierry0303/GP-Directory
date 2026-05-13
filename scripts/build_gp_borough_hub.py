#!/usr/bin/env python3
"""
build_gp_borough_hubs.py
------------------------
Generate one borough hub page per London borough, listing every GP practice
in that borough with map, top-rated practices by GPPS, and a "currently
accepting new patients" filter highlighted at the top.

Also generates PCN hub pages under /pcns/{pcn-slug}/.

Usage:
    python3 scripts/build_gp_borough_hubs.py

Reads:    gps.json
Writes:   gps/{borough-slug}/index.html
          gps/index.html              (top-level borough directory)
          pcns/{pcn-slug}/index.html
          pcns/index.html             (top-level PCN directory)
"""

from __future__ import annotations

import json
import re
import statistics
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
GPS_JSON = REPO_ROOT / "gps.json"
GPS_DIR = REPO_ROOT / "gps"
PCN_DIR = REPO_ROOT / "pcns"
SITE_URL = "https://londongp.directory"

# Shared with build_gp_pages.py
FIELD_MAP = {
    "ods_code": ["ods_code", "OrganisationCode", "code", "id"],
    "name": ["name", "Name", "practice_name"],
    "address": ["address", "Address"],
    "postcode": ["postcode", "Postcode"],
    "borough": ["borough", "Borough", "local_authority"],
    "pcn": ["pcn", "PCN", "primary_care_network"],
    "accepting": ["accepting_new_patients", "accepting", "open_for_registration"],
    "list_size": ["list_size", "registered_patients", "patients"],
    "gpps_overall": ["gpps_overall", "overall_experience"],
    "gpps_phone": ["gpps_phone", "phone_satisfaction"],
    "gpps_appointment": ["gpps_appointment", "appointment_satisfaction"],
    "gpps_continuity": ["gpps_continuity", "continuity_of_care"],
    "cqc_rating": ["cqc_rating", "CQC_rating"],
    "lat": ["lat", "latitude"],
    "lng": ["lng", "lon", "longitude"],
    "slug": ["slug", "practice_slug"],
}

LONDON_BOROUGHS = [
    "Barking and Dagenham", "Barnet", "Bexley", "Brent", "Bromley", "Camden",
    "City of London", "Croydon", "Ealing", "Enfield", "Greenwich", "Hackney",
    "Hammersmith and Fulham", "Haringey", "Harrow", "Havering", "Hillingdon",
    "Hounslow", "Islington", "Kensington and Chelsea", "Kingston upon Thames",
    "Lambeth", "Lewisham", "Merton", "Newham", "Redbridge",
    "Richmond upon Thames", "Southwark", "Sutton", "Tower Hamlets",
    "Waltham Forest", "Wandsworth", "Westminster",
]


def pick(row: dict[str, Any], key: str, default: Any = None) -> Any:
    for candidate in FIELD_MAP.get(key, []):
        if candidate in row and row[candidate] not in (None, ""):
            return row[candidate]
    return default


def slugify(v: str) -> str:
    if not v:
        return ""
    v = unicodedata.normalize("NFKD", v).encode("ascii", "ignore").decode("ascii")
    v = re.sub(r"[^\w\s-]", "", v).strip().lower()
    return re.sub(r"[\s_]+", "-", v)


def canonical_borough(raw: str) -> str | None:
    if not raw:
        return None
    norm = re.sub(r"\s+", " ", raw.strip()).lower().replace("&", "and")
    norm = re.sub(r"^london borough of\s+", "", norm)
    for b in LONDON_BOROUGHS:
        if b.lower() == norm:
            return b
    return None


def is_accepting(v: Any) -> bool:
    return v is True or (isinstance(v, str) and v.lower() in {"yes", "y", "true", "open"})


def safe_mean(vals: Iterable[Any]) -> float | None:
    clean: list[float] = []
    for v in vals:
        try:
            if v in (None, ""):
                continue
            # GPPS values may be "70%" — strip the percent
            if isinstance(v, str):
                v2 = v.replace("%", "").strip()
                clean.append(float(v2))
            else:
                clean.append(float(v))
        except (TypeError, ValueError):
            continue
    if not clean:
        return None
    return round(statistics.mean(clean), 1)


HUB_TEMPLATE = """<!doctype html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<meta name="description" content="{meta_description}">
<link rel="canonical" href="{canonical}">
<meta property="og:title" content="{title}">
<meta property="og:description" content="{meta_description}">
<meta property="og:url" content="{canonical}">
<meta name="theme-color" content="#005EB8">
<meta name="robots" content="index,follow,max-image-preview:large">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  :root{{--ink:#0b2545;--ink-2:#1e3a5f;--nhs:#005EB8;--muted:#5b6b85;--line:#e3e8ef;--bg:#fff;--bg-2:#f6f8fb;--good:#137333;--bad:#b3261e;--radius:10px}}
  *{{box-sizing:border-box}}html,body{{margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Inter","Segoe UI",Roboto,sans-serif;color:var(--ink);line-height:1.55}}
  a{{color:var(--nhs);text-decoration:none}}a:hover{{text-decoration:underline}}
  header.site{{border-bottom:1px solid var(--line);position:sticky;top:0;background:var(--bg);z-index:50}}
  header.site .inner{{max-width:1100px;margin:0 auto;padding:14px 20px;display:flex;justify-content:space-between;align-items:center}}
  header.site a.brand{{color:var(--ink);font-weight:700}}
  header.site nav a{{margin-left:18px;color:var(--ink-2);font-size:14px}}
  main{{max-width:1100px;margin:0 auto;padding:28px 20px 80px}}
  .crumbs{{font-size:13px;color:var(--muted);margin-bottom:14px}}
  .crumbs a{{color:var(--muted)}}
  h1{{font-size:32px;letter-spacing:-0.02em;margin:0 0 8px}}
  .lede{{color:var(--ink-2);font-size:17px;max-width:720px}}
  .stamp{{font-size:12px;color:var(--muted);margin-top:8px}}
  .stamp .dot{{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--good);vertical-align:middle;margin-right:6px}}
  .grid-stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin:24px 0 8px}}
  .stat{{background:var(--bg-2);border:1px solid var(--line);border-radius:var(--radius);padding:14px 16px}}
  .stat .v{{font-size:24px;font-weight:700;color:var(--ink)}}
  .stat .l{{font-size:12px;color:var(--muted);text-transform:uppercase;letter-spacing:0.04em}}
  section{{margin-top:36px}}
  h2{{font-size:22px;margin:0 0 12px;letter-spacing:-0.01em}}
  table{{width:100%;border-collapse:collapse;font-size:14px}}
  th,td{{text-align:left;padding:10px 12px;border-bottom:1px solid var(--line)}}
  th{{font-size:12px;text-transform:uppercase;letter-spacing:0.04em;color:var(--muted);font-weight:600}}
  .accept{{display:inline-block;background:var(--good);color:#fff;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600}}
  .closed{{display:inline-block;background:var(--bad);color:#fff;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600}}
  .unknown{{display:inline-block;background:var(--muted);color:#fff;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600}}
  #map{{height:380px;border-radius:var(--radius);border:1px solid var(--line)}}
  .twocol{{display:grid;grid-template-columns:1.4fr 1fr;gap:28px}}
  @media (max-width:820px){{.twocol{{grid-template-columns:1fr}}}}
  details.faq{{border:1px solid var(--line);border-radius:var(--radius);padding:14px 16px;margin-bottom:8px;background:var(--bg)}}
  details.faq summary{{cursor:pointer;font-weight:600;color:var(--ink)}}
  .sources{{font-size:13px;color:var(--muted)}}
  .sources a{{color:var(--muted);text-decoration:underline}}
  .cta{{display:inline-block;background:var(--nhs);color:#fff;padding:10px 14px;border-radius:var(--radius);font-size:14px;font-weight:600}}
  .cta:hover{{background:#004593;text-decoration:none}}
  footer{{border-top:1px solid var(--line);padding:28px 20px;color:var(--muted);font-size:13px}}
  footer .inner{{max-width:1100px;margin:0 auto;display:flex;gap:24px;flex-wrap:wrap}}
</style>
<script type="application/ld+json">
{schema_json}
</script>
</head>
<body>
<header class="site">
  <div class="inner">
    <a class="brand" href="/">London GP Directory</a>
    <nav>
      <a href="/">Search</a>
      <a href="/gps/">Boroughs</a>
      <a href="/guides/how-to-register-with-a-london-gp/">Register</a>
      <a href="/methodology.html">Methodology</a>
    </nav>
  </div>
</header>
<main>
  <div class="crumbs">
    <a href="/">Home</a> &rsaquo; <a href="/gps/">London</a> &rsaquo; {area_name}
  </div>

  <h1>{h1}</h1>
  <p class="lede">{lede}</p>
  <p class="stamp"><span class="dot"></span>Data last verified {last_updated}. Sources: NHS Digital ODS, GP Patient Survey, CQC.</p>

  <section>
    <div class="grid-stats">
      <div class="stat"><div class="v">{total}</div><div class="l">GP practices</div></div>
      <div class="stat"><div class="v">{accepting_count}</div><div class="l">Accepting new patients</div></div>
      <div class="stat"><div class="v">{mean_overall}</div><div class="l">Avg overall experience</div></div>
      <div class="stat"><div class="v">{mean_phone}</div><div class="l">Avg ease of phone</div></div>
      <div class="stat"><div class="v">{total_list_size}</div><div class="l">Registered patients</div></div>
    </div>
  </section>

  <section>
    <div class="twocol">
      <div>
        <h2>Practices accepting new patients</h2>
        {accepting_table}
        <p style="margin-top:14px"><a class="cta" href="{filter_link}">See all {total} practices</a></p>
      </div>
      <div>
        <h2>Map</h2>
        <div id="map" aria-label="Map of GP practices in {area_name}"></div>
      </div>
    </div>
  </section>

  <section>
    <h2>All GP practices in {area_name}</h2>
    {all_table}
  </section>

  <section>
    <h2>Frequently asked</h2>
    {faq_html}
  </section>

  <section>
    <h2>Sources</h2>
    <p class="sources">
      Practice register from <a href="https://digital.nhs.uk/services/organisation-data-service" rel="nofollow">NHS Digital ODS</a> (refreshed weekly).
      Patient experience scores from <a href="https://gp-patient.co.uk/" rel="nofollow">GP Patient Survey</a> (annual).
      CQC ratings from <a href="https://www.cqc.org.uk/" rel="nofollow">Care Quality Commission</a> (refreshed monthly).
      Full <a href="/methodology.html">methodology</a>.
    </p>
  </section>
</main>
<footer>
  <div class="inner">
    <div>&copy; London GP Directory. Independent. Free to use. Not affiliated with the NHS.</div>
    <div><a href="/about.html">About</a> &middot; <a href="/methodology.html">Methodology</a> &middot; <a href="/sources.html">Sources</a></div>
  </div>
</footer>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
  (function() {{
    var points = {map_points_json};
    if (!points.length) return;
    var lats = points.map(function(p){{return p[0];}});
    var lngs = points.map(function(p){{return p[1];}});
    var avgLat = lats.reduce(function(a,b){{return a+b;}},0)/lats.length;
    var avgLng = lngs.reduce(function(a,b){{return a+b;}},0)/lngs.length;
    var map = L.map('map',{{ scrollWheelZoom:false }}).setView([avgLat,avgLng], 12);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png',{{
      maxZoom:18, attribution:'&copy; OpenStreetMap contributors'
    }}).addTo(map);
    points.forEach(function(p){{
      var color = p[4] ? '#137333' : '#5b6b85';
      L.circleMarker([p[0],p[1]],{{ radius:5, color:'#0b2545', fillColor:color, fillOpacity:0.85, weight:1 }})
        .addTo(map).bindPopup('<a href="'+p[2]+'">'+p[3]+'</a>');
    }});
  }})();
</script>
</body>
</html>
"""


def render_table(rows: list[dict[str, Any]], borough_slug: str) -> str:
    if not rows:
        return "<p>No practices listed yet.</p>"
    parts = [
        "<table><thead><tr><th>Practice</th><th>Accepting</th><th>GPPS overall</th><th>CQC</th><th>List size</th></tr></thead><tbody>"
    ]
    for r in rows:
        name = pick(r, "name") or "Unknown"
        slug = pick(r, "slug") or slugify(name)
        b_slug = slugify(pick(r, "borough") or "")
        url = f"/gps/{b_slug}/{slug}/"
        accept = is_accepting(pick(r, "accepting"))
        accept_pill = '<span class="accept">Yes</span>' if accept else '<span class="closed">No</span>'
        gpps = pick(r, "gpps_overall") or "—"
        cqc = pick(r, "cqc_rating") or "—"
        list_size = pick(r, "list_size")
        list_size_str = f"{int(float(list_size)):,}" if list_size not in (None, "") else "—"
        parts.append(
            f'<tr><td><a href="{url}">{name}</a></td><td>{accept_pill}</td><td>{gpps}</td><td>{cqc}</td><td>{list_size_str}</td></tr>'
        )
    parts.append("</tbody></table>")
    return "\n".join(parts)


def make_faq(area: str, total: int, accepting_count: int) -> tuple[str, list[dict[str, str]]]:
    faqs = [
        {"q": f"How many GP practices are there in {area}?",
         "a": f"There are {total} NHS GP practices currently listed in {area}, drawn from the NHS Digital Organisation Data Service. At the most recent refresh, {accepting_count} were accepting new patients."},
        {"q": f"How do I register with a GP in {area}?",
         "a": f"You can register with any practice in {area} that has open lists, using the <a href='https://www.nhs.uk/nhs-services/gps/how-to-register-with-a-gp-surgery/' rel='nofollow'>NHS online registration service</a>, by phoning the practice, or in person. You don't need proof of address or immigration status to register. See our <a href='/guides/how-to-register-with-a-london-gp/'>full registration guide</a>."},
        {"q": "What if I can't get through to my GP by phone?",
         "a": "If you need urgent advice and can't reach your practice, call NHS 111 or use 111 online. For a life-threatening emergency, call 999. You can raise a complaint about access with the practice manager, NHS England, or via <a href='https://www.healthwatch.co.uk/' rel='nofollow'>Healthwatch</a>."},
        {"q": "Can I switch GP?",
         "a": "Yes — you can switch GP at any time. Simply register with your new practice and your records will be transferred. You don't need permission from your existing GP. See our <a href='/guides/how-to-switch-gp-in-london/'>switching guide</a>."},
        {"q": "What is a Primary Care Network (PCN)?",
         "a": "PCNs are groups of GP practices that work together — typically covering 30,000–50,000 patients — to deliver shared services like pharmacists, physiotherapists, and social prescribers. Your registered practice is part of one PCN, but you don't choose your PCN directly."},
    ]
    html = "\n".join(f'<details class="faq"><summary>{f["q"]}</summary><p>{f["a"]}</p></details>' for f in faqs)
    jsonld = [{"q": f["q"], "a": re.sub(r"<.*?>", "", f["a"])} for f in faqs]
    return html, jsonld


def make_schema(area: str, canonical: str, items: list[dict[str, Any]], faqs: list[dict[str, str]], breadcrumb_extra: list[dict[str, str]] | None = None) -> str:
    item_list = {
        "@context": "https://schema.org",
        "@type": "ItemList",
        "name": f"GP practices in {area}",
        "numberOfItems": len(items),
        "itemListElement": [],
    }
    for i, r in enumerate(items[:50], 1):
        name = pick(r, "name") or "Practice"
        slug = pick(r, "slug") or slugify(name)
        b_slug = slugify(pick(r, "borough") or "")
        item_list["itemListElement"].append({
            "@type": "ListItem",
            "position": i,
            "url": f"{SITE_URL}/gps/{b_slug}/{slug}/",
            "name": name,
        })
    crumbs = [
        {"@type": "ListItem", "position": 1, "name": "Home", "item": SITE_URL + "/"},
        {"@type": "ListItem", "position": 2, "name": "London", "item": SITE_URL + "/gps/"},
    ]
    crumbs.append({"@type": "ListItem", "position": 3, "name": area, "item": canonical})
    breadcrumbs = {"@context": "https://schema.org", "@type": "BreadcrumbList", "itemListElement": crumbs}
    faq_page = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": f["q"], "acceptedAnswer": {"@type": "Answer", "text": f["a"]}}
            for f in faqs
        ],
    }
    return json.dumps([item_list, breadcrumbs, faq_page], indent=2)


def render_hub(area: str, slug: str, group: list[dict[str, Any]], canonical: str, today: str, area_label: str, filter_link: str, lede: str) -> str:
    total = len(group)
    accepting = [p for p in group if is_accepting(pick(p, "accepting"))]
    accepting_sorted = sorted(
        accepting,
        key=lambda r: (float(str(pick(r, "gpps_overall") or "0").replace("%", "")) if pick(r, "gpps_overall") else 0),
        reverse=True,
    )[:10]
    all_sorted = sorted(group, key=lambda r: (pick(r, "name") or "").lower())

    mean_overall = safe_mean(pick(p, "gpps_overall") for p in group)
    mean_phone = safe_mean(pick(p, "gpps_phone") for p in group)
    list_sizes: list[float] = []
    for p in group:
        v = pick(p, "list_size")
        try:
            if v not in (None, ""):
                list_sizes.append(float(v))
        except (TypeError, ValueError):
            continue
    total_list_size = f"{int(sum(list_sizes)):,}" if list_sizes else "—"

    map_points: list[list[Any]] = []
    for p in group:
        lat = pick(p, "lat")
        lng = pick(p, "lng")
        try:
            lat_f = float(lat); lng_f = float(lng)
        except (TypeError, ValueError):
            continue
        name = pick(p, "name") or "Practice"
        b_slug = slugify(pick(p, "borough") or "")
        p_slug = pick(p, "slug") or slugify(name)
        url = f"/gps/{b_slug}/{p_slug}/"
        map_points.append([lat_f, lng_f, url, name, is_accepting(pick(p, "accepting"))])

    faq_html, faq_jsonld = make_faq(area, total, len(accepting))
    schema_json = make_schema(area, canonical, all_sorted, faq_jsonld)

    title = f"GP practices in {area}, London — {total} surgeries | London GP Directory"
    meta_description = (
        f"{total} GP practices in {area}, London — {len(accepting)} currently accepting new patients. "
        f"Patient experience scores, CQC ratings, list sizes. Updated weekly."
    )[:158]

    return HUB_TEMPLATE.format(
        title=title,
        meta_description=meta_description,
        canonical=canonical,
        area_name=area,
        h1=f"GP practices in {area_label}",
        lede=lede,
        last_updated=today,
        total=total,
        accepting_count=len(accepting),
        mean_overall=f"{mean_overall}%" if mean_overall is not None else "—",
        mean_phone=f"{mean_phone}%" if mean_phone is not None else "—",
        total_list_size=total_list_size,
        accepting_table=render_table(accepting_sorted, slug),
        all_table=render_table(all_sorted, slug),
        filter_link=filter_link,
        faq_html=faq_html,
        map_points_json=json.dumps(map_points),
        schema_json=schema_json,
    )


def build():
    if not GPS_JSON.exists():
        raise SystemExit(f"Could not find {GPS_JSON}. Run refresh_nhs_data.py first.")

    raw = json.loads(GPS_JSON.read_text(encoding="utf-8"))
    practices = raw if isinstance(raw, list) else raw.get("practices", raw.get("gps", []))
    print(f"Loaded {len(practices)} practices")

    by_borough: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_pcn: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for p in practices:
        b = canonical_borough(pick(p, "borough"))
        if b:
            by_borough[b].append(p)
        pcn = pick(p, "pcn")
        if pcn:
            by_pcn[pcn].append(p)

    today = datetime.now(timezone.utc).strftime("%d %B %Y")

    GPS_DIR.mkdir(parents=True, exist_ok=True)
    PCN_DIR.mkdir(parents=True, exist_ok=True)
    borough_links: list[str] = []

    for borough in LONDON_BOROUGHS:
        group = by_borough.get(borough, [])
        slug = slugify(borough)
        out_dir = GPS_DIR / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        canonical = f"{SITE_URL}/gps/{slug}/"
        lede = (
            f"All NHS GP practices in {borough}, with current registration status, GP Patient Survey scores, "
            f"CQC ratings and map. Updated weekly from official NHS Digital data."
        )
        html = render_hub(borough, slug, group, canonical, today, borough, f"/?borough={slug}", lede)
        (out_dir / "index.html").write_text(html, encoding="utf-8")
        accepting_count = sum(1 for p in group if is_accepting(pick(p, "accepting")))
        borough_links.append(
            f'<li><a href="/gps/{slug}/">{borough}</a> '
            f'<span style="color:#5b6b85">({len(group)} practices, {accepting_count} accepting)</span></li>'
        )
        print(f"  wrote /gps/{slug}/index.html  ({len(group)} practices)")

    # Top-level borough index
    index_html = f"""<!doctype html>
<html lang="en-GB"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>GP practices by London borough — London GP Directory</title>
<meta name="description" content="Browse all NHS GP practices by London borough. Accepting new patients, GP Patient Survey scores, CQC ratings, map.">
<link rel="canonical" href="{SITE_URL}/gps/">
<style>body{{font-family:-apple-system,BlinkMacSystemFont,"Inter",sans-serif;max-width:900px;margin:0 auto;padding:28px 20px;color:#0b2545}}h1{{font-size:28px;letter-spacing:-0.02em}}ul{{list-style:none;padding:0;columns:2;column-gap:24px}}@media(max-width:640px){{ul{{columns:1}}}}li{{break-inside:avoid;padding:8px 0;border-bottom:1px solid #e3e8ef}}a{{color:#005EB8;text-decoration:none;font-weight:500}}a:hover{{text-decoration:underline}}.crumbs{{font-size:13px;color:#5b6b85;margin-bottom:14px}}.crumbs a{{color:#5b6b85}}</style>
</head><body>
<div class="crumbs"><a href="/">Home</a> &rsaquo; London</div>
<h1>GP practices by London borough</h1>
<p>Browse every NHS GP practice in each of London's 33 boroughs. Each hub shows which are accepting new patients, average patient experience, CQC ratings and a borough map. Updated weekly.</p>
<ul>{''.join(borough_links)}</ul>
</body></html>"""
    (GPS_DIR / "index.html").write_text(index_html, encoding="utf-8")

    # PCN hubs
    pcn_links: list[str] = []
    for pcn, group in sorted(by_pcn.items()):
        slug = slugify(pcn)
        out_dir = PCN_DIR / slug
        out_dir.mkdir(parents=True, exist_ok=True)
        canonical = f"{SITE_URL}/pcns/{slug}/"
        total_patients = 0
        for p in group:
            try:
                v = pick(p, "list_size")
                if v not in (None, ""):
                    total_patients += int(float(v))
            except (TypeError, ValueError):
                continue
        lede = (
            f"{pcn} is a Primary Care Network covering {len(group)} GP practice"
            f"{'s' if len(group) != 1 else ''} and approximately {total_patients:,} registered patients. "
            "PCNs share services such as pharmacists, social prescribers and physiotherapists across their member practices."
        )
        html = render_hub(pcn, slug, group, canonical, today, pcn, f"/?pcn={slug}", lede)
        (out_dir / "index.html").write_text(html, encoding="utf-8")
        pcn_links.append(f'<li><a href="/pcns/{slug}/">{pcn}</a> <span style="color:#5b6b85">({len(group)} practices)</span></li>')

    pcn_index = f"""<!doctype html>
<html lang="en-GB"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Primary Care Networks (PCNs) in London — London GP Directory</title>
<meta name="description" content="All London Primary Care Networks. Practices, patient list size, services and patient experience by PCN.">
<link rel="canonical" href="{SITE_URL}/pcns/">
<style>body{{font-family:-apple-system,BlinkMacSystemFont,"Inter",sans-serif;max-width:900px;margin:0 auto;padding:28px 20px;color:#0b2545}}h1{{font-size:28px;letter-spacing:-0.02em}}ul{{list-style:none;padding:0;columns:2;column-gap:24px}}@media(max-width:640px){{ul{{columns:1}}}}li{{break-inside:avoid;padding:8px 0;border-bottom:1px solid #e3e8ef}}a{{color:#005EB8;text-decoration:none;font-weight:500}}a:hover{{text-decoration:underline}}.crumbs{{font-size:13px;color:#5b6b85;margin-bottom:14px}}.crumbs a{{color:#5b6b85}}</style>
</head><body>
<div class="crumbs"><a href="/">Home</a> &rsaquo; Primary Care Networks</div>
<h1>Primary Care Networks (PCNs) in London</h1>
<p>Primary Care Networks are groups of GP practices that share services. London has roughly 200 PCNs across its 33 boroughs.</p>
<ul>{''.join(pcn_links)}</ul>
</body></html>"""
    (PCN_DIR / "index.html").write_text(pcn_index, encoding="utf-8")

    print(f"\nDone. {len(LONDON_BOROUGHS)} borough hubs, {len(by_pcn)} PCN hubs.")


if __name__ == "__main__":
    build()
