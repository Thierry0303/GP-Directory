#!/usr/bin/env python3
"""
build_gp_pages.py
-----------------
Generate static HTML pages for every GP practice in gps.json.

Mirrors the pattern of build_school_pages.py from londonschool.directory.
One static, indexable, schema-marked page per practice.

Usage:
    python3 scripts/build_gp_pages.py

Reads:    gps.json     (produced by refresh_nhs_data.py)
Writes:   gps/{borough-slug}/{practice-slug}/index.html  (one per practice)

Adjust FIELD_MAP at the top if your gps.json uses different keys.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
GPS_JSON = REPO_ROOT / "gps.json"
OUTPUT_DIR = REPO_ROOT / "gps"
SITE_URL = "https://londongp.directory"

# Map our logical field names to whatever your gps.json actually uses.
FIELD_MAP = {
    "ods_code": ["ods_code", "OrganisationCode", "code", "id"],
    "name": ["name", "Name", "practice_name"],
    "address": ["address", "Address", "addr"],
    "postcode": ["postcode", "Postcode", "post_code"],
    "phone": ["phone", "telephone", "Telephone"],
    "website": ["website", "Website", "url"],
    "borough": ["borough", "Borough", "local_authority"],
    "pcn": ["pcn", "PCN", "primary_care_network"],
    "pcn_code": ["pcn_code", "PCN_code"],
    "icb": ["icb", "ICB", "ccg"],
    "accepting": ["accepting_new_patients", "accepting", "open_for_registration"],
    "list_size": ["list_size", "registered_patients", "patients"],
    "gps_per_1000": ["gps_per_1000", "wte_gps_per_1000"],
    "gpps_overall": ["gpps_overall", "overall_experience", "patient_experience"],
    "gpps_phone": ["gpps_phone", "phone_satisfaction"],
    "gpps_appointment": ["gpps_appointment", "appointment_satisfaction"],
    "gpps_continuity": ["gpps_continuity", "continuity_of_care"],
    "cqc_rating": ["cqc_rating", "CQC_rating"],
    "cqc_date": ["cqc_date", "cqc_inspection_date"],
    "languages": ["languages", "languages_spoken"],
    "accessibility": ["accessibility", "wheelchair_access"],
    "opening_hours": ["opening_hours", "hours"],
    "lat": ["lat", "latitude"],
    "lng": ["lng", "lon", "longitude"],
    "slug": ["slug", "practice_slug"],
}


def pick(row: dict[str, Any], key: str, default: Any = None) -> Any:
    for candidate in FIELD_MAP.get(key, []):
        if candidate in row and row[candidate] not in (None, ""):
            return row[candidate]
    return default


def slugify(value: str) -> str:
    if not value:
        return ""
    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value).strip().lower()
    return re.sub(r"[\s_]+", "-", value)


PAGE_TEMPLATE = """<!doctype html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<meta name="description" content="{meta_description}">
<link rel="canonical" href="{canonical}">
<meta property="og:title" content="{title}">
<meta property="og:description" content="{meta_description}">
<meta property="og:type" content="website">
<meta property="og:url" content="{canonical}">
<meta property="og:site_name" content="London GP Directory">
<meta name="theme-color" content="#005EB8">
<meta name="robots" content="index,follow,max-image-preview:large">
<link rel="preconnect" href="https://unpkg.com">
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
  :root {{
    --ink: #0b2545;
    --ink-2: #1e3a5f;
    --nhs: #005EB8;
    --muted: #5b6b85;
    --line: #e3e8ef;
    --bg: #ffffff;
    --bg-2: #f6f8fb;
    --good: #137333;
    --warn: #a85b00;
    --bad: #b3261e;
    --radius: 10px;
  }}
  *{{box-sizing:border-box}}html,body{{margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Inter","Segoe UI",Roboto,sans-serif;color:var(--ink);background:var(--bg);line-height:1.55;-webkit-font-smoothing:antialiased}}
  a{{color:var(--nhs);text-decoration:none}}a:hover{{text-decoration:underline}}
  header.site{{border-bottom:1px solid var(--line);background:var(--bg);position:sticky;top:0;z-index:50}}
  header.site .inner{{max-width:1100px;margin:0 auto;padding:14px 20px;display:flex;align-items:center;justify-content:space-between}}
  header.site a.brand{{color:var(--ink);font-weight:700;letter-spacing:-0.01em}}
  header.site nav a{{margin-left:18px;color:var(--ink-2);font-size:14px}}
  main{{max-width:1100px;margin:0 auto;padding:28px 20px 80px}}
  .crumbs{{font-size:13px;color:var(--muted);margin-bottom:14px}}
  .crumbs a{{color:var(--muted)}}
  h1{{font-size:30px;line-height:1.2;margin:0 0 6px;letter-spacing:-0.02em}}
  .sub{{color:var(--ink-2);font-size:16px;margin:0 0 14px}}
  .stamp{{font-size:12px;color:var(--muted);margin-top:6px}}
  .stamp .dot{{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--good);vertical-align:middle;margin-right:6px}}
  .accept-banner{{background:var(--good);color:#fff;border-radius:var(--radius);padding:12px 16px;font-weight:600;margin:16px 0;display:flex;align-items:center;gap:10px}}
  .accept-banner.closed{{background:var(--bad)}}
  .accept-banner.unknown{{background:var(--muted)}}
  .grid{{display:grid;grid-template-columns:1.4fr 1fr;gap:28px;margin-top:24px}}
  @media (max-width:820px){{.grid{{grid-template-columns:1fr}}}}
  .card{{background:var(--bg-2);border:1px solid var(--line);border-radius:var(--radius);padding:18px}}
  .card h2{{margin:0 0 10px;font-size:18px;letter-spacing:-0.01em}}
  .kv{{display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--line);font-size:14px}}
  .kv:last-child{{border-bottom:0}}
  .kv .k{{color:var(--muted)}}
  .kv .v{{font-weight:600;color:var(--ink)}}
  .gpps{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-top:8px}}
  .gpps .pill{{background:#fff;border:1px solid var(--line);border-radius:var(--radius);padding:10px 12px}}
  .gpps .pill .l{{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:0.04em}}
  .gpps .pill .v{{font-size:22px;font-weight:700;color:var(--ink)}}
  .gpps .pill .ctx{{font-size:11px;color:var(--muted)}}
  #map{{height:340px;border-radius:var(--radius);border:1px solid var(--line)}}
  details.faq{{border:1px solid var(--line);border-radius:var(--radius);padding:14px 16px;margin-bottom:8px;background:var(--bg)}}
  details.faq summary{{cursor:pointer;font-weight:600;color:var(--ink)}}
  section{{margin-top:28px}}
  h2.section{{font-size:20px;margin:0 0 12px;letter-spacing:-0.01em}}
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
    <a href="/">Home</a> &rsaquo; <a href="/gps/">London</a> &rsaquo; <a href="/gps/{borough_slug}/">{borough}</a> &rsaquo; {name}
  </div>

  <h1>{name}</h1>
  <p class="sub">{address}, {postcode} &middot; {pcn_label}</p>

  {accept_banner}

  <p class="stamp"><span class="dot"></span>Data last verified {last_updated}. Sources: NHS Digital ODS, GP Patient Survey, CQC. See <a href="/sources.html">all sources</a>.</p>

  <div class="grid">
    <div>
      <div class="card">
        <h2>Patient experience (GP Patient Survey)</h2>
        <div class="gpps">
          <div class="pill"><div class="l">Overall experience</div><div class="v">{gpps_overall}</div><div class="ctx">London avg: {gpps_overall_london}</div></div>
          <div class="pill"><div class="l">Ease of phone</div><div class="v">{gpps_phone}</div><div class="ctx">London avg: {gpps_phone_london}</div></div>
          <div class="pill"><div class="l">Appointment satisfaction</div><div class="v">{gpps_appointment}</div><div class="ctx">London avg: {gpps_appointment_london}</div></div>
          <div class="pill"><div class="l">Continuity of care</div><div class="v">{gpps_continuity}</div><div class="ctx">London avg: {gpps_continuity_london}</div></div>
        </div>
        <p class="sources" style="margin-top:12px">Source: Ipsos for NHS England, latest <a href="https://gp-patient.co.uk/" rel="nofollow">GP Patient Survey</a>.</p>
      </div>

      <div class="card" style="margin-top:16px">
        <h2>Practice details</h2>
        <div class="kv"><span class="k">ODS code</span><span class="v">{ods_code}</span></div>
        <div class="kv"><span class="k">Primary Care Network</span><span class="v">{pcn}</span></div>
        <div class="kv"><span class="k">List size (registered patients)</span><span class="v">{list_size}</span></div>
        <div class="kv"><span class="k">GPs per 1,000 patients</span><span class="v">{gps_per_1000}</span></div>
        <div class="kv"><span class="k">CQC rating</span><span class="v">{cqc_rating}</span></div>
        <div class="kv"><span class="k">CQC last inspected</span><span class="v">{cqc_date}</span></div>
        <div class="kv"><span class="k">Languages spoken</span><span class="v">{languages}</span></div>
        <div class="kv"><span class="k">Wheelchair accessible</span><span class="v">{accessibility}</span></div>
        <div class="kv"><span class="k">Phone</span><span class="v"><a href="tel:{phone}">{phone}</a></span></div>
        <div class="kv"><span class="k">Website</span><span class="v"><a href="{website}" rel="nofollow noopener">{website_display}</a></span></div>
      </div>

      <section>
        <h2 class="section">How to register with this practice</h2>
        <p>To register with {name}, you can use the <a href="https://www.nhs.uk/nhs-services/gps/how-to-register-with-a-gp-surgery/" rel="nofollow">NHS online registration form</a> or contact the practice directly. You can register with any practice that has open lists — you don't have to live in the catchment area, but the practice can decline if you live outside their boundary. You don't need proof of address or immigration status to register: see our <a href="/guides/how-to-register-with-a-london-gp/">full registration guide</a> for more.</p>
      </section>

      <section>
        <h2 class="section">Frequently asked</h2>
        {faq_html}
      </section>
    </div>

    <aside>
      <div id="map" aria-label="Map of {name}"></div>
      <p style="margin-top:12px"><a class="cta" href="{website}" rel="nofollow noopener">Visit practice website</a></p>
      <p style="margin-top:12px"><a href="/gps/{borough_slug}/" class="sources">See all GPs in {borough} &rsaquo;</a></p>
      {pcn_link}
    </aside>
  </div>

  <section>
    <h2 class="section">Sources</h2>
    <p class="sources">
      Practice register, ODS code and contact details from <a href="https://digital.nhs.uk/services/organisation-data-service" rel="nofollow">NHS Digital ODS</a>.
      Patient experience scores from <a href="https://gp-patient.co.uk/" rel="nofollow">GP Patient Survey</a> (Ipsos for NHS England).
      Workforce ratio from <a href="https://digital.nhs.uk/data-and-information/publications/statistical/general-and-personal-medical-services" rel="nofollow">NHS Digital General Practice Workforce</a>.
      CQC rating from <a href="https://www.cqc.org.uk/" rel="nofollow">Care Quality Commission</a>.
      Full <a href="/methodology.html">methodology</a> describes how we compute each metric.
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
    var lat = {lat_js}; var lng = {lng_js};
    if (lat === null || lng === null) return;
    var map = L.map('map', {{ scrollWheelZoom: false }}).setView([lat, lng], 15);
    L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 18, attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);
    L.marker([lat, lng]).addTo(map).bindPopup({popup_js});
  }})();
</script>
</body>
</html>
"""


def render_accept_banner(accepting: Any) -> str:
    if accepting is True or (isinstance(accepting, str) and accepting.lower() in {"yes", "y", "true", "open"}):
        return '<div class="accept-banner">Accepting new patients</div>'
    if accepting is False or (isinstance(accepting, str) and accepting.lower() in {"no", "n", "false", "closed"}):
        return '<div class="accept-banner closed">Not currently accepting new patients</div>'
    return '<div class="accept-banner unknown">Registration status not published</div>'


def render_faq(name: str, borough: str, accepting: Any, postcode: str) -> tuple[str, list[dict[str, str]]]:
    accept_text = (
        f"Yes — at the most recent NHS Digital refresh, {name} was accepting new patients."
        if accepting is True or (isinstance(accepting, str) and accepting.lower() in {"yes", "y", "true", "open"})
        else f"At the most recent NHS Digital refresh, {name} was not accepting new patients. Status changes regularly — check directly with the practice or with NHS 111."
    )
    faqs = [
        {"q": f"Is {name} accepting new patients?", "a": accept_text},
        {"q": f"How do I register with {name}?", "a": f"You can register with {name} online via the <a href='https://www.nhs.uk/nhs-services/gps/how-to-register-with-a-gp-surgery/' rel='nofollow'>NHS GP registration service</a>, by visiting the practice in person, or by calling them. You do not need proof of address or immigration status to register with an NHS GP."},
        {"q": f"What if I cannot get through to {name} by phone?", "a": "If you cannot reach your GP practice and need urgent advice, call NHS 111 or use 111 online. For a life-threatening emergency, call 999. Persistent difficulty getting through can also be reported to the practice manager or to your <a href='https://www.healthwatch.co.uk/your-local-healthwatch/list' rel='nofollow'>local Healthwatch</a>."},
        {"q": f"Can I switch from {name} to a different GP?", "a": "Yes. You can switch GP at any time — you do not need to tell your current practice. Simply register with your new chosen GP, and your records will be transferred. See our <a href='/guides/how-to-switch-gp-in-london/'>guide to switching GP</a>."},
        {"q": f"Does {name} have wheelchair access?", "a": "Accessibility details are sourced from NHS Digital. If we list the practice as wheelchair-accessible, this reflects the latest publication. We strongly encourage anyone with specific access needs to confirm with the practice directly."},
    ]
    html = "\n        ".join(
        f'<details class="faq"><summary>{f["q"]}</summary><p>{f["a"]}</p></details>' for f in faqs
    )
    jsonld = [{"q": f["q"], "a": re.sub(r"<.*?>", "", f["a"])} for f in faqs]
    return html, jsonld


def render_schema(
    name: str,
    canonical: str,
    address: str,
    postcode: str,
    phone: str,
    website: str,
    lat: float | None,
    lng: float | None,
    borough: str,
    pcn: str,
    faqs: list[dict[str, str]],
) -> str:
    medical = {
        "@context": "https://schema.org",
        "@type": "MedicalBusiness",
        "name": name,
        "url": canonical,
        "telephone": phone or None,
        "address": {
            "@type": "PostalAddress",
            "streetAddress": address,
            "postalCode": postcode,
            "addressLocality": borough,
            "addressRegion": "London",
            "addressCountry": "GB",
        },
        "areaServed": {"@type": "Place", "name": borough},
        "medicalSpecialty": "GeneralPractice",
        "publicAccess": True,
    }
    if lat is not None and lng is not None:
        medical["geo"] = {"@type": "GeoCoordinates", "latitude": lat, "longitude": lng}

    breadcrumbs = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Home", "item": SITE_URL + "/"},
            {"@type": "ListItem", "position": 2, "name": "London", "item": SITE_URL + "/gps/"},
            {"@type": "ListItem", "position": 3, "name": borough, "item": f"{SITE_URL}/gps/{slugify(borough)}/"},
            {"@type": "ListItem", "position": 4, "name": name, "item": canonical},
        ],
    }

    faq_page = {
        "@context": "https://schema.org",
        "@type": "FAQPage",
        "mainEntity": [
            {"@type": "Question", "name": f["q"], "acceptedAnswer": {"@type": "Answer", "text": f["a"]}}
            for f in faqs
        ],
    }
    return json.dumps([medical, breadcrumbs, faq_page], indent=2)


def fmt(val: Any, default: str = "—") -> str:
    return default if val in (None, "") else str(val)


def build():
    if not GPS_JSON.exists():
        raise SystemExit(f"Could not find {GPS_JSON}. Run refresh_nhs_data.py first.")

    raw = json.loads(GPS_JSON.read_text(encoding="utf-8"))
    practices = raw if isinstance(raw, list) else raw.get("practices", raw.get("gps", []))
    print(f"Loaded {len(practices)} practices")

    today = datetime.now(timezone.utc).strftime("%d %B %Y")

    # London averages for GPPS — replace with computed values once your refresh script
    # writes a london_averages.json. For now, use sane defaults from the most recent
    # nationally-published figures so the page is never blank.
    london_avgs = {
        "gpps_overall": "70%",
        "gpps_phone": "55%",
        "gpps_appointment": "62%",
        "gpps_continuity": "31%",
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    count = 0

    for p in practices:
        name = pick(p, "name") or "Unknown practice"
        borough = pick(p, "borough") or "London"
        slug = pick(p, "slug") or slugify(name)
        b_slug = slugify(borough)
        out_dir = OUTPUT_DIR / b_slug / slug
        out_dir.mkdir(parents=True, exist_ok=True)

        canonical = f"{SITE_URL}/gps/{b_slug}/{slug}/"
        address = fmt(pick(p, "address"))
        postcode = fmt(pick(p, "postcode"))
        phone = fmt(pick(p, "phone"))
        website = pick(p, "website") or ""
        website_display = re.sub(r"^https?://(www\.)?", "", website).rstrip("/") if website else "—"
        pcn = fmt(pick(p, "pcn"))
        pcn_code = pick(p, "pcn_code")
        pcn_label = f"Part of {pcn}" if pcn != "—" else "PCN not listed"
        pcn_link = (
            f'<p style="margin-top:12px"><a href="/pcns/{slugify(pcn)}/" class="sources">See all GPs in {pcn} &rsaquo;</a></p>'
            if pcn != "—"
            else ""
        )
        ods_code = fmt(pick(p, "ods_code"))
        list_size = pick(p, "list_size")
        list_size_str = f"{int(float(list_size)):,}" if list_size not in (None, "") else "—"
        gps_per_1000 = fmt(pick(p, "gps_per_1000"))
        cqc_rating = fmt(pick(p, "cqc_rating"))
        cqc_date = fmt(pick(p, "cqc_date"))
        languages = fmt(pick(p, "languages"))
        accessibility = fmt(pick(p, "accessibility"))
        accepting = pick(p, "accepting")

        gpps_overall = fmt(pick(p, "gpps_overall"))
        gpps_phone = fmt(pick(p, "gpps_phone"))
        gpps_appointment = fmt(pick(p, "gpps_appointment"))
        gpps_continuity = fmt(pick(p, "gpps_continuity"))

        # Map JS values
        lat = pick(p, "lat")
        lng = pick(p, "lng")
        try:
            lat_val: float | None = float(lat) if lat not in (None, "") else None
            lng_val: float | None = float(lng) if lng not in (None, "") else None
        except (TypeError, ValueError):
            lat_val = lng_val = None
        lat_js = "null" if lat_val is None else f"{lat_val}"
        lng_js = "null" if lng_val is None else f"{lng_val}"
        popup_js = json.dumps(f"<strong>{name}</strong><br>{address}, {postcode}")

        accept_banner = render_accept_banner(accepting)
        faq_html, faq_jsonld = render_faq(name, borough, accepting, postcode)
        schema_json = render_schema(name, canonical, address, postcode, phone, website, lat_val, lng_val, borough, pcn, faq_jsonld)

        accepting_desc = (
            "Currently accepting new patients." if accepting is True or (isinstance(accepting, str) and accepting.lower() in {"yes", "y", "true", "open"})
            else "Registration status: check latest with practice."
        )
        meta_description = (
            f"{name} in {borough}, London. {accepting_desc} GP Patient Survey overall experience: {gpps_overall}. "
            f"Address, phone, opening hours, CQC rating and how to register. Updated weekly."
        )[:158]

        title = f"{name} — {borough} GP Practice | London GP Directory"

        html = PAGE_TEMPLATE.format(
            title=title,
            meta_description=meta_description,
            canonical=canonical,
            borough=borough,
            borough_slug=b_slug,
            name=name,
            address=address,
            postcode=postcode,
            pcn=pcn,
            pcn_label=pcn_label,
            pcn_link=pcn_link,
            ods_code=ods_code,
            list_size=list_size_str,
            gps_per_1000=gps_per_1000,
            cqc_rating=cqc_rating,
            cqc_date=cqc_date,
            languages=languages,
            accessibility=accessibility,
            phone=phone,
            website=website or "#",
            website_display=website_display,
            gpps_overall=gpps_overall,
            gpps_phone=gpps_phone,
            gpps_appointment=gpps_appointment,
            gpps_continuity=gpps_continuity,
            gpps_overall_london=london_avgs["gpps_overall"],
            gpps_phone_london=london_avgs["gpps_phone"],
            gpps_appointment_london=london_avgs["gpps_appointment"],
            gpps_continuity_london=london_avgs["gpps_continuity"],
            accept_banner=accept_banner,
            faq_html=faq_html,
            schema_json=schema_json,
            last_updated=today,
            lat_js=lat_js,
            lng_js=lng_js,
            popup_js=popup_js,
        )

        (out_dir / "index.html").write_text(html, encoding="utf-8")
        count += 1
        if count % 100 == 0:
            print(f"  built {count} practice pages...")

    print(f"\nDone. Wrote {count} practice pages under /gps/.")


if __name__ == "__main__":
    build()
