#!/usr/bin/env python3
"""
Build the Pharmacies section from pharmacies.json (produced by
fetch_pharmacies.py): an index page listing boroughs, and one card-grid page
per borough. Matches the site's navy header, canonical nav and card styling.

    /pharmacies/                      -> index (boroughs + counts)
    /pharmacies/<borough-slug>/       -> pharmacy cards for that borough
"""
import json, html, re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
PHARM_JSON = ROOT / "pharmacies.json"
OUT_DIR = ROOT / "pharmacies"
BASE_URL = "https://londongp.directory"
SITE_NAME = "London GP Directory"

_WEEK = [("Monday", "Mon"), ("Tuesday", "Tue"), ("Wednesday", "Wed"),
         ("Thursday", "Thu"), ("Friday", "Fri"), ("Saturday", "Sat"),
         ("Sunday", "Sun")]


def slugify(s):
    s = (s or "").lower().replace("&", "and")
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def hours_summary(oh):
    if not isinstance(oh, dict) or not oh:
        return ""
    seq = [(i, ab, oh.get(full)) for i, (full, ab) in enumerate(_WEEK)
           if full in oh and oh.get(full) and oh[full].lower() != "closed"]
    groups = []
    for i, ab, val in seq:
        if groups and groups[-1]["val"] == val and groups[-1]["end_i"] == i - 1:
            groups[-1]["end"] = ab; groups[-1]["end_i"] = i
        else:
            groups.append({"start": ab, "end": ab, "end_i": i, "val": val})
    if not groups:
        return ""
    parts = [(g["start"] if g["start"] == g["end"] else f'{g["start"]}–{g["end"]}')
             + " " + g["val"] for g in groups[:2]]
    txt = ", ".join(parts) + (", …" if len(groups) > 2 else "")
    return f'<div class="card-hours">🕒 {html.escape(txt)}</div>'


CSS = (
    '*{box-sizing:border-box;margin:0;padding:0}\n'
    "body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f3;color:#1a1a1a;font-size:15px;line-height:1.5}\n"
    'a{text-decoration:none;color:inherit}\n'
    '.hdr{background:#003087;color:#fff}\n'
    '.hdr-top{padding:14px 24px;border-bottom:1px solid rgba(255,255,255,.08)}\n'
    '.hdr-in{max-width:1300px;margin:0 auto;display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}\n'
    '.logo h1{font-family:Georgia,serif;font-size:1.4rem;font-weight:700;letter-spacing:-0.02em;line-height:1.1}\n'
    '.logo h1 em{color:#B5D4F4;font-style:italic;font-weight:400}\n'
    '.main-nav{background:rgba(0,0,0,.18);border-bottom:4px solid #0072CE}\n'
    '.main-nav-in{max-width:1300px;margin:0 auto;padding:10px 24px;display:flex;flex-wrap:wrap;gap:8px 20px;font-size:.88rem;align-items:center}\n'
    '.main-nav a{color:rgba(255,255,255,.78);font-weight:500;transition:color .12s;padding:4px 0;white-space:nowrap}\n'
    '.main-nav a:hover,.main-nav a.active{color:#fff}\n'
    '.main-nav a.active{border-bottom:2px solid #B5D4F4}\n'
    '.hdr-page-title{background:#003087;padding:18px 24px;border-bottom:1px solid rgba(255,255,255,.08)}\n'
    '.hdr-page-title-in{max-width:1300px;margin:0 auto;color:#fff}\n'
    '.crumbs{font-size:12px;opacity:.65;margin-bottom:6px}\n'
    '.crumbs a{color:#B5D4F4}\n'
    '.hdr-page-title-in h1{font-family:Georgia,serif;font-size:1.6rem;font-weight:700;line-height:1.15;margin-bottom:8px}\n'
    '.hdr-page-title-in h1 em{color:#B5D4F4;font-style:italic;font-weight:400}\n'
    '.hdr-sub{font-size:.9rem;opacity:.8;max-width:680px;line-height:1.45}\n'
    '.wrap{max-width:1300px;margin:0 auto;padding:24px}\n'
    '.results-bar{font-size:13px;color:#888;margin-bottom:14px}\n'
    '.results-bar strong{color:#222}\n'
    '#grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(300px,1fr));gap:13px}\n'
    '.card{background:#fff;border:1px solid #ddd;border-radius:12px;padding:15px 16px;display:flex;flex-direction:column}\n'
    '.card-name{font-family:Georgia,serif;font-size:14px;font-weight:700;line-height:1.3;color:#003087;margin-bottom:6px}\n'
    '.card-addr{font-size:11.5px;color:#888;margin-bottom:10px;line-height:1.4}\n'
    '.card-hours{font-size:11.5px;color:#0F6E56;margin:-4px 0 10px}\n'
    '.card-foot{display:flex;align-items:center;justify-content:space-between;border-top:1px solid #f0f0ee;padding-top:10px;gap:8px;margin-top:auto}\n'
    '.card-phone{font-size:11.5px;color:#444;font-weight:500}\n'
    '.actions{display:flex;gap:6px;flex-wrap:wrap;justify-content:flex-end}\n'
    '.pill{font-size:10.5px;padding:4px 9px;border-radius:6px;font-weight:600;white-space:nowrap}\n'
    '.pill-web{background:#EDF4FC;color:#0072CE}\n'
    '.pill-dir{background:#F0F0EE;color:#555}\n'
    '.bgrid{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:12px}\n'
    '.bcard{background:#fff;border:1px solid #ddd;border-radius:12px;padding:16px 18px;transition:border-color .15s,box-shadow .15s}\n'
    '.bcard:hover{border-color:#003087;box-shadow:0 2px 10px rgba(0,48,135,.12)}\n'
    '.bcard strong{color:#003087;font-family:Georgia,serif;font-size:1.05rem;display:block}\n'
    '.bcard span{font-size:12px;color:#888}\n'
    'footer{max-width:1300px;margin:0 auto;padding:24px;font-size:12px;color:#999}\n'
    'footer a{color:#0072CE}\n'
    '@media(max-width:600px){.hdr-top{padding:10px 16px}.main-nav-in{padding:8px 16px;gap:6px 14px;font-size:.82rem}.wrap{padding:16px}#grid{grid-template-columns:1fr}}\n'
)

NAV = (
    '<a href="/">Search</a><a href="/boroughs/">Boroughs</a><a href="/nhs-services/">NHS Services</a>'
    '<a href="/private/">Private Clinics</a><a href="/dentists/">Dentists</a>'
    '<a href="/pharmacies/" class="active">Pharmacies</a><a href="/guides/">Guides</a>'
    '<a href="/methodology.html">Methodology</a><a href="/sources.html">Sources</a>'
    '<a class="support-btn" href="https://ko-fi.com/thierry81" target="_blank" rel="noopener" '
    'style="background:#FF5E5B;color:#fff;padding:6px 13px;border-radius:999px;font-weight:700;font-size:.8rem;white-space:nowrap">&#9749; Support</a>'
)


def head(title, desc, canonical, json_ld):
    return (
        '<!DOCTYPE html>\n<html lang="en">\n<head>\n'
        '<meta charset="UTF-8">\n<meta name="viewport" content="width=device-width, initial-scale=1.0">\n'
        f'<title>{html.escape(title)}</title>\n'
        f'<meta name="description" content="{html.escape(desc)}">\n'
        f'<link rel="canonical" href="{canonical}">\n'
        f'<script type="application/ld+json">{json_ld}</script>\n'
        f'<style>\n{CSS}</style>\n</head>\n<body>\n'
        '<header class="hdr">\n'
        '  <div class="hdr-top"><div class="hdr-in"><div class="logo"><h1>London GP <em>Directory</em></h1></div></div></div>\n'
        f'  <nav class="main-nav" aria-label="Main navigation"><div class="main-nav-in">{NAV}</div></nav>\n'
    )


def render_card(p):
    name = html.escape(p.get("n", ""))
    addr = html.escape(", ".join(b for b in (p.get("a", ""), p.get("p", "")) if b))
    ph = p.get("ph", "")
    web = p.get("web", "")
    phone_html = (f'<a class="card-phone" href="tel:{ph.replace(" ", "")}">{html.escape(ph)}</a>'
                  if ph else "<span></span>")
    acts = []
    if web:
        acts.append(f'<a class="pill pill-web" href="{html.escape(web)}" target="_blank" rel="noopener">Website</a>')
    dir_q = html.escape(f"{p.get('n','')} {p.get('p','')}".strip())
    acts.append(f'<a class="pill pill-dir" href="https://www.google.com/maps/search/?api=1&query={dir_q}" target="_blank" rel="noopener">Directions</a>')
    addr_html = f'<div class="card-addr">{addr}</div>' if addr else ""
    return (f'<div class="card"><div class="card-name">{name}</div>{addr_html}'
            f'{hours_summary(p.get("oh"))}'
            f'<div class="card-foot">{phone_html}<div class="actions">{"".join(acts)}</div></div></div>')


def render_borough_page(borough, plist, all_boroughs):
    slug = slugify(borough)
    plist = sorted(plist, key=lambda x: x.get("n", ""))
    cards = "\n".join(render_card(p) for p in plist)
    json_ld = json.dumps({
        "@context": "https://schema.org", "@type": "CollectionPage",
        "name": f"Pharmacies in {borough}",
        "url": f"{BASE_URL}/pharmacies/{slug}/",
        "description": f"NHS pharmacy directory for {borough}, London — addresses, opening hours and websites.",
        "isPartOf": {"@type": "WebSite", "name": SITE_NAME, "url": BASE_URL},
    }, separators=(",", ":"))
    with_hours = sum(1 for p in plist if p.get("oh"))
    return (
        head(f"Pharmacies in {borough} - {SITE_NAME}",
             f"Find {len(plist)} NHS pharmacies in {borough}, London — opening hours, "
             "websites and locations.",
             f"{BASE_URL}/pharmacies/{slug}/", json_ld)
        + '  <div class="hdr-page-title"><div class="hdr-page-title-in">'
        + f'<div class="crumbs"><a href="/">Home</a> &rsaquo; <a href="/pharmacies/">Pharmacies</a> &rsaquo; <strong>{html.escape(borough)}</strong></div>'
        + f'<h1>Pharmacies in <em>{html.escape(borough)}</em></h1>'
        + f'<p class="hdr-sub">{len(plist)} NHS-listed pharmacies in {html.escape(borough)}, '
          f'with opening hours where published ({with_hours} of {len(plist)}).</p>'
        + '</div></div>\n</header>\n'
        + f'<main class="wrap"><div class="results-bar">Showing <strong>{len(plist)}</strong> pharmacies in {html.escape(borough)}</div>'
        + f'<div id="grid">{cards}</div></main>\n'
        + '<footer>Source: NHS Directory of Healthcare Services (Service Search) &middot; '
        + f'updated {datetime.now(timezone.utc).strftime("%-d %B %Y")}<br>'
        + f'<a href="/">{SITE_NAME}</a> &middot; <a href="/pharmacies/">All London pharmacies</a></footer>\n'
        + '</body>\n</html>\n'
    )


def render_index(by_borough, total):
    boroughs = sorted(by_borough)
    json_ld = json.dumps({
        "@context": "https://schema.org", "@type": "CollectionPage",
        "name": "London Pharmacies", "url": f"{BASE_URL}/pharmacies/",
        "description": f"Directory of {total} NHS pharmacies across London, by borough.",
        "isPartOf": {"@type": "WebSite", "name": SITE_NAME, "url": BASE_URL},
    }, separators=(",", ":"))
    cards = "\n".join(
        f'<a class="bcard" href="/pharmacies/{slugify(b)}/"><strong>{html.escape(b)}</strong>'
        f'<span>{len(by_borough[b])} pharmacies</span></a>'
        for b in boroughs)
    return (
        head(f"London Pharmacies by Borough - {SITE_NAME}",
             f"Find NHS pharmacies across London — {total} pharmacies in "
             f"{len(boroughs)} boroughs, with opening hours and websites.",
             f"{BASE_URL}/pharmacies/", json_ld)
        + '  <div class="hdr-page-title"><div class="hdr-page-title-in">'
        + '<div class="crumbs"><a href="/">Home</a> &rsaquo; <strong>Pharmacies</strong></div>'
        + '<h1>London <em>Pharmacies</em></h1>'
        + f'<p class="hdr-sub">{total} NHS-listed pharmacies across {len(boroughs)} London boroughs — '
          'opening hours, websites and locations, straight from the NHS Directory of Healthcare Services.</p>'
        + '</div></div>\n</header>\n'
        + f'<main class="wrap"><div class="results-bar"><strong>{total}</strong> pharmacies · {len(boroughs)} boroughs</div>'
        + f'<div class="bgrid">{cards}</div></main>\n'
        + '<footer>Source: NHS Directory of Healthcare Services (Service Search) &middot; '
        + f'updated {datetime.now(timezone.utc).strftime("%-d %B %Y")}<br><a href="/">{SITE_NAME}</a></footer>\n'
        + '</body>\n</html>\n'
    )


def main():
    if not PHARM_JSON.exists():
        print(f"{PHARM_JSON.name} not found — run fetch_pharmacies.py first. Skipping.")
        return
    data = json.loads(PHARM_JSON.read_text(encoding="utf-8"))
    by_borough = defaultdict(list)
    for p in data:
        if p.get("ar"):
            by_borough[p["ar"]].append(p)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "index.html").write_text(render_index(by_borough, len(data)), encoding="utf-8")
    for borough, plist in by_borough.items():
        d = OUT_DIR / slugify(borough)
        d.mkdir(parents=True, exist_ok=True)
        (d / "index.html").write_text(
            render_borough_page(borough, plist, by_borough), encoding="utf-8")
    print(f"Wrote /pharmacies/ index + {len(by_borough)} borough pages "
          f"({len(data)} pharmacies).")


if __name__ == "__main__":
    main()
