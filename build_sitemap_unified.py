#!/usr/bin/env python3
"""
build_sitemap_unified.py — single source of truth for sitemap.xml.

Walks the generated site and emits every indexable page. Run this LAST in
the refresh workflow (after all build_*_pages.py scripts), replacing the
partial sitemaps that build_borough_pages.py / build_practice_pages.py
used to overwrite each other with.

Covered:
  /                      (homepage)
  /boroughs/             (borough index)
  /practice/<b>/         (borough hubs)
  /practice/<b>/<slug>/  (NHS practices + private clinics)
  /private/              (private index + specialty hubs)
  /nhs-services/         (NHS service hubs)
  /guides/**             (editorial guides)
  about / methodology / sources / corrections
"""
import re
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SITE = "https://londongp.directory"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

def url(loc, priority, changefreq="weekly"):
    return (f'  <url><loc>{loc}</loc><lastmod>{TODAY}</lastmod>'
            f'<changefreq>{changefreq}</changefreq>'
            f'<priority>{priority}</priority></url>')

def walk_index_pages(subdir):
    """Yield site-relative URLs for every index.html under subdir."""
    base = ROOT / subdir
    if not base.exists():
        return
    for f in sorted(base.rglob("index.html")):
        rel = f.parent.relative_to(ROOT).as_posix()
        yield f"{SITE}/{rel}/"

def main():
    entries = [url(f"{SITE}/", "1.0", "daily")]

    # Static top-level pages
    for page in ("about.html", "methodology.html", "sources.html",
                 "corrections.html"):
        if (ROOT / page).exists():
            entries.append(url(f"{SITE}/{page}", "0.4", "monthly"))

    # Section indexes + hubs + leaf pages
    seen = set()
    for subdir, prio in (("boroughs", "0.8"), ("private", "0.8"),
                         ("dentists", "0.7"),
                         ("nhs-services", "0.7"), ("guides", "0.6")):
        for loc in walk_index_pages(subdir):
            if loc not in seen:
                seen.add(loc)
                # hubs (1 path segment under the section) rank above leaves
                depth = loc[len(SITE):].strip("/").count("/")
                entries.append(url(loc, prio if depth <= 1 else "0.6"))

    # Practice pages: borough hubs 0.8, practice/clinic leaves 0.6
    for loc in walk_index_pages("practice"):
        if loc in seen:
            continue
        seen.add(loc)
        depth = loc[len(SITE):].strip("/").count("/")
        entries.append(url(loc, "0.8" if depth == 1 else "0.6"))

    xml = ('<?xml version="1.0" encoding="UTF-8"?>\n'
           '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
           + "\n".join(entries) + "\n</urlset>\n")
    (ROOT / "sitemap.xml").write_text(xml, encoding="utf-8")
    print(f"Wrote sitemap.xml with {len(entries)} URLs.")

if __name__ == "__main__":
    main()
