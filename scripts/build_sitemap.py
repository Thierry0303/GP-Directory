#!/usr/bin/env python3
"""
build_sitemap.py
----------------
Walks the static site and emits a sitemap.xml at the repo root.

Includes:
  - Top-level pages (/, /about.html, /methodology.html, /sources.html)
  - Every borough hub  (/gps/{slug}/)
  - Every PCN hub      (/pcns/{slug}/)
  - Every practice page (/gps/{borough}/{slug}/)
  - All editorial guides (/guides/*/)
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SITE_URL = "https://londongp.directory"
SITEMAP = REPO_ROOT / "sitemap.xml"


def url_entry(loc: str, lastmod: str, priority: float = 0.6, changefreq: str = "weekly") -> str:
    return (
        f"  <url>\n"
        f"    <loc>{SITE_URL}{loc}</loc>\n"
        f"    <lastmod>{lastmod}</lastmod>\n"
        f"    <changefreq>{changefreq}</changefreq>\n"
        f"    <priority>{priority:.1f}</priority>\n"
        f"  </url>"
    )


def build():
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entries: list[str] = []

    # Top-level
    for path, prio, freq in [
        ("/", 1.0, "weekly"),
        ("/gps/", 0.9, "weekly"),
        ("/pcns/", 0.7, "monthly"),
        ("/about.html", 0.5, "monthly"),
        ("/methodology.html", 0.5, "monthly"),
        ("/sources.html", 0.5, "monthly"),
    ]:
        entries.append(url_entry(path, today, prio, freq))

    # Borough hubs
    gps_dir = REPO_ROOT / "gps"
    if gps_dir.exists():
        for sub in sorted(gps_dir.iterdir()):
            if sub.is_dir() and (sub / "index.html").exists():
                entries.append(url_entry(f"/gps/{sub.name}/", today, 0.8, "weekly"))
                # Practice pages
                for practice in sorted(sub.iterdir()):
                    if practice.is_dir() and (practice / "index.html").exists():
                        entries.append(url_entry(f"/gps/{sub.name}/{practice.name}/", today, 0.7, "weekly"))

    # PCN hubs
    pcn_dir = REPO_ROOT / "pcns"
    if pcn_dir.exists():
        for sub in sorted(pcn_dir.iterdir()):
            if sub.is_dir() and (sub / "index.html").exists():
                entries.append(url_entry(f"/pcns/{sub.name}/", today, 0.6, "monthly"))

    # Editorial guides
    guides_dir = REPO_ROOT / "guides"
    if guides_dir.exists():
        for sub in sorted(guides_dir.iterdir()):
            if sub.is_dir() and (sub / "index.html").exists():
                entries.append(url_entry(f"/guides/{sub.name}/", today, 0.7, "monthly"))

    xml = (
        '<?xml version="1.0" encoding="UTF-8"?>\n'
        '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">\n'
        + "\n".join(entries)
        + "\n</urlset>\n"
    )
    SITEMAP.write_text(xml, encoding="utf-8")
    print(f"Wrote {SITEMAP} with {len(entries)} URLs.")


if __name__ == "__main__":
    build()
