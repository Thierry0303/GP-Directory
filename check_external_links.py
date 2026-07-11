#!/usr/bin/env python3
"""
check_external_links.py — verify every external website URL the site links
to, and quarantine the dead ones.

Why: website URLs come from CQC's register, which is full of stale domains —
NHS trusts merge/rename constantly (candi.nhs.uk, wlmht.nhs.uk, even a
years-old typo bartsandthelondion.nhs.uk). Linking users to dead sites is
worse than linking only to the CQC profile.

How it integrates:
  - Collects unique website URLs from nhs_specialties.json and
    private_clinics.json.
  - Checks each (HEAD, falling back to GET), following redirects.
    Results cached in link_check_cache.json; alive URLs re-checked
    monthly, dead ones re-checked weekly (so recovered sites come back).
  - A URL is only declared dead after failing on TWO different runs
    (avoids transient outages nuking good links).
  - Writes dead_links.json — gen_nhs_specialties.py and
    gen_private_clinics.py blank any website found in it, so the card
    builders automatically fall back to the CQC profile link.

Run in CI BEFORE the gen_* scripts. No secrets needed.
"""
import json, ssl, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CACHE_FILE = ROOT / "link_check_cache.json"
DEAD_FILE = ROOT / "dead_links.json"

SOURCES = ["nhs_specialties.json", "private_clinics.json"]
RECHECK_ALIVE_DAYS = 30
RECHECK_DEAD_DAYS = 7
MAX_WORKERS = 12
TIMEOUT = 12

CTX = ssl.create_default_context()
CTX.check_hostname = False          # stale certs on old NHS domains are
CTX.verify_mode = ssl.CERT_NONE     # common; we test reachability, not TLS hygiene

HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; londongp.directory link checker)",
           "Accept": "text/html,*/*"}


def collect_urls():
    urls = set()
    for fname in SOURCES:
        f = ROOT / fname
        if not f.exists():
            continue
        for r in json.loads(f.read_text()):
            w = (r.get("website") or "").strip()
            if w.startswith(("http://", "https://")):
                urls.add(w)
    return sorted(urls)


def probe(url):
    """Return (ok, status_or_error)."""
    for method in ("HEAD", "GET"):
        try:
            req = urllib.request.Request(url, headers=HEADERS, method=method)
            with urllib.request.urlopen(req, timeout=TIMEOUT, context=CTX) as r:
                return True, r.status
        except urllib.error.HTTPError as e:
            if method == "HEAD" and e.code in (403, 405, 400, 501):
                continue            # some servers reject HEAD; try GET
            # 403 on GET often means bot-blocking, not a dead site
            return (e.code == 403), e.code
        except Exception as e:
            if method == "HEAD":
                continue
            return False, type(e).__name__
    return False, "unreachable"


def main():
    urls = collect_urls()
    cache = json.loads(CACHE_FILE.read_text()) if CACHE_FILE.exists() else {}
    today = date.today()

    def needs_check(u):
        e = cache.get(u)
        if not e:
            return True
        age = (today - date.fromisoformat(e["checked"])).days
        return age >= (RECHECK_ALIVE_DAYS if e["ok"] else RECHECK_DEAD_DAYS)

    todo = [u for u in urls if needs_check(u)]
    print(f"{len(urls)} unique URLs; {len(todo)} to check this run.")

    checked = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(probe, u): u for u in todo}
        for fut in as_completed(futures):
            u = futures[fut]
            ok, status = fut.result()
            prev = cache.get(u, {})
            # two-strike rule: only flip alive->dead after 2 consecutive fails
            strikes = 0 if ok else prev.get("strikes", 0) + 1
            cache[u] = {"ok": ok or strikes < 2,
                        "really_ok": ok,
                        "strikes": strikes,
                        "status": str(status),
                        "checked": today.isoformat()}
            checked += 1
            if checked % 100 == 0:
                print(f"  {checked}/{len(todo)}…")
                CACHE_FILE.write_text(json.dumps(cache, indent=0, sort_keys=True))

    # prune cache entries for URLs no longer referenced
    cache = {u: e for u, e in cache.items() if u in set(urls)}
    CACHE_FILE.write_text(json.dumps(cache, indent=0, sort_keys=True))

    dead = sorted(u for u, e in cache.items() if not e["ok"])
    DEAD_FILE.write_text(json.dumps(dead, indent=1))
    alive = sum(1 for e in cache.values() if e["ok"])
    print(f"Done: {alive} alive, {len(dead)} dead → {DEAD_FILE.name}")
    if dead:
        print("Dead links (first 20):")
        for u in dead[:20]:
            print(f"  {cache[u]['status']:>12}  {u}")


if __name__ == "__main__":
    main()
