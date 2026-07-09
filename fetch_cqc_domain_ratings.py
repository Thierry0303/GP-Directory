#!/usr/bin/env python3
"""
fetch_cqc_domain_ratings.py
Fetch CQC's five key-question ratings (Safe / Effective / Caring /
Responsive / Well-led) plus last inspection date for every location in
merged.json, and cache them in cqc_domains.json:

    { "<locationId>": {"overall": "Good", "safe": "Good", ...,
                       "inspected": "2024-03-12", "_fetched": "2026-07-09"} }

build_practice_pages.py renders these as a ratings grid when present.

Run in CI after merge_into_dataset.py and before build_practice_pages.py.
Needs CQC_KEY (same secret the cache builder uses); exits 0 without it.

To keep daily runs light, each run refreshes at most MAX_CALLS entries:
new locations first, then the stalest (>REFRESH_DAYS old). Within about
a week of first deployment the whole directory is covered.
"""
import gzip, json, os, re, sys, time, urllib.request, urllib.error
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MERGED = ROOT / "merged.json"
OUT = ROOT / "cqc_domains.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"
MAX_CALLS = 500        # per run
REFRESH_DAYS = 30      # re-check entries older than this
VALID = {"Outstanding", "Good", "Requires improvement", "Inadequate"}
LOC_RE = re.compile(r"/location/(1-\d+)")


def cqc_get(loc_id, key, retries=3):
    url = f"{CQC_BASE}/locations/{loc_id}"
    headers = {"Ocp-Apim-Subscription-Key": key,
               "User-Agent": "londongp.directory/1.0",
               "Accept": "application/json"}
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if e.code in (429, 503):
                time.sleep(2 * (attempt + 1))
                continue
            return None
        except Exception:
            time.sleep(1)
    return None


def parse_domains(detail):
    """Extract overall + key question ratings + inspection date."""
    if not detail:
        return None
    out = {}
    overall = (detail.get("currentRatings", {}) or {}).get("overall", {}) or {}
    if (overall.get("rating") or "") in VALID:
        out["overall"] = overall["rating"]
    for kq in (overall.get("keyQuestionRatings") or []):
        name = (kq.get("name") or "").strip().lower().replace("-", "").replace(" ", "")
        rating = (kq.get("rating") or "").strip()
        if rating in VALID and name in {"safe", "effective", "caring", "responsive", "wellled"}:
            out["wellLed" if name == "wellled" else name] = rating
    insp = ((detail.get("lastInspection") or {}).get("date")
            or overall.get("reportDate") or "")
    if insp:
        out["inspected"] = insp[:10]
    return out or None


def collect_location_ids():
    """Location ids from merged.json: private records carry the CQC id in
    'o'; NHS records carry it inside their CQC report URL ('cu')."""
    records = json.loads(MERGED.read_text())
    ids = []
    for r in records:
        cand = ""
        if r.get("type") == "Private" and str(r.get("o", "")).startswith("1-"):
            cand = r["o"]
        else:
            m = LOC_RE.search(r.get("cu") or "")
            if m:
                cand = m.group(1)
        if cand:
            ids.append(cand)
    return list(dict.fromkeys(ids))  # dedupe, keep order


def main():
    key = os.environ.get("CQC_KEY")
    if not key:
        print("CQC_KEY not set — skipping domain-ratings refresh "
              "(existing cqc_domains.json, if any, is kept).")
        return

    if not MERGED.exists():
        sys.exit("merged.json not found — run the pipeline first.")

    cache = json.loads(OUT.read_text()) if OUT.exists() else {}
    ids = collect_location_ids()
    today = date.today().isoformat()

    def staleness(loc_id):
        entry = cache.get(loc_id)
        if not entry:
            return "0000-00-00"          # never fetched → highest priority
        return entry.get("_fetched", "0000-00-00")

    cutoff = (datetime.now().date().toordinal() - REFRESH_DAYS)
    todo = sorted(ids, key=staleness)
    todo = [i for i in todo
            if i not in cache
            or date.fromisoformat(cache[i].get("_fetched", "2000-01-01")).toordinal() < cutoff]
    todo = todo[:MAX_CALLS]

    print(f"{len(ids)} locations total; {len(todo)} to fetch this run "
          f"({len(cache)} already cached).")

    fetched = with_ratings = 0
    for i, loc_id in enumerate(todo, 1):
        detail = cqc_get(loc_id, key)
        domains = parse_domains(detail)
        entry = domains or {}
        entry["_fetched"] = today
        cache[loc_id] = entry
        fetched += 1
        if domains and any(k != "_fetched" for k in domains):
            with_ratings += 1
        if i % 100 == 0:
            print(f"  {i}/{len(todo)}…")
            OUT.write_text(json.dumps(cache, indent=0, sort_keys=True))
        time.sleep(0.35)   # stay well under CQC rate limits

    OUT.write_text(json.dumps(cache, indent=0, sort_keys=True))
    print(f"Done: fetched {fetched}, {with_ratings} had ratings. "
          f"Cache now {len(cache)} entries → {OUT.name}")


if __name__ == "__main__":
    main()
