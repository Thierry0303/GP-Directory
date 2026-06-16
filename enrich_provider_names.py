#!/usr/bin/env python3
"""
Enrich cqc_london_cache with provider names + ownership type.

Why: CQC's /locations/{id} doesn't return providerName, so we can't
reliably classify a location as NHS-vs-private. Fetching /providers/{id}
once per unique provider fills this gap and lets gen_nhs_specialties.py
identify NHS Trusts confidently.

Cost: one-time ~30 min run. Subsequent runs are skip-if-already-known
(fast).

Output: updates cqc_london_cache.json.gz in place. Adds two fields per
location:
  - providerName  ("Imperial College Healthcare NHS Trust" etc.)
  - ownershipType ("NHS" / "Independent" / "Charity" etc.)
"""
import gzip, json, os, ssl, sys, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parent
CACHE = ROOT / "cqc_london_cache.json.gz"
META  = ROOT / "cqc_london_cache.meta.json"

CQC_BASE = "https://api.service.cqc.org.uk/public/v1"
WORKERS = 5

# Optional SSL bypass for corporate networks
SSL_CTX = ssl.create_default_context()
if os.environ.get("CQC_INSECURE_SSL"):
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

def cqc_get(path):
    key = os.environ["CQC_KEY"]
    req = urllib.request.Request(f"{CQC_BASE}{path}", headers={
        "Ocp-Apim-Subscription-Key": key,
        "Accept": "application/json",
        "User-Agent": "londongp.directory/provider-enrich/1.0",
    })
    for attempt in range(5):
        try:
            with urllib.request.urlopen(req, timeout=30, context=SSL_CTX) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < 4:
                wait = min(10 * (2 ** attempt), 240)
                sys.stderr.write(f"    [{e.code} sleep {wait}s]\n"); sys.stderr.flush()
                time.sleep(wait); continue
            if e.code == 404: return None
            raise
        except Exception:
            if attempt < 4: time.sleep(3); continue
            raise
    return None

def fetch_provider(pid):
    """Return (providerName, ownershipType) for one provider."""
    d = cqc_get(f"/providers/{pid}")
    if not d: return ("", "")
    name = (d.get("name") or "").strip()
    ownership = (d.get("ownershipType") or "").strip()
    return (name, ownership)

def main():
    if not os.environ.get("CQC_KEY"):
        sys.exit("Need CQC_KEY env var.")
    if not CACHE.exists():
        sys.exit(f"{CACHE} not found. Run build_cqc_london_cache.py first.")

    with gzip.open(CACHE, "rt") as f:
        cache = json.load(f)
    print(f"Loaded {len(cache):,} locations from cache.")

    # Collect unique providerIds we don't already have names for
    todo = set()
    already = Counter()
    for rec in cache.values():
        pid = (rec.get("providerId") or "").strip()
        if not pid: continue
        # Skip if we already have a provider name (rerun-safe)
        if rec.get("providerName"):
            already["already-known"] += 1
            continue
        todo.add(pid)
    print(f"  Unique providers to fetch: {len(todo):,}")
    print(f"  Already enriched: {already['already-known']:,}\n")
    if not todo:
        print("Nothing to do. Cache is fully enriched.")
        return

    print(f"Fetching provider details ({WORKERS} workers, ~{len(todo)//(WORKERS*4)} min estimated)...")
    provider_cache = {}
    done = 0
    start = time.time()
    def worker(pid):
        try: return (pid, fetch_provider(pid))
        except Exception: return (pid, ("", ""))
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(worker, pid): pid for pid in todo}
        for fut in as_completed(futures):
            pid, (name, ownership) = fut.result()
            done += 1
            provider_cache[pid] = (name, ownership)
            if done % 100 == 0:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                eta = (len(todo) - done) / rate if rate > 0 else 0
                print(f"  {done:5d}/{len(todo)} rate={rate:.1f}/s eta={eta/60:.0f}min")
            # Checkpoint every 500
            if done % 500 == 0:
                apply_and_save(cache, provider_cache)
                print(f"    [checkpoint saved at {done}]")

    apply_and_save(cache, provider_cache)
    print(f"\nDone. {done} providers fetched in {(time.time()-start)/60:.1f} min.")
    print_ownership_distribution(cache)

def apply_and_save(cache, provider_cache):
    """Apply newly-fetched provider info to every location, then save."""
    for rec in cache.values():
        pid = (rec.get("providerId") or "").strip()
        if pid in provider_cache:
            name, ownership = provider_cache[pid]
            if name and not rec.get("providerName"):
                rec["providerName"] = name
            if ownership and not rec.get("ownershipType"):
                rec["ownershipType"] = ownership
    tmp = CACHE.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(cache, f, separators=(",", ":"))
    tmp.replace(CACHE)

def print_ownership_distribution(cache):
    print("\nOwnership type distribution across all London locations:")
    own = Counter()
    for rec in cache.values():
        own[rec.get("ownershipType") or "(unknown)"] += 1
    for k, n in own.most_common():
        print(f"  {k:30s} {n:5d}")

if __name__ == "__main__":
    main()
