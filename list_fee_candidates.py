#!/usr/bin/env python3
"""
list_fee_candidates.py — prioritised worklist for private_fees.json.

Prints private-GP clinics that have a website but no fee entry yet,
biggest/most prominent first (rated clinics first, then by borough
prominence), so ten minutes of manual price-list checking covers the
clinics users are most likely to see.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent
clinics = json.loads((ROOT / "private_clinics.json").read_text())
fees_file = ROOT / "private_fees.json"
fees = json.loads(fees_file.read_text()) if fees_file.exists() else {}

RATING_ORDER = {"Outstanding": 0, "Good": 1, "": 2, "Requires improvement": 3, "Inadequate": 4}
CENTRAL = {"Westminster", "Camden", "City of London", "Kensington and Chelsea", "Islington", "Southwark"}

cands = [c for c in clinics
         if "private-gp" in c.get("specialties", [])
         and c.get("website")
         and c["cqc_id"] not in fees]
cands.sort(key=lambda c: (RATING_ORDER.get(c.get("cqc_rating",""), 2),
                          0 if c.get("localAuthority") in CENTRAL else 1,
                          c["name"].lower()))

print(f"{len(cands)} clinics without fee data. Top 40 to research:\n")
for c in cands[:40]:
    print(f'  "{c["cqc_id"]}": {{"from_gbp": 0, "note": "", '
          f'"source": "{c["website"]}", "checked": ""}},'
          f'   # {c["name"]} — {c.get("localAuthority","?")} ({c.get("cqc_rating") or "unrated"})')
