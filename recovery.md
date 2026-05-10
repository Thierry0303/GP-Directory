# Recovery + permanent fix — gps.json

## What went wrong
1. NHS Digital blocks GitHub Actions egress IPs at their CDN. Every "fetch
   the master GP list" endpoint we tried (ePraccur ZIP, FHIR list/search,
   ORD JSON) returned 403 or 406.
2. One of those failed runs wrote `[]` into gps.json before we'd added a
   safety guard. Result: gps.json is currently empty / broken.

## Permanent fix
Stop trying to rebuild the master list from CI. Do it once on your laptop
(home internet — not blocked), commit the result, leave it alone.

CQC rating enrichment + the weekly NHS refresh keep working in CI as
before — only the *initial master list* needs the local rebuild.

---

## Step 1 — disable the broken workflow (so it can't fire again)

In your repo, **delete** `.github/workflows/rebuild-gps-json.yml`. If you
already added `expand-gps-via-cqc.yml`, delete that too. Commit + push.

---

## Step 2 — restore gps.json from git history

On GitHub web UI:

1. Go to <https://github.com/Thierry0303/GP-Directory/commits/main/gps.json>
2. The most recent commits will be the bad "chore: rebuild gps.json from
   ePraccur" runs. Scroll down past those.
3. Click on the LAST GOOD commit (older, before the bad runs — should be
   the "Rename gps (2).json to gps.json" commit from last month, or any
   commit before that).
4. Click on `gps.json` in that snapshot.
5. Click the "Raw" button.
6. Save (Ctrl/Cmd-S) the file as `gps.json` to your desktop.
7. Back in your repo on GitHub, click `gps.json` → pencil-edit icon →
   delete all the contents → paste the raw file contents → "Commit
   changes". (Or just upload-replace via "Add file → Upload files".)

After this commit, `gps.json` should be ~250+ KB / 10,000+ lines / ~830
records. Verify by clicking on it: the file should look like a JSON array
of practice records, NOT `[]`.

---

## Step 3 — rebuild on your laptop (fixes the Twickenham/outer-London gap)

This runs locally, takes 30 seconds, and is the only step that needs
your home internet.

1. **Download ePraccur in your browser.** Open this URL (right-click →
   "Save Link As" if your browser tries to display it):
   <https://files.digital.nhs.uk/assets/ods/current/epraccur.zip>

   Save `epraccur.zip` somewhere convenient (Desktop is fine).

2. **Save `build_gps_locally.py`** (provided alongside this README) in
   the same folder as `epraccur.zip`.

3. **Open a terminal** in that folder. (Mac: open Terminal, `cd` to the
   folder. Windows: open PowerShell, `cd` to the folder.)

4. **First time, get a copy of your current good gps.json** (either from
   the GitHub web UI raw view, or from your local clone of the repo).
   Save it as `old_gps.json` in the same folder. This step is optional
   but preserves your existing CQC ratings + GPPS scores.

5. **Run the rebuild:**

   ```
   python3 build_gps_locally.py epraccur.zip --merge old_gps.json
   ```

   You should see something like:

   ```
   Loaded 830 records from old_gps.json for merge.
   Reading epraccur.zip…

   Read 8523 rows, 6845 active practices nationally.
   Wrote gps.json: 1247 London GPs, 412 KB.

   Coverage by postcode area:
     N    142
     E    138
     SE   127
     SW   118
     ...
     TW    78  <-- outer London
     KT    54  <-- outer London
     HA    52  <-- outer London
     ...
   ✅ Twickenham/Richmond (TW): 78 practices.
   ```

   If you see "✅ Twickenham/Richmond" with a non-zero count, the master
   list is fixed.

6. **Commit + push the new gps.json:**

   - GitHub web UI route: in your repo, open `gps.json` → pencil-edit →
     paste the new contents over the old → commit.
   - CLI route: copy `gps.json` over your repo's gps.json, `git add
     gps.json`, `git commit -m "fix: rebuild gps.json with full London
     coverage from ePraccur"`, `git push`.

---

## Step 4 — let the weekly refresh re-enrich CQC ratings

Your existing `weekly-nhs-refresh.yml` workflow runs `refresh_nhs_data.py`
which uses the FHIR identifier-lookup endpoint (works fine from Actions —
this is the pattern that's been working all along). It will pick up the
new master list automatically.

You can also kick it off manually right now: Actions → Weekly NHS Data
Refresh → Run workflow.

After it completes, the new TW/KT/HA/UB/etc. practices will be
searchable on londongp.directory.

---

## Step 5 — when you want to refresh in 6+ months

GP practice openings/closures are slow (~50 changes per year nationally).
Every 6-12 months, repeat steps 3-4: re-download epraccur.zip locally,
re-run `build_gps_locally.py`, commit. That's the entire ongoing
maintenance.
