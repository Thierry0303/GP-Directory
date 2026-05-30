# Automated ODS Data Pipeline for GP-Directory

## Overview

This pipeline automates GP practice data freshness by:

1. **`fetch_ods_automated.py`** — Fetches ODS base data (ODS codes, names, postcodes) from NHS Spine API
2. **`refresh_nhs_data.py`** (existing) — Enriches with live NHS FHIR, GPPS scores, CQC ratings
3. **GitHub Actions** — Runs weekly, commits refreshed data to repo
4. **Vercel** — Auto-deploys on commit

## Installation

### 1. Add Scripts to Your Repo

```bash
cd GP-Directory
cp fetch_ods_automated.py .
# keep your existing refresh_nhs_data.py unchanged
```

### 2. Add GitHub Actions Workflow

```bash
cp ods_refresh_workflow.yml .github/workflows/automated-ods-refresh.yml
```

### 3. Test Locally

```bash
# Generate base gps.json from ODS
python fetch_ods_automated.py

# Enrich with NHS data
python refresh_nhs_data.py

# Verify
ls -la gps.json index.html
```

### 4. Push to GitHub

```bash
git add fetch_ods_automated.py .github/workflows/automated-ods-refresh.yml
git commit -m "Add automated ODS refresh pipeline"
git push origin main
```

### 5. Trigger Workflow (Optional)

Go to: **Actions → Automated ODS + NHS Data Refresh → Run workflow**

Or it will run automatically every Monday at 2 AM UTC.

## Data Flow

```
NHS Spine API
     ↓
fetch_ods_automated.py → generates gps.json (ODS codes, names, postcodes)
     ↓
refresh_nhs_data.py → enriches with:
     ├─ FHIR data (current address, phone)
     ├─ GPPS scores (Patients' feedback)
     └─ CQC ratings
     ↓
index.template.html → generates index.html with static site
     ↓
Vercel → deployed
```

## How It Works

### Step 1: `fetch_ods_automated.py`

**Purpose:** Fetch base ODS data (authoritative source of active GPs)

**Process:**
- Queries NHS Spine FHIR API for active GP practices
- Filters to London using postcode districts (E, N, W, SE, SW, NW, etc.)
- Verifies London location via postcodes.io
- Extracts borough/ward from postcodes.io
- Outputs **base `gps.json`** with:
  ```json
  {
    "ods_code": "A81001",
    "name": "Abbey Medical Centre",
    "postcode": "SW1A 1AA",
    "address": "...",
    "phone": "...",
    "borough": "Westminster",
    "ward": "...",
    "gpps_overall_pct": null,  // filled in next step
    "cqc_rating": null         // filled in next step
  }
  ```

**Why separate this from refresh_nhs_data.py?**
- `fetch_ods_automated.py` is the **source of truth** — it defines which practices exist
- Your existing `refresh_nhs_data.py` is the **enrichment** — it adds live scores, ratings, contact details
- Separating them makes the pipeline clearer and easier to debug

### Step 2: `refresh_nhs_data.py` (your existing script)

Your script already does this! It:
1. Reads the base `gps.json`
2. Validates genuine GPs (filters out walk-in centres, special schemes, etc.)
3. Fetches live data from NHS Spine FHIR API by ODS code
4. Merges with GPPS/CQC data (if available)
5. Outputs final `index.html`

**No changes needed to this file.**

### Step 3: GitHub Actions Workflow

Runs on schedule (weekly, customizable):

```yaml
on:
  schedule:
    - cron: '0 2 * * 1'  # Monday 2 AM UTC
  workflow_dispatch:     # Manual trigger anytime
```

**What it does:**
1. Checks out your repo
2. Runs `fetch_ods_automated.py` → generates fresh `gps.json`
3. Runs `refresh_nhs_data.py` → enriches with NHS data
4. Commits back if there are changes
5. Vercel auto-deploys on commit

## Customization

### Change the refresh schedule

Edit `.github/workflows/automated-ods-refresh.yml`:

```yaml
on:
  schedule:
    - cron: '0 2 * * 1'  # Change this
```

Common cron patterns:
- `'0 2 * * 1'` = Weekly Monday 2 AM UTC
- `'0 2 1 * *'` = Monthly 1st at 2 AM UTC
- `'0 */6 * * *'` = Every 6 hours

### Filter to specific boroughs

Edit `fetch_ods_automated.py`, the `LONDON_POSTCODE_PREFIXES` and `london_boroughs` sets to narrow down.

### Add more data sources

After `refresh_nhs_data.py` runs, you could add another enrichment script:

```yaml
- name: Add custom enrichment
  run: python my_enrichment_script.py
```

## Troubleshooting

### "No practices found"

The Spine API might be temporarily unavailable. Check:
1. Visit https://directory.spineservices.nhs.uk (is it accessible?)
2. Run locally: `python fetch_ods_automated.py` to see detailed errors
3. If Spine API is down, the script falls back to a minimal seed (needs expansion)

**Temporary fix:** If Spine API is unreliable, download the ePraccur CSV from NHS Digital manually and use `fetch_ods_base.py` (slower but more reliable).

### "Low practice count" warning

If you see <50 practices, the NHS Spine API search might not be finding them. Debug:

```bash
# Test Spine API manually
curl "https://directory.spineservices.nhs.uk/STU3/Organization?type=GP&_format=json&_count=10"
```

If that returns empty, the Spine API might not support the `type=GP` filter. Let me know and we'll adjust the query.

### GitHub Actions job fails

Check the **Actions** tab → **Automated ODS + NHS Data Refresh** → latest run → logs

Common issues:
- Network timeout: Spine API or postcodes.io temporarily down
- Postcode quota: postcodes.io is rate-limited (free tier: 100 req/hr); adjust script to batch requests
- Missing dependencies: Check `pip install requests` runs successfully

### Data looks stale

If deployed data is from last week but you just ran refresh:
1. Check the auto-commit actually happened (look in repo commit history)
2. Check Vercel is set to auto-deploy on push to `main`
3. Check GitHub Actions logs to confirm job succeeded

## Advanced: Replace with ODS CSV

If you want **maximum reliability** (at cost of manual updates):

1. Download ePraccur CSV from https://www.datadictionary.nhs.uk/nhs_data_dictionary/nhs_data_items/organisation_code.html
2. Store it in your repo as `data/ePraccur.csv`
3. Use `fetch_ods_base.py` instead:

```bash
python fetch_ods_base.py  # reads data/ePraccur.csv
python refresh_nhs_data.py
```

Then in GitHub Actions, add a manual step to re-download the CSV periodically, or commit it to your repo and update quarterly.

## Questions?

- **How often should it refresh?** Weekly is a good balance (catches new GPs, avoids spam);  monthly is fine for cost.
- **Will the API cost money?** No. NHS Spine, postcodes.io (free tier), and FHIR are public.
- **Can I add other practitioner types?** Yes! Modify the Spine API query to include nurses, dentists, etc. Let me know the ODS types.
- **What's the data retention?** GitHub stores history; Vercel caches latest. You can set GitHub to auto-delete old workflow runs to save storage.

Let me know once you've integrated this and run the first refresh!
