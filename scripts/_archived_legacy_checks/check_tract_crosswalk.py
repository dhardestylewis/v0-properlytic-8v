"""
Compute tract-level protest probability aggregation directly from exp02 CSV.
Bypasses the broken parcel_ladder JOIN (different ID systems).
Uses the geo_parcel_poly ↔ geo_tract20_tx spatial crosswalk that was already
pre-computed in the parcel_ladder, but we need to map TCAD PIDs → geo accts first.

Strategy: Load the entire per_parcel_scores.csv, group by (year), compute
per-parcel protest_prob, then load directly into metrics_tract via the REST API.

Since we don't have a PID→tract crosswalk, we'll compute tract-level averages
directly from the CSV using a tract assignment file if available.
Alternatively, we load the scores with correct IDs into metrics_parcel.
"""
import csv, json, urllib.request, time

BASE_URL = "https://lzwuerruoiqdoiycvntf.supabase.co/rest/v1"
KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Imx6d3VlcnJ1b2lxZG9peWN2bnRmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3MjEzMDQ5NSwiZXhwIjoyMDg3NzA2NDk1fQ.x0bhMiH9SwbzXpGejVsDYps-8MbKmQVvNyDXNgtUsHM"
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
}

# Step 1: Check what columns geo_parcel_poly has — maybe there's a TCAD PID column
# For now, let's try a different approach: query parcel_ladder to see if we can
# find matching accts by stripping leading zeros

# Actually, let's check if LTRIM(metrics_parcel.acct, '0') matches parcel_ladder.acct
# parcel_ladder: 375115
# metrics_parcel: 0212101230
# Stripping zeros: 212101230 — nope, doesn't match 375115

# These are fundamentally different ID systems.
# The geo_parcel_poly was loaded from a shapefile with internal IDs.
# The exp02 CSV uses TCAD property IDs (PIDs).

# Solution: Bypass parcel_ladder entirely.
# Load tract assignments from the original TCAD data or do it in Python.

# Check if there's a tract assignment in the exp02 data
src = "Analysis/Results/Experiments/exp02_isotonic/per_parcel_scores.csv"
print(f"Reading {src}...")
with open(src) as f:
    reader = csv.DictReader(f)
    cols = reader.fieldnames
    print(f"Columns: {cols}")
    
    # Check for tract column
    tract_cols = [c for c in cols if 'tract' in c.lower() or 'geoid' in c.lower() or 'census' in c.lower()]
    print(f"Tract-related columns: {tract_cols}")
    
    if not tract_cols:
        print("\nNo tract column found. Looking at sample data...")
        f.seek(0)
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < 3:
                print(f"  Row {i}: {dict(row)}")
            else:
                break
