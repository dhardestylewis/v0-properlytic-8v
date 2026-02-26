"""
Upload Pre-2021 True Historical Actuals to Supabase

This script reads the raw property-year panel and extracts the ground truth 
protest records for 2019 and 2020. Since the model wasn't scored on these 
years (they are part of the training period), we just upload the actuals.
The protest_prob is set to NULL (None) for these records to ensure the map 
and fan chart handle them linearly and truthfully.
"""
import pandas as pd
import requests
import os
import json
from dotenv import load_dotenv

# Load environment variables from the project root
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env.local')
load_dotenv(dotenv_path)

URL = os.environ['NEXT_PUBLIC_SUPABASE_URL'] + '/rest/v1/rpc/oppcastr_bulk_insert_scores'
KEY = os.environ['SUPABASE_SERVICE_ROLE_KEY']
HEADERS = {
    'apikey': KEY,
    'Authorization': f'Bearer {KEY}',
    'Content-Type': 'application/json'
}

csv_path = os.path.join(os.path.dirname(__file__), '..', '..', 'Data', 'Panel', 'Output', 'Property_Year_Panel_v3.csv')
print(f"Reading {csv_path}...")
df = pd.read_csv(csv_path, usecols=['standardized_tcad_id', 'year', 'protest', 'ears_matched', 'ears_source'])

# Filter for 2007-2020 actuals that are EARS matched
mask_backfill = df['ears_source'].fillna('').astype(str).str.contains('backfill', na=False)
df = df[(df['year'] >= 2007) & (df['year'] <= 2020) & (df['ears_matched'] == 1) & (~mask_backfill)].copy()

print(f"Processing {len(df)} rows for 2007-2020...")
df['acct'] = df['standardized_tcad_id'].astype(str).str.zfill(10)
df['protest_prob'] = None
df['protest_actual'] = df['protest'].apply(lambda x: True if str(x)=='1.0' or str(x)=='1' else (False if str(x)=='0.0' or str(x)=='0' else None))

records = df[['acct', 'year', 'protest_prob', 'protest_actual']].to_dict(orient='records')

print(f"Uploading {len(records)} records in batches...")
batch_size = 5000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    res = requests.post(URL, headers=HEADERS, json={'data': batch})
    if res.status_code != 200:
        print(f'Error at batch {i}:', res.status_code, res.text)
        break
    else:
        print(f'  Inserted {i+len(batch)} / {len(records)}')

print('Done uploading 2019-2020 historical actuals!')
