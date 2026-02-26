"""
Upload Historical & Forecast Parcel Scores to Supabase

This script reads the outputs from the diffusion experiments (exp02_isotonic) 
which contain 2021-2024 protest probability scores, formats the TCAD parcel IDs
with the correct zero-padding (10 digits), and uses the Supabase REST API 
(RPC oppcastr_bulk_insert_scores) to securely upload the data in batches.

Run this script when replacing or updating the underlying protest scores.
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

# Use relative path from the script directory
csv_path = os.path.join(os.path.dirname(__file__), '..', 'Analysis', 'Results', 'Experiments', 'exp02_isotonic', 'per_parcel_scores.csv')
print(f"Reading {csv_path}...")
df = pd.read_csv(csv_path)

print(f"Processing {len(df)} rows...")
df['acct'] = df['pid'].astype(str).str.zfill(10)
df['protest_prob'] = df['ens_calibrated'].round(5)
df['protest_actual'] = df['actual'].apply(lambda x: True if str(x)=='1.0' else (False if str(x)=='0.0' else None))

records = df[['acct', 'year', 'protest_prob', 'protest_actual']].to_dict(orient='records')

print(f"Uploading in batches...")
batch_size = 5000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    res = requests.post(URL, headers=HEADERS, json={'data': batch})
    if res.status_code != 200:
        print(f'Error at batch {i}:', res.status_code, res.text)
        break
    else:
        print(f'  Inserted {i+len(batch)} / {len(records)}')

print('Done uploading parcel data!')
