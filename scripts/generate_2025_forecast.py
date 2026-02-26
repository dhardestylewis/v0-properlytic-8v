"""
Generate and Upload 2025 Out-Of-Sample Protest Forecast

Trains a balanced logistic regression model on the entire historical dataset 
(2019-2024) and scores the universe of 2024 parcels to project their 2025 
protest probabilities. Uploads the results directly to the Supabase 
`metrics_parcel` table.
"""
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
import requests
import os
import json
from dotenv import load_dotenv

# Load environment variables
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
print(f"Reading panel from {csv_path}...")
df = pd.read_csv(csv_path)

# Filter criteria exactly as backtest_naive.py
mask_backfill = df['ears_source'].fillna('').astype(str).str.contains('backfill', na=False)
df = df[(df['year'] >= 2019) & (df['ears_matched'] == 1) & (~mask_backfill)].copy()
print(f"Loaded {len(df)} training rows from 2019-2024.")

# Features
NUMERIC_FEATURES = [
    "market_value", "assessed_value", "land_value", "improvement_value",
    "living_area", "deed_acreage", "year_built", "land_acres", "improvement_count",
]
CATEGORICAL_FEATURES = ["property_category_code", "lui_general_land_use", "council_district"]

# Quick auto-imputation and dummies (minimal reproduction of featurize)
for num in NUMERIC_FEATURES:
    df[num] = pd.to_numeric(df.get(num, pd.Series(0.0, index=df.index)), errors='coerce').fillna(0.0)

for cat in CATEGORICAL_FEATURES:
    df[cat] = df.get(cat, pd.Series('', index=df.index)).fillna('').astype(str)

df = pd.get_dummies(df, columns=CATEGORICAL_FEATURES, dummy_na=False)
feature_cols = [c for c in df.columns if any(c.startswith(f) for f in NUMERIC_FEATURES + CATEGORICAL_FEATURES) and c not in CATEGORICAL_FEATURES]

# Train vs Score masks
# We train on all data up to 2024, but score the 2024 objects to project to 2025
train_mask = df['year'] <= 2024
score_mask = df['year'] == 2024 # Target baseline for projecting h=1 (2025)

X_train = df.loc[train_mask, feature_cols].values
y_train = df.loc[train_mask, 'protest'].fillna(0).astype(int).values

X_score = df.loc[score_mask, feature_cols].values
ids_score = df.loc[score_mask, 'standardized_tcad_id'].values

scaler = StandardScaler()
X_train_s = np.nan_to_num(scaler.fit_transform(X_train), nan=0)
X_score_s = np.nan_to_num(scaler.transform(X_score), nan=0)

model = LogisticRegression(class_weight="balanced", max_iter=1000, solver="lbfgs", random_state=42)
print("Training Logistic Regression on all historical features...")
model.fit(X_train_s, y_train)

print(f"Scoring {len(X_score_s)} parcels for 2025...")
probs_2025 = model.predict_proba(X_score_s)[:, 1]

# Prepare for Supabase
out_df = pd.DataFrame({
    'acct': pd.Series(ids_score).astype(str).str.zfill(10),
    'year': 2025,
    'protest_prob': np.round(probs_2025, 5),
    'protest_actual': None
})
records = out_df.to_dict(orient='records')

print(f"Uploading {len(records)} forecast records to Supabase...")
batch_size = 5000
for i in range(0, len(records), batch_size):
    batch = records[i:i+batch_size]
    res = requests.post(URL, headers=HEADERS, json={'data': batch})
    if res.status_code != 200:
        print(f'Error at batch {i}:', res.status_code, res.text)
        break
    else:
        print(f'  Inserted {i+len(batch)} / {len(records)}')

print('Done uploading 2025 OOT forecast!')
