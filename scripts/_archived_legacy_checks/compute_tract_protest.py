"""
Generate SQL INSERT statements for tract-level protest probabilities.
Outputs SQL that the user can paste into Supabase SQL Editor.
"""
import csv, json, os
from collections import defaultdict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
THESIS_DIR = os.path.dirname(PROJECT_DIR)

SCORES_PATH = os.path.join(THESIS_DIR, "Analysis", "Results", "Experiments",
                           "exp02_isotonic", "per_parcel_scores.csv")
GEOJSON_PATH = os.path.join(THESIS_DIR, "Data", "Protest_Petitions",
                            "GeoJSON", "protest_petitions_v1.geojson")

# Step 1: Build PID → GEOID mapping from GeoJSON
print("Building PID → tract_GEOID mapping from GeoJSON...")
pid_to_geoid = {}

with open(GEOJSON_PATH, 'r', encoding='utf-8') as f:
    data = json.load(f)
for feat in data['features']:
    p = feat.get('properties', {}) or {}
    pid = (p.get('standardized_tcad_id') or '').strip()
    geoid = (p.get('nearby_GEOID') or '').strip()
    if pid and geoid and len(geoid) >= 11:
        pid_to_geoid[pid] = geoid[:11]

print(f"  {len(pid_to_geoid)} PID → tract mappings")

# Step 2: Aggregate by (tract, year)
tract_agg = defaultdict(lambda: {'sum_prob': 0.0, 'n': 0})
matched = 0
with open(SCORES_PATH) as f:
    for row in csv.DictReader(f):
        pid = row['pid'].strip()
        year = int(row['year'])
        prob = float(row['ens_calibrated'])
        geoid = pid_to_geoid.get(pid)
        if geoid:
            key = (geoid, year)
            tract_agg[key]['sum_prob'] += prob
            tract_agg[key]['n'] += 1
            matched += 1

print(f"  {matched:,} scores matched, {len(tract_agg)} tract-year groups")

# Step 3: Generate SQL
sql_path = os.path.join(SCRIPT_DIR, "tract_protest_upsert.sql")
with open(sql_path, 'w') as f:
    f.write("-- Auto-generated tract-level protest probability upsert\n")
    f.write("-- From exp02_isotonic scores × GeoJSON nearby_GEOID\n\n")
    
    # Build VALUES list
    values = []
    for (geoid, year), agg in sorted(tract_agg.items()):
        avg_prob = agg['sum_prob'] / agg['n']
        values.append(f"  ('{geoid}', {year}, {avg_prob:.6f}, {agg['n']})")
    
    f.write("INSERT INTO oppcastr.metrics_tract (tract_geoid20, year, protest_prob, n)\nVALUES\n")
    f.write(",\n".join(values))
    f.write("\nON CONFLICT (tract_geoid20, year) DO UPDATE SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;\n")

print(f"  SQL written to: {sql_path}")
print(f"  {len(values)} rows in INSERT statement")

# Print year stats
year_stats = defaultdict(lambda: {'tracts': 0, 'min': 1, 'max': 0, 'sum': 0})
for (geoid, year), agg in tract_agg.items():
    avg_prob = agg['sum_prob'] / agg['n']
    ys = year_stats[year]
    ys['tracts'] += 1
    ys['min'] = min(ys['min'], avg_prob)
    ys['max'] = max(ys['max'], avg_prob)
    ys['sum'] += avg_prob

print("\nYear summary:")
for year in sorted(year_stats):
    ys = year_stats[year]
    avg = ys['sum'] / ys['tracts']
    print(f"  {year}: {ys['tracts']} tracts, avg={avg:.4f}, range=[{ys['min']:.4f}, {ys['max']:.4f}]")
