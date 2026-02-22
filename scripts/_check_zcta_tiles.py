import psycopg2, os
c = psycopg2.connect(os.environ['SUPABASE_DB_URL'])
cur = c.cursor()

# Find all tables in the forecast schema
cur.execute("""
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'forecast_20260220_7f31c6e4'
ORDER BY table_name
""")
print("Tables in forecast schema:")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Find any table with geo info
cur.execute("""
SELECT table_name FROM information_schema.tables
WHERE table_schema = 'forecast_20260220_7f31c6e4'
AND table_name LIKE '%geo%'
""")
print("\nGeo tables:")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Check if there's a public geo table
cur.execute("""
SELECT table_schema, table_name FROM information_schema.tables
WHERE table_name LIKE '%geo%parcel%' OR table_name LIKE '%parcel%geo%'
ORDER BY table_schema, table_name
""")
print("\nGeo parcel tables across all schemas:")
for r in cur.fetchall():
    print(f"  {r[0]}.{r[1]}")

# Check metrics_parcel_forecast columns for geo info
cur.execute("""
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'forecast_20260220_7f31c6e4'
AND table_name = 'metrics_parcel_forecast'
""")
print("\nmetrics_parcel_forecast columns:")
for r in cur.fetchall():
    print(f"  {r[0]}")

# Also check what the rebuild script used previously
cur.execute("""
SELECT column_name FROM information_schema.columns
WHERE table_schema = 'forecast_20260220_7f31c6e4'
AND table_name = 'metrics_tabblock_forecast'
""")
print("\nmetrics_tabblock_forecast columns:")
for r in cur.fetchall():
    print(f"  {r[0]}")

print("\nDone")
