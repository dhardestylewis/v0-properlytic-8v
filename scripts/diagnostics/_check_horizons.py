import psycopg2, os
c = psycopg2.connect(os.environ['SUPABASE_DB_URL'])
cur = c.cursor()

for tbl in ['metrics_zcta_forecast', 'metrics_tract_forecast', 'metrics_tabblock_forecast', 'metrics_parcel_forecast']:
    cur.execute(f'SELECT horizon_m, count(*) FROM forecast_20260220_7f31c6e4.{tbl} GROUP BY horizon_m ORDER BY horizon_m')
    print(f'\n{tbl}:')
    for r in cur.fetchall():
        print(f'  horizon_m={r[0]:>4}, count={r[1]}')

print('\nDone')
