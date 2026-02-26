-- =============================================================================
-- oppcastr: Aggregate parcel scores to tabblock/tract/zcta
-- Run each query ONE AT A TIME in the Supabase SQL Editor
-- Run AFTER oppcastr_build_ladder.sql steps are complete
-- =============================================================================

-- QUERY 1: Tabblock aggregation
INSERT INTO oppcastr.metrics_tabblock (tabblock_geoid20, year, protest_prob, n)
SELECT pl.tabblock_geoid20, mp.year,
       AVG(COALESCE(mp.protest_prob, mp.protest_actual::int::float8)),
       COUNT(*)
FROM oppcastr.metrics_parcel mp JOIN oppcastr.parcel_ladder pl ON pl.acct = mp.acct
WHERE pl.tabblock_geoid20 IS NOT NULL
  AND (mp.protest_prob IS NOT NULL OR mp.protest_actual IS NOT NULL)
GROUP BY pl.tabblock_geoid20, mp.year
ON CONFLICT (tabblock_geoid20, year) DO UPDATE SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;

-- QUERY 2: Tract aggregation
INSERT INTO oppcastr.metrics_tract (tract_geoid20, year, protest_prob, n)
SELECT pl.tract_geoid20, mp.year,
       AVG(COALESCE(mp.protest_prob, mp.protest_actual::int::float8)),
       COUNT(*)
FROM oppcastr.metrics_parcel mp JOIN oppcastr.parcel_ladder pl ON pl.acct = mp.acct
WHERE pl.tract_geoid20 IS NOT NULL
  AND (mp.protest_prob IS NOT NULL OR mp.protest_actual IS NOT NULL)
GROUP BY pl.tract_geoid20, mp.year
ON CONFLICT (tract_geoid20, year) DO UPDATE SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;

-- QUERY 3: ZCTA aggregation
INSERT INTO oppcastr.metrics_zcta (zcta5, year, protest_prob, n)
SELECT pl.zcta5, mp.year,
       AVG(COALESCE(mp.protest_prob, mp.protest_actual::int::float8)),
       COUNT(*)
FROM oppcastr.metrics_parcel mp JOIN oppcastr.parcel_ladder pl ON pl.acct = mp.acct
WHERE pl.zcta5 IS NOT NULL
  AND (mp.protest_prob IS NOT NULL OR mp.protest_actual IS NOT NULL)
GROUP BY pl.zcta5, mp.year
ON CONFLICT (zcta5, year) DO UPDATE SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;

