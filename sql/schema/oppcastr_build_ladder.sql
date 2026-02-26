-- =============================================================================
-- oppcastr: Build parcel_ladder in steps (avoids SQL Editor timeout)
-- Run each block ONE AT A TIME in the Supabase SQL Editor
-- =============================================================================

-- STEP 1: Seed the ladder with all parcel accts (fast, no spatial join)
INSERT INTO oppcastr.parcel_ladder (acct, prop_id)
SELECT acct, prop_id FROM oppcastr.geo_parcel_poly
ON CONFLICT (acct) DO NOTHING;

-- STEP 2: Assign tracts (290 tracts, fastest spatial join)
-- Run this AFTER step 1
UPDATE oppcastr.parcel_ladder pl
SET tract_geoid20 = t.geoid
FROM oppcastr.geo_parcel_poly p
JOIN oppcastr.geo_tract20_tx t
    ON ST_Intersects(ST_Centroid(p.geom), t.geom)
WHERE pl.acct = p.acct
  AND pl.tract_geoid20 IS NULL;

-- STEP 3: Assign ZCTAs (449 ZCTAs)
-- Run this AFTER step 2
UPDATE oppcastr.parcel_ladder pl
SET zcta5 = z.zcta5
FROM oppcastr.geo_parcel_poly p
JOIN oppcastr.geo_zcta20_us z
    ON ST_Intersects(ST_Centroid(p.geom), z.geom)
WHERE pl.acct = p.acct
  AND pl.zcta5 IS NULL;

-- STEP 4: Assign tabblocks — BATCHED (16,906 tabblocks, heaviest join)
-- Run these one at a time. Each batch takes ~30s.

-- Batch 4a: first 100K parcels
UPDATE oppcastr.parcel_ladder pl
SET tabblock_geoid20 = tb.geoid20
FROM (SELECT acct, geom FROM oppcastr.geo_parcel_poly ORDER BY acct LIMIT 100000) p
JOIN oppcastr.geo_tabblock20_tx tb
    ON ST_Intersects(ST_Centroid(p.geom), tb.geom)
WHERE pl.acct = p.acct
  AND pl.tabblock_geoid20 IS NULL;

-- Batch 4b: next 100K parcels
UPDATE oppcastr.parcel_ladder pl
SET tabblock_geoid20 = tb.geoid20
FROM (SELECT acct, geom FROM oppcastr.geo_parcel_poly ORDER BY acct LIMIT 100000 OFFSET 100000) p
JOIN oppcastr.geo_tabblock20_tx tb
    ON ST_Intersects(ST_Centroid(p.geom), tb.geom)
WHERE pl.acct = p.acct
  AND pl.tabblock_geoid20 IS NULL;

-- Batch 4c: remaining parcels
UPDATE oppcastr.parcel_ladder pl
SET tabblock_geoid20 = tb.geoid20
FROM (SELECT acct, geom FROM oppcastr.geo_parcel_poly ORDER BY acct LIMIT 100000 OFFSET 200000) p
JOIN oppcastr.geo_tabblock20_tx tb
    ON ST_Intersects(ST_Centroid(p.geom), tb.geom)
WHERE pl.acct = p.acct
  AND pl.tabblock_geoid20 IS NULL;
