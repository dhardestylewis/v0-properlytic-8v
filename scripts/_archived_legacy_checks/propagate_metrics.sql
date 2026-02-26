-- 1. Clean up any remaining fake test data (0.35) from finer/coarser levels
DELETE FROM oppcastr.metrics_tabblock WHERE protest_prob = 0.35;
DELETE FROM oppcastr.metrics_zcta WHERE protest_prob = 0.35;

-- 2. Propagate real tract data down to tabblocks (Zoom 12+)
-- Tabblock GEOIDs are 15 digits. The first 11 digits are the tract GEOID.
INSERT INTO oppcastr.metrics_tabblock (tabblock_geoid20, year, protest_prob, n)
SELECT b.geoid20, t.year, t.protest_prob, t.n
FROM oppcastr.geo_tabblock20_tx b
JOIN oppcastr.metrics_tract t ON SUBSTRING(b.geoid20, 1, 11) = t.tract_geoid20
ON CONFLICT (tabblock_geoid20, year) DO UPDATE 
SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;

-- 3. Propagate real tract data up to ZCTAs (Zoom <= 7)
-- We do a spatial join: average the tract values for tracts whose centroid is inside the ZCTA
INSERT INTO oppcastr.metrics_zcta (zcta5, year, protest_prob, n)
SELECT z.zcta5, t.year, AVG(t.protest_prob), SUM(t.n)
FROM oppcastr.geo_zcta20_us z
JOIN oppcastr.geo_tract20_tx gt ON ST_Intersects(ST_Centroid(gt.geom), z.geom)
JOIN oppcastr.metrics_tract t ON gt.geoid = t.tract_geoid20
GROUP BY z.zcta5, t.year
ON CONFLICT (zcta5, year) DO UPDATE 
SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;

-- Check counts
SELECT 'tracts' as level, COUNT(*) FROM oppcastr.metrics_tract
UNION ALL
SELECT 'tabblocks' as level, COUNT(*) FROM oppcastr.metrics_tabblock
UNION ALL
SELECT 'zctas' as level, COUNT(*) FROM oppcastr.metrics_zcta;
