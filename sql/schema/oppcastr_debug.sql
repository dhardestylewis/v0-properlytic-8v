-- Debug: check if data exists and joins work
-- Paste ONE query at a time

-- 1) Count parcels in geo table
SELECT COUNT(*) as geo_parcels FROM oppcastr.geo_parcel_poly;

-- 2) Count scores in metrics table
-- SELECT COUNT(*) as scores FROM oppcastr.metrics_parcel WHERE protest_prob IS NOT NULL;

-- 3) Sample accts from both tables to check format
-- SELECT acct FROM oppcastr.geo_parcel_poly LIMIT 5;

-- 4) Sample accts from metrics 
-- SELECT acct FROM oppcastr.metrics_parcel WHERE protest_prob IS NOT NULL LIMIT 5;

-- 5) Test JOIN
-- SELECT COUNT(*) as joined FROM oppcastr.geo_parcel_poly g JOIN oppcastr.metrics_parcel m ON m.acct = g.acct WHERE m.protest_prob IS NOT NULL;

-- 6) Count tract metrics
-- SELECT COUNT(*) FROM oppcastr.metrics_tract WHERE protest_prob IS NOT NULL;
