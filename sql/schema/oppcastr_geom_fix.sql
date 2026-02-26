-- =============================================================================
-- oppcastr Geometry Type Fix + Updated Bulk Insert Functions
-- Paste into Supabase SQL Editor
-- =============================================================================

-- Fix geometry columns to accept any polygon type (Polygon OR MultiPolygon)
ALTER TABLE oppcastr.geo_tract20_tx ALTER COLUMN geom TYPE geometry(Geometry, 4326);
ALTER TABLE oppcastr.geo_tabblock20_tx ALTER COLUMN geom TYPE geometry(Geometry, 4326);
ALTER TABLE oppcastr.geo_zcta20_us ALTER COLUMN geom TYPE geometry(Geometry, 4326);
ALTER TABLE oppcastr.geo_parcel_poly ALTER COLUMN geom TYPE geometry(Geometry, 4326);

-- Updated bulk insert functions with ST_Multi() cast
CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_tracts(data JSONB)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE rec JSONB; cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data) LOOP
        INSERT INTO oppcastr.geo_tract20_tx (geoid, geom)
        VALUES (rec->>'geoid', ST_Multi(ST_GeomFromText(rec->>'wkt', 4326)))
        ON CONFLICT (geoid) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END; $$;

CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_tabblocks(data JSONB)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE rec JSONB; cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data) LOOP
        INSERT INTO oppcastr.geo_tabblock20_tx (geoid20, geom)
        VALUES (rec->>'geoid20', ST_Multi(ST_GeomFromText(rec->>'wkt', 4326)))
        ON CONFLICT (geoid20) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END; $$;

CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_zctas(data JSONB)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE rec JSONB; cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data) LOOP
        INSERT INTO oppcastr.geo_zcta20_us (zcta5, geom)
        VALUES (rec->>'zcta5', ST_Multi(ST_GeomFromText(rec->>'wkt', 4326)))
        ON CONFLICT (zcta5) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END; $$;

CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_parcels(data JSONB)
RETURNS INTEGER LANGUAGE plpgsql SECURITY DEFINER AS $$
DECLARE rec JSONB; cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data) LOOP
        INSERT INTO oppcastr.geo_parcel_poly (acct, prop_id, geom)
        VALUES (rec->>'acct', rec->>'prop_id', ST_Multi(ST_GeomFromText(rec->>'wkt', 4326)))
        ON CONFLICT (acct) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END; $$;
