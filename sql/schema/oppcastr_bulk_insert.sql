-- =============================================================================
-- oppcastr Bulk Insert Functions (for REST API loading)
-- Paste into Supabase SQL Editor AFTER oppcastr_schema.sql
-- These functions let us load geometry data via the REST API
-- =============================================================================

-- Bulk insert parcels from JSON (WKT geometry)
CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_parcels(
    data JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    rec JSONB;
    cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data)
    LOOP
        INSERT INTO oppcastr.geo_parcel_poly (acct, prop_id, geom)
        VALUES (
            rec->>'acct',
            rec->>'prop_id',
            ST_GeomFromText(rec->>'wkt', 4326)
        )
        ON CONFLICT (acct) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END;
$$;

-- Bulk insert tracts from JSON
CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_tracts(
    data JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    rec JSONB;
    cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data)
    LOOP
        INSERT INTO oppcastr.geo_tract20_tx (geoid, geom)
        VALUES (
            rec->>'geoid',
            ST_GeomFromText(rec->>'wkt', 4326)
        )
        ON CONFLICT (geoid) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END;
$$;

-- Bulk insert tabblocks from JSON
CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_tabblocks(
    data JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    rec JSONB;
    cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data)
    LOOP
        INSERT INTO oppcastr.geo_tabblock20_tx (geoid20, geom)
        VALUES (
            rec->>'geoid20',
            ST_GeomFromText(rec->>'wkt', 4326)
        )
        ON CONFLICT (geoid20) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END;
$$;

-- Bulk insert ZCTAs from JSON
CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_zctas(
    data JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    rec JSONB;
    cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data)
    LOOP
        INSERT INTO oppcastr.geo_zcta20_us (zcta5, geom)
        VALUES (
            rec->>'zcta5',
            ST_GeomFromText(rec->>'wkt', 4326)
        )
        ON CONFLICT (zcta5) DO NOTHING;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END;
$$;

-- Bulk insert protest scores from JSON
CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_scores(
    data JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    rec JSONB;
    cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data)
    LOOP
        INSERT INTO oppcastr.metrics_parcel (acct, year, protest_prob, protest_actual)
        VALUES (
            rec->>'acct',
            (rec->>'year')::integer,
            (rec->>'protest_prob')::double precision,
            CASE WHEN rec->>'protest_actual' IS NOT NULL
                 THEN (rec->>'protest_actual')::boolean
                 ELSE NULL END
        )
        ON CONFLICT (acct, year) DO UPDATE SET
            protest_prob = EXCLUDED.protest_prob,
            protest_actual = EXCLUDED.protest_actual;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END;
$$;

-- Bulk build parcel_ladder from JSON
CREATE OR REPLACE FUNCTION public.oppcastr_bulk_insert_ladder(
    data JSONB
)
RETURNS INTEGER
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    rec JSONB;
    cnt INTEGER := 0;
BEGIN
    FOR rec IN SELECT jsonb_array_elements(data)
    LOOP
        INSERT INTO oppcastr.parcel_ladder (acct, prop_id, tabblock_geoid20, tract_geoid20, zcta5)
        VALUES (
            rec->>'acct',
            rec->>'prop_id',
            rec->>'tabblock_geoid20',
            rec->>'tract_geoid20',
            rec->>'zcta5'
        )
        ON CONFLICT (acct) DO UPDATE SET
            tabblock_geoid20 = EXCLUDED.tabblock_geoid20,
            tract_geoid20 = EXCLUDED.tract_geoid20,
            zcta5 = EXCLUDED.zcta5;
        cnt := cnt + 1;
    END LOOP;
    RETURN cnt;
END;
$$;
