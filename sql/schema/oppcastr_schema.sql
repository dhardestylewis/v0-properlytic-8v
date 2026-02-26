-- =============================================================================
-- oppcastr Schema + MVT Functions
-- Paste this into the Supabase SQL Editor for the oppcastr project
-- =============================================================================

-- 0) Schema
CREATE SCHEMA IF NOT EXISTS oppcastr;

-- 1) Geometry tables
CREATE TABLE IF NOT EXISTS oppcastr.geo_parcel_poly (
    acct TEXT PRIMARY KEY,
    prop_id TEXT,
    geom geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS geo_parcel_poly_geom_gix
    ON oppcastr.geo_parcel_poly USING gist (geom);

CREATE TABLE IF NOT EXISTS oppcastr.geo_tract20_tx (
    geoid TEXT PRIMARY KEY,
    geom geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS geo_tract20_tx_geom_gix
    ON oppcastr.geo_tract20_tx USING gist (geom);

CREATE TABLE IF NOT EXISTS oppcastr.geo_tabblock20_tx (
    geoid20 TEXT PRIMARY KEY,
    geom geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS geo_tabblock20_tx_geom_gix
    ON oppcastr.geo_tabblock20_tx USING gist (geom);

CREATE TABLE IF NOT EXISTS oppcastr.geo_zcta20_us (
    zcta5 TEXT PRIMARY KEY,
    geom geometry(MultiPolygon, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS geo_zcta20_us_geom_gix
    ON oppcastr.geo_zcta20_us USING gist (geom);

-- 2) Parcel ladder (parcel → tabblock → tract → zcta)
CREATE TABLE IF NOT EXISTS oppcastr.parcel_ladder (
    acct TEXT PRIMARY KEY,
    prop_id TEXT,
    tabblock_geoid20 TEXT,
    tract_geoid20 TEXT,
    zcta5 TEXT
);

-- 3) Metrics tables
CREATE TABLE IF NOT EXISTS oppcastr.metrics_parcel (
    acct TEXT NOT NULL,
    year INTEGER NOT NULL,
    protest_prob DOUBLE PRECISION,
    protest_actual BOOLEAN,
    n INTEGER DEFAULT 1,
    PRIMARY KEY (acct, year)
);

CREATE TABLE IF NOT EXISTS oppcastr.metrics_tabblock (
    tabblock_geoid20 TEXT NOT NULL,
    year INTEGER NOT NULL,
    protest_prob DOUBLE PRECISION,
    n INTEGER,
    PRIMARY KEY (tabblock_geoid20, year)
);

CREATE TABLE IF NOT EXISTS oppcastr.metrics_tract (
    tract_geoid20 TEXT NOT NULL,
    year INTEGER NOT NULL,
    protest_prob DOUBLE PRECISION,
    n INTEGER,
    PRIMARY KEY (tract_geoid20, year)
);

CREATE TABLE IF NOT EXISTS oppcastr.metrics_zcta (
    zcta5 TEXT NOT NULL,
    year INTEGER NOT NULL,
    protest_prob DOUBLE PRECISION,
    n INTEGER,
    PRIMARY KEY (zcta5, year)
);

-- 4) Generic MVT builder
CREATE OR REPLACE FUNCTION oppcastr.mvt_protest_generic(
    p_layer_name TEXT,
    p_geom_table TEXT,
    p_geom_key TEXT,
    p_metrics_table TEXT,
    p_metrics_key TEXT,
    z INTEGER, x INTEGER, y INTEGER,
    p_year INTEGER DEFAULT 2024,
    p_limit INTEGER DEFAULT NULL
)
RETURNS bytea
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_sql TEXT;
    v_mvt bytea;
    v_limit TEXT := '';
BEGIN
    IF p_limit IS NOT NULL AND p_limit > 0 THEN
        v_limit := format(' LIMIT %s', p_limit);
    END IF;

    v_sql := format($fmt$
        WITH bounds AS (
            SELECT ST_TileEnvelope($1,$2,$3) AS b3857,
                   ST_Transform(ST_TileEnvelope($1,$2,$3),4326) AS b4326
        ),
        src AS (
            SELECT
                g.%1$I::text AS id,
                m.year,
                m.protest_prob AS value,
                m.protest_prob AS p50,
                m.n,
                ST_AsMVTGeom(ST_Transform(g.geom,3857), bounds.b3857, 4096, 256, true) AS geom
            FROM %2$s g
            JOIN %3$s m ON m.%4$I = g.%1$I
            CROSS JOIN bounds
            WHERE g.geom && bounds.b4326
              AND ST_Intersects(g.geom, bounds.b4326)
              AND m.year = $4
            %5$s
        )
        SELECT ST_AsMVT(src, %6$L, 4096, 'geom') FROM src
    $fmt$,
        p_geom_key,
        p_geom_table,
        p_metrics_table,
        p_metrics_key,
        v_limit,
        p_layer_name
    );

    EXECUTE v_sql USING z, x, y, p_year INTO v_mvt;
    RETURN COALESCE(v_mvt, ''::bytea);
END;
$$;

-- 5) Zoom-level router (matches properlytic's pattern)
CREATE OR REPLACE FUNCTION oppcastr.mvt_choropleth_protest(
    z INTEGER, x INTEGER, y INTEGER,
    p_year INTEGER DEFAULT 2024,
    p_level_override TEXT DEFAULT NULL,
    p_parcel_limit INTEGER DEFAULT 3500
)
RETURNS bytea
LANGUAGE plpgsql STABLE
AS $$
BEGIN
    IF p_level_override IS NOT NULL THEN
        CASE lower(p_level_override)
            WHEN 'zcta' THEN
                RETURN oppcastr.mvt_protest_generic('zcta', 'oppcastr.geo_zcta20_us', 'zcta5', 'oppcastr.metrics_zcta', 'zcta5', z, x, y, p_year);
            WHEN 'tract' THEN
                RETURN oppcastr.mvt_protest_generic('tract', 'oppcastr.geo_tract20_tx', 'geoid', 'oppcastr.metrics_tract', 'tract_geoid20', z, x, y, p_year);
            WHEN 'tabblock' THEN
                RETURN oppcastr.mvt_protest_generic('tabblock', 'oppcastr.geo_tabblock20_tx', 'geoid20', 'oppcastr.metrics_tabblock', 'tabblock_geoid20', z, x, y, p_year);
            WHEN 'parcel' THEN
                RETURN oppcastr.mvt_protest_generic('parcel', 'oppcastr.geo_parcel_poly', 'acct', 'oppcastr.metrics_parcel', 'acct', z, x, y, p_year, p_parcel_limit);
            ELSE
                RETURN ''::bytea;
        END CASE;
    END IF;

    IF z <= 7 THEN
        RETURN oppcastr.mvt_protest_generic('zcta', 'oppcastr.geo_zcta20_us', 'zcta5', 'oppcastr.metrics_zcta', 'zcta5', z, x, y, p_year);
    ELSIF z <= 11 THEN
        RETURN oppcastr.mvt_protest_generic('tract', 'oppcastr.geo_tract20_tx', 'geoid', 'oppcastr.metrics_tract', 'tract_geoid20', z, x, y, p_year);
    ELSIF z <= 16 THEN
        RETURN oppcastr.mvt_protest_generic('tabblock', 'oppcastr.geo_tabblock20_tx', 'geoid20', 'oppcastr.metrics_tabblock', 'tabblock_geoid20', z, x, y, p_year);
    ELSE
        RETURN oppcastr.mvt_protest_generic('parcel', 'oppcastr.geo_parcel_poly', 'acct', 'oppcastr.metrics_parcel', 'acct', z, x, y, p_year, p_parcel_limit);
    END IF;
END;
$$;

-- 6) Aggregation queries (run after loading parcel data)
-- These recompute tabblock/tract/zcta averages from parcel-level scores.
-- Run step by step after data is loaded.

-- TABBLOCK:
-- INSERT INTO oppcastr.metrics_tabblock (tabblock_geoid20, year, protest_prob, n)
-- SELECT pl.tabblock_geoid20, mp.year, AVG(mp.protest_prob), COUNT(*)
-- FROM oppcastr.metrics_parcel mp
-- JOIN oppcastr.parcel_ladder pl ON pl.acct = mp.acct
-- WHERE pl.tabblock_geoid20 IS NOT NULL
-- GROUP BY pl.tabblock_geoid20, mp.year
-- ON CONFLICT (tabblock_geoid20, year) DO UPDATE SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;

-- TRACT:
-- INSERT INTO oppcastr.metrics_tract (tract_geoid20, year, protest_prob, n)
-- SELECT pl.tract_geoid20, mp.year, AVG(mp.protest_prob), COUNT(*)
-- FROM oppcastr.metrics_parcel mp
-- JOIN oppcastr.parcel_ladder pl ON pl.acct = mp.acct
-- WHERE pl.tract_geoid20 IS NOT NULL
-- GROUP BY pl.tract_geoid20, mp.year
-- ON CONFLICT (tract_geoid20, year) DO UPDATE SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;

-- ZCTA:
-- INSERT INTO oppcastr.metrics_zcta (zcta5, year, protest_prob, n)
-- SELECT pl.zcta5, mp.year, AVG(mp.protest_prob), COUNT(*)
-- FROM oppcastr.metrics_parcel mp
-- JOIN oppcastr.parcel_ladder pl ON pl.acct = mp.acct
-- WHERE pl.zcta5 IS NOT NULL
-- GROUP BY pl.zcta5, mp.year
-- ON CONFLICT (zcta5, year) DO UPDATE SET protest_prob = EXCLUDED.protest_prob, n = EXCLUDED.n;
