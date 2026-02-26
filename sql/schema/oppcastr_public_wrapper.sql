-- =============================================================================
-- oppcastr: Public schema wrapper for MVT tiles
-- PostgREST only exposes `public` by default, so we need a thin wrapper
-- Paste into Supabase SQL Editor
-- =============================================================================

CREATE OR REPLACE FUNCTION public.mvt_choropleth_protest(
    z INTEGER, x INTEGER, y INTEGER,
    p_year INTEGER DEFAULT 2024,
    p_level_override TEXT DEFAULT NULL,
    p_parcel_limit INTEGER DEFAULT 3500
)
RETURNS bytea
LANGUAGE plpgsql STABLE SECURITY DEFINER
AS $$
BEGIN
    RETURN oppcastr.mvt_choropleth_protest(z, x, y, p_year, p_level_override, p_parcel_limit);
END;
$$;
