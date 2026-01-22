
-- Function to fetch parcels in a bounding box
-- Used by the frontend VectorMap for block-level viewing (Zoom 14+)
CREATE OR REPLACE FUNCTION public.get_parcels_in_bounds(
    min_lat float8,
    max_lat float8,
    min_lng float8,
    max_lng float8
)
RETURNS TABLE (
    acct_key text,
    geometry json
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
AS $function$
BEGIN
    RETURN QUERY
    SELECT 
        p.acct_key,
        ST_AsGeoJSON(p.geom)::json as geometry
    FROM parcels p
    WHERE 
        -- Use bounding box operator for performance
        p.geom && ST_MakeEnvelope(min_lng, min_lat, max_lng, max_lat, 4326)
        AND ST_Intersects(p.geom, ST_MakeEnvelope(min_lng, min_lat, max_lng, max_lat, 4326))
    LIMIT 200; -- Safety cap for performance
END;
$function$;
