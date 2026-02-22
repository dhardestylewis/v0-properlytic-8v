import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

const TILE_HEADERS = {
    "Content-Type": "application/vnd.mapbox-vector-tile",
    "Cache-Control": "public, max-age=3600",
    "Access-Control-Allow-Origin": "*",
} as const

/** Return an empty 204 so MapLibre silently skips this tile instead of throwing AJAXError */
function emptyTile() {
    return new NextResponse(null, { status: 204, headers: TILE_HEADERS })
}

/**
 * Forecast choropleth MVT tile endpoint.
 *
 * Calls forecast_20260220_7f31c6e4.mvt_choropleth_forecast(z,x,y,origin_year,horizon_m)
 * which auto-routes by zoom:
 *   z <= 7  → ZCTA
 *   z <= 11 → Tract
 *   z <= 16 → Tabblock
 *   z >= 17 → Parcel (capped at 3500)
 *
 * On transient Supabase errors we retry once, then return an empty tile (204)
 * rather than a 500 that would flood the browser console with AJAXErrors.
 *
 * Query params:
 *   originYear  (default 2025)
 *   horizonM    (default 12)
 *   seriesKind  (default 'forecast')
 *   variantId   (default '__forecast__')
 *   level       (optional override: zcta/tract/tabblock/parcel/unsd/neighborhood)
 */
export async function GET(
    request: Request,
    { params }: { params: Promise<{ z: string; x: string; y: string }> }
) {
    const { z: zStr, x: xStr, y: yStr } = await params

    const z = parseInt(zStr)
    const x = parseInt(xStr)
    const y = parseInt(yStr)

    const { searchParams } = new URL(request.url)
    const originYear = parseInt(searchParams.get("originYear") || "2025")
    const horizonM = parseInt(searchParams.get("horizonM") || "12")
    const seriesKind = searchParams.get("seriesKind") || "forecast"
    const variantId = searchParams.get("variantId") || "__forecast__"
    const levelOverride = searchParams.get("level") || null

    const rpcParams = {
        z,
        x,
        y,
        p_origin_year: originYear,
        p_horizon_m: horizonM,
        p_level_override: levelOverride,
        p_series_kind: seriesKind,
        p_variant_id: variantId,
        p_run_id: null,
        p_backtest_id: null,
        p_parcel_limit: 3500,
    }

    // Retry once on transient errors before returning empty tile
    for (let attempt = 0; attempt < 2; attempt++) {
        try {
            const supabase = getSupabaseAdmin()

            const { data, error } = await supabase
                .schema("forecast_20260220_7f31c6e4" as any)
                .rpc("mvt_choropleth_forecast", rpcParams)

            if (error) {
                console.error(`[FORECAST-TILE] RPC error (attempt ${attempt + 1}):`, {
                    message: error.message,
                    code: error.code,
                    tile: `${z}/${x}/${y}`,
                })
                if (attempt === 0) {
                    await new Promise((r) => setTimeout(r, 500))
                    continue
                }
                // Final attempt failed — return empty tile instead of 500
                return emptyTile()
            }

            if (!data) {
                return emptyTile()
            }

            // Supabase bytea → Buffer
            let buffer: Buffer
            if (typeof data === "string") {
                if (data.startsWith("\\x")) {
                    buffer = Buffer.from(data.substring(2), "hex")
                } else {
                    buffer = Buffer.from(data, "base64")
                }
            } else {
                buffer = Buffer.from(data)
            }

            return new NextResponse(new Uint8Array(buffer), {
                status: 200,
                headers: TILE_HEADERS,
            })
        } catch (e: any) {
            console.error(`[FORECAST-TILE] Exception (attempt ${attempt + 1}):`, e.message, `tile=${z}/${x}/${y}`)
            if (attempt === 0) {
                await new Promise((r) => setTimeout(r, 500))
                continue
            }
            // Final attempt — empty tile, not 500
            return emptyTile()
        }
    }

    // Shouldn't reach here, but safety net
    return emptyTile()
}
