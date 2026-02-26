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
 * oppcastr – Protest probability choropleth MVT tile endpoint.
 *
 * Calls oppcastr.mvt_choropleth_protest(z,x,y,p_year)
 * which auto-routes by zoom:
 *   z <= 7  → ZCTA
 *   z <= 11 → Tract
 *   z <= 16 → Tabblock
 *   z >= 17 → Parcel (capped at 3500)
 *
 * Query params:
 *   year   (default 2024)
 *   level  (optional override: zcta/tract/tabblock/parcel)
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
    const year = parseInt(searchParams.get("year") || searchParams.get("originYear") || "2024")
    const levelOverride = searchParams.get("level") || null

    const rpcParams = {
        z,
        x,
        y,
        p_year: year,
        p_level_override: levelOverride,
        p_parcel_limit: 3500,
    }

    // Retry once on transient errors before returning empty tile
    for (let attempt = 0; attempt < 2; attempt++) {
        try {
            const supabase = getSupabaseAdmin()

            const { data, error } = await supabase
                .rpc("mvt_choropleth_protest", rpcParams)

            if (error) {
                console.error(`[OPPCASTR-TILE] RPC error (attempt ${attempt + 1}):`, {
                    message: error.message,
                    code: error.code,
                    tile: `${z}/${x}/${y}`,
                })
                if (attempt === 0) {
                    await new Promise((r) => setTimeout(r, 500))
                    continue
                }
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
            console.error(`[OPPCASTR-TILE] Exception (attempt ${attempt + 1}):`, e.message, `tile=${z}/${x}/${y}`)
            if (attempt === 0) {
                await new Promise((r) => setTimeout(r, 500))
                continue
            }
            return emptyTile()
        }
    }

    return emptyTile()
}

