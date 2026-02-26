import { NextResponse } from "next/server"
import { getSupabaseAdmin } from "@/lib/supabase/admin"

/**
 * Fetch ALL horizons for a given geography ID so the tooltip can render a FanChart.
 * Also fetches historical values (2019-2025) from the history table.
 *
 * Query params:
 *   level   = zcta | tract | tabblock | parcel
 *   id      = the geography key (e.g. "77079" for zcta, "48201..." for tract)
 *   originYear (default 2025)
 *
 * Returns { years, p10, p25, p50, p75, p90, y_med, historicalValues } arrays.
 */
const LEVEL_TABLE: Record<string, { table: string; key: string }> = {
    zcta: { table: "metrics_zcta", key: "zcta5" },
    tract: { table: "metrics_tract", key: "tract_geoid20" },
    tabblock: { table: "metrics_tabblock", key: "tabblock_geoid20" },
    parcel: { table: "metrics_parcel", key: "acct" },
}

export async function GET(request: Request) {
    const { searchParams } = new URL(request.url)
    const level = searchParams.get("level") || "zcta"
    const id = searchParams.get("id")

    if (!id) {
        return NextResponse.json({ error: "id is required" }, { status: 400 })
    }

    const meta = LEVEL_TABLE[level]
    if (!meta) {
        return NextResponse.json({ error: `Unknown level: ${level}` }, { status: 400 })
    }

    const supabase = getSupabaseAdmin()

    try {
        const { data, error } = await supabase
            .rpc("get_historical_metrics", {
                p_table: meta.table,
                p_key_col: meta.key,
                p_id: id
            })

        let queryData = data;
        if (error || !queryData) {
            console.warn(`[FORECAST-DETAIL] RPC failed for ${level}/${id}:`, error?.message)
            const { data: directData, error: dErr } = await supabase
                .schema('oppcastr')
                .from(meta.table)
                .select('*')
                .eq(meta.key, id)
                .order('year', { ascending: true })

            if (dErr) {
                console.error(`[FORECAST-DETAIL] Fallback query also failed for ${level}/${id}:`, dErr.message)
                throw dErr
            }
            queryData = directData
        }

        // The fan chart expects historicalValues for years [2019, 2020, 2021, 2022, 2023, 2024, 2025]
        const historicalValues = [null, null, null, null, null, null, null]
        // Forecast years: [2026, 2027, 2028, 2029, 2030]
        const p50 = [null, null, null, null, null]

        if (queryData && Array.isArray(queryData)) {
            for (const row of queryData) {
                const val = (row.protest_actual !== null && row.protest_actual !== undefined)
                    ? (row.protest_actual ? 1.0 : 0.0)
                    : row.protest_prob

                const year = row.year
                if (year >= 2019 && year <= 2025) {
                    historicalValues[year - 2019] = val
                } else if (year >= 2026 && year <= 2030) {
                    p50[year - 2026] = val
                }
            }
        }

        return NextResponse.json({
            years: [],
            p10: [],
            p25: [],
            p50,
            p75: [],
            p90: [],
            y_med: [],
            historicalValues,
        }, {
            headers: {
                "Cache-Control": "public, max-age=3600",
            },
        })
    } catch (e: any) {
        console.error("[FORECAST-DETAIL] Error:", e)
        return NextResponse.json({ error: e.message }, { status: 500 })
    }
}
