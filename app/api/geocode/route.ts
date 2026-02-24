import { NextRequest, NextResponse } from "next/server"

export async function GET(req: NextRequest) {
    const lat = req.nextUrl.searchParams.get("lat")
    const lng = req.nextUrl.searchParams.get("lng")
    const zoom = req.nextUrl.searchParams.get("zoom") || "16"

    if (!lat || !lng) {
        return NextResponse.json({ error: "lat and lng required" }, { status: 400 })
    }

    try {
        const url = `https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&zoom=${zoom}&format=json`
        const res = await fetch(url, {
            headers: { "User-Agent": "HomecastrUI/1.0 (properlytic.com)" },
            next: { revalidate: 86400 }, // cache for 24h
        })

        if (!res.ok) {
            return NextResponse.json({ error: "Nominatim error" }, { status: res.status })
        }

        const data = await res.json()
        return NextResponse.json(data, {
            headers: { "Cache-Control": "public, max-age=86400" },
        })
    } catch (err) {
        console.error("[GEOCODE PROXY] Error:", err)
        return NextResponse.json({ error: "Geocode failed" }, { status: 500 })
    }
}
