import { NextRequest, NextResponse } from "next/server"
import crypto from "crypto"

/**
 * Signs a Google Maps Street View Static API URL using HMAC-SHA1.
 * This removes the 25,000/day unsigned request limit.
 *
 * GET /api/streetview-sign?lat=29.76&lng=-95.36&w=400&h=300
 */
export async function GET(req: NextRequest) {
    const { searchParams } = req.nextUrl
    const lat = searchParams.get("lat")
    const lng = searchParams.get("lng")
    const w = searchParams.get("w") || "400"
    const h = searchParams.get("h") || "300"

    const apiKey = process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY
    const signingSecret = process.env.GOOGLE_MAPS_SIGNING_SECRET

    if (!lat || !lng || !apiKey) {
        return NextResponse.json({ error: "Missing params" }, { status: 400 })
    }

    // Build the unsigned path+query (no domain)
    const path = `/maps/api/streetview?size=${w}x${h}&location=${lat},${lng}&key=${apiKey}`

    if (!signingSecret) {
        // No signing secret configured — return unsigned URL
        return NextResponse.json({
            url: `https://maps.googleapis.com${path}`,
        })
    }

    // Decode the URL-safe base64 signing secret
    const decodedKey = Buffer.from(
        signingSecret.replace(/-/g, "+").replace(/_/g, "/"),
        "base64"
    )

    // HMAC-SHA1 sign the path
    const signature = crypto
        .createHmac("sha1", decodedKey)
        .update(path)
        .digest("base64")
        // Make URL-safe
        .replace(/\+/g, "-")
        .replace(/\//g, "_")

    return NextResponse.json({
        url: `https://maps.googleapis.com${path}&signature=${signature}`,
    })
}
