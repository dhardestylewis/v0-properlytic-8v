import { cellToLatLng } from "h3-js"

export interface PropertyLocation {
    lat: number
    lng: number
    label?: string
}

/**
 * Generates a list of representative property locations for a given set of H3 hexes.
 * If one hex is selected, it generates offsets around the center.
 * If multiple hexes are selected, it uses the centers of those hexes.
 */
export function getRepresentativeProperties(h3Ids: string[]): PropertyLocation[] {
    if (h3Ids.length === 0) return []

    if (h3Ids.length === 1) {
        const id = h3Ids[0]
        const [lat, lng] = cellToLatLng(id)

        // Generate a few stable offsets to simulate different properties in the same hex
        // We use small fixed offsets so the "properties" are consistent for the same hex
        return [
            { lat: lat, lng: lng, label: "Center" },
            { lat: lat + 0.0004, lng: lng + 0.0004, label: "North East" },
            { lat: lat - 0.0004, lng: lng - 0.0004, label: "South West" },
            { lat: lat + 0.0004, lng: lng - 0.0004, label: "North West" },
            { lat: lat - 0.0004, lng: lng + 0.0004, label: "South East" },
        ]
    }

    // For multiple hexes, take the center of up to 5 hexes
    return h3Ids.slice(0, 5).map((id, index) => {
        const [lat, lng] = cellToLatLng(id)
        return {
            lat,
            lng,
            label: `Area ${index + 1}`
        }
    })
}

/**
 * Constructs a Google Street View Static API URL for a given location.
 * Returns an unsigned URL (for use when signing is handled separately or not needed).
 */
export function getStreetViewImageUrl(lat: number, lng: number, apiKey: string, width = 400, height = 300): string {
    return `https://maps.googleapis.com/maps/api/streetview?size=${width}x${height}&location=${lat},${lng}&key=${apiKey}`
}

/**
 * Fetches a signed Street View URL from the server-side signing endpoint.
 * Falls back to unsigned URL if signing fails.
 */
export async function getSignedStreetViewUrl(lat: number, lng: number, width = 400, height = 300): Promise<string> {
    try {
        const res = await fetch(`/api/streetview-sign?lat=${lat}&lng=${lng}&w=${width}&h=${height}`)
        if (res.ok) {
            const data = await res.json()
            return data.url
        }
    } catch {
        // Fall back to unsigned
    }
    const apiKey = process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY || ""
    return getStreetViewImageUrl(lat, lng, apiKey, width, height)
}
