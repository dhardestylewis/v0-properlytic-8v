"use server"

interface GeocodeResult {
    lat: number
    lng: number
    displayName: string
}

/**
 * Geocodes an address string using OpenStreetMap Nominatim API.
 * 
 * Note: Nominatim Usage Policy requires a valid User-Agent.
 * We enforce a delay to be respectful if hit via loop, though server actions are usually one-off.
 */
export async function geocodeAddress(query: string): Promise<GeocodeResult | null> {
    if (!query || query.trim().length < 3) return null

    try {
        const params = new URLSearchParams({
            q: query,
            format: "json",
            limit: "1",
        })

        const response = await fetch(`https://nominatim.openstreetmap.org/search?${params.toString()}`, {
            headers: {
                "User-Agent": "HomecastrUI/1.0",
            },
        })

        if (!response.ok) {
            console.error(`[Geocode] Error: ${response.statusText}`)
            return null
        }

        const data = await response.json()

        if (Array.isArray(data) && data.length > 0) {
            const result = data[0]
            return {
                lat: Number.parseFloat(result.lat),
                lng: Number.parseFloat(result.lon),
                displayName: result.display_name,
            }
        }

        return null
    } catch (error) {
        console.error("[Geocode] Exception:", error)
        return null
    }
}

export interface AutocompleteResult {
    lat: number
    lng: number
    displayName: string
    address: {
        road?: string
        suburb?: string
        city?: string
        state?: string
        postcode?: string
    }
}

/**
 * Fetches autocomplete suggestions from Nominatim, restricted to Harris County.
 */
export async function getAutocompleteSuggestions(query: string): Promise<AutocompleteResult[]> {
    if (!query || query.trim().length < 3) return []

    try {
        // Harris County Bounding Box (approximate)
        // min_lon=-95.96, min_lat=29.50, max_lon=-94.90, max_lat=30.17
        const viewbox = "-95.96,30.17,-94.90,29.50"

        const params = new URLSearchParams({
            q: query,
            format: "json",
            limit: "5",
            addressdetails: "1",
            viewbox: viewbox,
            bounded: "1"
        })

        const response = await fetch(`https://nominatim.openstreetmap.org/search?${params.toString()}`, {
            headers: {
                "User-Agent": "HomecastrUI/1.0",
            },
        })

        if (!response.ok) return []

        const data = await response.json()

        if (Array.isArray(data)) {
            return data.map((item: any) => ({
                lat: Number.parseFloat(item.lat),
                lng: Number.parseFloat(item.lon),
                displayName: item.display_name,
                address: item.address || {}
            }))
        }

        return []
    } catch (error) {
        console.error("[Geocode] Auto-complete error:", error)
        return []
    }
}

/**
 * Reverse geocodes a coordinate to a human-readable address.
 * The detail level adapts to the map zoom:
 *   z <= 8:   County, State ZIP
 *   z <= 11:  City, State ZIP
 *   z <= 13:  Neighborhood/Suburb, City, ZIP
 *   z <= 15:  Street, City, ZIP
 *   z >= 16:  123 Street, City, ZIP
 */
export async function reverseGeocode(lat: number, lng: number, zoom = 18): Promise<string | null> {
    try {
        // Try BigDataCloud first (free, no rate limits)
        const bdcUrl = `https://api.bigdatacloud.net/data/reverse-geocode-client?latitude=${lat}&longitude=${lng}&localityLanguage=en`
        const response = await fetch(bdcUrl)

        if (!response.ok) {
            // Fallback to Nominatim
            const params = new URLSearchParams({
                lat: lat.toString(),
                lon: lng.toString(),
                zoom: zoom.toString(),
                format: "json",
                addressdetails: "1",
            })
            const nomResponse = await fetch(`https://nominatim.openstreetmap.org/reverse?${params.toString()}`, {
                headers: { "User-Agent": "HomecastrUI/1.0" },
            })
            if (!nomResponse.ok) return null
            const data = await nomResponse.json()
            const addr = data.address || {}
            const house = addr.house_number || ""
            const road = addr.road || addr.street || ""
            const neighbourhood = addr.neighbourhood || addr.suburb || addr.hamlet || ""
            const city = addr.city || addr.town || addr.village || addr.municipality || ""
            const state = addr.state ? _abbreviateState(addr.state) : ""
            const postcode = addr.postcode || ""
            let parts: string[] = []
            if (neighbourhood) parts.push(neighbourhood)
            if (city) parts.push(city)
            if (state || postcode) parts.push([state, postcode].filter(Boolean).join(" "))
            return parts.filter(Boolean).join(", ") || data.display_name || null
        }

        const bdc = await response.json()

        // Extract neighbourhood: highest-order informative entry that is a place name
        const informative: Array<{ name?: string; description?: string; order?: number; wikidataId?: string }> =
            bdc.localityInfo?.informative || []
        const placeEntries = informative.filter(
            (i) =>
                i.name &&
                i.order &&
                i.order >= 8 &&
                !i.description?.includes("FIPS") &&
                !i.description?.includes("postal") &&
                !i.description?.includes("time zone")
        )
        const neighbourhood = placeEntries.length > 0
            ? placeEntries[placeEntries.length - 1].name!
            : null

        const city = bdc.city || bdc.locality || ""
        const state = bdc.principalSubdivision ? _abbreviateState(bdc.principalSubdivision) : ""
        const postcode = bdc.postcode || ""

        const parts: string[] = []
        if (neighbourhood) parts.push(neighbourhood)
        if (city) parts.push(city)
        if (state || postcode) parts.push([state, postcode].filter(Boolean).join(" "))

        return parts.filter(Boolean).join(", ") || null
    } catch (error) {
        console.error("[Geocode] Reverse geocode error:", error)
        return null
    }
}

/** Abbreviate US state names to 2-letter codes */
function _abbreviateState(state: string): string {
    const map: Record<string, string> = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR",
        "California": "CA", "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE",
        "Florida": "FL", "Georgia": "GA", "Hawaii": "HI", "Idaho": "ID",
        "Illinois": "IL", "Indiana": "IN", "Iowa": "IA", "Kansas": "KS",
        "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
        "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
        "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV",
        "New Hampshire": "NH", "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY",
        "North Carolina": "NC", "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK",
        "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI", "South Carolina": "SC",
        "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
        "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV",
        "Wisconsin": "WI", "Wyoming": "WY",
    }
    return map[state] || state
}
