"use client"

import React, { useEffect, useRef, useState, useCallback, useMemo } from "react"
import { createPortal } from "react-dom"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"
import type { FilterState, MapState, FanChartData } from "@/lib/types"
import { cn } from "@/lib/utils"
import { useRouter, useSearchParams } from "next/navigation"
import { HomecastrLogo } from "@/components/homecastr-logo"
import { Bot } from "lucide-react"
import { FanChart } from "@/components/fan-chart"
import { StreetViewCarousel } from "@/components/street-view-carousel"
import { useKeyboardOpen } from "@/hooks/use-keyboard-open"

// Tooltip positioning constants
const SIDEBAR_WIDTH = 340
const TOOLTIP_WIDTH = 320
const TOOLTIP_HEIGHT = 620

// Geography level definitions — zoom breakpoints must match the SQL router
const GEO_LEVELS = [
    { name: "zcta", minzoom: 0, maxzoom: 7.99, label: "ZIP Code" },
    { name: "tract", minzoom: 8, maxzoom: 11.99, label: "Tract" },
    { name: "tabblock", minzoom: 12, maxzoom: 16.99, label: "Block" },
    { name: "parcel", minzoom: 17, maxzoom: 22, label: "Parcel" },
] as const

function getSmartTooltipPos(x: number, y: number, windowWidth: number, windowHeight: number) {
    if (typeof window === "undefined") return { x, y }

    let left = x + 20
    let top = y - 20

    if (left + TOOLTIP_WIDTH > windowWidth - 20) {
        const tryLeft = x - TOOLTIP_WIDTH - 20
        if (tryLeft < SIDEBAR_WIDTH + 10) {
            const spaceRight = windowWidth - x
            const spaceLeft = x - SIDEBAR_WIDTH
            if (spaceRight > spaceLeft && spaceRight > TOOLTIP_WIDTH) {
                left = x + 20
            } else if (spaceLeft > TOOLTIP_WIDTH) {
                left = tryLeft
            } else {
                left = Math.min(left, windowWidth - TOOLTIP_WIDTH - 10)
            }
        } else {
            left = tryLeft
        }
    }

    top = Math.max(10, Math.min(top, windowHeight - TOOLTIP_HEIGHT - 10))
    left = Math.max(10, left)

    return { x: left, y: top }
}

// Format currency values
function formatValue(v: number | null | undefined): string {
    if (v == null) return "N/A"
    return "$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 })
}

// Get label for current zoom
function getLevelLabel(zoom: number): string {
    for (const lvl of GEO_LEVELS) {
        if (zoom >= lvl.minzoom && zoom <= lvl.maxzoom) return lvl.label
    }
    return "Parcel"
}

// Get source-layer name for current zoom
function getSourceLayer(zoom: number): string {
    for (const lvl of GEO_LEVELS) {
        if (zoom >= lvl.minzoom && zoom <= lvl.maxzoom) return lvl.name
    }
    return "parcel"
}

// Build all fill layer IDs for querying
function getAllFillLayerIds(suffix: string): string[] {
    return GEO_LEVELS.map((lvl) => `forecast-fill-${lvl.name}-${suffix}`)
}

interface ForecastMapProps {
    filters: FilterState
    mapState: MapState
    onFeatureSelect: (id: string | null) => void
    onFeatureHover: (id: string | null) => void
    onCoordsChange?: (coords: [number, number] | null) => void
    year: number
    className?: string
    onConsultAI?: (details: {
        predictedValue: number | null
        opportunityScore: number | null
        capRate: number | null
    }) => void
    predictedValue?: number | null
    opportunityScore?: number | null
    capRate?: number | null
    isChatOpen?: boolean
}

export function ForecastMap({
    filters,
    mapState,
    year,
    onFeatureSelect,
    onFeatureHover,
    onCoordsChange,
    className,
    onConsultAI,
    isChatOpen = false,
}: ForecastMapProps) {
    const mapContainerRef = useRef<HTMLDivElement>(null)
    const mapRef = useRef<maplibregl.Map | null>(null)
    const [isLoaded, setIsLoaded] = useState(false)

    const router = useRouter()
    const searchParams = useSearchParams()

    // Tooltip state
    const [tooltipData, setTooltipData] = useState<{
        globalX: number
        globalY: number
        properties: any
    } | null>(null)
    const [fixedTooltipPos, setFixedTooltipPos] = useState<{
        globalX: number
        globalY: number
    } | null>(null)
    const [selectedId, setSelectedId] = useState<string | null>(null)
    const selectedIdRef = useRef<string | null>(null)
    const hoveredIdRef = useRef<string | null>(null)

    // Geographic coordinates for StreetView (from MapLibre events)
    const [tooltipCoords, setTooltipCoords] = useState<[number, number] | null>(null)
    // Coordinates locked to the selected area (for StreetView — doesn't follow hover)
    const [selectedCoords, setSelectedCoords] = useState<[number, number] | null>(null)

    // Notify parent of coordinate changes (for search bar geocoding)
    useEffect(() => { onCoordsChange?.(selectedCoords) }, [selectedCoords, onCoordsChange])

    // Mobile detection
    const [isMobile, setIsMobile] = useState(false)
    useEffect(() => {
        const check = () => setIsMobile(window.innerWidth < 768)
        check()
        window.addEventListener('resize', check)
        return () => window.removeEventListener('resize', check)
    }, [])

    const isKeyboardOpen = useKeyboardOpen()

    // Mobile swipe-to-minimize state
    const [mobileMinimized, setMobileMinimized] = useState(false)
    const [swipeTouchStart, setSwipeTouchStart] = useState<number | null>(null)
    const [swipeDragOffset, setSwipeDragOffset] = useState(0)

    // Desktop drag-to-reposition state (locked tooltip)
    const dragRef = useRef<{ startX: number; startY: number; origX: number; origY: number } | null>(null)
    const userDraggedRef = useRef(false) // true once user has manually repositioned

    useEffect(() => {
        const onMouseMove = (e: MouseEvent) => {
            if (!dragRef.current) return
            const dx = e.clientX - dragRef.current.startX
            const dy = e.clientY - dragRef.current.startY
            setFixedTooltipPos({ globalX: dragRef.current.origX + dx, globalY: dragRef.current.origY + dy })
            userDraggedRef.current = true // user chose this position
        }
        const onMouseUp = () => { dragRef.current = null }
        window.addEventListener('mousemove', onMouseMove)
        window.addEventListener('mouseup', onMouseUp)
        return () => { window.removeEventListener('mousemove', onMouseMove); window.removeEventListener('mouseup', onMouseUp) }
    }, [])

    // Reset minimize when selection changes
    useEffect(() => {
        if (selectedId) setMobileMinimized(false)
    }, [selectedId])

    // Reverse geocode when selection changes — adapt to geography scale
    useEffect(() => {
        if (!selectedId || !selectedCoords) {
            setGeocodedName(null)
            return
        }
        const map = mapRef.current
        const zoom = map?.getZoom() || 10
        const geoLevel = getSourceLayer(zoom)

        // At ZIP Code scale, just show the ZIP code from the feature ID
        if (geoLevel === "zcta") {
            // ZCTA5 IDs are 5-digit ZIP codes
            const zip = selectedId?.length === 5 ? selectedId : selectedId?.slice(-5)
            setGeocodedName(`ZIP ${zip}`)
            return
        }

        const cacheKey = `${geoLevel}:${selectedCoords[0].toFixed(4)},${selectedCoords[1].toFixed(4)}`
        if (geocodeCacheRef.current[cacheKey]) {
            setGeocodedName(geocodeCacheRef.current[cacheKey])
            return
        }
        setGeocodedName(null) // Show loading
        const [lat, lng] = selectedCoords
        // Always request max detail from Nominatim (zoom=16) so we get
        // suburb/neighbourhood/road data. We pick the right field below.
        fetch(`https://nominatim.openstreetmap.org/reverse?lat=${lat}&lon=${lng}&zoom=16&format=json`, {
            headers: { 'User-Agent': 'HomecastrUI/1.0' }
        })
            .then(r => r.ok ? r.json() : null)
            .then(data => {
                if (!data) return
                const addr = data.address || {}
                let name: string | null = null
                if (geoLevel === "tract") {
                    // Tract: show suburb/neighborhood
                    name = addr.suburb || addr.neighbourhood || addr.city_district || null
                } else {
                    // Block/Parcel: show street/road
                    name = addr.road || addr.suburb || addr.neighbourhood || data.display_name?.split(',')[0] || null
                }
                if (name) {
                    geocodeCacheRef.current[cacheKey] = name
                    setGeocodedName(name)
                }
            })
            .catch(() => { })
    }, [selectedId, selectedCoords])

    // Fan chart detail state
    const [fanChartData, setFanChartData] = useState<FanChartData | null>(null)
    const [historicalValues, setHistoricalValues] = useState<number[] | undefined>(undefined)
    const [isLoadingDetail, setIsLoadingDetail] = useState(false)
    const detailFetchRef = useRef<string | null>(null)
    // LRU cache for forecast detail responses (key: "level:featureId", value: {fanChart, historicalValues})
    const detailCacheRef = useRef<Map<string, { fanChart: FanChartData | null; historicalValues: number[] | undefined }>>(new Map())
    const DETAIL_CACHE_MAX = 1000

    // Selected feature's properties (locked when clicked)
    const [selectedProps, setSelectedProps] = useState<any>(null)

    // Reverse geocoded name for tooltip header
    const [geocodedName, setGeocodedName] = useState<string | null>(null)
    const geocodeCacheRef = useRef<Record<string, string>>({})

    // Comparison state: hover overlay when a feature is selected
    const [comparisonData, setComparisonData] = useState<FanChartData | null>(null)
    const [comparisonHistoricalValues, setComparisonHistoricalValues] = useState<number[] | undefined>(undefined)
    const comparisonFetchRef = useRef<string | null>(null)
    const comparisonTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)
    const hoverDetailTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

    // Viewport y-axis domain: fixed range from visible features
    const [viewportYDomain, setViewportYDomain] = useState<[number, number] | null>(null)

    // Shift-to-freeze comparison
    const [isShiftHeld, setIsShiftHeld] = useState(false)
    useEffect(() => {
        const down = (e: KeyboardEvent) => { if (e.key === 'Shift') setIsShiftHeld(true) }
        const up = (e: KeyboardEvent) => { if (e.key === 'Shift') setIsShiftHeld(false) }
        window.addEventListener('keydown', down)
        window.addEventListener('keyup', up)
        return () => { window.removeEventListener('keydown', down); window.removeEventListener('keyup', up) }
    }, [])

    // origin_year is always 2025, horizon_m is (year - 2025) * 12
    // Negative horizon_m = past years (historical), positive = future (forecast)
    // year 2025 (origin) → horizon_m=0, year 2026 → 12, year 2020 → -60
    const originYear = 2025
    const horizonM = (year - originYear) * 12

    // Fetch all horizons for a given feature to build FanChart data
    const fetchForecastDetail = useCallback(async (featureId: string, level: string) => {
        const cacheKey = `${level}:${featureId}`
        // Check cache first — instant re-hover
        const cached = detailCacheRef.current.get(cacheKey)
        if (cached) {
            // Move to end for LRU freshness
            detailCacheRef.current.delete(cacheKey)
            detailCacheRef.current.set(cacheKey, cached)
            setFanChartData(cached.fanChart)
            setHistoricalValues(cached.historicalValues)
            detailFetchRef.current = cacheKey
            return
        }
        if (detailFetchRef.current === cacheKey) return // already fetching
        detailFetchRef.current = cacheKey
        setIsLoadingDetail(true)
        try {
            const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(featureId)}&originYear=${originYear}`)
            if (!res.ok) throw new Error(`HTTP ${res.status}`)
            const json = await res.json()
            const fanChart = json.years?.length > 0 ? (json as FanChartData) : null
            const histVals = json.historicalValues?.some((v: any) => v != null) ? json.historicalValues : undefined
            setFanChartData(fanChart)
            setHistoricalValues(histVals)
            // Store in cache with LRU eviction
            detailCacheRef.current.set(cacheKey, { fanChart, historicalValues: histVals })
            if (detailCacheRef.current.size > DETAIL_CACHE_MAX) {
                // Delete oldest entry (first key in Map iteration order)
                const oldest = detailCacheRef.current.keys().next().value
                if (oldest) detailCacheRef.current.delete(oldest)
            }
        } catch (err) {
            console.error('[FORECAST-DETAIL] fetch error:', err)
            setFanChartData(null)
        } finally {
            setIsLoadingDetail(false)
        }
    }, [originYear])

    // VIEW SYNC: Update URL when map moves
    useEffect(() => {
        if (!mapRef.current) return
        const map = mapRef.current

        const onMoveEnd = () => {
            const center = map.getCenter()
            const zoom = map.getZoom()
            const params = new URLSearchParams(searchParams.toString())
            params.set("lat", center.lat.toFixed(5))
            params.set("lng", center.lng.toFixed(5))
            params.set("zoom", zoom.toFixed(2))
            router.replace(`?${params.toString()}`, { scroll: false })
        }

        map.on("moveend", onMoveEnd)
        return () => {
            map.off("moveend", onMoveEnd)
        }
    }, [isLoaded, searchParams, router])



    // Color ramp: growth mode uses growth_pct (% change from baseline).
    // Breakpoints are fitted from HISTORICAL parcel-level percentiles:
    //   p05 (deep blue) | p25 (light blue) | median (neutral) | p75 (amber) | p95 (red)
    //   1yr: -9 | 0 | 2.5 | 10 | 28
    //   3yr: -18 | 0 | 14.5 | 33 | 88
    //   5yr: -22 | 1 | 27 | 51 | 163
    // Ramp is asymmetric because the underlying distribution is right-skewed.
    // Value mode uses absolute p50 with fixed percentile breakpoints.
    const buildFillColor = (colorMode?: string): any => {
        if (colorMode === "growth") {
            const presentYear = originYear + 1  // 2026
            if (year === presentYear) return "#e5e5e5" // Present year: growth=0 → flat neutral
            const yrsFromPresent = Math.max(Math.abs(year - presentYear), 1)

            // Empirical percentile fits from Houston parcel history
            const p05 = -5 - 4 * yrsFromPresent   // 1yr≈-9, 3yr≈-17, 5yr≈-25
            const p25 = 0                           // ~0% across all horizons
            const med = 5 * yrsFromPresent          // 1yr≈5, 3yr≈15, 5yr≈25
            const p75 = 10 * yrsFromPresent         // 1yr≈10, 3yr≈30, 5yr≈50
            const p95 = 30 * yrsFromPresent         // 1yr≈30, 3yr≈90, 5yr≈150
            return [
                "interpolate",
                ["linear"],
                ["coalesce", ["to-number", ["get", "growth_pct"], 0], 0],
                p05, "#3b82f6",    // p5  — rare decline → deep blue
                p25, "#93c5fd",    // p25 — below average → light blue
                med, "#f8f8f8",    // p50 — median expected → neutral white
                p75, "#f59e0b",    // p75 — above average → amber
                p95, "#ef4444",    // p95 — rare hot growth → deep red
            ]
        }
        return [
            "interpolate",
            ["linear"],
            ["coalesce", ["get", "p50"], ["get", "value"], 0],
            150000, "#1e1b4b",   // p5
            235000, "#4c1d95",   // p25
            335000, "#7c3aed",   // p50
            525000, "#db2777",   // p75
            1000000, "#fbbf24",  // p95
        ]
    }

    // INITIALIZE MAP
    useEffect(() => {
        if (!mapContainerRef.current) return

        const urlParams = new URLSearchParams(window.location.search)
        const initialLat = parseFloat(urlParams.get("lat") || "29.76")
        const initialLng = parseFloat(urlParams.get("lng") || "-95.37")
        const initialZoom = parseFloat(urlParams.get("zoom") || "10")

        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            style: {
                version: 8,
                sources: {
                    osm: {
                        type: "raster",
                        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
                        tileSize: 256,
                        attribution: "&copy; OpenStreetMap contributors",
                    },
                },
                layers: [
                    {
                        id: "osm-layer",
                        type: "raster",
                        source: "osm",
                    },
                ],
            },
            center: [initialLng, initialLat],
            zoom: initialZoom,
            maxZoom: 18,
            minZoom: 2,
        })

        map.on("load", () => {
            setIsLoaded(true)

            const fillColor = buildFillColor()

            // Add A/B sources
            const addSource = (id: string) => {
                map.addSource(id, {
                    type: "vector",
                    tiles: [
                        `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}?originYear=${originYear}&horizonM=${horizonM}&v=2`,
                        // v=2 cache-buster to force fresh tiles after SQL function updates
                    ],
                    minzoom: 0,
                    maxzoom: 18,
                    promoteId: "id",
                })
            }

            addSource("forecast-a")
            addSource("forecast-b")

            // For each A/B source, create fill + outline layers for EACH geography level
            // with proper minzoom/maxzoom so MapLibre automatically shows the right one
            const addLayersForSource = (sourceId: string, suffix: string, visible: boolean) => {
                for (const lvl of GEO_LEVELS) {
                    // Fill layer
                    map.addLayer({
                        id: `forecast-fill-${lvl.name}-${suffix}`,
                        type: "fill",
                        source: sourceId,
                        "source-layer": lvl.name,
                        minzoom: lvl.minzoom,
                        maxzoom: lvl.maxzoom + 0.01,
                        layout: { visibility: visible ? "visible" : "none" },
                        paint: {
                            "fill-color": fillColor,
                            "fill-opacity": 0.55,
                            "fill-outline-color": "rgba(255,255,255,0.2)",
                        },
                    })

                    // Outline layer for hover/selected
                    map.addLayer({
                        id: `forecast-outline-${lvl.name}-${suffix}`,
                        type: "line",
                        source: sourceId,
                        "source-layer": lvl.name,
                        minzoom: lvl.minzoom,
                        maxzoom: lvl.maxzoom + 0.01,
                        layout: { visibility: visible ? "visible" : "none" },
                        paint: {
                            "line-color": [
                                "case",
                                ["boolean", ["feature-state", "selected"], false],
                                "#fbbf24",
                                ["boolean", ["feature-state", "hover"], false],
                                "#ffffff",
                                "rgba(0,0,0,0)",
                            ],
                            "line-width": [
                                "case",
                                ["boolean", ["feature-state", "selected"], false],
                                3,
                                ["boolean", ["feature-state", "hover"], false],
                                2,
                                0,
                            ],
                            "line-opacity": [
                                "case",
                                [
                                    "any",
                                    ["boolean", ["feature-state", "selected"], false],
                                    ["boolean", ["feature-state", "hover"], false],
                                ],
                                1,
                                0,
                            ],
                        },
                    })
                }
            }

            addLayersForSource("forecast-a", "a", true)
            addLayersForSource("forecast-b", "b", false)
        })

        // Suppress MapLibre tile loading error events (e.g. transient 500s from Supabase)
        map.on("error", (e: any) => {
            if (e?.error?.message?.includes("status") || e?.error?.message?.includes("AJAXError")) {
                // Silently ignore tile fetch errors — the retry + empty tile fallback handles these
                return
            }
            console.error("[MapLibre] Error:", e?.error?.message || e)
        })

        // HOVER handling
        map.on("mousemove", (e: maplibregl.MapMouseEvent) => {
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)

            // Query fill layers for active suffix
            const activeSuffix = (map as any)._activeSuffix || "a"
            const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`

            const features = map.getLayer(fillLayerId)
                ? map.queryRenderedFeatures(e.point, { layers: [fillLayerId] })
                : []

            if (features.length === 0) {
                // Clear hover
                if (hoveredIdRef.current) {
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.removeFeatureState({ source: s, sourceLayer })
                        } catch (err) {
                            /* ignore */
                        }
                    })
                    hoveredIdRef.current = null
                    if (!selectedIdRef.current) {
                        setTooltipData(null)
                    }
                    onFeatureHover(null)
                }
                map.getCanvas().style.cursor = ""
                return
            }

            map.getCanvas().style.cursor = "pointer"
            const feature = features[0]
            const id = (feature.properties?.id || feature.id) as string
            if (!id) return

            // Clear previous hover
            const isNewFeature = hoveredIdRef.current !== id
            if (hoveredIdRef.current && isNewFeature) {
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id: hoveredIdRef.current! },
                            { hover: false }
                        )
                    } catch (err) {
                        /* ignore */
                    }
                })
            }

            hoveredIdRef.current = id
            onFeatureHover(id)

                // Set hover state
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id },
                            { hover: true }
                        )
                    } catch (err) {
                        /* ignore */
                    }
                })


            // Debounced fan chart fetch on hover (only when NOT locked, only on new feature)
            if (!selectedIdRef.current && isNewFeature) {
                if (hoverDetailTimerRef.current) clearTimeout(hoverDetailTimerRef.current)
                hoverDetailTimerRef.current = setTimeout(() => {
                    const hoverZoom = map.getZoom()
                    const hoverLevel = getSourceLayer(hoverZoom)
                    fetchForecastDetail(id, hoverLevel)
                }, 500)
            }

            if (selectedIdRef.current) {
                // Locked mode: DON'T move tooltip (it stays pinned), but DO update
                // tooltipData.properties with the hovered feature so comparison works.
                setTooltipData(prev => prev ? { ...prev, properties: feature.properties } : prev)
                return
            }

            // Unlocked: tooltip follows cursor
            const smartPos = getSmartTooltipPos(
                e.originalEvent.clientX,
                e.originalEvent.clientY,
                window.innerWidth,
                window.innerHeight
            )
            setTooltipData({
                globalX: smartPos.x,
                globalY: smartPos.y,
                properties: feature.properties,
            })
            // Only update coords (used for StreetView) when the feature changes, not every pixel
            if (isNewFeature) {
                setTooltipCoords([e.lngLat.lat, e.lngLat.lng])
            }
        })

        // MOBILE LONG-PRESS HOVER: simulate hover/comparison on touch hold
        let longPressTimer: ReturnType<typeof setTimeout> | null = null
        let longPressActive = false
        let touchStartPos: { x: number; y: number } | null = null

        const simulateHoverAtPoint = (clientX: number, clientY: number) => {
            if (!selectedIdRef.current) return
            const point = map.project(map.unproject([clientX - map.getCanvas().getBoundingClientRect().left, clientY - map.getCanvas().getBoundingClientRect().top]))
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)
            const activeSuffix = (map as any)._activeSuffix || "a"
            const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
            const features = map.getLayer(fillLayerId)
                ? map.queryRenderedFeatures(point, { layers: [fillLayerId] })
                : []
            if (features.length === 0) return
            const feature = features[0]
            const id = (feature.properties?.id || feature.id) as string
            if (!id || id === selectedIdRef.current) return
            // Update hover state for comparison
            hoveredIdRef.current = id
            onFeatureHover(id)
            setTooltipData(prev => prev ? { ...prev, properties: feature.properties } : prev)
        }

        map.getCanvas().addEventListener("touchstart", (e: TouchEvent) => {
            if (!selectedIdRef.current) return
            const touch = e.touches[0]
            touchStartPos = { x: touch.clientX, y: touch.clientY }
            longPressActive = false
            longPressTimer = setTimeout(() => {
                longPressActive = true
                simulateHoverAtPoint(touch.clientX, touch.clientY)
            }, 400)
        }, { passive: true })

        map.getCanvas().addEventListener("touchmove", (e: TouchEvent) => {
            const touch = e.touches[0]
            if (touchStartPos) {
                const dx = touch.clientX - touchStartPos.x
                const dy = touch.clientY - touchStartPos.y
                if (Math.sqrt(dx * dx + dy * dy) > 10 && !longPressActive) {
                    // Moved too much before long-press activated — cancel
                    if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null }
                    return
                }
            }
            if (longPressActive) {
                e.preventDefault()
                simulateHoverAtPoint(touch.clientX, touch.clientY)
            }
        })

        map.getCanvas().addEventListener("touchend", () => {
            if (longPressTimer) { clearTimeout(longPressTimer); longPressTimer = null }
            longPressActive = false
            touchStartPos = null
        }, { passive: true })

        // MOUSELEAVE: clear tooltip when cursor exits the map (unless locked)
        map.getCanvas().addEventListener("mouseleave", () => {
            if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
            if (!selectedIdRef.current) {
                setTooltipData(null)
                detailFetchRef.current = null // allow re-fetch on re-hover
            }
            if (hoveredIdRef.current) {
                const zoom = map.getZoom()
                const sourceLayer = getSourceLayer(zoom)
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer, id: hoveredIdRef.current! },
                                { hover: false }
                            )
                        } catch { }
                    })
                hoveredIdRef.current = null
                onFeatureHover(null)
                // Clear comparison data when mouse leaves (locked mode)
                if (selectedIdRef.current) {
                    setComparisonData(null)
                    setComparisonHistoricalValues(undefined)
                    comparisonFetchRef.current = null
                }
            }
            map.getCanvas().style.cursor = ""
        })

        // CLICK handling
        map.on("click", (e: maplibregl.MapMouseEvent) => {
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)
            const activeSuffix = (map as any)._activeSuffix || "a"
            const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`

            const features = map.getLayer(fillLayerId)
                ? map.queryRenderedFeatures(e.point, { layers: [fillLayerId] })
                : []

            // Clear hover detail timer so click fetch takes precedence
            if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
            detailFetchRef.current = null // allow click to re-fetch even if same id

            if (features.length === 0) {
                // Clear selection
                if (selectedIdRef.current) {
                    ;["forecast-a", "forecast-b"].forEach((s) => {
                        try {
                            map.removeFeatureState({ source: s, sourceLayer })
                        } catch (err) {
                            /* ignore */
                        }
                    })
                    selectedIdRef.current = null
                    setSelectedId(null)
                    setSelectedProps(null)
                    setFixedTooltipPos(null)
                    userDraggedRef.current = false // reset for next lock
                    setSelectedCoords(null)
                    setComparisonData(null)
                    setComparisonHistoricalValues(undefined)
                    comparisonFetchRef.current = null
                    onFeatureSelect(null)
                }
                return
            }

            const feature = features[0]
            const id = (feature.properties?.id || feature.id) as string
            if (!id) return

            // Clear prev selection
            if (selectedIdRef.current) {
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id: selectedIdRef.current! },
                            { selected: false }
                        )
                    } catch (err) {
                        /* ignore */
                    }
                })
            }

            // Toggle selection
            if (selectedIdRef.current === id) {
                selectedIdRef.current = null
                setSelectedId(null)
                setSelectedProps(null)
                setFixedTooltipPos(null)
                setSelectedCoords(null)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                onFeatureSelect(null)
                return
            }

            selectedIdRef.current = id
            setSelectedId(id)
            setSelectedProps(feature.properties)
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            onFeatureSelect(id)

            // Fetch fan chart detail for newly selected area (critical on mobile where hover doesn't fire)
            const clickLevel = getSourceLayer(zoom)
            fetchForecastDetail(id, clickLevel)

                // Set selected state
                ;["forecast-a", "forecast-b"].forEach((s) => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer, id },
                            { selected: true }
                        )
                    } catch (err) {
                        /* ignore */
                    }
                })

            // Fix tooltip position
            const smartPos = getSmartTooltipPos(
                e.originalEvent.clientX,
                e.originalEvent.clientY,
                window.innerWidth,
                window.innerHeight
            )
            // If user has manually dragged the tooltip, keep it at that position.
            // Otherwise, position it near the new click.
            if (!userDraggedRef.current) {
                setFixedTooltipPos({ globalX: smartPos.x, globalY: smartPos.y })
            }
            setTooltipData({
                globalX: smartPos.x,
                globalY: smartPos.y,
                properties: feature.properties,
            })
            setTooltipCoords([e.lngLat.lat, e.lngLat.lng])
            setSelectedCoords([e.lngLat.lat, e.lngLat.lng])
        })

            // Store refs
            ; (map as any)._activeSuffix = "a"
            ; (map as any)._isLoaded = true
        mapRef.current = map

        return () => {
            map.remove()
        }
    }, []) // Init once

    // ESC key handler to clear selection
    useEffect(() => {
        const handleKeyDown = (e: KeyboardEvent) => {
            if (e.key === "Escape" && selectedIdRef.current) {
                const map = mapRef.current
                if (map) {
                    const zoom = map.getZoom()
                    const sourceLayer = getSourceLayer(zoom)
                        ;["forecast-a", "forecast-b"].forEach((s) => {
                            try {
                                map.setFeatureState(
                                    { source: s, sourceLayer, id: selectedIdRef.current! },
                                    { selected: false }
                                )
                            } catch (err) { /* ignore */ }
                        })
                }
                if (hoverDetailTimerRef.current) { clearTimeout(hoverDetailTimerRef.current); hoverDetailTimerRef.current = null }
                selectedIdRef.current = null
                setSelectedId(null)
                setSelectedProps(null)
                setFixedTooltipPos(null)
                setFanChartData(null)
                setHistoricalValues(undefined)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                detailFetchRef.current = null
                onFeatureSelect(null)
            }
        }
        window.addEventListener("keydown", handleKeyDown)
        return () => window.removeEventListener("keydown", handleKeyDown)
    }, [onFeatureSelect])

    // Tavus map-action handler: clear_selection, fly_to_location
    useEffect(() => {
        const handleTavusAction = (e: Event) => {
            const { action, params } = (e as CustomEvent).detail || {}
            if (action === "clear_selection") {
                const map = mapRef.current
                if (map && selectedIdRef.current) {
                    const zoom = map.getZoom()
                    const sourceLayer = getSourceLayer(zoom)
                        ;["forecast-a", "forecast-b"].forEach((s) => {
                            try {
                                map.setFeatureState(
                                    { source: s, sourceLayer, id: selectedIdRef.current! },
                                    { selected: false }
                                )
                            } catch (err) { /* ignore */ }
                        })
                }
                selectedIdRef.current = null
                setSelectedId(null)
                setSelectedProps(null)
                setFixedTooltipPos(null)
                setSelectedCoords(null)
                setFanChartData(null)
                setHistoricalValues(undefined)
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
                comparisonFetchRef.current = null
                detailFetchRef.current = null
                onFeatureSelect(null)
            } else if (action === "fly_to_location" && params) {
                const map = mapRef.current
                if (map && params.lat && params.lng) {
                    const targetZoom = params.zoom || 14
                    map.flyTo({
                        center: [params.lng, params.lat],
                        zoom: targetZoom,
                        duration: 2000,
                    })
                    // After fly completes AND tiles load, auto-select the feature at center
                    map.once("idle", () => {
                        const zoom = map.getZoom()
                        const sourceLayer = getSourceLayer(zoom)
                        const activeSuffix = (map as any)._activeSuffix || "a"
                        const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
                        const center = map.project(map.getCenter())
                        const features = map.getLayer(fillLayerId)
                            ? map.queryRenderedFeatures(center, { layers: [fillLayerId] })
                            : []
                        if (features.length > 0) {
                            const feature = features[0]
                            const id = (feature.properties?.id || feature.id) as string
                            if (id) {
                                // Clear prev selection
                                if (selectedIdRef.current) {
                                    ;["forecast-a", "forecast-b"].forEach((s) => {
                                        try { map.setFeatureState({ source: s, sourceLayer, id: selectedIdRef.current! }, { selected: false }) } catch { }
                                    })
                                }
                                // Set new selection
                                selectedIdRef.current = id
                                setSelectedId(id)
                                setSelectedProps(feature.properties)
                                setSelectedCoords([params.lat, params.lng])
                                onFeatureSelect(id)
                                    ;["forecast-a", "forecast-b"].forEach((s) => {
                                        try { map.setFeatureState({ source: s, sourceLayer, id }, { selected: true }) } catch { }
                                    })
                                fetchForecastDetail(id, sourceLayer)
                            }
                        }
                    })
                }
            } else if (action === "location_to_area" || action === "add_location_to_selection" || action === "resolve_place" || action === "get_forecast_area") {
                // All these return lat/lng — fly to it and auto-select the center feature
                const result = (e as CustomEvent).detail?.result || params
                const lat = result?.chosen?.lat || result?.location?.lat || result?.area?.location?.lat || params?.lat
                const lng = result?.chosen?.lng || result?.location?.lng || result?.area?.location?.lng || params?.lng
                const map = mapRef.current
                if (map && lat && lng) {
                    map.flyTo({
                        center: [lng, lat],
                        zoom: Math.max(map.getZoom(), 13),
                        duration: 2000,
                    })
                    map.once("idle", () => {
                        const zoom = map.getZoom()
                        const sourceLayer = getSourceLayer(zoom)
                        const activeSuffix = (map as any)._activeSuffix || "a"
                        const fillLayerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
                        const center = map.project(map.getCenter())
                        const features = map.getLayer(fillLayerId)
                            ? map.queryRenderedFeatures(center, { layers: [fillLayerId] })
                            : []
                        if (features.length > 0) {
                            const feature = features[0]
                            const id = (feature.properties?.id || feature.id) as string
                            if (id) {
                                if (selectedIdRef.current) {
                                    ;["forecast-a", "forecast-b"].forEach((s) => {
                                        try { map.setFeatureState({ source: s, sourceLayer, id: selectedIdRef.current! }, { selected: false }) } catch { }
                                    })
                                }
                                selectedIdRef.current = id
                                setSelectedId(id)
                                setSelectedProps(feature.properties)
                                setSelectedCoords([lat, lng])
                                onFeatureSelect(id)
                                    ;["forecast-a", "forecast-b"].forEach((s) => {
                                        try { map.setFeatureState({ source: s, sourceLayer, id }, { selected: true }) } catch { }
                                    })
                                fetchForecastDetail(id, sourceLayer)
                            }
                        }
                    })
                }
            }
        }
        window.addEventListener("tavus-map-action", handleTavusAction)
        return () => window.removeEventListener("tavus-map-action", handleTavusAction)
    }, [onFeatureSelect])

    // UPDATE MODE — reactive fill-color paint
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current
        const newColor = buildFillColor(filters.mode)
        for (const lvl of GEO_LEVELS) {
            for (const suffix of ["a", "b"]) {
                const layerId = `forecast-fill-${lvl.name}-${suffix}`
                if (map.getLayer(layerId)) {
                    map.setPaintProperty(layerId, "fill-color", newColor)
                }
            }
        }
    }, [filters.mode, isLoaded])

    // UPDATE YEAR — Seamless A/B swap (same pattern as vector-map.tsx)
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const currentSuffix = (map as any)._activeSuffix || "a"
        const nextSuffix = currentSuffix === "a" ? "b" : "a"
        const nextSource = `forecast-${nextSuffix}`

        // Update NEXT source tiles
        const source = map.getSource(nextSource)
        if (source && source.type === "vector") {
            ; (source as any).setTiles([
                `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}?originYear=${originYear}&horizonM=${horizonM}&v=2`,
            ])
        }

        // Apply color logic to all fill layers
        const fillColor = buildFillColor(filters.colorMode)

        for (const lvl of GEO_LEVELS) {
            ;["a", "b"].forEach((s) => {
                const layerId = `forecast-fill-${lvl.name}-${s}`
                if (map.getLayer(layerId)) {
                    map.setPaintProperty(layerId, "fill-color", fillColor)
                }
            })
        }

        // Seamless swap
        let swapCompleted = false

        const performSwap = () => {
            if (swapCompleted) return
            swapCompleted = true

            const latestTarget = (map as any)._targetYear
            if (latestTarget !== year) return

            // Show next, hide current — for all geo levels
            if (!map.getStyle?.()) return // guard against hot-reload race
            for (const lvl of GEO_LEVELS) {
                const fillNext = `forecast-fill-${lvl.name}-${nextSuffix}`
                const outlineNext = `forecast-outline-${lvl.name}-${nextSuffix}`
                const fillCur = `forecast-fill-${lvl.name}-${currentSuffix}`
                const outlineCur = `forecast-outline-${lvl.name}-${currentSuffix}`

                if (map.getLayer(fillNext)) map.setLayoutProperty(fillNext, "visibility", "visible")
                if (map.getLayer(outlineNext)) map.setLayoutProperty(outlineNext, "visibility", "visible")
                if (map.getLayer(fillCur)) map.setLayoutProperty(fillCur, "visibility", "none")
                if (map.getLayer(outlineCur)) map.setLayoutProperty(outlineCur, "visibility", "none")
            }

            ; (map as any)._activeSuffix = nextSuffix
        }

        const onSourceData = (e: any) => {
            if (
                e.sourceId === nextSource &&
                map.isSourceLoaded(nextSource) &&
                e.isSourceLoaded
            ) {
                map.off("sourcedata", onSourceData)
                map.once("idle", () => {
                    requestAnimationFrame(() => {
                        requestAnimationFrame(performSwap)
                    })
                })
                setTimeout(performSwap, 600)
            }
        }

            ; (map as any)._targetYear = year
        map.on("sourcedata", onSourceData)
    }, [year, isLoaded, filters.colorMode, originYear, horizonM])

    // SYNC VIEWPORT
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        try {
            if (map.getStyle()) {
                const center = map.getCenter()
                const zoom = map.getZoom()
                if (
                    Math.abs(center.lng - mapState.center[0]) > 0.001 ||
                    Math.abs(center.lat - mapState.center[1]) > 0.001 ||
                    Math.abs(zoom - mapState.zoom) > 0.1
                ) {
                    map.flyTo({
                        center: [mapState.center[0], mapState.center[1]],
                        zoom: mapState.zoom,
                        speed: 0.8,
                        curve: 1.42,
                        easing: (t: number) =>
                            t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2,
                    })
                }
            }
        } catch (e) {
            /* map might be in transition */
        }
    }, [mapState.center, mapState.zoom, isLoaded])

    // Determine display tooltip info
    const displayPos = selectedId && fixedTooltipPos ? fixedTooltipPos : tooltipData
    // When locked, show the SELECTED feature's properties (not hover)
    const displayProps = selectedId && selectedProps ? selectedProps : tooltipData?.properties

    // Effective y-domain: extend viewport range ONLY when selected/hovered
    // feature's P50 median or historical values fall outside. P10/P90 uncertainty
    // bands are allowed to clip — they shouldn't drive the axis scale.
    const effectiveYDomain = useMemo<[number, number] | null>(() => {
        if (!viewportYDomain) return null
        const [lo, hi] = viewportYDomain
        // Only gather P50 (median) and historical — NOT P10/P90 extremes
        const vals: number[] = []
        if (fanChartData?.p50) vals.push(...fanChartData.p50.filter(v => Number.isFinite(v)))
        if (historicalValues) vals.push(...historicalValues.filter(v => Number.isFinite(v)))
        if (comparisonData?.p50) vals.push(...comparisonData.p50.filter(v => Number.isFinite(v)))
        if (comparisonHistoricalValues) vals.push(...comparisonHistoricalValues.filter(v => Number.isFinite(v)))
        if (vals.length === 0) return viewportYDomain
        const dataMin = Math.min(...vals)
        const dataMax = Math.max(...vals)
        // Only extend, never shrink from viewport range
        if (dataMin >= lo && dataMax <= hi) return viewportYDomain // no extension needed
        return [Math.min(lo, dataMin), Math.max(hi, dataMax)]
    }, [viewportYDomain, fanChartData, historicalValues, comparisonData, comparisonHistoricalValues])

    // Comparison: when locked, fetch comparison detail for hovered feature
    // Skip updates when Shift is held (freeze current comparison)
    useEffect(() => {
        if (isShiftHeld) return // freeze comparison
        if (!selectedId || !tooltipData?.properties) {
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            return
        }
        const hoveredId = tooltipData.properties.id as string
        if (!hoveredId || hoveredId === selectedId) {
            setComparisonData(null)
            setComparisonHistoricalValues(undefined)
            comparisonFetchRef.current = null
            return
        }
        // Debounce comparison fetch
        if (comparisonTimerRef.current) clearTimeout(comparisonTimerRef.current)
        comparisonTimerRef.current = setTimeout(async () => {
            const map = mapRef.current
            if (!map) return
            const zoom = map.getZoom()
            const level = getSourceLayer(zoom)
            const cacheKey = `${level}:${hoveredId}`
            if (comparisonFetchRef.current === cacheKey) return
            comparisonFetchRef.current = cacheKey
            // Check shared cache first
            const cached = detailCacheRef.current.get(cacheKey)
            if (cached) {
                detailCacheRef.current.delete(cacheKey)
                detailCacheRef.current.set(cacheKey, cached)
                setComparisonData(cached.fanChart)
                setComparisonHistoricalValues(cached.historicalValues)
                return
            }
            try {
                const res = await fetch(`/api/forecast-detail?level=${level}&id=${encodeURIComponent(hoveredId)}&originYear=${originYear}`)
                if (!res.ok) return
                const json = await res.json()
                const fanChart = json.years?.length > 0 ? (json as FanChartData) : null
                const histVals = json.historicalValues?.some((v: any) => v != null) ? json.historicalValues : undefined
                setComparisonData(fanChart)
                setComparisonHistoricalValues(histVals)
                // Store in shared cache
                detailCacheRef.current.set(cacheKey, { fanChart, historicalValues: histVals })
                if (detailCacheRef.current.size > DETAIL_CACHE_MAX) {
                    const oldest = detailCacheRef.current.keys().next().value
                    if (oldest) detailCacheRef.current.delete(oldest)
                }
            } catch {
                setComparisonData(null)
                setComparisonHistoricalValues(undefined)
            }
        }, 200)
        return () => {
            if (comparisonTimerRef.current) clearTimeout(comparisonTimerRef.current)
        }
    }, [selectedId, tooltipData?.properties?.id, originYear, isShiftHeld])

    // Viewport Y domain: compute from visible features on moveend + after initial tile load
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current
        const computeYDomain = () => {
            // Freeze y-range while tooltip is locked — prevents jumps on pan
            if (selectedIdRef.current) return false
            const zoom = map.getZoom()
            const sourceLayer = getSourceLayer(zoom)
            const activeSuffix = (map as any)._activeSuffix || 'a'
            const layerId = `forecast-fill-${sourceLayer}-${activeSuffix}`
            if (!map.getLayer(layerId)) return false
            const features = map.queryRenderedFeatures(undefined, { layers: [layerId] })
            if (features.length === 0) return false
            const allVals: number[] = []
            for (const f of features) {
                const p = f.properties
                if (p?.value != null && Number.isFinite(p.value)) allVals.push(p.value)
                if (p?.p10 != null && Number.isFinite(p.p10)) allVals.push(p.p10)
                if (p?.p90 != null && Number.isFinite(p.p90)) allVals.push(p.p90)
            }
            if (allVals.length < 2) return false
            allVals.sort((a, b) => a - b)
            // Use P10/P90 of visible values to exclude outliers
            const lo = allVals[Math.floor(allVals.length * 0.1)]
            const hi = allVals[Math.ceil(allVals.length * 0.9) - 1]
            if (lo < hi) {
                setViewportYDomain([lo, hi])
                return true
            }
            return false
        }

        map.on('moveend', computeYDomain)

        // Initial tile load: tiles may not be rendered when isLoaded fires.
        // Listen for 'idle' (fires after all tiles painted) to catch initial load.
        let initialDone = computeYDomain()
        if (!initialDone) {
            const onIdle = () => {
                if (computeYDomain()) {
                    map.off('idle', onIdle) // success — stop listening
                }
            }
            map.on('idle', onIdle)
            // Safety: remove after 10s to avoid leaking
            const safetyTimer = setTimeout(() => map.off('idle', onIdle), 10000)
            return () => {
                map.off('moveend', computeYDomain)
                map.off('idle', onIdle)
                clearTimeout(safetyTimer)
            }
        }

        return () => { map.off('moveend', computeYDomain) }
    }, [isLoaded])

    return (
        <div className={cn("relative w-full h-full", className)}>
            <div ref={mapContainerRef} className="w-full h-full" />

            {!isLoaded && (
                <div className="absolute inset-0 flex items-center justify-center bg-background/50 backdrop-blur-sm z-50">
                    <div className="flex flex-col items-center gap-2">
                        <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin" />
                        <span className="text-sm font-medium">
                            Initializing Forecast Engine...
                        </span>
                    </div>
                </div>
            )}

            {/* Forecast Tooltip — portal-based, responsive for mobile + desktop */}
            {isLoaded && displayPos && displayProps && createPortal(
                <div
                    className={cn(
                        "z-[9999] glass-panel shadow-2xl overflow-hidden",
                        isMobile
                            ? `fixed bottom-0 left-0 ${isChatOpen ? 'w-1/2' : 'right-0 w-full'} rounded-t-xl rounded-b-none border-t border-x-0 border-b-0 pointer-events-auto`
                            : "fixed rounded-xl w-[320px]",
                        !isMobile && (selectedId ? "pointer-events-auto cursor-move" : "pointer-events-none")
                    )}
                    style={isMobile ? {
                        transform: `translateY(calc(${mobileMinimized ? '100% - 24px' : '0px'} + ${swipeDragOffset}px))`,
                        transition: swipeTouchStart === null ? 'transform 0.3s ease-out' : 'none',
                        height: isChatOpen ? (isKeyboardOpen ? '170px' : '40vh') : undefined,
                        maxHeight: isKeyboardOpen ? '170px' : '40vh',
                        overflowY: 'hidden',
                    } : {
                        left: displayPos.globalX,
                        top: displayPos.globalY,
                        maxHeight: 'calc(100vh - 40px)',
                        overflowY: 'auto',
                    }}
                    onMouseDown={!isMobile && selectedId ? (e) => {
                        // Don't drag when clicking interactive elements
                        const tag = (e.target as HTMLElement)?.tagName?.toLowerCase()
                        if (tag === 'button' || tag === 'a' || tag === 'input' || tag === 'select') return
                        e.preventDefault()
                        if (fixedTooltipPos) {
                            dragRef.current = { startX: e.clientX, startY: e.clientY, origX: fixedTooltipPos.globalX, origY: fixedTooltipPos.globalY }
                        }
                    } : undefined}
                    onTouchStart={isMobile ? (e) => {
                        // Skip swipe tracking if touch is on the header bar
                        const target = e.target as HTMLElement
                        if (target.closest('[data-tooltip-header]')) return
                        setSwipeTouchStart(e.touches[0].clientY)
                    } : undefined}
                    onTouchMove={isMobile ? (e) => {
                        if (swipeTouchStart === null) return
                        const delta = e.touches[0].clientY - swipeTouchStart
                        if (mobileMinimized) { if (delta < 0) setSwipeDragOffset(delta) }
                        else { if (delta > 0) setSwipeDragOffset(delta) }
                    } : undefined}
                    onTouchEnd={isMobile ? () => {
                        if (mobileMinimized) {
                            if (swipeDragOffset < -50) setMobileMinimized(false)
                        } else {
                            if (swipeDragOffset > 150) {
                                // Dismiss completely
                                onFeatureSelect(null)
                            } else if (swipeDragOffset > 50) {
                                setMobileMinimized(true)
                            }
                        }
                        setSwipeDragOffset(0)
                        setSwipeTouchStart(null)
                    } : undefined}
                >
                    {/* Mobile Header with Close Button — hidden when keyboard open */}
                    {isMobile && !isKeyboardOpen && (
                        <div
                            className="w-full flex items-center justify-between px-3 h-9 bg-muted/40 backdrop-blur-md border-b border-border/50"
                            data-tooltip-header="true"
                        >
                            <div className="flex items-center gap-2">
                                <HomecastrLogo variant="horizontal" size={16} />
                                <span className="px-1.5 py-0.5 bg-violet-500/20 text-violet-400 text-[8px] font-semibold uppercase tracking-wider rounded">Forecast</span>
                            </div>
                            <button
                                onClick={() => onFeatureSelect(null)}
                                className="w-9 h-9 -mr-2 flex items-center justify-center rounded-full active:bg-muted/60 text-muted-foreground"
                                aria-label="Close tooltip"
                                style={{ touchAction: 'manipulation' }}
                            >
                                <svg width="14" height="14" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" style={{ pointerEvents: 'none' }}>
                                    <line x1="2" y1="2" x2="10" y2="10" /><line x1="10" y1="2" x2="2" y2="10" />
                                </svg>
                            </button>
                        </div>
                    )}

                    {/* Header - matching MapTooltip (hidden on mobile) */}
                    {!isMobile && (
                        <>
                            <div
                                className="flex items-center justify-between px-3 py-2 border-b border-border/50 bg-muted/40 backdrop-blur-md select-none"
                            >
                                <div className="flex items-center gap-2">
                                    <HomecastrLogo variant="horizontal" size={18} />
                                    <span className="px-1.5 py-0.5 bg-violet-500/20 text-violet-400 text-[8px] font-semibold uppercase tracking-wider rounded">Forecast</span>
                                    {selectedId && (
                                        <span className="px-1.5 py-0.5 bg-yellow-500/20 text-yellow-600 dark:text-yellow-400 text-[8px] font-semibold uppercase tracking-wider rounded">Locked</span>
                                    )}
                                </div>
                                {selectedId && <span className="text-[9px] text-muted-foreground">ESC to exit</span>}
                            </div>

                            {/* Subheader - geography level */}
                            <div className="p-3 border-b border-border/50 bg-muted/30">
                                <div className="flex justify-between items-start">
                                    <div className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">
                                        {getLevelLabel(mapRef.current?.getZoom() || 10)} Scale
                                    </div>
                                    <div className="text-[9px] px-1.5 py-0.5 bg-primary/10 text-primary rounded-full font-bold">
                                        {displayProps.n != null ? `${displayProps.n} Prop` : ""}
                                    </div>
                                </div>
                                <div className="font-semibold text-xs text-foreground truncate">
                                    {geocodedName || displayProps.id}
                                </div>
                                {geocodedName && !geocodedName.startsWith('ZIP') && (
                                    <div className="font-mono text-[9px] text-muted-foreground/60 truncate">
                                        {displayProps.id}
                                    </div>
                                )}
                            </div>
                        </>
                    )}

                    {/* Street View Carousel — hidden when keyboard is open on mobile */}
                    {!(isMobile && isKeyboardOpen) && process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY && (selectedId ? selectedCoords : tooltipCoords) && (
                        <StreetViewCarousel
                            h3Ids={[]}
                            apiKey={process.env.NEXT_PUBLIC_GOOGLE_MAPS_KEY}
                            coordinates={(selectedId ? selectedCoords : tooltipCoords)!}
                        />
                    )}

                    {/* Content Body */}
                    {isMobile ? (
                        /* Mobile Layout: Full-width chart with overlaid stat badges */
                        <div className="px-1 py-1 flex-1 min-h-0">
                            {(() => {
                                const currentVal = historicalValues?.[historicalValues.length - 1] ?? null
                                const forecastVal = displayProps.p50 ?? displayProps.value ?? null
                                const isPast = year < originYear + 1
                                const isPresent = year === originYear + 1 // 2026 = "now"
                                const leftLabel = isPresent ? "Now" : isPast ? String(year) : "Now"
                                const leftVal = isPresent ? currentVal : isPast ? forecastVal : currentVal
                                const rightLabel = isPresent ? String(year) : isPast ? "Now" : String(year)
                                const rightVal = isPresent ? currentVal : isPast ? currentVal : forecastVal
                                const pctBase = isPresent ? null : isPast ? forecastVal : currentVal
                                const pctTarget = isPresent ? null : isPast ? currentVal : forecastVal
                                const pctChange = pctBase && pctTarget ? ((pctTarget - pctBase) / pctBase * 100) : null
                                return (
                                    <div className="relative w-full h-full min-h-[160px]">
                                        {/* Full-width chart */}
                                        <div className="w-full h-full">
                                            {fanChartData ? (
                                                <FanChart data={fanChartData} currentYear={year} height={160} historicalValues={historicalValues} comparisonData={comparisonData} comparisonHistoricalValues={comparisonHistoricalValues} yDomain={effectiveYDomain} />
                                            ) : isLoadingDetail ? (
                                                <div className="h-full flex items-center justify-center">
                                                    <div className="w-4 h-4 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                                </div>
                                            ) : null}
                                        </div>
                                        {/* Overlaid stat badges */}
                                        <div className="absolute top-1 left-1 px-1.5 py-0.5 rounded bg-background/80 backdrop-blur-sm border border-border/30">
                                            <div className="text-[7px] uppercase tracking-wider text-muted-foreground font-semibold">{leftLabel}</div>
                                            <div className="text-[10px] font-bold text-foreground">{formatValue(leftVal)}</div>
                                        </div>
                                        {!isPresent && (
                                            <div className="absolute top-1 right-1 px-1.5 py-0.5 rounded bg-background/80 backdrop-blur-sm border border-border/30 text-right">
                                                <div className="text-[7px] uppercase tracking-wider text-muted-foreground font-semibold">{rightLabel}</div>
                                                <div className="text-[10px] font-bold text-foreground">{formatValue(rightVal)}</div>
                                                {pctChange != null && (
                                                    <div className={`text-[8px] font-bold ${pctChange >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                                        {pctChange >= 0 ? '▲' : '▼'} {Math.abs(pctChange).toFixed(1)}%
                                                    </div>
                                                )}
                                            </div>
                                        )}
                                    </div>
                                )
                            })()}
                        </div>
                    ) : (
                        /* Desktop Layout: Values above, full-width chart below */
                        <div className="p-4 space-y-3">
                            {/* Current → Forecast header with % change */}
                            {(() => {
                                const currentVal = historicalValues?.[historicalValues.length - 1] ?? null
                                const forecastVal = displayProps.p50 ?? displayProps.value ?? null
                                const isPast = year < originYear + 1
                                const isPresent = year === originYear + 1 // 2026 = "now"
                                const leftLabel = isPresent ? "Now" : isPast ? String(year) : "Now"
                                const leftVal = isPresent ? currentVal : isPast ? forecastVal : currentVal
                                const rightLabel = isPresent ? String(year) : isPast ? "Now" : String(year)
                                const rightVal = isPresent ? currentVal : isPast ? currentVal : forecastVal
                                const pctBase = isPresent ? null : isPast ? forecastVal : currentVal
                                const pctTarget = isPresent ? null : isPast ? currentVal : forecastVal
                                const pctChange = pctBase && pctTarget ? ((pctTarget - pctBase) / pctBase * 100) : null
                                return (
                                    <div className="flex items-center justify-between gap-3">
                                        <div className="text-center flex-1">
                                            <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">{leftLabel}</div>
                                            <div className="text-lg font-bold text-foreground tracking-tight">{formatValue(leftVal)}</div>
                                        </div>
                                        <div className="text-center shrink-0">
                                            {pctChange != null && (
                                                <div className={`text-sm font-bold ${pctChange >= 0 ? 'text-emerald-500' : 'text-red-500'}`}>
                                                    {pctChange >= 0 ? '▲' : '▼'} {Math.abs(pctChange).toFixed(1)}%
                                                </div>
                                            )}
                                            <div className="text-[9px] text-muted-foreground">→ {rightLabel}</div>
                                        </div>
                                        <div className="text-center flex-1">
                                            <div className="text-[9px] uppercase tracking-wider text-muted-foreground font-semibold mb-0.5">{rightLabel}</div>
                                            <div className="text-lg font-bold text-foreground tracking-tight">{formatValue(rightVal)}</div>
                                        </div>
                                    </div>
                                )
                            })()}

                            {/* Fan Chart full-width with P-values overlaid top-right */}
                            <div className="relative h-52 -mx-2">
                                {fanChartData ? (
                                    <FanChart
                                        data={fanChartData}
                                        currentYear={year}
                                        height={200}
                                        historicalValues={historicalValues}
                                        comparisonData={comparisonData}
                                        comparisonHistoricalValues={comparisonHistoricalValues}
                                        yDomain={effectiveYDomain}
                                    />
                                ) : isLoadingDetail ? (
                                    <div className="h-full flex items-center justify-center">
                                        <div className="w-5 h-5 border-2 border-primary/30 border-t-primary rounded-full animate-spin" />
                                    </div>
                                ) : null}

                                {/* P-values overlay — top-right corner */}
                                {(displayProps.p10 != null || displayProps.p90 != null) && (
                                    <div className="absolute top-5 right-4 text-[8px] leading-snug rounded px-1 py-0.5" style={{ textShadow: '0 0 3px var(--background), 0 0 3px var(--background)' }}>
                                        <div className="flex items-baseline gap-1">
                                            <span className="font-medium text-[9px]">{formatValue(displayProps.p90)}</span>
                                            <span className="text-muted-foreground/50">P90</span>
                                        </div>
                                        <div className="flex items-baseline gap-1">
                                            <span className="font-medium text-[9px]">{formatValue(displayProps.p75)}</span>
                                            <span className="text-muted-foreground/50">P75</span>
                                        </div>
                                        <div className="flex items-baseline gap-1 bg-primary/10 rounded px-0.5">
                                            <span className="font-bold text-[9px] text-primary">{formatValue(displayProps.p50 ?? displayProps.value)}</span>
                                            <span className="text-primary/70">P50</span>
                                        </div>
                                        <div className="flex items-baseline gap-1">
                                            <span className="font-medium text-[9px]">{formatValue(displayProps.p25)}</span>
                                            <span className="text-muted-foreground/50">P25</span>
                                        </div>
                                        <div className="flex items-baseline gap-1">
                                            <span className="font-medium text-[9px]">{formatValue(displayProps.p10)}</span>
                                            <span className="text-muted-foreground/50">P10</span>
                                        </div>
                                    </div>
                                )}
                            </div>

                            <div className="pt-1 mt-0 border-t border-border/50 text-center">
                                <div className="text-[9px] text-muted-foreground flex justify-center items-center gap-1.5">
                                    <Bot className="w-3 h-3 text-primary/50" />
                                    <span>AI Forecast</span>
                                </div>
                            </div>

                            {/* Talk to Homecastr button — only when selected */}
                            {selectedId && onConsultAI && (
                                <div className="pt-2 mt-1 border-t border-border/50">
                                    <button
                                        onClick={(e) => {
                                            e.stopPropagation()
                                            onConsultAI({
                                                predictedValue: displayProps.p50 ?? displayProps.value ?? null,
                                                opportunityScore: null,
                                                capRate: null,
                                            })
                                        }}
                                        className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-lg bg-primary/15 hover:bg-primary/25 border border-primary/30 text-primary text-xs font-semibold transition-all hover:scale-[1.02] active:scale-[0.98]"
                                    >
                                        <HomecastrLogo variant="horizontal" size={14} />
                                        <span>Talk to live agent</span>
                                    </button>
                                </div>
                            )}
                        </div>
                    )}
                </div>,
                document.body
            )}

            {/* Forecast mode badge */}
            <div className="absolute bottom-20 right-4 md:bottom-auto md:top-4 z-50">
                <div className="glass-panel rounded-lg px-3 py-1.5 flex items-center gap-2 shadow-lg border border-white/10">
                    <div className="w-2 h-2 rounded-full bg-violet-500 animate-pulse" />
                    <span className="text-[10px] font-bold uppercase tracking-wider text-violet-300">
                        Forecast Mode
                    </span>
                    <span className="text-[10px] font-mono text-muted-foreground">
                        {year}
                    </span>
                </div>
            </div>
        </div>
    )
}
