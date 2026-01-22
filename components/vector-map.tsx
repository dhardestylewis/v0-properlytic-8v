"use client"

import React, { useEffect, useRef, useState, useCallback, useMemo } from "react"
import { createPortal } from "react-dom"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"
import type { FilterState, MapState, DetailsResponse } from "@/lib/types"
import { getH3CellDetails } from "@/app/actions/h3-details"
import { getParcels } from "@/app/actions/parcels"
import { cn } from "@/lib/utils"
import { getOpportunityColor, getValueColor, formatOpportunity, formatCurrency, formatReliability } from "@/lib/utils/colors"
import { TrendingUp, TrendingDown, Minus, Building2 } from "lucide-react"
import { FanChart } from "./fan-chart"
import { cellToLatLng } from "h3-js"
import { MapTooltip } from "./map-tooltip"
import { useRouter, useSearchParams } from "next/navigation"

interface VectorMapProps {
    filters: FilterState
    mapState: MapState
    year: number
    onFeatureSelect: (id: string | null) => void
    onFeatureHover: (id: string | null) => void
    className?: string
}

export function VectorMap({
    filters,
    mapState,
    year,
    onFeatureSelect,
    onFeatureHover,
    className
}: VectorMapProps) {
    const mapContainerRef = useRef<HTMLDivElement>(null)
    const mapRef = useRef<maplibregl.Map | null>(null)
    const [isLoaded, setIsLoaded] = useState(false)

    // INTERACTION STATE (Parity with MapView)
    const [hoveredHex, setHoveredHex] = useState<string | null>(null)
    const [selectedHexes, setSelectedHexes] = useState<string[]>([])
    const [selectionDetails, setSelectionDetails] = useState<DetailsResponse | null>(null)
    const [hoveredDetails, setHoveredDetails] = useState<DetailsResponse | null>(null)
    const [comparisonHex, setComparisonHex] = useState<string | null>(null)
    const [isShiftHeld, setIsShiftHeld] = useState(false)
    const [isLoadingDetails, setIsLoadingDetails] = useState(false)
    const [isMobile, setIsMobile] = useState(false)

    // ADVANCED TOOLTIP STATE (Full Parity with MapView)
    const [primaryDetails, setPrimaryDetails] = useState<DetailsResponse | null>(null)
    const [comparisonDetails, setComparisonDetailsState] = useState<DetailsResponse | null>(null)
    // Note: previewDetails is computed via useMemo below, removed useState duplicate
    // Note: year comes from props, not local state

    const router = useRouter()
    const searchParams = useSearchParams()

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
        return () => { map.off("moveend", onMoveEnd) }
    }, [isLoaded, searchParams, router])

    // TRACK MOBILE
    useEffect(() => {
        const check = () => setIsMobile(window.innerWidth < 768)
        check()
        window.addEventListener('resize', check)
        return () => window.removeEventListener('resize', check)
    }, [])

    // DATA CACHE & REFS (Avoid stale closures and trails)
    const detailsCache = useRef<Map<string, DetailsResponse>>(new Map())
    const hoveredHexRef = useRef<string | null>(null)
    const activeHoverIds = useRef<Set<string>>(new Set()) // Track all for sweep
    const selectedHexesRef = useRef<string[]>([])
    const [mousePos, setMousePos] = useState<{ x: number, y: number } | null>(null)
    const [tooltipProps, setTooltipProps] = useState<any>(null)

    // HELPERS (Parity with MapView)
    const aggregateDetails = (list: DetailsResponse[]): DetailsResponse => {
        if (list.length === 0) throw new Error("Empty list")
        const primary = list[0]
        return {
            ...primary,
            metrics: {
                ...primary.metrics,
                n_accts: list.reduce((sum, d) => sum + (d.metrics?.n_accts || 0), 0)
            },
            proforma: {
                predicted_value: list.reduce((sum, d) => sum + (d.proforma?.predicted_value || 0), 0) / list.length,
                noi: null,
                monthly_rent: null,
                dscr: null,
                breakeven_occ: null,
                cap_rate: null,
                liquidity_rank: null
            },
            opportunity: {
                ...primary.opportunity,
                value: list.reduce((sum, d) => sum + (d.opportunity?.value || 0), 0) / list.length
            },
            reliability: {
                ...primary.reliability,
                value: list.reduce((sum, d) => sum + (d.reliability?.value || 0), 0) / list.length
            },
            fanChart: primary.fanChart ? {
                years: [...primary.fanChart.years],
                p10: primary.fanChart.p10.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.p10[i] || 0), 0) / list.length),
                p50: primary.fanChart.p50.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.p50[i] || 0), 0) / list.length),
                p90: primary.fanChart.p90.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.p90[i] || 0), 0) / list.length),
                y_med: primary.fanChart.y_med.map((_, i) => list.reduce((sum, d) => sum + (d.fanChart?.y_med[i] || 0), 0) / list.length),
            } : undefined
        } as DetailsResponse
    }

    const getTrendIcon = (trend: "up" | "down" | "stable" | undefined) => {
        if (trend === "up") return <TrendingUp className="h-3 w-3 text-green-500" />
        if (trend === "down") return <TrendingDown className="h-3 w-3 text-red-500" />
        return <Minus className="h-3 w-3 text-muted-foreground" />
    }

    // TRACK SHIFT KEY
    useEffect(() => {
        const handleDown = (e: KeyboardEvent) => { if (e.key === "Shift") setIsShiftHeld(true) }
        const handleUp = (e: KeyboardEvent) => { if (e.key === "Shift") setIsShiftHeld(false) }
        window.addEventListener("keydown", handleDown)
        window.addEventListener("keyup", handleUp)
        return () => {
            window.removeEventListener("keydown", handleDown)
            window.removeEventListener("keyup", handleUp)
        }
    }, [])

    // INITIALIZE MAP (Read from URL for persistence across engine switches)
    useEffect(() => {
        if (!mapContainerRef.current) return

        // Read initial position from URL params (synced by both engines)
        const urlParams = new URLSearchParams(window.location.search)
        const initialLat = parseFloat(urlParams.get("lat") || String(mapState.center[1]))
        const initialLng = parseFloat(urlParams.get("lng") || String(mapState.center[0]))
        const initialZoom = parseFloat(urlParams.get("zoom") || String(mapState.zoom))

        const map = new maplibregl.Map({
            container: mapContainerRef.current,
            style: {
                version: 8,
                sources: {
                    "osm": {
                        type: "raster",
                        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
                        tileSize: 256,
                        attribution: "&copy; OpenStreetMap contributors"
                    }
                },
                layers: [
                    {
                        id: "osm-layer",
                        type: "raster",
                        source: "osm"
                    }
                ]
            },
            center: [initialLng, initialLat],
            zoom: initialZoom,
            maxZoom: 18,
            minZoom: 2
        })

        map.on("load", () => {
            setIsLoaded(true)

            // Add Double-Buffered H3 Sources
            const addH3Source = (id: string) => {
                map.addSource(id, {
                    type: "vector",
                    tiles: [`${window.location.origin}/api/tiles/{z}/{x}/{y}?year=${year}`],
                    minzoom: 2,
                    maxzoom: 14,
                    promoteId: "h3_id"
                })
            }

            const addH3Layers = (suffix: string, sourceId: string, visible: boolean) => {
                // Hex Fill Layer
                map.addLayer({
                    id: `h3-fill-${suffix}`,
                    type: "fill",
                    source: sourceId,
                    "source-layer": "default",
                    layout: { visibility: visible ? "visible" : "none" },
                    paint: {
                        "fill-color": [
                            "interpolate", ["linear"], ["get", "val"],
                            100000, "#3b0764",
                            800000, "#f43f5e",
                            1500000, "#fbbf24"
                        ],
                        "fill-opacity": 0.5,
                        "fill-outline-color": "rgba(255,255,255,0.2)"
                    }
                })

                // Comparison Layer
                map.addLayer({
                    id: `h3-comparison-${suffix}`,
                    type: "line",
                    source: sourceId,
                    "source-layer": "default",
                    layout: { visibility: visible ? "visible" : "none" },
                    paint: {
                        "line-color": "#f97316",
                        "line-width": 2.5,
                        "line-dasharray": [3, 2],
                        "line-opacity": ["case", ["boolean", ["feature-state", "comparison"], false], 0.8, 0]
                    }
                })

                // Hover/Selected Layer
                map.addLayer({
                    id: `h3-selected-${suffix}`,
                    type: "line",
                    source: sourceId,
                    "source-layer": "default",
                    layout: { visibility: visible ? "visible" : "none" },
                    paint: {
                        "line-color": [
                            "case",
                            ["boolean", ["feature-state", "primary"], false], "#14b8a6",
                            ["boolean", ["feature-state", "selected"], false], "#f97316",
                            ["boolean", ["feature-state", "hover"], false], "#ffffff",
                            "transparent"
                        ],
                        "line-width": ["case", ["boolean", ["feature-state", "primary"], false], 4, 3],
                        "line-opacity": [
                            "case",
                            ["any", ["boolean", ["feature-state", "selected"], false], ["boolean", ["feature-state", "primary"], false], ["boolean", ["feature-state", "hover"], false]], 1,
                            0
                        ]
                    }
                })
            }

            addH3Source("h3-a")
            addH3Source("h3-b")
            addH3Layers("a", "h3-a", true)
            addH3Layers("b", "h3-b", false)

            // Sources was added above

            // Add Parcels Source & Layer (Empty initially)
            map.addSource("parcels", {
                type: "geojson",
                data: { type: "FeatureCollection", features: [] }
            })

            map.addLayer({
                id: "parcels-layer",
                type: "line",
                source: "parcels",
                paint: {
                    "line-color": "rgba(255,255,255,0.4)",
                    "line-width": 1
                },
                minzoom: 14
            })
        })

        // INTERACTION HANDLING
        const activeSourceRef = { current: "h3-a" }
        const activeSuffixRef = { current: "a" }

        const handleMove = (e: maplibregl.MapLayerMouseEvent) => {
            const feature = e.features?.[0]
            if (!feature) return

            setMousePos({ x: e.point.x, y: e.point.y })
            const id = feature.properties?.h3_id as string
            if (!id) return

            // Map Vector Tile Props -> FeatureProperties
            const p = feature.properties || {}
            setTooltipProps({
                id: p.h3_id,
                O: (p.opp || 0) * 100,
                R: p.rel || 0,
                n_accts: p.count || 0,
                med_mean_ape_pct: (p.acc || 0) * 100,
                med_mean_pred_cv_pct: 0,
                stability_flag: (p.ap || 0) > 0.15,
                robustness_flag: (p.ap || 0) > 0.25,
                has_data: (p.count || 0) > 0,
                med_predicted_value: p.val || 0
            })

            if (hoveredHexRef.current !== id) {
                const sources = ["h3-a", "h3-b"]
                activeHoverIds.current.forEach(oldId => {
                    sources.forEach(s => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: "default", id: oldId },
                                { hover: false, comparison: false }
                            )
                        } catch (err) { /* ignore */ }
                    })
                })
                activeHoverIds.current.clear()

                hoveredHexRef.current = id
                activeHoverIds.current.add(id)

                setHoveredHex(id)

                const isSelected = selectedHexesRef.current.includes(id)
                const hasSelection = selectedHexesRef.current.length > 0

                sources.forEach(s => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer: "default", id: id },
                            {
                                hover: !hasSelection,
                                comparison: hasSelection && !isSelected
                            }
                        )
                    } catch (err) { /* ignore */ }
                })
                onFeatureHover(id)
            }
        }

        map.on("mousemove", "h3-fill-a", handleMove)
        map.on("mousemove", "h3-fill-b", handleMove)

        const handleLeave = () => {
            setMousePos(null)
            const sources = ["h3-a", "h3-b"]
            activeHoverIds.current.forEach(oldId => {
                sources.forEach(s => {
                    map.setFeatureState(
                        { source: s, sourceLayer: "default", id: oldId },
                        { hover: false, comparison: false }
                    )
                })
            })
            activeHoverIds.current.clear()

            hoveredHexRef.current = null
            setHoveredHex(null)
            onFeatureHover(null)
        }

        map.on("mouseleave", "h3-fill-a", handleLeave)
        map.on("mouseleave", "h3-fill-b", handleLeave)

        const handleClick = (e: maplibregl.MapLayerMouseEvent) => {
            if (!e.features || e.features.length === 0) {
                const sources = ["h3-a", "h3-b"]
                selectedHexesRef.current.forEach(hId => {
                    sources.forEach(s => {
                        try {
                            map.setFeatureState(
                                { source: s, sourceLayer: "default", id: hId },
                                { selected: false, primary: false, comparison: false }
                            )
                        } catch (err) { /* ignore */ }
                    })
                })
                selectedHexesRef.current = []
                setSelectedHexes([])
                onFeatureSelect(null)
                return
            }

            const id = e.features[0].properties?.h3_id as string
            if (!id) return
            const isShift = (e.originalEvent as MouseEvent).shiftKey
            const prev = selectedHexesRef.current
            const next = isShift
                ? (prev.includes(id) ? prev.filter(x => x !== id) : [...prev, id])
                : ((prev.length === 1 && prev[0] === id) ? [] : [id])

            const sources = ["h3-a", "h3-b"]
            prev.forEach(pId => {
                sources.forEach(s => {
                    try {
                        map.setFeatureState({ source: s, sourceLayer: "default", id: pId }, { selected: false, primary: false })
                    } catch (err) { /* ignore */ }
                })
            })

            next.forEach((nId, idx) => {
                const isPrimary = idx === 0
                sources.forEach(s => {
                    try {
                        map.setFeatureState(
                            { source: s, sourceLayer: "default", id: nId },
                            { selected: true, primary: isPrimary }
                        )
                    } catch (err) { /* ignore */ }
                })
            })

            // MOBILE: Gently pan map if tap was in lower 40% of screen
            const containerHeight = mapContainerRef.current?.clientHeight || window.innerHeight
            const tapY = e.point.y
            const isMobileDevice = window.innerWidth < 768
            const isLowerScreen = tapY > containerHeight * 0.6

            if (isMobileDevice && isLowerScreen && next.length > 0) {
                // Pan upward by 150 pixels to keep the tile visible above the tooltip
                map.panBy([0, -150], { duration: 400, easing: (t) => t * (2 - t) })
            }


            selectedHexesRef.current = next
            setSelectedHexes(next)

            if (!isShift) {
                onFeatureSelect(next.length > 0 ? id : null)
            }
        }

        map.on("click", "h3-fill-a", handleClick)
        map.on("click", "h3-fill-b", handleClick)

            // Storage for toggle logic
            ; (map as any)._activeSuffix = "a";
        ; (map as any)._isLoaded = true;

        mapRef.current = map
        return () => { map.remove() }
    }, []) // Initialize map ONCE. Updates happen via other effects.

    // UPDATE YEAR & COLORS & SEAMLESS TOGGLE
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const currentSuffix = (map as any)._activeSuffix || "a"
        const nextSuffix = currentSuffix === "a" ? "b" : "a"
        const nextSource = `h3-${nextSuffix}`

        // Update NEXT source tiles
        const source = map.getSource(nextSource)
        if (source && source.type === 'vector') {
            (source as any).setTiles([`${window.location.origin}/api/tiles/{z}/{x}/{y}?year=${year}`])
        }

        // Apply Color Logic (HEX equivalents of OKLCH for MapLibre compatibility)
        const fillColor: any = filters.colorMode === "growth"
            ? [
                "interpolate", ["linear"], ["get", "opp"],
                -0.50, "#b159ff", // Deep Purple
                0.00, "#f8f8f8", // Whiteish
                0.50, "#3b82f6"  // Deep Blue
            ]
            : [
                "interpolate", ["linear"], ["get", "val"],
                100000, "#3b0764",
                800000, "#f43f5e",
                1500000, "#fbbf24"
            ];

        // Update colors on BOTH layers to be ready
        ["a", "b"].forEach(s => {
            const layerId = `h3-fill-${s}`
            if (map.getLayer(layerId)) {
                map.setPaintProperty(layerId, "fill-color", fillColor)
            }
        })

        // Seamless Swap: Robust 'sourcedata' check
        const onData = (e: any) => {
            if (e.sourceId === nextSource && map.isSourceLoaded(nextSource) && e.isSourceLoaded) {
                map.off("sourcedata", onData)

                // Ensure the source we just loaded IS the one we want to show
                const latestTargetYear = (map as any)._targetYear
                if (latestTargetYear !== year) return

                // Toggle Visibility
                map.setLayoutProperty(`h3-fill-${nextSuffix}`, "visibility", "visible")
                map.setLayoutProperty(`h3-comparison-${nextSuffix}`, "visibility", "visible")
                map.setLayoutProperty(`h3-selected-${nextSuffix}`, "visibility", "visible")

                map.setLayoutProperty(`h3-fill-${currentSuffix}`, "visibility", "none")
                map.setLayoutProperty(`h3-comparison-${currentSuffix}`, "visibility", "none")
                map.setLayoutProperty(`h3-selected-${currentSuffix}`, "visibility", "none")

                    // Update active state
                    ; (map as any)._activeSuffix = nextSuffix
            }
        }

            // Store target year to handle fast scrubbing
            ; (map as any)._targetYear = year
        map.on("sourcedata", onData)

    }, [year, isLoaded, filters.colorMode])

    // DATA FETCHING (Aggregation & Details)
    useEffect(() => {
        const timer = setTimeout(() => {
            if (selectedHexes.length === 0 && !hoveredHex) {
                setHoveredDetails(null)
                setSelectionDetails(null)
                return
            }

            setIsLoadingDetails(true)

            const fetchWithCache = async (id: string, y: number) => {
                const key = `${id}-${y}`
                if (detailsCache.current.has(key)) return detailsCache.current.get(key)!
                const d = await getH3CellDetails(id, y)
                if (d) detailsCache.current.set(key, d)
                return d
            }

            const selectionPromise = selectedHexes.length > 0
                ? Promise.all(selectedHexes.map(id => fetchWithCache(id, year)))
                : Promise.resolve([])

            const hoverPromise = hoveredHex
                ? fetchWithCache(hoveredHex, year)
                : Promise.resolve(null)

            Promise.all([selectionPromise, hoverPromise])
                .then(([selectionResults, hoverResult]) => {
                    const validSelection = selectionResults.filter((d): d is DetailsResponse => d !== null)

                    if (validSelection.length > 0) {
                        setSelectionDetails(aggregateDetails(validSelection))
                    } else {
                        setSelectionDetails(null)
                    }
                    setHoveredDetails(hoverResult)
                })
                .finally(() => setIsLoadingDetails(false))
        }, 150)
        return () => clearTimeout(timer)
    }, [selectedHexes, hoveredHex, year])

    // COMPARISON & PREVIEW LOGIC
    // (comparisonDetails is already declared above as state)
    const computedPreviewDetails = useMemo(() => {
        // Candidate Comparison (Preview) = Current Selection + New Candidate (if Shift held)
        if (!selectionDetails || !hoveredDetails || !isShiftHeld) return null
        if (selectedHexes.includes(hoveredHex!)) return null // Don't preview what is already selected for now (classic logic handles this specifically for removal but simplified here)

        try {
            return aggregateDetails([selectionDetails, hoveredDetails])
        } catch { return null }
    }, [selectionDetails, hoveredDetails, isShiftHeld, selectedHexes, hoveredHex])

    useEffect(() => {
        if (selectedHexes.length === 0) {
            setComparisonHex(null)
            setComparisonDetailsState(null)
            return
        }
        if (hoveredHex && !selectedHexes.includes(hoveredHex)) {
            setComparisonHex(hoveredHex)
            setComparisonDetailsState(hoveredDetails)
        } else {
            setComparisonHex(null)
            setComparisonDetailsState(null)
        }
    }, [hoveredHex, selectedHexes, hoveredDetails])

    useEffect(() => {
        if (!mapRef.current || !isLoaded || !comparisonHex) return
        const map = mapRef.current

        try {
            if (map.getStyle()) {
                ["h3-a", "h3-b"].forEach(s => {
                    map.setFeatureState({ source: s, sourceLayer: "default", id: comparisonHex }, { comparison: true })
                })
            }
        } catch (e) {
            console.warn("Failed to set comparison state", e)
        }

        return () => {
            if (comparisonHex) {
                try {
                    if (mapRef.current && mapRef.current.getStyle()) {
                        ["h3-a", "h3-b"].forEach(s => {
                            mapRef.current?.setFeatureState({ source: s, sourceLayer: "default", id: comparisonHex }, { comparison: false })
                        })
                    }
                } catch (e) { /* ignore */ }
            }
        }
    }, [comparisonHex, isLoaded])

    // DYNAMIC FILTERING (Disabled for Parity with MapView)
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        // MapView shows ALL data regardless of filters
        // We ensure "all" filter to show everything
        try {
            if (map.getStyle()) {
                const layers = [
                    "h3-fill-a", "h3-fill-b",
                    "h3-selected-a", "h3-selected-b",
                    "h3-comparison-a", "h3-comparison-b"
                ]
                layers.forEach(id => {
                    if (map.getLayer(id)) map.setFilter(id, null) // Clear any filters
                })
            }
        } catch (e) {
            console.warn("Failed to apply filters", e)
        }
    }, [isLoaded])

    // PARCEL FETCHING
    useEffect(() => {
        if (!isLoaded || !mapRef.current) return
        const map = mapRef.current

        const updateParcels = async () => {
            const zoom = map.getZoom()
            if (zoom < 14) return

            const bounds = map.getBounds()
            const p = await getParcels({
                minLat: bounds.getSouth(),
                maxLat: bounds.getNorth(),
                minLng: bounds.getWest(),
                maxLng: bounds.getEast()
            })

            const source = map.getSource("parcels")
            if (source && source.type === 'geojson') {
                (source as any).setData({
                    type: "FeatureCollection",
                    features: p.map(parcel => ({
                        type: "Feature",
                        geometry: parcel.geometry,
                        properties: { acct_key: parcel.acct_key }
                    }))
                })
            }
        }

        const handler = () => {
            clearTimeout((map as any)._parcelTimer)
                ; (map as any)._parcelTimer = setTimeout(updateParcels, 500)
        }

        map.on("moveend", handler)
        handler() // Initial fetch
        return () => { map.off("moveend", handler) }
    }, [isLoaded])

    // SYNC VIEWPORT
    useEffect(() => {
        if (!mapRef.current || !isLoaded) return
        const map = mapRef.current

        try {
            if (map.getStyle()) {
                const center = map.getCenter()
                const zoom = map.getZoom()
                if (Math.abs(center.lng - mapState.center[0]) > 0.001 ||
                    Math.abs(center.lat - mapState.center[1]) > 0.001 ||
                    Math.abs(zoom - mapState.zoom) > 0.1) {
                    map.flyTo({ center: [mapState.center[0], mapState.center[1]], zoom: mapState.zoom, speed: 1.5 })
                }
            }
        } catch (e) { /* map might be in transition or removed */ }
    }, [mapState.center, mapState.zoom, isLoaded])

    // TOOLTIP POSITIONING & RENDER PARITY
    const displayId = hoveredHex || selectedHexes[0] || null
    const displayDetails = hoveredDetails || selectionDetails
    const h3Resolution = mapRef.current ? Math.floor(mapRef.current.getZoom()) : 0

    return (
        <div className={cn("relative w-full h-full", className)}>
            <div ref={mapContainerRef} className="w-full h-full" />

            {!isLoaded && (
                <div className="absolute inset-0 flex items-center justify-center bg-background/50 backdrop-blur-sm z-50">
                    <div className="flex flex-col items-center gap-2">
                        <div className="w-8 h-8 border-4 border-primary border-t-transparent rounded-full animate-spin" />
                        <span className="text-sm font-medium">Initializing Vector Engine...</span>
                    </div>
                </div>
            )}

            {/* SHARED TOOLTIP UI */}
            {isLoaded && displayId && mousePos && (
                <MapTooltip
                    x={mousePos.x + 20}
                    y={mousePos.y - 20}
                    // VectorMap passes fixed coord prop for header display
                    coordinates={cellToLatLng(displayId)}
                    displayProps={tooltipProps || {
                        id: displayId,
                        O: 0, R: 0, n_accts: 0, med_mean_ape_pct: 0, med_mean_pred_cv_pct: 0,
                        stability_flag: false, robustness_flag: false, has_data: false
                    }} // Fallback to safe empty object if props missing
                    displayDetails={displayDetails}
                    primaryDetails={null} // Not yet implemented - single selection mode
                    selectionDetails={selectionDetails}
                    comparisonDetails={comparisonDetails} // Now properly computed
                    previewDetails={computedPreviewDetails} // Computed useMemo
                    selectedHexes={selectedHexes}
                    hoveredDetails={hoveredDetails}
                    year={year}
                    h3Resolution={h3Resolution}
                    isLoadingDetails={isLoadingDetails}
                    isMobile={isMobile}
                    isMinimized={false}
                    onYearChange={() => { }} // Placeholder - no timelapse in vector mode yet
                />
            )}
        </div>
    )
}
