"use client"

import { useEffect, useRef } from "react"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"

/**
 * Minimal diagnostic map — bypasses all ForecastMap logic.
 * If this page shows colored tracts, the issue is in ForecastMap component.
 * If this also shows N/A, the issue is in the tile data or color ramp.
 */
export default function MapTestPage() {
    const containerRef = useRef<HTMLDivElement>(null)

    useEffect(() => {
        if (!containerRef.current) return

        const map = new maplibregl.Map({
            container: containerRef.current,
            style: {
                version: 8,
                sources: {
                    osm: {
                        type: "raster",
                        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
                        tileSize: 256,
                    },
                },
                layers: [{ id: "osm", type: "raster", source: "osm" }],
            },
            center: [-97.74, 30.27],
            zoom: 10,
        })

        map.on("load", () => {
            // Single source — no A/B swap
            map.addSource("protest", {
                type: "vector",
                tiles: [
                    `${window.location.origin}/api/forecast-tiles/{z}/{x}/{y}?year=2024&v=100`,
                ],
                minzoom: 0,
                maxzoom: 18,
            })

            const fillColorExpression = [
                "interpolate",
                ["linear"],
                ["coalesce", ["get", "p50"], ["get", "value"], 0],
                0.0, "#e0e7ff",   // near-zero → very faint blue
                0.05, "#93c5fd",   // 5%  → light blue
                0.15, "#3b82f6",   // 15% → blue
                0.30, "#fbbf24",   // 30% → amber
                0.50, "#f97316",   // 50% → orange
                0.70, "#ef4444",   // 70% → red
                0.90, "#7f1d1d",   // 90% → dark red
            ]

            const fillOpacityExpression = [
                "interpolate",
                ["linear"],
                ["coalesce", ["get", "p50"], ["get", "value"], 0],
                0.0, 0.0,          // zero → transparent
                0.01, 0.0,          // <1% → still transparent
                0.05, 0.5,          // 5%  → half visible
                0.15, 0.75,         // 15% → mostly visible
                0.30, 0.85,         // 30%+ → fully opaque
            ]

            // ZCTA fill — zoom 0-8
            map.addLayer({
                id: "zcta-fill",
                type: "fill",
                source: "protest",
                "source-layer": "zcta",
                minzoom: 0,
                maxzoom: 8,
                paint: {
                    "fill-color": fillColorExpression as any,
                    "fill-opacity": fillOpacityExpression as any,
                },
            })

            // Tract fill — zoom 8-12
            map.addLayer({
                id: "tract-fill",
                type: "fill",
                source: "protest",
                "source-layer": "tract",
                minzoom: 8,
                maxzoom: 12,
                paint: {
                    "fill-color": fillColorExpression as any,
                    "fill-opacity": fillOpacityExpression as any,
                },
            })

            // Tabblock fill — zoom 12-18
            map.addLayer({
                id: "tabblock-fill",
                type: "fill",
                source: "protest",
                "source-layer": "tabblock",
                minzoom: 12,
                maxzoom: 18,
                paint: {
                    "fill-color": fillColorExpression as any,
                    "fill-opacity": fillOpacityExpression as any,
                },
            })

            // Tract outline
            map.addLayer({
                id: "tract-outline",
                type: "line",
                source: "protest",
                "source-layer": "tract",
                minzoom: 8,
                maxzoom: 12,
                paint: {
                    "line-color": "#ffffff",
                    "line-width": 1,
                    "line-opacity": 0.5,
                },
            })

            // Debug: log tile loads
            map.on("sourcedata", (e: any) => {
                if (e.sourceId === "protest" && e.isSourceLoaded) {
                    console.log("[MAP-TEST] Source loaded:", e.sourceId, e)
                }
            })

            // Debug: log feature properties on click
            map.on("click", (e: any) => {
                const features = map.queryRenderedFeatures(e.point, { layers: ["zcta-fill", "tract-fill", "tabblock-fill"] })
                if (features.length > 0) {
                    const feat = features[0]
                    console.log(`[MAP-TEST] Clicked ${feat.layer.id} feature:`, feat.properties)
                    alert(`${feat.layer.id} properties:\n${JSON.stringify(feat.properties, null, 2)}`)
                } else {
                    console.log("[MAP-TEST] No features at click point")
                    alert("No features found at this point. Ensure you click on a colored area.")
                }
            })
        })

        return () => map.remove()
    }, [])

    return (
        <div style={{ position: "relative", width: "100vw", height: "100vh" }}>
            <div ref={containerRef} style={{ width: "100%", height: "100%" }} />
            <div
                style={{
                    position: "absolute",
                    top: 10,
                    left: 10,
                    background: "rgba(0,0,0,0.8)",
                    color: "#fff",
                    padding: "12px 16px",
                    borderRadius: 8,
                    fontSize: 14,
                    zIndex: 10,
                    maxWidth: 350,
                }}
            >
                <strong>🔬 Diagnostic Map</strong>
                <br />
                Zoom 8-12 = Tract fill (p50 color ramp)
                <br />
                Click a tract to see its properties.
                <br />
                <span style={{ color: "#fbbf24" }}>■ Amber = p50 ≈ 0.35</span>
            </div>
        </div>
    )
}
