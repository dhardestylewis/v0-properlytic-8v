"use client"

import React, { useEffect, useRef, useState, useCallback } from "react"
import maplibregl from "maplibre-gl"
import "maplibre-gl/dist/maplibre-gl.css"

export default function ProtestMapPage() {
  const mapContainer = useRef<HTMLDivElement>(null)
  const map = useRef<maplibregl.Map | null>(null)
  const [selectedFeature, setSelectedFeature] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [stats, setStats] = useState({ total: 0, highRisk: 0 })

  useEffect(() => {
    if (!mapContainer.current || map.current) return

    const m = new maplibregl.Map({
      container: mapContainer.current,
      style: {
        version: 8,
        sources: {
          "carto-dark": {
            type: "raster",
            tiles: [
              "https://a.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png",
            ],
            tileSize: 256,
            attribution: "&copy; CartoDB",
          },
        },
        layers: [
          {
            id: "carto-dark-layer",
            type: "raster",
            source: "carto-dark",
            minzoom: 0,
            maxzoom: 22,
          },
        ],
      },
      center: [-97.7431, 30.2672],
      zoom: 11,
    })

    map.current = m

    m.on("load", async () => {
      // Load protest scores GeoJSON
      const res = await fetch("/data/protest_scores.json")
      const geojson = await res.json()

      setStats({
        total: geojson.features.length,
        highRisk: geojson.features.filter(
          (f: any) => f.properties.protest_prob > 0.05
        ).length,
      })
      setLoading(false)

      m.addSource("protests", {
        type: "geojson",
        data: geojson,
      })

      // Heatmap layer for zoomed-out view
      m.addLayer({
        id: "protest-heat",
        type: "heatmap",
        source: "protests",
        maxzoom: 14,
        paint: {
          "heatmap-weight": [
            "interpolate", ["linear"], ["get", "protest_prob"],
            0, 0,
            0.01, 0.1,
            0.05, 0.4,
            0.1, 0.7,
            0.5, 1,
          ],
          "heatmap-intensity": [
            "interpolate", ["linear"], ["zoom"],
            8, 0.5, 14, 2,
          ],
          "heatmap-color": [
            "interpolate", ["linear"], ["heatmap-density"],
            0, "rgba(0,0,0,0)",
            0.1, "rgba(30,60,180,0.4)",
            0.3, "rgba(50,150,220,0.6)",
            0.5, "rgba(60,200,180,0.7)",
            0.7, "rgba(250,220,70,0.8)",
            0.9, "rgba(255,120,30,0.9)",
            1, "rgba(255,40,40,1)",
          ],
          "heatmap-radius": [
            "interpolate", ["linear"], ["zoom"],
            8, 8, 12, 15, 14, 25,
          ],
          "heatmap-opacity": [
            "interpolate", ["linear"], ["zoom"],
            13, 1, 14.5, 0,
          ],
        },
      })

      // Circle layer for zoomed-in view
      m.addLayer({
        id: "protest-circles",
        type: "circle",
        source: "protests",
        minzoom: 13,
        paint: {
          "circle-radius": [
            "interpolate", ["linear"], ["zoom"],
            13, 3, 16, 8,
          ],
          "circle-color": [
            "interpolate", ["linear"], ["get", "protest_prob"],
            0, "#1e3a8a",
            0.005, "#2563eb",
            0.01, "#38bdf8",
            0.02, "#34d399",
            0.05, "#fbbf24",
            0.1, "#f97316",
            0.5, "#ef4444",
            1.0, "#dc2626",
          ],
          "circle-opacity": [
            "interpolate", ["linear"], ["zoom"],
            13, 0, 14, 0.8,
          ],
          "circle-stroke-width": [
            "case",
            ["==", ["get", "actual"], 1], 2,
            0,
          ],
          "circle-stroke-color": "#ffffff",
        },
      })

      // Click handler
      m.on("click", "protest-circles", (e) => {
        if (e.features && e.features[0]) {
          const props = e.features[0].properties
          setSelectedFeature({
            ...props,
            series: typeof props.series === "string" ? JSON.parse(props.series) : props.series,
          })
        }
      })

      // Cursor
      m.on("mouseenter", "protest-circles", () => {
        m.getCanvas().style.cursor = "pointer"
      })
      m.on("mouseleave", "protest-circles", () => {
        m.getCanvas().style.cursor = ""
      })
    })

    return () => m.remove()
  }, [])

  return (
    <div style={{ width: "100%", height: "100vh", position: "relative", background: "#0f172a" }}>
      <div ref={mapContainer} style={{ width: "100%", height: "100%" }} />

      {/* Header */}
      <div style={{
        position: "absolute", top: 16, left: 16, right: 16,
        display: "flex", justifyContent: "space-between", alignItems: "flex-start",
        pointerEvents: "none",
      }}>
        <div style={{
          background: "rgba(15,23,42,0.92)", backdropFilter: "blur(12px)",
          borderRadius: 12, padding: "16px 20px",
          border: "1px solid rgba(255,255,255,0.1)",
          pointerEvents: "auto",
        }}>
          <h1 style={{ color: "#f8fafc", fontSize: 20, fontWeight: 700, margin: 0 }}>
            Austin Property Tax Protest Forecast
          </h1>
          <p style={{ color: "#94a3b8", fontSize: 13, margin: "4px 0 0" }}>
            Diffusion Model v4 · Isotonic Calibrated · {stats.total.toLocaleString()} parcels
          </p>
        </div>

        {/* Stats */}
        <div style={{
          display: "flex", gap: 8, pointerEvents: "auto",
        }}>
          {[
            { label: "Total Parcels", value: stats.total.toLocaleString(), color: "#60a5fa" },
            { label: "High Risk (>5%)", value: stats.highRisk.toLocaleString(), color: "#f59e0b" },
          ].map(({ label, value, color }) => (
            <div key={label} style={{
              background: "rgba(15,23,42,0.92)", backdropFilter: "blur(12px)",
              borderRadius: 10, padding: "12px 16px",
              border: "1px solid rgba(255,255,255,0.1)",
              textAlign: "center",
            }}>
              <div style={{ color, fontSize: 22, fontWeight: 700 }}>{loading ? "..." : value}</div>
              <div style={{ color: "#94a3b8", fontSize: 11 }}>{label}</div>
            </div>
          ))}
        </div>
      </div>

      {/* Legend */}
      <div style={{
        position: "absolute", bottom: 24, left: 16,
        background: "rgba(15,23,42,0.92)", backdropFilter: "blur(12px)",
        borderRadius: 10, padding: "12px 16px",
        border: "1px solid rgba(255,255,255,0.1)",
      }}>
        <div style={{ color: "#94a3b8", fontSize: 11, marginBottom: 8 }}>Protest Probability</div>
        <div style={{ display: "flex", gap: 4, alignItems: "center" }}>
          {[
            { color: "#1e3a8a", label: "0%" },
            { color: "#2563eb", label: "0.5%" },
            { color: "#38bdf8", label: "1%" },
            { color: "#34d399", label: "2%" },
            { color: "#fbbf24", label: "5%" },
            { color: "#f97316", label: "10%" },
            { color: "#ef4444", label: "50%+" },
          ].map(({ color, label }) => (
            <div key={label} style={{ textAlign: "center" }}>
              <div style={{ width: 24, height: 12, background: color, borderRadius: 2 }} />
              <div style={{ color: "#64748b", fontSize: 9, marginTop: 2 }}>{label}</div>
            </div>
          ))}
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 8, marginTop: 8 }}>
          <div style={{ width: 12, height: 12, borderRadius: "50%", border: "2px solid white", background: "transparent" }} />
          <span style={{ color: "#94a3b8", fontSize: 11 }}>Actually protested</span>
        </div>
      </div>

      {/* Inspector Panel */}
      {selectedFeature && (
        <div style={{
          position: "absolute", top: 80, right: 16, width: 320,
          background: "rgba(15,23,42,0.95)", backdropFilter: "blur(12px)",
          borderRadius: 12, padding: 20,
          border: "1px solid rgba(255,255,255,0.1)",
        }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <h3 style={{ color: "#f8fafc", fontSize: 14, fontWeight: 600, margin: 0 }}>
              Parcel {selectedFeature.id}
            </h3>
            <button
              onClick={() => setSelectedFeature(null)}
              style={{
                background: "none", border: "none", color: "#64748b",
                fontSize: 18, cursor: "pointer", padding: 0,
              }}
            >
              ✕
            </button>
          </div>

          <div style={{ marginTop: 16, display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
            <div>
              <div style={{ color: "#64748b", fontSize: 11 }}>Protest Prob</div>
              <div style={{ color: "#f59e0b", fontSize: 20, fontWeight: 700 }}>
                {(selectedFeature.protest_prob * 100).toFixed(1)}%
              </div>
            </div>
            <div>
              <div style={{ color: "#64748b", fontSize: 11 }}>Ensemble</div>
              <div style={{ color: "#60a5fa", fontSize: 20, fontWeight: 700 }}>
                {(selectedFeature.ensemble_prob * 100).toFixed(1)}%
              </div>
            </div>
            <div>
              <div style={{ color: "#64748b", fontSize: 11 }}>LogReg Score</div>
              <div style={{ color: "#94a3b8", fontSize: 16, fontWeight: 600 }}>
                {(selectedFeature.lr_prob * 100).toFixed(1)}%
              </div>
            </div>
            <div>
              <div style={{ color: "#64748b", fontSize: 11 }}>Actually Protested</div>
              <div style={{
                color: selectedFeature.actual === 1 ? "#ef4444" : "#22c55e",
                fontSize: 16, fontWeight: 600,
              }}>
                {selectedFeature.actual === 1 ? "YES" : "No"}
              </div>
            </div>
          </div>

          {/* Time series */}
          {selectedFeature.series && (
            <div style={{ marginTop: 16 }}>
              <div style={{ color: "#64748b", fontSize: 11, marginBottom: 8 }}>
                Probability Over Time
              </div>
              <div style={{ display: "flex", gap: 4, height: 60, alignItems: "flex-end" }}>
                {selectedFeature.series.map((s: any) => {
                  const h = Math.max(4, Math.min(56, s.cal * 500))
                  return (
                    <div key={s.yr} style={{ flex: 1, textAlign: "center" }}>
                      <div style={{
                        height: h,
                        background: s.actual ? "#ef4444" : "#3b82f6",
                        borderRadius: 2,
                        transition: "height 0.3s",
                      }} />
                      <div style={{ color: "#475569", fontSize: 9, marginTop: 2 }}>{s.yr}</div>
                    </div>
                  )
                })}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
