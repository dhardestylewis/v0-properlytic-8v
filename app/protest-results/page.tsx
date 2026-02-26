"use client"

import { useEffect, useState } from "react"

interface YearResult {
    year: number
    avg_prob: number
    actual_rate: number
    n: number
}

export default function ProtestResultsPage() {
    const [results, setResults] = useState<YearResult[]>([])

    useEffect(() => {
        fetch("/protest-results.json")
            .then((r) => r.json())
            .then(setResults)
            .catch(console.error)
    }, [])

    const maxRate = Math.max(
        ...results.map((r) => Math.max(r.avg_prob, r.actual_rate)),
        0.1
    )

    return (
        <div
            style={{
                minHeight: "100vh",
                background: "#0a0a0a",
                color: "#e5e5e5",
                fontFamily: "'Inter', system-ui, sans-serif",
                padding: "2rem",
            }}
        >
            <link
                href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap"
                rel="stylesheet"
            />
            <div style={{ maxWidth: 900, margin: "0 auto" }}>
                <h1
                    style={{
                        fontSize: "2rem",
                        fontWeight: 700,
                        marginBottom: "0.5rem",
                        background: "linear-gradient(135deg, #fbbf24, #ef4444)",
                        WebkitBackgroundClip: "text",
                        WebkitTextFillColor: "transparent",
                    }}
                >
                    Oppcastr — Protest Probability Results
                </h1>
                <p style={{ color: "#888", marginBottom: "2rem" }}>
                    Calibrated ensemble model predictions (exp02_isotonic) vs actual protest rates, by year.
                </p>

                {/* BAR CHART */}
                <div
                    style={{
                        background: "#111",
                        borderRadius: 12,
                        padding: "2rem",
                        marginBottom: "2rem",
                        border: "1px solid #222",
                    }}
                >
                    <h2 style={{ fontSize: "1.1rem", fontWeight: 600, marginBottom: "1.5rem" }}>
                        Predicted vs Actual Protest Rate by Year
                    </h2>
                    <div style={{ display: "flex", gap: "2rem", alignItems: "flex-end", height: 240 }}>
                        {results.map((r) => (
                            <div
                                key={r.year}
                                style={{
                                    flex: 1,
                                    display: "flex",
                                    flexDirection: "column",
                                    alignItems: "center",
                                    gap: 4,
                                }}
                            >
                                <div
                                    style={{
                                        display: "flex",
                                        gap: 4,
                                        alignItems: "flex-end",
                                        height: 200,
                                        width: "100%",
                                    }}
                                >
                                    {/* Predicted bar */}
                                    <div
                                        style={{
                                            flex: 1,
                                            background: "linear-gradient(to top, #f97316, #fbbf24)",
                                            borderRadius: "4px 4px 0 0",
                                            height: `${(r.avg_prob / maxRate) * 100}%`,
                                            minHeight: 4,
                                            position: "relative",
                                        }}
                                        title={`Predicted: ${(r.avg_prob * 100).toFixed(1)}%`}
                                    >
                                        <span
                                            style={{
                                                position: "absolute",
                                                top: -20,
                                                left: "50%",
                                                transform: "translateX(-50%)",
                                                fontSize: 11,
                                                color: "#fbbf24",
                                                whiteSpace: "nowrap",
                                            }}
                                        >
                                            {(r.avg_prob * 100).toFixed(1)}%
                                        </span>
                                    </div>
                                    {/* Actual bar */}
                                    <div
                                        style={{
                                            flex: 1,
                                            background: "linear-gradient(to top, #dc2626, #ef4444)",
                                            borderRadius: "4px 4px 0 0",
                                            height: `${(r.actual_rate / maxRate) * 100}%`,
                                            minHeight: 4,
                                            position: "relative",
                                        }}
                                        title={`Actual: ${(r.actual_rate * 100).toFixed(1)}%`}
                                    >
                                        <span
                                            style={{
                                                position: "absolute",
                                                top: -20,
                                                left: "50%",
                                                transform: "translateX(-50%)",
                                                fontSize: 11,
                                                color: "#ef4444",
                                                whiteSpace: "nowrap",
                                            }}
                                        >
                                            {(r.actual_rate * 100).toFixed(1)}%
                                        </span>
                                    </div>
                                </div>
                                <span style={{ fontSize: 14, fontWeight: 600, color: "#ccc" }}>
                                    {r.year}
                                </span>
                            </div>
                        ))}
                    </div>
                    <div style={{ display: "flex", gap: "1.5rem", marginTop: "1rem", justifyContent: "center" }}>
                        <span style={{ fontSize: 12, color: "#fbbf24" }}>■ Predicted (ens_calibrated)</span>
                        <span style={{ fontSize: 12, color: "#ef4444" }}>■ Actual protest rate</span>
                    </div>
                </div>

                {/* DATA TABLE */}
                <div
                    style={{
                        background: "#111",
                        borderRadius: 12,
                        padding: "1.5rem",
                        border: "1px solid #222",
                        marginBottom: "2rem",
                    }}
                >
                    <h2 style={{ fontSize: "1.1rem", fontWeight: 600, marginBottom: "1rem" }}>
                        Yearly Summary
                    </h2>
                    <table style={{ width: "100%", borderCollapse: "collapse" }}>
                        <thead>
                            <tr style={{ borderBottom: "1px solid #333" }}>
                                <th style={{ textAlign: "left", padding: "8px 12px", color: "#888", fontWeight: 500 }}>Year</th>
                                <th style={{ textAlign: "right", padding: "8px 12px", color: "#888", fontWeight: 500 }}>Parcels</th>
                                <th style={{ textAlign: "right", padding: "8px 12px", color: "#fbbf24", fontWeight: 500 }}>Avg Predicted</th>
                                <th style={{ textAlign: "right", padding: "8px 12px", color: "#ef4444", fontWeight: 500 }}>Actual Rate</th>
                                <th style={{ textAlign: "right", padding: "8px 12px", color: "#888", fontWeight: 500 }}>Calibration Gap</th>
                            </tr>
                        </thead>
                        <tbody>
                            {results.map((r) => {
                                const gap = r.avg_prob - r.actual_rate
                                return (
                                    <tr key={r.year} style={{ borderBottom: "1px solid #1a1a1a" }}>
                                        <td style={{ padding: "10px 12px", fontWeight: 600 }}>{r.year}</td>
                                        <td style={{ padding: "10px 12px", textAlign: "right", color: "#aaa" }}>
                                            {r.n.toLocaleString()}
                                        </td>
                                        <td style={{ padding: "10px 12px", textAlign: "right", color: "#fbbf24" }}>
                                            {(r.avg_prob * 100).toFixed(2)}%
                                        </td>
                                        <td style={{ padding: "10px 12px", textAlign: "right", color: "#ef4444" }}>
                                            {(r.actual_rate * 100).toFixed(2)}%
                                        </td>
                                        <td
                                            style={{
                                                padding: "10px 12px",
                                                textAlign: "right",
                                                color: gap > 0 ? "#fbbf24" : "#22c55e",
                                            }}
                                        >
                                            {gap > 0 ? "+" : ""}
                                            {(gap * 100).toFixed(2)} pp
                                        </td>
                                    </tr>
                                )
                            })}
                        </tbody>
                    </table>
                </div>

                {/* METHODOLOGY NOTE */}
                <div
                    style={{
                        background: "#111",
                        borderRadius: 12,
                        padding: "1.5rem",
                        border: "1px solid #222",
                        fontSize: 13,
                        color: "#888",
                    }}
                >
                    <h3 style={{ color: "#ccc", fontWeight: 600, marginBottom: 8 }}>Methodology</h3>
                    <ul style={{ margin: 0, paddingLeft: "1.2rem", lineHeight: 1.8 }}>
                        <li>Model: Ensemble of isotonically calibrated classifiers (exp02_isotonic)</li>
                        <li>Target: Property tax protest filing (binary)</li>
                        <li>Geography: Travis County, TX (Austin metro)</li>
                        <li>Unit: Individual tax parcel (standardized TCAD ID)</li>
                        <li>Calibration: Predicted probabilities vs observed protest filing rates</li>
                    </ul>
                </div>
            </div>
        </div>
    )
}
