"use client"

import { Info } from "lucide-react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

// Value mode gradient (unchanged)
const VALUE_GRADIENT = "linear-gradient(to right, oklch(0.25 0.10 280), oklch(0.60 0.20 30), oklch(0.95 0.15 80))"
const VALUE_LABELS = ["$150k", "$525k", "$1M+"]

interface LegendProps {
  className?: string
  colorMode?: "growth" | "value"
  onColorModeChange?: (mode: "growth" | "value") => void
  year?: number
  originYear?: number
}

export function Legend({ className, colorMode = "growth", onColorModeChange, year = 2027, originYear = 2025 }: LegendProps) {
  // Compute horizon-aware labels from empirical percentile fits
  const presentYear = (originYear ?? 2025) + 1
  const yrsFromPresent = Math.max(Math.abs((year ?? 2027) - presentYear), 1)
  // Round to nearest 5 for clean labels
  const round5 = (n: number) => Math.round(n / 5) * 5
  const p05 = round5(-5 - 4 * yrsFromPresent)   // 1yr≈-10, 3yr≈-15, 5yr≈-25
  const med = round5(5 * yrsFromPresent)          // 1yr≈5, 3yr≈15, 5yr≈25
  const p95 = round5(30 * yrsFromPresent)         // 1yr≈30, 3yr≈90, 5yr≈150

  const growthLabels = [
    `${p05 > 0 ? "+" : ""}${p05}%`,
    `${med > 0 ? "+" : ""}${med}%`,
    `+${p95}%+`,
  ]

  // Growth gradient: same colors as buildFillColor ramp
  const OPPORTUNITY_GRADIENT = "linear-gradient(to right, #3b82f6, #93c5fd 30%, #f8f8f8 50%, #f59e0b 70%, #ef4444)"

  return (
    <div className={cn("glass-panel rounded-lg p-3 space-y-1 text-xs", className)}>
      {/* Color Scale */}
      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5 text-foreground font-medium">
          <span>{colorMode === "value" ? "Property Value" : "Projected Growth"}</span>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Info className="h-3 w-3 text-muted-foreground cursor-help" />
              </TooltipTrigger>
              <TooltipContent side="right" className="max-w-48">
                <p>
                  {colorMode === "value"
                    ? "Estimated median property value ($)."
                    : "Change in value vs 2025 baseline."}
                </p>
              </TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
        <div className="flex flex-col gap-1">
          <div
            className="h-3 w-full rounded-sm"
            style={{ background: colorMode === "value" ? VALUE_GRADIENT : OPPORTUNITY_GRADIENT }}
          />
          <div className="flex justify-between text-[9px] text-muted-foreground font-mono px-0.5">
            {colorMode === "value" ? (
              <>
                <span>{VALUE_LABELS[0]}</span>
                <span>{VALUE_LABELS[1]}</span>
                <span>{VALUE_LABELS[2]}</span>
              </>
            ) : (
              <>
                <span>{growthLabels[0]}</span>
                <span>{growthLabels[1]}</span>
                <span>{growthLabels[2]}</span>
              </>
            )}
          </div>
        </div>
      </div>

      {/* Bottom Row: No Data + Toggles */}
      <div className="flex items-center justify-between gap-4">
        {/* No Data Indicator */}
        <div className="flex items-center gap-1.5">
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
            <path d="M12 2L20.66 7V17L12 22L3.34 17V7L12 2Z" fill="#888888" fillOpacity="0.1" stroke="#888888" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
          <span className="text-[10px] text-muted-foreground leading-tight">No Residential Properties</span>
        </div>

        {/* Color Mode Toggle (Compact, Bottom Right) */}
        {onColorModeChange && (
          <div className="grid grid-cols-2 gap-1 p-0.5 bg-secondary/50 rounded-md shrink-0">
            <button
              onClick={() => onColorModeChange("growth")}
              className={cn(
                "px-2 py-1 text-[10px] font-medium rounded transition-all",
                colorMode === "growth"
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              Growth
            </button>
            <button
              onClick={() => onColorModeChange("value")}
              className={cn(
                "px-2 py-1 text-[10px] font-medium rounded transition-all",
                colorMode === "value"
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground"
              )}
            >
              Value
            </button>
          </div>
        )}
      </div>
    </div>
  )
}

