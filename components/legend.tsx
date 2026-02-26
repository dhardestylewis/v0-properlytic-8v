"use client"

import { Info } from "lucide-react"
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip"
import { cn } from "@/lib/utils"

export function Legend({ className }: { className?: string }) {
  // Protest probability gradient matching the map ramp:
  // 0.0:  "#e0e7ff" (near-zero → very faint blue)
  // 0.05: "#93c5fd" (5%  → light blue)
  // 0.15: "#3b82f6" (15% → blue)
  // 0.30: "#fbbf24" (30% → amber)
  // 0.50: "#f97316" (50% → orange)
  // 0.70: "#ef4444" (70% → red)
  // 0.90: "#7f1d1d" (90% → dark red)
  const PROTEST_GRADIENT = "linear-gradient(to right, #e0e7ff, #93c5fd 5%, #3b82f6 15%, #fbbf24 30%, #f97316 50%, #ef4444 70%, #7f1d1d 90%)"

  return (
    <div className={cn("glass-panel rounded-lg p-3 space-y-1 text-xs", className)}>
      <div className="space-y-1.5">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-1.5 text-foreground font-medium">
            <span>Protest Probability</span>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger asChild>
                  <Info className="h-3 w-3 text-muted-foreground cursor-help" />
                </TooltipTrigger>
                <TooltipContent side="right" className="max-w-48">
                  <p>Probability that a property or neighborhood will file a property tax protest appeal.</p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
        </div>
        <div className="flex flex-col gap-1">
          <div
            className="h-3 w-full rounded-sm"
            style={{ background: PROTEST_GRADIENT }}
          />
          <div className="flex justify-between text-[9px] text-muted-foreground font-mono px-0.5">
            <span>0%</span>
            <span>30%</span>
            <span>90%+</span>
          </div>
        </div>
      </div>
    </div>
  )
}
