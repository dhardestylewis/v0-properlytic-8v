# Homecastr — Hackathon Submission

> Changes since 8:30 AM 2/21/2026, covering `main` (local) and `feat/observability-integrations` (remote) branches.

## Inspiration

Every homebuyer asks: *"Is this neighborhood going to appreciate?"* Traditional tools show yesterday — comps, Zestimates, recent sales. Nobody shows you tomorrow, at the individual home level, with honest uncertainty bands.

We were inspired by how financial markets display probabilistic forecasts (P10/P50/P90 fan charts) and wanted to bring that rigor to real estate. A foundation model trained on every property in Houston generating thousands of scenarios per lot — not a magic number, but a full distribution of outcomes.

## What it does

Homecastr is a real-time, interactive property value forecasting platform for Houston, TX:

- **Live choropleth map** across 350K+ parcels, 5K+ blocks, 1K+ tracts, 150+ zip codes — all switching seamlessly as you zoom
- **Fan chart tooltips** showing P10–P90 forecast bands from 2019–2030 with historical overlay
- **Shift-hover comparison** — lock one area, hover another, see both forecasts overlaid
- **AI chat agent** — ask "What's the forecast for Montrose?" and the map flies there, locks the tooltip, and reports numbers like an analyst
- **Reverse geocoding** — tooltips now show human-readable names ("Montrose") instead of census IDs ("48201530701")
- **Time scrubber** — slide 2026–2030 and watch the choropleth update live
- **Full observability** — Datadog RUM for browser performance, dd-trace for server-side tracing, Braintrust for LLM call logging

## How we built it

**Today's work spanned two parallel branches:**

### `main` — Chat + Forecast Mode Integration
- **`forecastMode` detection**: Modified `route.ts` to accept `forecastMode` from the frontend, selecting forecast-specific tools (`location_to_area`, `rank_forecast_areas`) instead of H3 tools (`location_to_hex`)
- **Unified tool layer**: Both chat and Tavus video agent now share `executeTopLevelForecastTool` — one server-side executor for geocoding, Supabase queries, and structured results
- **Map action pipeline**: `page.tsx` → `ChatPanel` → `route.ts` → tool result → `tavus-map-action` event → `ForecastMap` tooltip lock
- **Reverse geocoding**: Added Nominatim API integration with client-side caching to `forecast-map.tsx` — resolves centroid coordinates to neighborhood names
- **`add_location_to_selection` tool**: Added to Tavus forecast tool definitions for comparison functionality

### `feat/observability-integrations` — Production Monitoring
- **Datadog RUM** (`components/datadog-rum.tsx`): Browser-side session replay, performance monitoring, error tracking
- **Server instrumentation** (`instrumentation.ts`): dd-trace integration for Next.js server-side request tracing
- **Braintrust LLM logging**: Wraps OpenAI client in `route.ts` to log every LLM call — inputs, outputs, latency, token usage — for quality monitoring
- **Config**: `instrumentationHook: true` in `next.config.mjs`, `@datadog/browser-rum` package

**Stack**: Next.js 16 + MapLibre GL JS + Supabase Postgres + OpenAI GPT-4o-mini + Tavus CVI + Datadog + Braintrust

## Challenges we ran into

1. **Git history bloat**: A 968MB `tiger_cache` file made pushes impossible. Solved with an orphan branch (`git checkout --orphan fresh-main`) to strip history — 968MB → ~50MB.

2. **Turbopack cache staleness**: A literal `\r\n` in source code (from a bad edit) caused a build error that persisted even after fixing the file on disk. Turbopack's aggressive caching required a full dev server restart.

3. **Forecast mode tool routing**: The chat API only knew about H3 hex tools. Wiring `forecastMode` through the full stack (frontend → ChatPanel → route.ts → tool executor → map action → tooltip) required coordinated changes across 5 files.

4. **Census ID readability**: Raw IDs like "48201530701" are meaningless. Adding async reverse geocoding with proper caching and loading states required careful state management to avoid re-rendering flicker.

5. **Google Maps StreetView 403**: Key works for free metadata calls but returns 403 on paid image requests despite billing showing $0 and all APIs enabled. Diagnosed as a Maps Platform billing activation issue separate from standard GCP billing.

6. **Branch divergence**: The observability branch was built against an older `main`. Cherry-picking only the new features (Datadog, Braintrust) without regressing recent work (forecast tools, geocoding, tooltip fixes) required file-by-file diff analysis across 12 changed files.

## Accomplishments that we're proud of

- **Full probabilistic forecasts at the parcel level** — P10–P90 uncertainty bands users can actually see
- **Natural language → map action pipeline** — "Show me River Oaks" pans, zooms, locks tooltip, reports forecast in one smooth interaction
- **Sub-second reverse geocoding** with client-side cache — "Montrose" instead of "48201530701"
- **Production observability stack** in one morning — Datadog RUM + dd-trace + Braintrust LLM logging
- **350,000+ properties forecasted** with a foundation model, not rule-of-thumb appreciation rates
- **Unified tool layer** — same executor powers both text chat and Tavus video agent

## What we learned

- **Turbopack caching** can be aggressive — stale artifacts persist after file edits, requiring dev server restarts
- **Orphan branches** are a powerful git pattern for cleaning up repo history without losing working tree
- **Google Maps Platform billing** is separate from GCP billing — even with an active GCP billing account, Maps image requests can 403 until the Maps-specific $200/month credit is activated
- **Function-calling LLMs** work remarkably well as real estate analysts with structured tools and a focused system prompt
- **Cherry-picking across diverged branches** requires careful file-by-file analysis — wholesale merges introduce regressions

## What's next for Homecastr

1. **Expanding beyond Houston** — the foundation model is city-agnostic; next: Dallas, Austin, San Antonio
2. **Address-level search** — type an address, fly to the lot, see its individual forecast
3. **Portfolio tracking** — save properties and track forecasted appreciation over time
4. **Alert system** — notifications when a neighborhood's forecast shifts significantly
5. **Intention data monetization** — high-intent search patterns as a data product for real estate professionals ($49/mo Pro tier)
6. **MetaProp accelerator** — applying the platform to institutional real estate investment analysis
