# AVM Vendor Compliance & Validation TODO Spec

> **Agent-ready specification** covering legal positioning, bank regulatory/procurement expectations, validation metrics for **point + bands**, confidence scoring, drift/monitoring, future-horizon fan charts, and deliverables.

---

## TODO: Product positioning and legal boundary conditions

### TODO-1 — Define the product category and prohibited claims

* Write a canonical positioning statement: **"AVM / market value estimate / analytics"**.
* Add a bright-line prohibition in marketing, docs, and contracts: do **not** describe outputs as an **"appraisal"** or "USPAP-compliant appraisal."
* Add explicit statements:

  * Outputs are **decision-support** and are not intended to satisfy statutory appraisal requirements.
  * Outputs may be used for screening, monitoring, and analytics; lender decides final valuation method.

### TODO-2 — Define "intended use" and "allowed use" per buyer workflow

Create a matrix that enumerates each integration type and allowed use:

* Developer feasibility / acquisition underwriting (non-regulated; still liability-managed).
* Portfolio monitoring / risk surveillance (regulated clients may treat as lower risk).
* Underwriting support / collateral determination for principal dwelling (highest scrutiny).
* Loss mitigation / HELOC management / servicing analytics (mid/high scrutiny).

For each workflow, specify:

* Primary decision being supported.
* Whether the workflow is likely covered by lender AVM QC controls.
* Minimum required validation artifacts and gating thresholds.

---

## TODO: Regulatory/procurement requirements mapping (what banks will flow down)

### TODO-3 — AVM QC rule mapping (controls, not a single accuracy number)

Create a mapping document that translates the AVM QC rule factors into vendor deliverables:

* "High level of confidence" → documented performance thresholds + evidence.
* Protection against manipulation → data integrity controls, audit logs, tamper-evident versioning.
* Conflict-of-interest controls → governance and access segregation.
* Random sample testing/reviews → testing plan + results reporting format.
* Nondiscrimination compliance support → transparency, auditability, and testing support artifacts.

Deliverables:

* "QC Controls Crosswalk" (rule factor → your control → evidence artifact).
* "Client responsibilities vs vendor responsibilities" page (what the lender must do vs what you provide).

### TODO-4 — Third-party risk / vendor due diligence packet

Assemble a procurement packet that banks can drop into their third-party risk process:

* Security overview (access control, encryption, logging, incident response, vulnerability management).
* Business continuity / disaster recovery summary.
* Subprocessor list + change notification process.
* Audit rights language (contract-ready).
* SLA template and uptime/latency reporting.

---

## TODO: Model documentation (validator-ready)

### TODO-5 — Model card + technical documentation set

Produce a versioned documentation set sufficient for independent validation:

* Model purpose and intended use.
* Training data sources, licensing constraints, update cadence, exclusions.
* Feature families and governance constraints (especially around sensitive attributes).
* Methodology overview (forecast architecture, uncertainty model, calibration method).
* Limitations and failure modes (thin markets, non-arm's-length sales, renovations, unique properties).
* Change management policy (when model changes, how clients are notified, how backtests are re-run).

### TODO-6 — Immutable versioning in API responses

Implement and document:

* `model_id`, `model_version`, `data_snapshot_id`, `calibration_version`.
* Per-response metadata: timestamp, geography/segment tags, confidence bucket tags.
* Changelog policy: semantic versioning + deprecation windows.

---

## TODO: Validation framework (point metrics still apply with fan charts)

### TODO-7 — Central estimate definition for evaluation (P50 policy)

Define and freeze:

* The representative point used for accuracy metrics is **P50 (median)** by default.
* If you publish mean, publish it separately; keep P50 as the evaluation anchor.
* In docs: explain why clients will still compute MdAPE from P50 even if you ship bands.

### TODO-8 — Backtesting protocol (out-of-time, leakage-safe)

Define a backtest protocol and implement it:

* Out-of-time evaluation: predictions must be generated as-of a fixed "anchor date."
* Sales used for evaluation must occur **after** the anchor date; ensure no leakage through updated assessor fields, post-sale records, etc.
* Sampling rules: arm's-length filters, outlier handling, and property-type stratification.

Outputs:

* Backtest report by: geography × property type × price tier × liquidity bucket.
* Time-sliced results (rolling quarters or months) to show stability.

### TODO-9 — Segment-level performance reporting (ratio-study style + AVM metrics)

For each segment, compute and publish:

* **MdAPE** for P50.
* Error buckets: PP10 (or equivalent): share of outcomes within ±10% (and optionally ±20%).
* Ratio-study metrics:

  * median ratio (level),
  * dispersion proxy (COD or equivalent),
  * price-related bias (PRD/PRB proxies if available).
* Tail risk: overvaluation frequency and magnitude (e.g., P95 error, exceedance rate beyond ±20%).

Deliverables:

* "Segment Performance Table" (machine-readable + PDF).
* "Eligible / ineligible segment list" based on tolerances.

### TODO-10 — Define "acceptable" performance bands by use case (gating thresholds)

Create a policy table that clients can adopt:

* **Underwriting / collateral decision support:** target P50 **MdAPE ≤ 10%** in eligible segments.
* **Portfolio monitoring / screening:** P50 **MdAPE ≤ 15%** may be acceptable with calibrated bands.
* **MdAPE > 20%:** treat as advisory-only (non-decision-critical), unless client policy explicitly allows it with compensating controls.

Important: label these as **procurement/industry practice targets**, not statutory requirements.

---

## TODO: Fan chart (uncertainty) requirements — calibration + sharpness

### TODO-11 — Band definition standardization

Define band levels and required outputs:

* P10/P50/P90 (minimum).
* Optionally P05/P50/P95 for tighter governance.
* Publish band widths as % of P50 (so bands are comparable across price levels).

### TODO-12 — Calibration testing (coverage)

For each segment and horizon, compute:

* Coverage@80, Coverage@90 (empirical): fraction of realized outcomes within the stated band.
* Calibration curves: predicted quantile vs realized frequency.
* Sharpness: average width of the bands (must not be trivially wide).

Acceptance spec (for internal QA and external packet):

* Coverage@90 should be close to 90% within a tolerance band you define (e.g., 88–92%).
* If coverage fails, the segment/horizon becomes "restricted use" until recalibrated.

### TODO-13 — Confidence score / bucket mapping

If you publish a confidence score (or bucket):

* Provide a calibration table: score bin → expected MdAPE distribution and PP10 rate.
* Provide minimum recommended score thresholds per use case.
* Ensure score is stable across model versions or explicitly version it.

---

## TODO: Future-horizon forecasting ("compounded bands")

### TODO-14 — Horizon-specific fan charts (1y/3y/5y) with validated coverage

Define horizons you ship (e.g., 1, 3, 5 years), and for each:

* Provide P10/P50/P90 future value distribution.
* Validate coverage using realized outcomes at the horizon (when available) or proxy outcomes.

### TODO-15 — Uncertainty growth model and documentation

Implement and document a horizon uncertainty model:

* A base current-value uncertainty component.
* A horizon growth component (annual return uncertainty).
* A policy for correlation / regime shifts (stress widening during market shocks).

Publish:

* (σ_H) (or equivalent) by horizon and segment.
* How the model transitions from current value to future value.

### TODO-16 — Horizon gating

Define "allowed use" thresholds by horizon:

* Underwriting support likely restricted to shorter horizons and higher confidence buckets.
* Longer horizons default to portfolio analytics unless exceptional evidence exists.

---

## TODO: Drift monitoring and recalibration policy (operational controls)

### TODO-17 — Drift metrics and triggers

Monitor continuously (by segment):

* Central accuracy drift: MdAPE trend and error bucket deterioration.
* Calibration drift: Coverage@90 deviation from target.
* Data drift: feature distribution drift, missingness shifts, schema changes.
* Market shock detection: regime flags that trigger widening bands or disabling.

Define triggers:

* When a segment/horizon is disabled.
* When model retraining is required.
* When recalibration-only is sufficient.

### TODO-18 — Recalibration and change management playbook

Write a formal playbook:

* Retraining cadence (scheduled + trigger-based).
* Recalibration method and validation.
* Release process: canary, parallel run, rollback, client notification.
* Artifact regeneration: all reports updated for each released version.

---

## TODO: Anti-manipulation, independence, and liability controls

### TODO-19 — Anti-manipulation and integrity controls

Implement and document:

* Input validation and anomaly detection (e.g., extreme feature combos).
* Tamper-evident audit logs for requests/responses (hash chaining optional).
* Rate limiting, abuse detection, and client authentication.

### TODO-20 — Independence / "no steering" workflow constraints

Design restrictions for regulated workflows:

* Avoid UI/features that allow clients to "tune" a value to a desired number without traceability.
* Ensure overrides are logged, justified, and segregated from core model outputs.

### TODO-21 — Contractual and product liability posture

Prepare standard terms for:

* Intended use disclaimers.
* Limits on reliance for specific decisions unless explicitly contracted.
* Indemnity posture appropriate for data/model vendors.
* Audit rights and compliance cooperation language (especially for bank clients).

---

## TODO: Standard deliverables package (what the "separate agent" should record)

### TODO-22 — "Bank-ready AVM Vendor Packet" (single folder, versioned)

Include:

1. Positioning + intended use statement
2. Model card + methodology doc
3. Data provenance + licensing summary
4. Backtest report (out-of-time)
5. Segment performance tables (MdAPE, PP10, ratio-study metrics)
6. Fan chart calibration report (Coverage@80/90 + sharpness)
7. Drift monitoring spec + dashboards (sample outputs)
8. Recalibration/change management policy
9. QC controls crosswalk (rule factor → control → evidence)
10. Third-party risk/security packet (SOC2-aligned narrative even if uncertified)
11. API spec with versioning fields and example responses
12. "Model Use Policy" one-pager (gating thresholds by use case + horizon)

---

## Calibration Notes

* **Persona used:** Senior bank model risk / collateral valuation validation lead.
* **Difficulty tuning:** ~70% ZPD, ~20% stretch (procurement-grade artifactization), ~10% aspirational (turning compliance + uncertainty into a product moat).
* **Agent tip:** When handing this to an agent, require output of **one consolidated "acceptance table"** (use case × horizon × confidence bucket → allowed/blocked + required evidence). This single table prevents scope creep and accelerates diligence.

---

## References

* [Legal Issues with AVM API](https://chatgpt.com/c/69865a31-ec14-832a-94c8-60943019548c)
* [QVM Changes and Benefits](https://chatgpt.com/c/6848dfe7-1900-8004-9e24-64477624c69b)
