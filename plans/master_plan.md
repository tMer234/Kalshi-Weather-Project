# Master Plan — Kalshi Weather Fair-Value System: All Remaining Build Steps

## Context

**The project** (defined across the three planning docs + NHIGH contract rules): estimate
calibrated fair probabilities for Kalshi binary weather-threshold contracts (e.g. NHIGH =
"Central Park daily max > τ°F"), compare them to market-implied probabilities, quote/bet
only when the edge survives fees, spread, inventory risk, and adverse selection — first on
paper, then (per user decision) **live with real capital**.

**What is already built** (this repo, `NWS data ingestion pipeline v1`, commit `efba24a`):
Stage 0's *weather half* is done — NWS gridpoint forecast vintages (`grid_forecasts`,
append-only with `issued_time`/`valid_start`/`horizon_hours`) and settlement ground truth
(`climate_reports`, parsed from the same CLI product Kalshi settles on), in DuckDB, with
46+ fixture-based tests and a data dictionary (`docs/data_dictionary.md`).

> **Update 2026-07-11**: Phase 0b (Kalshi collector) is also built and verified, plus a
> unified `kalshi-weather` CLI and the Phase 0c backfill tooling (IEM climate reports;
> Kalshi settled history + candlesticks). See the STATUS notes in Phases 0b/0c and
> `docs/runbook.md` for operations. Still pending: cron scheduling, running the actual
> backfills, and the NDFD forecast-archive backfill (unbuilt, now the binding data
> constraint).

**What remains** (this plan): the Kalshi market-data collector (Stage 0's other half), the
residual dataset, the M2 probability models (Gaussian → empirical → logistic), the
calibration layer (Platt → isotonic), proper-scoring evaluation + walk-forward validation,
the paper quoting simulator, risk controls, and finally live execution with real capital.

**Deviations from the PDFs, already decided in this repo**: DuckDB (not parquet files) is
the storage layer; the 6 cities/settlement stations are hardcoded; forecasts are °C /
settlement is °F (convert at the residual step); the "climatological day" is the CLI
report's day, not midnight-to-midnight (see data dictionary §5.3, §6).

**Source key** (used throughout; RL# = numbered item in the Pre-Coding Reading List PDF):
- **[Proposal]** — *Kalshi Weather Fair-Value & Execution Simulator Proposal*
- **[BuildOrder]** — *Full Model Stack Build Order: Stage-by-Stage Guide*
- **[MathDoc]** — *Probabilistic Weather Model: Mathematical Details & Reading List*
- **[NHIGH]** — Kalshi NHIGH contract rules PDF
- **RL1** Casella & Berger, *Statistical Inference* ch. 1–5 · **RL2** Gelman et al., *BDA* ch. 1–3, 6
  · **RL3** K. Murphy, *ML: A Probabilistic Perspective* ch. 3, 8 · **RL4** Gneiting & Raftery (2007),
  *Strictly Proper Scoring Rules* · **RL5** Brier (1950) · **RL6** NWS/NOAA verification overview
  · **RL7** Wilks (2009), *Extending Logistic Regression…* · **RL8** Murphy & Winkler (1987)
  · **RL9** Platt (1999) · **RL10** Zadrozny & Elkan (2002) · **RL11** sklearn calibration docs
  · **RL12** Niculescu-Mizil & Caruana (2005) · **RL13** Wolfers & Zitzewitz (2004)
  · **RL14** Manski (2006) + Wolfers-Zitzewitz reply · **RL15** Avellaneda & Stoikov (2008)
  · **RL16** Glosten & Milgrom (1985) · **RL17** Northlake Labs, *Market Making on Prediction
  Markets* · **RL19** Northlake Labs, *What I Learned Losing Money on Kalshi Weather Markets*
  · **RL20** Göransson-Gaspar (2025), *Modeling Binary Prediction Markets* · **RL21** Hastie et
  al., *ESL* ch. 4–5 · **RL23** GitHub `Oalkhadra/prediction-market-trading`

Phases below follow [BuildOrder]'s stage numbering and its gating rule: **a stage begins
only when the previous stage's gate passes.** Each phase lists the math it depends on and
the specific source to read first.

---

## Phase 0b — Kalshi Market-Data Collector (completes Stage 0)

> **STATUS 2026-07-11: BUILT AND VERIFIED** (except cron scheduling). Implemented as
> `kalshi_client.py` + `kalshi_ingest.py` + `markets`/`market_snapshots`/`market_outcomes`
> tables; run via `kalshi-weather ingest kalshi` (see docs/runbook.md). Verified live:
> 6/6 series collect; Kalshi's settled values matched our CLI-parsed values 12/12
> station-days. The NHIGH spot-check against the official CFTC filing confirmed the
> resolution assumptions below (one fix: current markets expire "first 7/8 AM ET after
> the data release", not the filing's 10 AM). Strike semantics verified live:
> `greater`/`less` strict, `between` inclusive; the ticker's -T/-B suffix does NOT
> identify the side — only `strike_type` does. **Open item: install the cron entries**
> (runbook §1.3) — without them, quote history only accumulates when run by hand.

The missing half of Stage 0: the pipeline the original NWS plan explicitly deferred.
New module in this repo (`src/kalshi_weather/kalshi_client.py`, `kalshi_ingest.py`)
mirroring the existing NWS patterns: `nws_client.py`'s retry/conditional-GET style,
`ingest.py`'s per-endpoint error isolation, `ingest_runs` auditing, idempotent upserts.

**Build:**
1. **Market universe**: poll Kalshi public REST (no API key needed for market data) for the
   6 cities' daily high/low series (NHIGH etc.). New DuckDB tables (extend `schema.sql`):
   - `markets` (ticker, event_ticker, series, station_id, strike/threshold τ, strike_type
     — note [NHIGH]: "greater than" is *strict*, "between" is inclusive on both ends —
     close_time, status)
   - `market_snapshots` (ticker, ts, yes_bid, yes_ask, last, volume, open_interest) —
     append-only mid/quote history; order-book *depth* snapshots optional now, required
     by Phase 8
   - `market_outcomes` (ticker, resolved_outcome, settlement_value, resolution_ts)
2. **Threshold parser**: ticker/title → (station, obs_date, τ, direction). Cross-check the
   resolved obs_date semantics against [NHIGH] (expiration ≈ 7–8 AM ET after the CLI final;
   revisions after expiration ignored — matches our `climate_reports` final-report logic).
   *(As built: strikes come structured from the API — floor_strike/cap_strike/strike_type —
   only obs_date is parsed, from the event ticker.)*
3. Cron it alongside `run_ingest.py`; snapshot cadence ~5–15 min while markets are open.
   *(Superseded: scheduling is now planned via GitHub Actions + a private GCS bucket,
   not local cron, with quotes/resolutions/grid/CLI reports each on their own cadence —
   see `plans/data_automation_plan.md`.)*
4. Idempotency test in the style of `tests/test_ingest_upsert.py`.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Why mid-price ≈ market-implied probability, and its biases (favorite-longshot, wealth-weighting) | **RL13** Wolfers & Zitzewitz (2004) — read first; then **RL14** Manski (2006) + reply for when price ≠ mean belief |
| Why NOT to model the price as a mean-reverting time series (price → 0/1 at resolution by construction) | **RL20** Göransson-Gaspar (2025); [MathDoc] §1.3 "naive mean-reversion trap" |
| Market-implied baseline p̂_market = mid (or spread-penalized one-sided estimate) | [Proposal] "Model v0"; **RL13** |

**Gate ([BuildOrder] Stage 0):** collector runs daily without babysitting; every market row
has parsed threshold/station/date; snapshots join cleanly to `grid_forecasts` timestamps.

---

## Phase 0c — Residual Dataset (the empirical foundation)

**Build:** a DuckDB view/table `residuals`: one row per (station, obs_date, issued_time) =
NWS forecast max/min (°C→°F converted) minus CLI-settled value, with `horizon_hours` and
horizon bucket (0–6h, 6–24h, 24–48h, 48–72h per [MathDoc] §2.1). The join recipe, unit
conversion, and window caveats are already worked out and tested in
`docs/data_dictionary.md` §6 — use that SQL as the starting point, including its two
gotchas (degC vs degF; forecast max window 8AM–9PM ≠ settlement day 1AM–12:59AM).

> **SCOPED 2026-07-12 (was previously undefined — the §6 join example pulls every vintage
> ever collected, unbounded):**
> - **Max horizon: 72h.** Confirmed live that NHIGH markets only open ~24h before
>   `obs_date` and close ~39–41h after opening — so nothing beyond ~40h out is ever
>   live-tradeable. 72h is kept anyway (not capped at 40h) because it matches the
>   existing 4-bucket scheme and gives real value beyond direct trading: (a) curve-fitting
>   leverage if σ(h) is ever fit as a smooth function rather than discrete per-bucket
>   values — borrowing strength from 40–72h stabilizes the curve near 24–40h while data
>   is still sparse; (b) trend/momentum features for Phase 7's already-planned "recent
>   NWS bias (rolling residual mean)" — e.g. how the forecast moved from 3 days out to
>   now. NWP skill growth roughly saturates by day 3–4 (the plan's own cited MAE curve
>   flattens there), so days 4–7 were judged to add backfill cost without adding
>   modeling value for either use case — excluded. **Enforced at ingest as of
>   2026-07-13** (`ingest.py`'s `MAX_HORIZON_HOURS`) — the live collector no longer
>   stores periods beyond 72h at all (same NWS request either way; just unused rows
>   before). Pre-existing rows beyond 72h are being cleared manually, not backfilled.
> - **Same-day cutoff: exclude maxTemperature forecasts issued after 17:00 station-local
>   on `obs_date`** (concretely: `horizon_hours < -6` relative to the 8AM–9PM window).
>   Grounded in real `value_time` data: the daily high occurs on average at 13.8–15.2h
>   local across all 6 stations, tail observed to 16:08 (Denver). Forecasts issued later
>   than that are, on average, nowcasting an already-occurred peak, not predicting one —
>   left in, they'd dilute the 0–6h bucket's σ with near-zero-uncertainty rows that don't
>   represent genuine forecast skill.
> - This directly scopes the NDFD backfill (see Phase 0c backfill status below): 72h of
>   forecast history per obs_date, not the full ~168h(7-day) horizon NDFD archives —
>   roughly a 55–60% cut in that backfill's download volume.

**Bootstrap history (decision embedded in plan):** live collection started ~2026-07-09, so
organic accumulation gives only ~180 residuals/station/6-months. To fit σ per
(horizon-bucket × station) sooner, **backfill**: Iowa Environmental Mesonet archives CLI
text products (feed them through the existing `cli_parser.py`), and NOAA NDFD archives
historical gridpoint forecasts. Backfill lands through the same upsert paths, flagged by a
`source` column. If backfill proves painful, the fallback is simply waiting — the plan's
phase order doesn't change, only the calendar.

> **STATUS 2026-07-11: backfill tooling partially built** (see docs/runbook.md §2):
> - `kalshi-weather backfill nws-cli` — historical **climate reports** via IEM AFOS
>   (verified: IEM text parses byte-identically through `cli_parser.py`); rows flagged
>   `source='iem_afos'`.
> - `kalshi-weather backfill kalshi` — historical **settled markets + outcomes** plus
>   **candlestick price bars** (`market_candles` table) reconstructing pre-collector
>   quote history.
> - **NOT built: the NDFD forecast-vintage backfill** (GRIB2 archive at NCEI; heavy —
>   needs grib tooling and grid-point extraction). Until it exists, `grid_forecasts`
>   history starts 2026-07-09 and the residual dataset is bounded by it. This is now the
>   single binding constraint on Phase 1's σ fitting; decide build-vs-wait when Phase 0c
>   starts. No backfills have been RUN yet — tooling is tested but the historical pulls
>   themselves are pending a user decision on date range.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Random variables, CDFs, sample moments (mean/sd/skew/kurtosis of residuals) | **RL1** Casella & Berger ch. 1–5 |
| Why residual *bias* (non-zero mean) matters — shifts every probability estimate | [BuildOrder] Stage 1 "Topics to Master" |
| Horizon stratification — why pooling horizons corrupts σ | [BuildOrder] Stage 1; NWS MAE growth 2–4°F (day 1–4) → 5–6°F (day 5–8) per [MathDoc] §2.1 / RL ref #8 (MRF verification) |

**Gate:** clean residual dataset exists; every row has both issued and valid timestamps
([BuildOrder] Stage 0's "single most important design decision").

---

## Phase 1 — Gaussian Baseline Model (M2 v1)

**Build:** `src/kalshi_weather/model/m2_gaussian.py` + `sigma_estimates` table.

- Model: observed value Y = f + ε, ε ~ N(0, σ²(h_bucket, station));
  **p_raw = 1 − Φ((τ − f) / σ)** via `scipy.stats.norm.cdf`. Direction/strictness per
  [NHIGH] payout criterion (integer °F settlement makes P(Y > τ) vs P(Y ≥ τ) materially
  different — use a continuity-aware threshold, e.g. τ + 0.5 for "greater than").
- σ = sample sd of residuals per (horizon bucket × station) cell; refit on a schedule.
- EDA notebook: residual histograms + QQ-plots per horizon bucket; σ(h) monotonicity check.
- Output table `predictions` (ts, ticker, model_version, p_raw) — every later model writes
  here too, keyed by `model_version`.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Normal distribution, Φ, z-scores; P(Y > τ) = 1 − Φ((τ−f)/σ) | **RL1** Casella & Berger ch. 1–5; scipy.stats.norm docs (RL implementation ref #22) |
| Threshold-exceedance framing (not price prediction) | [MathDoc] Part 1–2; **RL7** Wilks (2009) intro sections |
| QQ-plot interpretation (S-curves = heavy tails) | [BuildOrder] Stage 1 "Topics to Master" |
| Known Gaussian failure modes on Kalshi weather specifically (fat tails, skew, season dependence) | **RL19** Northlake Labs postmortem — short, read in full |

**Gate ([BuildOrder]):** σ increases monotonically with horizon; sanity check p̂ ≈ 0.5 when
τ ≈ f; QQ-plots documented (their non-normality sets Phase 6's urgency).

---

## Phase 2 — Evaluation Infrastructure (Proper Scoring)

**Build:** `src/kalshi_weather/evaluation/` — `scoring.py` (Brier, log loss, BSS, ECE,
sharpness), `reliability.py` (reliability diagrams), `reports.py` (per-horizon-bucket ×
per-probability-bucket breakdown). Evaluate M2 v1 raw vs the **market midpoint baseline**
(not climatology — [BuildOrder] Stage 2). Chronological 60/20/20 train/validation/test
split; the test window is touched once, at the end.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Proper scoring rules — why Brier/log-loss reward honest probabilities and accuracy/AUC are wrong here | **RL4** Gneiting & Raftery (2007) — the canonical reference; skim **RL5** Brier (1950) for the original |
| Brier decomposition BS = REL − RES + UNC (calibration vs resolution vs base-rate) | **RL8** Murphy & Winkler (1987); **RL6** NOAA verification overview for the applied version |
| Brier Skill Score vs a reference forecast: BSS = 1 − BS_model/BS_market | **RL6**; [BuildOrder] Stage 2 (BSS > 0 necessary but NOT sufficient after fees) |
| Reliability diagrams + ECE construction and reading | **RL12** Niculescu-Mizil & Caruana (2005); **RL11** sklearn calibration docs |
| Chronological splits / no look-ahead | [MathDoc] §3.4 validation protocol |

**Gate:** raw BSS vs market computed per horizon bucket. **BSS > 0 somewhere** = proceed.
BSS ≤ 0 everywhere = stop and diagnose (more data / bias correction), don't calibrate noise.

---

## Phase 3 — Platt Scaling Calibration (C v1)

**Build:** `src/kalshi_weather/calibration/platt.py` + `calibration_store` table
(A, B, fit window, per horizon bucket). Implementation detail from [BuildOrder] Stage 3:
fit the sigmoid on the **z-score** (τ−f)/σ rather than on p_raw — more numerically stable
for a normal-CDF model. Fit A, B by maximizing Bernoulli log-likelihood
(`scipy.optimize.minimize`, BFGS) on the *validation* window only. Re-run the full
Phase 2 suite on p_cal.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Platt scaling: p_cal = 1/(1+exp(A·s+B)), derivation + MLE fitting | **RL9** Platt (1999) — primary; **RL11** sklearn docs (`CalibratedClassifierCV(cv='prefit')`) for the exact usage pattern |
| MLE mechanics (write the Bernoulli log-likelihood, differentiate, optimize) | **RL1** Casella & Berger (estimation chapters); **RL3** K. Murphy ch. 8 |
| When Platt is the right fix (sigmoid-shaped distortion) vs not | **RL12** Niculescu-Mizil & Caruana (2005) |
| Data leakage via calibration set (must be chronologically after σ's training window) | [BuildOrder] Stage 3; [MathDoc] §3.4 |

**Gate:** reliability diagram tightens toward diagonal; post-Platt BSS ≥ pre-Platt BSS.
If BSS doesn't improve, the model lacks *resolution* — invest in features (Phase 7), not
more calibration.

---

## Phase 4 — Walk-Forward Validation & Horizon-Stratified Reporting

**Build:** `evaluation/walkforward.py` — expanding-window walk-forward: fit σ on past,
fit Platt on next window, predict the following, roll. Report BSS/ECE distribution across
folds, stratified by horizon bucket, probability bucket, season, station. Bootstrap CIs
on BSS across folds. Also report **fill-relevant** stats: at hurdle δ, how many tradeable
quotes/day survive.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Time-series cross-validation / rolling forecast origin; why random k-fold is categorically wrong here | Hyndman & Athanasopoulos, *Forecasting: Principles & Practice* §5.10 (OTexts, free — [BuildOrder] ref #23) |
| Expanding vs rolling window trade-off (structural breaks, e.g. NWS model upgrades) | [BuildOrder] Stage 4 "Topics to Master" |
| Bootstrap confidence intervals on BSS | Efron & Tibshirani, *An Introduction to the Bootstrap*, ch. 1–6 (canonical addition — the reading list names the need in [BuildOrder] Stage 4 but no text) |
| Seasonality confounding in stratified evaluation | [BuildOrder] Stage 4 |

**Gate (the critical research→execution gate, [BuildOrder]):** BSS > 0 across **most folds
and horizon buckets**, with CIs excluding 0 in the buckets you intend to trade. Do not
build Phase 8 without this — "building the simulator without this validation is
equivalent to trading without a model."

---

## Phase 5 — Isotonic Regression Calibration (C v2)

**Build when:** Platt-calibrated reliability curves still deviate in the tails AND ≥500–1000
resolved contracts per horizon bucket exist ([BuildOrder] Stage 5). `calibration/isotonic.py`
via `sklearn.isotonic.IsotonicRegression(out_of_bounds='clip')`. A/B against Platt per
bucket on ECE + BSS; deploy the winner per bucket (mixing Platt short-horizon / isotonic
long-horizon is explicitly valid).

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Isotonic calibration + PAV algorithm | **RL10** Zadrozny & Elkan (2002) — primary; trace a 5-point PAV example by hand per [BuildOrder] Stage 5 |
| Why PAV is the exact solution to least-squares-under-monotonicity | Barlow et al. (1972) / Univ. of Minnesota Stat 8054 isotonic lecture notes ([BuildOrder] ref #28) |
| Platt-vs-isotonic decision table (data volume, distortion shape, tails) | **RL12**; [BuildOrder] Stage 5 comparison table |
| ECE = Σ (n_b/N)·|acc_b − conf_b| | [BuildOrder] Stage 5; **RL11** |

**Gate:** deploy whichever calibrator has lower ECE per bucket; document the choice.

---

## Phase 6 — Empirical / KDE Error Distribution (M2 v2)

**Build when:** ~200+ residuals per (horizon × station) cell. `model/m2_empirical.py`:
p_raw = 1 − F̂(τ − f) where F̂ is the ECDF (`np.searchsorted` on sorted residuals) or a
KDE-CDF (`scipy.stats.gaussian_kde` + `integrate_box_1d`), bandwidth via Silverman then
cross-validated on held-out log-loss. Head-to-head BSS vs Gaussian on the test window;
expect the biggest gains exactly where Phase 1's QQ-plots were worst.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| ECDF, convergence (Glivenko–Cantelli), tail unreliability at small n | **RL1** Casella & Berger; [BuildOrder] Stage 6 |
| KDE, bandwidth bias-variance trade-off, Silverman's rule | Silverman, *Density Estimation for Statistics and Data Analysis* (1986) ch. 2–3 (canonical addition); practical: scipy `gaussian_kde` docs + *Python Data Science Handbook* KDE chapter ([BuildOrder] refs #32–34) |
| Bandwidth selection by leave-one-out likelihood CV | [BuildOrder] Stage 6 "Topics to Master" |

**Gate:** replace Gaussian only where empirical BSS is *materially* better per bucket.

---

## Phase 7 — Logistic / Feature-Engineered Model (M2 v3)

**Build:** `model/m2_logistic.py` + `features/feature_engineering.py`. Regularized logistic
regression on: z-score (core signal), horizon sin/cos, station one-hot, month sin/cos,
recent NWS bias (rolling residual mean — regime drift), **market-implied probability**
(turns the model into a Bayesian update *on top of* the market), days-to-resolution.
Calibrate its output with the Phase 5 winner. Three-way walk-forward comparison
(Gaussian+Platt vs empirical+isotonic vs logistic+cal). Feature-importance check: if
market mid dominates z-score, the model has no independent information — stop and say so.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Logistic regression for weather threshold exceedance (the exact MOS framework) | **RL7** Wilks (2009) — the direct methodological reference |
| Derivation: Bernoulli likelihood → MLE → gradient; discriminative vs generative | **RL3** K. Murphy ch. 8 (and ch. 3); **RL21** Hastie et al. ESL ch. 4 |
| L1/L2 regularization, cross-validated C | **RL21** ESL ch. 4–5 |
| Market price as an aggregated-information prior; Bayesian-update framing | **RL2** Gelman et al. BDA ch. 1–3; **RL13** |
| Multicollinearity z-score × market mid (VIF check) | [BuildOrder] Stage 7 |
| A directly comparable system + its finding (markets overprice uncertainty ~1.27×) | **RL23** `Oalkhadra/prediction-market-trading` — study methodology *and* limitations |

**Gate:** replace incumbent only if walk-forward BSS improvement exceeds a pre-registered
threshold.

---

## Phase 8 — Paper Quoting Simulator (Execution Layer)

**Build:** `src/kalshi_weather/simulator/` — `orderbook.py` (snapshot+delta replay from
Phase 0b's collector; add Kalshi WebSocket order-book collection here if not already),
`quote_engine.py`, `fill_model.py`, `fee_model.py`, `risk_controls.py`. New tables:
`paper_orders`, `paper_fills` (with 5-min/30-min/resolution markouts).

- **Trading rule** ([Proposal]/[MathDoc]): quote YES only when
  p_cal − p_ask > δ + fee + α_adverse (mirror for NO), δ = 1–3 prob. points.
- **Quote placement**: centered on p_cal ± half-spread, shifted by Avellaneda-Stoikov
  inventory skew adapted to binary settlement: skew ∝ q·γ·σ_p²·T with σ_p² ≈ p(1−p).
- **Fees**: Kalshi formula `fee = ceil(0.07 × C × P × (1−P))` — exact, not flat
  ([Proposal]); worst near P=0.5.
- **Fill realism ladder** ([Proposal]): Level 1 (trade-through) → Level 2 (queue-aware
  using displayed size ahead). Target Level 2.
- **Diagnostics**: gross vs net P&L, fill rate, realized edge (p_cal − fill price),
  drawdown, adverse-selection rate (fraction of fills followed by adverse moves), fraction
  of edges rejected by each filter.

**Math/stats & sources:**
| Concept | Source |
|---|---|
| Inventory-risk quoting: reservation price + optimal spread/skew | **RL15** Avellaneda & Stoikov (2008) — primary; **RL17** Northlake Labs guide for the binary-contract (0/1 settlement) adaptation |
| Adverse selection: why makers widen against informed flow | **RL16** Glosten & Milgrom (1985); calibrate the α buffer from your own markout data per [MathDoc] §5.1 |
| Fee math and thin-probability-bucket traps | Kalshi fee schedule ([Proposal]); **RL19** postmortem's fee examples |
| Binary settlement inventory risk (loss = q × 1 on the losing side) | [MathDoc] §5.2; [BuildOrder] Stage 8 |

**Gate ([BuildOrder] Stage 8):** positive **net** P&L (after fees) across the full
walk-forward window, drawdown within limits, adverse-selection rate not pathological.

---

## Phase 9 — Risk Controls & Robustness ([Proposal] week 5)

**Build (all hard-coded before any live code exists):** per-market and per-day exposure
caps; daily loss limit; no-trade windows near forecast updates and near expiration
(adverse selection spikes — [NHIGH]: trading until 11:59 PM ET, CLI final ~1–3 AM);
stale-data kill-switch (ingest heartbeat: if `ingest_runs` shows no fresh NWS/Kalshi data,
cancel quotes); probability clipping; exclusion of contracts with ambiguous settlement.
**Sensitivity analysis**: sweep δ, fee buffer, fill assumptions, α — the strategy must not
depend on one optimistic setting. Markout monitoring per [Proposal].

Sources: [Proposal] risk-controls table (the checklist to implement verbatim);
**RL17**/**RL19** for prediction-market-specific failure modes.

---

## Phase 10 — Live Betting (user-directed scope extension beyond the PDFs)

The PDFs stop at paper trading with live capital "optional." User has directed the plan to
extend through live betting. Structure it as three sub-stages with the same gating
discipline:

1. **10a — Kalshi demo environment**: authenticated API (key management in `.env`, never
   in git), order create/cancel/status against Kalshi's **demo** endpoints; the live
   engine is the Phase 8 simulator with the fill model swapped for real (demo) fills.
   Reconcile: demo fills vs simulator-predicted fills → recalibrate `fill_model.py`.
2. **10b — Tiny live bankroll**: real orders, strict Phase 9 limits, position sizes far
   below Kelly (see below). Run ≥1 month. Compare realized edge/markout vs paper.
3. **10c — Scale-up rule**: predefine the criteria (realized net edge CI > 0 over N fills)
   under which size increases, and the drawdown level at which the system turns itself off.

**Bet sizing math** (not in the reading list — canonical additions):
| Concept | Source |
|---|---|
| Kelly criterion for binary bets: f* = (p(1+b) − 1)/b; for a $1 binary at price c, f* = (p − c)/(1 − c) on YES | Kelly (1956), *A New Interpretation of Information Rate* |
| Why fractional Kelly (¼–½) in practice: parameter uncertainty makes full Kelly severely overbet; drawdown/growth trade-off | Thorp (2006), *The Kelly Criterion in Blackjack, Sports Betting, and the Stock Market* (in *Handbook of Asset & Liability Management*) — the practical treatment |
| Estimation-error shrinkage of p̂ before sizing (your p is itself uncertain) | **RL2** Gelman et al. BDA ch. 6 (model checking) applied to the calibration posterior; conservative default: size on the CI lower bound of edge |

Note: quoting (Phase 8's maker logic) and sizing (Kelly) interact — inventory caps from
Phase 9 bind first at small bankrolls; that's intended.

**Gate to stay live:** rolling realized-edge and markout monitors stay positive;
kill-switch events zero; any gate failure drops the system back to paper mode
automatically.

---

## Cross-cutting engineering notes

- **Storage**: everything lands in the existing DuckDB via `schema.sql` + idempotent
  upserts; reuse `db.py`, `nws_client.py` retry patterns, `ingest_runs` auditing,
  `http_cache` conditional-GET for any polled REST endpoint.
- **Repo layout**: keep one repo; add `model/`, `calibration/`, `evaluation/`,
  `simulator/` packages under `src/kalshi_weather/` (the [Proposal] repo sketch, adapted
  to this repo's conventions). Notebooks in `notebooks/`, gitignored outputs.
- **Every prediction is written to the DB with model_version + timestamp** before the
  outcome is known — this is what makes all later evaluation honest.
- **Source PDFs** referenced above ([Proposal], [BuildOrder], [MathDoc], [NHIGH]) live in
  the local `references/` folder, which is deliberately gitignored — keep them there.
- **Testing**: continue the repo's fixture-based, no-network style; scoring functions get
  hand-computable unit tests (e.g. Brier of known toy sets); simulator gets deterministic
  replay tests.

## Verification (per phase, end-to-end)

- **0b**: run collector live for 48h; row counts per ticker sane; thresholds spot-checked
  against Kalshi's website; idempotency test passes.
- **0c**: `SELECT` residuals for one known date/station and verify by hand against the CLI
  text (product_id link) and the raw gridpoint JSON.
- **1**: σ table monotone in horizon; p_raw(τ≈f) ≈ 0.5; predictions land in DB.
- **2–5**: metrics reproduce on fixtures with hand-computed values; reliability diagrams
  render; walk-forward runs end-to-end on backfilled history.
- **6–7**: three-way comparison notebook runs from a single command against the DB.
- **8–9**: simulator replays a recorded order-book day deterministically; risk-control
  unit tests (limits actually block orders; kill-switch fires on stale data).
- **10**: demo-env round-trip (place, query, cancel) verified before any real order; first
  live week reconciled trade-by-trade against paper predictions.

## The gates, in one table

| Phase | Gate to proceed |
|---|---|
| 0b/0c | Clean joined residual + market dataset exists |
| 1 | σ(h) monotone; sanity checks pass |
| 2 | Raw BSS vs market > 0 somewhere |
| 3 | Post-Platt BSS ≥ pre-Platt; reliability tightens |
| 4 | **BSS > 0 across most folds/buckets (CI excludes 0)** ← research/execution gate |
| 5 | Lower-ECE calibrator chosen per bucket |
| 6/7 | Replace models only on material walk-forward BSS gain |
| 8 | Positive net P&L after fees across walk-forward window |
| 9 | Robust across sensitivity sweep; controls tested |
| 10a→b→c | Demo reconciles with paper → live edge realized → predefined scale rule |
