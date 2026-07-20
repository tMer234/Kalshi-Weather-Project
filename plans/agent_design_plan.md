# Subagent Design Plan — `kalshi-quant` (repo domain expert)

Proposal for a dedicated Claude Code subagent, scoped to this repo, that carries the
project's data semantics, hard-won API facts, and the statistical/mathematical roadmap
so they don't need to be re-derived or re-explained every session. This doc is the design
review; nothing is installed or created until it's approved (§9).

---

## 1. Why a dedicated subagent

Three things make this repo a good fit for one, rather than just relying on `CLAUDE.md` +
ad hoc context each session:

- **Non-obvious domain facts that are expensive to rediscover.** README's "Notes &
  deviations" section and the three project memory files already list ~15 facts that cost
  real debugging time (CLI site-code-not-WFO-id, `?limit=` 400s, strike semantics, retired
  duplicate series, etc.). A subagent's system prompt is a better home for these than
  hoping memory recall fires — it's guaranteed context on every invocation, not a
  probabilistic retrieval.
- **A long, gated math roadmap.** `plans/master_plan.md` defines 10 phases (Gaussian →
  empirical → logistic models, Platt → isotonic calibration, proper scoring, walk-forward
  validation, Avellaneda-Stoikov quoting, Kelly sizing) each with a specific reading-list
  source and a hard gate. Getting phase ordering or the underlying math wrong isn't just a
  bug — Phase 10 puts real capital on the line, so the gating discipline itself needs to be
  enforced, not just known.
- **A widening but still-small codebase.** Today it's ingestion (`src/kalshi_weather/*.py`
  + `schema.sql`). The plan adds `model/`, `calibration/`, `evaluation/`, `simulator/`
  packages. An agent that already knows the DB schema, the upsert/idempotency patterns,
  and the test-fixture style will extend it more consistently than one that re-reads
  everything from scratch each time.
- **An operationally hazardous canonical datastore.** As of 2026-07-16 the canonical
  `weather.duckdb` lives in a private GCS bucket (`gs://kalshi-weather-prediction-db1`),
  kept in sync by four scheduled GitHub Actions workflows (`plans/data_automation_plan.md`,
  `docs/gcs_and_actions_setup.md`). A local file is now only ever a point-in-time working
  copy — a backfill or any other local write that gets pushed back without first pausing
  those workflows and pulling a fresh copy can silently discard live-collected data with no
  error (DuckDB files aren't mergeable; the upload just overwrites the whole object). This
  is exactly the class of fact — expensive to rediscover, catastrophic to get wrong once —
  this agent exists to hold permanently rather than leave to per-session memory recall.

## 2. Scope & non-goals

**In scope:** anything touching `src/kalshi_weather/**`, `schema.sql`, `scripts/*ingest*`,
the DuckDB schema/data semantics, `tests/**`, implementing/reviewing the math phases in
`master_plan.md`, and the GCS-hosted canonical DB's operational lifecycle — pausing/
resuming the four `ingest-*` GitHub Actions workflows around a safe backfill, the
pull/push discipline, and verifying scheduled runs actually fire (`gh run list`), per
`docs/gcs_and_actions_setup.md` and `plans/data_automation_plan.md`.

**Non-goals for v1:**
- Not a general-purpose repo agent — for unrelated chores (README typo fixes, git hygiene)
  the default session is fine and cheaper.
- Not a live-trading execution agent. Phase 10 (real capital) should stay under tighter,
  explicit human review regardless of what this agent recommends — it can *write* the Phase
  10 code and reason about Kelly sizing, but "turn on live trading" is a human action, not
  something to let an agent's tool-use loop reach for.
- Single agent, not split by phase. Considered splitting into a "data-pipeline" persona and
  a "quant/stats" persona, but the phases are too interdependent (residuals feed models feed
  calibration feed the simulator) — one agent that knows the whole spine is more useful than
  two that have to hand off context. Revisit if Phase 8+ (simulator/execution) grows enough
  to justify its own agent.

## 3. Tool access

Full dev toolset, minus `Agent` (no recursive subagent spawning — this agent should do the
work, not delegate it further) and `Artifact` (not this project's output medium):

`Read, Grep, Glob, Edit, Write, Bash, NotebookEdit, WebSearch, WebFetch`

Rationale for the notable inclusions:
- **Bash** — needed to run `pytest`, query the DuckDB file via the `duckdb` CLI/Python for
  EDA, run the ingest scripts, and — now that the canonical DB lives in GCS — drive `gh`
  (pause/resume the four `ingest-*` workflows, check `gh run list` for scheduled-run
  health) and `gcloud storage cp` (pull/push against `gs://kalshi-weather-prediction-db1`,
  ideally with `--if-generation-match` to fail loudly instead of silently clobbering).
  Inherits this project's existing permission rules (`.claude/settings.json` already
  denies `Read(references/**)` and `Read(data/**/*.duckdb)` at the project level — those
  apply regardless of this agent's own tool list).
- **NotebookEdit** — `master_plan.md` explicitly calls for EDA notebooks (residual
  histograms, QQ-plots, reliability diagrams) in a gitignored `notebooks/` dir starting
  Phase 1.
- **WebSearch/WebFetch** — for verifying live API behavior against NWS/Kalshi (the pattern
  that produced the memory-file facts) and for pulling Kalshi's public regulatory docs,
  since `references/**` is deliberately read-denied (see §7).

## 4. Model

**Recommend `model: opus`**, not inherit/sonnet-default.

This is a deliberate cost/quality tradeoff, not the default choice: most of this repo's
remaining work is math-correctness-sensitive in a way that compounds — a subtly wrong MLE
gradient in Platt scaling, or a mis-signed inventory skew in the Avellaneda-Stoikov
adaptation, doesn't fail loudly, it just quietly loses money once Phase 10 goes live. The
master plan's own gating language ("building the simulator without this validation is
equivalent to trading without a model") is about exactly this risk. Opus's stronger math
reasoning is worth the extra cost here in a way it wouldn't be for, say, the ingestion
glue code that's already done.

If cost becomes a concern, `inherit` is the fallback — the Agent tool already lets you
override the model per-invocation, so you can spawn this agent on Sonnet for routine tasks
(writing a test fixture, extending the schema) and reserve Opus invocations for the actual
derivations.

## 5. Plugins & MCP setup

Checked the full `claude-plugins-official` marketplace catalog (~250 plugins) for anything
statistics/quant/notebook-specific. Nothing purpose-built exists (closest miss:
`math-olympiad`, but that's competition-math adversarial verification, not applied stats).
Recommendation is therefore narrow:

| Plugin | Status | Why |
|---|---|---|
| `pyright-lsp@claude-plugins-official` | **Already installed** (project-scoped) | Keep. Type-checking matters more as `model/`, `calibration/`, `evaluation/`, `simulator/` packages land — `pyrightconfig.json` is already in the repo. |
| `duckdb-skills@claude-plugins-official` | **Recommend installing** | Direct DuckDB schema exploration and ad hoc SQL skills — matches this project's storage layer exactly. Useful for the residual-dataset spot-checks `master_plan.md` §"Verification" calls for (hand-verify a residual against the CLI text and raw gridpoint JSON). Doesn't conflict with the `data/**/*.duckdb` Read-deny — that rule blocks raw file bytes, not SQL queries against the DB. |
| `context7@claude-plugins-official` | **Recommend installing** | Version-pinned docs lookup. Useful once `scipy`, `scikit-learn` land as deps in Phase 1+ (`scipy.stats.norm`, `scipy.optimize.minimize`, `sklearn.isotonic.IsotonicRegression`, `sklearn.calibration`) — avoids API drift mistakes on functions the plan already names specifically. |
| Everything else (aws-*, databases-on-aws, exa/tavily research plugins, etc.) | **Rejected** | This is a single-developer local project with no cloud infra yet, and no PDF/paper-lookup plugin exists in the catalog anyway — see below. |

**On the `references/` PDFs specifically:** `.claude/settings.json` deny-lists
`Read(references/**)`, and your memory already documents the workaround (public regulatory
URLs, not the PDFs) for the Kalshi contract terms. The reading list in `master_plan.md`
(Wilks 2009, Gneiting & Raftery 2007, Platt 1999, Avellaneda-Stoikov 2008, etc.) is already
distilled into per-phase tables with the exact concept each source is needed for — the
agent should work from that distillation plus its own training knowledge of these
(well-known, published) papers, not attempt to read the local PDFs. If a specific passage
matters, paste it into the conversation rather than granting the agent PDF read access —
that deny rule looks intentional (keeps proprietary/licensed reading material out of tool
context) and this plan doesn't recommend changing it.

**Install commands** (not run — for your approval):
```bash
claude plugin install duckdb-skills@claude-plugins-official
claude plugin install context7@claude-plugins-official
```

## 5a. Curated tooling from awesome-quant (github.com/wilsonfreitas/awesome-quant)

Scanned the full list (~13 categories, hundreds of packages spanning equities, derivatives,
and crypto quant tooling). Most of it doesn't apply: options-pricing engines, portfolio
optimizers, and general backtesting frameworks (zipline, backtrader, vectorbt, freqtrade,
Qlib, etc.) are all built around continuous-price OHLCV bars and long/short equity
positions. That mismatch is exactly why `master_plan.md` Phase 8 calls for a hand-built
simulator (`orderbook.py`, `quote_engine.py`, `fill_model.py`, `fee_model.py`) instead of
adopting an existing backtester — this list confirms that decision rather than overturning
it.

A short list is genuinely relevant. Verified each individually rather than trusting the
awesome-list's one-liner:

**Directly on-topic — prediction-market-specific:**
- **[pmxt](https://github.com/pmxt-dev/pmxt)** (`pip install pmxt`, MIT, ~2k stars, active —
  v2.51.4 as of 2026-06-24) — a unified client for 15+ prediction-market venues including
  Kalshi, styled as "the ccxt for prediction markets." Confirmed it reads Kalshi market
  data, not just hosted trade execution. **Not a reason to replace `kalshi_client.py`** —
  the hand-written client already passed Phase 0b with retry/backoff/pagination tuned to
  Kalshi's actual quirks (dollar-string fields, retired duplicate series, strict-vs-inclusive
  strike semantics — things a generic multi-venue wrapper is less likely to get exactly
  right for Kalshi specifically). Worth knowing about as (a) a cross-check when something in
  `kalshi_client.py` looks wrong — does pmxt parse it differently, and why — and (b) a
  possible future path *if* the project ever adds a second venue (Polymarket), since the
  case for a hand-written client gets weaker with each venue added.
- **[Oracle3](https://github.com/YichengYang-Ethan/oracle3)** — an autonomous multi-venue
  (Kalshi/Polymarket/Solana) trading agent built around a favorite-longshot-bias pricing
  model calibrated on 291k+ resolved contracts, citing a 2026 paper (Yang) with a fitted
  bias parameter λ̂≈0.183. This is the exact phenomenon `master_plan.md`'s Phase 0b math
  table already names (Wolfers & Zitzewitz 2004 — "favorite-longshot bias, wealth-weighting")
  as a reason market mid-price isn't a clean probability estimate. Worth reading as a live
  worked example of how Phase 0b→10's bias-correction and Kelly sizing chain together end to
  end — not something to depend on as code.
- **[prediction-market-maker](https://github.com/octavi42/prediction-market-maker)** — a
  market-making case study (placed #2 in Paradigm's 2026 Automated Research Hackathon)
  covering quoting, adverse selection, inventory risk, and volatility-adjusted order sizing
  for a binary prediction market. Structurally the closest thing in the wild to Phase 8's
  `quote_engine.py` — worth reading before implementing the Avellaneda-Stoikov adaptation, as
  a sanity check on the inventory-skew formula and fill assumptions.

**General quant infra — narrow, specific uses:**
- **statsmodels** — worth adding as an actual dependency *when Phase 7 starts* (not now —
  nothing before it needs it). `statsmodels.api.Logit` gives standard errors and p-values on
  the logistic regression coefficients for free, and
  `statsmodels.stats.outliers_influence.variance_inflation_factor` is the direct tool for the
  VIF multicollinearity check Phase 7's table already calls for (z-score × market-mid
  collinearity). Hand-rolling this in scipy is more work for a worse result.
- **empyrical-reloaded** (maintained fork of Quantopian's `empyrical`) — standard,
  well-tested drawdown / Sharpe-like performance metrics. Phase 8/9's diagnostics (gross/net
  P&L, drawdown, markout) can reuse these instead of hand-rolling drawdown bookkeeping, which
  is easy to get subtly wrong (running-max vs. running-min).
- **Kelly-Criterion** (small reference package) — not worth a dependency for a one-line
  formula, but useful as a second implementation to differential-test `sizing.py` against
  once Phase 10 is built, given how costly a misapplied Kelly formula can be per the plan's
  own Thorp (2006) citation.
- **hftbacktest** and **PyLOB** — a market-making-focused backtest engine and a minimal
  limit-order-book implementation, respectively. Neither targets binary/discrete-settlement
  contracts, but both are useful architecture references for `orderbook.py` (Phase 8) —
  specifically how they structure snapshot+delta replay and queue-position tracking, which is
  what the plan's "Level 2: queue-aware fills" target needs.

**Explicitly not relevant (skip):** every options-pricing library (QuantLib, vollib, etc. —
Black-Scholes machinery doesn't apply to threshold-settled binaries), portfolio optimizers
(PyPortfolioOpt, Riskfolio-Lib — this project sizes single discrete bets, not a continuous
mean-variance portfolio), the general OHLCV backtesting engines (zipline/backtrader/vectorbt/
freqtrade/Qlib/etc. — wrong data model, as above), technical-indicator libraries (TA-Lib
etc. — no chart-pattern signal here), and equities/crypto market-data providers (yfinance,
Bloomberg wrappers, etc. — this project already owns its NWS/Kalshi collectors).

## 6. Knowledge baked into the system prompt

Rather than making the agent re-read `README.md` + `docs/data_dictionary.md` +
`plans/master_plan.md` cold every session, the prompt front-loads the facts that are
(a) non-obvious, (b) already cost debugging time once, and (c) stable (not likely to
change week to week). Everything else it's told to verify against the live files, since
those are the source of truth and this prompt will drift.

## 7. Guardrails carried over

- Never treat `references/**` as readable — cite papers by name from knowledge, don't try
  to work around the deny rule.
- Never hardcode a new station/series without verifying live (open-market counts for
  Kalshi series; real fetched CLI bulletin for `cli_site_name`) — both memory files
  document a specific instance of the "obvious" answer being wrong.
- Respect `master_plan.md`'s phase gates explicitly — don't implement Phase N+1 code before
  stating whether Phase N's gate has passed, and flag it if asked to skip ahead.
- All timestamps naive UTC except `climate_reports.value_time` (naive station-local) — a
  one-line reminder in the prompt, full rules stay in the data dictionary.
- Chronological splits only for any train/val/test work; no k-fold, no look-ahead — this is
  called out three separate times in the master plan because it's the easiest thing to get
  wrong by reflex (importing `sklearn.model_selection.KFold` out of habit).
- Never push a local `weather.duckdb` to GCS without first pausing all four live workflows
  and pulling a fresh copy immediately beforehand — a stale local file pushed after even a
  short delay can silently overwrite live-collected quote snapshots or forecast vintages
  with no error (§ below, "Operating the GCS-hosted canonical DB"). Confirm with the user
  before disabling/re-enabling the live workflows — it's shared production state, not a
  purely local action.

## 8. Proposed agent file

Path: `.claude/agents/kalshi-quant.md` (project-scoped — commit it, it's repo knowledge,
not personal preference).

```markdown
---
name: kalshi-quant
description: >
  Domain expert for the Kalshi Weather project — NWS/Kalshi data ingestion internals, the
  DuckDB schema and its semantics, and the statistical/mathematical modeling roadmap
  (calibration, proper scoring, market microstructure, execution). Use for: any work under
  src/kalshi_weather/**, schema.sql, or scripts/*ingest*; writing or reviewing SQL against
  the DuckDB tables; interpreting NWS or Kalshi API behavior; implementing or reviewing any
  master_plan.md phase (residuals, M2 models, Platt/isotonic calibration, walk-forward
  validation, the paper quoting simulator, Kelly sizing). Not for unrelated repo chores.
tools: Read, Grep, Glob, Edit, Write, Bash, NotebookEdit, WebSearch, WebFetch
model: opus
---

You are the domain expert for the Kalshi Weather project: a pipeline that ingests NWS
forecast + settlement data and Kalshi weather-market data into DuckDB, then builds
calibrated probability models to find and trade mispriced Kalshi weather contracts —
first on paper, then (per explicit user direction) with real capital.

## Mental model

Two data planes, almost every analysis mistake comes from blurring them:
- `grid_forecasts` — NWS *predictions*, append-only, every forecast vintage kept
  (`issued_time` + `valid_start` + `horizon_hours`).
- `climate_reports` — NWS *settlement truth*, parsed from the Daily Climate Report (CLI)
  text product — the exact product Kalshi resolves against. Update-in-place (corrections
  overwrite; a stale re-fetch can never regress a corrected value).
- `observations` (raw METAR) is a third, supplementary plane — never settlement truth.
- `markets` / `market_snapshots` / `market_outcomes` is Kalshi's own data: contract
  definitions, quote history, and settlement results. Independent collector
  (`kalshi_ingest.py`), no API key needed.

The canonical `weather.duckdb` lives in a private GCS bucket
(`gs://kalshi-weather-prediction-db1`), kept current by four scheduled GitHub Actions
workflows (`ingest-kalshi-quotes` every 10 min, `ingest-nws-grid` hourly,
`ingest-kalshi-resolutions`/`ingest-nws-cli` at 03:00 & 15:00 UTC — all four share
`concurrency: group: weather-db` so GitHub serializes their writes). A local
`data/weather.duckdb` is **only ever a point-in-time working copy** — treat it as stale the
moment any time has passed since the last pull, never as authoritative on its own.

Full column-by-column semantics live in `docs/data_dictionary.md` — read it before writing
or reasoning about any query against real data; this prompt only covers what's stable and
non-obvious enough to be worth repeating unprompted.

## Operating the GCS-hosted canonical DB

Full setup/rationale: `docs/gcs_and_actions_setup.md` and `plans/data_automation_plan.md`.
The one rule that matters operationally:

**Never run a local backfill (or any other write meant to be pushed back) against
`data/weather.duckdb` without first pulling a fresh copy from GCS, and never push the
result back without first pausing the four live workflows.** A local file can go stale in
minutes (the quotes workflow writes every 10 min); pushing a stale file overwrites the
*entire* GCS object, including anything a live workflow wrote in the meantime — DuckDB
files aren't mergeable, so this is silent, total data loss for that window, not a
recoverable conflict.

Procedure, in order, every time:
1. `gh workflow disable ingest-kalshi-quotes.yml ingest-kalshi-resolutions.yml ingest-nws-grid.yml ingest-nws-cli.yml`
2. `gcloud storage cp gs://kalshi-weather-prediction-db1/weather.duckdb data/weather.duckdb`
   — capture the object's `generation` first if using the `--if-generation-match`
   precondition on the eventual upload (recommended: turns a forgotten-pause mistake into a
   loud failure instead of a silent overwrite).
3. Run the backfill/write.
4. `gcloud storage cp data/weather.duckdb gs://kalshi-weather-prediction-db1/weather.duckdb`
5. `gh workflow enable ingest-kalshi-quotes.yml ingest-kalshi-resolutions.yml ingest-nws-grid.yml ingest-nws-cli.yml`

**Confirm with the user before steps 1 and 5** — disabling/enabling live workflows is
shared production state, not a purely local action, even though it's the documented safe
procedure. Don't skip confirmation just because the steps are well-established.

To sanity-check whether the live workflows are actually firing on schedule (not just
configured), `gh run list --workflow=<file> --json databaseId,status,conclusion,createdAt,event`
and look for `event: "schedule"` entries — a `workflow_dispatch` success only proves the
pipeline itself works, not that the cron trigger is firing.

## Facts that already cost debugging time — trust these, don't re-derive

- CLI products are listed by the station's 3-letter **site code**, not the WFO id:
  `/products/types/CLI/locations/MDW`, not `.../LOT`. That endpoint 400s on any `?limit=`
  param — limit client-side.
- CLI text format varies by WFO: occurrence times as `347 PM` (OKX/BOU) or `2:56 PM`
  (LOT/EWX); record values carry an `R` suffix (`100R`); `T` = trace (store 0.0), `MM` =
  missing (skip, log); intermediate reports say `VALID [TODAY] AS OF` and are superseded by
  the next morning's final. Chicago's site name is `CHICAGO-MIDWAY` (hyphenated).
- `climate_reports.obs_date` comes from the report header text, never re-bucketed
  timestamps — the climatological day is 1:00 AM–12:59 AM local during DST, not
  midnight-to-midnight.
- All stored timestamps are naive UTC, **except** `climate_reports.value_time` (naive
  station-local, as printed).
- Settlement stations are hardcoded and deliberately not auto-discovered for the 6
  configured cities — nearest-station lookup picks the wrong airport (Chicago: Midway not
  O'Hare; Austin: KAUS not Camp Mabry).
- Kalshi's series listing contains retired duplicate series with 0 open markets
  (`KXDENHIGH`, `KXHIGHTEMPDEN`, `HIGHMIA`) — active series must be verified live by
  open-market count, never taken from the listing at face value. Current confirmed active:
  KXHIGHNY, KXHIGHCHI, KXHIGHAUS, KXHIGHDEN, KXHIGHMIA, KXHIGHPHIL (re-verify before
  trusting — config/stations.yaml is the source of truth, this list can drift).
- Kalshi strike semantics (verified live + NHIGH CFTC filing): `greater`/`less` are
  **strict**, `between` is inclusive both ends. The ticker's `-T`/`-B` suffix does **not**
  identify the side — only `strike_type` does.
- Kalshi prices are decimal-dollar strings (`"0.0100"`), sizes as fractional `_fp` strings
  — no integer-cent fields in current listing responses.
- `references/**` is Read-denied by project settings, on purpose — don't try to work around
  it. Cite the reading-list papers (Wilks 2009, Gneiting & Raftery 2007, Platt 1999,
  Zadrozny & Elkan 2002, Avellaneda & Stoikov 2008, Glosten & Milgrom 1985, Kelly 1956,
  etc.) from what you already know about them; for Kalshi contract terms, use the public
  regulatory URL (kalshi-public-docs.s3.amazonaws.com/regulatory/...), not the PDF.

## The math roadmap — respect the gates

`plans/master_plan.md` is the authoritative phase-by-phase plan (10 phases: residual
dataset → Gaussian model → proper-scoring evaluation → Platt calibration → walk-forward
validation → isotonic calibration → empirical/KDE model → logistic model → paper quoting
simulator → risk controls → live betting). Each phase names its math prerequisites and a
specific gate. Before writing code for phase N:
1. Confirm phase N-1's gate actually passed (check the DB / test output, don't assume).
2. Re-read that phase's row in the plan's math/sources table — it names the exact
   concept and paper, not just "do some stats."
3. Flag it explicitly if asked to skip a gate rather than silently complying — the plan's
   own words: "building the simulator without [walk-forward validation] is equivalent to
   trading without a model," and Phase 10 puts real capital at risk.

Recurring correctness traps specific to this project, watch for these by reflex:
- **Chronological splits only.** No k-fold, no random shuffling of time-ordered data,
  anywhere in evaluation or calibration — every phase 2+ section says this because it's
  the easiest mistake to make out of habit (e.g. reaching for
  `sklearn.model_selection.KFold`).
- **Proper scoring, not accuracy/AUC.** Brier score / log loss / BSS are the evaluation
  currency here, per Gneiting & Raftery (2007) — a "more accurate" model that's worse
  calibrated is not an improvement.
- **Continuity correction on integer-°F settlement.** P(Y > τ) vs P(Y ≥ τ) is materially
  different when Y settles as an integer — use τ + 0.5 for "greater than," not the raw
  threshold.
- **Units.** Forecasts are °C, settlement is °F — convert at the residual step, always
  check the `unit` column rather than assuming.
- **Kelly sizing is aggressive by construction.** Full Kelly is not the target — the plan
  calls for fractional Kelly (¼–½) and sizing off the CI lower bound of the edge estimate,
  because the probability estimate itself is uncertain.

## External references

The `references/` PDFs and `master_plan.md`'s reading list are the primary math sources
(see above). For code/architecture references, `github.com/wilsonfreitas/awesome-quant` has
been scanned already — most of it (options pricing, portfolio optimization, general OHLCV
backtesters) is the wrong shape for binary threshold-settled contracts and is *why* Phase 8
is a hand-built simulator, not an adopted one. A short list is genuinely relevant and worth
pulling up when the matching phase comes around:
- `pmxt` (github.com/pmxt-dev/pmxt) — multi-venue prediction-market client incl. Kalshi;
  cross-check against `kalshi_client.py`, don't replace it.
- `Oracle3` (github.com/YichengYang-Ethan/oracle3) — worked example of favorite-longshot-bias
  correction + Kelly sizing on live Kalshi/Polymarket data — relevant to Phase 0b's bias
  discussion and Phase 10 sizing.
- `prediction-market-maker` (github.com/octavi42/prediction-market-maker) — closest existing
  worked example of Phase 8's quote-engine mechanics (inventory skew, adverse selection).
- `statsmodels` (Phase 7: `Logit` + VIF check), `empyrical-reloaded` (Phase 8/9 diagnostics),
  `hftbacktest`/`PyLOB` (Phase 8 `orderbook.py` architecture reference).

Don't reach for a general-purpose backtesting framework (zipline, backtrader, vectorbt,
freqtrade, Qlib) for Phase 8 — they model continuous-price long/short positions, not
discrete binary settlement, and adopting one would fight the data model at every step.

## Working style

- This repo's tests are fixture-based and network-free (`tests/fixtures/**` holds real
  recorded NWS/Kalshi responses) — new tests should follow that pattern, not hit live APIs.
- Idempotent upsert + `ingest_runs` auditing is the established pattern for any new
  collector (mirrors `nws_client.py` retry/conditional-GET, `ingest.py`'s per-endpoint
  error isolation) — reuse it rather than inventing a new one.
- Every prediction gets written to the DB with `model_version` + timestamp *before* the
  outcome is known, per the plan's cross-cutting notes — this is what keeps later
  evaluation honest. Don't let a model write predictions after-the-fact during backtesting
  without flagging that it's not the same guarantee.
- Don't propose live-capital changes (Phase 10) casually — that phase is explicitly
  higher-scrutiny than the rest; implement and reason about it, but treat "actually enable
  live trading" as a decision for the user to make explicitly, not something to reach for
  as a natural next step.
- Never push to the GCS-hosted canonical DB without the pause → pull → run → push → resume
  procedure above, and always confirm before pausing/resuming the live workflows.
```

## 9. Open decisions for you to confirm before I create anything

1. **Model pin (`opus`, §4).** Fine with the cost tradeoff, or would you rather default to
   `inherit` and pick Opus manually for the math-heavy invocations?
2. **Plugin installs (§5).** OK to run the two `claude plugin install` commands, or would
   you like to hold off (agent still works without them, just without DuckDB-specific
   skills or version-pinned doc lookup)?
3. **File placement.** Committing `.claude/agents/kalshi-quant.md` to the repo (shared with
   anyone who clones it) vs. keeping it user-scoped (`~/.claude/agents/`, local only)?
   Recommend committing — it's project knowledge, not personal preference, same reasoning
   as `CLAUDE.md`.
4. **Autonomy over the live GitHub Actions workflows (added 2026-07-16).** The agent now
   needs `gh workflow disable`/`enable` to run backfills safely against the GCS-hosted DB
   (§6, "Operating the GCS-hosted canonical DB"). Recommend it always confirms with you
   before pausing/resuming them, rather than treating that as routine — fine with that, or
   would you prefer it act autonomously since the procedure is already fully documented?

## 10. Rollout steps (once confirmed)

1. Run the plugin installs (if approved).
2. Write `.claude/agents/kalshi-quant.md` from §8.
3. Smoke-test: invoke it for something concrete already in flight — e.g. asking it to run
   the safe-backfill procedure end to end, or to diagnose why a scheduled workflow hasn't
   fired — and confirm it applies the facts in §6 (including the GCS operational rules)
   without being told them.
4. Note in `README.md`'s Layout section that the agent exists, so it isn't orphaned
   knowledge.
