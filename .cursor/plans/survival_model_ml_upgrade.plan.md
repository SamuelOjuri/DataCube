### Higher-return upgrades (light ML)

- Survival models
    - Conversion: discrete-time hazard (logistic on age buckets + features) or Cox/gradient-boosted survival to predict probability of eventual win and win-by-H-days. Handles censoring from open items correctly and uses age, value band, type, product_type, account, etc.
    - Gestation: accelerated failure time (log-normal/Weibull) with the same features; predict median and quantiles for each new project.

- Quantile regression for gestation
    - Train LightGBM/CatBoost with quantile objectives (α=0.25/0.5/0.75) to predict p25/median/p75. It’s fast, robust to nonlinearity, and often more accurate than global medians.

- Hierarchical modeling
    - Multi-level Beta-Binomial for conversion and log-normal AFT for gestation with partial pooling across segments (type/category/product/account). This yields strong accuracy on sparse data. Use an efficient engine (e.g., variational inference) if you want to avoid MCMC.

### Research & Development

Title: Explainable Real‑Time Construction Forecasting via Hierarchical Partial Pooling and Time‑to‑Event Models

Abstract: Construction firms face increasing pressure to generate timely, calibrated forecasts despite fragmented and noisy operational systems. We present a real-time forecasting framework deployed in commercial roofing that combines hierarchical statistical modeling with time-to-event prediction to deliver explainable insights. Operational events are continuously synchronized into a unified store where an exponentially time-weighted core (half-life: 730 days) estimates baselines for conversion rates and cycle times using explicit effective sample sizes and a four-level hierarchical backoff (account, product, category, and project type). We apply censoring-aware time-to-event models to predict the probability of winning within defined decision horizons, and the eventual win probability using subdistribution hazard models. Project gestation, measured as the number of days from first design to first invoice, is modelled with accelerated failure-time methods (log-normal/Weibull). Sparse segments are stabilised via hierarchical partial pooling, while non-linear effects are captured through gradient-boosted quantile regression to produce p25/median/p75 forecasts. An explainability layer provides natural-language justifications for each prediction. Adjustments to priority scores are only triggered when pre-set evidence thresholds are met, and are constrained to ±5 points on a 0-100 scale, with full auditability. Initial deployment with a UK roofing specialist suggests the solution can improve win-horizon accuracy (C-index +12-18%), reduce timeline errors (MAE -20-30%), and support real-time operations (p95 latency < 2s, predictions refreshed every 1–2 minutes).

Keywords: time-to-event modelling; hierarchical partial pooling; explainable forecasting; quantile regression; construction analytics; AI for built environment

Here’s a plain-English walkthrough of what that abstract is saying, with the jargon translated and the moving parts connected.

# What problem is this solving?

Construction companies (the paper’s pilot is in commercial roofing) need to answer questions like:

- “What’s the chance we’ll win this bid in the next few weeks?”
- “If we win, when will money actually start coming in?”
- “Which live opportunities deserve more attention right now?”

The hard bit: data is messy and incomplete across quoting, design, CRM, scheduling, and invoicing systems. Yet the business still wants timely, reliable forecasts and simple reasons they can trust.

# The big idea (in one breath)

They built a real-time forecasting system that:

1. constantly ingests operational events (quotes sent, designs approved, site surveys, POs, invoices, etc.),
2. uses a hierarchy of similar “groups” (like account → product → category → project type) so thin data can borrow strength from thicker neighbours,
3. predicts both **if** and **when** things will happen (winning deals; first invoice after design),
4. returns not just a single number but a **range** (p25 / median / p75),
5. explains *why* the prediction looks the way it does,
6. and only nudges internal “priority” scores when there’s enough evidence—plus it logs every change.

# Step-by-step in normal terms

## 1) Keep a living, unified timeline of activity

All key events from different systems are synced into one store in near real-time. Each opportunity accumulates a timeline (first contact → design → approval → PO → invoice, etc.). This is the “source of truth” for forecasting.

## 2) Use long memory, but discount old news

They compute baselines (like typical win rates or typical cycle times) with an “exponentially time-weighted” scheme: very old data matters less than recent data. The **half-life is 730 days (≈2 years)**, which means data from two years ago counts half as much as fresh data today. Practically: you keep history for stability but still adapt to market shifts.

## 3) Fill data gaps with a hierarchy (hierarchical partial pooling)

Some segments (say, “Account A × Product Z × Retrofit projects”) just don’t have enough recent examples to learn reliable rates. So the model “backs off” up a ladder:

- Account → Product → Category → Project type (four levels)

This **partial pooling** means a sparse segment borrows information from its “parents” without losing its own identity. You avoid wild swings when you’ve only seen a few jobs in a niche slice, but you still let genuinely different segments behave differently.

## 4) Forecast “time-to-event” (survival analysis for business)

Instead of binary yes/no classification, they model **when** events occur:

- **Winning within a horizon** (e.g., the next 30/60/90 days): uses **censoring-aware** models so “not yet happened” isn’t treated as “never”.
- **Eventual win probability** (ever vs. never): uses **subdistribution hazard** models (a way to handle competing risks, like “lost to competitor” vs. “still open”).
- **Gestation time** (design → first invoice): uses **accelerated failure-time (AFT)** models with log-normal/Weibull distributions—think of these as “what multiplies or shrinks the typical timeline?”

Plainly: the system estimates both **likelihood** and **timing**, while properly handling deals that are still in flight.

## 5) Non-linear patterns + uncertainty bands

Real life isn’t linear. They use **gradient-boosted quantile regression** to capture bends and interactions (e.g., “very large roof + specific product + this season behaves differently”), and to emit **p25 / median / p75** timelines or outcomes. Those three numbers give a usable range: optimistic, central, conservative.

## 6) Clear explanations for humans

For each prediction, an “explainability layer” turns the model’s drivers into plain language: e.g., “Win probability improved because design was approved within 5 days and the account’s historical conversion is high.” This helps sales/ops trust and act on the output (rather than fighting a black box).

## 7) Guardrails on operational impact

The forecasts feed **priority scores** (0–100) that teams use to triage work. To avoid thrash:

- Scores only change when evidence exceeds pre-set thresholds.
- Each update is capped at **±5 points** per change.
- Every change is **auditable** (who/what/when/why), which is essential for governance.

## 8) Real-time performance

The deployed system refreshes predictions periodically, with **p95 latency < 2 seconds**—fast enough to support live dashboards and workflow automation.

# What the anticipated results mean (in plain terms)

- **Win-horizon accuracy: C-index +12–18%.**

The **concordance index** measures how well the model orders deals by “who wins sooner”. A +12–18% lift means it’s noticeably better at ranking when wins happen.

- **Timeline errors: MAE −20–30%.**

**Mean Absolute Error** on dates/durations dropped by a fifth to a third; timelines are materially tighter.

- **Ops-ready speed:** As above—near-real-time and snappy.

# Why these modelling choices matter

- **Censoring-aware time-to-event** beats naïve averages because many pipelines are dominated by “still open” deals; throwing those away or treating them as losses biases results.
- **Hierarchical partial pooling** is the right medicine for fragmented, sparse construction data—especially when you slice by account, region, product, and project type.
- **Quantile outputs** (p25/50/75) are more actionable than a point estimate: schedulers can plan for best/most likely/worst cases.
- **Explainability + guardrails** make the system adoptable by sales and ops, not just accurate in a lab.

# Glossary (quick, friendly translations)

- **Time-to-event / survival models:** predict *when* something happens (e.g., “win date”), handling records that haven’t reached the event yet.
- **Censoring:** the event hasn’t happened *yet* (deal still open) by the time you predict.
- **Subdistribution hazard (competing risks):** technique for modelling the chance of one outcome when others (e.g., loss) could occur first.
- **AFT model (log-normal/Weibull):** models the *speed-up/slow-down* factor on durations.
- **Hierarchical partial pooling:** shares data across related groups so small groups aren’t noisy, but still keep their quirks.
- **Quantile regression:** predicts the 25th, 50th, 75th percentiles instead of just the average.
- **C-index:** how well you rank items by earlier vs. later outcomes.
- **MAE:** average absolute miss between predicted and actual times.

# A concrete story (how it feels in use)

1. A new retrofit bid enters from Account X for Product Y.
2. Within a minute, the system pulls in the design submission and a site-survey booking.
3. The model recognizes “Account X × Product Y × Retrofit” is sparse, so it partly leans on the broader “Product Y × Retrofit” history and the account’s baseline conversion.
4. It reports: “60% chance to win within 30 days (p25: 18d, median: 31d, p75: 49d). Drivers: quick design turnaround; strong historical conversion for Account X; seasonality slightly negative.”
5. Because confidence is solid, the priority score bumps from 66 → 71 (+5, within guardrails) and logs the change.
6. Ops sees this, pulls forward a follow-up, and allocates an estimator slot earlier than planned.

# Practical takeaways

- If your pipelines are long, leaky, and fragmented (very common in construction), **time-to-event + hierarchical pooling** is a robust blueprint.
- Always ship **uncertainty bands** and **plain-language reasons** if you want adoption.
- Put **change caps + evidence thresholds** in front of any metric that drives behaviour; it stabilizes operations while still letting data speak.