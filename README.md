# Across the Water
## French Nuclear Unplanned Outages and GB Power Prices
Python 3.11 | Energy markets | 2026

---

## Findings — Notebook 02 Gate: FAILED
Signal absorbed by IFA2 implicit coupling. The DA edge does not survive conditioning on the French day-ahead price.
The project stopped at Notebook 02. This is a pre-registered outcome.

---

## What Was Tested

GB imports electricity from France via the IFA and IFA2 interconnectors (~3 GW combined capacity). When a French nuclear reactor trips unexpectedly, French export capacity falls and GB must dispatch more expensive domestic generation — typically gas — at higher marginal cost. The price impact is real and well understood.

The question was: **does the market fully price unplanned outage announcements, and if so, how quickly?**

A pre-registered two-stage gate was used. Either the intraday market absorbs the announcement within hours (Notebook 02b), or a residual mispricing survives into the next morning's DA auction (Notebook 02). The DA gate was run first. It failed. Notebook 02b was not run.

---

## What Was Found

French nuclear unplanned outages produce a significant total effect on GB day-ahead prices in Model A. This effect disappears entirely in Model B when the French day-ahead clearing price is added as a control:

| Test | β (£/MWh per GW) | p (1-tail) | Gate |
|---|---|---|---|
| Model A — total outage effect (no FR DA price) | +2.942 | 0.010 | ✅ passes |
| Model B — DA residual after FR DA clear | −0.411 | 0.677 | ❌ fails |

Model A passes all three gate conditions (β > 0, p < 0.10, effect > £2/MWh/GW). Model B fails: conditioning on the FR DA clearing price collapses the estimate to −£0.41/MWh per GW with p = 0.677. The β drops by £3.35/MWh/GW — 114% of the Model A estimate.

The residual is indistinguishable from zero. The DA hard gate fails on Model B.

---

## Why the Signal Disappears

The transmission mechanism is IFA2 implicit coupling:

1. An unplanned outage notice is filed on ENTSO-E — often within hours of the event
2. The French DA auction clears at ~08:00 UTC. The outage is already visible to participants and priced into the FR DA clearing price
3. IFA2's implicit coupling algorithm uses the FR DA price to jointly optimise cross-border flows — a higher FR DA price directly reduces IFA2 exports to GB
4. The GB DA auction clears at ~11:00 UTC. By this point the outage signal has been fully transmitted via the IFA2 algorithm

Model B conditions on the FR DA 08:00 clearing price, capturing this channel exactly. Once included, there is nothing left for the GB DA market to price independently. The mechanism is real; the market has already priced it.

Large energy desks with IFA2 pricing infrastructure have evidently incorporated this channel into their DA bidding strategies. The edge, if it ever existed at the DA horizon, has been arbitraged away.

---

## What This Means Commercially

This null result is itself precisely useful. It tells a participant operating at the GB DA horizon that:

- French nuclear outage information is fully priced into GB DA by 11:00 via IFA2 coupling — no systematic edge exists at this horizon with public data
- The correct instrument to test next is the intraday market in the 15:00–20:00 window D-2 — before either DA auction clears — where the signal arrives earliest and cleanest
- Testing that properly requires tick-level N2EX/EPEX data and sub-hourly event timestamps from the ENTSO-E API. It cannot be done with hourly public data

The minimum detectable effect at hourly resolution is ~£1.5–2.0/MWh. Sub-hourly reactions below this threshold exist but are untestable with the data available here.

Either result from a hard gate — pass or fail — was pre-registered as useful. A failed gate with a precise causal diagnosis is more useful than a spurious positive.

---

## The Commercial Question

GB imports electricity from France at up to ~3 GW via IFA (1 GW, 1986) and IFA2 (1 GW, 2021). When French nuclear availability falls, the merit order for GB domestic generation shifts upward. The commercial question is not simply whether outages affect GB prices — they do. The question is **at which point in the market clearing sequence the information is priced, and whether any residual mispricing is exploitable at the DA horizon.**

The market clearing sequence matters:

- **~15:00 D-2:** Unplanned outage notice filed on ENTSO-E. ID market open.
- **~08:00 D-1:** French DA auction clears. IFA2 implicit coupling transmits outage signal.
- **~11:00 D-1:** GB DA auction clears. IFA2 flow already adjusted.

A participant who can act between 15:00 D-2 and 08:00 D-1 — before the FR DA clear — is operating in the window where the signal is live and the GB DA price has not yet adjusted. That is an intraday strategy, not a DA strategy. This project tests the DA horizon. It finds nothing there.

The pre-registered two-model specification makes the transmission channel auditable. Model A vs Model B β gap locates precisely where the signal is absorbed. A β that is significant in Model A but collapses in Model B is the exact signature of IFA2 coupling — not confounding, not noise, but the mechanism working as designed.

```
# Pre-registered model specifications
#
# Model A — total outage effect (excludes FR DA price):
#   gb_da_price(t) = α
#                  + β  × unplanned_outage_mw(t)        ← treatment
#                  + γ  × planned_outage_mw(t)           ← baseline control
#                  + ζ  × ttf_spot(t)                    ← cost confounder
#                  + η  × fr_temperature_deviation(t)    ← demand confounder
#                  + θ  × de_wind_generation(t)          ← continental proxy
#                  + ι  × ifa_flow(t)                    ← mediator check
#                  + season_fe + year_fe
#                  + u(t)   [HAC SE, 12 lags]
#
# Model B — DA residual after FR clear (adds FR DA price):
#   Same as Model A, with additional term:
#                  + κ  × fr_da_price_at_0800(t)         ← IFA2 coupling anchor
#
# Gate requires BOTH models to pass. A positive Model A β
# that collapses in Model B is a precise IFA2 diagnosis,
# not a gate pass.
#
# Pre-registered result: Model A β = +£2.94/MWh/GW (p=0.010) ✅
#                        Model B β = −£0.41/MWh/GW (p=0.677) ❌
# Gate: FAILED
```

---

## Why the Instrument Is Right (Even Though the Gate Failed)

Unplanned outages are the correct instrument. They are:

- **Discrete** — a reactor either trips or it doesn't. The treatment assignment is cleaner than a continuous variable
- **Announced** — filed on ENTSO-E in near real-time, creating a specific information event around which market reactions can be studied
- **Large** — a single reactor trip is 900–1,400 MW, roughly 1.5–2.5% of GB peak demand. The SNR is structurally more favourable than the EV project

The instrument is not perfectly exogenous. Two contamination sources are identified and handled:

**Load-following events:** EDF sometimes reduces output during high-renewables periods and files the reduction as unplanned. These are not genuine engineering failures and may be partially anticipated. Expected to produce attenuation bias toward zero — the gate passing despite this would mean the true effect is at least as large as estimated.

**Accumulation bias:** `unplanned_outage_mw` is total active outage MW on each day, not the announcement-day increment. A cleaner instrument would use the MW increment at first filing. This limitation does not change the conclusion given the Model B result.

Neither limitation invalidates the design. They bound the interpretation and are reported alongside results.

---

## Identification (Gate Failed — Notebooks 03–06 Not Run)

Notebooks 03–06 (full SCM identification, quantile model, trading rule, signal decay) were not run. The hard gate in Notebook 02 prevents execution of downstream notebooks. All specifications are preserved in the codebase unmodified for future use.

The two-model specification used at the gate is:

```
Unplanned outage increment (Z)
           ↓
French export capacity (X) ←── observed confounders W:
           ↓                     - TTF natural gas spot
    IFA/IFA2 flow (M)            - French temperature deviation
           ↓                     - German wind generation
    GB DA price (Y) ←───────────┘
```

All confounders in W are directly observed. The SCM diagram makes the exclusion restriction, mediation structure, and confounder controls explicit and auditable.

---

## Falsification Conditions

Six pre-registered conditions. Only the DA hard gate was evaluated:

| Condition | Status | Result |
|---|---|---|
| DA hard gate passes (Model A AND Model B) | EVALUATED | ❌ FAILED — project stopped |
| Planned outage effect ≠ unplanned | Not run | — |
| Placebo null holds (+24h shift) | Not run | — |
| IFA flow mediates the outage effect | Not run | — |
| Out-of-sample Sharpe > 0 | Not run | — |
| Rolling β stable or declining | Not run | — |

---

## Data Sources

| Dataset | Source | Status |
|---|---|---|
| FR nuclear unavailability | ENTSO-E Transparency Platform (A77) | ✓ Fetched |
| GB DA prices (daily) | Elexon Insights API (MID) | ✓ Fetched |
| GB intraday prices (half-hourly) | Elexon MID — proxy for EPEX GB ID | ✓ Fetched |
| IFA1/IFA2 flows + GB demand/wind | NESO Historic Demand Data | ✓ Fetched |
| FR generation mix + nuclear actual | RTE éCO2mix (open data) | ✓ Fetched |
| FR DA price (08:00 clearing) | ENTSO-E A44 Publication Document | ✓ Fetched |
| TTF natural gas spot | Yahoo Finance (TTF=F) | ✓ Fetched |
| FR temperature deviation | open-meteo archive API | ✓ Fetched |
| German wind generation | ENTSO-E A75 | ✓ Fetched |

**Note on data sources:** EPEX GB hourly ID settlement prices are not publicly available post-Brexit. N2EX is now operated by Nordpool and requires a commercial account. Elexon MID at half-hourly resolution (N2EX/APXMIDP volume-weighted) is used as the closest free equivalent. This constraint is pre-registered. The TTF EMBER bulk download URL became inactive during the project; Yahoo Finance (TTF=F front-month futures) is used as a replacement.

---

## Project Structure

```
across-the-water/
│
├── README.md
│
├── notebooks/
│   ├── 01_data_pipeline.ipynb        ✅ Complete — all data fetched and cached
│   ├── 02_da_gate.ipynb              ✅ Complete — GATE FAILED at Model B
│   ├── 02b_id_market_test.ipynb      📋 Not run (DA gate failed first)
│   ├── 03_identification.ipynb       📋 Not run (gate enforcement)
│   ├── 04_quantile_model.ipynb       📋 Not run
│   ├── 05_trading_rule.ipynb         📋 Not run
│   └── 06_signal_decay.ipynb         📋 Not run
│
├── src/
│   ├── __init__.py
│   ├── fetchers.py                   Data fetchers for all 9 sources
│   └── utils.py                      Logging, paths, cache helpers
│
├── data/
│   └── README.md                     Data directory — parquet files not committed
│
├── .env.example
├── .gitignore
└── requirements.txt
```

Notebooks 03–06 are included unmodified to demonstrate that the full methodology was written before the gate was run. They were not executed. The code is published as-is.

---

## Limitations

- **Accumulation bias** — `unplanned_outage_mw` is total active outage MW, not announcement-day increment. A cleaner instrument uses the MW increment at first filing. Does not change the Model B conclusion
- **Load-following contamination** — EDF output reductions during low-demand periods filed as unplanned. Expected to produce attenuation bias toward zero
- **No wind/demand forecast data** — NESO historic demand CSV does not include forecast columns. Controls absent from regression. Available via NESO's separate forecast API
- **Hourly ID data only** — sub-hourly intraday reaction untestable with public data. Minimum detectable effect ~£1.5–2.0/MWh at hourly resolution
- **IFA2 structural break** — implicit coupling commenced 2020. Main regression sample starts 2020. Pre-2020 data used only for the reduced-form IFA flow regression
- **Reduced-form outage variable** — total active outage MW rather than new announcement MW. The IFA flow regression uses actuals not forecasts for the same reason

---

## Prior Work

Pre-registered gate structure and null-testing discipline carried forward from
**Ahead of the Curve (2026).**¹
The EV project established the GB data pipeline; the gate structure and SCM identification framework are applied here.

The gate failed there too. Both failures are informative.

¹ github.com/AndyMoran/ahead-of-the-curve

---

Built 2026. Part of a quantitative research portfolio focused on causal identification, pre-registered hypothesis testing, and honest constraint disclosure under real-world data limitations.

andrewgmoran@gmail.com
