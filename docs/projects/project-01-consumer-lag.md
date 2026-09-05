# Project 01 — Consumer Lag During a Volatility Spike

> World and full cast: see [meridian.md](meridian.md).

---

## 1. What this project teaches

This project uses one data-platform incident to practice a set of SRE and distributed-systems skills. Read this section to know what to focus on, then work the scenario.

**Scenario summary.** A volatility spike floods Meridian's trade pipeline. Every ticker except the hot one behaves normally, but users trading the hot ticker see stale portfolio balances. The task is to determine whether this is a capacity problem or something else, and to fix it.

**SRE skills:**
- Turn a business complaint ("users see wrong balances") into a measurable signal.
- Choose the right metric, and use two forms of it to answer two questions: where the backlog is, and how much it affects users.
- Define a Service Level Objective and justify each number by user impact.
- Localize a fault by how a signal is distributed across partitions, not only its total.
- Write an alert and a runbook to prevent a repeat.

**Distributed-systems concepts:**
- How Kafka partitioning and message keying determine where data lands.
- Why a consumer group's parallelism is limited to the number of partitions.
- Key skew and hot partitions: how uneven load is hidden by an aggregate metric.
- Consumer lag as a measure of back-pressure, and how to distinguish skew from a capacity shortfall.

**Common mistake to avoid.** When total lag rises, the reflex is to add consumers. This project shows why that does not help when the backlog is concentrated on one partition, and what to do instead.

---

## 2. The business story

**Wednesday, March 4th, 2026 — market open.** Overnight, a heavily-shorted stock, ticker **$VOLT**, went viral. A post claiming a short squeeze was coming racked up millions of views. By the time US markets open at 9:30am ET, Meridian's app is flooded with users trying to buy.

- **9:30am** — Markets open. Trade volume on $VOLT is already 30× a normal day. A handful of other meme tickers spike alongside it. Overall trade volume across the platform is up, but it is *concentrated* on a few symbols.
- **9:42am** — Users on $VOLT start seeing something strange: they buy shares, the order fills, but their portfolio screen still shows the *old* buying power. Thinking they still have money to spend, they place another order — which is rejected downstream because the cash is already gone. Support tickets start climbing.
- **9:48am** — **Marcus Webb** (Brokerage Operations) pages me: *"Users trading $VOLT are seeing stale balances. They're double-ordering and getting rejections. Every other stock looks fine. What's going on?"*
- **9:50am** — I pull in **Priya Nair**, who owns the positions consumer. Her first words on the call: *"Volume's through the roof — let's just spin up more consumer instances."*

**Why this is expensive.** Every minute the portfolio screen is stale during a volatility spike:

- Users place orders they can't cover → **rejected trades**, a broken and confusing experience at the worst possible moment.
- Some users see stale positions and **sell or buy on wrong information** → real financial harm and complaints.
- Support is buried in tickets; Brokerage Ops is firefighting; trust in "your money, right now" erodes exactly when the most users are watching.

The market doesn't calm down on our schedule. The pipeline has to catch up *while the spike is still happening.*

---

## 3. The cast in play

- **Me — SRE, Data Platforms.** On call when Marcus pages. I own the pipeline's signals, and the diagnosis is mine to make. The whole project is my decision: is this a capacity problem, or something else? Get it wrong and I either waste time scaling the wrong thing or make it worse.

- **Priya Nair — data engineer, owns the positions consumer.** She wrote the consumer, including the choice to key events by **ticker symbol** so each symbol's trades stay in order. That choice is exactly what concentrated the $VOLT surge onto one partition — so the cause traces back to her design, though she can't see it yet. On the call she pushes hard for "add more consumers." She's not an SRE; she's reasoning from "more volume → more workers." My job is to show her, with the graph, why that won't help here — and to have the harder conversation about her keying strategy.

- **Marcus Webb — Brokerage Operations lead.** He's the one absorbing the business damage in real time: the support queue, the rejected orders, the angry users. He gives me the first, most valuable clue without knowing it — *"every other stock looks fine"* — which points at one symbol, not the whole platform. He needs an honest ETA and he speaks in user impact, so I have to translate the fix back into "when will $VOLT balances be fresh again."

---

## 4. The pipeline

Every trade fill emits a `TRADE_EXECUTED` event. A consumer group reads it, updates the user's positions, and writes them to the **positions store** — the pipeline's *sink* — which sits behind the portfolio screen.

```
  trade fills   ──▶  trades-events   ──▶  positions consumer   ──▶  positions store  ──▶  portfolio
  (TRADE_          (Kafka topic,         group (updates each      (Postgres — the      screen in
   EXECUTED)        partitioned)          user's holdings)         app reads here)      the app
```

**The sink is Postgres.** The positions store is a Postgres database holding each user's current holdings and buying power (a natural per-user, per-symbol relational upsert). The consumer writes into it on every `TRADE_EXECUTED`; the portfolio screen reads from it. Postgres is Meridian's operational database, so it's the production-faithful store here — and because the scenario's fault is consumer lag, not sink slowness, a fast well-understood write target keeps the bottleneck where we intend it.

**Where the scenario hits this pipeline:**

- Events are **keyed by ticker symbol** so all trades for one symbol land on the same partition, in order. This is Priya's design choice, and normally it's fine.
- Kafka splits `trades-events` into **partitions** — independent, ordered logs processed in parallel, one consumer instance per partition at most.
- When $VOLT's volume explodes, **all of it hashes onto a single partition.** One consumer instance is buried; the others sit nearly idle. The pipeline's total capacity is barely touched — but the one partition carrying $VOLT falls badly behind, so $VOLT users see stale balances while everyone else is fine.

That last line is the whole incident: **the backlog is real, but it lives in one partition, not the group.**

---

## 5. The SRE work

**The metric.** The single metric is consumer lag, observed in two forms. Both are required: records lag shows *where* the backlog is; event age shows *how much* it affects users. Operational definitions — exact formula, unit, and dashboard panels — are in Module 0.

Records lag, per partition
- Records produced to a partition but not yet processed by the consumer group (source: Kafka Exporter, `kafka_consumergroup_lag`).
- Its distribution across partitions separates the two failure modes: one partition behind is key skew; all partitions behind is a capacity shortfall.

Event age, p99
- Seconds from when a trade fills to when the portfolio reflects it (source: a Prometheus histogram exposed by the positions consumer).
- The user-facing staleness, and the signal the Service Level Objective is written against.

**Service Level Objective:** event age p99 < 5 seconds during market hours. Each term is deliberate:
- **Time-based, not count-based** — the harm is time-based. "500 records behind" has no business meaning; "5 seconds stale" does.
- **p99, not p95** — the cost is per user, per stale order. At spike volume, p95 leaves too many users in breach.
- **5 seconds** — during volatility, users place follow-up orders within a few seconds; past that, the next order rides on stale data and is rejected.

**Service Level Agreement:** Meridian makes no per-second freshness promise to users, but regulators expect accurate balances. The SLO is an internal early-warning threshold set well inside that expectation.

**Diagnosis — how the signals localize the fault:**
- Records lag per partition: one partition climbing while the others stay flat indicates key skew, not a capacity shortfall.
- Event age p99: rises only for users trading the hot ticker.
- Consumer-group parallelism is capped at the partition count. Adding consumers past that count does nothing; key skew is resolved by changing the partitioning, not by adding capacity.

---

## 6. Modules

Each module produces committed deliverables and has numeric exit criteria. Do not advance until every exit criterion is met and every deliverable exists.

### Module 0 — Baseline

**Goal:** establish and record the numeric "healthy" range of each signal, at a normal trade rate, before any fault is introduced.

**Signals defined here** (used by all three modules):

Records lag, per partition
- **Source:** Kafka Exporter metric `kafka_consumergroup_lag`, labeled by `topic`, `partition`, and `consumergroup`.
- **Definition:** per partition, the partition's log-end offset minus the consumer group's committed offset — the count of records produced to that partition but not yet processed by the positions consumer.
- **Unit:** records (integer count).
- **Role:** localizes *where* backlog accumulates. Read as a distribution across the 6 partitions: near-zero and even is healthy; one partition rising while the others stay flat is key skew; all partitions rising is a capacity shortfall.
- **Panel:** one time series with a line per partition.

Event age, p99
- **Source:** a Prometheus histogram exposed by the positions consumer and scraped by Prometheus.
- **Definition:** seconds between a trade's `filled_at` timestamp (the fill time carried in the event) and the wall-clock time the consumer commits that trade's position write to Postgres. p99 is computed over the histogram with `histogram_quantile(0.99, ...)`.
- **Unit:** seconds.
- **Role:** user-facing staleness — how out of date the portfolio screen is. This is the signal the Service Level Objective is written against.
- **Panel:** one time series of the p99.

**Normal trade rate:** the producer emits `TRADE_EXECUTED` at a steady **25 records/second** across the full 50-symbol universe, keyed by `symbol`, with no skew (rate set by `TRADE_RATE_PER_SEC`). This is the baseline load; Module 1 departs from it.

**Work:**
1. Run the full pipeline (producer → `trades-events` → positions consumer → `meridian-postgres`) at the normal trade rate.
2. Deploy the observability stack: Prometheus and Grafana.
3. Wire records lag: deploy Kafka Exporter, scrape it into Prometheus, and build the per-partition Grafana panel defined above.
4. Instrument the positions consumer to record event age as a Prometheus histogram, and build the p99 Grafana panel.

**Deliverables (committed):**
- `baseline.md`, recording: the observed per-partition records-lag range over the quiet window; the observed event-age p99 over the quiet window; and the agreed Service Level Objective (event age p99 < 5s during market hours).
- The Grafana dashboard as code (sidecar ConfigMap) containing both panels.

**Exit criteria:**
- Both panels are live and populated from real pipeline traffic.
- Over a continuous 15-minute quiet window at the normal trade rate, per-partition records lag stays near zero and even across all 6 partitions (no partition materially above the others).
- Over that same window, event age p99 stays below 5 seconds for the entire duration.
- `baseline.md` and the dashboard ConfigMap are committed, with `baseline.md` populated from the observed numbers above.

---

### Module 1 — Simulate the volatility spike

**Goal:** reproduce the $VOLT incident by injecting key skew, and prove on the dashboard that the fault is unambiguous and breaches the Service Level Objective.

**Work:**
1. Skew the producer so one ticker ($VOLT) dominates the trade mix, concentrating its volume on the single partition its key hashes to. Record the injected parameters (skew percentage, target rate).
2. Hold the injected load and observe both signals until the exit criteria are met.

**Deliverables (committed):**
- `module-1-simulation.md`, recording: the injected parameters (skew %, target rate); the start and stop timestamps; and the peak value each signal reached.
- A dashboard snapshot (image) captured at peak, showing the skew fingerprint.

**Exit criteria:**
- One partition's records lag is clearly separated from the rest — the hot partition keeps climbing while the others stay within their baseline range.
- Event age p99 for the hot ticker crosses the 5s SLO and continues rising, reaching at least 25 seconds (5× the SLO).
- Both of the above hold continuously for at least 10 minutes, with the hot partition's lag still growing (unbounded), not plateauing.
- If any criterion is not met — lag plateaus near baseline, or event age never clears 5s — the injected load was insufficient: increase the skew or rate and repeat.

---

### Module 2 — Identify, troubleshoot, and fix

**Goal:** run the observe → localize → remediate → verify loop and prove recovery against the baseline, then commit the alert and runbook that prevent recurrence.

**Work:**
1. **Localize:** read records lag per partition — one partition hot, the rest flat — and identify the fault as key skew, not a capacity shortfall. Confirm one consumer instance is pinned busy while its peers sit idle.
2. **Root-cause:** establish that consumer-group parallelism is capped at the partition count, that keying by ticker concentrated $VOLT on one partition, and that adding consumers past the partition count therefore does nothing.
3. **Remediate:** change the partitioning so hot-ticker traffic spreads across partitions (a higher-cardinality key, e.g. `symbol + user bucket`), and/or add partitions. State explicitly whether the change is a *fix* (removes the cause) or a *mask* (buys time), and state the message-ordering tradeoff it introduces.

**Deliverables (committed):**
- `module-2-remediation.md`, a recovery record / mini-postmortem: incident timeline, root cause, the change applied, the fix-vs-mask determination, and the ordering tradeoff.
- The alert rule for per-partition lag skew, committed as code.
- The runbook entry: hot-ticker surge → check per-partition skew first; adding consumers past the partition count is a no-op.

**Exit criteria** (measured against `baseline.md`):
- Per-partition records lag is back within baseline range and even across partitions, with no single hot partition, and previously-idle consumers sharing load.
- Event age p99 is back below 5 seconds.
- Both of the above hold continuously for at least 15 minutes after the change.
- The new skew alert fires when replayed against the Module 1 fault (it would have paged before the business noticed).

---

## 7. Definition of done

- [ ] **M0:** both lag signals live; `baseline.md` + dashboard-as-code committed; 15-min quiet-window criterion met.
- [ ] **M1:** spike reproduced to the numeric bar (unambiguous fingerprint + SLO breached ≥5× + sustained ≥10 min); `module-1-simulation.md` + peak snapshot committed.
- [ ] **M2:** localized to key skew from the signals; remediated; recovery verified (even lag + p99 <5s, sustained ≥15 min); `module-2-remediation.md`, skew alert, and runbook committed; alert proven to fire against the M1 fault.
