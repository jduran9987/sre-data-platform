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

The single metric is **consumer lag**, measured two ways.

**Records lag, per partition** — source: Kafka Exporter (`kafka_consumergroup_lag`). Events that have arrived but aren't yet processed, broken down by partition. The distribution across partitions is what distinguishes skew (one partition behind) from capacity (all partitions behind).

**Event age, p99** — source: a Prometheus metric exposed by our own consumer. Seconds from when a trade fills to when the portfolio reflects it. This is the user-facing staleness.

Records lag shows *where* the backlog is; event age shows *how much* it affects users. Both are required.

**Service Level Objective:** event age p99 < 5 seconds during market hours.

- **Time-based** — the harm is time-based. A user acts on a stale balance; "500 records behind" is not meaningful to the business, "5 seconds stale" is.
- **p99** — the cost is per user, per stale order. At spike volume, p95 lets too many users breach.
- **5 seconds** — during volatility, users place follow-up orders within a few seconds. Past that, the next order rides on stale data and is rejected.

**Service Level Agreement:** Meridian makes no per-second freshness promise to users, but regulators expect accurate balances. The SLO is an internal early-warning line set well inside that expectation.

**Diagnosis:**

- Records lag per partition: one partition climbing while the others stay flat means skew, not capacity.
- Event age p99: rises only for users on the hot ticker.
- A consumer group's parallelism is capped at the partition count, so adding consumers past that count does nothing. Skew is fixed by changing the partitioning, not by adding capacity.

---

## 6. Modules

Each module produces a **committed deliverable** and has a **numeric exit criterion** — you don't move on until the number is met and the artifact exists.

### Module 0 — Baseline

**Goal:** know what "healthy" looks like, numerically, before anything breaks.

**Build:**
- Pipeline running steadily at a normal trade rate.
- **Records lag** wired: Kafka Exporter → Prometheus → per-partition Grafana panel.
- **Event age** wired: instrument the consumer to record seconds from trade-fill to portfolio-write; Grafana panel for p99.

**Deliverables (committed):**
- `baseline.md` — the recorded normal range of each signal (e.g. per-partition records lag `0–N`, event age p99 `~Xs`) and the agreed SLO.
- The Grafana dashboard **as code** (sidecar ConfigMap), so both panels are reproducible.

**Exit criterion (how we know the baseline is real):**
- Both signals live on the dashboard, and over a **15-minute quiet window**: records lag flat, near zero, and **even across partitions**; event age p99 **< 5s** the whole window. Those observed numbers are what `baseline.md` records.

---

### Module 1 — Simulate the volatility spike

**Goal:** reproduce the $VOLT event and prove the fault is unambiguous on the dashboard.

**Inject the fault (by hand):** skew the producer so one ticker dominates, landing the surge on a single partition.

**Deliverables (committed):**
- `module-1-simulation.md` — the **fault record:** the injected parameters (skew %, target rate), start/stop timestamps, and the peak value each signal reached.
- A **dashboard snapshot** (image) captured at peak, showing the skew fingerprint.

**Exit criterion (how we know we've spiked *enough*):** all three must hold, so there's no doubt it's skew and no doubt it hurts —
1. **Fingerprint is unambiguous:** one partition's records lag is **clearly separated** from the others — the hot partition keeps climbing while the rest stay in baseline range.
2. **SLO is breached with margin:** event age p99 for the hot ticker crosses the 5s SLO and keeps rising — sustain until it reaches **≥ 25s (5× the SLO)**, so it's plainly a breach, not jitter.
3. **It's sustained, not a blip:** the above holds for **≥ 10 minutes**, and the hot partition's lag is still growing (unbounded), not plateauing at baseline.

> If lag plateaus near baseline or event age never clears 5s, the spike was too small — increase the skew or rate and repeat. That's the point of a numeric bar.

---

### Module 2 — Identify, troubleshoot, and fix

**Goal:** run the real SRE loop and prove recovery, not just a dip.

**Identify:** read records lag *per partition* → one hot, others flat → **key skew**, not capacity. Confirm one consumer instance pinned busy while peers idle.

**Root-cause call to Priya, with the graph:** parallelism is capped at partition count; her ticker-keying concentrated $VOLT on one partition; adding consumers is a no-op.

**Remediate:** change partitioning so hot-ticker traffic spreads (higher-cardinality key, e.g. `symbol + user bucket`), and/or add partitions. State whether it's a *fix* (removes the cause) or a *mask* (buys time), and the ordering tradeoff.

**Deliverables (committed):**
- `module-2-remediation.md` — a **recovery record** / mini-postmortem: timeline, root cause, the fix applied, fix-vs-mask call, and the ordering tradeoff discussed with Priya.
- The **alert rule** — per-partition lag skew (committed as code).
- The **runbook entry** — "hot-ticker surge → check per-partition skew first; adding consumers past partition count is a no-op."

**Exit criterion (how we know we're *back to normal*):** measured against `baseline.md`, all must hold —
1. **Where:** per-partition records lag is back in baseline range **and even** across partitions (no single hot partition); previously-idle consumers are sharing load.
2. **How bad:** event age p99 is back **< 5s**.
3. **Stable, not a dip:** both hold continuously for **≥ 15 minutes** after the fix — long enough to prove recovery, not a momentary drain.
4. **Prevention proven:** the new skew alert **fires** when replayed against the Module 1 fault (i.e. it would have paged me before Marcus).

---

## 7. Definition of done

- [ ] **M0:** both lag signals live; `baseline.md` + dashboard-as-code committed; 15-min quiet-window criterion met.
- [ ] **M1:** spike reproduced to the numeric bar (unambiguous fingerprint + SLO breached ≥5× + sustained ≥10 min); `module-1-simulation.md` + peak snapshot committed.
- [ ] **M2:** localized to key skew from the signals; remediated; recovery verified (even lag + p99 <5s, sustained ≥15 min); `module-2-remediation.md`, skew alert, and runbook committed; alert proven to fire against the M1 fault.
