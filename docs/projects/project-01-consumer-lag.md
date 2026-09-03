# Project 01 — Consumer Lag During a Volatility Spike

> World and full cast: see [meridian.md](meridian.md).

---

## 1. The business story

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

## 2. The cast in play

- **Me — SRE, Data Platforms.** On call when Marcus pages. I own the pipeline's signals, and the diagnosis is mine to make. The whole project is my decision: is this a capacity problem, or something else? Get it wrong and I either waste time scaling the wrong thing or make it worse.

- **Priya Nair — data engineer, owns the positions consumer.** She wrote the consumer, including the choice to key events by **ticker symbol** so each symbol's trades stay in order. That choice is exactly what concentrated the $VOLT surge onto one partition — so the cause traces back to her design, though she can't see it yet. On the call she pushes hard for "add more consumers." She's not an SRE; she's reasoning from "more volume → more workers." My job is to show her, with the graph, why that won't help here — and to have the harder conversation about her keying strategy.

- **Marcus Webb — Brokerage Operations lead.** He's the one absorbing the business damage in real time: the support queue, the rejected orders, the angry users. He gives me the first, most valuable clue without knowing it — *"every other stock looks fine"* — which points at one symbol, not the whole platform. He needs an honest ETA and he speaks in user impact, so I have to translate the fix back into "when will $VOLT balances be fresh again."

---

## 3. The pipeline

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

## 4. The SRE work

This is the most important section — the point of the project. It has four parts:

1. **What we measure** — the metric, in two forms.
2. **The target we hold ourselves to** — the SLO, every number justified by the business.
3. **What we've promised outside the team** — the SLA.
4. **How the two forms of the metric localize the fault** — the diagnosis.

### 4.1 What we measure — consumer lag, in two forms

The single metric is **consumer lag**: how far behind the consumer is. We need it in two forms because they answer different questions.

**Form A — Records lag, per partition** *(where the backlog is)*
- **Source:** Kafka Exporter — `kafka_consumergroup_lag` (infrastructure metric, no app changes).
- **Meaning:** for each partition, how many events have arrived but aren't processed yet.
- **Why per partition:** the *shape across partitions* is the diagnosis. Even lag = the whole group is behind. Lag on one partition = skew.

**Form B — Event age, p99** *(how much it hurts the user)*
- **Source:** our own consumer, **instrumented** to expose a Prometheus metric (Kafka can't see this — it's a fact about the app's per-event work).
- **Meaning:** seconds from when a trade filled to when the portfolio reflects it. Literally "how stale the app is." This is what Marcus feels.

> Records lag says *where the problem is*; event age says *how badly it hurts*. You can't fix seconds directly — you fix the backlog's cause, and records-per-partition lag points at it. The scenario needs both.

### 4.2 The target — the SLO, justified by the business

**SLO (Service Level Objective — an internal target we hold ourselves to):**
**portfolio freshness, event age p99 < 5 seconds during market hours.**

Every choice below is a business decision, not an engineering preference:

- **Why a *time* SLO, not a records one?** The harm is time-shaped — a user acts on a stale screen. "500 records behind" means nothing to Marcus; "5 seconds stale" maps directly to a wrong buying-power number.
- **Why 5 seconds?** The deadline is *the user's next action.* During volatility users fire follow-up orders within a few seconds. If buying power is stale past that gap, the next order rides on wrong data and gets rejected. Five seconds sits just inside a realistic back-to-back-order interval, with margin for normal processing jitter — tight enough to protect the decision, loose enough not to page on noise.
- **Why p99, not p95?** The cost is **per user, per stale order.** At meme-stock volume, p95 lets 1-in-20 events breach — during a spike that's a large, visible crowd of users placing orders on stale balances. p99 caps it at 1-in-100. The tail *is* the harm, so we hold a tight percentile.
- **Why not p99.9?** Chasing the last 0.1% costs disproportionate engineering effort and error budget for a marginal gain. We'd tighten only if incidents showed that top 0.1% causing real, repeated harm.

### 4.3 The commitment — the SLA

**SLA (Service Level Agreement — an external promise with consequences):** Meridian makes no per-second freshness promise to users, but **regulators expect accurate balances.** Our SLO is the internal early-warning line, set well inside that regulatory expectation, so we catch drift long before it becomes a compliance problem.

### 4.4 The diagnosis — reading the two forms together

The skill is knowing *which form moves, and how*:

```
  Records lag, per partition:   ONE partition climbs, the others flat   → skew, not capacity
  Event age p99:                rises — but only for users on the hot ticker
```

- Total lag being up **tempts** Priya's "add more consumers."
- The **per-partition shape** overrules that: one hot partition = one hot key = one worker doing the job. A consumer group's parallelism is **capped at the partition count**, so extra consumers just idle. Adding capacity cannot fix a skew problem — the correct move is to change the partitioning.

---

## 5. Modules

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

## 6. Definition of done

- [ ] **M0:** both lag signals live; `baseline.md` + dashboard-as-code committed; 15-min quiet-window criterion met.
- [ ] **M1:** spike reproduced to the numeric bar (unambiguous fingerprint + SLO breached ≥5× + sustained ≥10 min); `module-1-simulation.md` + peak snapshot committed.
- [ ] **M2:** localized to key skew from the signals; remediated; recovery verified (even lag + p99 <5s, sustained ≥15 min); `module-2-remediation.md`, skew alert, and runbook committed; alert proven to fire against the M1 fault.
