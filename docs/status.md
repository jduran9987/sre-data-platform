# Status — where we are right now

> Living document. Update at the end of each working session: what changed, and the next step.

## Active project

**[Project 01 — Consumer Lag During a Volatility Spike](projects/project-01-consumer-lag.md)**

Currently in **Module 0 — Baseline** (stand up the pipeline and observability; record what "healthy" looks like before anything breaks).

## Work completed so far

**Cluster + Kafka are up.** Module 0's foundation is in place:

- **minikube cluster** `sre-data` — 3 nodes, provisioned for stress work. Bootstrap steps recorded in [setup.md](setup.md).
- **CSI hostpath storage** enabled (required — the default hostPath StorageClass silently ignores `fsGroup`, which breaks Kafka's non-root writes). See [setup.md](setup.md) for the why.
- **Strimzi Kafka operator** 1.2.0 installed via helmfile into `data-platform`, watching that namespace.
- **`meridian` Kafka cluster** running and `Ready`: 3 controllers + 3 brokers, KRaft, replication factor 3, `min.insync.replicas=2`, persistent storage on `csi-hostpath-sc`. Manifest: `kubernetes/kafka/kafka-cluster.yaml`.

## Next steps (Module 0 remainder)

1. **`trades-events` topic** — declare as a `KafkaTopic` manifest (topic operator is enabled). Decide the **partition count** deliberately: it's the knob the entire consumer-lag scenario turns on.
2. **Positions store (the sink)** — **Postgres**, where the consumer upserts each user's holdings and the portfolio screen reads from. Deploy production-style via a Postgres operator in `data-platform` (design when we get there).
3. **Producer** — emits `TRADE_EXECUTED` at a steady, normal trade rate.
4. **Positions consumer** — reads events, updates the store, and is **instrumented** to expose event-age (trade-fill → portfolio-write, in seconds) as a Prometheus metric.
5. **Observability stack** — Prometheus + Grafana, plus **Kafka Exporter** for per-partition records lag.
6. **Dashboard-as-code** — both signals (per-partition records lag; event-age p99) as a sidecar ConfigMap.

**Module 0 exit criterion:** over a 15-minute quiet window — records lag flat, near zero, even across partitions; event-age p99 < 5s the whole window. Record those numbers in `baseline.md` along with the agreed Service Level Objective.
