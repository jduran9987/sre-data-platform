# Status — where we are right now

> Living document. See [CLAUDE.md](../CLAUDE.md#maintaining-statusmd) for how to maintain this file.
>
> Two parts: **Current state** is forward-looking (active project, module, next step) and is overwritten as work moves. **Changelog** is the historical record for the active project — timestamped session summaries, oldest first, appended never rewritten. When a new project begins, the changelog is cleared and started fresh.

## Current state

**Active project:** [Project 01 — Consumer Lag During a Volatility Spike](projects/project-01-consumer-lag.md)

**Module:** Module 0 — Baseline (stand up the pipeline and observability; record what "healthy" looks like before anything breaks).

**Next step:** Build the **positions consumer** (in `project-01-consumer-lag`). It reads `trades-events`, upserts each user's holdings/buying-power into `meridian-postgres` → `positions`, and is instrumented to expose **event-age** (`filled_at` → portfolio-write, seconds) as a Prometheus metric. Key facts to build against:
- **Kafka:** bootstrap `meridian-kafka-kafka-bootstrap.data-platform.svc.cluster.local:9092`, topic `trades-events`, its own consumer group.
- **Postgres:** RW service `meridian-postgres-rw.data-platform.svc.cluster.local:5432`, database `positions`, user `app`, credentials in secret `meridian-postgres-app` — which lives in `data-platform`, so **the consumer (in `project-01-consumer-lag`) needs those credentials made available in its namespace** (secrets are namespace-scoped; decide how to replicate/reference).
- **Schema is consumer-owned:** the `holdings` + `buying_power` tables don't exist yet — the consumer creates them (migrations on startup). Upsert idempotently by `trade_id`.
- **Event JSON schema:** `trade_id`, `user_id`, `symbol`, `side` (BUY/SELL), `quantity`, `price`, `filled_at` (RFC 3339 UTC — the event-age backbone).

**Namespace split (decided 2026-09-04):** shared **data stores + operators** stay in `data-platform` — the Kafka cluster, its topics, and (like Kafka) a **shared Postgres cluster reused across projects**, with the positions data as a **database inside it**. Only project-specific **app workloads** — producer, positions consumer, this project's dashboards/alerts — live in the **`project-01-consumer-lag`** namespace.

**Remaining in Module 0, after the consumer:**

1. **Observability stack** — Prometheus + Grafana (a `kube-prometheus-stack` release is already declared in `helmfile.yaml`, not yet installed), plus **Kafka Exporter** for per-partition records lag.
2. **Dashboard-as-code** — both signals (per-partition records lag; event-age p99) as a sidecar ConfigMap.

**Module 0 exit criterion:** over a 15-minute quiet window — records lag flat, near zero, even across partitions; event-age p99 < 5s the whole window. Record those numbers in `baseline.md` along with the agreed Service Level Objective.

## Changelog — Project 01

Session summaries for the active project, oldest first. Append a new dated entry at the end of each working session; do not rewrite past entries.

### 2026-09-03 — Cluster + Kafka up

Module 0's foundation is in place:

- **minikube cluster** `sre-data` — 3 nodes, provisioned for stress work. Bootstrap steps recorded in [setup.md](setup.md).
- **CSI hostpath storage** enabled (required — the default hostPath StorageClass silently ignores `fsGroup`, which breaks Kafka's non-root writes). See [setup.md](setup.md) for the why.
- **Strimzi Kafka operator** 1.2.0 installed via helmfile into `data-platform`, watching that namespace.
- **`meridian` Kafka cluster** running and `Ready`: 3 controllers + 3 brokers, KRaft, replication factor 3, `min.insync.replicas=2`, persistent storage on `csi-hostpath-sc`. Manifest: `kubernetes/kafka/kafka-cluster.yaml`.

### 2026-09-04 — `trades-events` topic created

- **`trades-events` `KafkaTopic`** applied to `data-platform` and reconciled `Ready`. Manifest: `kubernetes/kafka/trades-events-topic.yaml`.
- **Partition count: 6** — chosen deliberately (this is the knob the whole scenario turns on): even across 3 brokers, gives a clear 1-hot-vs-5-flat skew fingerprint for Module 1, and leaves room to demo scaling consumers up to the partition count in Module 2. Replication factor 3, `min.insync.replicas=2`, 7-day retention.
- **Verified at the broker** (`kafka-topics.sh --describe`): 6 partitions, RF 3, leaders spread 2 per broker, every partition's ISR = all 3 replicas.
- **Decided the namespace split** (see Current state) — topic stays in `data-platform`; project app workloads will go in a project namespace.

### 2026-09-04 — Postgres operator, naming convention, Kafka renamed

- **`project-01-consumer-lag` namespace** created for this project's app workloads. Manifest: `kubernetes/namespaces/project-01-consumer-lag.yaml`.
- **CloudNativePG operator** installed via helmfile into `data-platform` (chart `0.29.0`, operator 1.30.0). Running `1/1`. Values: `helm/cloudnative-pg/values.yaml`. This is the operator for the shared Postgres sink.
- **Naming convention adopted** (now in [CLAUDE.md](../CLAUDE.md#infrastructure-conventions)): shared data-store instances are named `meridian-<tool>` so operator-derived child resources stay self-identifying and collision-proof in the shared namespace.
- **Kafka cluster renamed `meridian` → `meridian-kafka`** to match. Strimzi can't rename in place, so the cluster + topic were deleted (PVCs cleared) and recreated. Bootstrap service is now **`meridian-kafka-kafka-bootstrap:9092`**. Cluster back `Ready` (3 controllers + 3 brokers), topic re-reconciled `Ready` on `meridian-kafka`.
- **Known issue to watch:** `strimzi-cluster-operator` showed ~73 restarts (crashlooping intermittently). Brokers reconcile fine, but diagnose before relying on it.

### 2026-09-05 — Postgres sink + producer built; cluster stability diagnosed

**Postgres sink complete.**
- **`meridian-postgres` cluster** applied and `Ready` — CloudNativePG, PostgreSQL 18.4, 3 instances (1 primary + 2 replicas). Manifest: `kubernetes/postgres/postgres-cluster.yaml`.
- **`positions` database** created via a CNPG `Database` CR (`APPLIED true`), owner `app`. Manifest: `kubernetes/postgres/positions-database.yaml`. Reach it at `meridian-postgres-rw`, secret `meridian-postgres-app`. Tables not yet created — consumer owns schema.

**Producer built, deployed, verified — then paused.**
- Code at `apps/project-01-consumer-lag/producer/` (`producer.py`, `Dockerfile`, `requirements.txt`). Python + `confluent-kafka==2.15.0`, JSON events keyed by `symbol`, **idempotent** producer (`acks=all` + `enable.idempotence`), graceful SIGTERM shutdown (flush), 50-ticker universe (incl. `VOLT`), rate `TRADE_RATE_PER_SEC` (default 25/s).
- Deployment: `kubernetes/projects/project-01-consumer-lag/producer-deployment.yaml`. Image `project-01-consumer-lag/producer:0.1.2`. **Currently scaled to 0 (paused)** — resume with `kubectl scale ... --replicas=1`.
- Verified producing evenly across all 6 partitions. Baseline distribution note: partition-count vs symbol-count hashing means one partition runs ~2× hotter (small-numbers effect, accepted — lag baseline is unaffected, and VOLT's 30× Module-1 spike will dominate it).

**Conventions/infra learned this session (also in [CLAUDE.md](../CLAUDE.md#infrastructure-conventions)):**
- **Repo layout for apps:** project-scoped — `apps/<project>/<app>/` for code, `kubernetes/projects/<project>/` for that project's workload manifests. Shared platform stays under `kubernetes/kafka|postgres|namespaces`.
- **Cross-namespace addressing:** app workloads in `project-01-consumer-lag` must use the fully-qualified `<svc>.data-platform.svc.cluster.local` name to reach Kafka/Postgres (this bit the producer).
- **minikube images:** build then `minikube image load <tag>` (distributes to ALL 3 nodes; `image build` alone doesn't). Use an **immutable tag per build** (never reuse) + `imagePullPolicy: IfNotPresent`. A stale image on the worker node cost us a debug cycle.
- **Pod security:** `runAsNonRoot: true` needs a numeric `runAsUser` (non-numeric image `USER` fails with `CreateContainerConfigError`).

**Cluster stability diagnosed (resolved as benign).**
- The Kafka **KRaft controller quorum was flapping** — `LeaderEpoch` climbed to 46 — which caused the producer's idempotence PID acquisition to intermittently fail (`Coordinator load in progress`). A **full Kafka teardown + rebuild** (delete cluster, clear PVCs, fresh operator via `rollout restart`) produced a stable quorum holding `LeaderEpoch: 1`. **Root cause: the host laptop sleeping pauses the minikube VM → raft fetch timeouts → re-election on wake.** Benign, self-healing local-dev artifact; not a production issue. Keep the laptop awake during Module 1 fault injection. The Strimzi operator crashloop also cleared on the `rollout restart`.
