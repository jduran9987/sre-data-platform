# SRE for Data Platforms

A hands-on project for learning to operate a **data platform** as a **Site Reliability Engineer** — keeping streaming, storage, and processing systems reliable, available, and performant, and diagnosing them when they misbehave.

The focus is the SRE craft, not tool trivia:

- **Observability** — wire up real signals (metrics, dashboards) that mean something.
- **Diagnosis** — the `observe → localize → remediate → verify` loop.
- **Distributed-systems reasoning** — why partitioning, replication, coordination, and back-pressure cause or complicate failures.
- **Prevention** — Service Level Objectives, alerts, and runbooks.

Individual tools are learned only as deeply as the reliability work in front of us requires.

## The platform

Everything is built for **Meridian** — a *fictional* commission-free retail stock-trading app (think Robinhood). Meridian's data platform is what produces everything a user sees: balances, positions, market data, history. A realistic, multi-tool stack:

```
                    ┌────────────────────────────────────────────┐
                    │            Meridian data platform          │
                    ├────────────────────────────────────────────┤
   streaming  ──▶   │   Kafka          Flink                     │
   storage    ──▶   │   Postgres       Iceberg        Parquet    │
   batch      ──▶   │   Spark          Airflow                   │
                    ├────────────────────────────────────────────┤
                    │          Kubernetes    +    AWS            │
                    └────────────────────────────────────────────┘
```

- **Kafka** — event streaming
- **Flink** — stream processing
- **Spark** — large-scale batch processing
- **Airflow** — workflow orchestration
- **Postgres** — operational databases
- **Iceberg** / **Parquet** — data-lake table format & columnar storage
- **Kubernetes** / **AWS** — where the platform runs

## How the learning works

Learning happens through **scoped projects**. Each project is:

- **Hyper-focused** — one reliability problem, start to finish.
- **Business-scenario-driven** — a real incident at Meridian, with user impact and a cast of stakeholders, not an abstract exercise.
- **Production-faithful** — real signals, real Service Level Objectives, real remediations a peer SRE would recognize. No dumbed-down shortcuts.

Every project runs in the same **three modules**:

```
  Module 0            Module 1                 Module 2
  Baseline     ──▶    Simulation        ──▶    Resolution
  ───────────         ────────────             ──────────────────
  Stand up the        Inject the fault;        observe → localize →
  pipeline; record    prove it's obvious       remediate → verify.
  what "healthy"      on the dashboard         Prove recovery; ship
  looks like,         and breaches the         the alert + runbook
  numerically.        Service Level            that prevent a repeat.
                      Objective.
```

Each module produces a **committed deliverable** and has a **numeric exit criterion** — you don't advance until the number is met and the artifact exists.

## Repo layout

```
docs/
  projects/       one doc per project (scenario, pipeline, SRE work, modules)
  setup.md        stand up the cluster from scratch
  status.md       where the work stands right now
helmfile.yaml     all Helm releases (operators, charts), pinned
helm/<tool>/      chart values per release
kubernetes/       raw manifests we own (namespaces, Kafka cluster, topics, …)
```

## Getting started

See **[docs/setup.md](docs/setup.md)** to stand up the cluster and the platform from scratch.

To understand the world and the current work, start with **[docs/projects/meridian.md](docs/projects/meridian.md)** (the company and cast) and **[docs/status.md](docs/status.md)** (the active project and next step).
