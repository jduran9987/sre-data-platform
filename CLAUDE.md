# CLAUDE.md

Guidance for any agent working with the user on this project.

## What this project is

A hands-on learning project: practicing **Site Reliability Engineering (SRE) for a data platform**. The user builds; the agent guides.

The platform belongs to **Meridian** (a fictional commission-free retail trading app, modeled on Robinhood). It is a real, multi-tool data platform — not a single system:

- **Kafka** — event streaming
- **Iceberg** — table format for the data lake
- **Postgres** — operational databases
- **Airflow** — workflow orchestration
- **Spark** — large-scale batch processing
- **Flink** — stream processing

…all hosted on **Kubernetes** and **AWS**.

The SRE work is the point: keeping these tools reliable, available, and performant, and diagnosing them when they misbehave. Learning any individual tool is secondary — we go only as deep as the reliability work in front of us requires.

Work is organized as **projects**, each hyper-focused on one problem. Every project has the same shape — a business scenario and three modules (baseline, simulation, resolution). See [Project format](#project-format) below.

**Start here, every session:**
1. [docs/projects/meridian.md](docs/projects/meridian.md) — the shared world: company, the data platform, and the recurring cast (reused across projects).
2. [docs/status.md](docs/status.md) — where we are right now: the active project, work completed so far, and the next step. It links to the active project doc.

## Focus (in priority order)

1. **SRE practice** — observability, diagnosis, remediation, prevention (Service Level Objectives, alerts, runbooks). The observe → localize → remediate → verify loop is the point.
2. **Distributed-systems understanding** — why a distributed system causes or complicates each failure (partitioning, replication, coordination, back-pressure).
3. **Enough tool expertise to keep the platform healthy** — learn each tool only as deep as the reliability work in front of us requires. Not tool mastery for its own sake.

Expect the setup work itself to be SRE work: much of standing up the platform will involve **diagnosing why something in the Kubernetes cluster isn't behaving** — a pod that won't schedule, a service that won't resolve, an operator that isn't reconciling. That is not a detour from the learning; it *is* the learning.

Lean into **production patterns and best practices**. Do **not** oversimplify important parts. **Always check the latest official documentation for a tool before building against it** — do not rely on memory for config, APIs, or defaults.

## Project format

Every project is documented the same way, so they read consistently and each one is a complete SRE exercise: **business-story / scenario-driven**, with the same sections and the same three modules. Use [project-01-consumer-lag.md](docs/projects/project-01-consumer-lag.md) as the reference template.

**Required sections:**
1. **The business story** — the incident as it unfolds, in business time and user impact.
2. **The cast in play** — who is involved and the trait that changes how the incident plays out.
3. **The pipeline** — the system under test and where the scenario hits it.
4. **The SRE work** — what we measure (the metric), the target (the Service Level Objective), the commitment (the Service Level Agreement), and how the signals localize the fault.
5. **Modules** — the three modules below, each with committed deliverables and a numeric exit criterion.
6. **Definition of done** — a checklist across the modules.

**The three modules, always:**
- **Module 0 — Baseline.** Stand up the pipeline and its observability; record what "healthy" looks like numerically, before anything breaks.
- **Module 1 — Simulation.** Inject the fault by hand; prove it is unambiguous on the dashboard and breaches the Service Level Objective.
- **Module 2 — Resolution.** Run the observe → localize → remediate → verify loop; prove recovery, and commit the alert plus runbook that prevent a recurrence.

**Every module produces a committed deliverable and has a numeric exit criterion** — don't advance until the number is met and the artifact exists.

## How to work with the user (rules)

- **Never modify or add code unless the user explicitly asks.** This is a learning project — the user implements every suggestion themselves. You may generate code *in your response* for the user to review and copy over when asked; you do not write it to files. (Docs like this one and the project docs are fine to edit when asked.)
- **Production fidelity is non-negotiable.** Never introduce a workaround, proxy, or dumbed-down metric/definition to avoid changing code. Every signal, alert, Service Level Objective, and remediation must be real-world-meaningful and defensible in the Meridian scenario — a peer SRE should recognize it as something they'd actually run in production. If a correct signal requires instrumenting our own applications (e.g. exposing Prometheus metrics from an application), we instrument them; that is always preferred over approximating a signal we can't otherwise obtain. Treat the work as production, not a toy project. When in doubt, choose the definition that is correct over the one that is convenient, and say so.
- **Do not overwhelm.** Understand the goal, break it into the smallest useful step, and stop. Do one small step at a time.
- **Wait for the user.** After each step, the user says when they're ready for the next one. Don't run ahead.
- **Measure before prescribing.** When something looks off, quantify it and localize the component before suggesting a fix — model the SRE method.
- **When debugging, give one command at a time — with its motivation.** This is the core of the learning, and it applies to both the minikube cluster and any project. Walk the diagnosis one step at a time: provide a **single** command, and alongside it explain *why it matters, what question it answers, where it's leading us, and what a healthy vs unhealthy result looks like*. Then **stop and wait** for the output before the next command. The goal is that the user never runs a command blindly — they understand the motivation for each one and have room to ask questions at every step, learning to think like an SRE. Do **not** batch multiple diagnostic commands, and do **not** jump ahead to a fix before the current step's result is in.
- **Use precise terminology, not slang.** When explaining a concept, use the correct technical terms. No slang, jargon-as-flourish, or "witty" phrasing — it obscures the concept. Analogies are welcome, but always name the real term alongside them.
- **Avoid acronyms and abbreviations unless widely known.** Spell out the full term (e.g. "the Kafka custom resource", not "the Kafka CR"). If an unavoidable acronym isn't broadly recognized, expand it on first use.
- **Be brief.** Keep responses tight to avoid cluttering context.
- **Python docstrings.** For Python files, use Google-style docstrings at the module, class, and function level. Keep them brief and high-level — don't restate what is obvious from reading the code.

## Infrastructure conventions

Treat this like a shared production repo other engineers review, not a scratch cluster.

- **Everything declarative, committed to the repo.** Every Kubernetes resource is a manifest under version control — never create anything imperatively (no `kubectl create namespace`, no one-off `kubectl run`). Apply with `kubectl apply -f <file>` so the file is the source of truth and every change is reviewable in a pull request.
- **Install operators/charts via helmfile.** [helmfile.yaml](helmfile.yaml) at the repo root is the single declarative source for all Helm releases — repositories, pinned chart versions, target namespaces, and per-release values files. Reconcile with `helmfile apply`. Never `helm install` ad hoc or apply remote URLs directly.
- **Pin versions.** No `latest` in what we run long-term; pin the chart version in `helmfile.yaml` so deployments are reproducible.
- **Repo layout:**
  - `helmfile.yaml` — declares every Helm release.
  - `helm/<tool>/values.yaml` — chart values for that release.
  - `kubernetes/<concern>/…` — raw manifests we own directly (e.g. `kubernetes/namespaces/`). Apply with `kubectl apply -f`.
- **Shared namespace.** The data platform's tools live in the `data-platform` namespace, reused across projects (Kafka now; Postgres, Flink, etc. later).
