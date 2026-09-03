# Meridian — Company & Cast

> Shared context for all projects. Update this as projects introduce new cast members.

## The company

**Meridian** is a commission-free retail stock-trading app.

Meridian gives everyday people direct access to the financial markets that used to be reserved for professionals: commission-free trading, real-time market data, and a live view of their own money. For millions of users, it is the primary way they invest, track their positions, and act on the market from their phone.

## The data platform

Everything Meridian shows a user — balances, positions, market data, history — is produced by the **data platform**. It is the set of systems that move, store, process, and serve the company's data.

**My role:** SRE on the Data Platforms team. I own the **reliability, availability, troubleshooting, maintenance, and performance** of all tools within the data platform:

- **Kafka** — event streaming
- **Postgres** — operational databases
- **AWS** — cloud infrastructure
- **Spark** — large-scale batch processing
- **Flink** — stream processing
- **Iceberg** — table format for the data lake
- **Parquet** — columnar storage format
- **Kubernetes** — orchestration for the platform's workloads

## The cast

Each person has a trait that *changes how an incident plays out* — not decoration.

- **Me** — SRE, Data Platforms. Own the reliability, availability, troubleshooting, maintenance, and performance of every tool above.
- **Priya Nair** — data engineer, owns the consumer applications' logic. Strong engineer, **not a trained SRE**: her reflex for any slowdown is "just add more consumers." Sometimes right, often not — and knowing which is the SRE's job, not hers.
- **Marcus Webb** — Brokerage Operations lead; the business stakeholder. Feels user pain first and pages me. **Speaks in user impact** ("people are seeing wrong balances"), never in records or percentiles — translating his complaint into a signal is on me.
