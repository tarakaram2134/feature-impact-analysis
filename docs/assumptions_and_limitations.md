# Assumptions and Limitations

This project uses a simulated SaaS dataset to replicate a real-world product analytics workflow.

While the analytical approach mirrors industry practices, several assumptions and limitations apply.

---

## Simulated Dataset

The dataset used in this project is synthetically generated to simulate realistic SaaS product usage.

Data includes:

- user signups
- product events
- sessions
- subscription plans
- billing invoices

Because the dataset is simulated, observed relationships may not represent real user behavior.

---

## Observational Analysis

The analysis is observational rather than experimental.

This means:

- users self-select into feature adoption
- adopters may differ from non-adopters in ways not fully captured by the data

To mitigate this issue, the analysis controls for **baseline engagement levels**.

However, the results should not be interpreted as definitive causal proof.

---

## No A/B Experiment

The analysis does not use a randomized controlled experiment.

In production environments, feature impact is typically validated through:

- A/B testing
- randomized feature rollouts

Future analysis could incorporate experimental methods to validate the results.

---

## Short-Term Revenue Window

Revenue analysis focuses on the months immediately surrounding feature adoption.

Some product features influence revenue indirectly through:

- improved retention
- team expansion
- longer customer lifetimes

These effects may only appear over longer time horizons.

---

## Event Tracking Assumptions

Feature adoption is inferred from product event logs.

This assumes event instrumentation accurately captures feature usage.

In real production systems, event tracking issues such as missing events or delayed ingestion may occur.
