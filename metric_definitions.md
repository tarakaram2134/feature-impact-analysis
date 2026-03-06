# Metric Definitions

This document defines the core metrics used in the feature adoption analysis.

---

## Feature Adoption Rate

The percentage of users who use the feature at least once.

Formula:

Adoption Rate = Users Who Used Feature / Total Eligible Users

Adoption is defined as the first occurrence of a `team_collab_use` event.

---

## Time to Adoption

The number of days between user signup and the first usage of the feature.

Formula:

Time to Adoption = First Feature Event Timestamp − Signup Timestamp

This metric measures how quickly users discover and begin using the feature.

---

## Retention Rate

Retention measures the percentage of users who return to the product after signup.

Weekly retention is defined as:

Users with at least one session during a given week after signup.

Formula:

Retention Rate = Active Users in Week N / Total Users in Cohort

Retention curves are calculated for weeks 1–8 after signup.

---

## Cohort

A cohort groups users based on their signup week.

Cohort analysis allows tracking how retention changes over time across different groups of users.

---

## ARPU (Average Revenue Per User)

ARPU measures average subscription revenue generated per user.

Formula:

ARPU = Total Revenue / Number of Users

ARPU is calculated at the monthly level using billing invoice data.

---

## Revenue Relative to Adoption

Revenue is analyzed relative to the feature adoption event.

Month 0 represents the month when a user first adopts the feature.

Negative values represent months before adoption, while positive values represent months after adoption.

This analysis helps evaluate whether adoption leads to revenue expansion.
