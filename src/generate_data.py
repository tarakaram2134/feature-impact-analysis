import os
import json
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional

import numpy as np
import pandas as pd
import psycopg2


UTC = timezone.utc


@dataclass
class DbConfig:
    host: str = "localhost"
    port: int = 5432
    dbname: str = "feature_impact_analysis"
    user: str = "fia_user"
    password: str = "fia_password"


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _ensure_dirs() -> None:
    os.makedirs("data/raw", exist_ok=True)


def _weighted_choice(items: List[str], weights: List[float], size: int) -> List[str]:
    return list(np.random.choice(items, size=size, p=np.array(weights) / np.sum(weights)))


def generate_dims(
    n_users: int = 30000,
    start_date: str = "2025-08-01",
    end_date: str = "2026-02-15",
    seed: int = 7
) -> Dict[str, pd.DataFrame]:
    """
    Generates: dim_users, dim_plans, dim_features
    """
    rng = np.random.default_rng(seed)
    random.seed(seed)

    start = _utc(datetime.fromisoformat(start_date))
    end = _utc(datetime.fromisoformat(end_date))
    total_seconds = int((end - start).total_seconds())

    user_ids = [f"u_{i:06d}" for i in range(1, n_users + 1)]
    signup_offsets = rng.integers(0, total_seconds, size=n_users)
    signup_at = [start + timedelta(seconds=int(x)) for x in signup_offsets]

    industries = ["software", "education", "healthcare", "finance", "retail", "manufacturing"]
    industry = _weighted_choice(industries, [0.30, 0.18, 0.16, 0.12, 0.14, 0.10], n_users)

    company_sizes = ["1-10", "11-50", "51-200", "201-1000", "1000+"]
    company_size = _weighted_choice(company_sizes, [0.35, 0.32, 0.20, 0.10, 0.03], n_users)

    countries = ["US", "IN", "CA", "GB", "DE", "AU", "BR", "FR"]
    country = _weighted_choice(countries, [0.48, 0.18, 0.08, 0.06, 0.05, 0.04, 0.06, 0.05], n_users)

    channels = ["organic", "paid_search", "sales", "partner", "referral"]
    acquisition_channel = _weighted_choice(channels, [0.36, 0.20, 0.24, 0.10, 0.10], n_users)

    # small internal user set (to test filtering later)
    is_internal = rng.random(n_users) < 0.006

    dim_users = pd.DataFrame({
        "user_id": user_ids,
        "signup_at": signup_at,
        "industry": industry,
        "company_size": company_size,
        "country": country,
        "acquisition_channel": acquisition_channel,
        "is_internal": is_internal
    })

    dim_plans = pd.DataFrame([
        {"plan_id": "free", "plan_name": "Free", "monthly_price": 0.00, "seat_based": False},
        {"plan_id": "starter", "plan_name": "Starter", "monthly_price": 19.00, "seat_based": False},
        {"plan_id": "pro", "plan_name": "Pro", "monthly_price": 49.00, "seat_based": False},
        {"plan_id": "business", "plan_name": "Business", "monthly_price": 99.00, "seat_based": True},
    ])

    dim_features = pd.DataFrame([{
        "feature_name": "team_collab",
        "launched_at": _utc(datetime(2025, 12, 1, 0, 0, 0)),
        "rollout_start_at": _utc(datetime(2025, 12, 1, 0, 0, 0)),
        "rollout_pct_initial": 20.00,
        "notes": "Rolled out gradually to 100% over ~6 weeks"
    }])

    return {
        "dim_users": dim_users,
        "dim_plans": dim_plans,
        "dim_features": dim_features
    }


def generate_subscriptions_and_invoices(
    dim_users: pd.DataFrame,
    seed: int = 7
) -> Dict[str, pd.DataFrame]:
    """
    Simulates:
    - trial -> paid conversion
    - plan switching
    - churn
    - delayed billing invoices
    """
    rng = np.random.default_rng(seed)
    random.seed(seed)

    users = dim_users.copy()
    users = users[~users["is_internal"]].reset_index(drop=True)

    plan_ids = ["free", "starter", "pro", "business"]
    plan_weights = [0.52, 0.25, 0.18, 0.05]
    initial_plan = _weighted_choice(plan_ids, plan_weights, len(users))

    # Make larger companies more likely to start on higher plans
    size_to_boost = {"1-10": 0.0, "11-50": 0.04, "51-200": 0.09, "201-1000": 0.14, "1000+": 0.18}

    upgraded_plan = []
    for s, p in zip(users["company_size"], initial_plan):
        if p in ["pro", "business"]:
            upgraded_plan.append(p)
            continue
        prob_upgrade = 0.08 + size_to_boost.get(s, 0.0)
        if rng.random() < prob_upgrade:
            upgraded_plan.append("pro" if rng.random() < 0.75 else "business")
        else:
            upgraded_plan.append(p)

    # churn probability by plan (free churns a lot)
    churn_prob = {"free": 0.20, "starter": 0.12, "pro": 0.08, "business": 0.05}

    subs_rows = []
    inv_rows = []

    def month_floor(dt: datetime) -> datetime:
        return _utc(datetime(dt.year, dt.month, 1, 0, 0, 0))

    for i, row in users.iterrows():
        user_id = row["user_id"]
        signup = _utc(pd.to_datetime(row["signup_at"]).to_pydatetime())

        sub_id_1 = f"s_{user_id}_1"
        plan1 = initial_plan[i]
        # trial window (0-14 days)
        trial_days = int(rng.integers(0, 15))
        started_at = signup
        switch_at = signup + timedelta(days=trial_days + int(rng.integers(7, 45)))

        # churn window
        will_churn = rng.random() < churn_prob.get(plan1, 0.12)
        churn_at = signup + timedelta(days=int(rng.integers(30, 150))) if will_churn else None

        # subscription 1
        subs_rows.append({
            "subscription_id": sub_id_1,
            "user_id": user_id,
            "plan_id": plan1,
            "started_at": started_at,
            "ended_at": switch_at if upgraded_plan[i] != plan1 else churn_at,
            "status": "active" if churn_at is None else "canceled"
        })

        # subscription 2 if switched and not churned before switch
        if upgraded_plan[i] != plan1 and (churn_at is None or churn_at > switch_at):
            sub_id_2 = f"s_{user_id}_2"
            subs_rows.append({
                "subscription_id": sub_id_2,
                "user_id": user_id,
                "plan_id": upgraded_plan[i],
                "started_at": switch_at,
                "ended_at": churn_at,
                "status": "active" if churn_at is None else "canceled"
            })

        # Invoices: only for paid plans
        # We'll bill from month after signup, until churn (if any), with delays/backfills.
        plan_price = {"free": 0.0, "starter": 19.0, "pro": 49.0, "business": 99.0}
        first_bill_month = month_floor(signup + timedelta(days=30))
        end_bill = month_floor(churn_at) if churn_at else month_floor(_utc(datetime(2026, 2, 28)))

        current = first_bill_month
        inv_idx = 0

        while current <= end_bill:
            # Determine plan at this month based on switch date
            active_plan = plan1
            if upgraded_plan[i] != plan1 and current >= month_floor(switch_at):
                active_plan = upgraded_plan[i]

            amount = plan_price[active_plan]
            if amount > 0:
                period_start = current
                next_month = (period_start.replace(day=28) + timedelta(days=4)).replace(day=1)
                period_end = next_month

                # Delay: 20% invoices issued late 7-21 days after period end
                late = rng.random() < 0.20
                issued_at = period_end + timedelta(days=int(rng.integers(7, 22))) if late else period_end + timedelta(days=1)

                # Some paid_at missing (open invoices)
                is_paid = rng.random() < 0.92
                paid_at = issued_at + timedelta(days=int(rng.integers(0, 10))) if is_paid else None
                status = "paid" if is_paid else "open"

                inv_idx += 1
                inv_rows.append({
                    "invoice_id": f"inv_{user_id}_{inv_idx}",
                    "user_id": user_id,
                    "invoice_period_start": period_start,
                    "invoice_period_end": period_end,
                    "amount_usd": float(amount),
                    "issued_at": issued_at,
                    "paid_at": paid_at,
                    "status": status
                })

            current = (current.replace(day=28) + timedelta(days=4)).replace(day=1)

    fct_subs = pd.DataFrame(subs_rows)
    fct_invoices = pd.DataFrame(inv_rows)

    return {
        "fct_subscriptions": fct_subs,
        "fct_billing_invoices": fct_invoices
    }


def generate_raw_events(
    dim_users: pd.DataFrame,
    seed: int = 7,
    avg_days_observed: int = 70
) -> pd.DataFrame:
    """
    Generates raw events with intentional issues:
    - ~1% duplicate event_ids
    - ~1-2% missing event_time
    - ~5% session_id inconsistencies
    - rollout: team_collab exposure starts 2025-12-01 and ramps to 100% over 6 weeks
    """
    rng = np.random.default_rng(seed)
    random.seed(seed)

    users = dim_users.copy()
    users = users[~users["is_internal"]].reset_index(drop=True)

    launch = _utc(datetime(2025, 12, 1, 0, 0, 0))
    rollout_end = launch + timedelta(days=42)

    base_events = ["login", "page_view", "project_created", "message_sent", "file_uploaded"]
    device_types = ["desktop", "mobile", "tablet"]

    rows = []
    event_counter = 0

    for _, u in users.iterrows():
        user_id = u["user_id"]
        signup = _utc(pd.to_datetime(u["signup_at"]).to_pydatetime())

        # Baseline engagement propensity (drives selection bias)
        propensity = float(rng.beta(2, 5))  # mostly low, some high
        size = u["company_size"]
        channel = u["acquisition_channel"]

        # Larger + sales tends to be more active
        if size in ["201-1000", "1000+"]:
            propensity = min(1.0, propensity + 0.12)
        if channel == "sales":
            propensity = min(1.0, propensity + 0.08)

        days = int(rng.integers(14, avg_days_observed + 1))
        end_time = signup + timedelta(days=days)

        # Sessions per day depends on propensity
        sessions_per_day = 0.4 + 2.3 * propensity

        current_day = signup.date()
        while datetime(current_day.year, current_day.month, current_day.day, tzinfo=UTC) < end_time:
            num_sessions = int(rng.poisson(sessions_per_day))
            for s in range(num_sessions):
                session_start = _utc(datetime(current_day.year, current_day.month, current_day.day, tzinfo=UTC)) + timedelta(
                    minutes=int(rng.integers(0, 1440))
                )

                session_id = f"ses_{user_id}_{int(session_start.timestamp())}"

                # 5% chance session id gets inconsistent (split session)
                inconsistent = rng.random() < 0.05

                n_events = int(rng.integers(2, 10))
                for e in range(n_events):
                    event_counter += 1
                    event_time = session_start + timedelta(minutes=int(rng.integers(0, 45)), seconds=int(rng.integers(0, 60)))

                    event_name = random.choice(base_events)
                    device = random.choice(device_types)
                    country = u["country"]

                    sid = session_id
                    if inconsistent and e > 0 and rng.random() < 0.5:
                        sid = session_id + "_x"

                    props = {"screen": "app", "client_event_time": event_time.isoformat()}

                    # Feature rollout + exposure + usage
                    # Eligible probability ramps linearly: 20% -> 100% over 42 days after launch
                    if event_time >= launch:
                        if event_time >= rollout_end:
                            eligible_prob = 1.0
                        else:
                            eligible_prob = 0.20 + 0.80 * ((event_time - launch).total_seconds() / (rollout_end - launch).total_seconds())

                        eligible = rng.random() < eligible_prob

                        if eligible and rng.random() < (0.03 + 0.12 * propensity):
                            # exposure
                            event_counter += 1
                            rows.append({
                                "event_id": f"e_{event_counter:09d}",
                                "user_id": user_id,
                                "event_name": "feature_exposed",
                                "event_time": event_time + timedelta(seconds=1),
                                "feature_name": "team_collab",
                                "session_id": sid,
                                "device_type": device,
                                "country": country,
                                "properties_json": json.dumps({"entry_point": "sidebar", "client_event_time": (event_time + timedelta(seconds=1)).isoformat()})
                            })

                            # viewed
                            if rng.random() < 0.70:
                                event_counter += 1
                                rows.append({
                                    "event_id": f"e_{event_counter:09d}",
                                    "user_id": user_id,
                                    "event_name": "feature_viewed",
                                    "event_time": event_time + timedelta(seconds=5),
                                    "feature_name": "team_collab",
                                    "session_id": sid,
                                    "device_type": device,
                                    "country": country,
                                    "properties_json": json.dumps({"client_event_time": (event_time + timedelta(seconds=5)).isoformat()})
                                })

                            # used (counts as adoption)
                            if rng.random() < (0.22 + 0.45 * propensity):
                                event_counter += 1
                                rows.append({
                                    "event_id": f"e_{event_counter:09d}",
                                    "user_id": user_id,
                                    "event_name": "feature_used",
                                    "event_time": event_time + timedelta(seconds=20),
                                    "feature_name": "team_collab",
                                    "session_id": sid,
                                    "device_type": device,
                                    "country": country,
                                    "properties_json": json.dumps({"action": "collab_start", "client_event_time": (event_time + timedelta(seconds=20)).isoformat()})
                                })

                    rows.append({
                        "event_id": f"e_{event_counter:09d}",
                        "user_id": user_id,
                        "event_name": event_name,
                        "event_time": event_time,
                        "feature_name": None,
                        "session_id": sid,
                        "device_type": device,
                        "country": country,
                        "properties_json": json.dumps(props)
                    })

            current_day = (datetime(current_day.year, current_day.month, current_day.day) + timedelta(days=1)).date()

    raw_events = pd.DataFrame(rows)

    # Inject data issues:
    # 1.2% missing event_time
    missing_mask = rng.random(len(raw_events)) < 0.012
    raw_events.loc[missing_mask, "event_time"] = pd.NaT

    # 1% duplicate event_id (copy some IDs onto other rows)
    dup_mask = rng.random(len(raw_events)) < 0.010
    dup_source = raw_events.loc[dup_mask, "event_id"].sample(frac=1.0, random_state=seed).values
    if len(dup_source) > 0:
        raw_events.loc[raw_events.index[:len(dup_source)], "event_id"] = dup_source

    # 2% invalid device_type/country
    bad_mask = rng.random(len(raw_events)) < 0.02
    raw_events.loc[bad_mask, "device_type"] = "unkn0wn"
    raw_events.loc[bad_mask, "country"] = "XX"

    return raw_events


def write_csvs(dims: Dict[str, pd.DataFrame], facts: Dict[str, pd.DataFrame], raw_events: pd.DataFrame) -> None:
    _ensure_dirs()

    dims["dim_users"].to_csv("data/raw/dim_users.csv", index=False)
    dims["dim_plans"].to_csv("data/raw/dim_plans.csv", index=False)
    dims["dim_features"].to_csv("data/raw/dim_features.csv", index=False)

    facts["fct_subscriptions"].to_csv("data/raw/fct_subscriptions.csv", index=False)
    facts["fct_billing_invoices"].to_csv("data/raw/fct_billing_invoices.csv", index=False)

    raw_events.to_csv("data/raw/raw_events.csv", index=False)


def copy_csv(conn, table: str, csv_path: str) -> None:
    df = pd.read_csv(csv_path)
    columns = list(df.columns)
    column_list = ", ".join(columns)

    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table} CASCADE;")
        with open(csv_path, "r", encoding="utf-8") as f:
            cur.copy_expert(
                f"COPY {table} ({column_list}) FROM STDIN WITH (FORMAT csv, HEADER true)",
                f
            )

def load_into_postgres(db: DbConfig) -> None:
    conn = psycopg2.connect(
        host=db.host,
        port=db.port,
        dbname=db.dbname,
        user=db.user,
        password=db.password,
    )
    conn.autocommit = True
    try:
        copy_csv(conn, "core.dim_users", "data/raw/dim_users.csv")
        copy_csv(conn, "core.dim_plans", "data/raw/dim_plans.csv")
        copy_csv(conn, "core.dim_features", "data/raw/dim_features.csv")
        copy_csv(conn, "core.fct_subscriptions", "data/raw/fct_subscriptions.csv")
        copy_csv(conn, "core.fct_billing_invoices", "data/raw/fct_billing_invoices.csv")
        copy_csv(conn, "raw.raw_events", "data/raw/raw_events.csv")
    finally:
        conn.close()


def main() -> None:
    os.chdir(os.path.join(os.path.dirname(__file__), ".."))

    dims = generate_dims(n_users=30000)
    facts = generate_subscriptions_and_invoices(dims["dim_users"])
    raw_events = generate_raw_events(dims["dim_users"])

    write_csvs(dims, facts, raw_events)
    load_into_postgres(DbConfig())

    print("Done.")
    print("Rows loaded:")
    print(" - core.dim_users:", len(dims["dim_users"]))
    print(" - core.fct_subscriptions:", len(facts["fct_subscriptions"]))
    print(" - core.fct_billing_invoices:", len(facts["fct_billing_invoices"]))
    print(" - raw.raw_events:", len(raw_events))


if __name__ == "__main__":
    main()
