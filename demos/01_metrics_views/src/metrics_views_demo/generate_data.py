"""Generate the synthetic food-delivery dataset for the Metric Views demo.

Pure Python + numpy -> Parquet (via pyarrow). No Spark needed to generate, so it's
instant to iterate on locally in VSCode/PyCharm. Domain matches the lakehouse-stack /
SDP order schema (regions, orders, delivery) so it stays consistent with the rest of the stack.

The data is *deliberately engineered* to expose two classic semantic-layer footguns:

  1. RATIO OF RATIOS (conversion rate)
     Region sizes are wildly skewed and small regions have high conversion rates. So
     AVG(per-region conversion rate) massively overstates the true blended rate
     (SUM(converted) / COUNT(sessions)).

  2. DISTINCT-COUNT DOUBLE COUNTING (active users)
     Users return on multiple days within the month. So SUM(daily distinct users)
     multiplies each returning user, while COUNT(DISTINCT user_id) at the month grain
     is the truth.

Run:
    python -m metrics_views_demo.generate_data
"""

from __future__ import annotations

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .config import DATA_DIR

SEED = 42
MONTH_DAYS = 30  # single month: 2026-06-01 .. 2026-06-30
N_USERS = 10_000

# region -> (n_sessions, conversion_rate). Small regions have HIGH rates on purpose:
# a naive AVG of these rates is dominated by the tiny 'remote'/'rural' regions.
REGIONS = {
    "metro":    (50_000, 0.08),
    "urban":    (20_000, 0.10),
    "suburban": (8_000,  0.12),
    "rural":    (800,    0.35),
    "remote":   (200,    0.45),
}

AVG_ORDER_TOTAL = 28.0  # dollars; lognormal spread


def _rng() -> np.random.Generator:
    return np.random.default_rng(SEED)


def build_tables():
    rng = _rng()

    # --- users dimension -----------------------------------------------------
    user_ids = np.array([f"user_{i:05d}" for i in range(N_USERS)])
    # each user's home region, weighted by region size
    region_names = np.array(list(REGIONS.keys()))
    region_sizes = np.array([REGIONS[r][0] for r in region_names], dtype=float)
    user_region = rng.choice(region_names, size=N_USERS, p=region_sizes / region_sizes.sum())

    users_tbl = pa.table({"user_id": user_ids, "home_region": user_region})

    # --- sessions fact -------------------------------------------------------
    # Build per-region session blocks so counts + conversion rates are exact-ish.
    s_user, s_region, s_day, s_converted = [], [], [], []
    for region, (n_sessions, conv_rate) in REGIONS.items():
        pool = user_ids[user_region == region]
        if len(pool) == 0:  # tiny regions may draw no home users; fall back to all
            pool = user_ids
        # returning behaviour: each session picks a user from the region pool, so
        # the same user recurs across days -> distinct-count double counting.
        s_user.append(rng.choice(pool, size=n_sessions))
        s_region.append(np.full(n_sessions, region))
        s_day.append(rng.integers(1, MONTH_DAYS + 1, size=n_sessions))
        s_converted.append((rng.random(n_sessions) < conv_rate).astype(bool))

    session_user = np.concatenate(s_user)
    session_region = np.concatenate(s_region)
    session_day = np.concatenate(s_day)
    session_converted = np.concatenate(s_converted)
    n_total = session_user.shape[0]

    order_ts = np.array([f"2026-06-{d:02d}" for d in session_day])
    session_ids = np.array([f"sess_{i:07d}" for i in range(n_total)])

    sessions_tbl = pa.table(
        {
            "session_id": session_ids,
            "user_id": session_user,
            "region": session_region,
            "session_date": order_ts,
            "converted": session_converted,
        }
    )

    # --- orders fact (one per converted session) -----------------------------
    conv_idx = np.flatnonzero(session_converted)
    order_totals = np.round(rng.lognormal(mean=np.log(AVG_ORDER_TOTAL), sigma=0.5, size=conv_idx.size), 2)
    orders_tbl = pa.table(
        {
            "order_id": np.array([f"ord_{i:07d}" for i in range(conv_idx.size)]),
            "session_id": session_ids[conv_idx],
            "user_id": session_user[conv_idx],
            "region": session_region[conv_idx],
            "order_date": order_ts[conv_idx],
            "order_total": order_totals,
        }
    )

    return users_tbl, sessions_tbl, orders_tbl


def _preview_footgun(sessions_tbl) -> None:
    """Print the punchline numbers so you know the data lands before touching Spark."""
    import pyarrow.compute as pc

    region = sessions_tbl.column("region")
    converted = pc.cast(sessions_tbl.column("converted"), pa.int64())

    per_region = {}
    for r in REGIONS:
        mask = pc.equal(region, r)
        n = pc.sum(pc.cast(mask, pa.int64())).as_py()
        c = pc.sum(pc.filter(converted, mask)).as_py()
        per_region[r] = (n, c, c / n if n else 0.0)

    naive_avg = np.mean([v[2] for v in per_region.values()])
    total_n = sum(v[0] for v in per_region.values())
    total_c = sum(v[1] for v in per_region.values())
    true_rate = total_c / total_n

    print("\n  Conversion-rate footgun preview")
    print("  region      sessions  converted   rate")
    for r, (n, c, rate) in per_region.items():
        print(f"    {r:<9} {n:>9,} {c:>10,}  {rate:6.1%}")
    print(f"\n    NAIVE  AVG(region rate) = {naive_avg:6.1%}   <- the lie")
    print(f"    TRUE   SUM(conv)/SUM(sess) = {true_rate:6.1%}   <- MEASURE()")
    print(f"    overstatement: {naive_avg / true_rate:.1f}x\n")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    users_tbl, sessions_tbl, orders_tbl = build_tables()

    pq.write_table(users_tbl, DATA_DIR / "users.parquet")
    pq.write_table(sessions_tbl, DATA_DIR / "sessions.parquet")
    pq.write_table(orders_tbl, DATA_DIR / "orders.parquet")

    print(f"Wrote parquet to {DATA_DIR}")
    print(f"  users:    {users_tbl.num_rows:>8,} rows")
    print(f"  sessions: {sessions_tbl.num_rows:>8,} rows")
    print(f"  orders:   {orders_tbl.num_rows:>8,} rows")
    _preview_footgun(sessions_tbl)


if __name__ == "__main__":
    main()
