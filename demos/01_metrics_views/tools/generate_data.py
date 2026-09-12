"""Generate the demonstration dataset: one month of users, sessions and orders for food delivery.

    python tools/generate_data.py            # or: make data

Writes three Parquet files to data/ with numpy and pyarrow; Spark is not needed. The seed is fixed,
so every run writes the same rows (blog post §2).

Two properties of the data make aggregation errors visible:

- Region sizes differ by a factor of 250, and the smaller regions convert at higher rates, so an
  unweighted average of the per-region conversion rates is far from the overall rate.
- Each session draws its user from the region's residents, so users return on several days, and
  the sum of daily distinct users exceeds the monthly distinct count.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
SEED = 42
MONTH_DAYS = 30  # June 2026
N_USERS = 10_000
# region: (sessions, conversion probability)
REGIONS = {
    "metro": (50_000, 0.08),
    "urban": (20_000, 0.10),
    "suburban": (8_000, 0.12),
    "rural": (800, 0.35),
    "remote": (200, 0.45),
}
AVG_ORDER_TOTAL = 28.0  # dollars; order totals are lognormal around this value


def build_tables() -> tuple[pa.Table, pa.Table, pa.Table]:
    """Return the users, sessions and orders tables. The order of random draws fixes every row."""
    rng = np.random.default_rng(SEED)

    # Users, each with a home region drawn in proportion to the region's sessions.
    user_ids = np.array([f"user_{i:05d}" for i in range(N_USERS)])
    region_names = np.array(list(REGIONS))
    region_sizes = np.array([REGIONS[r][0] for r in region_names], dtype=float)
    user_region = rng.choice(region_names, size=N_USERS, p=region_sizes / region_sizes.sum())
    users = pa.table({"user_id": user_ids, "home_region": user_region})

    # Sessions, generated region by region.
    s_user, s_region, s_day, s_converted = [], [], [], []
    for region, (n_sessions, conversion) in REGIONS.items():
        pool = user_ids[user_region == region]
        if len(pool) == 0:  # a small region can draw no residents; use every user instead
            pool = user_ids
        s_user.append(rng.choice(pool, size=n_sessions))
        s_region.append(np.full(n_sessions, region))
        s_day.append(rng.integers(1, MONTH_DAYS + 1, size=n_sessions))
        s_converted.append((rng.random(n_sessions) < conversion).astype(bool))

    session_user = np.concatenate(s_user)
    session_region = np.concatenate(s_region)
    session_converted = np.concatenate(s_converted)
    session_date = np.array([f"2026-06-{day:02d}" for day in np.concatenate(s_day)])
    session_ids = np.array([f"sess_{i:07d}" for i in range(session_user.shape[0])])
    sessions = pa.table(
        {
            "session_id": session_ids,
            "user_id": session_user,
            "region": session_region,
            "session_date": session_date,
            "converted": session_converted,
        }
    )

    # Orders, one for each converted session.
    converted = np.flatnonzero(session_converted)
    totals = rng.lognormal(mean=np.log(AVG_ORDER_TOTAL), sigma=0.5, size=converted.size)
    orders = pa.table(
        {
            "order_id": np.array([f"ord_{i:07d}" for i in range(converted.size)]),
            "session_id": session_ids[converted],
            "user_id": session_user[converted],
            "region": session_region[converted],
            "order_date": session_date[converted],
            "order_total": np.round(totals, 2),
        }
    )
    return users, sessions, orders


def summarize(sessions: pa.Table) -> None:
    """Print each region's conversion rate and the two ways of combining the rates."""
    region = sessions.column("region").to_numpy()
    converted = sessions.column("converted").to_numpy()
    rates: list[float] = []
    total_sessions = total_converted = 0
    print(f"  {'region':<10}{'sessions':>10}{'converted':>11}{'rate':>8}")
    for name in REGIONS:
        mask = region == name
        n = int(mask.sum())
        c = int(converted[mask].sum())
        rates.append(c / n)
        total_sessions += n
        total_converted += c
        print(f"  {name:<10}{n:>10,}{c:>11,}{c / n:>8.1%}")
    print(f"  unweighted average of the region rates   {sum(rates) / len(rates):.1%}")
    print(f"  converted sessions / all sessions        {total_converted / total_sessions:.1%}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate the demonstration dataset.")
    parser.add_argument("--out", type=Path, default=ROOT / "data")
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    tables = dict(zip(("users", "sessions", "orders"), build_tables(), strict=True))
    out = args.out.resolve()
    shown = out.relative_to(ROOT) if out.is_relative_to(ROOT) else out
    print(f"wrote {shown} (seed {SEED})")
    for name, table in tables.items():
        pq.write_table(table, args.out / f"{name}.parquet")
        print(f"  {name:<10}{table.num_rows:>10,} rows")
    print()
    summarize(tables["sessions"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
