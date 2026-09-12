"""The generator writes the documented dataset, and the documented figures follow from it."""
from decimal import ROUND_HALF_UP, Decimal

import pandas as pd
import pytest

import figures
from generate_data import build_tables


def spark_round(value: float, places: int) -> float:
    """Round as Spark's ROUND does for a double: half up, from the shortest decimal form."""
    exact = Decimal(repr(float(value)))
    return float(exact.quantize(Decimal(1).scaleb(-places), ROUND_HALF_UP))


@pytest.fixture(scope="module")
def tables() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    users, sessions, orders = build_tables()
    return users.to_pandas(), sessions.to_pandas(), orders.to_pandas()


def test_row_counts(tables):
    users, sessions, orders = tables
    assert (len(users), len(sessions), len(orders)) == (
        figures.USERS, figures.SESSIONS, figures.ORDERS
    )


def test_columns(tables):
    users, sessions, orders = tables
    assert list(users.columns) == ["user_id", "home_region"]
    assert list(sessions.columns) == [
        "session_id", "user_id", "region", "session_date", "converted"
    ]
    assert list(orders.columns) == [
        "order_id", "session_id", "user_id", "region", "order_date", "order_total"
    ]


def test_every_run_builds_the_same_rows():
    first, second = build_tables(), build_tables()
    assert all(a.equals(b) for a, b in zip(first, second, strict=True))


def test_regions(tables):
    sessions = tables[1]
    grouped = sessions.groupby("region").agg(
        sessions=("session_id", "size"),
        converted=("converted", "sum"),
        users=("user_id", "nunique"),
    )
    for region, (count, converted, rate, users) in figures.REGIONS.items():
        row = grouped.loc[region]
        assert (row.sessions, row.converted, row.users) == (count, converted, users)
        assert spark_round(row.converted / row.sessions, 4) == rate


def test_average_of_ratios(tables):
    sessions = tables[1]
    average = float(sessions.groupby("region")["converted"].mean().mean())
    overall = float(sessions["converted"].mean())
    assert spark_round(average, 4) == figures.AVERAGE_OF_REGION_RATES
    assert spark_round(overall, 4) == figures.CONVERSION_RATE
    assert spark_round(average / overall, 1) == figures.RATE_RATIO


def test_summing_distinct_counts(tables):
    sessions = tables[1]
    daily = sessions.groupby("session_date")["user_id"].nunique().sort_index()
    monthly = sessions["user_id"].nunique()
    rates = sessions.groupby("session_date")["converted"].mean().sort_index()
    assert tuple(daily) == figures.DAILY_ACTIVE_USERS
    assert tuple(spark_round(rate, 4) for rate in rates.iloc[:5]) == (
        figures.FIRST_DAYS_CONVERSION_RATES
    )
    assert daily.sum() == figures.SUM_OF_DAILY_ACTIVE_USERS
    assert monthly == figures.MONTHLY_ACTIVE_USERS
    assert spark_round(daily.sum() / monthly, 1) == figures.ACTIVE_USERS_RATIO


def test_modeled_source(tables):
    _, sessions, orders = tables
    facts = sessions.merge(orders[["session_id", "order_id", "order_total"]], "left", "session_id")
    facts = facts[facts["region"] != "remote"]
    # Spark's dayofweek numbers Sunday 1 and Saturday 7; pandas numbers Saturday 5 and Sunday 6.
    weekend = pd.to_datetime(facts["session_date"]).dt.dayofweek >= 5
    core = facts["region"].isin(["metro", "urban"])
    facts = facts.assign(
        day_type=weekend.map({True: "weekend", False: "weekday"}),
        region_tier=core.map({True: "core", False: "long_tail"}),
    )

    def measures(frame: pd.DataFrame) -> tuple[int, float, int, float, float]:
        placed = frame.loc[frame["order_id"].notna()]
        revenue = float(placed["order_total"].sum())
        paying = int(placed["user_id"].nunique())
        rate = float(frame["converted"].mean())
        return len(frame), rate, paying, revenue / len(placed), revenue / paying

    placed = facts.loc[facts["order_id"].notna()]
    assert len(placed) == figures.MODELED_ORDERS
    assert spark_round(float(placed["order_total"].sum()), 2) == figures.MODELED_REVENUE
    _, _, _, aov, arppu = measures(facts)
    assert (spark_round(aov, 2), spark_round(arppu, 2)) == (
        figures.MODELED_AOV, figures.MODELED_ARPPU
    )

    slices = {key: measures(group) for key, group in facts.groupby(["region_tier", "day_type"])}
    assert set(slices) == set(figures.SLICES)
    for key, (count, rate, paying, aov, arppu) in figures.SLICES.items():
        m_count, m_rate, m_paying, m_aov, m_arppu = slices[key]
        assert (m_count, m_paying) == (count, paying)
        assert spark_round(m_rate, 4) == rate
        assert (spark_round(m_aov, 2), spark_round(m_arppu, 2)) == (aov, arppu)
    average_aov = sum(s[3] for s in slices.values()) / len(slices)
    average_arppu = sum(s[4] for s in slices.values()) / len(slices)
    assert spark_round(average_aov, 2) == figures.AVERAGE_OF_SLICE_AOV
    assert spark_round(average_arppu, 2) == figures.AVERAGE_OF_SLICE_ARPPU
