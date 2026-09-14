"""Figures measured on Apache Spark 4.2.0 with the generated dataset.

The tests check the generator, Spark Classic and Spark Connect against these values, and
tools/check.py checks that the documents quote them. Rates are rounded to four places and money to
two, as the examples round them.
"""

USERS = 10_000
SESSIONS = 79_000
ORDERS = 7_295

# region: (sessions, converted sessions, conversion rate, distinct users)
REGIONS = {
    "metro": (50_000, 3_937, 0.0787, 6_370),
    "urban": (20_000, 1_979, 0.0990, 2_519),
    "suburban": (8_000, 974, 0.1218, 974),
    "rural": (800, 302, 0.3775, 99),
    "remote": (200, 103, 0.5150, 32),
}

# examples/01_average_of_ratios.py
AVERAGE_OF_REGION_RATES = 0.2384
CONVERSION_RATE = 0.0923
RATE_RATIO = 2.6

# examples/02_summing_distinct_counts.py, and Figure 3-2 of the blog post: June 1 to June 30
DAILY_ACTIVE_USERS = (
    2_301, 2_328, 2_308, 2_340, 2_286, 2_350, 2_243, 2_329, 2_332, 2_233,
    2_331, 2_334, 2_375, 2_244, 2_256, 2_340, 2_225, 2_292, 2_303, 2_335,
    2_314, 2_370, 2_340, 2_423, 2_319, 2_313, 2_288, 2_360, 2_295, 2_313,
)
# examples/04_measure_at_every_grain.py, June 1 to June 5
FIRST_DAYS_CONVERSION_RATES = (0.0952, 0.0889, 0.0893, 0.0930, 0.0890)
SUM_OF_DAILY_ACTIVE_USERS = 69_420
MONTHLY_ACTIVE_USERS = 9_994
ACTIVE_USERS_RATIO = 6.9

# examples/08_modeled_source.py, whose filter excludes the remote region
MODELED_ORDERS = 7_192
MODELED_REVENUE = 230_267.73
MODELED_AOV = 32.02
MODELED_ARPPU = 46.10
# (region_tier, day_type): (sessions, conversion rate, paying users, aov, arppu)
SLICES = {
    ("core", "weekday"): (51_381, 0.0849, 3_420, 32.07, 40.90),
    ("core", "weekend"): (18_619, 0.0835, 1_427, 31.52, 34.35),
    ("long_tail", "weekday"): (6_486, 0.1380, 578, 32.09, 49.69),
    ("long_tail", "weekend"): (2_314, 0.1646, 310, 33.24, 40.85),
}
AVERAGE_OF_SLICE_AOV = 32.23
AVERAGE_OF_SLICE_ARPPU = 41.45
