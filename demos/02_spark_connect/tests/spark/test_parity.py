"""Tests for pipeline/parity.py. Each failure the comparison must detect is constructed directly."""
from parity import compare, compare_frames, content_digest

ROWS = [(1, "metro", 24.0), (2, "urban", 18.0), (3, "rural", 52.0)]
SCHEMA = "id int, region string, total double"


def test_digest_does_not_depend_on_row_order(spark):
    forward = spark.createDataFrame(ROWS, SCHEMA)
    backward = spark.createDataFrame(list(reversed(ROWS)), SCHEMA)
    assert content_digest(forward) == content_digest(backward)


def test_digest_does_not_depend_on_partitioning(spark):
    one = spark.createDataFrame(ROWS, SCHEMA).repartition(1)
    three = spark.createDataFrame(ROWS, SCHEMA).repartition(3)
    assert content_digest(one) == content_digest(three)


def test_digest_distinguishes_field_boundaries(spark):
    left = spark.createDataFrame([("a", "bc")], "x string, y string")
    right = spark.createDataFrame([("ab", "c")], "x string, y string")
    assert content_digest(left) != content_digest(right)


def test_digest_distinguishes_null_from_an_empty_string(spark):
    left = spark.createDataFrame([(1, None)], "id int, v string")
    right = spark.createDataFrame([(1, "")], "id int, v string")
    assert content_digest(left) != content_digest(right)


def test_empty_tables_compare_equal(spark):
    left = spark.createDataFrame([], SCHEMA)
    right = spark.createDataFrame([], SCHEMA)
    assert content_digest(left) == content_digest(right)


def test_identical_tables_pass(spark):
    result = compare_frames(
        "t", spark.createDataFrame(ROWS, SCHEMA), spark.createDataFrame(ROWS, SCHEMA)
    )
    assert result.ok, result.verdict()
    assert result.left_rows == 3


def test_different_row_counts_are_reported(spark):
    result = compare_frames(
        "t", spark.createDataFrame(ROWS, SCHEMA), spark.createDataFrame(ROWS[:2], SCHEMA)
    )
    assert result.verdict().startswith("ROWS")


def test_same_row_count_with_different_values_is_reported(spark):
    changed = [(1, "metro", 24.0), (2, "urban", 18.0), (3, "rural", 52.5)]
    result = compare_frames(
        "t", spark.createDataFrame(ROWS, SCHEMA), spark.createDataFrame(changed, SCHEMA)
    )
    assert result.left_rows == result.right_rows
    assert result.verdict().startswith("CONTENT")


def test_different_schemas_are_reported(spark):
    result = compare_frames(
        "t",
        spark.createDataFrame(ROWS, SCHEMA),
        spark.createDataFrame([(1, "metro", 24)], "id int, region string, total int"),
    )
    assert result.verdict().startswith("SCHEMA")


def test_a_missing_table_is_reported_as_an_error(spark):
    (result,) = compare(spark, "/no/such/left", "/no/such/right", ("t",))
    assert not result.ok
    assert result.verdict().startswith("ERROR")
