"""Tests for pipeline/lib/geo.py, the module the pipeline's UDF imports on the executors."""
from lib.geo import haversine_km


def test_distance_to_the_same_point_is_zero():
    assert haversine_km(43.65, -79.38, 43.65, -79.38) == 0.0


def test_one_degree_of_latitude_is_about_111_km():
    assert round(haversine_km(0.0, 0.0, 1.0, 0.0), 1) == 111.2


def test_toronto_to_montreal():
    assert 500 < haversine_km(43.6532, -79.3832, 45.5019, -73.5674) < 510


def test_distance_is_symmetric():
    a = haversine_km(43.65, -79.38, 49.28, -123.12)
    b = haversine_km(49.28, -123.12, 43.65, -79.38)
    assert a == b
