"""Generate the pipeline's input files (order events, stores and brands) deterministically.

    python pipeline/input/generate.py
"""
import csv
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
STORES = [
    (1, "Toronto", 43.6532, -79.3832),
    (2, "Montreal", 45.5019, -73.5674),
    (3, "Vancouver", 49.2827, -123.1207),
    (4, "Calgary", 51.0447, -114.0719),
]
BRANDS = [
    (1, "Night Noodle"),
    (2, "Green Bowl"),
    (3, "Taco Lab"),
    (4, "Pizza Yard"),
    (5, "Sushi Box"),
]
ORDERS = 60
UNDELIVERED = 5
INCOMPLETE_EVENTS = 3


def main() -> None:
    rng = random.Random(42)
    start = datetime(2026, 9, 1, 11, 0)
    events = []

    for n in range(1, ORDERS + 1):
        store_id, _, lat, lng = rng.choice(STORES)
        body = {
            "brand_id": rng.choice(BRANDS)[0],
            "total": round(rng.uniform(12, 80), 2),
            "lat": round(lat + rng.uniform(-0.05, 0.05), 5),
            "lng": round(lng + rng.uniform(-0.05, 0.05), 5),
        }
        created = start + timedelta(minutes=rng.randint(0, 600))
        ready = created + timedelta(minutes=rng.randint(8, 25))
        picked_up = ready + timedelta(minutes=rng.randint(2, 10))
        delivered = picked_up + timedelta(minutes=rng.randint(10, 35))
        steps = [
            ("order_created", created),
            ("order_ready", ready),
            ("driver_picked_up", picked_up),
        ]
        if n <= ORDERS - UNDELIVERED:
            steps.append(("delivered", delivered))
        for event_type, ts in steps:
            events.append((ts, event_type, f"o{n:03d}", store_id, body))

    for _ in range(INCOMPLETE_EVENTS):
        ts = start + timedelta(minutes=rng.randint(0, 600))
        events.append((ts, "order_created", None, 1, {}))

    events.sort(key=lambda e: (e[0], e[1], e[2] or ""))
    with open(HERE / "order_events.jsonl", "w") as out:
        for i, (ts, event_type, order_id, store_id, body) in enumerate(events, start=1):
            record = {
                "event_id": f"e{i:04d}",
                "event_type": event_type,
                "ts": ts.isoformat(),
                "order_id": order_id,
                "store_id": store_id,
                "body": json.dumps(body, sort_keys=True),
            }
            out.write(json.dumps(record) + "\n")

    with open(HERE / "stores.csv", "w", newline="") as out:
        csv.writer(out).writerows([("id", "city", "lat", "lng"), *STORES])
    with open(HERE / "brands.csv", "w", newline="") as out:
        csv.writer(out).writerows([("id", "name"), *BRANDS])
    print(f"wrote {len(events)} events, {len(STORES)} stores, {len(BRANDS)} brands")


if __name__ == "__main__":
    main()
