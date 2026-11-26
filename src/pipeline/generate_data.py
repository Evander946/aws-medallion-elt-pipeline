"""Generate a small synthetic e-commerce dataset with deliberately dirty records.

Everything here is fake data created by a seeded random generator. It exists only to exercise the
pipeline: duplicates, late updates, bad dates, negative amounts, mixed-case statuses and null keys.
"""
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
COUNTRIES = ["IN", "US", "GB", "DE", "AE", "SG"]


def generate(out_dir: Path, n_customers: int = 200, n_orders: int = 2000, seed: int = 42) -> None:
    rnd = random.Random(seed)
    out_dir.mkdir(parents=True, exist_ok=True)
    base = datetime(2026, 1, 1)

    with open(out_dir / "customers.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["customer_id", "name", "email", "country", "signup_date"])
        for i in range(1, n_customers + 1):
            w.writerow([
                f"C{i:05d}", f"Customer {i}", f"customer{i}@example.com",
                rnd.choice(COUNTRIES + [c.lower() for c in COUNTRIES[:2]]),  # mixed case on purpose
                (base + timedelta(days=rnd.randint(0, 300))).strftime("%Y-%m-%d"),
            ])

    rows = []
    for i in range(1, n_orders + 1):
        placed = base + timedelta(days=rnd.randint(0, 330), hours=rnd.randint(0, 23))
        updated = placed + timedelta(hours=rnd.randint(0, 120))
        rows.append([
            f"O{i:06d}", f"C{rnd.randint(1, n_customers):05d}",
            rnd.choice(STATUSES), round(rnd.uniform(5, 500), 2), rnd.randint(1, 6),
            placed.strftime("%Y-%m-%d %H:%M:%S"), updated.strftime("%Y-%m-%d %H:%M:%S"),
        ])

    # --- inject data quality problems -----------------------------------------------------------
    for r in rnd.sample(rows, 60):                      # late-arriving update -> duplicate order_id
        later = datetime.strptime(r[6], "%Y-%m-%d %H:%M:%S") + timedelta(days=2)
        rows.append([r[0], r[1], "delivered", r[3], r[4], r[5], later.strftime("%Y-%m-%d %H:%M:%S")])
    for r in rnd.sample(rows, 25): r[3] = -abs(r[3])      # negative amount
    for r in rnd.sample(rows, 20): r[5] = "not-a-date"    # unparseable timestamp
    for r in rnd.sample(rows, 15): r[1] = ""              # missing customer key
    for r in rnd.sample(rows, 40): r[2] = r[2].upper()    # inconsistent status casing
    rnd.shuffle(rows)

    with open(out_dir / "orders.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id", "customer_id", "status", "amount", "quantity", "order_ts", "updated_at"])
        w.writerows(rows)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    args = ap.parse_args()
    generate(Path(args.out))
    print(f"Wrote synthetic data to {args.out}")
