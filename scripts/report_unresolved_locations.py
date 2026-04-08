from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from ove_scraper.csv_transform import load_csv_rows, parse_state
from ove_scraper.location_zip_lookup import resolve_location_zip


EXPORT_GLOB = "exports/*.csv"
OUTPUT_PATH = Path("artifacts") / "unresolved_pickup_locations.csv"


def main() -> None:
    counts: Counter[tuple[str, str, str]] = Counter()

    for path in sorted(Path("exports").glob("*.csv")):
        rows = load_csv_rows(path)
        for row in rows:
            pickup_location = (row.get("Pickup Location") or "").strip()
            auction_house = (row.get("Auction House") or "").strip()
            state = parse_state(pickup_location)
            if resolve_location_zip(pickup_location, auction_house, state):
                continue
            key = (pickup_location, auction_house, state or "")
            counts[key] += 1

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["count", "pickup_location", "auction_house", "state"])
        for (pickup_location, auction_house, state), count in counts.most_common():
            writer.writerow([count, pickup_location, auction_house, state])

    print(OUTPUT_PATH)


if __name__ == "__main__":
    main()
