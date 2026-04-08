from __future__ import annotations

import argparse
import concurrent.futures
import csv
import json
import random
import time
from pathlib import Path

from ove_scraper.browser import BrowserSessionError, ListingNotFoundError
from ove_scraper.cdp_browser import PlaywrightCdpBrowserSession
from ove_scraper.config import Settings, load_env_file


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Benchmark concurrent OVE VIN scrapes")
    parser.add_argument("--count", type=int, default=5)
    parser.add_argument("--sample-size", type=int, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", default="artifacts/benchmarks/concurrent-scrape.json")
    parser.add_argument("--vin", action="append", dest="vins", default=[])
    return parser


def main() -> None:
    load_env_file()
    args = build_parser().parse_args()
    settings = Settings.from_env()

    explicit_vins = [vin.strip().upper() for vin in args.vins if vin and vin.strip()]
    candidates = load_candidate_vins(settings.export_dir) if not explicit_vins else explicit_vins
    if not candidates:
        raise SystemExit("No candidate VINs found in exports directory")

    if explicit_vins:
        requested_workers = len(explicit_vins)
        live_vins = find_live_vins(settings, explicit_vins, requested_workers)
        if len(live_vins) < requested_workers:
            raise SystemExit(
                f"Only found {len(live_vins)} live VINs from explicit list; needed {requested_workers}"
            )
    else:
        random.seed(args.seed)
        random.shuffle(candidates)
        sample = candidates[: max(args.count, args.sample_size)]
        live_vins = find_live_vins(settings, sample, args.count)
        if len(live_vins) < args.count:
            raise SystemExit(f"Only found {len(live_vins)} live VINs from current exports; needed {args.count}")
        requested_workers = args.count

    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=requested_workers, thread_name_prefix="benchmark") as executor:
        futures = [executor.submit(scrape_one, settings, vin) for vin in live_vins]
        results = [future.result() for future in concurrent.futures.as_completed(futures)]
    total_seconds = time.perf_counter() - started

    summary = {
        "requested_workers": requested_workers,
        "configured_workers": settings.deep_scrape_max_workers,
        "candidate_pool": len(candidates),
        "live_vins": live_vins,
        "total_seconds": round(total_seconds, 2),
        "successful": sum(1 for item in results if item["success"]),
        "failed": sum(1 for item in results if not item["success"]),
        "results": sorted(results, key=lambda item: item["vin"]),
    }

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


def load_candidate_vins(export_dir: Path) -> list[str]:
    vins: list[str] = []
    for path in sorted(export_dir.glob("*.csv")):
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                vin = (row.get("Vin") or row.get("VIN") or "").strip()
                if len(vin) == 17:
                    vins.append(vin)
    return sorted(set(vins))


def find_live_vins(settings: Settings, candidates: list[str], limit: int) -> list[str]:
    browser = PlaywrightCdpBrowserSession(settings)
    try:
        browser.ensure_session()
        page = browser._get_ove_page(browser._connect_browser().contexts)
        page = browser._prepare_vin_search_page(page)
        live: list[str] = []
        for vin in candidates:
            if len(live) >= limit:
                break
            try:
                search = browser._find_vin_search_input(page)
                search.click(timeout=5000)
                search.fill(vin)
                search.press("Enter")
                page.wait_for_timeout(2000)
                browser._find_listing_for_vin(page, vin)
                live.append(vin)
            except ListingNotFoundError:
                continue
            except BrowserSessionError:
                raise
            except Exception:
                continue
        return live
    finally:
        browser.close()


def scrape_one(settings: Settings, vin: str) -> dict[str, object]:
    browser = PlaywrightCdpBrowserSession(settings)
    started = time.perf_counter()
    try:
        detail = browser.deep_scrape_vin(vin)
        return {
            "vin": vin,
            "success": True,
            "seconds": round(time.perf_counter() - started, 2),
            "image_count": len(detail.images),
            "has_condition_report": detail.condition_report is not None,
        }
    except Exception as exc:
        return {
            "vin": vin,
            "success": False,
            "seconds": round(time.perf_counter() - started, 2),
            "error": str(exc),
        }
    finally:
        browser.close()


if __name__ == "__main__":
    main()
