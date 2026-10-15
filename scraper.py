"""Scrape Moroccan supermarket-chain locations from Google Maps.

The scraper intentionally targets named chains only. Search results are checked
against ``supermarket_chains.json`` before they are written, so unrelated cafes,
restaurants, independent grocers and corporate offices do not leak into the CSV.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import math
import random
import re
import unicodedata
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, TypeVar

try:
    from playwright.async_api import (
        Browser,
        BrowserContext,
        Error as PlaywrightError,
        Page,
        async_playwright,
    )
except ModuleNotFoundError:  # Allows config validation and unit tests before installation.
    Browser = BrowserContext = Page = Any
    PlaywrightError = OSError
    async_playwright = None


DEFAULT_CITIES_FILE = Path("morocco_cities.json")
DEFAULT_CHAINS_FILE = Path("supermarket_chains.json")
DEFAULT_OUTPUT_FILE = Path("morocco_supermarkets.csv")
CSV_FIELDS = [
    "Company", "Brand", "Name", "City", "Address", "Latitude", "Longitude",
    "Link", "SearchKeyword", "ScrapedAtUTC",
]
DEFAULT_EXCLUDED_TERMS = (
    "head office", "siège", "siege", "holding", "entrepôt", "entrepot",
    "warehouse", "distribution center", "centre logistique",
    "direction régionale", "direction regionale",
)

LOGGER = logging.getLogger("morocco-supermarket-scraper")
T = TypeVar("T")


@dataclass(frozen=True)
class Chain:
    company: str
    brand: str
    query: str
    aliases: tuple[str, ...]
    dense: bool = False


@dataclass(frozen=True)
class City:
    name: str
    latitude: float
    longitude: float
    population: int


@dataclass(frozen=True)
class ScrapeOptions:
    output: Path
    concurrency: int
    timeout_ms: int
    zoom: int
    grid_step_km: float
    max_distance_km: float
    scrolls: int
    headed: bool
    delay_min: float
    delay_max: float


def normalize_text(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(char for char in value if not unicodedata.combining(char))
    value = re.sub(r"[^\w]+", " ", value.casefold(), flags=re.UNICODE)
    return " ".join(value.split())


def contains_term(value: str, term: str) -> bool:
    normalized_value = f" {normalize_text(value)} "
    normalized_term = normalize_text(term)
    return bool(normalized_term) and f" {normalized_term} " in normalized_value


def load_cities(path: Path) -> list[City]:
    try:
        raw_cities = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Cities file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in cities file {path}: {exc}") from exc

    cities: list[City] = []
    for index, item in enumerate(raw_cities):
        try:
            cities.append(City(
                name=str(item["city"]).strip(),
                latitude=float(item["lat"]),
                longitude=float(item["lng"]),
                population=int(float(item.get("population") or 50_000)),
            ))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"Invalid city at index {index} in {path}: {item!r}") from exc
    if not cities:
        raise ValueError(f"No cities found in {path}")
    return cities


def load_chains(path: Path) -> list[Chain]:
    try:
        raw_chains = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(f"Chains file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in chains file {path}: {exc}") from exc

    chains: list[Chain] = []
    for index, item in enumerate(raw_chains):
        try:
            aliases = tuple(str(alias).strip() for alias in item["aliases"] if str(alias).strip())
            chain = Chain(
                company=str(item["company"]).strip(),
                brand=str(item["brand"]).strip(),
                query=str(item["query"]).strip(),
                aliases=aliases,
                dense=bool(item.get("dense", False)),
            )
        except (KeyError, TypeError) as exc:
            raise ValueError(f"Invalid chain at index {index} in {path}: {item!r}") from exc
        if not all((chain.company, chain.brand, chain.query, chain.aliases)):
            raise ValueError(f"Empty required chain value at index {index} in {path}")
        chains.append(chain)
    if not chains:
        raise ValueError(f"No chains found in {path}")
    return chains


def city_radius_km(population: int) -> float:
    return min(15.0, max(4.0, population / 100_000))


def _axis_offsets(radius_km: float, step_km: float) -> list[float]:
    if radius_km <= step_km / 2:
        return [0.0]
    steps = max(1, math.floor(radius_km / step_km))
    return [index * step_km for index in range(-steps, steps + 1)]


def generate_grid(city: City, step_km: float) -> list[tuple[float, float]]:
    """Return a latitude-aware grid that always includes the city centre."""
    radius = city_radius_km(city.population)
    offsets = _axis_offsets(radius, step_km)
    longitude_km = 111.32 * max(0.2, math.cos(math.radians(city.latitude)))
    return [
        (city.latitude + north / 110.574, city.longitude + east / longitude_km)
        for north in offsets
        for east in offsets
    ]


def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    radius_km = 6371.0088
    lat_delta = math.radians(lat2 - lat1)
    lon_delta = math.radians(lon2 - lon1)
    a = (
        math.sin(lat_delta / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(lon_delta / 2) ** 2
    )
    return radius_km * 2 * math.atan2(math.sqrt(a), math.sqrt(max(0.0, 1 - a)))


def extract_coordinates(url: str) -> tuple[float, float] | None:
    for pattern in (
        r"!3d(-?\d+(?:\.\d+)?)!4d(-?\d+(?:\.\d+)?)",
        r"/@(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?),",
        r"[?&]query=(-?\d+(?:\.\d+)?),(-?\d+(?:\.\d+)?)",
    ):
        match = re.search(pattern, url)
        if match:
            return float(match.group(1)), float(match.group(2))
    return None


def canonicalize_url(url: str) -> str:
    if not url:
        return ""
    parsed = urllib.parse.urlsplit(url)
    return urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


def classify_chain(name: str, chains: Iterable[Chain]) -> Chain | None:
    if any(contains_term(name, term) for term in DEFAULT_EXCLUDED_TERMS):
        return None
    candidates: list[tuple[int, Chain]] = []
    for chain in chains:
        matches = [alias for alias in chain.aliases if contains_term(name, alias)]
        if matches:
            candidates.append((max(len(normalize_text(alias)) for alias in matches), chain))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def nearest_city(latitude: float, longitude: float, cities: Iterable[City]) -> str:
    return min(cities, key=lambda city: calculate_distance(
        latitude, longitude, city.latitude, city.longitude
    )).name


def clean_labeled_value(value: str, labels: Iterable[str]) -> str:
    cleaned = re.sub(r"[\ue000-\uf8ff]", "", value).replace("\n", ", ").strip(" ,")
    lowered = cleaned.casefold()
    for label in labels:
        if lowered.startswith(label.casefold()):
            return cleaned[len(label):].strip(" :,")
    return cleaned


def result_key(name: str, latitude: float | None, longitude: float | None, url: str) -> str:
    if latitude is not None and longitude is not None:
        return f"{normalize_text(name)}|{latitude:.5f}|{longitude:.5f}"
    return canonicalize_url(url) or normalize_text(name)


class CsvSink:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = asyncio.Lock()
        self.seen: set[str] = set()
        self.count = 0

    def load_existing(self) -> None:
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                try:
                    latitude = float(row["Latitude"])
                    longitude = float(row["Longitude"])
                except (KeyError, TypeError, ValueError):
                    latitude = longitude = None
                self.seen.add(result_key(
                    row.get("Name", ""), latitude, longitude, row.get("Link", "")
                ))
        self.count = len(self.seen)

    async def add(self, row: dict[str, Any], key: str) -> bool:
        async with self.lock:
            if key in self.seen:
                return False
            self.path.parent.mkdir(parents=True, exist_ok=True)
            write_header = not self.path.exists() or self.path.stat().st_size == 0
            with self.path.open("a", encoding="utf-8-sig", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
                if write_header:
                    writer.writeheader()
                writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})
            self.seen.add(key)
            self.count += 1
            return True


async def block_heavy_resources(route: Any) -> None:
    if route.request.resource_type in {"image", "media", "font"}:
        await route.abort()
    else:
        await route.continue_()


async def goto_with_retry(page: Page, url: str, timeout_ms: int, attempts: int = 2) -> None:
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            return
        except (PlaywrightError, OSError) as exc:
            last_error = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(1.0 + attempt)
    assert last_error is not None
    raise last_error


async def dismiss_consent(page: Page) -> None:
    for label in ("Accept all", "Tout accepter", "J'accepte"):
        button = page.get_by_role("button", name=label, exact=True)
        if await button.count():
            try:
                await button.first.click(timeout=1_500)
                return
            except PlaywrightError:
                pass


async def extract_search_cards(page: Page, scrolls: int) -> list[dict[str, str]]:
    try:
        await page.wait_for_selector('div[role="feed"], a.hfpxzc, h1', timeout=7_000)
    except PlaywrightError:
        return []

    feed = page.locator('div[role="feed"]')
    if await feed.count():
        previous_count = -1
        unchanged = 0
        for _ in range(scrolls):
            count = await page.locator("a.hfpxzc").count()
            unchanged = unchanged + 1 if count == previous_count else 0
            if unchanged >= 2:
                break
            previous_count = count
            await feed.evaluate("element => element.scrollTo(0, element.scrollHeight)")
            await asyncio.sleep(0.65)

    cards = await page.locator("a.hfpxzc").evaluate_all(
        """links => links.map(link => {
            const article = link.closest('[role="article"]');
            return {
                name: link.getAttribute('aria-label') || '',
                href: link.href || '',
                text: article ? article.innerText : ''
            };
        })"""
    )
    if cards:
        return cards

    heading = page.locator("h1").first
    if await heading.count():
        return [{"name": (await heading.inner_text()).strip(), "href": page.url, "text": ""}]
    return []


async def get_address(page: Page, url: str, timeout_ms: int) -> str:
    try:
        separator = "&" if "?" in url else "?"
        await goto_with_retry(page, f"{url}{separator}hl=en", timeout_ms)
        locator = page.locator('button[data-item-id="address"]').first
        await locator.wait_for(state="visible", timeout=min(5_000, timeout_ms))
        value = (await locator.get_attribute("aria-label")) or (await locator.inner_text())
        return clean_labeled_value(value, ("Address", "Adresse")) or "Unknown"
    except (PlaywrightError, OSError):
        return "Unknown"


async def scrape_task(
    context: BrowserContext,
    search_page: Page,
    detail_page: Page,
    city: City,
    point: tuple[float, float],
    requested_chain: Chain,
    all_chains: list[Chain],
    all_cities: list[City],
    sink: CsvSink,
    options: ScrapeOptions,
) -> int:
    latitude, longitude = point
    await context.set_geolocation({"latitude": latitude, "longitude": longitude})
    keyword = f"{requested_chain.query} {city.name} Morocco"
    encoded_keyword = urllib.parse.quote(keyword, safe="")
    url = (
        f"https://www.google.com/maps/search/{encoded_keyword}/"
        f"@{latitude:.7f},{longitude:.7f},{options.zoom}z?hl=en"
    )

    await goto_with_retry(search_page, url, options.timeout_ms)
    await dismiss_consent(search_page)
    page_text = (await search_page.locator("body").inner_text()).casefold()
    if "unusual traffic" in page_text or "not a robot" in page_text:
        raise RuntimeError("Google Maps presented a bot challenge; reduce concurrency or retry later")

    added = 0
    for card in await extract_search_cards(search_page, options.scrolls):
        name = clean_labeled_value(card.get("name") or card.get("text", "").split("\n")[0], ())
        href = card.get("href", "")
        chain = classify_chain(name, all_chains)
        if not name or not href or chain is None or chain.brand != requested_chain.brand:
            continue

        coordinates = extract_coordinates(href)
        if coordinates is None:
            continue
        place_latitude, place_longitude = coordinates
        if calculate_distance(latitude, longitude, place_latitude, place_longitude) > options.max_distance_km:
            continue

        key = result_key(name, place_latitude, place_longitude, href)
        if key in sink.seen:
            continue
        address = await get_address(detail_page, href, options.timeout_ms)
        row = {
            "Company": chain.company,
            "Brand": chain.brand,
            "Name": name,
            "City": nearest_city(place_latitude, place_longitude, all_cities),
            "Address": address,
            "Latitude": f"{place_latitude:.7f}",
            "Longitude": f"{place_longitude:.7f}",
            "Link": canonicalize_url(href),
            "SearchKeyword": requested_chain.query,
            "ScrapedAtUTC": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        if await sink.add(row, key):
            added += 1
    return added


async def worker(
    worker_id: int,
    queue: asyncio.Queue[Any],
    browser: Browser,
    chains: list[Chain],
    cities: list[City],
    sink: CsvSink,
    options: ScrapeOptions,
    stats: dict[str, int],
) -> None:
    context = await browser.new_context(
        locale="en-US",
        geolocation={"latitude": 31.7917, "longitude": -7.0926},
        permissions=["geolocation"],
    )
    await context.route("**/*", block_heavy_resources)
    search_page = await context.new_page()
    detail_page = await context.new_page()
    try:
        while True:
            task = await queue.get()
            if task is None:
                queue.task_done()
                return
            city, point, chain = task
            try:
                added = await scrape_task(
                    context, search_page, detail_page, city, point, chain,
                    chains, cities, sink, options,
                )
                stats["completed"] += 1
                if added:
                    LOGGER.info(
                        "Worker %d: +%d %s location(s) near %s (total %d)",
                        worker_id, added, chain.brand, city.name, sink.count,
                    )
                elif stats["completed"] % 25 == 0:
                    LOGGER.info(
                        "Progress: %d search task(s) completed, %d failed, %d location(s)",
                        stats["completed"], stats["failed"], sink.count,
                    )
            except Exception as exc:  # Keep the queue alive, but expose failures.
                stats["failed"] += 1
                LOGGER.warning("Worker %d: %s / %s failed: %s", worker_id, city.name, chain.brand, exc)
            finally:
                queue.task_done()
            await asyncio.sleep(random.uniform(options.delay_min, options.delay_max))
    finally:
        await context.close()


def build_tasks(cities: Iterable[City], chains: Iterable[Chain], grid_step_km: float) -> list[Any]:
    tasks = []
    for city in cities:
        dense_points = generate_grid(city, grid_step_km)
        for chain in chains:
            points = dense_points if chain.dense else [(city.latitude, city.longitude)]
            tasks.extend((city, point, chain) for point in points)
    return tasks


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cities-file", type=Path, default=DEFAULT_CITIES_FILE)
    parser.add_argument("--chains-file", type=Path, default=DEFAULT_CHAINS_FILE)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--city", action="append", default=[], help="Only scrape this city (repeatable)")
    parser.add_argument("--brand", action="append", default=[], help="Only scrape this brand (repeatable)")
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=20.0, help="Navigation timeout in seconds")
    parser.add_argument("--zoom", type=int, default=13)
    parser.add_argument("--grid-step-km", type=float, default=8.0, help="Grid spacing for dense chains")
    parser.add_argument("--max-distance-km", type=float, default=10.0)
    parser.add_argument("--scrolls", type=int, default=6)
    parser.add_argument("--delay-min", type=float, default=0.5)
    parser.add_argument("--delay-max", type=float, default=1.5)
    parser.add_argument("--headed", action="store_true", help="Show Chromium for debugging")
    parser.add_argument("--list-brands", action="store_true")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING"), default="INFO")
    return parser.parse_args()


def select_named(
    items: list[T], requested: list[str], value: Callable[[T], str], kind: str
) -> list[T]:
    if not requested:
        return items
    wanted = {normalize_text(name) for name in requested}
    selected = [item for item in items if normalize_text(value(item)) in wanted]
    missing = wanted - {normalize_text(value(item)) for item in selected}
    if missing:
        raise ValueError(f"Unknown {kind}(s): {', '.join(sorted(missing))}")
    return selected


async def run(args: argparse.Namespace) -> None:
    if args.concurrency < 1:
        raise ValueError("--concurrency must be at least 1")
    if args.grid_step_km <= 0 or args.max_distance_km <= 0:
        raise ValueError("Grid step and maximum distance must be positive")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        raise ValueError("Delay values must satisfy 0 <= minimum <= maximum")

    catalog = load_chains(args.chains_file)
    if args.list_brands:
        for chain in catalog:
            print(f"{chain.brand} ({chain.company})")
        return
    if async_playwright is None:
        raise ValueError("Playwright is not installed; run: pip install -r requirements.txt")
    cities = select_named(load_cities(args.cities_file), args.city, lambda item: item.name, "city")
    chains = select_named(catalog, args.brand, lambda item: item.brand, "brand")

    options = ScrapeOptions(
        output=args.output,
        concurrency=args.concurrency,
        timeout_ms=int(args.timeout * 1_000),
        zoom=args.zoom,
        grid_step_km=args.grid_step_km,
        max_distance_km=args.max_distance_km,
        scrolls=args.scrolls,
        headed=args.headed,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
    )
    sink = CsvSink(options.output)
    sink.load_existing()
    if sink.count:
        LOGGER.info("Resuming with %d existing location(s) in %s", sink.count, options.output)

    tasks = build_tasks(cities, chains, options.grid_step_km)
    random.shuffle(tasks)
    LOGGER.info(
        "Starting %d search task(s) for %d brand(s) across %d city/cities",
        len(tasks), len(chains), len(cities),
    )
    queue: asyncio.Queue[Any] = asyncio.Queue()
    for task in tasks:
        queue.put_nowait(task)
    stats = {"completed": 0, "failed": 0}

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=not options.headed)
        worker_count = min(options.concurrency, len(tasks))
        workers = [
            asyncio.create_task(worker(
                index + 1, queue, browser, catalog, cities, sink, options, stats
            ))
            for index in range(worker_count)
        ]
        for _ in workers:
            queue.put_nowait(None)
        await queue.join()
        await asyncio.gather(*workers)
        await browser.close()

    LOGGER.info(
        "Done: %d location(s), %d successful search task(s), %d failed task(s)",
        sink.count, stats["completed"], stats["failed"],
    )


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s"
    )
    try:
        asyncio.run(run(args))
    except (ValueError, OSError) as exc:
        LOGGER.error("%s", exc)
        return 2
    except KeyboardInterrupt:
        LOGGER.warning("Interrupted; completed rows are already saved")
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
