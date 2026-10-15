# Morocco Supermarket Maps Scraper

An asynchronous, resumable Google Maps scraper for the major supermarket,
hypermarket, convenience-store and food-discount chains operating in Morocco.
It exports each store's holding company, brand, name, nearest city, address,
coordinates and Maps link to CSV.

## Included chains

The default catalog is stored in `supermarket_chains.json`, not hard-coded in
Python. It currently includes:

| Company/group | Store banners |
| --- | --- |
| Marjane Holding | Marjane, Marjane Market, Marjane City |
| Groupe LabelVie / Retail Holding | Carrefour, Carrefour Market, Carrefour Express, Carrefour Gourmet, Atacadao, Supeco |
| BIM Maroc | BIM |
| Ynna Holding | Aswak Assalam |
| Africa Retail Market | Hyper U, U Express |
| Kazyon Morocco | Kazyon |
| H&S Invest Holding / One Retail | Franprix, Monoprix |

The catalog is based on Morocco's Competition Council overview of the six major
food-retail operators, official company brand pages, and the 2026 launch of the
Franprix/Monoprix partnership. `Acima` remains an alias for Marjane Market so
older Maps listings are classified correctly.

Catalog references:

- [Morocco Competition Council food-distribution opinion (2025)](https://conseil-concurrence.ma/wp-content/uploads/2025/07/Livre-WEB-AVIS-A-1-25-FR.pdf)
- [Marjane store finder](https://www.marjane.ma/?findStores=true)
- [Groupe LabelVie brands](https://labelvie.ma/activites/nos-marques/)
- [Africa Retail Market](https://www.arm.co.ma/a-propos.html)
- [Aswak Assalam](https://aswakassalam.com/qui-sommes-nous/)
- [H&S Group retail banners](https://hns.ma/secteurs/pole-retail/)
- [IFC announcement on Kazyon Morocco](https://www.ifc.org/en/pressroom/2025/ifc-and-kazyon-strengthen-partnership-to-boost-morocco-s-food-retail-sector-create)

## Why this version is safer and faster

- Results must match a known banner before they are saved. Cafes, restaurants,
  independent shops, offices and logistics sites are filtered out.
- Dense neighborhood chains use a population-aware grid; sparse hypermarket
  brands use one city-centered search instead of repeating hundreds of queries.
- A fixed worker pool reuses browser contexts and pages.
- Coordinates are geofenced and each result is assigned to the nearest city in
  the city database.
- CSV writes are locked, deduplicated by name and coordinates, and immediately
  persisted. Re-running the same command resumes from the existing file.
- Navigation retries, CAPTCHA detection and visible warnings replace silent
  `except` blocks.
- Images, video and fonts are blocked to reduce bandwidth while Maps scripts and
  styles remain available.

## Quick start with Docker

```bash
docker compose build
docker compose run --rm scraper
```

Results are written to `morocco_supermarkets.csv` in the repository directory.
Stop with `Ctrl+C`; rows already collected remain usable.

For a quick smoke test before a national run:

```bash
docker compose run --rm scraper \
  python scraper.py --city Casablanca --brand BIM --output test_bim.csv
```

## Local installation

Python 3.11 or newer is recommended.

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
playwright install --with-deps chromium
python scraper.py
```

## Useful commands

```bash
# Show every configured banner
python scraper.py --list-brands

# One or more cities and banners (options can be repeated)
python scraper.py --city Rabat --city Salé --brand BIM --brand Kazyon

# Debug Maps in a visible browser with lower concurrency
python scraper.py --city Casablanca --brand Marjane --headed --concurrency 1

# Save to a different file
python scraper.py --output data/supermarkets.csv
```

Run `python scraper.py --help` for every tuning option. Defaults are deliberately
conservative (`--concurrency 4`, 0.5–1.5 seconds between searches). Increasing
concurrency can trigger Google rate limits or CAPTCHA challenges.

## Updating the chain catalog

Each entry in `supermarket_chains.json` contains:

```json
{
  "company": "BIM Maroc",
  "brand": "BIM",
  "query": "BIM supermarket",
  "aliases": ["BIM", "BIM Maroc"],
  "dense": true
}
```

- `query` is sent to Maps along with the city and country.
- `aliases` are whole words/phrases accepted in a result name.
- `dense: true` enables grid searches for banners with many neighborhood shops.

Use a specific alias and query for ambiguous names. Do not add generic aliases
such as `market` or `supermarket`, because they would admit unrelated stores.

## Testing

The unit suite covers brand classification, accents, false-positive filtering,
coordinate parsing, distance/grid calculations and CSV deduplication.

```bash
python -m unittest discover -s tests -v
```

## Output columns

`Company`, `Brand`, `Name`, `City`, `Address`, `Latitude`, `Longitude`, `Link`,
`SearchKeyword`, and `ScrapedAtUTC`.

Google Maps markup and anti-automation behavior can change. Use the data in
accordance with applicable law and Google's terms, keep concurrency modest, and
review a sample of the CSV before relying on a national export.
