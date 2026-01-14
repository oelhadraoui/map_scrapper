# Morocco Maps Scraper

This tool scrapes business data (Cafes, Restaurants, Snacks) from Google Maps for all major Moroccan cities.

## Output
The data is saved to `data.csv` with the following columns: `City`, `Name`, `Category`, `Rating`, `Latitude`, `Longitude`, `Link`

## Prerequisites
- Docker Desktop installed.

## How to Run
1. Open this folder in your terminal.
2. Run the following command:
   ```bash
   docker compose up --build
   ```
