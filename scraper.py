import asyncio
import os
import json
import re
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio

INPUT_JSON = "morocco_cities.json"
OUTPUT_FILE = "data.csv"

KEYWORDS = ["Cafe", "Restaurant", "Snack", "Tea House", "Pizzeria", "Fast Food"]

CONCURRENT_TABS = 6
STEP_SIZE = 0.015          # ~2.5km grid (Fast coverage).
ZOOM_LEVEL = 16
TIMEOUT_SEC = 20000       # 20s timeout

def load_cities():
    if not os.path.exists(INPUT_JSON):
        print(f"❌ Error: {INPUT_JSON} missing.")
        return []
    with open(INPUT_JSON, 'r', encoding='utf-8') as f:
        return json.load(f)

def generate_grid(city_data):
    lat = float(city_data['lat'])
    lng = float(city_data['lng'])
    pop = float(city_data.get('population', 50000))
    
    radius_km = min(15, max(4, int(pop / 100000)))
    
    lat_range = radius_km / 111
    lng_range = radius_km / 90
    
    points = []
    curr_lat = lat - lat_range
    end_lat = lat + lat_range
    start_lng = lng - lng_range
    end_lng = lng + lng_range
    
    while curr_lat <= end_lat:
        curr_lng = start_lng
        while curr_lng <= end_lng:
            points.append((curr_lat, curr_lng))
            curr_lng += STEP_SIZE
        curr_lat += STEP_SIZE
    return points

def clean_rating(details_text):
    """
    🧹 ROBUST RATING CLEANER
    Extracts ONLY numbers 0.0 to 5.0. Returns 'Unknown' if failed.
    """
    if not details_text: return "Unknown"

    match = re.search(r'(?:^|\s)([0-5][.,]\d)(?:\s|$|·|\()', details_text)
    
    if match:
        val = match.group(1).replace(',', '.')
        try:
            float_val = float(val)
            if 0 <= float_val <= 5:
                return str(float_val)
        except:
            pass
            
    return "Unknown"

def parse_place_data(raw_text, raw_link):
    """
    Parses Name and Rating. Handles UTF-8 (Arabic/French) correctly.
    """
    lines = raw_text.split('\n')
    if not lines: return None, None, None

    name = lines[0].strip()
    
    # Combine the rest of the lines to search for rating
    details = " ".join(lines[1:])
    
    rating = clean_rating(details)
    clean_link = raw_link.split('?')[0] if raw_link else ""

    return name, rating, clean_link

async def scrape_sector(context, lat, lng, keyword):
    page = await context.new_page()
    results = []
    
    # URL (Forced English interface ensures the code structure is consistent)
    url = f"https://www.google.com/maps/search/{keyword}/@{lat},{lng},{ZOOM_LEVEL}z?hl=en"
    
    try:
        await page.goto(url, timeout=TIMEOUT_SEC)
        
        try:
            await page.wait_for_selector('div[role="feed"], div[role="main"]', timeout=5000)
        except:
            return []

        # Scroll logic
        feed = page.locator('div[role="feed"]')
        if await feed.count() > 0:
            for _ in range(3):
                await feed.hover()
                await page.mouse.wheel(0, 4000)
                await asyncio.sleep(0.7)
                if await page.locator("text=You've reached the end").is_visible():
                    break
        
        # JS Extraction
        elements = await page.evaluate('''() => {
            const items = document.querySelectorAll('div[role="article"]');
            return Array.from(items).map(item => {
                const linkEl = item.querySelector('a');
                // We capture aria-label too because it often has cleaner text
                return { 
                    text: item.innerText, 
                    aria: linkEl ? linkEl.getAttribute('aria-label') : "",
                    href: linkEl ? linkEl.href : "" 
                };
            });
        }''')

        for el in elements:
            if not el['href']: continue
            
            # Use Python parser
            name, rate, link = parse_place_data(el['text'], el['href'])

            if (not name or name == "") and el['aria']:
                try:
                    name = el['aria'].split(" · ")[0].split(" 4.")[0]
                except:
                    name = el['aria']

            coords = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', el['href'])
            final_lat = coords.group(1) if coords else str(lat)
            final_lng = coords.group(2) if coords else str(lng)
            
            results.append({
                "Name": name,
                "Category": keyword,
                "Rating": rate,
                "Latitude": final_lat,
                "Longitude": final_lng,
                "Link": link
            })
    except:
        pass 
    finally:
        await page.close()
    
    return results

async def worker(queue, browser, seen_links, stats, total_tasks):
    # Resource Blocker
    async def block_resources(route):
        if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
            await route.abort()
        else:
            await route.continue_()

    while True:
        task = await queue.get()
        if task is None: break
        
        lat, lng, city, keyword = task
        current_idx = total_tasks - queue.qsize()

        context = await browser.new_context(locale="en-US")
        await context.route("**/*", block_resources)
        
        print(f"   ⚙️  [Sector {current_idx}/{total_tasks}] {city} ({keyword})...", end='\r')
        
        try:
            data = await scrape_sector(context, lat, lng, keyword)
            
            new_rows = []
            for row in data:
                if row['Link'] not in seen_links:
                    seen_links.add(row['Link'])
                    new_rows.append({
                        "City": city,
                        "Name": row['Name'],
                        "Category": row['Category'],
                        "Rating": row['Rating'],
                        "Latitude": row['Latitude'],
                        "Longitude": row['Longitude'],
                        "Link": row['Link']
                    })
            
            if new_rows:
                df = pd.DataFrame(new_rows)
                cols = ["City", "Name", "Category", "Rating", "Latitude", "Longitude", "Link"]
                df = df[cols]
                
                header = not os.path.isfile(OUTPUT_FILE)
                df.to_csv(OUTPUT_FILE, mode='a', header=header, index=False, encoding='utf-8-sig')
                
                stats['count'] += len(new_rows)
                print(f"   ✅ [Sector {current_idx}/{total_tasks}] {city}: Found +{len(new_rows)} {keyword}s (Total: {stats['count']})")
        
        except Exception:
            pass
        finally:
            await context.close()
            queue.task_done()

async def main():
    nest_asyncio.apply()
    cities = load_cities()
    
    seen_links = set()
    if os.path.isfile(OUTPUT_FILE):
        try:
            existing = pd.read_csv(OUTPUT_FILE, encoding='utf-8-sig')
            seen_links = set(existing['Link'].tolist())
            print(f"🔄 Resumed. Ignoring {len(seen_links)} known places.")
        except: pass

    async with async_playwright() as p:
        
        for city_data in cities:
            city_name = city_data['city']
            print(f"\n🌍 STARTING CITY: {city_name} ==================")

            browser = await p.chromium.launch(headless=True)
            
            queue = asyncio.Queue()
            grid = generate_grid(city_data)
            
            for lat, lng in grid:
                for kw in KEYWORDS:
                    queue.put_nowait((lat, lng, city_name, kw))
            
            total_tasks = queue.qsize()
            print(f"   📍 Load: {total_tasks} sectors.")
            
            stats = {'count': len(seen_links)}
            workers = []
            
            for _ in range(CONCURRENT_TABS):
                w = asyncio.create_task(worker(queue, browser, seen_links, stats, total_tasks))
                workers.append(w)
            
            await queue.join()
            
            for _ in range(CONCURRENT_TABS):
                queue.put_nowait(None)
            await asyncio.gather(*workers)
            
            await browser.close()
            print(f"   🏁 {city_name} Complete. Cooling down 3s...")
            await asyncio.sleep(3)

    print("\n🚀 ALL JOBS DONE.")

if __name__ == "__main__":
    asyncio.run(main())
