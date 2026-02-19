import asyncio
import os
import json
import re
import math
import urllib.parse
import pandas as pd
from playwright.async_api import async_playwright
import nest_asyncio

INPUT_JSON = "morocco_cities.json"
OUTPUT_FILE = "morocco_banks.csv"

# 🔍 KEYWORDS optimized for Moroccan Banks
KEYWORDS = [
    "Banque", "Bank", "Attijariwafa", "Banque Populaire", 
    "Bank of Africa", "BMCE", "BMCI", "CIH", 
    "Crédit Agricole", "Crédit du Maroc", "Société Générale", 
    "Al Barid", "CFG Bank"
]

CONCURRENT_TABS = 5
STEP_SIZE = 0.025          # ~2.5km grid
ZOOM_LEVEL = 15
TIMEOUT_SEC = 15000       

# ==========================================
# 🛠️ HELPER FUNCTIONS
# ==========================================

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

def calculate_distance(lat1, lon1, lat2, lon2):
    """
    🌐 GEOFENCE MATH: Calculates distance in km between two GPS points.
    Prevents Google from injecting Khouribga results into Casablanca.
    """
    R = 6371  # Earth radius in km
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

# ==========================================
# 🤖 CORE SCRAPER
# ==========================================

async def get_place_details(context, url):
    page = await context.new_page()
    address = "Unknown"
    
    try:
        nav_url = url + "?hl=en" if "?" not in url else url + "&hl=en"
        await page.goto(nav_url, timeout=10000)
        
        address_locator = page.locator('button[data-item-id="address"]')
        await address_locator.wait_for(state="visible", timeout=4000)
        
        if await address_locator.count() > 0:
            raw_address = await address_locator.first.inner_text()
            
            # Clean up newlines and strip hidden Google map-pin icons
            cleaned = re.sub(r'[\ue000-\uf8ff]', '', raw_address).replace('\n', ', ').strip()
            
            # Remove "Address:" prefixes and leading commas
            if cleaned.lower().startswith("address:"):
                cleaned = cleaned[8:].strip()
            if cleaned.lower().startswith("adresse:"):
                cleaned = cleaned[8:].strip()
            
            cleaned = cleaned.lstrip(', ') # Fixes the ", 103 Bd Mohammed VI" issue
                
            if cleaned and re.search(r'[a-zA-Z0-9]', cleaned):
                address = cleaned
                
    except Exception:
        pass 
    finally:
        await page.close()
        
    return address

async def scrape_sector(context, lat, lng, keyword):
    page = await context.new_page()
    results = []
    
    # URL encoded safely to prevent broken links
    safe_keyword = urllib.parse.quote(keyword)
    url = f"https://www.google.com/maps/search/{safe_keyword}/@{lat},{lng},{ZOOM_LEVEL}z?hl=en"
    
    try:
        await page.goto(url, timeout=TIMEOUT_SEC)
        
        try:
            await page.wait_for_selector('div[role="feed"], div[role="main"]', timeout=5000)
        except:
            return []

        feed = page.locator('div[role="feed"]')
        if await feed.count() > 0:
            for _ in range(3):
                await feed.hover()
                await page.mouse.wheel(0, 4000)
                await asyncio.sleep(0.7)
                if await page.locator("text=You've reached the end").is_visible():
                    break
        
        elements = await page.evaluate('''() => {
            const items = document.querySelectorAll('div[role="article"]');
            return Array.from(items).map(item => {
                const linkEl = item.querySelector('a');
                return { 
                    text: item.innerText, 
                    aria: linkEl ? linkEl.getAttribute('aria-label') : "",
                    href: linkEl ? linkEl.href : "" 
                };
            });
        }''')

        for el in elements:
            if not el['href']: continue
            
            name = ""
            if el['aria']:
                try:
                    name = el['aria'].split(" · ")[0].split(" 4.")[0].split(" 3.")[0].split(" 5.")[0]
                except:
                    name = el['aria']
            
            if not name:
                name = el['text'].split('\n')[0]
                
            name = re.sub(r'[\ue000-\uf8ff]', '', name).strip()

            coords = re.search(r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)', el['href'])
            if coords:
                final_lat = float(coords.group(1))
                final_lng = float(coords.group(2))
                
                # 🛑 THE GEOFENCE CHECK
                # If Google suggests a bank more than 6km away from our search grid, REJECT IT.
                dist = calculate_distance(lat, lng, final_lat, final_lng)
                if dist > 6.0:
                    continue  # Skip this bank entirely
            else:
                final_lat = lat
                final_lng = lng
            
            results.append({
                "Name": name,
                "Latitude": str(final_lat),
                "Longitude": str(final_lng),
                "Link": el['href'].split('?')[0]
            })
    except:
        pass 
    finally:
        await page.close()
    
    return results

async def worker(queue, browser, seen_links, stats, total_tasks):
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

        # 🛑 GPS SPOOFING
        # We tell the browser that it is physically standing at the lat/lng coordinates.
        context = await browser.new_context(
            locale="en-US",
            geolocation={"longitude": float(lng), "latitude": float(lat)},
            permissions=["geolocation"]
        )
        await context.route("**/*", block_resources)
        
        print(f"   ⚙️  [Sector {current_idx}/{total_tasks}] {city} ({keyword}) Scanning...", end='\r')
        
        try:
            discovered_banks = await scrape_sector(context, lat, lng, keyword)
            
            new_rows = []
            for item in discovered_banks:
                link = item['Link']
                
                if link and link not in seen_links:
                    seen_links.add(link)
                    
                    print(f"   🔎 [Sector {current_idx}/{total_tasks}] Extracting address for: {item['Name'][:20]}...", end='\r')
                    address = await get_place_details(context, link)
                    
                    new_rows.append({
                        "Name": item['Name'],
                        "City": city,
                        "Address": address,
                        "Latitude": item['Latitude'],
                        "Longitude": item['Longitude'],
                        "Link": link
                    })
            
            if new_rows:
                df = pd.DataFrame(new_rows)
                cols = ["Name", "City", "Address", "Latitude", "Longitude", "Link"]
                df = df[cols]
                
                header = not os.path.isfile(OUTPUT_FILE)
                df.to_csv(OUTPUT_FILE, mode='a', header=header, index=False, encoding='utf-8-sig')
                
                stats['count'] += len(new_rows)
                print(f"   ✅ [Sector {current_idx}/{total_tasks}] {city}: Found +{len(new_rows)} NEW Banks (Total: {stats['count']})")
        
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