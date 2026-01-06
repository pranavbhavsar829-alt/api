import aiohttp
import asyncio
import sqlite3
import os
import sys
import time
from datetime import datetime

# --- CONFIGURATION ---
# Must match Server's DB path exactly
if os.path.exists('/var/lib/data'):
    BASE_DIR = '/var/lib/data'
elif os.path.exists('/data'):
    BASE_DIR = '/data'
else:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))

DB_PATH = os.path.join(BASE_DIR, 'my_history_storage.db')
EXTERNAL_API_URL = "https://draw.ar-lottery01.com/WinGo/WinGo_1M/GetHistoryIssuePage.json"

# --- DATABASE INIT ---
def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''CREATE TABLE IF NOT EXISTS history (
                    issue TEXT PRIMARY KEY, 
                    number INTEGER, 
                    color TEXT, 
                    created_at TIMESTAMP
                )''')
    conn.commit()
    conn.close()

# --- FETCHING LOGIC ---
async def fetch_external_page(session, page_no):
    """Grabs a single page from the gambling site."""
    try:
        params = { "pageSize": 20, "pageNo": page_no, "typeId": 1 }
        async with session.get(EXTERNAL_API_URL, params=params, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json(content_type=None)
                return data.get('data', {}).get('list', [])
    except Exception as e:
        print(f"[FETCHER] Error fetching page {page_no}: {e}")
    return []

async def save_to_db(items):
    """Saves a list of items to OUR database."""
    if not items: return
    conn = sqlite3.connect(DB_PATH)
    new_count = 0
    for item in items:
        try:
            iss = str(item.get('issueNumber'))
            num = int(item.get('number'))
            col = item.get('color', '')
            
            # Insert (Ignore if already exists)
            conn.execute(
                "INSERT OR IGNORE INTO history (issue, number, color, created_at) VALUES (?, ?, ?, ?)",
                (iss, num, col, datetime.now())
            )
            if conn.total_changes > 0: new_count += 1
        except: pass
    conn.commit()
    conn.close()
    if new_count > 0:
        print(f"[RECORDER] Saved {new_count} new records.")

# --- MAIN LOOP ---
async def start_recording_engine():
    print(">>> DATA RECORDER ENGINE INITIALIZED <<<")
    init_db()
    
    async with aiohttp.ClientSession() as session:
        
        # PHASE 1: INITIAL BACKFILL (Grab last 1000 results on startup)
        print(">>> STARTING BACKFILL (Mining old data)...")
        for p in range(1, 51): # Pages 1 to 50
            items = await fetch_external_page(session, p)
            await save_to_db(items)
            await asyncio.sleep(0.5) # Be gentle
        print(">>> BACKFILL COMPLETE. SWITCHING TO LIVE MODE. <<<")

        # PHASE 2: LIVE RECORDING (Forever)
        while True:
            # We only need to check Page 1 constantly to catch new 1-minute results
            items = await fetch_external_page(session, 1)
            await save_to_db(items)
            
            # Wait 5 seconds before checking again (safe buffer)
            await asyncio.sleep(5)

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(start_recording_engine())