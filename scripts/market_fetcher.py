import os
import json
import logging
import asyncio
from datetime import datetime , timezone
import aiohttp
import aiofiles
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


async def fetch_markets_async():

    """Fetches active markets asynchronously from the API."""

    api_url = os.getenv('API_BASE_URL')

    if not api_url:
        logger.error("API_BASE_URL environment variable is not set.")
        return []
    
    timeout = aiohttp.ClientTimeout(total=10)

    for attempt in range(3):
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(f"{api_url}/markets/active") as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Fetched {len(data['data'])} markets from API.")
                        return data['data']
                    else:
                        logger.error(f"API request to fetch markets failed with status: {response.status}")

        except asyncio.TimeoutError:
            logger.error("API request to fetch markets timed out.")

        except Exception as e:
            logger.error(f"An error occurred while fetching markets: {e}", exc_info=True)
        
        if attempt < 2:
            logger.info(f"Retrying API request ({attempt + 2}/3)...")
            await asyncio.sleep(2 ** attempt)
    
    logger.error("All API attempts to fetch markets failed.")
    return []




def filter_hourly_markets(markets):

    """Filters hourly markets and merges related data to create complete market entries."""

    if len(markets) < 5:
        print("Insufficient markets to merge")
        return []

    hourly_markets = [m for m in markets if 'Hourly' in m.get('tags', [])]

    merged_markets = []
    other_markets = []
    BTC_market = []
    for m in hourly_markets:
        if 'positionIds' in m:
            other_markets.append(m)
        else: 
             BTC_market.append(m)

    if not BTC_market or not other_markets:
        print("Missing valid markets for merging")
        return []

    for market in hourly_markets:
        try:
            if 'tokens' not in market:
                tokens = { 
                          'yes': market['positionIds'][0] if 'positionIds' in market else '0',
                          'no': market['positionIds'][1] if 'positionIds' in market else '0'
                          }
                merged_markets.append({
                'slug': market.get('slug', 'unknown'),
                'address': market.get('address', '0'),
                'tokens': tokens,
                'tradeType' : market.get('tradeType', 'unknown'),
                'expiration': market.get('expirationTimestamp', '0')
            })
            if 'address' not in market:
                tokens = BTC_market[0]['tokens']
                merged_markets.append({
                'slug': market.get('slug', 'unknown'),
                'address': market.get('address', '0'),
                'tokens': tokens,
                'tradeType' : market.get('tradeType', 'unknown'),
                'expiration': market.get('expirationTimestamp', '0') }
                )

        except Exception as e:

            logger.error(f"Failed to merge market {market.get('id', 'unknown')}: {e}")

    for index, item in enumerate(merged_markets):
        expiration_time = datetime.fromtimestamp(item['expiration']/1000)
        if (expiration_time - datetime.now()).total_seconds() <= 3600:
          return merged_markets




async def save_markets_to_file_async(markets):

    """Saves the list of markets to a JSON file asynchronously."""

    try:
        os.makedirs('config', exist_ok=True)
        async with aiofiles.open('config/markets.json', 'w') as f:
            await f.write(json.dumps(markets, indent=2))
        logger.info(f"Saved {len(markets)} markets to config/markets.json")
    except Exception as e:
        logger.error(f"Failed to write to markets.json: {e}", exc_info=True)
        raise



async def update_markets():

    """Fetches, filters, and saves the latest market data."""

    markets = await fetch_markets_async()
    if not markets:
        logger.warning("No markets fetched, cannot update.")
        return []
    
    filtered_markets = filter_hourly_markets(markets)

    if filtered_markets:
        try:
            await save_markets_to_file_async(filtered_markets)
        except Exception:
            logger.warning("Markets file not saved due to a write failure.")
    else:
        logger.warning("No hourly markets found after filtering.")
    
    return filtered_markets




async def get_active_markets():

    """Loads markets from the local cache file, falling back to the API if necessary."""

    try:
        async with aiofiles.open('config/markets.json', 'r') as f:
            content = await f.read()
            cached_markets = json.loads(content)

        if not cached_markets:
            logger.warning('markets.json is empty... Fetching from API')
            return await update_markets()
        
        expiration_ts_ms = cached_markets[0].get('expiration')
        if not expiration_ts_ms:
            logger.warning('Cached markets are missing expiration timestamps... Refetching from API..')
            return await update_markets()
        
        expiration_time_utc = datetime.fromtimestamp(expiration_ts_ms / 1000, tz=timezone.utc) 
        now_utc = datetime.now(timezone.utc)


        if expiration_time_utc < now_utc:
            logger.info(f'Cached markets have expired at: {expiration_time_utc}... Fetching fresh markets from API..')
            return await update_markets()
        

        logger.info(f"Loaded {len(cached_markets)} valid hourly markets from config/markets.json")
        return cached_markets
    
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logger.warning(f"Could not load from markets.json ({e}). Fetching from API instead.")
        return await update_markets()