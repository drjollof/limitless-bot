import os
import asyncio
import logging
from datetime import datetime, timezone
from scripts.websocket import CustomWebSocket
from scripts.strategy import check_and_execute_buy_strategy
from scripts.config import load_config
from scripts.market_fetcher import update_markets
from dotenv import load_dotenv

load_dotenv()

os.makedirs('logs', exist_ok=True)
logging.basicConfig(level=logging.DEBUG if os.getenv('DEBUG', 'false').lower() == 'true' else logging.INFO, 
                    format='%(asctime)s  - %(levelname)s - %(filename)s - %(funcName)s - %(message)s',
                    handlers=[
        logging.FileHandler('logs/bot.log'),
        logging.StreamHandler()
    ])


logging.getLogger("web3").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)
logging.getLogger("socketio").setLevel(logging.WARNING)
logging.getLogger("engineio").setLevel(logging.WARNING)



# Market updater background task to periodically fetch new markets and update subscriptions
async def market_updater_task(client: CustomWebSocket, initial_markets: list):
  
   
    try:
        await client.connect()
    except Exception as e:
        logging.error(f"Initial connection failed: {e}. Updater task will exit.")
        return

    markets_to_update = initial_markets
    
    while True:
        try:

            # Sleep until just after the next market expiration
            if not markets_to_update:

                # If we have no market info, fall back to a 1-hour sleep
                sleep_duration = 3600
                logging.warning("Updater has no market data; falling back to 1-hour sleep.")

            else:

                # Find the soonest expiration time from the current list of markets
                now_utc = datetime.now(timezone.utc)
                exp_times = [m.get('expiration') for m in markets_to_update if m.get('expiration')]

                if not exp_times:
                    sleep_duration = 3600
                    logging.warning("Markets have no expiration data; falling back to 1-hour sleep.")

                else:
                    soonest_exp_ms = min(exp_times)
                    soonest_exp_utc = datetime.fromtimestamp(soonest_exp_ms / 1000, tz=timezone.utc)
                    
                    seconds_to_expiration = (soonest_exp_utc - now_utc).total_seconds()
                    
                    # Sleep until 15 seconds after the market expires to ensure the new one is available.
                    # max(15) to prevent negative sleep times if we're already past expiration.

                    sleep_duration = max(15, seconds_to_expiration + 15)
            

            logging.info(f"Market updater task is sleeping for {int(sleep_duration)} seconds (until next market activation).")
            await asyncio.sleep(sleep_duration)
            

            logging.info("Woke up. Now attempting to fetch NEW hourly markets...")

            # Get the expiration of the last traded markets
            old_expiration_ts = min([m.get('expiration', 0) for m in markets_to_update]) if markets_to_update else 0


            latest_markets = None
            MAX_RETRIES = 10  
            RETRY_DELAY = 15  

            for attempt in range(MAX_RETRIES):
                fetched_markets = await update_markets()
                
                if not fetched_markets:
                    logging.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: API returned no active markets. Retrying in {RETRY_DELAY}s...")
                    await asyncio.sleep(RETRY_DELAY)
                    continue

                new_expiration_ts = min([m.get('expiration', 0) for m in fetched_markets])
                new_expiration_utc = datetime.fromtimestamp(new_expiration_ts / 1000, tz=timezone.utc)


                # check if new new expiration is above old expiration
                if new_expiration_ts > old_expiration_ts:
                    logging.info(f"Successfully fetched new markets for the next hour (Expiration: {new_expiration_utc}).")
                    latest_markets = fetched_markets

                    break 

                else:
                    logging.warning(f"Attempt {attempt + 1}/{MAX_RETRIES}: Fetched stale markets (Expiration: {new_expiration_utc}). Retrying in {RETRY_DELAY}s...")
                    await asyncio.sleep(RETRY_DELAY)


            
            if not latest_markets:
                logging.warning("Hourly update: No new markets found. No changes made.")
                continue

            # Update the state for the next iteration of the loop
            markets_to_update = latest_markets

            await client.update_market_subscriptions(latest_markets)
            logging.info("Hourly market update complete.")

        except Exception as e:
            logging.error(f"Error in market_updater_task: {e}", exc_info=True)
            await asyncio.sleep(300) 

            







async def main():
    
    logging.info("Starting the trading bot...")


    # Create an event that will keep the main function alive until a shutdown is triggered.
    shutdown_event = asyncio.Event()
    

    # Load configuration
    config = await load_config()
    active_markets = config.get('markets', [])
    websocket_url = config.get('WS_URL')
    api_url = config.get('API_URL')
    private_key = os.getenv("PRIVATE_KEY")


    # Warn if private key and active market are missing (read-only mode)
    if not private_key:
        logging.warning("PRIVATE_KEY environment variable not found. Running in public/read-only mode.")

    if not active_markets:
        logging.error("No active markets found in configuration. cannot proceed. Exiting...")
        return


    # Ensure we have all necessary config to run the bot
    if not all([active_markets, websocket_url, api_url]):
        logging.error("Configuration is incomplete (markets, WS_URL, or API_URL). Exiting.")
        return
    

    # Filter out any invalid markets (missing address)
    valid_active_markets = []
    for market in active_markets:
        if 'address' in market and market['address'] != '0':
            valid_active_markets.append(market)
 
    # test for single market
    single_market = valid_active_markets[:1]

   
    # Initialize the WebSocket client with strategy and initial markets
    client = CustomWebSocket(
        websocket_url=websocket_url,
        private_key=private_key, 
        strategy_func=check_and_execute_buy_strategy,
        initial_markets =  active_markets,  
        api_url=api_url
    )

    # Starts the market updater background task
    updater_task = None
    try:
        
        logging.info("Launching the hourly market updater background task...")

        # pass initial markets to the updater task
        updater_task = asyncio.create_task(market_updater_task(client, valid_active_markets))
        
        logging.info("Bot is running. Listening for price updates to trigger strategy...")
        await client.wait()
        await shutdown_event.wait()


    except KeyboardInterrupt:
        logging.info("Interrupted by user. Shutting down...")
    except Exception as e:
        logging.error(f"A critical error occurred in main: {e}", exc_info=True)

    # Cleanup on shutdown
    finally:
        if updater_task and not updater_task.done():
            updater_task.cancel()
            logging.info("Market updater task cancelled.")

        if client.connected:
            await client.disconnect()
        
        # This signals the main loop to exit if it hasn't already.
        shutdown_event.set()
        logging.info("Shutdown complete.")







# Entry point
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:

        logging.info("Program terminated.")