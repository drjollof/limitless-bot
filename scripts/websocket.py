import asyncio
import json
import logging
import time
import os
import aiohttp
from typing import Optional, List
import socketio
from scripts.auth import authenticate_async, get_signing_message_async
from scripts.trade_utils import prepare_signed_order

logger = logging.getLogger(__name__)

#main websocket client class from API Docs, modified for our use case

class LimitlessWebSocket:
    def __init__(self, websocket_url: str, private_key: Optional[str] = None):
        self.websocket_url = websocket_url
        self.private_key = private_key
        self.session_cookie = None
        self.connected = False
        self.subscribed_markets: List[str] = []
        self.sio = socketio.AsyncClient(logger=False, engineio_logger=False)
        self._setup_handlers()

    def _setup_handlers(self):
        """Setup essential event handlers"""

        @self.sio.event(namespace='/markets')
        async def connect():
            self.connected = True
            print("Connected to /markets")

            # Send authentication if available
            if self.session_cookie:
                await self.sio.emit('authenticate', f'Bearer {self.session_cookie}', namespace='/markets')

            # Re-subscribe to markets after reconnection
            if self.subscribed_markets:
                await asyncio.sleep(1)
                await self._resubscribe()

        @self.sio.event(namespace='/markets')
        async def disconnect():
            self.connected = False
            print("Disconnected from /markets")

        @self.sio.event(namespace='/markets')
        async def authenticated(data):
            print(f"Received packet MESSAGE data 2/markets, [\"authenticated\", {json.dumps(data)}]")

        @self.sio.event(namespace='/markets')
        async def newPriceData(data):
            """Print raw newPriceData packet"""
            #print(f"Received packet MESSAGE data 2/markets, [\"newPriceData\", {json.dumps(data)}]")
            print(f'{json.dumps(data)}')
           

        @self.sio.event(namespace='/markets')
        async def positions(data):
            """Print raw positions packet"""
            print(f"Received packet MESSAGE data 2/markets, [\"positions\", {json.dumps(data)}]")

        @self.sio.event(namespace='/markets')
        async def system(data):
            print(f"Received packet MESSAGE data 2/markets, [\"system\", {json.dumps(data)}]")

        @self.sio.event(namespace='/markets')
        async def exception(data):
            """Print raw exception packet"""
            print(f"Received packet MESSAGE data 2/markets, [\"exception\", {json.dumps(data)}]")

    async def authenticate(self):
        """Get session cookie for authentication"""
        if not self.private_key:
            print("No private key - running in public mode")
            return

        try:
            print("Authenticating with private key...")
            signing_message = await get_signing_message_async()
            self.session_cookie, user_data = await authenticate_async(self.private_key)
            print(f"Authenticated as: {user_data['account']}")
        except Exception as e:
            print(f"Authentication failed: {e}")

    async def connect(self):
        """Connect to WebSocket with working configuration"""
        try:
            # Authenticate first if private key provided
            await self.authenticate()

            # Connect with same options as working version
            print(f"🔌 Connecting to {self.websocket_url}...")

            # Prepare connection options with authentication headers if available
            connect_options = {'transports': ['websocket']}
            if self.session_cookie:
                connect_options['headers'] = {
                    'Cookie': f'limitless_session={self.session_cookie}'
                }
                print("Adding session cookie to connection headers")

            await self.sio.connect(
                self.websocket_url,
                namespaces=['/markets'],
                **connect_options
            )

            # Wait for connection to establish
            max_retries = 10
            for _ in range(max_retries):
                if self.connected:
                    break
                await asyncio.sleep(0.2)

            if self.connected:
                print("Successfully connected")
            else:
                print("Connection failed")

        except Exception as e:
            print(f"Connection error: {e}")
            raise

    async def subscribe_markets(self, market_addresses: List[str]):
        """Subscribe to market price updates"""
        if not self.connected:
            print("Not connected - call connect() first")
            return

        print(f"Subscribing to {len(market_addresses)} markets")
        payload = {'marketAddresses': market_addresses}

        # Subscribe to price updates
        await self.sio.emit('subscribe_market_prices', payload, namespace='/markets')
        print("Subscribed to market prices")

        # Subscribe to positions if authenticated
        if self.session_cookie:
            await self.sio.emit('subscribe_positions', payload, namespace='/markets')
            print("Subscribed to positions")

        # Track subscribed markets for reconnection
        self.subscribed_markets.extend(
            addr for addr in market_addresses if addr not in self.subscribed_markets
        )

    async def _resubscribe(self):
        """Re-subscribe to markets after reconnection"""
        if self.subscribed_markets:
            await self.subscribe_markets(self.subscribed_markets)

    async def disconnect(self):
        """Disconnect from WebSocket"""
        if self.connected:
            await self.sio.disconnect()
            print("Disconnected")

    async def wait(self):
        """Keep connection alive and listen for events"""
        await self.sio.wait()

   


# Custom websocket client with strategy integration inherited from main websocket class

class CustomWebSocket(LimitlessWebSocket):
    def __init__(self, websocket_url, private_key: Optional[str] , strategy_func, initial_markets: list , api_url: str):
        super().__init__(websocket_url, private_key)
        self.latest_prices = {}
        self.strategy = strategy_func
        self.user_data = None
        self.api_url = api_url
        self.market_data_cache = {}
        self.subscribed_markets = [m['address'] for m in initial_markets if m.get('address') and m['address'] != '0']


        if initial_markets:
            logger.info(f"Initializing client with data for {len(initial_markets)} markets.")
            for market in initial_markets:
                if 'address' in market and market['address'] != '0':
                    self.market_data_cache[market['address']] = market
        
        self._setup_handlers()

    def _setup_handlers(self):
        sio = self.sio

        @sio.event(namespace='/markets')
        async def connect():
            self.connected = True
            logger.info("Connected to WebSocket /markets namespace.")

            if self.session_cookie:
                await sio.emit('authenticate', f'Bearer {self.session_cookie}', namespace='/markets')
            
            
            if self.subscribed_markets:
                logger.info("subscribing to tracked markets after connection.")
                await self.subscribe_markets(self.subscribed_markets)


        @sio.event(namespace='/markets')
        async def disconnect(*args):
            self.connected = False

            if args:
                logger.warning(f"Disconnected from WebSocket. Reason: {args}")
            else:
                logger.warning("Disconnected from WebSocket /markets namespace.")
            

        @sio.event(namespace='/markets')
        async def newPriceData(data):
            logging.info("Received newPriceData event from server.")

            try:
                market_address = None
                prices = None

                '''API sends price data in two possible formats; handle both.'''


                # Format 1: marketAddress is at the top level

                if 'marketAddress' in data and isinstance(data.get('updatedPrices'), dict):
                    market_address = data['marketAddress']
                    prices = data['updatedPrices']

                
                # Format 2: updatedPrices is a list

                elif isinstance(data.get('updatedPrices'), list) and len(data['updatedPrices']) > 0:

                    # Extract the first price object from the list

                    price_info = data['updatedPrices'][0]
                    if isinstance(price_info, dict) and 'marketAddress' in price_info:
                        market_address = price_info['marketAddress']
                        prices = price_info # The whole dictionary is the price info

                # If after checking both formats, we don't have what we need, skip.
                if not market_address or not prices or 'noPrice' not in prices or 'yesPrice' not in prices:
                    logging.warning(f"Skipping price data with unknown format: {data}")
                    return


                # record valid address and prices.

                yes_price = float(prices['yesPrice']) / 100
                no_price = float(prices['noPrice']) / 100

                self.latest_prices[market_address] = {'yes': yes_price, 'no': no_price, 'timestamp': time.time()}
                logging.info(f"Parsed Market {market_address}: YES=${yes_price:.2f}, NO=${no_price:.2f}")

                
                # Execute strategy if defined

                if self.strategy:
                    market_data = {'market_address': market_address, 
                                   'yes_price': yes_price, 
                                   'no_price': no_price}
                    
                    await self.strategy(market_data, self)


            except (ValueError, TypeError) as e:
                logger.error(f"Error parsing price data: {e}. Data: {data}", exc_info=True)  

            except Exception as e:
                logger.error(f"Error processing newPriceData: {e}. Data: {data}", exc_info=True)
    


    async def authenticate(self):
        if not self.private_key:
            logger.info("No private key provided. Running in public mode.")
            return
        try:
            logger.info("Authenticating session with private key...")
            self.session_cookie, self.user_data = await authenticate_async(self.private_key)
            logger.info(f"Authenticated as: {self.user_data.get('account')}")
        except Exception as e:
            logger.error(f"Session authentication failed: {e}", exc_info=True)
            raise



    async def connect(self):
        try:
            if self.private_key:
                await self.authenticate()
            
            logger.info(f"Connecting to WebSocket at {self.websocket_url}...")
            connect_options = {'transports': ['websocket']}
            if self.session_cookie:
                connect_options['headers'] = {'Cookie': f'limitless_session={self.session_cookie}'}
            
            await self.sio.connect(self.websocket_url, namespaces=['/markets'], **connect_options)
           
    

        except socketio.exceptions.ConnectionError as e:
            logger.error(f"WebSocket connection failed: {e}")
            raise



    async def disconnect(self):
        if self.connected:
            await self.sio.disconnect()
            logger.info("Disconnected from WebSocket.")
    


    async def subscribe_markets(self, market_addresses: List[str]):
        if not self.connected: 
            return
        
        

        logger.info(f"Subscribing to {len(market_addresses)} market(s).")
        payload = {'marketAddresses': market_addresses}
        await self.sio.emit('subscribe_market_prices', payload, namespace='/markets')
       

        
    # update market subscriptions after markets expires in an hour... used by the market updater task in main.py

    async def update_market_subscriptions(self, new_market_addresses: list[str]):
        """
        Disconnects, updates the list of tracked markets, and reconnects.
        
        """

        logger.info("Updating market subscriptions...")
        
        # Disconnect if currently connected
        if self.connected:
            await self.sio.disconnect()

            # Give it a moment to process the disconnection
            await asyncio.sleep(1)

        # Replace the old list of markets with the new, definitive list

        self.subscribed_markets = new_market_addresses
        logger.info(f"Client will now track {len(self.subscribed_markets)} new markets.")

        # Reconnect. The 'connect' event handler will automatically handle
        # subscribing to the new self.subscribed_markets list
        await self.connect()


    # Place order method to execute trades via the API

    async def place_order(self, market_address: str, share_type: str, size: float, price: float):
        if not all([self.private_key, self.user_data, self.session_cookie]):
            logger.warning("Cannot place order: Client is not authenticated.")
            return
        
        # Ensure required attributes are set and not None
        assert self.user_data is not None, "User data should not be None if authenticated"
        assert self.private_key is not None, "Private key should not be None if authenticated"


        # Check if market data is available in cache
        if market_address not in self.market_data_cache:
            logger.error(f"Cannot place order: Market data for {market_address} not found in cache.")
            return
        
        market_data = self.market_data_cache[market_address]
        trade_params = {"share_type": share_type, "size": size, "price": price}
        
        #initiate trade process
        logger.info("--- INITIATING TRADE ---")

        try:
            loop = asyncio.get_running_loop()
            final_order_payload = await loop.run_in_executor(
                None, prepare_signed_order, self.user_data, market_data, trade_params, self.private_key
            )
            
            order_endpoint = f"{self.api_url}/orders"
            headers = {
                'Cookie': f'limitless_session={self.session_cookie}',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
            }


            # Print the final payload in a readable JSON format to ensure correctness before sending---- should be removed in production
           # logger.info(f"Final order payload being sent: {json.dumps(final_order_payload, indent=2)}")


            async with aiohttp.ClientSession() as session:
                async with session.post(order_endpoint, headers=headers, json=final_order_payload) as response:

                    if response.status in [200, 201]:
                        result = await response.json()
                        logger.info(f"TRADE SUCCESSFUL: {result}")

                    else:
                        error_text = await response.text()
                        logger.error(f"TRADE FAILED: Status {response.status}, Response: {error_text}")

        except Exception as e:
            logger.error(f"An error occurred during trade execution: {e}", exc_info=True)

    async def wait(self):
        """Keep connection alive and listen for events."""
        await self.sio.wait()