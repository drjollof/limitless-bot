import asyncio
import json
import logging
import time
import os
import aiohttp
import uuid
from typing import Optional, List
import socketio
from datetime import datetime, timezone
from web3 import AsyncWeb3
from web3.providers import AsyncHTTPProvider
from scripts.trade_logger import trade_logger
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

   
# Nonce manager to manage nonce for multiple concurrent trades 
class NonceManager:
    def __init__(self, w3: AsyncWeb3, address: str):
        self._w3 = w3
        self._address = address
        self._lock = asyncio.Lock()
        self._nonce = -1  # Initialize to -1 to force a fetch on the first call

    async def get_nonce(self) -> int:
        async with self._lock:
            if self._nonce == -1:

                #first time, fetch from the network
                self._nonce = await self._w3.eth.get_transaction_count(self._address)
            else:
                # For subsequent calls, just increment the local copy
                self._nonce += 1
            return self._nonce







# Custom websocket client with strategy integration inherited from main websocket class

class CustomWebSocket(LimitlessWebSocket):
    def __init__(self, websocket_url, private_key: Optional[str] , strategy_func, initial_markets: list , api_url: str):
        super().__init__(websocket_url, private_key)
        self.latest_prices = {}
        self.strategy = strategy_func
        self.user_data = None
        self.api_url = api_url
        self.market_data_cache = {}
        self.w3 = None
        self.tx_lock = asyncio.Lock()
        self.nonce_manager = None
        self.traded_markets = set()
        self.subscribed_markets = [m['address'] for m in initial_markets if m.get('address') and m['address'] != '0']

        rpc_url = os.getenv('BASE_RPC_URL')
        

        if rpc_url:
            self.w3 = AsyncWeb3(AsyncHTTPProvider(rpc_url))
            account = self.w3.eth.account.from_key(self.private_key)
            self.nonce_manager = NonceManager(self.w3, account.address)

            logger.info("Initialized Web3 provider for AMM trading")

        else:
            logger.warning("BASE_RPC_URL not found.. AMM trading will be disabled")


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
            logging.debug("Received newPriceData event from server.")

            try:
                market_address = None
                prices = None

                '''API sends price data in two possible formats'''


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

                
                if not market_address or not prices or 'noPrice' not in prices or 'yesPrice' not in prices:
                    logging.warning(f"Skipping price data with unknown format: {data}")
                    return


                # record valid address and prices.
                yes_price = float(prices['yesPrice']) / 100
                no_price = float(prices['noPrice']) / 100

                self.latest_prices[market_address] = {'yes': yes_price, 'no': no_price, 'timestamp': time.time()}
                logging.debug(f"Parsed Market {market_address}: YES=${yes_price:.2f}, NO=${no_price:.2f}")

                
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
        MAX_CONNECT_ATTEMPTS = 3
        RECONNECT_DELAY = 5  

        for attempt in range(MAX_CONNECT_ATTEMPTS):

            try:
                if self.private_key:
                    await self.authenticate()
            
                logger.info(f"Connecting to WebSocket at {self.websocket_url}...")
                connect_options = {'transports': ['websocket']}

                if self.session_cookie:
                    connect_options['headers'] = {'Cookie': f'limitless_session={self.session_cookie}'}
            
                await self.sio.connect(self.websocket_url, namespaces=['/markets'], **connect_options)
                
                logger.info("WebSocket connection established successfully.")
                return 
         
            except socketio.exceptions.ConnectionError as e:
        
                logger.error(f"WebSocket connection attempt {attempt + 1} failed: {e}")
                if attempt < MAX_CONNECT_ATTEMPTS - 1:
                    logger.info(f"Retrying in {RECONNECT_DELAY} seconds...")
                    await asyncio.sleep(RECONNECT_DELAY)
                else:
                    logger.error("All WebSocket connection attempts failed.")
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
    

    async def _ensure_web3_connected(self) -> bool:
        """
        Checks the Web3 connection and returns True if it's healthy.
        """
        if not self.w3:
            return False
        
        try:
            
            if await self.w3.is_connected():
                return True
            
            else:
                logger.warning("Web3 provider is not connected. Attempting to reconnect...")
                self.w3 = AsyncWeb3(AsyncHTTPProvider(os.getenv('BASE_RPC_URL')))
                return await self.w3.is_connected()
            
        except Exception as e:
            logger.error(f"Failed to check Web3 connection: {e}")
            return False

        
    # update market subscriptions after markets expires in an hour... used by the market updater task in main.py
    async def update_market_subscriptions(self, new_market: list):
        """
        Disconnects, updates the list of tracked markets, and reconnects.
        
        """

        logger.info("Updating market subscriptions and cache for the new hour...")

        
        # Clear old cache
        self.market_data_cache.clear()


        # Fill the empty cache with new market data
        for market in new_market:
            if 'address' in market and market['address'] != '0':
                self.market_data_cache[market['address']] = market

        logger.info(f'Market data cache updated with {len(self.market_data_cache)} new markets.')


        new_market_addresses = list(self.market_data_cache.keys())

        self.traded_markets.clear()
        logger.info('Cleared traded markets memory for the new hour')


        if self.connected:
            await self.sio.disconnect()

            await asyncio.sleep(1)


        self.subscribed_markets = new_market_addresses
        logger.info(f"Client will now track {len(self.subscribed_markets)} new markets.")

       
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
        
        # Initiate trade process
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
    


    async def execute_amm_sell(self, market_address: str, share_type: str, return_amount_usd: float, price: float, reason: str) -> bool:
            """
            Executes a SELL order on an AMM market to get a specific amount of USDC back.
            """

            if not await self._ensure_web3_connected():
                logger.error("Cannot execute AMM sell: Web3 provider is not connected.")
                return False

            
            if not self.private_key or not self.w3 or not self.nonce_manager:
                logger.error("Cannot execute AMM sell: Web provider or NonceManger is not initialized.")
                return False
                
            
             

            async with self.tx_lock:

                logger.info(f"--- INITIATING AMM SELL on market {market_address} ---")  

                try:
                    w3 = self.w3
                    erc1155_address = os.getenv('CONDITIONAL_TOKEN_ADDRESS')
                    
                    if not erc1155_address:
                        logger.error("CRITICAL: CONDITIONAL_TOKEN_ADDRESS environment variable not set.. Cannot execute trade")
                        return False

                    
                    account = w3.eth.account.from_key(self.private_key)
                        
                    with open('config/amm_abi.json', 'r') as f:
                            amm_abi = json.load(f)

                    with open('config/conditional_abi.json', 'r') as f:
                                erc1155_abi = json.load(f)
                        
                    current_market_address = w3.to_checksum_address(market_address)
                    conditional_tokens_address = w3.to_checksum_address(erc1155_address)

                    amm_contract = w3.eth.contract(address=current_market_address, abi=amm_abi)
                    erc1155_contract = w3.eth.contract(address= conditional_tokens_address,abi = erc1155_abi )

                    # set sell transaction parameters
                    outcome_index = 0 if share_type.lower() == 'yes' else 1
                    outcome_name = 'YES' if outcome_index == 0 else 'NO'
                    scaling_factor = 10**6
                    return_amount_base_units = int(return_amount_usd * scaling_factor)
                    
                    max_outcome_tokens_to_sell = 2**256 - 1 # "Infinite" amount

                    # set gas details
                    priority_fee = w3.to_wei(1, 'wei') 
                    latest_block = await w3.eth.get_block('latest')
                    base_fee = latest_block['baseFeePerGas']
                    max_fee = base_fee * 2

                    is_approved = await erc1155_contract.functions.isApprovedForAll(account.address, current_market_address).call()


                    if not is_approved:

                        logger.warning('ERC1155 is not approved yet.. Sending approval transaction....')

                        #ERC1155 APPROVAL TRANSACTION
                        logger.info('Approving ERC1155 contract....')
                        approve_sell_nonce = await self.nonce_manager.get_nonce()
                        approval_tx = await erc1155_contract.functions.setApprovalForAll(
                            current_market_address,True).build_transaction({
                                    'from' : account.address,
                                    'nonce' : approve_sell_nonce
                                })
                                
                        signed_approval_tx = w3.eth.account.sign_transaction(approval_tx, self.private_key)

                        
                        approval_hash = await w3.eth.send_raw_transaction(signed_approval_tx.raw_transaction)
                        logger.info(f"Approval transaction sent. Waiting for confirmation... Hash: {approval_hash.to_0x_hex()}")
                        approval_receipt = await w3.eth.wait_for_transaction_receipt(approval_hash)
                        logger.info(f"Approval transaction confirmed in block: {approval_receipt['blockNumber']}")
                        logger.info(f'Check transaciton at https://basescan.org/tx/{approval_hash.to_0x_hex()}')

                    else:
                        logger.info('ERC1155 aproval is already set..skipping approval..')
                    

                    #SELL TRANSACTION
                    logger.info(f"Preparing AMM 'sell' transaction: Selling {outcome_name} shares to receive {return_amount_usd} USDC.")
                    sell_nonce = await self.nonce_manager.get_nonce()

                    sell_txn = await amm_contract.functions.sell(
                            return_amount_base_units,
                            outcome_index,
                            max_outcome_tokens_to_sell).build_transaction({
                            'from': account.address,
                            'nonce': sell_nonce
                            })

                    signed_sell_txn = w3.eth.account.sign_transaction(sell_txn, self.private_key)

                    
                    sell_txn_hash = await w3.eth.send_raw_transaction(signed_sell_txn.raw_transaction)
                    logger.info(f"Sell trade transaction sent: {sell_txn_hash.to_0x_hex()}. Waiting for confirmation...")
                    sell_txn_receipt = await w3.eth.wait_for_transaction_receipt(sell_txn_hash)

                    if sell_txn_receipt['status'] == 1:
                            trade_id = str(uuid.uuid4())
                            market_data = self.market_data_cache.get(market_address, {})
                        
                            trade_logger.log_trade({
                                'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                                'trade_id': trade_id,
                                'market_address': market_address,
                                'market_slug': market_data.get('slug', 'N/A'),
                                'action': 'SELL_STOP_LOSS',
                                'side': share_type.upper(),
                                'usdc_amount': return_amount_usd,
                                'price' : price,
                                'reason': reason,
                                'transaction_hash': sell_txn_hash.hex()
                            })

                            logger.info(f"Sell Txn SUCCESSFUL. Confirmed in block: {sell_txn_receipt['blockNumber']}")
                            logger.info(f'See Transaction: https://basescan.org/tx/{sell_txn_hash.to_0x_hex()} ')
                            logger.info(f"--- SELL TRADE SUCCESSFUL FOR MARKET : {market_address} ---")

                            return True
                        
                    else:
                            logger.error(f"Sell txn FAILED on-chain. Receipt: {sell_txn_receipt}")
                            return False 
                        
                    
                except Exception as e:
                    logger.error(f'An error occurred during AMM sell execution: {e}', exc_info=True)
                    return False






    async def execute_amm_buy(self, market_address: str, share_type: str, size: float, price: float, reason: str) -> bool:
        '''
        Executes AMM  buy trade via the smart contract directly
        
        '''

        if not await self._ensure_web3_connected():
            logger.error("Cannot execute AMM trade: Web3 provider is not connected.")
            return False 
        
        if not self.private_key or not self.w3 or not self.nonce_manager:
            logger.error("Cannot execute AMM sell: Web provider or NonceManager is not initialized.")
            return False
            
        
        
        async with self.tx_lock:
            
            logger.info("--- INITIATING AMM TRADE on market {market_address} ---")

            try:
                
                usdc = os.getenv('USDC_ADDRESS')
                w3 = self.w3

                if not usdc:
                    logger.error("CRITICAL: USDC_ADDRESS environment variable not set.. Cannot execute trade")
                    return False

                account = w3.eth.account.from_key(self.private_key)

                with open('config/amm_abi.json', 'r') as f:
                        amm_abi = json.load(f)

                with open('config/usdc_abi.json', 'r') as f:
                        usdc_abi = json.load(f)

                usdc_address = w3.to_checksum_address(usdc)
                current_market_address = w3.to_checksum_address(market_address)
                usdc_contract = w3.eth.contract(address = usdc_address, abi = usdc_abi)
                amm_contract = w3.eth.contract(address = current_market_address, abi = amm_abi)

                # buy parameters
                outcome_index = 0 if share_type.lower() == 'yes' else 1
                outcome_name = 'YES' if outcome_index == 0 else 'NO'
                scaling_factor = 10 ** 6  # USDC has 6 decimals
                investment_amount = int(size * scaling_factor)
                min_outcome_tokens_to_buy = 0 * scaling_factor

                # set gas details
                priority_fee = w3.to_wei(1, 'wei') 
                latest_block = await w3.eth.get_block('latest')
                base_fee = latest_block.get('baseFeePerGas')
                max_fee = base_fee * 2
            


                current_allowance = await usdc_contract.functions.allowance(account.address, current_market_address).call()

                if current_allowance < investment_amount:

                    logger.warning(f"USDC allowance ({current_allowance}) is less than required ({investment_amount}). Sending USDC approve transaction...")
            
                    #USDC APPROVAL
                    logger.info('Approving USDC token.....')
                    approve_nonce = await self.nonce_manager.get_nonce()
                    usdc_txn = await usdc_contract.functions.approve(market_address, investment_amount).build_transaction({
                            'from' : account.address,
                            'nonce' : approve_nonce,
                        })
                        
                    signed_usdc_txn = w3.eth.account.sign_transaction(usdc_txn, self.private_key)

                    usdc_txn_hash = await w3.eth.send_raw_transaction(signed_usdc_txn.raw_transaction)
                    logger.info(f"Approve txn sent: {usdc_txn_hash.to_0x_hex()}. Waiting for confirmation...")
                    usdc_txn_receipt = await w3.eth.wait_for_transaction_receipt(usdc_txn_hash)
                    logger.info(f"USDT txn confirmed in block: {usdc_txn_receipt['blockNumber']}")
                    logger.info(f'USDC approved... Check transaction: https://basescan.org/tx/{usdc_txn_hash.to_0x_hex()}')

                else:
                    logger.info("Sufficient USDC allowance already set. Skipping approval.")
                



                #BUY TRANSACTION
                logger.info(f"Preparing AMM 'buy' transaction: {size} USDC for {outcome_name} Shares")

                buy_nonce = await self.nonce_manager.get_nonce()

                buy_txn = await amm_contract.functions.buy(investment_amount,
                                                                outcome_index,
                                                                min_outcome_tokens_to_buy).build_transaction({
                        'from' : account.address,
                        'nonce' : buy_nonce,
                    })
                    
                signed_buy_txn = w3.eth.account.sign_transaction(buy_txn, self.private_key)

                buy_txn_hash = await w3.eth.send_raw_transaction(signed_buy_txn.raw_transaction)

                logger.info(f"Buy txn sent: {buy_txn_hash.hex()}. Waiting for confirmation...")
            
                buy_txn_receipt = await w3.eth.wait_for_transaction_receipt(buy_txn_hash)
                

                if buy_txn_receipt['status'] == 1:
                    trade_id = str(uuid.uuid4())
                    trade_market_data = self.market_data_cache.get(market_address, {})

                    trade_logger.log_trade({
                        'timestamp_utc': datetime.now(timezone.utc).isoformat(),
                        'trade_id': trade_id,
                        'market_address': market_address,
                        'market_slug': trade_market_data.get('slug', 'N/A'),
                        'action': 'BUY_ENTRY',
                        'side': share_type.upper(),
                        'usdc_amount': size,
                        'price': price,
                        'reason': reason,
                        'transaction_hash': buy_txn_hash.hex()
                    })
                    logger.info(f"Buy txn SUCCESSFUL. Confirmed in block: {buy_txn_receipt['blockNumber']}")
                    logger.info(f'See transaction: https://basescan.org/tx/{buy_txn_hash.to_0x_hex()} ')
                    logger.info(f"--- TRADE SUCCESSFUL FOR MARKET : {market_address} ---")
                    return True
                    
                else:
                        logger.error(f"Buy txn FAILED on-chain. Receipt: {buy_txn_receipt}")
                        return False 

            except Exception as e:
                logger.error(f'An error occured during AMM buy trade execution: {e}', exc_info = True)
                return False




    async def get_share_balance(self, market_address: str, share_type: str) -> int:
        """
        Queries the contract to get the actual balance of a specific share token.
        Returns the balance in base unit .
        """
        async with self.tx_lock:

            if not await self._ensure_web3_connected():
                logger.error("Cannot get share balance: Web3 provider is not connected.")
                return 0

            if not self.w3 or not self.private_key:
                logger.error("Cannot get share balance: Web3 provider/private key is not initialized.")
                return 0 
                
            w3 = self.w3
            erc1155_address = os.getenv('CONDITIONAL_TOKEN_ADDRESS')

            if not erc1155_address:
                logger.error('ERC1155 address is not set')
                return 0

            try:
                account = w3.eth.account.from_key(self.private_key)

                market_data = self.market_data_cache.get(market_address)

                if not market_data or 'tokens' not in market_data:
                    logger.warning('Market_data is not available')
                    return 0
                

                conditional_tokens_address = w3.to_checksum_address(erc1155_address)
                token_id = market_data['tokens'][share_type.lower()]
                
                with open('config/conditional_abi.json', 'r') as f:
                        erc1155_abi = json.load(f)

                token_contract = w3.eth.contract(address=conditional_tokens_address, abi=erc1155_abi)
                
                balance = await token_contract.functions.balanceOf(account.address, int(token_id)).call()
                return balance
            

            except Exception as e:
                logger.error(f"Failed to get share balance for {market_address}: {e}")
                return 0



    async def wait(self):
        """Keep connection alive and listen for events."""
        await self.sio.wait()





