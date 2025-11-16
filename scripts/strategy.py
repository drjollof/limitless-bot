import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripts.websocket import CustomWebSocket

logger = logging.getLogger(__name__)


BUY_THRESHOLD = 0.70
TRADE_AMOUNT_USD = 1
STOP_LOSS_THRESHOLD = 0.20
TIME_WINDOW_MINUTES = 20


TRADE_IN_PROCESS = set()
OPEN_POSITIONS = {}


async def check_and_execute_buy_strategy(market_data: dict, client: "CustomWebSocket"):

    """
    trading strategy
    """


    trade_market_address = market_data.get('market_address')
    no_price = market_data.get('no_price')
    yes_price = market_data.get('yes_price')

    if not all([trade_market_address, no_price is not None, yes_price is not None]):
         logger.warning("Strategy received incomplete market data.. Skipping strategy")
         return


    full_market_data = client.market_data_cache.get(trade_market_address)
    if not full_market_data or 'expiration' not in full_market_data:
        return
    

    market_type = full_market_data.get('tradeType')
    expiration_ts_ms = full_market_data['expiration']
    expiration_time_utc = datetime.fromtimestamp(expiration_ts_ms/ 1000, tz= timezone.utc)
    now_utc = datetime.now(timezone.utc)
    minutes_to_expiration = (expiration_time_utc - now_utc).total_seconds()/60


    logging.debug(f"Evaluating strategy for market {trade_market_address}...{minutes_to_expiration:.2f} mins left" f" Current YES price: ${yes_price:.2f}. Current NO price: ${no_price:.2f}.")

    if trade_market_address in TRADE_IN_PROCESS:
        logger.info(f"Trade already in process for {trade_market_address}.. skipping..")
        return
    
    #STOP-LOSS LOGIC
    if trade_market_address in OPEN_POSITIONS and trade_market_address in client.traded_markets:
         position = OPEN_POSITIONS[trade_market_address]
         side = position['side']
         current_price = yes_price if side == 'YES' else no_price

         if current_price <= STOP_LOSS_THRESHOLD:

            logger.info(f"!!! STOP-LOSS TRIGGERED for {side} position on {trade_market_address} !!!")
                    
                    
            try:
                TRADE_IN_PROCESS.add(trade_market_address)
                logger.info(f"Current {side} price (${current_price:.2f}) is at or below stop-loss level (${STOP_LOSS_THRESHOLD:.2f}).")

                    
                share_balance = await client.get_share_balance(trade_market_address, side)
                logger.info(f"Current on-chain balance of {side} shares: {share_balance}")

                if share_balance > 0:
                    usdc_to_get_back = (share_balance / 10**6) * current_price
                    logger.info(f"Sending sell transaction for {side} shares to receive ${usdc_to_get_back:.2f}")
                            
                        
                    sell_successful = await client.execute_amm_sell(
                                market_address=trade_market_address,
                                share_type=side,
                                return_amount_usd=usdc_to_get_back,
                                price = current_price,
                                reason="Stop-Loss Triggered"
                            
                            )

                    if sell_successful:
                        logger.info(f"Successfully exited position for market {trade_market_address}. Updating state.")
                        del OPEN_POSITIONS[trade_market_address]

                    else:
                         logger.warning(f"Sell trade for {trade_market_address} failed. State not updated.")

                else: 
                    logger.warning("On-chain balance is zero.. Removing stale position from state")
                    del OPEN_POSITIONS[trade_market_address]


            finally:
                TRADE_IN_PROCESS.remove(trade_market_address)





    #ENTRY LOGIC
    elif trade_market_address not in OPEN_POSITIONS and trade_market_address not in client.traded_markets:

        if minutes_to_expiration <= TIME_WINDOW_MINUTES:

            share_type_to_buy = None
            price_to_buy = None

            if no_price >= BUY_THRESHOLD:
                 share_type_to_buy = 'NO'
                 price_to_buy = no_price

            elif yes_price >= BUY_THRESHOLD:
                 share_type_to_buy = 'YES'
                 price_to_buy = yes_price

            if share_type_to_buy:
                logger.info(f"STRATEGY TRIGGERED: Market in endgame and {share_type_to_buy} price is high")

                try:
                    TRADE_IN_PROCESS.add(trade_market_address)
                    
                    if market_type == 'clob':
                            await client.place_order(
                                market_address=trade_market_address, 
                                share_type= share_type_to_buy,
                                size=TRADE_AMOUNT_USD,        
                                price=market_data['no_price']
                        )
                        
                    elif market_type == 'amm':
                        buy_successful =  await client.execute_amm_buy(
                                    market_address=trade_market_address,
                                    share_type= share_type_to_buy,
                                    size=TRADE_AMOUNT_USD,
                                    price = price_to_buy,
                                    reason= f"{share_type_to_buy} price >= {BUY_THRESHOLD} at {TIME_WINDOW_MINUTES} mins left"
                                    )
                        
                        if buy_successful:
                                    logger.info(f"Successfully entered {share_type_to_buy} position for {trade_market_address}.. Updating state..")
                                    client.traded_markets.add(trade_market_address)
                                    OPEN_POSITIONS[trade_market_address] = {'side' : share_type_to_buy, 'entry_price': price_to_buy, 'size': TRADE_AMOUNT_USD}

                        else: 
                                    logger.warning(f"Buy trade for {share_type_to_buy} on {trade_market_address} failed.. State not updated.")


                finally:
                    if trade_market_address in TRADE_IN_PROCESS:
                        TRADE_IN_PROCESS.remove(trade_market_address)
                                




              


