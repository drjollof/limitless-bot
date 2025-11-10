import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripts.websocket import CustomWebSocket

logger = logging.getLogger(__name__)


BUY_NO_THRESHOLD = 0.60 
BUY_YES_THRESHOLD = 0.60
TRADE_AMOUNT_USD = 1

STOP_LOSS_FOR_YES_POSITION = 0.50
STOP_LOSS_FOR_NO_POSITION = 0.50


OPEN_POSITIONS = {}


async def check_and_execute_buy_strategy(market_data: dict, client: "CustomWebSocket"):

    """
    trading strategy: Buy 'NO' shares if the price is below a certain threshold.
    Buy 'YES' shares if the price is below a certain threshold.
    """


    #trade_market_address = market_data.get('market_address')
    trade_market_address = market_data['market_address']
    no_price = market_data['no_price']
    yes_price = market_data['yes_price']
    full_market_data = client.market_data_cache.get(trade_market_address)

    if not full_market_data:
        return
    
    market_type = full_market_data.get('tradeType')


    logging.info(f"Evaluating strategy for market {trade_market_address}. Current YES price: ${yes_price:.2f}. Current NO price: ${no_price:.2f}.")

    if trade_market_address not in OPEN_POSITIONS and trade_market_address not in client.traded_markets:

        if no_price >= BUY_NO_THRESHOLD:
            logger.info(f"STRATEGY TRIGGERED: 'NO' price is at or above threshold. Buying NO...")


            if market_type == 'clob':
                await client.place_order(
                    market_address=trade_market_address, 
                    share_type="NO",
                    size=TRADE_AMOUNT_USD,        
                    price=market_data['no_price']
            )
            
            elif market_type == 'amm':
                trade =  await client.execute_amm_buy(
                        market_address=trade_market_address,
                        share_type= 'NO',
                        size=TRADE_AMOUNT_USD
                        )
            
                if trade:
                        logger.info(f"Successfully entered NO position for market {trade_market_address}. Updating state...")
                        client.traded_markets.add(trade_market_address)
                        OPEN_POSITIONS[trade_market_address] = {'side': 'NO', 'entry_price': no_price, 'size': TRADE_AMOUNT_USD}
                    
                else:
                        logger.warning(f"Buy trade for NO on {trade_market_address} failed. State not updated.")



        if yes_price >= BUY_YES_THRESHOLD:
            logger.info(f"STRATEGY TRIGGERED: 'YES' price is at or above threshold. Buying YES.")
            
         
            if market_type == 'clob':
                await client.place_order(
                market_address= trade_market_address,
                share_type="YES",
                size=TRADE_AMOUNT_USD,            
                price=market_data['yes_price']
            )
            

            elif market_type == 'amm':
               
               trade_successful =  await client.execute_amm_buy(
                        market_address=trade_market_address,
                        share_type= 'YES',
                        size=TRADE_AMOUNT_USD
                        )
            
               if trade_successful:
                        logger.info(f"Successfully entered YES position for market {trade_market_address}. Updating state...")
                        client.traded_markets.add(trade_market_address)
                        OPEN_POSITIONS[trade_market_address] = {'side': 'YES', 'entry_price': yes_price, 'size': TRADE_AMOUNT_USD}
                    
               else:
                        logger.warning(f"Buy trade for YES on {trade_market_address} failed. State not updated.")



    elif trade_market_address in OPEN_POSITIONS: 
            position = OPEN_POSITIONS[trade_market_address]
            side = position['side']

            if side == 'YES':
                stop_loss_trigger_price =  STOP_LOSS_FOR_YES_POSITION

                current_price = yes_price 
                
            
                if current_price <= stop_loss_trigger_price:
                    logger.info(f"!!! STOP-LOSS TRIGGERED for YES position on {trade_market_address} !!!")
                    logger.info(f"Current YES price (${current_price:.2f}) is at or below stop-loss level (${stop_loss_trigger_price:.2f}).")

                
                    share_balance = await client.get_share_balance(trade_market_address, side)
                    logger.info(f"Current on-chain balance of {side} shares: {share_balance}")

                    if share_balance > 0:
                        
                        
                        usdc_to_get_back = (share_balance / 10**6) * current_price # Approximate
                        logger.info(f"Sending sell transaction for {side} shares to receive ${usdc_to_get_back:.2f}")
                        
                    
                        sell_successful = await client.execute_amm_sell(
                            market_address=trade_market_address,
                            share_type=side,
                            return_amount_usd=usdc_to_get_back

                        )

                        
                        if sell_successful:
                            logger.info(f"Successfully exited position for market {trade_market_address}. Updating state.")
                            del OPEN_POSITIONS[trade_market_address]
                            return

                    else:
                        logger.warning(f"Sell trade for {trade_market_address} failed. State not updated.")


            elif side == 'NO':
                stop_loss_trigger_price = STOP_LOSS_FOR_NO_POSITION
                current_price = no_price

                if current_price <= stop_loss_trigger_price:
                    logger.info(f"!!! STOP-LOSS TRIGGERED for NO position on {trade_market_address} !!!")
                    logger.info(f"Current NO price (${current_price:.2f}) is at or below stop-loss level (${stop_loss_trigger_price:.2f}).")

                
                    share_balance = await client.get_share_balance(trade_market_address, side)
                    logger.info(f"Current on-chain balance of {side} shares: {share_balance}")

                    if share_balance > 0:
                        
                        
                        usdc_to_get_back = (share_balance / 10**6) * current_price # Approximate

                        logger.info(f"Sending sell transaction for {side} shares to receive ${usdc_to_get_back:.2f}")

                        sell_successful = await client.execute_amm_sell(
                            market_address=trade_market_address,
                            share_type=side,
                            return_amount_usd=usdc_to_get_back

                        )

                        if sell_successful:
                            logger.info(f"Successfully exited position for market {trade_market_address}. Updating state.")
                            del OPEN_POSITIONS[trade_market_address]
                            return

                    else:
                        logger.warning(f"Sell trade for {trade_market_address} failed. State not updated.")
