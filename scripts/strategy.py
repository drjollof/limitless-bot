import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripts.websocket import CustomWebSocket

logger = logging.getLogger(__name__)

async def check_and_execute_buy_strategy(market_data: dict, client: "CustomWebSocket"):
    """
    trading strategy: Buy 'NO' shares if the price is below a certain threshold.
    Buy 'YES' shares if the price is below a certain threshold.
    """


    #trade_market_address = market_data.get('market_address')
    trade_market_address = market_data['market_address']
    no_price = market_data.get('no_price')
    yes_price = market_data.get('yes_price')
    full_market_data = client.market_data_cache.get(trade_market_address)

    if not full_market_data:
        return
    
    market_type = full_market_data.get('tradeType')


    logging.info(f"Evaluating strategy for market {trade_market_address}. Current YES price: ${yes_price:.2f}. Current NO price: ${no_price:.2f}.")
    
    #check if current market address is already in traded market
    if trade_market_address in client.traded_markets:
        return
    

    # strategy logic goes here
    
    BUY_NO_THRESHOLD = 1.60  
    BUY_YES_THRESHOLD = 1.60 



    if market_data['no_price'] >= BUY_NO_THRESHOLD:
        logger.info(f"STRATEGY TRIGGERED: 'NO' price is at or above threshold. Buying NO.")

        # add market address to memory to avoid duplicate trade
        client.traded_markets.add(trade_market_address)

        if market_type == 'clob':
            await client.place_order(
                market_address=trade_market_address, 
                share_type="NO",
                size=1.0,         # $1 for testing
                price=market_data['no_price']
        )
        
        elif market_type == 'amm':
            await client.execute_amm_trade(
                market_address=trade_market_address,
                share_type= 'NO',
                size=1
                )


    if market_data['yes_price'] >= BUY_YES_THRESHOLD:
        logger.info(f"STRATEGY TRIGGERED: 'YES' price is at or above threshold. Buying YES.")
        
        # add market address to memory to avoid duplicate trade
        client.traded_markets.add(trade_market_address)

        if market_type == 'clob':
            await client.place_order(
            market_address= trade_market_address,
            share_type="YES",
            size=1.0,                # $1 for testing
            price=market_data['yes_price']
        )
        
        elif market_type == 'amm':
            await client.execute_amm_trade(
                market_address= trade_market_address,
                share_type= 'YES',
                size=1 # $1 for testing
            )

