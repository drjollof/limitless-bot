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

    market_address = market_data.get('market_address')
    no_price = market_data.get('no_price')
    yes_price = market_data.get('yes_price')


    logging.info(f"Evaluating strategy for market {market_address}. Current YES price: ${yes_price:.2f}. Current NO price: ${no_price:.2f}.")


    # STRATEGY LOGIC GOES HERE
    
    BUY_NO_THRESHOLD = 0.40  # e.g., Buy NO if it's cheap
    BUY_YES_THRESHOLD = 0.40 # e.g., Buy YES if it's cheap

    if market_data['no_price'] <= BUY_NO_THRESHOLD:
        logger.info(f"STRATEGY TRIGGERED: 'NO' price is at or below threshold. Buying NO.")
        await client.place_order(
            market_address=market_data['market_address'],
            share_type="NO",
            size=1.0,         #  for testing
            price=market_data['no_price']
        )
    
    if market_data['yes_price'] <= BUY_YES_THRESHOLD:
        logger.info(f"STRATEGY TRIGGERED: 'YES' price is at or below threshold. Buying YES.")
        await client.place_order(
            market_address=market_data['market_address'],
            share_type="YES",
            size=1.0,                # for testing
            price=market_data['yes_price']
        )


