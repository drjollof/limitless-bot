import logging
from scripts.order import create_order_payload_without_signature, create_signature_for_order_payload

logger = logging.getLogger(__name__)

def prepare_signed_order(user_data: dict, market_data: dict, trade_params: dict, private_key: str) -> dict:

    """Translates a trade intent into a fully signed EIP-712 order payload for the API."""

    price_in_dollars = trade_params["price"]
    amount = trade_params["size"]
    share_type = trade_params["share_type"]  # "YES" or "NO"
    
    side_code = 0  # 0 = BUY, 1 = SELL. We are buying shares.
    


    # Select token ID based on share type

    try:
        token_id = market_data['tokens'][share_type.lower()]

    except KeyError:
        raise ValueError(f"CRITICAL: Market '{market_data.get('slug')}' has a malformed 'tokens' object.")




    maker_address = user_data["account"] 
    fee_rate_bps = user_data.get("rank", {}).get("feeRateBps", 0)    # Get fee rate from user data...default to 0 if not available
    scaling_factor = 1000000  # For USDC with 6 decimals
    maker_amount = round(price_in_dollars * amount * scaling_factor)       
    taker_amount = round(amount * scaling_factor)

    logger.info(f"Preparing trade: {amount} '{share_type}' shares at ${price_in_dollars:.2f} each.")

    unsigned_payload = create_order_payload_without_signature(
        maker_address, 
        token_id, 
        maker_amount, 
        taker_amount, 
        fee_rate_bps,
        side_code
    )

    signature = create_signature_for_order_payload("CLOB", unsigned_payload, private_key)

    final_api_payload = {
        "order": {
        **unsigned_payload, 
        "price": price_in_dollars, 
        "signature": signature
        },
        "ownerId": user_data["id"],
        "orderType": "GTC",
        "marketSlug": market_data["slug"],
    }
    
    return final_api_payload