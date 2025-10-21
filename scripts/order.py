import os
import time
import json
import logging
from eth_account import Account
from dotenv import load_dotenv
from eth_account.messages import encode_typed_data

load_dotenv()
logger = logging.getLogger(__name__)

ORDER_TYPES = {
    "Order": [
        {"name": "salt", "type": "uint256"}, 
        {"name": "maker", "type": "address"},
        {"name": "signer", "type": "address"}, 
        {"name": "taker", "type": "address"},
        {"name": "tokenId", "type": "uint256"}, 
        {"name": "makerAmount", "type": "uint256"},
        {"name": "takerAmount", "type": "uint256"}, 
        {"name": "expiration", "type": "uint256"},
        {"name": "nonce", "type": "uint256"}, 
        {"name": "feeRateBps", "type": "uint256"},
        {"name": "side", "type": "uint8"}, 
        {"name": "signatureType", "type": "uint8"},
    ]
}

def get_eip712_domain(market_type="CLOB"):
    contract_address = os.getenv('CLOB_CFT_ADDR') if market_type == "CLOB" else os.getenv('NEGRISK_CFT_ADDR')

    return {
        "name": "Limitless CTF Exchange", 
        "version": "1",
        "chainId": 8453,  #Base chain ID
        "verifyingContract": contract_address,
    }



def create_order_payload_without_signature(maker_address, token_id, maker_amount, taker_amount, fee_rate_bps, side_code: int):
    salt = int(time.time() * 1000) + (24 * 60 * 60 * 1000)
    return {
        "salt": salt, 
        "maker": maker_address,
        "signer": maker_address,
        "taker": "0x0000000000000000000000000000000000000000",
        "tokenId": str(token_id),
        "makerAmount": maker_amount,
        "takerAmount": taker_amount,
        "expiration": "0", 
        "nonce": 0, 
        "feeRateBps": fee_rate_bps,
        "side": side_code, #0 for buy, 1 for sell
        "signatureType": 0,
    }

def create_signature_for_order_payload(market_type, order_payload, private_key):

    # Remove '0x' prefix if present
    if private_key.startswith("0x"):
        private_key = private_key[2:]

    account = Account.from_key(private_key)
    domain_data = get_eip712_domain(market_type)
    
    message_data = {
        **order_payload,
        "tokenId": int(order_payload["tokenId"]),
        "expiration": int(order_payload["expiration"]) if order_payload["expiration"] else 0,
    }
    
    encoded_message = encode_typed_data(full_message={'domain': domain_data, 'types': ORDER_TYPES, 'primaryType': 'Order', 'message': message_data})
    signed_message = account.sign_message(encoded_message)
    return signed_message.signature.hex()