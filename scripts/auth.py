import os
import aiohttp
import logging
from dotenv import load_dotenv
from eth_account import Account
from eth_account.messages import encode_defunct

load_dotenv()
logger = logging.getLogger(__name__)


def string_to_hex(text):
    return "0x" + text.encode("utf-8").hex()


def sign_message(self, message: str) -> str:

    """Sign a message using the private key."""

    if not self.account:
        raise Exception("Private key not provided. Cannot sign message.")

    print(f"Signing message for account: {self.account.address}")

    message_hash = encode_defunct(text=message)
    signed_message = self.account.sign_message(message_hash)
    signature_hex = signed_message.signature.hex()

    print(
        f"Generated signature: {signature_hex[:10]}... (length: {len(signature_hex)})"
    )

    return signature_hex

async def get_signing_message_async():

    api_url = os.getenv('API_BASE_URL')

    timeout = aiohttp.ClientTimeout(total=10)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{api_url}/auth/signing-message") as response:
                response.raise_for_status()
                return await response.text()
    except Exception as e:
        logger.error(f"Error fetching signing message: {e}")
        raise



async def authenticate_async(private_key: str):

    api_url = os.getenv('API_BASE_URL')

    signing_message = await get_signing_message_async()
    
    if not private_key.startswith("0x"):
        private_key = "0x" + private_key
    account = Account.from_key(private_key)
    ethereum_address = account.address

    logger.info(f"Using address: {ethereum_address}")
    logger.info(f"Signing message: {repr(signing_message)}")
    
    message = encode_defunct(text=signing_message)
    signature = account.sign_message(message)
    sig_hex = signature.signature.hex()
    if not sig_hex.startswith("0x"):
        sig_hex = "0x" + sig_hex
    
    headers = {
        "x-account": ethereum_address,
        "x-signing-message": string_to_hex(signing_message),
        "x-signature": sig_hex,
        "Content-Type": "application/json",
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{api_url}/auth/login", headers=headers, json={"client": "eoa"}) as response:
                response.raise_for_status()
                session_cookie = response.cookies.get("limitless_session").value
                user_data = await response.json()
                return session_cookie, user_data
    except Exception as e:
        logger.error(f"Error during authentication login POST: {e}")
        raise