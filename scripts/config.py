import os
import logging
from dotenv import find_dotenv, load_dotenv
from typing import Dict, Any
from scripts.market_fetcher import get_active_markets

logger = logging.getLogger(__name__)

async def load_config() -> Dict[str, Any]:
    """Loads all necessary configuration from environment variables and market data files."""

    # find_dotenv() will find the .env file in the project root
    load_dotenv(find_dotenv())
    
    logger.info("Loading configuration...")
    
    config = {
        'API_URL': os.getenv("API_BASE_URL"),
        'WS_URL': os.getenv("WS_URL"),
        'CLOB_CFT_ADDR': os.getenv("CLOB_CFT_ADDR"),
        'NEGRISK_CFT_ADDR': os.getenv("NEGRISK_CFT_ADDR"),
        'markets': []
    }

    try:
        config['markets'] = await get_active_markets()
        if not config['markets']:
            logger.warning("No valid hourly markets were loaded.")
        else:
            logger.info(f"Successfully loaded {len(config['markets'])} hourly markets.")
    except Exception as e:
        logger.error(f"Failed to load markets during configuration: {e}", exc_info=True)
        config['markets'] = []

    return config