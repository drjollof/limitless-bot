import csv
import os
import logging
from datetime import datetime, timezone
from threading import Lock

logger = logging.getLogger(__name__)


class TradeLogger:
    def __init__(self, log_file='logs/trades.csv'):
        self.log_file = log_file
        self._lock = Lock()  
        self._initialize_file()



    def _initialize_file(self):

        """Creates the CSV file and writes the header if it doesn't exist."""
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)

        # 'with self._lock' to ensure thread-safe file access
        with self._lock:
            if not os.path.exists(self.log_file):
                try:
                    with open(self.log_file, 'w', newline='') as f:
                        writer = csv.writer(f)
                        
                        writer.writerow([
                            'timestamp_utc', 'trade_id', 'market_address', 'market_slug',
                            'action',      # 'BUY_ENTRY', 'SELL_STOP_LOSS'
                            'side',        # 'YES' or 'NO'
                            'usdc_amount', # Amount of USDC spent (for buys) or received (for sells)
                            'price',       # The price at which the trade was executed
                            'reason',      # reason for trade 'Price > Threshold', 'Stop-Loss Triggered'
                            'transaction_hash'
                        ])
                except IOError as e:
                    logger.error(f"Failed to initialize trade log file: {e}")


    def log_trade(self, trade_data: dict):
        """Appends a new trade record to the CSV file."""
        with self._lock:
            try:
                with open(self.log_file, 'a', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=trade_data.keys())
                    writer.writerow(trade_data)
                logger.info(f"Successfully logged trade: {trade_data.get('trade_id')}")
            except IOError as e:
                logger.error(f"Failed to write to trade log: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred during trade logging: {e}")


trade_logger = TradeLogger()