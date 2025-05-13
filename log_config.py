import logging
from datetime import datetime
"""
log_config.py
log file configuration script

"""
def setup_logging():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[
                            logging.FileHandler("ScriptProcessingLog.log", mode="w"),  # File output A is append to existing, while w - overwrites the existing log file
                            logging.StreamHandler()  # Console output
                        ])

# Call the setup function to configure logging
setup_logging()

# Create a module-level logger
logger = logging.getLogger(__name__)